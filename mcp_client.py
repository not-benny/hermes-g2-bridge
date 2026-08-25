"""Minimal MCP JSON-RPC client for the phone-hosted tool registry."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)

MCP_PROTOCOL_VERSION = "2025-06-18"
BRIDGE_CLIENT_VERSION = "1.5.0"
_CANCEL_SEND_TIMEOUT_SECONDS = 1.0


class McpClient:
    """MCP client over an owner-provided JSON frame transport."""

    def __init__(
        self,
        send: Callable[[dict[str, Any]], Awaitable[None]],
        *,
        request_timeout: float = 20.0,
    ) -> None:
        self._send = send
        self._request_timeout = float(request_timeout)
        self._next_id = 1
        self._pending: dict[str | int, asyncio.Future] = {}
        self._initialized = False
        self._protocol_version: str | None = None
        self._server_name: str | None = None
        self._server_version: str | None = None
        self._tool_cache: list[dict[str, Any]] | None = None
        self._closed = False

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def negotiated_identity(self) -> tuple[str | None, str | None, str | None]:
        """Protocol, server name, and version returned by initialize.

        Generic bridge behavior remains compatible with older peers, but fixed
        workflow callers pin and validate this tuple before any phone call.
        """
        return self._protocol_version, self._server_name, self._server_version

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        if self._closed:
            raise ConnectionError("MCP transport closed")
        request_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        message: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            message["params"] = params
        sent = False
        try:
            await self._send(message)
            sent = True
            wait = self._request_timeout if timeout is None else float(timeout)
            try:
                return await asyncio.wait_for(asyncio.shield(future), timeout=wait)
            except asyncio.TimeoutError as exc:
                await self._cancel_request(request_id, "timeout")
                raise TimeoutError(
                    f"MCP request {method} timed out after {wait:g}s"
                ) from exc
        except asyncio.CancelledError:
            if sent:
                await self._cancel_request(request_id, "cancelled")
            raise
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def _cancel_request(self, request_id: str | int, reason: str) -> None:
        """Best-effort standard MCP cancellation for one exact pending id."""

        if self._closed:
            return
        try:
            await asyncio.wait_for(
                self._send({
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": request_id, "reason": reason},
                }),
                timeout=_CANCEL_SEND_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            return
        except Exception:
            # Local cancellation must not be replaced by a transport-cleanup
            # error. A late result is inert because the pending id is removed.
            return

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        if self._closed:
            return
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        await self._send(message)

    async def handle_message(self, message: Any) -> None:
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return
        request_id = message.get("id")
        if request_id is not None and (
            isinstance(request_id, bool) or not isinstance(request_id, (str, int))
        ):
            return
        if request_id is not None and ("result" in message or "error" in message):
            future = self._pending.get(request_id)
            if future is None or future.done():
                return
            error = message.get("error")
            if isinstance(error, dict):
                detail = str(error.get("message") or "MCP error")
                code = error.get("code")
                future.set_exception(RuntimeError(f"{detail} (code {code})"))
            else:
                future.set_result(message.get("result"))
            return
        method = message.get("method")
        if request_id is not None and isinstance(method, str):
            if method == "ping":
                await self._send({"jsonrpc": "2.0", "id": request_id, "result": {}})
            else:
                await self._send(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32601,
                            "message": f"Method not supported: {method}",
                        },
                    }
                )
            return
        if method == "notifications/tools/list_changed":
            self._tool_cache = None
            logger.debug("G2 phone MCP tool list changed; cache invalidated")

    async def initialize(self) -> Any:
        result = await self.request(
            "initialize",
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "hermes-g2-bridge",
                    "version": BRIDGE_CLIENT_VERSION,
                },
            },
        )
        self._protocol_version = (
            result.get("protocolVersion")
            if isinstance(result, dict) and isinstance(result.get("protocolVersion"), str)
            else None
        )
        server_info = result.get("serverInfo") if isinstance(result, dict) else None
        self._server_name = (
            server_info.get("name")
            if isinstance(server_info, dict) and isinstance(server_info.get("name"), str)
            else None
        )
        self._server_version = (
            server_info.get("version")
            if isinstance(server_info, dict) and isinstance(server_info.get("version"), str)
            else None
        )
        await self.notify("notifications/initialized")
        self._initialized = True
        return result

    async def list_tools(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        if not self._initialized:
            raise RuntimeError("MCP session not initialized")
        if self._tool_cache is not None and not force_refresh:
            return list(self._tool_cache)
        result = await self.request("tools/list", {})
        raw_tools = result.get("tools") if isinstance(result, dict) else []
        tools = [tool for tool in (raw_tools or []) if isinstance(tool, dict)]
        self._tool_cache = tools
        return list(tools)

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        if not self._initialized:
            raise RuntimeError("MCP session not initialized")
        return await self.request(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
            timeout=timeout,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(ConnectionError("MCP transport closed"))
        self._pending.clear()
        self._tool_cache = None
        self._protocol_version = None
        self._server_name = None
        self._server_version = None
