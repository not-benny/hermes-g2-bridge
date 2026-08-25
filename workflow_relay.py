"""Capability-verifying local relay for the standalone G2 workflow MCP.

The Unix socket is transport isolation only (0600 plus same-UID peer checks).
Authority is a process-local HMAC capability minted by Hermes core for one
exact package digest, public workflow, argument object and active G2 turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import stat
import struct
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping


logger = logging.getLogger(__name__)

_PROTOCOL_VERSION = 1
_MAX_LINE_BYTES = 64 * 1024
_REQUEST_TIMEOUT_SECONDS = 190.0
_MAX_LIVE_CAPABILITIES = 1024
_MAX_SUBCALLS = 8
_MAX_ATTEMPTS = 2
_CAPABILITY_AUDIENCE = "com.hermes.mcp/portable/hermes-g2-workflows/workflows"
_CAPABILITY_BINDING = "hermes-g2-workflows:workflows"
_REQUEST_ID = re.compile(r"^[0-9a-f]{32}$")


class _RelayRejectionStage(Enum):
    """Allowlisted relay diagnostics containing no request or exception data."""

    CAPABILITY_VERIFY = "capability_verify"
    BINDING = "binding"
    ACTIVE_TURN = "active_turn"
    REPLAY = "replay"
    DISPATCH = "dispatch"


def _log_relay_rejection(stage: _RelayRejectionStage) -> None:
    if type(stage) is not _RelayRejectionStage:
        raise TypeError("workflow relay rejection stage must be allowlisted")
    logger.warning("G2 workflow relay rejection stage=%s", stage.value)

# Exact internal sequence for each reviewed public workflow. A public
# capability can neither invoke a sibling workflow nor turn the relay into a
# generic native-tool endpoint.
WORKFLOW_INTERNAL_SEQUENCE: dict[str, tuple[str, ...]] = {
    "g2_work_task_add": ("g2.work_tasks.add",),
    "g2_clock_set_timer": ("g2.clock.set_timer",),
    "g2_clock_set_alarm": ("g2.clock.set_alarm",),
    "g2_reminder_create": ("g2.reminders.create",),
    "g2_weather_present": ("g2.weather.read_forecast", "g2.context.present"),
    "g2_train_departures_present": (
        "g2.transit.read_departures",
        "g2.context.present",
    ),
    "g2_apps_manage": ("g2.device.apps.manage",),
    "g2_media_control": ("g2.device.media.control",),
    "g2_navigation": ("g2.device.navigation",),
    "g2_notifications": ("g2.device.notifications",),
    "g2_health_summary": ("g2.device.health.summary",),
    "g2_calendar_agenda": ("g2.device.calendar.agenda",),
}


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _reject_nonfinite(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _profile_home() -> Path:
    raw = os.environ.get("HERMES_HOME")
    return Path(raw).expanduser().resolve() if raw else (Path.home() / ".hermes").resolve()


def relay_paths() -> Path:
    """Return the sole filesystem endpoint; no authority token file exists."""

    return _profile_home() / "run" / "g2-workflows.sock"


def _same_uid(writer: asyncio.StreamWriter) -> bool:
    if not hasattr(socket, "SO_PEERCRED"):
        return False
    transport_socket = writer.get_extra_info("socket")
    if transport_socket is None:
        return False
    try:
        raw = transport_socket.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        _pid, uid, _gid = struct.unpack("3i", raw)
        return uid == os.getuid()
    except (OSError, struct.error):
        return False


@dataclass
class _SubcallUse:
    fingerprint: str
    max_attempt: int


@dataclass
class _CapabilityUse:
    expires_at: int
    workflow: str
    current_subcall: int = 0
    subcalls: dict[int, _SubcallUse] = field(default_factory=dict)


class WorkflowRelay:
    """One profile-scoped Unix relay owned by the live platform adapter."""

    def __init__(self, authorize_active_turn: Callable[[Mapping[str, Any]], Any]) -> None:
        if not callable(authorize_active_turn):
            raise TypeError("WorkflowRelay requires an active-turn authorizer")
        self._authorize_active_turn = authorize_active_turn
        self._server: asyncio.AbstractServer | None = None
        self._socket_path = relay_paths()
        self._owns_socket_path = False
        self._uses: dict[str, _CapabilityUse] = {}
        self._uses_lock = asyncio.Lock()

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    async def start(self) -> None:
        if self._server is not None:
            return
        run_dir = self._socket_path.parent
        run_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(run_dir, stat.S_IRWXU)
        if self._socket_path.exists() or self._socket_path.is_symlink():
            if self._socket_path.is_socket():
                self._socket_path.unlink()
            else:
                raise RuntimeError("G2 workflow relay path is not a socket")
        # Remove the obsolete readable bearer-token authority from v0.2.
        legacy_token = run_dir / "g2-workflows.token"
        if legacy_token.exists() or legacy_token.is_symlink():
            if legacy_token.is_file() or legacy_token.is_symlink():
                legacy_token.unlink()
            else:
                raise RuntimeError("Legacy G2 workflow token path is unsafe")
        try:
            self._server = await asyncio.start_unix_server(
                self._handle_client,
                path=str(self._socket_path),
                limit=_MAX_LINE_BYTES + 1,
            )
            self._owns_socket_path = True
            os.chmod(self._socket_path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            await self.stop()
            raise

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()
        self._uses.clear()
        owned, self._owns_socket_path = self._owns_socket_path, False
        if owned:
            try:
                self._socket_path.unlink()
            except FileNotFoundError:
                pass

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            if not _same_uid(writer):
                return
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw or len(raw) > _MAX_LINE_BYTES or not raw.endswith(b"\n"):
                return
            try:
                request = json.loads(
                    raw,
                    object_pairs_hook=_unique_json_object,
                    parse_constant=_reject_nonfinite,
                )
            except (UnicodeError, ValueError):
                return
            response = await self._dispatch_while_connected(reader, request)
            if response is None:
                return
            encoded = json.dumps(
                response,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
            ).encode("utf-8")
            if len(encoded) <= _MAX_LINE_BYTES:
                writer.write(encoded + b"\n")
                await writer.drain()
        except (asyncio.TimeoutError, ConnectionError, OSError, ValueError):
            return
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _dispatch_while_connected(
        self, reader: asyncio.StreamReader, request: Any
    ) -> dict[str, Any] | None:
        """Cancel native work if the standalone caller cancels or disconnects."""

        dispatch_task = asyncio.create_task(self._dispatch(request))
        disconnect_task = asyncio.create_task(reader.read(1))
        tasks = {dispatch_task, disconnect_task}
        try:
            done, _pending = await asyncio.wait(
                tasks,
                timeout=_REQUEST_TIMEOUT_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if dispatch_task in done:
                return await dispatch_task
            # EOF, an unexpected second request byte, or the bounded timeout
            # all revoke this call's in-flight native authority.
            return None
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _error(request_id: str, message: str) -> dict[str, Any]:
        return {
            "version": _PROTOCOL_VERSION,
            "id": request_id,
            "ok": False,
            "error": message,
        }

    async def _claim_use(
        self,
        claims: Mapping[str, Any],
        *,
        workflow: str,
        tool: str,
        arguments: Mapping[str, Any],
        subcall_id: int,
        attempt: int,
    ) -> None:
        from tools.mcp_capability import canonical_arguments_digest

        sequence = WORKFLOW_INTERNAL_SEQUENCE.get(workflow)
        if (
            sequence is None
            or not 1 <= subcall_id <= len(sequence) <= _MAX_SUBCALLS
            or sequence[subcall_id - 1] != tool
        ):
            raise PermissionError("workflow subcall is not allowlisted")
        fingerprint = canonical_arguments_digest(
            {"tool": tool, "arguments": dict(arguments)}
        )
        nonce = str(claims["nonce"])
        expires_at = int(claims["expires_at"])
        now = int(time.time())
        async with self._uses_lock:
            for expired_nonce in tuple(
                key for key, state in self._uses.items() if state.expires_at <= now
            ):
                self._uses.pop(expired_nonce, None)
            state = self._uses.get(nonce)
            if state is None:
                if len(self._uses) >= _MAX_LIVE_CAPABILITIES or subcall_id != 1:
                    raise PermissionError("capability replay state is unavailable")
                # Attempt 2 may be the first packet observed when attempt 1
                # failed before reaching the relay. It remains one bounded use.
                state = _CapabilityUse(expires_at=expires_at, workflow=workflow)
                self._uses[nonce] = state
            if state.expires_at != expires_at or state.workflow != workflow:
                raise PermissionError("capability replay identity changed")

            previous = state.subcalls.get(subcall_id)
            if previous is None:
                if subcall_id != state.current_subcall + 1:
                    raise PermissionError("capability subcall is out of sequence")
                state.subcalls[subcall_id] = _SubcallUse(fingerprint, attempt)
                state.current_subcall = subcall_id
                return
            if (
                previous.fingerprint != fingerprint
                or attempt != previous.max_attempt + 1
                or attempt > _MAX_ATTEMPTS
            ):
                raise PermissionError("capability replay was rejected")
            previous.max_attempt = attempt

    async def _dispatch(self, request: Any) -> dict[str, Any]:
        fields = {
            "version",
            "id",
            "capability",
            "workflow",
            "workflow_arguments",
            "tool",
            "arguments",
            "subcall_id",
            "attempt",
        }
        if not isinstance(request, dict) or set(request) != fields:
            return self._error("", "invalid request")
        request_id = request.get("id")
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            return self._error("", "invalid request")
        if type(request.get("version")) is not int or request["version"] != _PROTOCOL_VERSION:
            return self._error(request_id, "invalid request")
        workflow = request.get("workflow")
        workflow_arguments = request.get("workflow_arguments")
        tool = request.get("tool")
        arguments = request.get("arguments")
        subcall_id = request.get("subcall_id")
        attempt = request.get("attempt")
        if (
            not isinstance(workflow, str)
            or not 1 <= len(workflow) <= 128
            or not isinstance(workflow_arguments, dict)
            or not isinstance(tool, str)
            or not 1 <= len(tool) <= 128
            or not isinstance(arguments, dict)
            or type(subcall_id) is not int
            or not 1 <= subcall_id <= _MAX_SUBCALLS
            or type(attempt) is not int
            or not 1 <= attempt <= _MAX_ATTEMPTS
        ):
            return self._error(request_id, "invalid request")

        try:
            from tools.mcp_capability import verify_mcp_capability

            claims = verify_mcp_capability(
                request.get("capability"),
                expected_audience=_CAPABILITY_AUDIENCE,
                expected_workflow=workflow,
                expected_arguments=workflow_arguments,
            )
        except (PermissionError, TypeError, ValueError, KeyError):
            _log_relay_rejection(_RelayRejectionStage.CAPABILITY_VERIFY)
            return self._error(request_id, "unauthorized")
        if claims.get("binding") != _CAPABILITY_BINDING:
            _log_relay_rejection(_RelayRejectionStage.BINDING)
            return self._error(request_id, "unauthorized")
        try:
            self._authorize_active_turn(claims)
        except (PermissionError, TypeError, ValueError, KeyError):
            _log_relay_rejection(_RelayRejectionStage.ACTIVE_TURN)
            return self._error(request_id, "unauthorized")
        try:
            await self._claim_use(
                claims,
                workflow=workflow,
                tool=tool,
                arguments=arguments,
                subcall_id=subcall_id,
                attempt=attempt,
            )
        except (PermissionError, TypeError, ValueError, KeyError):
            _log_relay_rejection(_RelayRejectionStage.REPLAY)
            return self._error(request_id, "unauthorized")

        try:
            from gateway.session_context import clear_session_vars, set_session_vars
            from .tools import dispatch_mcp_workflow

            tokens = set_session_vars(
                platform=str(claims["platform"]),
                source="mcp-capability",
                chat_id=str(claims["chat_id"]),
                message_id=str(claims["message_id"]),
                session_key=str(claims["session_id"]),
                session_id=str(claims["session_id"]),
                profile=str(claims["profile"]),
            )
            try:
                result = await dispatch_mcp_workflow(tool, arguments)
            finally:
                clear_session_vars(tokens)
            if not isinstance(result, str):
                raise TypeError("workflow dispatch result must be a string")
        except asyncio.CancelledError:
            raise
        except Exception:
            _log_relay_rejection(_RelayRejectionStage.DISPATCH)
            return self._error(request_id, "unavailable")
        return {
            "version": _PROTOCOL_VERSION,
            "id": request_id,
            "ok": True,
            "result": result,
        }
