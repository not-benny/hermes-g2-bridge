from __future__ import annotations

import asyncio
import importlib

import pytest


@pytest.mark.asyncio
async def test_mcp_initialize_list_call_cache_and_invalidation(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    client = module.McpClient(send, request_timeout=0.5)

    init_task = asyncio.create_task(client.initialize())
    await asyncio.sleep(0)
    assert sent[-1]["method"] == "initialize"
    assert sent[-1]["params"]["protocolVersion"] == "2025-06-18"
    assert sent[-1]["params"]["clientInfo"] == {
        "name": "hermes-g2-bridge",
        "version": "1.5.0",
    }
    await client.handle_message(
        {"jsonrpc": "2.0", "id": sent[-1]["id"], "result": {"capabilities": {}}}
    )
    await init_task
    assert sent[-1]["method"] == "notifications/initialized"
    assert client.negotiated_identity == (None, None, None)

    list_task = asyncio.create_task(client.list_tools())
    await asyncio.sleep(0)
    list_request = sent[-1]
    tools = [{"name": "glasses.get_state", "inputSchema": {"type": "object"}}]
    await client.handle_message(
        {"jsonrpc": "2.0", "id": list_request["id"], "result": {"tools": tools}}
    )
    assert await list_task == tools
    sent_count = len(sent)
    assert await client.list_tools() == tools
    assert len(sent) == sent_count

    await client.handle_message(
        {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
    )
    refreshed = asyncio.create_task(client.list_tools())
    await asyncio.sleep(0)
    assert sent[-1]["method"] == "tools/list"
    await client.handle_message(
        {"jsonrpc": "2.0", "id": sent[-1]["id"], "result": {"tools": []}}
    )
    assert await refreshed == []

    call = asyncio.create_task(client.call_tool("glasses.get_state", {}))
    await asyncio.sleep(0)
    assert sent[-1]["method"] == "tools/call"
    assert sent[-1]["params"] == {"name": "glasses.get_state", "arguments": {}}
    await client.handle_message(
        {
            "jsonrpc": "2.0",
            "id": sent[-1]["id"],
            "result": {"content": [{"type": "text", "text": "ok"}]},
        }
    )
    assert (await call)["content"][0]["text"] == "ok"


@pytest.mark.asyncio
async def test_mcp_retains_negotiated_server_identity_for_fixed_contracts(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    client = module.McpClient(send, request_timeout=0.5)
    task = asyncio.create_task(client.initialize())
    await asyncio.sleep(0)
    await client.handle_message({
        "jsonrpc": "2.0",
        "id": sent[-1]["id"],
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": "hermes-g2", "version": "1.0.0"},
        },
    })
    await task
    assert client.negotiated_identity == ("2025-06-18", "hermes-g2", "1.0.0")
    await client.close()
    assert client.negotiated_identity == (None, None, None)


@pytest.mark.asyncio
async def test_mcp_handles_ping_timeout_and_clean_close(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    client = module.McpClient(send, request_timeout=0.01)
    await client.handle_message({"jsonrpc": "2.0", "id": 9, "method": "ping"})
    assert sent[-1] == {"jsonrpc": "2.0", "id": 9, "result": {}}

    with pytest.raises(TimeoutError, match="tools/list"):
        await client.request("tools/list", {})
    timed_out_id = sent[-2]["id"]
    assert sent[-1] == {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": timed_out_id, "reason": "timeout"},
    }

    pending = asyncio.create_task(client.request("tools/list", {}, timeout=1))
    await asyncio.sleep(0)
    await client.close()
    with pytest.raises(ConnectionError, match="closed"):
        await pending


@pytest.mark.asyncio
async def test_mcp_task_cancellation_targets_exact_request_and_late_result_is_inert(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    client = module.McpClient(send, request_timeout=1)
    first = asyncio.create_task(client.request("tools/call", {"name": "one"}))
    second = asyncio.create_task(client.request("tools/call", {"name": "two"}))
    await asyncio.sleep(0)
    first_id, second_id = sent[0]["id"], sent[1]["id"]

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert sent[-1] == {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": first_id, "reason": "cancelled"},
    }

    # A result arriving after local cancellation cannot resolve a sibling or
    # resurrect the removed request.
    await client.handle_message({"jsonrpc": "2.0", "id": first_id, "result": {"late": True}})
    assert not second.done()
    await client.handle_message({"jsonrpc": "2.0", "id": second_id, "result": {"ok": True}})
    assert await second == {"ok": True}


@pytest.mark.asyncio
async def test_mcp_local_cancellation_is_not_hung_by_stalled_cancel_transport(
    plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    monkeypatch.setattr(module, "_CANCEL_SEND_TIMEOUT_SECONDS", 0.01)
    request_sent = asyncio.Event()

    async def send(message: dict) -> None:
        if message.get("method") == "notifications/cancelled":
            await asyncio.Future()
        request_sent.set()

    client = module.McpClient(send, request_timeout=1)
    task = asyncio.create_task(client.request("tools/list", {}))
    await request_sent.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.1)


@pytest.mark.asyncio
async def test_mcp_ignores_unhashable_and_invalid_jsonrpc_ids(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.mcp_client")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    client = module.McpClient(send)
    for request_id in ({"nested": "id"}, [1], True):
        await client.handle_message({"jsonrpc": "2.0", "id": request_id, "result": {}})
        await client.handle_message({"jsonrpc": "2.0", "id": request_id, "method": "ping"})
    assert sent == []
