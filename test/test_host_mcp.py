from __future__ import annotations

import asyncio
import importlib
import json

import pytest


async def _initialize(server, sent):
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "init-1",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "g2-phone", "version": "1.0"},
            },
        }
    )
    assert sent[-1] == {
        "jsonrpc": "2.0",
        "id": "init-1",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": True, "listChanged": False},
            },
            "serverInfo": {"name": "hermes-g2-host", "version": "1.0.0"},
        },
    }
    await server.handle_message(
        {"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    assert server.initialized


def _make_server(module, *, profile=None, on_conversate_cues=None, on_fatal=None):
    sent = []
    accepted = []
    cancelled = []

    async def send(message):
        sent.append(message)

    async def on_voice_turn(binding, request):
        accepted.append((binding, request))

    async def on_cancel(binding):
        cancelled.append(binding)

    server = module.HostSessionMcpServer(
        send,
        session_generation="host_connection_test",
        on_voice_turn=on_voice_turn,
        on_cancel=on_cancel,
        on_conversate_cues=on_conversate_cues,
        on_fatal=on_fatal,
        profile=profile,
    )
    return server, sent, accepted, cancelled


@pytest.mark.asyncio
async def test_host_status_and_cockpit_resources_replace_custom_frames(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    server, sent, accepted, cancelled = _make_server(module, profile="even-g2")
    await _initialize(server, sent)

    await server.handle_message(
        {"jsonrpc": "2.0", "id": "resources", "method": "resources/list"}
    )
    assert sent[-1]["result"]["resources"] == [
        module.HOST_STATUS_RESOURCE_SPEC,
        module.COCKPIT_STATE_RESOURCE_SPEC,
    ]

    async def read_status(request_id):
        await server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "resources/read",
                "params": {"uri": "hermes://session/status"},
            }
        )
        content = sent[-1]["result"]["contents"]
        assert len(content) == 1
        assert content[0]["uri"] == "hermes://session/status"
        assert content[0]["mimeType"] == "application/json"
        return json.loads(content[0]["text"])

    idle = await read_status("status-idle")
    assert idle == {
        "schemaVersion": 1,
        "connectionGeneration": "host_connection_test",
        "profile": "even-g2",
        "transport": {"state": "online", "authenticated": True},
        "sessionMcp": {
            "state": "ready",
            "voiceTurnState": "idle",
            "legacyChatFallback": False,
        },
        "cockpit": {
            "state": "online",
            "transport": "mcp-resource",
            "projection": "session-snapshot",
            "sharedSessions": 0,
            "commandsAvailable": False,
        },
        "companion": {
            "state": "unavailable",
            "reason": "backend-authority-absent",
            "commandsAvailable": False,
        },
    }
    assert "sessions" not in idle
    assert "transcript" not in idle

    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "voice-for-status",
            "method": "tools/call",
            "params": {
                "name": "hermes.voice.turn",
                "arguments": {"turnId": "status-turn", "text": "hello"},
            },
        }
    )
    assert accepted
    assert (await read_status("status-running"))["sessionMcp"][
        "voiceTurnState"
    ] == "running"
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": "voice-for-status"},
        }
    )
    assert cancelled == [accepted[0][0]]
    assert (await read_status("status-cancelling"))["sessionMcp"][
        "voiceTurnState"
    ] == "cancelling"

    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "unknown-resource",
            "method": "resources/read",
            "params": {"uri": "hermes://session/private-history"},
        }
    )
    assert sent[-1]["error"] == {
        "code": -32002,
        "message": "Unknown host MCP resource",
    }


@pytest.mark.asyncio
async def test_host_mcp_lists_bounded_tools_and_resolves_voice_only_on_completion(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    server, sent, accepted, _cancelled = _make_server(module)
    await _initialize(server, sent)

    await server.handle_message(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    )
    assert [tool["name"] for tool in sent[-1]["result"]["tools"]] == [
        "hermes.voice.turn",
        "hermes.cockpit.command",
    ]
    spec = sent[-1]["result"]["tools"][0]
    assert spec["inputSchema"]["additionalProperties"] is False
    assert spec["outputSchema"]["additionalProperties"] is False

    before_call = len(sent)
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "voice:7",
            "method": "tools/call",
            "params": {
                "name": "hermes.voice.turn",
                "arguments": {
                    "turnId": "phone-turn-7",
                    "text": "  What time is it?  ",
                    "context": {
                        "foregroundApp": "dashboard",
                        "screenOn": True,
                        "headsetBattery": 87,
                    },
                },
            },
        }
    )

    # Acceptance is intentionally silent: this long-running JSON-RPC request
    # remains open while Hermes thinks and uses tools.
    assert len(sent) == before_call
    assert server.pending_count == 1
    assert len(accepted) == 1
    binding, request = accepted[0]
    assert binding.request_id == "voice:7"
    assert binding.session_generation == "host_connection_test"
    assert binding.call_generation == 1
    assert request.text == "What time is it?"

    assert await server.complete_turn(
        binding,
        text="It is 10:42.",
        stop_reason="end_turn",
    )
    terminal = {
        "turnId": "phone-turn-7",
        "text": "It is 10:42.",
        "stopReason": "end_turn",
    }
    assert sent[-1] == {
        "jsonrpc": "2.0",
        "id": "voice:7",
        "result": {
            "content": [
                {
                    "type": "text",
                    "text": '{"turnId":"phone-turn-7","text":"It is 10:42.","stopReason":"end_turn"}',
                }
            ],
            "structuredContent": {
                **terminal,
                "generation": 1,
            },
            "isError": False,
        },
    }
    assert server.pending_count == 0
    assert not await server.complete_turn(
        binding, text="late", stop_reason="end_turn"
    )

    # Completed IDs remain reserved for the bounded life of the connection;
    # delayed cancellation cannot alias a newer request after cache churn.
    for index in range(300):
        await server.handle_message(
            {"jsonrpc": "2.0", "id": f"probe-{index}", "method": "ping"}
        )
    await server.handle_message(
        {"jsonrpc": "2.0", "id": "voice:7", "method": "ping"}
    )
    assert sent[-1]["error"]["message"] == "Duplicate request id"


@pytest.mark.asyncio
async def test_host_mcp_cancellation_is_exact_and_waits_for_turn_completion(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    server, sent, accepted, cancelled = _make_server(module)
    await _initialize(server, sent)
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "hermes.voice.turn",
                "arguments": {"turnId": "cancel-me", "text": "keep listening"},
            },
        }
    )
    binding = accepted[0][0]
    outbound_before_cancel = len(sent)

    # JSON-RPC number 9 and string "9" are different request identities.
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": "9", "reason": "wrong typed id"},
        }
    )
    assert cancelled == []
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": 9, "reason": "wearer dismissed"},
        }
    )
    assert cancelled == [binding]
    assert len(sent) == outbound_before_cancel

    # Duplicate cancellation is idempotent and cannot cancel another turn.
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": 9},
        }
    )
    assert cancelled == [binding]
    assert await server.complete_turn(binding, text="", stop_reason="cancelled")
    assert sent[-1]["id"] == 9
    assert sent[-1]["result"]["isError"] is True
    assert sent[-1]["result"]["structuredContent"]["stopReason"] == "cancelled"


@pytest.mark.asyncio
async def test_optional_conversate_cues_are_strict_latest_wins_and_tool_isolated(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    accepted = []
    releases = {1: asyncio.Event(), 2: asyncio.Event()}

    async def on_cues(request):
        accepted.append(request)
        await releases[request.revision].wait()
        return [{"kind": "question", "text": f"Question for revision {request.revision}?"}]

    server, sent, _voice, _cancelled = _make_server(
        module, on_conversate_cues=on_cues
    )
    await _initialize(server, sent)
    await server.handle_message(
        {"jsonrpc": "2.0", "id": "cue-list", "method": "tools/list", "params": {}}
    )
    assert [tool["name"] for tool in sent[-1]["result"]["tools"]] == [
        "hermes.voice.turn",
        "hermes.cockpit.command",
        "hermes.conversate.cues",
    ]
    cue_spec = sent[-1]["result"]["tools"][-1]
    assert cue_spec["inputSchema"]["additionalProperties"] is False
    assert cue_spec["outputSchema"]["properties"]["cues"]["maxItems"] == 3

    def call(request_id, revision, transcript):
        return server.handle_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": "hermes.conversate.cues",
                "arguments": {
                    "sessionId": "cv-session",
                    "revision": revision,
                    "transcript": transcript,
                },
            },
        })

    await call("cue-1", 1, "First finalized transcript")
    await asyncio.sleep(0)
    assert [request.revision for request in accepted] == [1]
    await call("cue-2", 2, "Newer finalized transcript")
    assert next(message for message in sent if message.get("id") == "cue-1")["result"]["isError"] is True
    await asyncio.sleep(0)
    assert [request.revision for request in accepted] == [1, 2]
    releases[2].set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    terminal = next(message for message in sent if message.get("id") == "cue-2")
    assert terminal["result"]["isError"] is False
    assert json.loads(terminal["result"]["content"][0]["text"]) == {
        "sessionId": "cv-session",
        "revision": 2,
        "cues": [{"kind": "question", "text": "Question for revision 2?"}],
    }
    assert server.pending_count == 0, "cue jobs never occupy the voice-turn lane"


@pytest.mark.asyncio
async def test_conversate_cue_deadline_fails_closed(plugin_package, monkeypatch):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    monkeypatch.setattr(module, "_CONVERSATE_CUE_DEADLINE_SECONDS", 0.01)

    async def slow_cues(_request):
        await asyncio.sleep(1)
        return []

    server, sent, _voice, _cancelled = _make_server(
        module, on_conversate_cues=slow_cues
    )
    await _initialize(server, sent)
    await server.handle_message({
        "jsonrpc": "2.0",
        "id": "cue-timeout",
        "method": "tools/call",
        "params": {"name": "hermes.conversate.cues", "arguments": {
            "sessionId": "cv-session", "revision": 1, "transcript": "Recent words",
        }},
    })
    await asyncio.sleep(0.03)
    terminal = next(message for message in sent if message.get("id") == "cue-timeout")
    assert terminal["result"] == {
        "content": [{"type": "text", "text": "Conversate cue request timed out"}],
        "isError": True,
    }


@pytest.mark.asyncio
async def test_conversate_latest_wins_detaches_a_provider_that_suppresses_cancellation(
    plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    monkeypatch.setattr(module, "_CONVERSATE_CUE_CANCEL_GRACE_SECONDS", 0.01)
    first_started = asyncio.Event()
    first_cancelled = asyncio.Event()
    release_first = asyncio.Event()

    async def cancellation_suppressing_cues(request):
        if request.revision == 1:
            first_started.set()
            try:
                await release_first.wait()
            except asyncio.CancelledError:
                first_cancelled.set()
                await release_first.wait()
            return [{"kind": "topic", "text": "Late stale cue"}]
        return [{"kind": "topic", "text": "Fresh cue"}]

    server, sent, _voice, _cancelled = _make_server(
        module, on_conversate_cues=cancellation_suppressing_cues
    )
    await _initialize(server, sent)

    def call(request_id, revision):
        return server.handle_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": "hermes.conversate.cues", "arguments": {
                "sessionId": "cv-session",
                "revision": revision,
                "transcript": f"Transcript revision {revision}",
            }},
        })

    await call("cue-stubborn-1", 1)
    await asyncio.wait_for(first_started.wait(), timeout=0.1)
    await asyncio.wait_for(call("cue-stubborn-2", 2), timeout=0.1)
    await asyncio.wait_for(first_cancelled.wait(), timeout=0.1)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    first_frames = [message for message in sent if message.get("id") == "cue-stubborn-1"]
    second_frames = [message for message in sent if message.get("id") == "cue-stubborn-2"]
    assert len(first_frames) == 1 and first_frames[0]["result"]["isError"] is True
    assert len(second_frames) == 1 and second_frames[0]["result"]["isError"] is False
    assert second_frames[0]["result"]["structuredContent"]["revision"] == 2

    release_first.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert len([message for message in sent if message.get("id") == "cue-stubborn-1"]) == 1
    server.close()


@pytest.mark.asyncio
async def test_completed_id_exhaustion_is_connection_fatal_and_a_new_generation_recovers(
    plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    monkeypatch.setattr(module, "_MAX_REQUESTS_PER_SESSION", 2)
    fatals = []

    async def on_cues(_request):
        return []

    async def on_fatal(reason):
        fatals.append(reason)

    server, sent, _voice, _cancelled = _make_server(
        module, on_conversate_cues=on_cues, on_fatal=on_fatal
    )
    await _initialize(server, sent)  # first connection-scoped tombstone

    def cue(server_under_test, request_id, revision):
        return server_under_test.handle_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": "hermes.conversate.cues", "arguments": {
                "sessionId": "cv-session",
                "revision": revision,
                "transcript": f"Transcript revision {revision}",
            }},
        })

    await cue(server, "cue-budget-1", 1)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    first = next(message for message in sent if message.get("id") == "cue-budget-1")
    assert first["result"]["isError"] is False

    await cue(server, "cue-budget-2", 2)
    exhausted = next(message for message in sent if message.get("id") == "cue-budget-2")
    assert exhausted["error"] == {
        "code": -32005,
        "message": "Host MCP request budget exhausted; reconnect before retrying",
    }
    assert fatals == ["Host MCP request budget exhausted; reconnect before retrying"]
    assert not server.initialized

    recovered, recovered_sent, _voice, _cancelled = _make_server(
        module, on_conversate_cues=on_cues, on_fatal=on_fatal
    )
    await _initialize(recovered, recovered_sent)
    await cue(recovered, "cue-budget-fresh", 1)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    fresh = next(message for message in recovered_sent if message.get("id") == "cue-budget-fresh")
    assert fresh["result"]["isError"] is False
    recovered.close()


@pytest.mark.asyncio
async def test_host_mcp_rejects_invalid_inputs_duplicate_ids_and_overlapping_turns(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    server, sent, accepted, _cancelled = _make_server(module)
    await _initialize(server, sent)

    for request_id in (True, -1, 9_007_199_254_740_992, "bad id", "x" * 129):
        before = len(sent)
        await server.handle_message(
            {"jsonrpc": "2.0", "id": request_id, "method": "ping"}
        )
        assert len(sent) == before

    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "extra-field",
            "method": "ping",
            "unexpected": True,
        }
    )
    assert sent[-1]["error"]["code"] == -32600
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "ping-params",
            "method": "ping",
            "params": {"unexpected": True},
        }
    )
    assert sent[-1]["error"]["code"] == -32602

    invalid_arguments = [
        {"turnId": "", "text": "hello"},
        {"turnId": "bad/id", "text": "hello"},
        {"turnId": "x" * 129, "text": "hello"},
        {"turn_id": "snake-case-is-not-the-phone-contract", "text": "hello"},
        {"turnId": "ok", "text": "line one\nline two"},
        {"turnId": "ok", "text": "x" * 4_097},
        {"turnId": "ok", "text": "hello", "unknown": True},
        {"turnId": "ok", "text": "hello", "context": {"private": "value"}},
        {"turnId": "ok", "text": "hello", "context": {"headsetBattery": 101}},
        {"turnId": "ok", "text": "hello", "context": {"headsetBattery": float("nan")}},
    ]
    for index, arguments in enumerate(invalid_arguments, start=20):
        await server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {"name": "hermes.voice.turn", "arguments": arguments},
            }
        )
        assert sent[-1]["id"] == index
        assert sent[-1]["error"]["code"] == -32602
    assert accepted == []

    valid_call = {
        "jsonrpc": "2.0",
        "id": "active",
        "method": "tools/call",
        "params": {
            "name": "hermes.voice.turn",
            "arguments": {"turnId": "one", "text": "first"},
        },
    }
    await server.handle_message(valid_call)
    assert len(accepted) == 1
    await server.handle_message(
        {
            **valid_call,
            "id": "overlap",
            "params": {
                "name": "hermes.voice.turn",
                "arguments": {"turnId": "two", "text": "second"},
            },
        }
    )
    assert sent[-1]["id"] == "overlap"
    assert sent[-1]["error"]["code"] == -32003
    assert len(accepted) == 1

    before_duplicate = len(sent)
    await server.handle_message(valid_call)
    assert len(sent) == before_duplicate + 1
    assert sent[-1]["error"]["message"] == "Duplicate request id"


@pytest.mark.asyncio
async def test_host_mcp_close_invalidates_generation_and_pending_binding(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    with pytest.raises(ValueError):
        module.HostSessionMcpServer(
            lambda _message: None,
            session_generation="bad generation",
            on_voice_turn=lambda _binding, _request: None,
            on_cancel=lambda _binding: None,
        )

    server, sent, accepted, _cancelled = _make_server(module)
    await _initialize(server, sent)
    await server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": "close-me",
            "method": "tools/call",
            "params": {
                "name": "hermes.voice.turn",
                "arguments": {"turnId": "turn-close", "text": "hello"},
            },
        }
    )
    binding = accepted[0][0]
    server.close()
    before = len(sent)
    assert not await server.complete_turn(
        binding, text="must not cross reconnect", stop_reason="end_turn"
    )
    assert len(sent) == before
    assert server.pending_count == 0


@pytest.mark.asyncio
async def test_live_cockpit_snapshot_and_all_exact_commands_dispatch(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    sent = []
    voice_calls = []
    dispatched = []

    async def send(message):
        sent.append(message)

    async def on_voice_turn(binding, request):
        voice_calls.append((binding, request))

    async def on_cancel(_binding):
        return None

    async def on_command(command, backend):
        dispatched.append((command, backend))
        return "accepted", None

    server = module.HostSessionMcpServer(
        send,
        session_generation="host_connection_test",
        on_voice_turn=on_voice_turn,
        on_cancel=on_cancel,
        on_cockpit_command=on_command,
        profile="even-g2",
    )
    await _initialize(server, sent)
    await server.handle_message({
        "jsonrpc": "2.0", "id": "subscribe-cockpit", "method": "resources/subscribe",
        "params": {"uri": module.COCKPIT_STATE_RESOURCE_URI},
    })
    await server.handle_message({
        "jsonrpc": "2.0", "id": "voice-cockpit", "method": "tools/call",
        "params": {"name": module.VOICE_TURN_TOOL, "arguments": {
            "turnId": "phone-cockpit-turn", "text": "Prepare the release",
        }},
    })
    binding = voice_calls[0][0]
    session_id = await server.open_cockpit_turn(
        binding, generation=7, user_text="Prepare the release"
    )
    assert session_id
    assert sent[-1] == {
        "jsonrpc": "2.0", "method": "notifications/resources/updated",
        "params": {"uri": module.COCKPIT_STATE_RESOURCE_URI},
    }
    assert await server.open_cockpit_question(
        binding,
        clarify_id="clarify-real-1",
        session_key="g2:glasses",
        question="Which target?",
        choices=["Staging", "Production"],
    )
    assert await server.open_cockpit_permission(
        binding,
        session_key="g2:glasses",
        approval_request_id="approval-backend-1",
        command="deploy --staging",
        description="Deploy once",
    )

    async def snapshot(request_id):
        await server.handle_message({
            "jsonrpc": "2.0", "id": request_id, "method": "resources/read",
            "params": {"uri": module.COCKPIT_STATE_RESOURCE_URI},
        })
        response = next(item for item in reversed(sent) if item.get("id") == request_id)
        return json.loads(response["result"]["contents"][0]["text"])

    state = await snapshot("cockpit-state-1")
    assert state["type"] == "snapshot"
    assert len(state["sessions"]) == 1
    session = state["sessions"][0]
    assert session["state"] == "waiting_human"
    assert session["timeline"][0]["kind"] == "user"
    question, permission = session["pending"]

    serial = 0

    async def submit(arguments):
        nonlocal serial
        serial += 1
        request_id = f"cockpit-command-{serial}"
        await server.handle_message({
            "jsonrpc": "2.0", "id": request_id, "method": "tools/call",
            "params": {"name": module.COCKPIT_COMMAND_TOOL, "arguments": arguments},
        })
        response = next(item for item in reversed(sent) if item.get("id") == request_id)
        receipt = json.loads(response["result"]["content"][0]["text"])
        assert receipt["outcome"] == "accepted"
        return receipt

    base = {
        "v": 1,
        "chan": "cockpit",
        "connection_generation": "host_connection_test",
        "session_id": session_id,
        "generation": 7,
    }
    await submit({
        **base,
        "type": "answer",
        "command_id": "command_answer_12345",
        "request_id": question["request_id"],
        "nonce": question["nonce"],
        "choice_id": question["choices"][0]["id"],
    })
    await submit({
        **base,
        "type": "permission_decide",
        "command_id": "command_permission_1",
        "request_id": permission["request_id"],
        "nonce": permission["nonce"],
        "decision": "allow_once",
    })
    await submit({
        **base,
        "type": "steer",
        "command_id": "command_steer_12345",
        "text": "Use the focused tests",
    })
    await submit({
        **base,
        "type": "interrupt",
        "command_id": "command_interrupt_1",
    })
    assert [item[0]["type"] for item in dispatched] == [
        "answer", "permission_decide", "steer", "interrupt"
    ]
    assert dispatched[0][1]["clarify_id"] == "clarify-real-1"
    assert dispatched[1][1]["session_key"] == "g2:glasses"
    assert dispatched[1][1]["approval_request_id"] == "approval-backend-1"

    state = await snapshot("cockpit-state-2")
    assert state["sessions"][0]["state"] == "interrupting"
    assert state["sessions"][0]["pending"] == []
    assert await server.finish_cockpit_turn(
        binding, text="", stop_reason="cancelled"
    )
    state = await snapshot("cockpit-state-3")
    assert state["sessions"][0]["state"] == "interrupted"
    assert state["sessions"][0]["timeline"][-1]["kind"] == "status"


@pytest.mark.asyncio
async def test_cockpit_notification_failures_cannot_block_voice_start_or_final(
    plugin_package,
):
    module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    sent = []
    accepted = []

    async def send(message):
        if message.get("method") == "notifications/resources/updated":
            raise ConnectionError("deterministic notification failure")
        sent.append(message)

    async def on_voice_turn(binding, request):
        accepted.append((binding, request))

    async def on_cancel(_binding):
        return None

    server = module.HostSessionMcpServer(
        send,
        session_generation="host_connection_test",
        on_voice_turn=on_voice_turn,
        on_cancel=on_cancel,
    )
    await _initialize(server, sent)
    await server.handle_message({
        "jsonrpc": "2.0", "id": "subscribe-failing", "method": "resources/subscribe",
        "params": {"uri": module.COCKPIT_STATE_RESOURCE_URI},
    })
    await server.handle_message({
        "jsonrpc": "2.0", "id": "voice-notify-failure", "method": "tools/call",
        "params": {"name": module.VOICE_TURN_TOOL, "arguments": {
            "turnId": "turn-notify-failure", "text": "Keep voice authoritative",
        }},
    })
    binding = accepted[0][0]
    assert await server.open_cockpit_turn(
        binding, generation=1, user_text="Keep voice authoritative"
    )
    assert await server.finish_cockpit_turn(
        binding, text="Authoritative final", stop_reason="end_turn"
    )
    assert await server.complete_turn(
        binding, text="Authoritative final", stop_reason="end_turn"
    )
    terminal = next(
        item for item in sent if item.get("id") == "voice-notify-failure"
    )
    assert terminal["result"]["isError"] is False
    assert terminal["result"]["structuredContent"]["text"] == "Authoritative final"
    snapshot = server._cockpit_snapshot()
    assert snapshot["sessions"][0]["state"] == "completed"
    assert snapshot["sessions"][0]["timeline"][-1]["kind"] == "assistant"
