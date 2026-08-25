from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
import websockets

from gateway.config import PlatformConfig
from gateway.platforms.base import ProcessingOutcome


async def _send(ws, **frame):
    await ws.send(json.dumps({"v": 1, **frame}))


async def _recv(ws):
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=2))


async def _authenticate_and_serve_mcp(
    ws, token="secret", device="fake-phone", capabilities=None
):
    if capabilities is None:
        capabilities = ["host-mcp-v1"]
    hello = {"chan": "ctl", "type": "hello", "token": token, "deviceName": device}
    if capabilities is not None:
        hello["capabilities"] = capabilities
    await _send(ws, **hello)
    ack = await _recv(ws)
    assert ack["chan"] == "ctl" and ack["type"] == "hello-ack"
    assert "host-mcp-v1" in capabilities
    expected = ["host-mcp-v1"]
    if "conversate-cues-v1" in capabilities:
        expected.append("conversate-cues-v1")
    assert ack["capabilities"] == expected
    return ack


async def _serve_device_mcp_ready(ws):
    """Finish the independent phone-owned MCP handshake."""
    while True:
        frame = await _recv(ws)
        if frame.get("chan") != "mcp":
            raise AssertionError(f"unexpected frame while initializing device MCP: {frame}")
        message = frame.get("msg") or {}
        if message.get("method") == "initialize":
            await _send(
                ws,
                chan="mcp",
                msg={
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {"tools": {"listChanged": True}},
                        "serverInfo": {"name": "fake-phone", "version": "1"},
                    },
                },
            )
        elif message.get("method") == "tools/list":
            await _send(
                ws,
                chan="mcp",
                msg={"jsonrpc": "2.0", "id": message["id"], "result": {"tools": []}},
            )
            return


async def _initialize_host_mcp(ws):
    await _send(
        ws,
        chan="host-mcp",
        msg={
            "jsonrpc": "2.0",
            "id": "host-init",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "fake-g2", "version": "1"},
            },
        },
    )
    frame = await _recv(ws)
    assert frame["chan"] == "host-mcp"
    assert frame["msg"]["id"] == "host-init"
    assert frame["msg"]["result"]["capabilities"] == {
        "tools": {"listChanged": False},
        "resources": {"subscribe": True, "listChanged": False},
    }
    await _send(
        ws,
        chan="host-mcp",
        msg={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )


def test_bridge_logs_do_not_include_peer_addresses_or_device_names(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    assert module.__file__ is not None
    source = Path(module.__file__).read_text()
    assert "remote_address" not in source
    assert not any("logger." in line and "device_name" in line for line in source.splitlines())
    assert not any("logger." in line and "self.host" in line for line in source.splitlines())


def test_websocket_json_boundary_rejects_nonfinite_and_duplicate_keys(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    assert module.G2Adapter._decode_frame('{"chan":"ctl","value":NaN}') is None
    assert module.G2Adapter._decode_frame('{"chan":"ctl","chan":"mcp"}') is None
    assert module.G2Adapter._decode_frame('{"chan":"ctl","value":1}') == {
        "chan": "ctl",
        "value": 1,
    }


@pytest.mark.asyncio
async def test_conversate_cues_use_only_the_tool_free_auxiliary_lane(
    adapter, plugin_package, monkeypatch
):
    adapter_module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_mcp = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    auxiliary = importlib.import_module("agent.auxiliary_client")
    calls = []

    async def fake_async_call_llm(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(
            content='{"cues":[{"kind":"topic","text":"Explore the release trade-offs"}]}'
        ))])

    monkeypatch.setattr(auxiliary, "async_call_llm", fake_async_call_llm)
    cues = await adapter._generate_conversate_cues(host_mcp.HostConversateCuesRequest(
        session_id="cv-session",
        revision=4,
        transcript="We are weighing release options",
    ))
    assert cues == [{"kind": "topic", "text": "Explore the release trade-offs"}]
    assert len(calls) == 1
    assert calls[0]["task"] == "hermes_g2_conversate_cues"
    assert calls[0]["tools"] is None
    assert calls[0]["timeout"] == 2.25
    assert calls[0]["max_tokens"] == 220
    assert calls[0]["sensitive_content"] is True
    assert calls[0]["allow_provider_fallback"] is False
    assert "recentTranscript" in calls[0]["messages"][1]["content"]
    assert not hasattr(adapter, "_active_turn_id") or adapter._active_turn_id is None

    async def fenced_response(**_kwargs):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(
            content='```json\n{"cues":[]}\n```'
        ))])

    monkeypatch.setattr(auxiliary, "async_call_llm", fenced_response)
    with pytest.raises(json.JSONDecodeError):
        await adapter._generate_conversate_cues(host_mcp.HostConversateCuesRequest(
            session_id="cv-session", revision=5, transcript="More words"
        ))

    for oversized in (
        "x" * (adapter_module._MAX_CONVERSATE_CUE_RESPONSE_SCALARS + 1),
        "💬" * (adapter_module._MAX_CONVERSATE_CUE_RESPONSE_BYTES // 4 + 1),
    ):
        async def oversized_response(**_kwargs):
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(
                content=oversized
            ))])

        monkeypatch.setattr(auxiliary, "async_call_llm", oversized_response)
        with pytest.raises(ValueError, match="exceeded its output limit"):
            await adapter._generate_conversate_cues(host_mcp.HostConversateCuesRequest(
                session_id="cv-session", revision=6, transcript="Bound this response"
            ))


@pytest.mark.asyncio
async def test_conversate_cue_tool_is_exposed_only_when_both_sides_negotiate(adapter):
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        ack = await _authenticate_and_serve_mcp(
            phone, capabilities=["host-mcp-v1", "conversate-cues-v1"]
        )
        assert ack["capabilities"] == ["host-mcp-v1", "conversate-cues-v1"]
        await _serve_device_mcp_ready(phone)
        await _initialize_host_mcp(phone)
        await _send(
            phone,
            chan="host-mcp",
            msg={"jsonrpc": "2.0", "id": "cue-tools", "method": "tools/list", "params": {}},
        )
        listed = await _recv(phone)
        assert [tool["name"] for tool in listed["msg"]["result"]["tools"]] == [
            "hermes.voice.turn",
            "hermes.cockpit.command",
            "hermes.conversate.cues",
        ]


@pytest.mark.asyncio
async def test_workflow_capability_maps_routing_key_to_exact_persisted_session_id(
    adapter, plugin_package
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    turn = module._Turn(
        turn_id="weather-turn",
        event_id="g2-turn-17-weather-turn",
        session_key="agent:main:g2:dm:glasses-context-v18",
        generation=17,
    )
    adapter._phone = SimpleNamespace()
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    adapter._session_store = SimpleNamespace(
        peek_session_id=lambda key: (
            "20260825_071849_ebe2e02a" if key == turn.session_key else None
        )
    )
    claims = {
        "platform": "g2",
        "profile": "even-g2",
        "chat_id": adapter.session_chat_id,
        "message_id": turn.event_id,
        "session_id": "20260825_071849_ebe2e02a",
    }

    authorization = adapter.authorize_workflow_capability(claims)
    assert authorization.turn_id == turn.turn_id
    assert authorization.turn_generation == turn.generation

    claims["session_id"] = turn.session_key
    with pytest.raises(PermissionError):
        adapter.authorize_workflow_capability(claims)

    adapter._session_store = SimpleNamespace(peek_session_id=lambda _key: None)
    claims["session_id"] = "20260825_071849_ebe2e02a"
    with pytest.raises(PermissionError):
        adapter.authorize_workflow_capability(claims)
    adapter._phone = None


def test_kanban_workflow_requires_current_turn_to_name_exact_board(
    adapter, plugin_package
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    turn = module._Turn(
        turn_id="kanban-turn",
        event_id="g2-turn-19-kanban-turn",
        session_key="agent:main:g2:dm:glasses-context-v19",
        generation=19,
        user_text="Add the release follow-up to the Hermes G2 board",
    )
    adapter._phone = SimpleNamespace()
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    adapter._session_store = SimpleNamespace(
        peek_session_id=lambda _key: "20260825_111141_bb5b0b83"
    )
    claims = {
        "platform": "g2",
        "profile": "even-g2",
        "chat_id": adapter.session_chat_id,
        "message_id": turn.event_id,
        "session_id": "20260825_111141_bb5b0b83",
    }

    authorized = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "hermes-g2"},
    )
    assert authorized.turn_id == turn.turn_id

    wrong_board = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "Default"},
    )
    assert wrong_board.error_code == "kanban_board_not_named"

    turn.user_text = "Add this to my onboard task board"
    onboard = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "Hermes G2"},
    )
    assert onboard.error_code == "work_tasks_requested"
    work_tasks = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_work_task_add",
        workflow_arguments={"title": "Release follow-up", "lane": "inbox"},
    )
    assert work_tasks.turn_id == turn.turn_id

    for phrase in (
        "Add this to Work Tasks",
        "Add this to my onboard tasks",
        "Add this to the on-device task board",
        "Add this to my local tasks",
        "Add this to the local task board",
        "Add this to my phone tasks",
        "Add this to the phone task board",
    ):
        turn.user_text = phrase
        denial = adapter.authorize_workflow_capability(
            claims,
            workflow="g2_kanban_task_create",
            workflow_arguments={"title": "Release follow-up", "board": "Hermes G2"},
        )
        assert denial.error_code == "work_tasks_requested"

    turn.user_text = "Add Hermes G2 follow-up to my local tasks"
    title_contains_board = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Hermes G2 follow-up", "board": "Hermes G2"},
    )
    assert title_contains_board.error_code == "work_tasks_requested"

    turn.user_text = "Add this to Kanban"
    guessed_kanban = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "Hermes G2"},
    )
    assert guessed_kanban.error_code == "kanban_board_not_named"
    wrong_store = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_work_task_add",
        workflow_arguments={"title": "Release follow-up", "lane": "inbox"},
    )
    assert wrong_store.error_code == "work_tasks_not_authorized"

    turn.user_text = "Add this to Work Tasks and the Hermes G2 Kanban board"
    conflict = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "Hermes G2"},
    )
    assert conflict.error_code == "task_board_target_conflict"
    conflict = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_work_task_add",
        workflow_arguments={"title": "Release follow-up", "lane": "inbox"},
    )
    assert conflict.error_code == "task_board_target_conflict"

    turn.user_text = "Yes"
    prior_context_guess = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Release follow-up", "board": "Hermes G2"},
    )
    assert prior_context_guess.error_code == "kanban_board_not_named"

    turn.user_text = "Other board task, follow up on Example Customer request"
    incident_kanban = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_kanban_task_create",
        workflow_arguments={"title": "Follow-up", "board": "Blocker Board"},
    )
    assert incident_kanban.error_code == "kanban_board_not_named"
    incident_work_tasks = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_work_task_add",
        workflow_arguments={"title": "Follow-up", "lane": "inbox"},
    )
    assert incident_work_tasks.turn_id == turn.turn_id

    turn.user_text = "Add a task to follow up on the release"
    unqualified_task = adapter.authorize_workflow_capability(
        claims,
        workflow="g2_work_task_add",
        workflow_arguments={"title": "Release follow-up", "lane": "inbox"},
    )
    assert unqualified_task.turn_id == turn.turn_id
    adapter._phone = None


@pytest_asyncio.fixture
async def adapter(plugin_package, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_G2_TOKEN", "secret")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    from gateway.platform_registry import PlatformEntry, platform_registry

    platform_registry.register(
        PlatformEntry(
            name="g2",
            label="G2 test",
            adapter_factory=lambda cfg: module.G2Adapter(cfg),
            check_fn=lambda: True,
        )
    )
    instance = module.G2Adapter(
        PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "bind": "127.0.0.1",
                "port": 0,
                "session_key": "test-glasses",
                "hello_profile": "even-g2",
                "tool_call_allowlist": ["glasses.test", "glasses.show_alert"],
            },
        )
    )
    yield instance
    await instance.disconnect()


@pytest.mark.asyncio
async def test_websocket_requires_token_and_new_phone_supersedes_old(adapter):
    assert await adapter.connect()
    uri = f"ws://127.0.0.1:{adapter.bound_port}"

    async with websockets.connect(uri) as unsupported:
        await unsupported.send(
            json.dumps({"v": 2, "chan": "ctl", "type": "hello", "token": "secret"})
        )
        error = await _recv(unsupported)
        assert error["type"] == "error" and error["message"] == "unsupported protocol"
        await unsupported.wait_closed()
        assert unsupported.close_code == 1002

    async with websockets.connect(uri) as rejected:
        await _send(rejected, chan="ctl", type="hello", token="wrong")
        error = await _recv(rejected)
        assert error["type"] == "error" and error["message"] == "invalid token"
        await rejected.wait_closed()
        assert rejected.close_code == 1008

    first = await websockets.connect(uri)
    ack = await _authenticate_and_serve_mcp(first)
    assert ack["sessionKey"] == "test-glasses"
    assert ack["profile"] == "even-g2"

    second = await websockets.connect(uri)
    await _authenticate_and_serve_mcp(second, device="replacement")
    while True:
        superseded = await _recv(first)
        if superseded.get("chan") == "ctl" and superseded.get("type") == "error":
            break
        # The adapter may already have started MCP initialization on the old
        # authenticated phone before the replacement arrives.
        assert superseded.get("chan") == "mcp"
    assert superseded["type"] == "error"
    assert "superseded" in superseded["message"]
    await first.wait_closed()
    await second.close()


@pytest.mark.asyncio
async def test_host_mcp_channel_requires_bounded_client_capability_negotiation(adapter):
    assert await adapter.connect()
    phone = await websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}")
    try:
        await _send(
            phone,
            chan="ctl",
            type="hello",
            token="secret",
            deviceName="legacy-phone",
            capabilities=["chat", "cockpit-v1"],
        )
        error = await _recv(phone)
        assert error["chan"] == "ctl" and error["type"] == "error"
        assert error["message"] == "host-mcp-v1 required"
        await phone.wait_closed()
        assert phone.close_code == 1002
        assert adapter._phone is None
    finally:
        await phone.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("send_initialize", [False, True])
async def test_negotiated_host_mcp_must_finish_initialize_deadline(
    adapter, send_initialize
):
    adapter.host_mcp_initialize_timeout = 0.05
    assert await adapter.connect()
    phone = await websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}")
    try:
        await _authenticate_and_serve_mcp(phone, capabilities=["host-mcp-v1"])
        if send_initialize:
            await _send(
                phone,
                chan="host-mcp",
                msg={
                    "jsonrpc": "2.0",
                    "id": "partial-init",
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "partial", "version": "1"},
                    },
                },
            )
        error = None
        while error is None:
            try:
                frame = await _recv(phone)
            except Exception:
                break
            if frame.get("chan") == "ctl" and frame.get("type") == "error":
                error = frame
        assert error is not None
        assert "initialization timed out" in error["message"]
        await phone.wait_closed()
    finally:
        await phone.close()


@pytest.mark.asyncio
async def test_custom_chat_cockpit_and_companion_channels_are_always_inert(adapter):
    received = []

    async def handler(event):
        received.append(event)
        return "must not run"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone, capabilities=["chat", "host-mcp-v1"])
        await _serve_device_mcp_ready(phone)
        await _initialize_host_mcp(phone)
        await _send(
            phone,
            chan="chat",
            type="utterance",
            turnId="legacy-on-negotiated",
            text="this channel has no authority",
        )
        await _send(
            phone,
            chan="chat",
            type="cancel",
            turnId="legacy-on-negotiated",
        )
        await _send(phone, chan="cockpit", type="steer", command_id="must-not-run")
        await _send(
            phone,
            chan="companion",
            type="operation",
            operation_id="must-not-run",
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(phone.recv(), timeout=0.05)
    assert received == []
    assert adapter._active_turn_id is None


def test_client_capability_parser_is_bounded_and_fail_closed(plugin_package):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    assert module._bounded_client_capabilities(["host-mcp-v1", "chat"]) == {
        "host-mcp-v1",
        "chat",
    }
    assert module._bounded_client_capabilities(["host-mcp-v1", 7]) == frozenset()
    assert module._bounded_client_capabilities(["host-mcp-v1"] * 33) == frozenset()
    assert module._bounded_client_capabilities(["x" * 65]) == frozenset()
    assert module._bounded_client_capabilities(["høst-mcp-v1"]) == frozenset()


@pytest.mark.asyncio
async def test_cockpit_dispatch_reaches_exact_live_hermes_authority(
    adapter, plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    binding = host_module.HostTurnBinding(
        request_id="voice-request",
        request_key="s:voice-request",
        session_generation="host_connection_test",
        call_generation=1,
        turn_id="turn-live",
    )
    turn = module._Turn(
        turn_id="turn-live",
        event_id="event-live",
        session_key="g2:profile:test-glasses",
        generation=3,
        host_binding=binding,
    )

    class LiveAgent:
        def __init__(self):
            self.steers = []
            self.interrupts = 0

        def steer(self, text):
            self.steers.append(text)
            return True

        def hard_interrupt(self):
            self.interrupts += 1

    agent = LiveAgent()
    adapter._phone = SimpleNamespace()
    adapter._turns[turn.turn_id] = turn
    adapter._active_turn_id = turn.turn_id
    adapter.gateway_runner = SimpleNamespace(
        _running_agents={turn.session_key: agent}
    )
    clarify_resolutions = []
    approval_resolutions = []
    from tools import approval, clarify_gateway

    monkeypatch.setattr(
        clarify_gateway,
        "resolve_gateway_clarify",
        lambda clarify_id, choice: clarify_resolutions.append((clarify_id, choice)) or True,
    )

    def resolve_approval(
        session_key, choice, *, resolve_all=False, request_id=None, **_kwargs
    ):
        approval_resolutions.append((session_key, choice, resolve_all, request_id))
        return 1

    monkeypatch.setattr(approval, "resolve_gateway_approval", resolve_approval)
    base = {"generation": 3}
    try:
        assert await adapter._dispatch_cockpit_command(
            {**base, "type": "answer", "choice_id": "choice_live_12345"},
            {
                "kind": "question",
                "session_key": turn.session_key,
                "clarify_id": "clarify-live",
                "choice_values": {"choice_live_12345": "Staging"},
            },
        ) == ("accepted", None)
        assert clarify_resolutions == [("clarify-live", "Staging")]

        assert await adapter._dispatch_cockpit_command(
            {**base, "type": "permission_decide", "decision": "allow_once"},
            {
                "kind": "permission",
                "session_key": turn.session_key,
                "approval_request_id": "approval-backend-1",
            },
        ) == ("accepted", None)
        assert approval_resolutions == [
            (turn.session_key, "once", False, "approval-backend-1")
        ]

        assert await adapter._dispatch_cockpit_command(
            {**base, "type": "steer", "text": "Run focused tests"}, None
        ) == ("accepted", None)
        assert agent.steers == ["Run focused tests"]

        assert await adapter._dispatch_cockpit_command(
            {**base, "type": "interrupt"}, None
        ) == ("accepted", None)
        assert agent.interrupts == 1

        assert await adapter._dispatch_cockpit_command(
            {"generation": 2, "type": "interrupt"}, None
        ) == ("rejected", "active_g2_turn_stale")
        assert agent.interrupts == 1
    finally:
        adapter._phone = None
        adapter._turns.clear()
        adapter._active_turn_id = None


@pytest.mark.asyncio
async def test_cockpit_approval_binds_unique_redacted_native_request(
    adapter, plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    binding = host_module.HostTurnBinding(
        request_id="voice-approval",
        request_key="s:voice-approval",
        session_generation="host_connection_test",
        call_generation=1,
        turn_id="turn-approval",
    )
    turn = module._Turn(
        turn_id="turn-approval",
        event_id="event-approval",
        session_key="g2:profile:test-glasses",
        generation=9,
        host_binding=binding,
    )

    class HostProjection:
        def __init__(self):
            self.projected = set()
            self.opens = []

        def has_cockpit_approval_backend(self, request_id):
            return request_id in self.projected

        async def open_cockpit_permission(self, _binding, **kwargs):
            self.projected.add(kwargs["approval_request_id"])
            self.opens.append(kwargs)
            return True

    host = HostProjection()
    adapter._phone = SimpleNamespace(host_mcp=host)
    adapter._turns[turn.turn_id] = turn
    adapter._active_turn_id = turn.turn_id
    from gateway import run as gateway_run
    from tools import approval

    def display_redact(value):
        return str(value).replace("secret-alpha", "[redacted]").replace(
            "secret-beta", "[redacted]"
        )

    monkeypatch.setattr(gateway_run, "_redact_approval_command", display_redact)
    common = {
        "description": "Send one request",
        "allow_permanent": True,
        "allow_session": True,
    }
    pending = [
        {**common, "request_id": "approval-unrelated", "command": "echo unrelated"},
        {
            **common,
            "request_id": "approval-intended",
            "command": "curl token=secret-alpha",
        },
    ]
    monkeypatch.setattr(approval, "list_gateway_approvals", lambda _key: pending)
    try:
        result = await adapter.send_exec_approval(
            "test-glasses",
            "curl token=[redacted]",
            turn.session_key,
            description="Send one request",
        )
        assert result.success
        assert [item["approval_request_id"] for item in host.opens] == [
            "approval-intended"
        ]

        host.opens.clear()
        pending[:] = [
            {
                **common,
                "request_id": "approval-ambiguous-a",
                "command": "curl token=secret-alpha",
            },
            {
                **common,
                "request_id": "approval-ambiguous-b",
                "command": "curl token=secret-beta",
            },
        ]
        ambiguous = await adapter.send_exec_approval(
            "test-glasses",
            "curl token=[redacted]",
            turn.session_key,
            description="Send one request",
        )
        assert not ambiguous.success
        assert ambiguous.error == "Approval request identity is ambiguous"
        assert host.opens == []
    finally:
        adapter._phone = None
        adapter._turns.clear()
        adapter._active_turn_id = None


@pytest.mark.asyncio
async def test_start_failure_discards_projection_and_cleans_adapter_state(
    adapter, plugin_package, monkeypatch
):
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    binding = host_module.HostTurnBinding(
        request_id="voice-start-failure",
        request_key="s:voice-start-failure",
        session_generation="host_connection_test",
        call_generation=1,
        turn_id="turn-start-failure",
    )

    class HostProjection:
        def __init__(self):
            self.discarded = []

        async def open_cockpit_turn(self, *_args, **_kwargs):
            return True

        async def discard_cockpit_turn(self, exact_binding):
            self.discarded.append(exact_binding)
            return True

    host = HostProjection()
    phone = SimpleNamespace(device_name="test-phone", host_mcp=host)
    adapter._phone = phone

    async def fail_after_projection(_event):
        raise RuntimeError("deterministic handler failure")

    monkeypatch.setattr(adapter, "handle_message", fail_after_projection)
    try:
        with pytest.raises(RuntimeError, match="deterministic handler failure"):
            await adapter._start_turn(
                expected_phone=phone,
                turn_id="turn-start-failure",
                text="must clean up",
                context=None,
                raw_message={"chan": "host-mcp"},
                host_binding=binding,
            )
        assert host.discarded == [binding]
        assert adapter._active_turn_id is None
        assert adapter._turns == {}
        assert adapter._event_to_turn == {}
    finally:
        adapter._phone = None


@pytest.mark.asyncio
async def test_finish_projection_failure_still_completes_authoritative_voice_call(
    adapter, plugin_package
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    binding = host_module.HostTurnBinding(
        request_id="voice-finish-failure",
        request_key="s:voice-finish-failure",
        session_generation="host_connection_test",
        call_generation=1,
        turn_id="turn-finish-failure",
    )

    class HostProjection:
        def __init__(self):
            self.completed = []

        async def finish_cockpit_turn(self, *_args, **_kwargs):
            raise ConnectionError("deterministic projection failure")

        async def complete_turn(self, exact_binding, **kwargs):
            self.completed.append((exact_binding, kwargs))
            return True

    host = HostProjection()
    phone = SimpleNamespace(host_mcp=host)
    adapter._phone = phone
    turn = module._Turn(
        turn_id="turn-finish-failure",
        event_id="event-finish-failure",
        session_key="g2:profile:test-glasses",
        generation=10,
        text="Authoritative final",
        host_binding=binding,
        owner_phone=phone,
        owner_host_mcp=host,
    )
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    try:
        await adapter._finish_turn(turn.turn_id, "end_turn")
        assert host.completed == [
            (
                binding,
                {
                    "text": "Authoritative final",
                    "stop_reason": "end_turn",
                    "error": None,
                },
            )
        ]
        assert adapter._active_turn_id is None
        assert adapter._turns == {}
    finally:
        adapter._phone = None


@pytest.mark.asyncio
async def test_start_turn_supersession_after_projection_cannot_dispatch_or_delete_reused_id(
    adapter, plugin_package, monkeypatch
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    opened = asyncio.Event()
    release = asyncio.Event()
    dispatched = []
    cancelled = []

    binding_a = host_module.HostTurnBinding(
        request_id="voice-start-a",
        request_key="s:voice-start-a",
        session_generation="host_connection_a",
        call_generation=1,
        turn_id="reused-turn",
    )
    binding_b = host_module.HostTurnBinding(
        request_id="voice-start-b",
        request_key="s:voice-start-b",
        session_generation="host_connection_b",
        call_generation=1,
        turn_id="reused-turn",
    )

    class HostA:
        def __init__(self):
            self.discarded = []

        async def open_cockpit_turn(self, *_args, **_kwargs):
            opened.set()
            await release.wait()
            return True

        async def discard_cockpit_turn(self, binding):
            self.discarded.append(binding)
            return True

    class HostB:
        pass

    host_a = HostA()
    host_b = HostB()
    phone_a = SimpleNamespace(device_name="phone-a", host_mcp=host_a)
    phone_b = SimpleNamespace(device_name="phone-b", host_mcp=host_b)
    adapter._phone = phone_a

    async def capture_dispatch(event):
        dispatched.append(event)

    async def capture_cancel(session_key):
        cancelled.append(session_key)

    monkeypatch.setattr(adapter, "handle_message", capture_dispatch)
    monkeypatch.setattr(adapter, "cancel_session_processing", capture_cancel)
    start = asyncio.create_task(
        adapter._start_turn(
            expected_phone=phone_a,
            turn_id="reused-turn",
            text="old request",
            context=None,
            raw_message={"chan": "host-mcp"},
            host_binding=binding_a,
        )
    )
    try:
        await asyncio.wait_for(opened.wait(), timeout=1)
        turn_a = adapter._turns["reused-turn"]
        turn_b = module._Turn(
            turn_id="reused-turn",
            event_id="event-b",
            session_key="agent:even-g2:g2:dm:test-glasses",
            generation=999,
            host_binding=binding_b,
            owner_phone=phone_b,
            owner_host_mcp=host_b,
        )
        adapter._phone = phone_b
        adapter._turns[turn_b.turn_id] = turn_b
        adapter._event_to_turn[turn_b.event_id] = (turn_b.turn_id, turn_b.generation)
        adapter._active_turn_id = turn_b.turn_id
        release.set()

        with pytest.raises(ConnectionError, match="changed during Cockpit open"):
            await start
        assert adapter._turns[turn_b.turn_id] is turn_b
        assert adapter._active_turn_id == turn_b.turn_id
        assert adapter._event_to_turn[turn_b.event_id] == (
            turn_b.turn_id,
            turn_b.generation,
        )
        assert turn_a is not turn_b
        assert dispatched == []
        assert cancelled == []
        assert host_a.discarded == []
    finally:
        release.set()
        if not start.done():
            start.cancel()
            with pytest.raises(asyncio.CancelledError):
                await start
        adapter._phone = None
        adapter._turns.clear()
        adapter._event_to_turn.clear()
        adapter._active_turn_id = None


@pytest.mark.asyncio
async def test_finish_turn_supersession_cannot_complete_or_delete_reused_id(
    adapter, plugin_package
):
    module = importlib.import_module(f"{plugin_package.__name__}.adapter")
    host_module = importlib.import_module(f"{plugin_package.__name__}.host_mcp")
    projecting = asyncio.Event()
    release = asyncio.Event()

    binding_a = host_module.HostTurnBinding(
        request_id="voice-finish-a",
        request_key="s:voice-finish-a",
        session_generation="host_connection_a",
        call_generation=1,
        turn_id="reused-turn",
    )
    binding_b = host_module.HostTurnBinding(
        request_id="voice-finish-b",
        request_key="s:voice-finish-b",
        session_generation="host_connection_b",
        call_generation=1,
        turn_id="reused-turn",
    )

    class HostA:
        def __init__(self):
            self.finished = []
            self.completed = []

        async def finish_cockpit_turn(self, binding, **kwargs):
            self.finished.append((binding, kwargs))
            projecting.set()
            await release.wait()
            return True

        async def complete_turn(self, binding, **kwargs):
            self.completed.append((binding, kwargs))
            return True

    class HostB:
        def __init__(self):
            self.calls = []

        async def finish_cockpit_turn(self, binding, **kwargs):
            self.calls.append(("finish", binding, kwargs))
            return True

        async def complete_turn(self, binding, **kwargs):
            self.calls.append(("complete", binding, kwargs))
            return True

    host_a = HostA()
    host_b = HostB()
    phone_a = SimpleNamespace(host_mcp=host_a)
    phone_b = SimpleNamespace(host_mcp=host_b)
    turn_a = module._Turn(
        turn_id="reused-turn",
        event_id="event-a",
        session_key="agent:even-g2:g2:dm:test-glasses",
        generation=20,
        text="old final",
        host_binding=binding_a,
        owner_phone=phone_a,
        owner_host_mcp=host_a,
    )
    adapter._phone = phone_a
    adapter._turns[turn_a.turn_id] = turn_a
    adapter._event_to_turn[turn_a.event_id] = (turn_a.turn_id, turn_a.generation)
    adapter._active_turn_id = turn_a.turn_id
    finish = asyncio.create_task(
        adapter._finish_turn(turn_a.turn_id, "end_turn", expected=turn_a)
    )
    try:
        await asyncio.wait_for(projecting.wait(), timeout=1)
        turn_b = module._Turn(
            turn_id="reused-turn",
            event_id="event-b",
            session_key="agent:even-g2:g2:dm:test-glasses",
            generation=21,
            text="new in-flight reply",
            host_binding=binding_b,
            owner_phone=phone_b,
            owner_host_mcp=host_b,
        )
        adapter._phone = phone_b
        adapter._turns[turn_b.turn_id] = turn_b
        adapter._event_to_turn[turn_b.event_id] = (turn_b.turn_id, turn_b.generation)
        adapter._active_turn_id = turn_b.turn_id
        release.set()
        await finish

        assert adapter._turns[turn_b.turn_id] is turn_b
        assert adapter._active_turn_id == turn_b.turn_id
        assert adapter._event_to_turn[turn_b.event_id] == (
            turn_b.turn_id,
            turn_b.generation,
        )
        assert not turn_b.finished
        assert host_a.finished == [
            (
                binding_a,
                {
                    "text": "old final",
                    "stop_reason": "end_turn",
                    "error": None,
                },
            )
        ]
        assert host_a.completed == []
        assert host_b.calls == []
    finally:
        release.set()
        if not finish.done():
            finish.cancel()
            with pytest.raises(asyncio.CancelledError):
                await finish
        adapter._phone = None
        adapter._turns.clear()
        adapter._event_to_turn.clear()
        adapter._active_turn_id = None


@pytest.mark.asyncio
async def test_workflow_relay_follows_gateway_adapter_lifecycle(adapter, plugin_package):
    relay_module = importlib.import_module(f"{plugin_package.__name__}.workflow_relay")
    socket_path = relay_module.relay_paths()
    assert not socket_path.exists()

    assert await adapter.connect()
    assert socket_path.is_socket()
    assert adapter._reminder_scheduler.running
    assert socket_path.stat().st_mode & 0o777 == 0o600

    await adapter.disconnect()
    assert not adapter._reminder_scheduler.running
    assert not socket_path.exists()


@pytest.mark.asyncio
async def test_workflow_relay_start_failure_closes_websocket_listener(adapter):
    class FailingRelay:
        def __init__(self):
            self.starts = 0
            self.stops = 0

        async def start(self):
            self.starts += 1
            raise RuntimeError("relay unavailable")

        async def stop(self):
            self.stops += 1

    relay = FailingRelay()
    adapter._workflow_relay = relay
    assert not await adapter.connect()
    assert relay.starts == 1
    assert relay.stops == 1
    assert adapter._server is None


@pytest.mark.asyncio
async def test_reminder_scheduler_start_failure_closes_all_private_services(adapter):
    class FailingScheduler:
        def __init__(self):
            self.starts = 0
            self.stops = 0

        async def start(self):
            self.starts += 1
            raise RuntimeError("outbox corrupt")

        async def stop(self):
            self.stops += 1

    scheduler = FailingScheduler()
    socket_path = adapter._workflow_relay.socket_path
    adapter._reminder_scheduler = scheduler
    assert not await adapter.connect()
    assert scheduler.starts == 1
    assert scheduler.stops == 1
    assert adapter._server is None
    assert not socket_path.exists()


@pytest.mark.asyncio
async def test_workflow_relay_failure_does_not_delete_unowned_path(adapter):
    socket_path = adapter._workflow_relay.socket_path
    socket_path.parent.mkdir(mode=0o700, parents=True)
    socket_path.write_text("not a socket")

    assert not await adapter.connect()
    assert socket_path.read_text() == "not a socket"
    assert adapter._server is None


@pytest.mark.asyncio
async def test_host_mcp_voice_turn_is_silent_until_authoritative_completion(adapter):
    received = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event):
        received.append(event)
        started.set()
        await release.wait()
        return "Final answer only"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(
            phone,
            capabilities=[
                "chat",
                "mcp",
                "cockpit-v1",
                "hermes-companion-v1",
                "host-mcp-v1",
            ],
        )
        await _serve_device_mcp_ready(phone)
        await _initialize_host_mcp(phone)

        await _send(
            phone,
            chan="host-mcp",
            msg={"jsonrpc": "2.0", "id": "host-resources", "method": "resources/list"},
        )
        resources = await _recv(phone)
        assert resources["chan"] == "host-mcp"
        assert resources["msg"]["result"]["resources"] == [
            {
                "uri": "hermes://session/status",
                "name": "hermes.session.status",
                "title": "Hermes Session Status",
                "description": (
                    "Authenticated, bounded connection status for the G2 host session. "
                    "It exposes no conversation history, transcript, device identity, "
                    "or session-control authority."
                ),
                "mimeType": "application/json",
            },
            {
                "uri": "hermes://cockpit/state",
                "name": "hermes.cockpit.state",
                "title": "Hermes Cockpit State",
                "description": (
                    "Authenticated bounded projection of explicitly shared G2 work: session state, "
                    "final timeline rows, and pending reviewed interactions."
                ),
                "mimeType": "application/json",
            },
        ]
        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "id": "host-status",
                "method": "resources/read",
                "params": {"uri": "hermes://session/status"},
            },
        )
        status_frame = await _recv(phone)
        status = json.loads(
            status_frame["msg"]["result"]["contents"][0]["text"]
        )
        assert status["profile"] == "even-g2"
        assert status["cockpit"] == {
            "state": "online",
            "transport": "mcp-resource",
            "projection": "session-snapshot",
            "sharedSessions": 0,
            "commandsAvailable": False,
        }
        assert status["companion"]["state"] == "unavailable"
        assert status["companion"]["commandsAvailable"] is False
        await _send(
            phone,
            chan="cockpit",
            type="steer",
            command_id="must-not-run",
        )
        await _send(
            phone,
            chan="companion",
            type="operation",
            operation_id="must-not-run",
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(phone.recv(), timeout=0.05)

        await _send(
            phone,
            chan="host-mcp",
            msg={"jsonrpc": "2.0", "id": "host-list", "method": "tools/list"},
        )
        listed = await _recv(phone)
        assert listed["chan"] == "host-mcp"
        assert [tool["name"] for tool in listed["msg"]["result"]["tools"]] == [
            "hermes.voice.turn",
            "hermes.cockpit.command",
        ]

        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "id": "host-call-1",
                "method": "tools/call",
                "params": {
                    "name": "hermes.voice.turn",
                    "arguments": {
                        "turnId": "host-turn-1",
                        "text": "What time is it?",
                        "context": {
                            "foregroundApp": "dashboard",
                            "screenOn": True,
                            "headsetBattery": 80,
                        },
                    },
                },
            },
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(phone.recv(), timeout=0.05)

        release.set()
        completed = await _recv(phone)

    assert completed["chan"] == "host-mcp"
    terminal = {
        "turnId": "host-turn-1",
        "text": "Final answer only",
        "stopReason": "end_turn",
    }
    assert completed["msg"] == {
        "jsonrpc": "2.0",
        "id": "host-call-1",
        "result": {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(terminal, separators=(",", ":")),
                }
            ],
            "structuredContent": {
                **terminal,
                "generation": 1,
            },
            "isError": False,
        },
    }
    assert len(received) == 1
    assert received[0].raw_message["chan"] == "host-mcp"
    assert received[0].raw_message["requestId"] == "host-call-1"
    assert received[0].metadata["g2_host_mcp_call_generation"] == 1
    assert received[0].source.profile == "even-g2"
    assert received[0].text.endswith("What time is it?")


@pytest.mark.asyncio
async def test_host_turn_rejects_profile_route_conflicting_with_hello_profile(
    adapter, monkeypatch
):
    phone = SimpleNamespace(device_name="test-phone")
    adapter._phone = phone
    original_build_source = adapter.build_source

    def build_conflicting_source(**kwargs):
        source = original_build_source(**kwargs)
        source.profile = "other-profile"
        return source

    monkeypatch.setattr(adapter, "build_source", build_conflicting_source)

    with pytest.raises(PermissionError, match="profile conflicts"):
        await adapter._start_turn(
            expected_phone=phone,
            turn_id="conflicting-profile-turn",
            text="must not run",
            context=None,
            raw_message={"chan": "host-mcp"},
            host_binding=SimpleNamespace(call_generation=1),
        )

    adapter._phone = None
    assert adapter._active_turn_id is None
    assert adapter._turns == {}


@pytest.mark.asyncio
async def test_host_mcp_cancel_notification_cancels_only_exact_bound_request(adapter):
    started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def handler(_event):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            handler_cancelled.set()

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(
            phone, capabilities=["chat", "mcp", "host-mcp-v1"]
        )
        await _serve_device_mcp_ready(phone)
        await _initialize_host_mcp(phone)
        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "id": 42,
                "method": "tools/call",
                "params": {
                    "name": "hermes.voice.turn",
                    "arguments": {"turnId": "host-cancel", "text": "wait"},
                },
            },
        )
        await asyncio.wait_for(started.wait(), timeout=1)

        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": "42", "reason": "wrong JSON-RPC type"},
            },
        )
        await asyncio.sleep(0)
        assert not handler_cancelled.is_set()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(phone.recv(), timeout=0.05)

        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": 42, "reason": "wearer dismissed"},
            },
        )
        completed = await _recv(phone)

    await asyncio.wait_for(handler_cancelled.wait(), timeout=1)
    assert completed["chan"] == "host-mcp"
    assert completed["msg"]["id"] == 42
    assert completed["msg"]["result"]["isError"] is True
    assert completed["msg"]["result"]["structuredContent"] == {
        "turnId": "host-cancel",
        "text": "Turn cancelled.",
        "stopReason": "cancelled",
        "generation": 1,
    }


@pytest.mark.asyncio
async def test_active_turn_mcp_call_carries_turn_id_and_does_not_deadlock_receiver(adapter):
    async def handler(event):
        from gateway.session_context import clear_session_vars, set_session_vars
        tokens = set_session_vars(platform="g2", chat_id=adapter.session_chat_id, message_id=event.message_id)
        try:
            result = await adapter.call_glasses_tool("glasses.test", {"value": "ok"})
            return result["content"][0]["text"]
        finally:
            clear_session_vars(tokens)

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone)
        while True:
            frame = await _recv(phone)
            if frame.get("chan") != "mcp":
                continue
            message = frame.get("msg") or {}
            if message.get("method") == "initialize":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": True}},
                               "serverInfo": {"name": "fake-phone", "version": "1"}},
                })
            elif message.get("method") == "tools/list":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"tools": [{"name": "glasses.test", "description": "test", "inputSchema": {"type": "object"}}]},
                })
                break

        await _initialize_host_mcp(phone)
        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "id": "host-tool-call",
                "method": "tools/call",
                "params": {
                    "name": "hermes.voice.turn",
                    "arguments": {
                        "turnId": "turn-tool",
                        "text": "test the glasses tool",
                    },
                },
            },
        )
        call = await _recv(phone)
        while call.get("chan") != "mcp" or (call.get("msg") or {}).get("method") != "tools/call":
            call = await _recv(phone)
        assert call["turnId"] == "turn-tool"
        request = call["msg"]
        await _send(phone, chan="mcp", msg={
            "jsonrpc": "2.0", "id": request["id"],
            "result": {"content": [{"type": "text", "text": "tool-ok"}]},
        })
        completed = await _recv(phone)

    assert completed["chan"] == "host-mcp"
    assert completed["msg"]["id"] == "host-tool-call"
    assert completed["msg"]["result"]["structuredContent"]["text"] == "tool-ok"


@pytest.mark.asyncio
async def test_allowlisted_proactive_mcp_call_is_explicitly_marked_for_the_phone(adapter):
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist = {"glasses.test"}
    adapter._active_turn_id = "unrelated-turn"
    adapter._turns["unrelated-turn"] = SimpleNamespace(
        turn_id="unrelated-turn", event_id="unrelated-event", generation=99, finished=False
    )
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone)
        while True:
            frame = await _recv(phone)
            if frame.get("chan") != "mcp":
                continue
            message = frame.get("msg") or {}
            if message.get("method") == "initialize":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": True}},
                               "serverInfo": {"name": "fake-phone", "version": "1"}},
                })
            elif message.get("method") == "tools/list":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"tools": [{"name": "glasses.test", "description": "test", "inputSchema": {"type": "object"}}]},
                })
                break

        task = asyncio.create_task(adapter.call_glasses_tool("glasses.test", {"value": "ok"}))
        call = await _recv(phone)
        assert call["chan"] == "mcp" and call["proactive"] is True
        assert "turnId" not in call
        request = call["msg"]
        await _send(phone, chan="mcp", msg={
            "jsonrpc": "2.0", "id": request["id"],
            "result": {"content": [{"type": "text", "text": "tool-ok"}]},
        })
        result = await task

    assert result["content"][0]["text"] == "tool-ok"


@pytest.mark.asyncio
async def test_proactive_authorization_is_rechecked_under_the_final_send_lock(adapter):
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist = {"glasses.test"}
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone)
        while True:
            frame = await _recv(phone)
            if frame.get("chan") != "mcp":
                continue
            message = frame.get("msg") or {}
            if message.get("method") == "initialize":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": True}},
                               "serverInfo": {"name": "fake-phone", "version": "1"}},
                })
            elif message.get("method") == "tools/list":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"tools": [{"name": "glasses.test", "description": "test", "inputSchema": {"type": "object"}}]},
                })
                break

        await adapter._phone.send_lock.acquire()
        try:
            task = asyncio.create_task(adapter.call_glasses_tool("glasses.test", {"value": "late"}))
            await asyncio.sleep(0.01)
            adapter.allow_proactive_tools = False
        finally:
            adapter._phone.send_lock.release()

        with pytest.raises(PermissionError, match="authorization is no longer current"):
            await task
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(phone.recv(), timeout=0.05)


@pytest.mark.asyncio
async def test_turn_authorization_is_rechecked_under_the_final_send_lock(adapter):
    finished = asyncio.Event()

    async def handler(event):
        from gateway.session_context import clear_session_vars, set_session_vars
        tokens = set_session_vars(platform="g2", chat_id=adapter.session_chat_id, message_id=event.message_id)
        try:
            await adapter.call_glasses_tool("glasses.test", {"value": "late"})
        except PermissionError:
            return "blocked"
        finally:
            clear_session_vars(tokens)
            finished.set()
        return "unexpected"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone)
        while True:
            frame = await _recv(phone)
            if frame.get("chan") != "mcp":
                continue
            message = frame.get("msg") or {}
            if message.get("method") == "initialize":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": True}},
                               "serverInfo": {"name": "fake-phone", "version": "1"}},
                })
            elif message.get("method") == "tools/list":
                await _send(phone, chan="mcp", msg={
                    "jsonrpc": "2.0", "id": message["id"],
                    "result": {"tools": [{"name": "glasses.test", "description": "test", "inputSchema": {"type": "object"}}]},
                })
                break

        await _initialize_host_mcp(phone)
        await adapter._phone.send_lock.acquire()
        try:
            await _send(
                phone,
                chan="host-mcp",
                msg={
                    "jsonrpc": "2.0",
                    "id": "host-stale-send",
                    "method": "tools/call",
                    "params": {
                        "name": "hermes.voice.turn",
                        "arguments": {
                            "turnId": "turn-stale",
                            "text": "test stale send",
                        },
                    },
                },
            )
            await asyncio.sleep(0.01)
            assert adapter._active_turn_id == "turn-stale"
            adapter._active_turn_id = None
        finally:
            adapter._phone.send_lock.release()

        await asyncio.wait_for(finished.wait(), timeout=1)
        completed = await _recv(phone)
        assert completed["chan"] == "host-mcp"
        assert completed["msg"]["id"] == "host-stale-send"
        assert completed["msg"]["result"]["structuredContent"]["text"] == "blocked"


@pytest.mark.asyncio
async def test_message_edits_remain_silent_until_one_host_mcp_final_result(adapter):
    async def handler(event):
        sent = await adapter.send(
            event.source.chat_id,
            "Hel",
            reply_to=event.message_id,
            metadata={"expect_edits": True},
        )
        await adapter.edit_message(
            event.source.chat_id,
            sent.message_id,
            "Hello",
            finalize=True,
        )
        return None

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    async with websockets.connect(f"ws://127.0.0.1:{adapter.bound_port}") as phone:
        await _authenticate_and_serve_mcp(phone)
        await _serve_device_mcp_ready(phone)
        await _initialize_host_mcp(phone)
        await _send(
            phone,
            chan="host-mcp",
            msg={
                "jsonrpc": "2.0",
                "id": "host-edit",
                "method": "tools/call",
                "params": {
                    "name": "hermes.voice.turn",
                    "arguments": {"turnId": "stream-id", "text": "stream"},
                },
            },
        )
        completed = await _recv(phone)
        assert completed["chan"] == "host-mcp"
        assert completed["msg"]["id"] == "host-edit"
        assert completed["msg"]["result"]["structuredContent"]["text"] == "Hello"


def test_adapter_does_not_advertise_delivery_after_turn_completion(adapter):
    assert adapter.supports_async_delivery is False


def test_bridge_bind_is_limited_to_loopback_or_tailscale(adapter):
    module = importlib.import_module(type(adapter).__module__)
    assert module._is_safe_bind_address("127.0.0.1")
    assert module._is_safe_bind_address("100.64.1.2")
    assert module._is_safe_bind_address("fd7a:115c:a1e0::1")
    assert not module._is_safe_bind_address("0.0.0.0")
    assert not module._is_safe_bind_address("192.168.2.10")
    assert not module._is_safe_bind_address("example.com")


def test_bridge_bind_private_lan_requires_explicit_opt_in(adapter):
    module = importlib.import_module(type(adapter).__module__)
    assert module._is_safe_bind_address("192.168.2.10", allow_private=True)
    assert module._is_safe_bind_address("10.0.0.42", allow_private=True)
    assert not module._is_safe_bind_address("0.0.0.0", allow_private=True)
    assert not module._is_safe_bind_address("::", allow_private=True)
    assert not module._is_safe_bind_address("8.8.8.8", allow_private=True)
    assert not module._is_safe_bind_address("example.com", allow_private=True)


def test_in_conversation_tool_gate_requires_active_g2_chat(adapter, monkeypatch):
    from gateway import session_context

    values = {
        "HERMES_SESSION_PLATFORM": "g2",
        "HERMES_SESSION_CHAT_ID": "different-chat",
        "HERMES_SESSION_MESSAGE_ID": "active-event",
    }
    adapter._active_turn_id = "active-turn"
    module = importlib.import_module(type(adapter).__module__)
    adapter._turns["active-turn"] = module._Turn(
        turn_id="active-turn",
        event_id="active-event",
        session_key="agent:main:g2:dm:glasses",
        generation=1,
    )
    adapter._event_to_turn["active-event"] = ("active-turn", 1)
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": values.get(name, default),
    )
    with pytest.raises(PermissionError):
        adapter.authorize_tool_call("glasses.show_alert")

    values["HERMES_SESSION_CHAT_ID"] = adapter.session_chat_id
    adapter.authorize_tool_call("glasses.show_alert")

    adapter._turns["active-turn"].finished = True
    with pytest.raises(PermissionError):
        adapter.authorize_tool_call("glasses.show_alert")


@pytest.mark.asyncio
async def test_scheduled_status_revalidates_turn_before_sending(adapter, monkeypatch):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="old",
        event_id="old-event",
        session_key="agent:main:g2:dm:glasses",
        generation=1,
    )
    adapter._turns["old"] = turn
    adapter._event_to_turn["old-event"] = ("old", 1)
    adapter._active_turn_id = "old"
    adapter.gateway_loop = asyncio.get_running_loop()
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_MESSAGE_ID": "old-event",
        }.get(name, default),
    )
    sent = []

    async def capture(frame):
        sent.append(frame)
        return True

    monkeypatch.setattr(adapter, "_send_frame", capture)
    adapter.set_status_text(adapter.session_chat_id, "working")
    turn.finished = True
    adapter._active_turn_id = None
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert sent == []


@pytest.mark.asyncio
async def test_tool_call_revalidates_turn_after_async_tool_lookup(adapter, monkeypatch):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="active",
        event_id="active-event",
        session_key="agent:main:g2:dm:glasses",
        generation=1,
    )
    adapter._turns["active"] = turn
    adapter._event_to_turn["active-event"] = ("active", 1)
    adapter._active_turn_id = "active"
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "g2",
            "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
            "HERMES_SESSION_MESSAGE_ID": "active-event",
        }.get(name, default),
    )
    calls = []

    class Mcp:
        async def list_tools(self):
            turn.finished = True
            return [{"name": "glasses.show_alert"}]

        async def call_tool(self, name, arguments, timeout):
            calls.append((name, arguments, timeout))
            return {}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(PermissionError):
        await adapter.call_glasses_tool("glasses.show_alert", {"text": "late"})
    assert calls == []


def _bind_fixed_device_test_turn(adapter, monkeypatch):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="device-turn",
        event_id="device-event",
        session_key="agent:main:g2:dm:test-glasses",
        generation=17,
    )
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "g2",
            "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
            "HERMES_SESSION_MESSAGE_ID": turn.event_id,
        }.get(name, default),
    )
    return turn


@pytest.mark.asyncio
async def test_contracted_phone_call_pins_identity_schema_and_active_turn(
    adapter, monkeypatch
):
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    _bind_fixed_device_test_turn(adapter, monkeypatch)
    name = "media.now_playing"
    adapter.tool_call_allowlist.add(name)
    calls = []

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            return [{
                "name": name,
                "description": "What is playing",
                "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[name],
            }]

        async def call_tool(self, tool_name, arguments, timeout):
            authorization = adapter._tool_authorization.get()
            assert authorization is not None and authorization.turn_id == "device-turn"
            calls.append((tool_name, arguments, timeout))
            return {"content": [{"type": "text", "text": "Nothing is currently playing."}], "isError": False}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    result = await adapter.call_contracted_glasses_tool(
        name,
        {},
        schema_fingerprint=contract.PHONE_SCHEMA_FINGERPRINTS[name],
    )
    assert result["isError"] is False
    assert calls == [(name, {}, adapter.tool_call_timeout)]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["identity", "schema", "duplicate"])
async def test_contracted_phone_call_fails_closed_before_call_on_drift(
    adapter, monkeypatch, drift
):
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    _bind_fixed_device_test_turn(adapter, monkeypatch)
    name = "calendar.list_events"
    adapter.tool_call_allowlist.add(name)
    called = False

    class Mcp:
        negotiated_identity = (
            "2025-06-18",
            "lookalike" if drift == "identity" else "hermes-g2",
            "1.0.0",
        )

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            schema = json.loads(json.dumps(contract.EXPECTED_PHONE_SCHEMAS[name]))
            if drift == "schema":
                schema["unreviewed"] = True
            entry = {"name": name, "description": "Calendar", "inputSchema": schema}
            return [entry, entry] if drift == "duplicate" else [entry]

        async def call_tool(self, *_args, **_kwargs):
            nonlocal called
            called = True
            return {}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(contract.DeviceContractError):
        await adapter.call_contracted_glasses_tool(
            name,
            {"within_hours": 24, "max_events": 5},
            schema_fingerprint=contract.PHONE_SCHEMA_FINGERPRINTS[name],
        )
    assert called is False


@pytest.mark.asyncio
async def test_contracted_phone_call_revalidates_turn_after_schema_lookup(
    adapter, monkeypatch
):
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    turn = _bind_fixed_device_test_turn(adapter, monkeypatch)
    name = "notifications.list"
    adapter.tool_call_allowlist.add(name)
    called = False

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            turn.finished = True
            return [{
                "name": name,
                "description": "Notifications",
                "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[name],
            }]

        async def call_tool(self, *_args, **_kwargs):
            nonlocal called
            called = True
            return {}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(PermissionError):
        await adapter.call_contracted_glasses_tool(
            name,
            {"max": 5},
            schema_fingerprint=contract.PHONE_SCHEMA_FINGERPRINTS[name],
        )
    assert called is False


@pytest.mark.asyncio
async def test_contracted_legacy_mutation_response_loss_is_unknown_and_not_retried(
    adapter, monkeypatch
):
    module = importlib.import_module(type(adapter).__module__)
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    _bind_fixed_device_test_turn(adapter, monkeypatch)
    name = "notifications.dismiss"
    adapter.tool_call_allowlist.add(name)
    calls = 0

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            return [{
                "name": name,
                "description": "Dismiss notification",
                "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[name],
            }]

        async def call_tool(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise ConnectionError("reply lost")

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(module.PhoneToolCallOutcomeUnknown) as error:
        await adapter.call_contracted_glasses_tool(
            name,
            {"key": "phone|7"},
            schema_fingerprint=contract.PHONE_SCHEMA_FINGERPRINTS[name],
        )
    assert error.value.commit_state == "unknown"
    assert calls == 1


def test_stale_g2_turn_cannot_downgrade_into_proactive_tool_call(adapter, monkeypatch):
    from gateway import session_context

    values = {
        "HERMES_SESSION_PLATFORM": "g2",
        "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
        "HERMES_SESSION_MESSAGE_ID": "stale-event",
    }
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": values.get(name, default),
    )
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist = {"glasses.show_alert"}
    with pytest.raises(PermissionError):
        adapter.authorize_tool_call("glasses.show_alert")


def test_notify_result_requires_global_and_exact_proactive_allowlists(adapter, monkeypatch):
    from gateway import session_context

    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "api_server",
            "HERMES_SESSION_CHAT_ID": "background-agent",
            "HERMES_SESSION_MESSAGE_ID": "background-result-1",
        }.get(name, default),
    )
    adapter.tool_call_allowlist.add("glasses.notify_result")

    with pytest.raises(PermissionError, match="Proactive glasses tool calls are disabled"):
        adapter.authorize_tool_call("glasses.notify_result")

    adapter.allow_proactive_tools = True
    with pytest.raises(PermissionError, match="proactive_tool_allowlist"):
        adapter.authorize_tool_call("glasses.notify_result")

    adapter.proactive_tool_allowlist.add("glasses.notify_result")
    authorization = adapter.authorize_tool_call("glasses.notify_result")
    assert authorization.proactive is True
    assert authorization.turn_id is None
    assert authorization.turn_generation is None

    adapter.tool_call_allowlist.remove("glasses.notify_result")
    with pytest.raises(PermissionError, match="tool_call_allowlist"):
        adapter.authorize_tool_call("glasses.notify_result")


@pytest.mark.asyncio
async def test_notify_result_response_loss_is_unknown_after_phone_call_begins(
    adapter, monkeypatch
):
    module = importlib.import_module(type(adapter).__module__)
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    adapter.tool_call_allowlist.add("glasses.notify_result")
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add("glasses.notify_result")

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            return [{
                "name": "glasses.notify_result",
                "description": "Durably queue one completed result",
                "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[
                    "glasses.notify_result"
                ],
            }]

        async def call_tool(self, name, arguments, timeout):
            assert name == "glasses.notify_result"
            assert arguments == {"operation_id": "rem.fixed-1", "text": "Reminder: X"}
            raise ConnectionError("phone response lost")

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(module.PhoneToolCallOutcomeUnknown, match="phone response lost") as error:
        await adapter._deliver_scheduled_reminder(
            "rem.fixed-1", "Reminder: X"
        )
    assert error.value.commit_state == "unknown"


@pytest.mark.asyncio
async def test_notify_result_final_send_lock_rejection_is_definitely_not_committed(
    adapter, monkeypatch
):
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    adapter.tool_call_allowlist.add("glasses.notify_result")
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add("glasses.notify_result")

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            return [{
                "name": "glasses.notify_result",
                "description": "Durably queue one completed result",
                "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[
                    "glasses.notify_result"
                ],
            }]

        async def call_tool(self, name, arguments, timeout):
            raise PermissionError("authorization is no longer current")

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(PermissionError, match="authorization is no longer current"):
        await adapter._deliver_scheduled_reminder(
            "rem.fixed-1", "Reminder: X"
        )


@pytest.mark.asyncio
async def test_scheduled_reminder_path_is_fixed_and_has_no_session_lookup(
    adapter, monkeypatch
):
    from gateway import session_context

    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    calls = []
    adapter.tool_call_allowlist.add("glasses.notify_result")
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add("glasses.notify_result")
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("background delivery consulted session context")
        ),
    )

    class Mcp:
        negotiated_identity = ("2025-06-18", "hermes-g2", "1.0.0")

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            return [
                {
                    "name": "glasses.notify_result",
                    "description": "Durably queue one completed result",
                    "inputSchema": contract.EXPECTED_PHONE_SCHEMAS[
                        "glasses.notify_result"
                    ],
                },
                {"name": "glasses.work_board.add_task"},
            ]

        async def call_tool(self, name, arguments, timeout):
            calls.append((name, arguments, timeout))
            return {"content": [], "isError": False}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    result = await adapter._deliver_scheduled_reminder(
        "rem.fixed-1", "Reminder: exact inert text."
    )
    assert result == {"content": [], "isError": False}
    assert calls == [
        (
            "glasses.notify_result",
            {
                "operation_id": "rem.fixed-1",
                "text": "Reminder: exact inert text.",
            },
            adapter.tool_call_timeout,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["identity", "schema", "duplicate"])
async def test_scheduled_reminder_fails_closed_on_phone_contract_drift(
    adapter, monkeypatch, drift
):
    contract = importlib.import_module(
        f"{type(adapter).__module__.rsplit('.', 1)[0]}.device_voice_contract"
    )
    name = "glasses.notify_result"
    adapter.tool_call_allowlist.add(name)
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add(name)
    called = False

    class Mcp:
        negotiated_identity = (
            "2025-06-18",
            "lookalike" if drift == "identity" else "hermes-g2",
            "1.0.0",
        )

        async def list_tools(self, *, force_refresh=False):
            assert force_refresh is True
            schema = json.loads(json.dumps(contract.EXPECTED_PHONE_SCHEMAS[name]))
            if drift == "schema":
                schema["unreviewed"] = True
            entry = {
                "name": name,
                "description": "Durably queue one completed result",
                "inputSchema": schema,
            }
            return [entry, entry] if drift == "duplicate" else [entry]

        async def call_tool(self, *_args, **_kwargs):
            nonlocal called
            called = True
            return {}

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(contract.DeviceContractError):
        await adapter._deliver_scheduled_reminder("rem.fixed-2", "Reminder: X")
    assert called is False


def test_work_task_phone_route_cannot_be_authorized_proactively(adapter, monkeypatch):
    from gateway import session_context

    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "api_server",
            "HERMES_SESSION_CHAT_ID": "background-agent",
            "HERMES_SESSION_MESSAGE_ID": "background-task-1",
        }.get(name, default),
    )
    tool_name = "glasses.work_board.add_task"
    adapter.tool_call_allowlist.add(tool_name)
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add(tool_name)

    with pytest.raises(PermissionError, match="authenticated active G2 turn"):
        adapter.authorize_tool_call(tool_name)


def test_work_task_phone_route_accepts_only_the_exact_current_g2_turn(adapter, monkeypatch):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="work-turn",
        event_id="work-event",
        session_key="agent:main:g2:dm:test-glasses",
        generation=4,
    )
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "g2",
            "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
            "HERMES_SESSION_MESSAGE_ID": turn.event_id,
        }.get(name, default),
    )
    tool_name = "glasses.work_board.add_task"
    adapter.tool_call_allowlist.add(tool_name)

    authorization = adapter.authorize_tool_call(tool_name)

    assert authorization.proactive is False
    assert authorization.turn_id == turn.turn_id
    assert authorization.turn_generation == turn.generation
    assert authorization.event_id == turn.event_id


@pytest.mark.parametrize(
    "tool_name",
    ["glasses.clock.set_timer", "glasses.clock.set_alarm"],
)
def test_clock_phone_routes_cannot_be_authorized_proactively(
    adapter, monkeypatch, tool_name
):
    from gateway import session_context

    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "api_server",
            "HERMES_SESSION_CHAT_ID": "background-agent",
            "HERMES_SESSION_MESSAGE_ID": "background-clock-1",
        }.get(name, default),
    )
    adapter.tool_call_allowlist.add(tool_name)
    adapter.allow_proactive_tools = True
    adapter.proactive_tool_allowlist.add(tool_name)

    with pytest.raises(PermissionError, match="authenticated active G2 turn"):
        adapter.authorize_tool_call(tool_name)


@pytest.mark.parametrize(
    "tool_name",
    ["glasses.clock.set_timer", "glasses.clock.set_alarm"],
)
def test_clock_phone_routes_accept_only_the_exact_current_g2_turn(
    adapter, monkeypatch, tool_name
):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="clock-turn",
        event_id="clock-event",
        session_key="agent:main:g2:dm:test-glasses",
        generation=7,
    )
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "g2",
            "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
            "HERMES_SESSION_MESSAGE_ID": turn.event_id,
        }.get(name, default),
    )
    adapter.tool_call_allowlist.add(tool_name)

    authorization = adapter.authorize_tool_call(tool_name)

    assert authorization.proactive is False
    assert authorization.turn_id == turn.turn_id
    assert authorization.turn_generation == turn.generation
    assert authorization.event_id == turn.event_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    [
        (
            "glasses.clock.set_timer",
            {"operation_id": "clock.turn-7", "duration_seconds": 600},
        ),
        (
            "glasses.clock.set_alarm",
            {"operation_id": "clock.turn-7", "local_time": "07:30"},
        ),
    ],
)
async def test_clock_phone_response_loss_is_unknown_after_call_begins(
    adapter, monkeypatch, tool_name, arguments
):
    from gateway import session_context

    module = importlib.import_module(type(adapter).__module__)
    turn = module._Turn(
        turn_id="clock-turn",
        event_id="clock-event",
        session_key="agent:main:g2:dm:test-glasses",
        generation=7,
    )
    adapter._turns[turn.turn_id] = turn
    adapter._event_to_turn[turn.event_id] = (turn.turn_id, turn.generation)
    adapter._active_turn_id = turn.turn_id
    monkeypatch.setattr(
        session_context,
        "get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_PLATFORM": "g2",
            "HERMES_SESSION_CHAT_ID": adapter.session_chat_id,
            "HERMES_SESSION_MESSAGE_ID": turn.event_id,
        }.get(name, default),
    )
    adapter.tool_call_allowlist.add(tool_name)

    class Mcp:
        async def list_tools(self):
            return [{"name": tool_name}]

        async def call_tool(self, name, payload, timeout):
            assert name == tool_name
            assert payload == arguments
            raise ConnectionError("phone response lost")

    async def ready():
        return Mcp()

    monkeypatch.setattr(adapter, "_ready_mcp", ready)
    with pytest.raises(module.PhoneToolCallOutcomeUnknown, match="phone response lost") as error:
        await adapter.call_glasses_tool(tool_name, arguments)
    assert error.value.commit_state == "unknown"


def test_empty_tool_call_allowlist_denies_every_phone_tool(adapter):
    adapter.tool_call_allowlist.clear()
    with pytest.raises(PermissionError, match="tool_call_allowlist"):
        adapter.authorize_tool_call("glasses.test")


def test_session_key_uses_gateway_resolved_profile(adapter):
    class Store:
        @staticmethod
        def _resolve_profile_for_key(_source):
            return "coder"

    adapter._session_store = Store()
    source = adapter.build_source(
        chat_id="glasses",
        chat_name="Hermes G2",
        chat_type="dm",
        user_id="phone",
        user_name="phone",
        message_id="profile-test",
    )
    assert adapter._session_key_for_source(source).startswith("agent:coder:g2:dm:")


@pytest.mark.asyncio
async def test_late_completion_cannot_finish_reused_client_turn_id(adapter):
    module = importlib.import_module(type(adapter).__module__)
    adapter._turns["t1"] = module._Turn(
        turn_id="t1",
        event_id="new-event",
        session_key="agent:main:g2:dm:glasses",
        generation=2,
        text="new reply",
    )
    adapter._event_to_turn["old-event"] = ("t1", 1)
    adapter._event_to_turn["new-event"] = ("t1", 2)
    adapter._active_turn_id = "t1"
    old_event = SimpleNamespace(
        message_id="old-event",
        metadata={"g2_turn_generation": 1},
    )

    await adapter.on_processing_complete(old_event, ProcessingOutcome.CANCELLED)

    assert adapter._turns["t1"].generation == 2
    assert adapter._turns["t1"].text == "new reply"
    assert adapter._active_turn_id == "t1"


def test_unmatched_old_reply_never_falls_back_to_new_active_turn(adapter):
    module = importlib.import_module(type(adapter).__module__)
    adapter._turns["new"] = module._Turn(
        turn_id="new",
        event_id="new-event",
        session_key="agent:main:g2:dm:glasses",
        generation=2,
    )
    adapter._event_to_turn["new-event"] = ("new", 2)
    adapter._active_turn_id = "new"
    assert adapter._resolve_turn("old-event", None) is None
