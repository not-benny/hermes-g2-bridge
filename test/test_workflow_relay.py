from __future__ import annotations

import copy
import asyncio
import importlib
import json
import logging
import stat
import math
import time

import pytest

from tools.mcp_capability import mint_mcp_capability


AUDIENCE = "com.hermes.mcp/portable/hermes-g2-workflows/workflows"
OUTER = "g2_work_task_add"
OUTER_ARGS = {"title": "Email Simon", "lane": "today"}
SESSION = {
    "platform": "g2",
    "profile": "even-g2",
    "chat_id": "test-glasses",
    "session_id": "agent:main:g2:dm:test-glasses",
    "message_id": "g2-turn-1-relay",
    "tool_call_id": "tool-call-relay-1",
}


def capability(*, audience=AUDIENCE, binding="hermes-g2-workflows:workflows", workflow=OUTER, arguments=OUTER_ARGS, session=SESSION, now=None):
    return mint_mcp_capability(
        audience=audience,
        binding=binding,
        package_digest="sha256:" + "a" * 64,
        workflow=workflow,
        arguments=arguments,
        session=session,
        now=int(time.time()) - 1 if now is None else now,
    )


def request(cap=None):
    return {
        "version": 1,
        "id": "1" * 32,
        "capability": cap or capability(),
        "workflow": OUTER,
        "workflow_arguments": copy.deepcopy(OUTER_ARGS),
        "tool": "g2.work_tasks.add",
        "arguments": {
            "operation_id": "task.0123456789abcdef0123456789abcdef",
            "title": "Email Simon",
            "lane": "today",
        },
        "subcall_id": 1,
        "attempt": 1,
    }


def kanban_request(
    board: str,
    *,
    tool_call_id: str,
    message_id: str = SESSION["message_id"],
) -> dict:
    outer_arguments = {"title": "Follow up", "board": board}
    session = {
        **SESSION,
        "message_id": message_id,
        "tool_call_id": tool_call_id,
    }
    cap = capability(
        workflow="g2_kanban_task_create",
        arguments=outer_arguments,
        session=session,
    )
    return {
        "version": 1,
        "id": tool_call_id[-1] * 32,
        "capability": cap,
        "workflow": "g2_kanban_task_create",
        "workflow_arguments": outer_arguments,
        "tool": "g2.kanban.task.create",
        "arguments": {
            "operation_id": "kanban." + tool_call_id[-1] * 32,
            **outer_arguments,
        },
        "subcall_id": 1,
        "attempt": 1,
    }


@pytest.fixture
def relay_parts(plugin_package, monkeypatch):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    dispatched = []

    async def dispatch(name, arguments):
        dispatched.append((name, copy.deepcopy(arguments)))
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    authorized = []

    def authorize(claims, **_request):
        if any(
            claims.get(key) != SESSION[key]
            for key in ("platform", "profile", "chat_id", "session_id", "message_id")
        ):
            raise PermissionError("stolen turn")
        authorized.append(dict(claims))

    return relay_module.WorkflowRelay(authorize), authorized, dispatched


@pytest.mark.asyncio
async def test_valid_capability_dispatches_one_exact_allowlisted_subcall(relay_parts):
    relay, authorized, dispatched = relay_parts
    response = await relay._dispatch(request())
    assert response["ok"] is True
    assert len(authorized) == 2
    assert authorized[0] == authorized[1]
    assert dispatched == [("g2.work_tasks.add", request()["arguments"])]


@pytest.mark.asyncio
async def test_one_wearer_turn_cannot_substitute_kanban_destination(
    relay_parts,
):
    relay, _authorized, dispatched = relay_parts
    first = await relay._dispatch(
        kanban_request("Blocker Board", tool_call_id="kanban-tool-call-1")
    )
    assert first["ok"] is True
    assert len(dispatched) == 1

    substituted = await relay._dispatch(
        kanban_request("Hermes G2", tool_call_id="kanban-tool-call-2")
    )
    assert substituted["ok"] is True
    substituted_result = json.loads(substituted["result"])
    assert substituted_result == {
        "commit_state": "not_committed",
        "error": (
            "Selecting a different task-board destination requires a fresh wearer "
            "request"
        ),
        "error_code": "task_destination_change_requires_fresh_turn",
        "success": False,
    }
    assert len(dispatched) == 1

    repeated_same_board = await relay._dispatch(
        kanban_request("Blocker Board", tool_call_id="kanban-tool-call-3")
    )
    assert repeated_same_board["ok"] is True
    assert len(dispatched) == 2


@pytest.mark.asyncio
async def test_fresh_authoritative_wearer_turn_can_select_a_new_kanban_board(
    plugin_package,
    monkeypatch,
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    active_message = {"value": SESSION["message_id"]}
    dispatched = []

    def authorize(claims, **_request):
        if claims.get("message_id") != active_message["value"]:
            raise PermissionError("stale turn")

    async def dispatch(name, arguments):
        dispatched.append((name, copy.deepcopy(arguments)))
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(authorize)

    first = await relay._dispatch(
        kanban_request("Blocker Board", tool_call_id="kanban-tool-call-4")
    )
    assert first["ok"] is True

    active_message["value"] = "g2-turn-2-relay"
    fresh = await relay._dispatch(
        kanban_request(
            "Hermes G2",
            tool_call_id="kanban-tool-call-5",
            message_id=active_message["value"],
        )
    )
    assert fresh["ok"] is True
    assert len(dispatched) == 2


@pytest.mark.asyncio
async def test_board_fence_consumes_capability_replay_before_typed_rejection(
    relay_parts,
):
    relay, _authorized, dispatched = relay_parts
    first = kanban_request("Blocker Board", tool_call_id="kanban-tool-call-6")
    substituted = kanban_request("Hermes G2", tool_call_id="kanban-tool-call-7")

    assert (await relay._dispatch(first))["ok"] is True
    rejected = await relay._dispatch(substituted)
    assert rejected["ok"] is True
    assert json.loads(rejected["result"])["error_code"] == (
        "task_destination_change_requires_fresh_turn"
    )

    replay = await relay._dispatch(substituted)
    assert replay["ok"] is False
    assert replay["error"] == "unauthorized"
    assert len(dispatched) == 1


@pytest.mark.asyncio
async def test_stale_turn_cannot_replace_fresh_turn_board_fence(
    plugin_package,
    monkeypatch,
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    old_message = SESSION["message_id"]
    new_message = "g2-turn-2-relay"
    active_message = {"value": old_message}
    old_claim_started = asyncio.Event()
    resume_old_claim = asyncio.Event()
    dispatched = []

    def authorize(claims, **_request):
        if claims.get("message_id") != active_message["value"]:
            raise PermissionError("stale turn")

    async def dispatch(name, arguments):
        dispatched.append((name, copy.deepcopy(arguments)))
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(authorize)
    original_claim_use = relay._claim_use

    async def delayed_claim_use(claims, **request_parts):
        if claims.get("message_id") == old_message:
            old_claim_started.set()
            await resume_old_claim.wait()
        await original_claim_use(claims, **request_parts)

    relay._claim_use = delayed_claim_use
    stale_task = asyncio.create_task(
        relay._dispatch(
            kanban_request("Old Board", tool_call_id="kanban-tool-call-8")
        )
    )
    await old_claim_started.wait()

    active_message["value"] = new_message
    fresh = await relay._dispatch(
        kanban_request(
            "Hermes G2",
            tool_call_id="kanban-tool-call-9",
            message_id=new_message,
        )
    )
    assert fresh["ok"] is True

    resume_old_claim.set()
    stale = await stale_task
    assert stale["ok"] is False
    assert stale["error"] == "unauthorized"

    substituted = await relay._dispatch(
        kanban_request(
            "Default",
            tool_call_id="kanban-tool-call-a",
            message_id=new_message,
        )
    )
    assert substituted["ok"] is True
    assert json.loads(substituted["result"])["error_code"] == (
        "task_destination_change_requires_fresh_turn"
    )
    assert [arguments["board"] for _name, arguments in dispatched] == [
        "Hermes G2"
    ]


@pytest.mark.asyncio
async def test_policy_denial_is_typed_and_consumes_capability_nonce(
    plugin_package,
    monkeypatch,
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    dispatched = []

    async def dispatch(name, arguments):
        dispatched.append((name, arguments))
        return '{"success":true}'

    def authorize(_claims, **_request):
        return relay_module.WorkflowPolicyDenial("work_tasks_requested")

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(authorize)
    denied_request = kanban_request(
        "Hermes G2", tool_call_id="kanban-tool-call-b"
    )

    denied = await relay._dispatch(denied_request)
    assert denied["ok"] is True
    assert json.loads(denied["result"]) == {
        "commit_state": "not_committed",
        "error": (
            "The wearer requested the onboard Work Tasks board, not Hermes Kanban"
        ),
        "error_code": "work_tasks_requested",
        "success": False,
    }
    replay = await relay._dispatch(denied_request)
    assert replay["ok"] is False
    assert replay["error"] == "unauthorized"
    assert dispatched == []


@pytest.mark.asyncio
async def test_missing_kanban_board_cannot_fall_back_to_work_tasks_in_same_turn(
    plugin_package,
    monkeypatch,
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    dispatched = []

    def authorize(claims, **_request):
        if any(
            claims.get(key) != SESSION[key]
            for key in (
                "platform",
                "profile",
                "chat_id",
                "session_id",
                "message_id",
            )
        ):
            raise PermissionError("stolen turn")

    async def dispatch(name, arguments):
        dispatched.append((name, copy.deepcopy(arguments)))
        if name == "g2.kanban.task.create":
            return json.dumps({
                "success": False,
                "commit_state": "not_committed",
                "error_code": "board_not_found",
                "error": "No active Hermes Kanban board exactly matches that name",
                "available_boards": [
                    {"slug": "default", "name": "Default"},
                    {"slug": "hermes-g2", "name": "Hermes G2"},
                ],
                "boards_truncated": False,
            })
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(authorize)
    kanban = await relay._dispatch(
        kanban_request("Blocker", tool_call_id="kanban-tool-call-c")
    )
    assert kanban["ok"] is True
    assert json.loads(kanban["result"])["error_code"] == "board_not_found"

    work_request = request(
        capability(
            session={**SESSION, "tool_call_id": "work-tool-call-d"},
        )
    )
    work_request["id"] = "d" * 32
    work_request["arguments"]["operation_id"] = (
        "task.dddddddddddddddddddddddddddddddd"
    )
    work = await relay._dispatch(work_request)
    assert work["ok"] is True
    assert json.loads(work["result"])["error_code"] == (
        "task_destination_change_requires_fresh_turn"
    )
    assert [name for name, _arguments in dispatched] == [
        "g2.kanban.task.create"
    ]


@pytest.mark.asyncio
async def test_work_tasks_turn_cannot_switch_to_kanban(relay_parts):
    relay, _authorized, dispatched = relay_parts
    assert (await relay._dispatch(request()))["ok"] is True

    kanban = await relay._dispatch(
        kanban_request("Hermes G2", tool_call_id="kanban-tool-call-e")
    )
    assert kanban["ok"] is True
    assert json.loads(kanban["result"])["error_code"] == (
        "task_destination_change_requires_fresh_turn"
    )
    assert [name for name, _arguments in dispatched] == ["g2.work_tasks.add"]


@pytest.mark.asyncio
async def test_policy_denied_kanban_does_not_bind_before_correct_work_tasks_call(
    plugin_package,
    monkeypatch,
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    dispatched = []

    def authorize(_claims, *, workflow, **_request):
        if workflow == "g2_kanban_task_create":
            return relay_module.WorkflowPolicyDenial("work_tasks_requested")
        return None

    async def dispatch(name, arguments):
        dispatched.append((name, copy.deepcopy(arguments)))
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(authorize)
    denied = await relay._dispatch(
        kanban_request("Hermes G2", tool_call_id="kanban-tool-call-f")
    )
    assert denied["ok"] is True
    assert json.loads(denied["result"])["error_code"] == "work_tasks_requested"

    work_request = request(
        capability(
            session={**SESSION, "tool_call_id": "work-tool-call-0"},
        )
    )
    work_request["id"] = "0" * 32
    work_request["arguments"]["operation_id"] = (
        "task.00000000000000000000000000000000"
    )
    work = await relay._dispatch(work_request)
    assert work["ok"] is True
    assert [name for name, _arguments in dispatched] == ["g2.work_tasks.add"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "signature",
        "audience",
        "binding",
        "workflow",
        "outer_arguments",
        "expired",
        "stolen_session",
        "internal_tool",
        "extra_field",
        "bool_subcall",
        "nonfinite",
    ],
)
async def test_wrong_stolen_expired_or_malformed_authority_is_rejected(
    relay_parts, mutation
):
    relay, _authorized, dispatched = relay_parts
    value = request()
    if mutation == "signature":
        value["capability"]["signature"] = "0" * 64
    elif mutation == "audience":
        value["capability"] = capability(audience=AUDIENCE + ".wrong")
    elif mutation == "binding":
        value["capability"] = capability(binding="stolen-package:workflows")
    elif mutation == "workflow":
        value["workflow"] = "g2_clock_set_timer"
    elif mutation == "outer_arguments":
        value["workflow_arguments"]["title"] = "Changed"
    elif mutation == "expired":
        value["capability"] = capability(now=int(time.time()) - 301)
    elif mutation == "stolen_session":
        stolen = {**SESSION, "session_id": "agent:main:g2:dm:someone-else"}
        value["capability"] = capability(session=stolen)
    elif mutation == "internal_tool":
        value["tool"] = "g2.clock.set_timer"
    elif mutation == "extra_field":
        value["extra"] = "no"
    elif mutation == "bool_subcall":
        value["subcall_id"] = True
    elif mutation == "nonfinite":
        value["workflow_arguments"]["value"] = math.nan

    response = await relay._dispatch(value)
    assert response["ok"] is False
    assert dispatched == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejection_stage",
    ["capability_verify", "binding", "active_turn", "replay", "dispatch"],
)
async def test_relay_rejection_stage_logs_never_expose_private_request_data(
    plugin_package, monkeypatch, caplog, rejection_stage
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    capability_module = importlib.import_module("tools.mcp_capability")

    location_secret = "Private Relay Location 4242"
    def private_canary(*parts):
        return "-".join(("private", *parts))

    session_secret = "agent:main:g2:dm:" + private_canary(
        "relay", "session", "9264"
    )
    chat_secret = private_canary("relay", "chat", "3187")
    exception_secret = private_canary("relay", "exception", "5701")
    binding_secret = private_canary("relay", "binding", "8436:workflows")
    outer_arguments = {"location": location_secret}
    private_session = {
        "platform": "g2",
        "profile": "even-g2",
        "chat_id": chat_secret,
        "session_id": session_secret,
        "message_id": "g2-turn-" + private_canary("relay", "7129"),
        "tool_call_id": "tool-call-" + private_canary("relay", "4653"),
    }
    selected_binding = (
        binding_secret
        if rejection_stage == "binding"
        else "hermes-g2-workflows:workflows"
    )
    private_capability = capability(
        binding=selected_binding,
        workflow="g2_weather_present",
        arguments=outer_arguments,
        session=private_session,
    )
    value = request(private_capability)
    value.update({
        "workflow": "g2_weather_present",
        "workflow_arguments": copy.deepcopy(outer_arguments),
        "tool": "g2.weather.read_forecast",
        "arguments": copy.deepcopy(outer_arguments),
    })

    def authorize(_claims, **_request):
        if rejection_stage == "active_turn":
            raise PermissionError(exception_secret)

    relay = relay_module.WorkflowRelay(authorize)

    if rejection_stage == "capability_verify":
        def reject_capability(*_args, **_kwargs):
            raise PermissionError(exception_secret)

        monkeypatch.setattr(
            capability_module,
            "verify_mcp_capability",
            reject_capability,
        )
    elif rejection_stage == "replay":
        async def reject_replay(*_args, **_kwargs):
            raise PermissionError(exception_secret)

        monkeypatch.setattr(relay, "_claim_use", reject_replay)
    elif rejection_stage == "dispatch":
        async def reject_dispatch(*_args, **_kwargs):
            raise RuntimeError(exception_secret)

        monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", reject_dispatch)

    caplog.set_level(logging.WARNING, logger=relay_module.__name__)
    response = await relay._dispatch(value)

    assert response == {
        "version": 1,
        "id": value["id"],
        "ok": False,
        "error": "unavailable" if rejection_stage == "dispatch" else "unauthorized",
    }
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == relay_module.__name__
        and record.getMessage().startswith("G2 workflow relay rejection stage=")
    ]
    assert messages == [
        f"G2 workflow relay rejection stage={rejection_stage}",
    ]
    rendered = "\n".join(messages)
    for secret in (
        location_secret,
        session_secret,
        chat_secret,
        exception_secret,
        binding_secret,
    ):
        assert secret not in rendered


def test_relay_rejection_logger_refuses_non_allowlisted_content(
    plugin_package, caplog
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    caplog.set_level(logging.WARNING, logger=relay_module.__name__)

    with pytest.raises(TypeError):
        relay_module._log_relay_rejection("dispatch-" + "-".join(("private", "data")))

    assert not [
        record
        for record in caplog.records
        if record.name == relay_module.__name__
        and record.getMessage().startswith("G2 workflow relay rejection stage=")
    ]


@pytest.mark.asyncio
async def test_exact_replay_is_rejected_but_identical_attempt_two_is_allowed(relay_parts):
    relay, _authorized, dispatched = relay_parts
    value = request()
    assert (await relay._dispatch(value))["ok"] is True
    assert (await relay._dispatch(copy.deepcopy(value)))["ok"] is False

    retry = copy.deepcopy(value)
    retry["attempt"] = 2
    assert (await relay._dispatch(retry))["ok"] is True
    assert len(dispatched) == 2
    assert dispatched[0] == dispatched[1]


@pytest.mark.asyncio
async def test_retry_cannot_change_internal_payload_or_exceed_attempt_budget(relay_parts):
    relay, _authorized, dispatched = relay_parts
    value = request()
    assert (await relay._dispatch(value))["ok"] is True
    changed = copy.deepcopy(value)
    changed["attempt"] = 2
    changed["arguments"]["title"] = "Changed"
    assert (await relay._dispatch(changed))["ok"] is False

    retry = copy.deepcopy(value)
    retry["attempt"] = 2
    assert (await relay._dispatch(retry))["ok"] is True
    third = copy.deepcopy(value)
    third["attempt"] = 3
    assert (await relay._dispatch(third))["ok"] is False
    assert len(dispatched) == 2


@pytest.mark.asyncio
async def test_multistep_workflow_must_follow_reviewed_sequence(plugin_package, monkeypatch):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def dispatch(name, arguments):
        calls.append(name)
        return '{"success":true}'

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(lambda _claims, **_request: None)
    outer_args = {"location": "Liverpool"}
    cap = capability(workflow="g2_weather_present", arguments=outer_args)
    value = request(cap)
    value.update({
        "workflow": "g2_weather_present",
        "workflow_arguments": outer_args,
        "tool": "g2.context.present",
    })
    assert (await relay._dispatch(value))["ok"] is False
    assert calls == []


@pytest.mark.asyncio
async def test_client_disconnect_cancels_exact_inflight_native_dispatch(
    plugin_package, monkeypatch, tmp_path
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    tool_module = importlib.import_module(f"{plugin_package.__name__}.tools")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def dispatch(_name, _arguments):
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(tool_module, "dispatch_mcp_workflow", dispatch)
    relay = relay_module.WorkflowRelay(lambda _claims, **_request: None)
    await relay.start()
    try:
        _reader, writer = await asyncio.open_unix_connection(str(relay.socket_path))
        writer.write(
            json.dumps(
                request(),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=1)
        writer.close()
        await writer.wait_closed()
        await asyncio.wait_for(cancelled.wait(), timeout=1)
    finally:
        await relay.stop()


@pytest.mark.asyncio
async def test_relay_socket_is_transport_only_and_legacy_token_is_removed(
    plugin_package, monkeypatch, tmp_path
):
    relay_module = importlib.import_module(
        f"{plugin_package.__name__}.workflow_relay"
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    legacy = run_dir / "g2-workflows.token"
    legacy.write_text("obsolete-readable-authority", encoding="utf-8")
    relay = relay_module.WorkflowRelay(lambda _claims, **_request: None)

    await relay.start()
    try:
        assert not legacy.exists()
        assert relay.socket_path.is_socket()
        assert stat.S_IMODE(relay.socket_path.stat().st_mode) == 0o600
    finally:
        await relay.stop()
    assert not relay.socket_path.exists()
