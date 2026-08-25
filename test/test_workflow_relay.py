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

    def authorize(claims):
        if any(claims.get(key) != value for key, value in SESSION.items()):
            raise PermissionError("stolen turn")
        authorized.append(dict(claims))

    return relay_module.WorkflowRelay(authorize), authorized, dispatched


@pytest.mark.asyncio
async def test_valid_capability_dispatches_one_exact_allowlisted_subcall(relay_parts):
    relay, authorized, dispatched = relay_parts
    response = await relay._dispatch(request())
    assert response["ok"] is True
    assert len(authorized) == 1
    assert dispatched == [("g2.work_tasks.add", request()["arguments"])]


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
    session_secret = "agent:main:g2:dm:private-relay-session-9264"
    chat_secret = "private-relay-chat-3187"
    exception_secret = "private-relay-exception-5701"
    binding_secret = "private-relay-binding-8436:workflows"
    outer_arguments = {"location": location_secret}
    private_session = {
        "platform": "g2",
        "profile": "even-g2",
        "chat_id": chat_secret,
        "session_id": session_secret,
        "message_id": "g2-turn-private-relay-7129",
        "tool_call_id": "tool-call-private-relay-4653",
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

    def authorize(_claims):
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
        relay_module._log_relay_rejection("dispatch-private-data")

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
    relay = relay_module.WorkflowRelay(lambda _claims: None)
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
    relay = relay_module.WorkflowRelay(lambda _claims: None)
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
    relay = relay_module.WorkflowRelay(lambda _claims: None)

    await relay.start()
    try:
        assert not legacy.exists()
        assert relay.socket_path.is_socket()
        assert stat.S_IMODE(relay.socket_path.stat().st_mode) == 0o600
    finally:
        await relay.stop()
    assert not relay.socket_path.exists()
