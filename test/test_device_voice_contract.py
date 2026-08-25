from __future__ import annotations

import copy
import importlib
import json
import subprocess
from pathlib import Path

import pytest


def mcp_text(text: str, *, error: bool = False) -> dict:
    return {"content": [{"type": "text", "text": text}], "isError": error}


def test_phone_identity_and_schema_fingerprints_are_literal_and_fail_closed(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    contract.validate_phone_identity("2025-06-18", "hermes-g2", "1.0.0")
    with pytest.raises(contract.DeviceContractError):
        contract.validate_phone_identity("2025-06-18", "lookalike", "1.0.0")

    assert set(contract.EXPECTED_PHONE_SCHEMAS) == set(contract.PHONE_SCHEMA_FINGERPRINTS)
    assert contract.PHONE_SCHEMA_FINGERPRINTS["glasses.context_dashboard.present"] == (
        "99ff3cd3d5b9f409f9ef54814c9a8eb60957985c6382666e4a059da6e26c653b"
    )
    for name, schema in contract.EXPECTED_PHONE_SCHEMAS.items():
        fingerprint = contract.PHONE_SCHEMA_FINGERPRINTS[name]
        assert len(fingerprint) == 64
        assert fingerprint == contract.schema_fingerprint(schema)
        advertised = {
            "name": name,
            "description": "Reviewed fixed phone contract",
            "inputSchema": schema,
        }
        contract.validate_phone_tool(advertised, name)
        drifted = json.loads(json.dumps(advertised))
        drifted["inputSchema"]["x-unreviewed"] = True
        with pytest.raises(contract.DeviceContractError, match="drifted"):
            contract.validate_phone_tool(drifted, name)


def test_pinned_schema_hashes_match_the_phone_registry_source(plugin_package):
    """Hash the exact data module imported by the phone tool registries."""
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    phone_root = Path(__file__).resolve().parents[2] / "hermes-faceclaw"
    schema_path = phone_root / "app/assistant/bridge-phone-contract-schemas.ts"
    assert schema_path.is_file(), "phone contract schema artifact is missing"
    script = r"""
      import { readFileSync } from "node:fs";
      import ts from "typescript";
      const source = readFileSync(process.argv[1], "utf8");
      const js = ts.transpileModule(source, {
        compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 },
      }).outputText;
      const url = "data:text/javascript;base64," + Buffer.from(js).toString("base64");
      const value = (await import(url)).BRIDGE_PINNED_PHONE_INPUT_SCHEMAS;
      process.stdout.write(JSON.stringify(value));
    """
    completed = subprocess.run(
        ["node", "--input-type=module", "-e", script, str(schema_path)],
        cwd=phone_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    phone_schemas = json.loads(completed.stdout)
    fixed_names = {
        "glasses.notify_result",
        "glasses.work_board.add_task",
        "glasses.clock.set_timer",
        "glasses.clock.set_alarm",
        "glasses.context_dashboard.present",
    }
    assert set(phone_schemas) == fixed_names
    for name in fixed_names:
        assert contract.schema_fingerprint(phone_schemas[name]) == (
            contract.PHONE_SCHEMA_FINGERPRINTS[name]
        )

    system_source = (phone_root / "app/assistant/system-tools.ts").read_text(
        encoding="utf-8"
    )
    clock_source = (phone_root / "app/assistant/clock-tools.ts").read_text(
        encoding="utf-8"
    )
    for name in {
        "glasses.notify_result",
        "glasses.work_board.add_task",
        "glasses.context_dashboard.present",
    }:
        assert f'BRIDGE_PINNED_PHONE_INPUT_SCHEMAS["{name}"]' in system_source
    for name in {"glasses.clock.set_timer", "glasses.clock.set_alarm"}:
        assert f'BRIDGE_PINNED_PHONE_INPUT_SCHEMAS["{name}"]' in clock_source


def test_window_folder_media_and_navigation_results_are_typed_and_bounded(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    windows = contract.windows_result(mcp_text(
        '- launcher:1 — "Launcher" (app: launcher) [foreground, pinned]\n'
        '- music:2 — "Music" (app: music)'
    ))
    assert windows == {
        "success": True,
        "state": "available",
        "windows": [
            {
                "window_id": "launcher:1",
                "title": "Launcher",
                "app_id": "launcher",
                "foreground": True,
                "pinned": True,
            },
            {
                "window_id": "music:2",
                "title": "Music",
                "app_id": "music",
                "foreground": False,
                "pinned": False,
            },
        ],
    }
    folders = contract.folders_result(mcp_text(
        "Games: blocks, freecell\nUngrouped: music, weather"
    ))
    assert folders["folders"] == [{"name": "Games", "app_ids": ["blocks", "freecell"]}]
    assert folders["ungrouped_app_ids"] == ["music", "weather"]
    reserved_name = contract.folders_result(mcp_text(
        "Ungrouped: blocks\nUngrouped: music"
    ))
    assert reserved_name["folders"] == [{"name": "Ungrouped", "app_ids": ["blocks"]}]
    assert reserved_name["ungrouped_app_ids"] == ["music"]
    assert contract.media_result(mcp_text("Playing: Track by Artist in Music."))["state"] == "playing"
    assert contract.navigation_result(mcp_text("Navigation is not active."))["state"] == "inactive"
    with pytest.raises(contract.DeviceResultError):
        contract.windows_result(mcp_text("unreviewed window format"))


def test_notifications_health_and_calendar_are_reduced_to_coarse_receipts(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    notifications = contract.notifications_result(mcp_text(
        "- [UniFi Protect] Doorbell: Person detected (key: protect|front-door|7)"
    ))
    assert notifications == {
        "success": True,
        "state": "available",
        "notifications": [{
            "key": "protect|front-door|7",
            "app": "UniFi Protect",
            "summary": "Doorbell: Person detected",
        }],
    }

    health_document = {
        "source": "hermes-g2-ring",
        "schemaVersion": 1,
        "generatedAtMs": 1_787_673_600_000,
        "retentionDays": 31,
        "range": {"startDate": "2026-08-24", "endDate": "2026-08-25"},
        "hourlyIncluded": False,
        "history": [{
            "dateKey": "2026-08-25",
            "updatedAtMs": 1_787_673_600_000,
            "restingHr": 58,
            "hrMin": 49,
            "hrMax": 142,
            "hrvAvg": 44,
            "spo2Avg": 97,
            "steps": 7_321,
            "sleepScore": 82,
            "sleepDurationMin": 431,
            "sleepDeepMin": 76,
            "sleepRemMin": 88,
            "bodyTempC": 36.6,
            "readinessScore": 79,
        }],
        "activityTotals": {
            "dateKey": "2026-08-25",
            "totalSteps": 7_321,
            "activeCalories": 421,
            "totalCalories": 2_104,
            "restingCalories": 1_683,
        },
    }
    health = contract.health_result(mcp_text(json.dumps(health_document)))
    assert health["latest"] == {
        "date": "2026-08-25",
        "steps": 7_321,
        "resting_hr_bpm": 58,
        "hrv_ms": 44,
        "spo2_percent": 97,
        "sleep_score": 82,
        "sleep_minutes": 431,
        "readiness_score": 79,
    }
    assert "history" not in health
    assert "hourly" not in health

    agenda = contract.calendar_result(mcp_text(
        "- Dentist — Tue Aug 25, 2:30 PM @ Town Centre"
    ))
    assert agenda == {
        "success": True,
        "state": "available",
        "events": ["Dentist — Tue Aug 25, 2:30 PM @ Town Centre"],
    }


def test_raw_mcp_error_and_control_text_never_cross_typed_boundary(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    with pytest.raises(contract.DeviceResultError, match="rejected"):
        contract.mutation_result(mcp_text("Denied", error=True))
    with pytest.raises(contract.DeviceResultError, match="controls"):
        contract.calendar_result(mcp_text("- unsafe\u202etext"))
    with pytest.raises(contract.DeviceResultError, match="limit"):
        contract.media_result(mcp_text("Playing: " + "x" * 30_000))


def test_context_presentation_requires_one_exact_frame_acknowledgement(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    operation_id = "weather." + "a" * 32
    dashboard_key = "weather-" + "b" * 32
    raw = {
        "status": "acknowledged",
        "dashboard_id": "ctx_" + "c" * 32,
        "presentation_generation": 1,
        "refresh_generation": 1,
        "revision": 1,
        "frame_id": 23,
    }
    projected = contract.context_present_result(
        mcp_text(json.dumps(raw)),
        expected_operation_id=operation_id,
        expected_dashboard_key=dashboard_key,
    )
    assert projected == {
        "success": True,
        "receipt": {
            **raw,
            "operation_id": operation_id,
            "dashboard_key": dashboard_key,
        },
    }

    historical = {**raw, "status": "historical_acknowledgement"}
    assert contract.context_present_result(
        mcp_text(json.dumps(historical)),
        expected_operation_id=operation_id,
        expected_dashboard_key=dashboard_key,
    )["receipt"]["status"] == "historical_acknowledgement"

    malformed = {
        "extra field": {**raw, "isError": False},
        "wrong revision": {**raw, "revision": 2},
        "wrong presentation generation": {**raw, "presentation_generation": 2},
        "wrong refresh generation": {**raw, "refresh_generation": 2},
        "zero frame": {**raw, "frame_id": 0},
        "boolean frame": {**raw, "frame_id": True},
        "wrong status": {**raw, "status": "queued"},
        "short dashboard id": {**raw, "dashboard_id": "short"},
    }
    missing = copy.deepcopy(raw)
    del missing["frame_id"]
    malformed["missing field"] = missing
    for label, value in malformed.items():
        with pytest.raises(contract.DeviceResultError, match="receipt") as failure:
            contract.context_present_result(
                mcp_text(json.dumps(value)),
                expected_operation_id=operation_id,
                expected_dashboard_key=dashboard_key,
            )
        assert failure.value is not None, label
    with pytest.raises(contract.DeviceResultError):
        contract.context_present_result(
            mcp_text('{"status":"acknowledged","status":"acknowledged"}'),
            expected_operation_id=operation_id,
            expected_dashboard_key=dashboard_key,
        )


def test_context_presentation_accepts_only_allowlisted_pre_delivery_blockers(plugin_package):
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    operation_id = "weather." + "a" * 32
    dashboard_key = "weather-" + "b" * 32
    cases = {
        "clock_alert_active": "The glasses display is busy with an active Clock alert.",
        "assistant_presentation_active": "The glasses display is busy with another assistant presentation.",
    }
    for error_code, error in cases.items():
        phone_failure = {"status": "rejected", "error_code": error_code, "error": error}
        assert contract.context_present_result(
            mcp_text(json.dumps(phone_failure), error=True),
            expected_operation_id=operation_id,
            expected_dashboard_key=dashboard_key,
        ) == {
            "success": False,
            "commit_state": "not_committed",
            "operation_id": operation_id,
            "error_code": error_code,
            "error": error,
        }

    for phone_failure in (
        {"status": "rejected", "error_code": "feed_unavailable", "error": "private"},
        {"status": "rejected", "error_code": ["clock_alert_active"], "error": "private"},
        {"status": "rejected", "error_code": "clock_alert_active", "error": "private"},
        {"status": "rejected", "error_code": "clock_alert_active", "error": cases["clock_alert_active"], "extra": True},
    ):
        with pytest.raises(contract.DeviceResultError, match="receipt"):
            contract.context_present_result(
                mcp_text(json.dumps(phone_failure), error=True),
                expected_operation_id=operation_id,
                expected_dashboard_key=dashboard_key,
            )


def context_present_arguments() -> dict:
    return {
        "operation_id": "weather." + "1" * 32,
        "intent": "Weather for Liverpool today",
        "refresh_policy": {"mode": "on_visible", "min_interval_seconds": 900},
        "regeneration": "self_contained_intent",
        "spec": {
            "version": 2,
            "presentation_mode": "deck",
            "dashboard_key": "weather-" + "2" * 32,
            "title": "Liverpool weather",
            "state": "ready",
            "privacy": "private",
            "summary": {"primary": "Rain · 10–17°C", "uncertainty": "estimated"},
            "sections": [{
                "id": "forecast",
                "order": 0,
                "type": "message",
                "load_state": "ready",
                "source_ids": ["weather"],
                "uncertainty": "estimated",
                "body": "Rain is likely",
            }],
            "sources": [{
                "id": "weather",
                "label": "Open-Meteo · UK Met Office data",
                "attribution_id": "open_meteo_ukmo",
                "observed_at_ms": 1_787_673_600_000,
                "stale_after_seconds": 900,
                "status": "current",
            }],
            "local_actions": [],
            "ttl_seconds": 900,
        },
    }


@pytest.mark.asyncio
async def test_context_present_handler_uses_only_the_fixed_contracted_route(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    arguments = context_present_arguments()
    calls = []

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, phone_arguments, *, schema_fingerprint
        ):
            calls.append((name, copy.deepcopy(phone_arguments), schema_fingerprint))
            return mcp_text(json.dumps({
                "status": "acknowledged",
                "dashboard_id": "ctx_" + "3" * 32,
                "presentation_generation": 1,
                "refresh_generation": 1,
                "revision": 1,
                "frame_id": 31,
            }))

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    result = json.loads(await tools.dispatch_mcp_workflow(
        "g2.context.present", arguments
    ))
    assert result["success"] is True
    assert result["receipt"]["operation_id"] == arguments["operation_id"]
    assert result["receipt"]["dashboard_key"] == arguments["spec"]["dashboard_key"]
    assert result["receipt"]["revision"] == 1
    assert result["receipt"]["frame_id"] == 31
    assert calls == [(
        "glasses.context_dashboard.present",
        arguments,
        contract.PHONE_SCHEMA_FINGERPRINTS["glasses.context_dashboard.present"],
    )]


@pytest.mark.asyncio
async def test_context_present_handler_never_claims_success_for_a_malformed_receipt(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    arguments = context_present_arguments()

    class Adapter:
        async def call_contracted_glasses_tool(self, *_args, **_kwargs):
            return mcp_text(json.dumps({
                "status": "acknowledged",
                "dashboard_id": "ctx_" + "4" * 32,
                "presentation_generation": 1,
                "refresh_generation": 1,
                "revision": 2,
                "frame_id": 32,
            }))

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    result = json.loads(await tools.dispatch_mcp_workflow(
        "g2.context.present", arguments
    ))
    assert result == {
        "success": False,
        "commit_state": "unknown",
        "error": "Context presentation may have completed but its frame acknowledgement was invalid",
        "operation_id": arguments["operation_id"],
    }


@pytest.mark.asyncio
async def test_context_present_handler_preserves_allowlisted_display_blocker(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    arguments = context_present_arguments()

    class Adapter:
        async def call_contracted_glasses_tool(self, *_args, **_kwargs):
            return mcp_text(json.dumps({
                "status": "rejected",
                "error_code": "clock_alert_active",
                "error": "The glasses display is busy with an active Clock alert.",
            }), error=True)

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    result = json.loads(await tools.dispatch_mcp_workflow(
        "g2.context.present", arguments
    ))
    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "operation_id": arguments["operation_id"],
        "error_code": "clock_alert_active",
        "error": "The glasses display is busy with an active Clock alert.",
    }


@pytest.mark.asyncio
async def test_native_handlers_map_every_action_to_one_fixed_contracted_phone_tool(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    health_empty = json.dumps({
        "source": "hermes-g2-ring",
        "schemaVersion": 1,
        "generatedAtMs": 1_787_673_600_000,
        "retentionDays": 31,
        "range": {"startDate": "2026-08-25", "endDate": "2026-08-25"},
        "hourlyIncluded": False,
        "history": [],
    })
    cases = (
        ("g2.device.apps.manage", {"action": "launch", "app_id": "music"}, "apps.launch", {"app_id": "music"}, "Opened music."),
        ("g2.device.apps.manage", {"action": "list_windows"}, "apps.list_windows", {}, ""),
        ("g2.device.apps.manage", {"action": "focus_window", "window_id": "music:1"}, "apps.focus_window", {"window_id": "music:1"}, 'Focused music:1 ("Music").'),
        ("g2.device.apps.manage", {"action": "close_window", "window_id": "music:1"}, "apps.close_window", {"window_id": "music:1"}, 'Closed music:1 ("Music").'),
        ("g2.device.apps.manage", {"action": "list_folders"}, "apps.list_folders", {}, "Ungrouped: (none)"),
        ("g2.device.apps.manage", {"action": "move_to_folder", "app_id": "music", "folder": "Daily"}, "apps.move_to_folder", {"app_id": "music", "folder": "Daily"}, 'Moved music into "Daily".'),
        ("g2.device.apps.manage", {"action": "remove_from_folder", "app_id": "music"}, "apps.remove_from_folder", {"app_id": "music"}, "music isn't in a folder."),
        ("g2.device.apps.manage", {"action": "disband_folder", "folder": "Daily"}, "apps.disband_folder", {"folder": "Daily"}, 'Disbanded "Daily"; 1 app moved to the top level.'),
        ("g2.device.media.control", {"action": "status"}, "media.now_playing", {}, "Nothing is currently playing."),
        ("g2.device.media.control", {"action": "play_pause"}, "media.play_pause", {}, "Toggled play/pause."),
        ("g2.device.media.control", {"action": "next"}, "media.next", {}, "Skipped to the next track."),
        ("g2.device.navigation", {"action": "start", "destination": "Manchester Piccadilly"}, "nav.start_navigation", {"destination": "Manchester Piccadilly", "profile": "driving"}, "Navigating to Manchester Piccadilly: 5 km, about 10 minutes, arriving 12:30 PM."),
        ("g2.device.navigation", {"action": "stop"}, "nav.stop_navigation", {}, "Navigation stopped."),
        ("g2.device.navigation", {"action": "status"}, "nav.route_status", {}, "Navigation is not active."),
        ("g2.device.notifications", {"action": "list", "max": 5}, "notifications.list", {"max": 5}, "No current notifications."),
        ("g2.device.notifications", {"action": "dismiss", "key": "phone|7"}, "notifications.dismiss", {"key": "phone|7"}, "Dismissed."),
        ("g2.device.health.summary", {"days": 1}, "health.get_ring_data", {"days": 1, "include_hourly": False}, health_empty),
        ("g2.device.calendar.agenda", {"within_hours": 24, "max_events": 3}, "calendar.list_events", {"within_hours": 24, "max_events": 3}, "No upcoming events in that window."),
    )
    calls: list[tuple[str, dict, str]] = []
    response_text = ""

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            calls.append((name, arguments, schema_fingerprint))
            return mcp_text(response_text)

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    for workflow, arguments, raw_name, raw_arguments, text in cases:
        response_text = text
        before = len(calls)
        result = json.loads(await tools.dispatch_mcp_workflow(workflow, arguments))
        assert result["success"] is True
        assert len(calls) == before + 1
        assert calls[-1] == (
            raw_name,
            raw_arguments,
            contract.PHONE_SCHEMA_FINGERPRINTS[raw_name],
        )


@pytest.mark.asyncio
async def test_native_dispatch_rejects_raw_phone_and_generic_meta_routes(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    called = False

    async def call_active(_factory):
        nonlocal called
        called = True
        raise AssertionError("raw route reached an adapter")

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    forbidden = set(contract.RAW_PHONE_TOOL_NAMES) | {
        "glasses_list_tools",
        "glasses_call",
        "g2.device.call",
        "g2.device.list_tools",
    }
    assert not forbidden & set(tools._MCP_WORKFLOW_HANDLERS)
    for name in sorted(forbidden):
        result = json.loads(await tools.dispatch_mcp_workflow(name, {}))
        assert result["success"] is False
        assert result["commit_state"] == "not_committed"
    assert called is False


@pytest.mark.asyncio
async def test_native_device_workflows_are_active_turn_only_and_fail_on_contract_drift(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    contract = importlib.import_module(f"{plugin_package.__name__}.device_voice_contract")
    calls = 0

    class Adapter:
        async def call_contracted_glasses_tool(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise contract.DeviceContractError("drift")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "api_server")
    denied = json.loads(await tools.dispatch_mcp_workflow(
        "g2.device.media.control", {"action": "status"}
    ))
    assert denied["error_code"] == "permission"
    assert calls == 0

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    drifted = json.loads(await tools.dispatch_mcp_workflow(
        "g2.device.media.control", {"action": "status"}
    ))
    assert drifted["error_code"] == "contract_drift"
    assert calls == 1
