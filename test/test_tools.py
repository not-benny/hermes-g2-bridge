from __future__ import annotations

import asyncio
import importlib
import json
import logging
import re
import threading
from datetime import date
from pathlib import Path

import pytest


_NOTIFY_OPERATION_ID = "result.task-42"
_REMINDER_OPERATION_ID = "rem.voice-42"
_WORK_TASK_OPERATION_ID = "work-task.voice-42"
_WORK_TASK_ID = "wt_0123456789abcdef0123456789abcdef"
_CLOCK_OPERATION_ID = "clock.voice-42"
_CLOCK_ITEM_ID = "clk_0123456789abcdef0123456789abcdef"


def _authorize_exact_g2_turn(monkeypatch, tools):
    authorization = object()

    async def authorize(expected=None):
        if expected is not None:
            assert expected is authorization
        return authorization

    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: Adapter())
    return authorization


def _notify_receipt_text(
    *,
    status="acknowledged",
    operation_id=_NOTIFY_OPERATION_ID,
    extra=None,
):
    receipt = {"status": status, "operation_id": operation_id}
    if extra is not None:
        receipt["extra"] = extra
    return json.dumps(receipt, separators=(",", ":"))


def _notify_mcp_result(receipt_text, *, is_error=False):
    return {
        "content": [{"type": "text", "text": receipt_text}],
        "isError": is_error,
    }


def _work_task_receipt_text(
    *,
    status="acknowledged",
    operation_id=_WORK_TASK_OPERATION_ID,
    task_id=_WORK_TASK_ID,
    lane="inbox",
    board_revision=7,
    extra=None,
):
    receipt = {
        "status": status,
        "operation_id": operation_id,
        "task_id": task_id,
        "lane": lane,
        "board_revision": board_revision,
    }
    if extra is not None:
        receipt["extra"] = extra
    return json.dumps(receipt, separators=(",", ":"))


def _clock_receipt_text(
    *,
    kind="timer",
    status="acknowledged",
    operation_id=_CLOCK_OPERATION_ID,
    item_id=_CLOCK_ITEM_ID,
    next_fire_at_ms=1_787_664_000_000,
    clock_revision=4,
    duration_seconds=600,
    local_time="07:30",
    date="2026-08-25",
    repeat_days=None,
    extra=None,
):
    receipt = {
        "status": status,
        "operation_id": operation_id,
        "item_id": item_id,
        "kind": kind,
        "next_fire_at_ms": next_fire_at_ms,
        "clock_revision": clock_revision,
    }
    if kind == "timer":
        receipt["duration_seconds"] = duration_seconds
    else:
        receipt.update({
            "local_time": local_time,
            "date": date,
            "repeat_days": [] if repeat_days is None else repeat_days,
        })
    if extra is not None:
        receipt["extra"] = extra
    return json.dumps(receipt, separators=(",", ":"))


@pytest.mark.asyncio
async def test_train_reader_is_typed_and_exact_active_g2_only(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    public_web = importlib.import_module(f"{plugin_package.__name__}.public_web")
    calls = []
    authorization = object()

    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

    async def call_active(factory):
        return await factory(Adapter())

    def read_train_departures(origin, destination, *, cancelled, deadline):
        calls.append((origin, destination, cancelled, deadline))
        return {
            "source": "National Rail",
            "origin_crs": origin,
            "destination_crs": destination,
            "data_kind": "live",
            "observed_at_ms": 1_777_102_400_000,
            "departures": [],
        }

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    monkeypatch.setattr(public_web, "read_train_departures", read_train_departures)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "api_server")
    denied = json.loads(await tools._handle_train_departures({
        "origin_crs": "BLN",
        "destination_crs": "LVC",
    }))
    assert denied == {
        "success": False,
        "error": "National Rail departures are available only during an active G2 turn",
    }
    assert calls == []

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    result = json.loads(await tools._handle_train_departures({
        "origin_crs": "BLN",
        "destination_crs": "LVC",
    }))
    assert result["success"] is True
    assert result["trust"] == "typed_national_rail_data"
    assert result["result"]["origin_crs"] == "BLN"
    assert result["result"]["destination_crs"] == "LVC"
    assert [(item[0], item[1]) for item in calls] == [("BLN", "LVC")]
    assert isinstance(calls[0][2], threading.Event)
    assert isinstance(calls[0][3], float)


@pytest.mark.asyncio
async def test_train_reader_rejects_shape_and_hides_failures(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    public_web = importlib.import_module(f"{plugin_package.__name__}.public_web")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")

    token = object()

    async def authorize(expected=None):
        if expected is not None and expected is not token:
            raise PermissionError("changed")
        return token

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)

    for payload in (
        {},
        {"origin_crs": "BLN", "destination_crs": "LVC", "url": "https://example.com"},
        {"origin_crs": "bln", "destination_crs": "LVC"},
        {"origin_crs": "BLN", "destination_crs": "BLN"},
    ):
        result = json.loads(await tools._handle_train_departures(payload))
        assert result["success"] is False

    def fail_safely(*_args, **_kwargs):
        raise public_web.TrainReadError("private diagnostic must not escape")

    monkeypatch.setattr(public_web, "read_train_departures", fail_safely)
    result = json.loads(await tools._handle_train_departures({
        "origin_crs": "BLN",
        "destination_crs": "LVC",
    }))
    assert result == {
        "success": False,
        "error": "National Rail departures could not be read safely in this active turn",
    }
    assert "diagnostic" not in json.dumps(result)


@pytest.mark.asyncio
async def test_train_stage_diagnostics_never_log_request_turn_or_exception_data(
    plugin_package, monkeypatch, caplog
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    public_web = importlib.import_module(f"{plugin_package.__name__}.public_web")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")

    session_secret = "private-train-session-9472"

    class PrivateAuthorization:
        def __str__(self):
            return session_secret

        __repr__ = __str__

    authorization = PrivateAuthorization()

    async def authorize(expected=None):
        if expected is not None:
            assert expected is authorization
        return authorization

    exception_secret = "private-train-exception-7318"

    def fail_unexpectedly(*_args, **_kwargs):
        raise RuntimeError(exception_secret)

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(public_web, "read_train_departures", fail_unexpectedly)
    caplog.set_level(logging.INFO, logger=tools.__name__)

    origin_secret = "QXZ"
    destination_secret = "ZQX"
    result = json.loads(await tools._handle_train_departures({
        "origin_crs": origin_secret,
        "destination_crs": destination_secret,
    }))

    assert result == {
        "success": False,
        "error": "the isolated National Rail reader is unavailable",
    }
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == tools.__name__
        and record.getMessage().startswith("G2 public-data workflow stage=")
    ]
    assert messages == [
        "G2 public-data workflow stage=train.entered",
        "G2 public-data workflow stage=train.authorized",
        "G2 public-data workflow stage=train.reader_started",
        "G2 public-data workflow stage=train.reader_unexpected",
    ]
    rendered = "\n".join(messages)
    for secret in (
        origin_secret,
        destination_secret,
        session_secret,
        exception_secret,
    ):
        assert secret not in rendered


@pytest.mark.asyncio
async def test_train_reader_cancellation_stops_the_browser_worker(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    public_web = importlib.import_module(f"{plugin_package.__name__}.public_web")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    token = object()
    async def authorize(expected=None):
        return token
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)

    entered = threading.Event()
    stopped = threading.Event()

    def wait_for_cancel(_origin, _destination, *, cancelled, deadline):
        entered.set()
        assert isinstance(deadline, float)
        cancelled.wait(2)
        if cancelled.is_set():
            stopped.set()
        raise public_web.TrainReadError("cancelled")

    monkeypatch.setattr(public_web, "read_train_departures", wait_for_cancel)
    task = asyncio.create_task(tools._handle_train_departures({
        "origin_crs": "BLN",
        "destination_crs": "LVC",
    }))
    assert await asyncio.to_thread(entered.wait, 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await asyncio.to_thread(stopped.wait, 1)


@pytest.mark.asyncio
async def test_weather_reader_is_typed_frozen_and_exact_active_g2_only(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    weather = importlib.import_module(f"{plugin_package.__name__}.weather_provider")
    token = object()
    authorizations = []
    calls = []

    async def authorize(expected=None):
        authorizations.append(expected)
        if expected is not None and expected is not token:
            raise PermissionError("changed")
        return token

    async def read_weather(location, **kwargs):
        calls.append((location, kwargs))
        return {
            "location_label": "Liverpool",
            "date": "2026-08-26",
            "weather_code": 61,
            "condition": "rain",
            "temperature_min_c": 9.7,
            "temperature_max_c": 17.2,
            "precipitation_probability_max_pct": None,
            "precipitation_amount_mm": 3.4,
            "wind_speed_max_kmh": 29.6,
            "source": "Open-Meteo · UK Met Office data",
            "observed_at_ms": 1_787_702_399_000,
        }

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(weather, "capture_reference_date", lambda: date(2026, 8, 25))
    monkeypatch.setattr(weather, "read_weather", read_weather)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "api_server")
    denied = json.loads(await tools._handle_weather_forecast({
        "location": "Liverpool",
        "day_offset": 1,
    }))
    assert denied["success"] is False
    assert denied["error_code"] == "permission"
    assert calls == []

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    result = json.loads(await tools._handle_weather_forecast({
        "location": "Liverpool",
        "day_offset": 1,
    }))
    assert result["success"] is True
    assert result["trust"] == "typed_open_meteo_ukmo_data"
    assert re.fullmatch(r"weather-[a-f0-9]{32}", result["dashboard_key"])
    assert result["title"] == "Liverpool · Tomorrow"
    assert result["result"]["condition"] == "rain"
    assert result["result"]["source"] == "Open-Meteo · UK Met Office data"
    assert authorizations == [None, token]
    assert calls == [("Liverpool", {
        "day_offset": 1,
        "date": None,
        "timeout_seconds": 15.0,
        "reference_date": date(2026, 8, 25),
    })]

    same_key, same_title = tools._weather_dashboard_identity(result["result"], {
        "location": "Liverpool", "day_offset": 1,
    })
    other_key, other_title = tools._weather_dashboard_identity({
        **result["result"], "location_label": "Cambridge",
    }, {"location": "Cambridge", "day_offset": 1})
    assert same_key == result["dashboard_key"]
    assert same_title == result["title"]
    assert other_key != same_key
    assert other_title == "Cambridge · Tomorrow"
    next_day_key, _next_day_title = tools._weather_dashboard_identity({
        **result["result"], "date": "2026-08-27",
    }, {"location": "Liverpool", "day_offset": 1})
    assert next_day_key == same_key
    absolute_key, _absolute_title = tools._weather_dashboard_identity({
        **result["result"], "date": "2026-08-27",
    }, {"location": "Liverpool", "date": "2026-08-27"})
    assert absolute_key != same_key


@pytest.mark.asyncio
async def test_weather_reader_rejects_shape_ambiguity_and_stale_turn_without_leaking(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    weather = importlib.import_module(f"{plugin_package.__name__}.weather_provider")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")

    calls = []
    token = object()

    async def authorize(expected=None):
        calls.append(expected)
        if expected is token:
            raise PermissionError("turn moved")
        return token

    async def ambiguous(*_args, **_kwargs):
        raise weather.WeatherLocationAmbiguous("private candidates must not escape")

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(weather, "capture_reference_date", lambda: date(2026, 8, 25))
    monkeypatch.setattr(weather, "read_weather", ambiguous)

    for payload in (
        {},
        {"location": "Liverpool", "day_offset": 1, "date": "2026-08-26"},
        {"location": "Liverpool", "url": "https://evil.example"},
    ):
        rejected = json.loads(await tools._handle_weather_forecast(payload))
        assert rejected["success"] is False
        assert rejected["error_code"] == "invalid_request"

    result = json.loads(await tools._handle_weather_forecast({"location": "Cambridge"}))
    assert result == {
        "success": False,
        "state": "error",
        "error_code": "permission",
        "error": "the exact G2 turn is no longer active",
    }
    assert "candidates" not in json.dumps(result)
    assert calls == [None, token]

    async def stable_authorize(expected=None):
        return token

    monkeypatch.setattr(tools, "_authorize_active_g2_read", stable_authorize)
    result = json.loads(await tools._handle_weather_forecast({"location": "Cambridge"}))
    assert result == {
        "success": False,
        "state": "error",
        "error_code": "ambiguous_location",
        "error": "Weather location is ambiguous; add a UK county or region",
    }
    assert "private" not in json.dumps(result)


@pytest.mark.asyncio
async def test_weather_stage_diagnostics_never_log_location_turn_or_exception_data(
    plugin_package, monkeypatch, caplog
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    weather = importlib.import_module(f"{plugin_package.__name__}.weather_provider")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")

    session_secret = "private-weather-session-2851"

    class PrivateAuthorization:
        def __str__(self):
            return session_secret

        __repr__ = __str__

    authorization = PrivateAuthorization()

    async def authorize(expected=None):
        if expected is not None:
            assert expected is authorization
        return authorization

    exception_secret = "private-weather-exception-6149"

    async def fail_unexpectedly(*_args, **_kwargs):
        raise RuntimeError(exception_secret)

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(weather, "capture_reference_date", lambda: date(2026, 8, 25))
    monkeypatch.setattr(weather, "read_weather", fail_unexpectedly)
    caplog.set_level(logging.INFO, logger=tools.__name__)

    location_secret = "Private Place 8844, Testville"
    result = json.loads(await tools._handle_weather_forecast({
        "location": location_secret,
    }))

    assert result == {
        "success": False,
        "state": "offline",
        "error_code": "unavailable",
        "error": "the isolated weather reader is unavailable",
    }
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == tools.__name__
        and record.getMessage().startswith("G2 public-data workflow stage=")
    ]
    assert messages == [
        "G2 public-data workflow stage=weather.entered",
        "G2 public-data workflow stage=weather.authorized",
        "G2 public-data workflow stage=weather.reader_started",
        "G2 public-data workflow stage=weather.reader_unexpected",
        "G2 public-data workflow stage=weather.turn_revalidated",
    ]
    rendered = "\n".join(messages)
    for secret in (location_secret, session_secret, exception_secret):
        assert secret not in rendered


def test_public_read_diagnostics_reject_non_allowlisted_stage_without_logging(
    plugin_package, caplog
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    caplog.set_level(logging.INFO, logger=tools.__name__)

    with pytest.raises(TypeError):
        tools._log_public_read_stage("weather.private-location")

    assert not [
        record
        for record in caplog.records
        if record.name == tools.__name__
        and record.getMessage().startswith("G2 public-data workflow stage=")
    ]


@pytest.mark.asyncio
async def test_weather_reader_cancellation_propagates(plugin_package, monkeypatch):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    weather = importlib.import_module(f"{plugin_package.__name__}.weather_provider")
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    token = object()

    async def authorize(expected=None):
        return token

    entered = asyncio.Event()

    async def wait_forever(*_args, **_kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(weather, "capture_reference_date", lambda: date(2026, 8, 25))
    monkeypatch.setattr(weather, "read_weather", wait_forever)
    task = asyncio.create_task(tools._handle_weather_forecast({"location": "Liverpool"}))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", ["queued", "acknowledged", "historical_acknowledgement"]
)
async def test_notify_result_routes_only_exact_bounded_payload_to_fixed_phone_tool(
    plugin_package, monkeypatch, status
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    class Adapter:
        async def call_contracted_notify_result(self, arguments, *, schema_fingerprint):
            assert len(schema_fingerprint) == 64
            calls.append(("glasses.notify_result", arguments))
            return _notify_mcp_result(_notify_receipt_text(status=status))

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    payload = {"operation_id": _NOTIFY_OPERATION_ID, "text": "The task is complete."}
    result = json.loads(await tools._handle_notify_result(payload))

    assert result == {
        "success": True,
        "receipt": {"status": status, "operation_id": _NOTIFY_OPERATION_ID},
    }
    assert calls == [
        (
            "glasses.notify_result",
            {"operation_id": _NOTIFY_OPERATION_ID, "text": "The task is complete."},
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mcp_result",
    [
        None,
        [],
        {},
        {"content": [{"type": "text", "text": _notify_receipt_text()}]},
        _notify_mcp_result(_notify_receipt_text(), is_error=True),
        {"content": [{"type": "text", "text": _notify_receipt_text()}], "isError": 0},
        {
            "content": [{"type": "text", "text": _notify_receipt_text()}],
            "isError": False,
            "structuredContent": {},
        },
        {"content": None, "isError": False},
        {"content": [], "isError": False},
        {
            "content": [
                {"type": "text", "text": _notify_receipt_text()},
                {"type": "text", "text": _notify_receipt_text()},
            ],
            "isError": False,
        },
        {"content": ["not-an-mcp-content-item"], "isError": False},
        {"content": [{"text": _notify_receipt_text()}], "isError": False},
        {"content": [{"type": "text"}], "isError": False},
        {
            "content": [
                {"type": "text", "text": _notify_receipt_text(), "annotations": {}}
            ],
            "isError": False,
        },
        {"content": [{"type": "image", "text": _notify_receipt_text()}], "isError": False},
        {"content": [{"type": "text", "text": 7}], "isError": False},
        _notify_mcp_result(""),
        _notify_mcp_result("acknowledged"),
        _notify_mcp_result("{}"),
        _notify_mcp_result("[]"),
        _notify_mcp_result(_notify_receipt_text(status="delivered")),
        _notify_mcp_result(_notify_receipt_text(status=[])),
        _notify_mcp_result(_notify_receipt_text(operation_id="another-operation")),
        _notify_mcp_result(_notify_receipt_text(extra=True)),
        _notify_mcp_result(
            '{"status":"acknowledged","status":"historical_acknowledgement",'
            '"operation_id":"result.task-42"}'
        ),
        _notify_mcp_result(
            '{"status":"acknowledged","operation_id":"result.task-42",'
            '"operation_id":"result.task-42"}'
        ),
        _notify_mcp_result(_notify_receipt_text() + _notify_receipt_text()),
        _notify_mcp_result((" " * 161) + _notify_receipt_text()),
    ],
)
async def test_notify_result_fails_closed_on_malformed_or_ambiguous_receipt(
    plugin_package, monkeypatch, mcp_result
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    class Adapter:
        async def call_contracted_notify_result(self, arguments, *, schema_fingerprint):
            assert len(schema_fingerprint) == 64
            calls.append(("glasses.notify_result", arguments))
            return mcp_result

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_notify_result({
        "operation_id": _NOTIFY_OPERATION_ID,
        "text": "The task is complete.",
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _NOTIFY_OPERATION_ID,
        "error": "glasses notification did not return an exact acknowledgement receipt",
    }
    assert calls == [(
        "glasses.notify_result",
        {"operation_id": _NOTIFY_OPERATION_ID, "text": "The task is complete."},
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"operation_id": "result-1"},
        {"text": "done"},
        {"operation_id": "result-1", "text": "done", "tool": "glasses.show_alert"},
        {"operation_id": "result-1", "text": "done", "arguments": {"wake": True}},
        {"operation_id": "bad/id", "text": "done"},
        {"operation_id": "x" * 65, "text": "done"},
        {"operation_id": "result-1", "text": ""},
        {"operation_id": "result-1", "text": " "},
        {"operation_id": "result-1", "text": "x" * 161},
    ],
)
async def test_notify_result_rejects_generic_or_unbounded_payload_before_adapter(
    plugin_package, monkeypatch, payload
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def call_active(factory):
        calls.append(factory)
        raise AssertionError("invalid payload reached the active adapter")

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_notify_result(payload))

    assert result["success"] is False
    assert result["commit_state"] == "not_committed"
    assert calls == []


@pytest.mark.asyncio
async def test_notify_result_marks_transport_failure_as_ambiguous_for_same_id_retry(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    class OutcomeUnknown(ConnectionError):
        commit_state = "unknown"

    class Adapter:
        async def call_contracted_notify_result(self, arguments, *, schema_fingerprint):
            assert len(schema_fingerprint) == 64
            assert arguments == {
                "operation_id": _NOTIFY_OPERATION_ID,
                "text": "Reminder: call Simon",
            }
            raise OutcomeUnknown("response lost")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_notify_result({
        "operation_id": _NOTIFY_OPERATION_ID,
        "text": "Reminder: call Simon",
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _NOTIFY_OPERATION_ID,
        "error": "glasses notification outcome is unknown after phone handoff",
    }
    assert "response lost" not in json.dumps(result)


@pytest.mark.asyncio
async def test_notify_result_marks_definite_pre_call_rejection_as_not_committed(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    class Adapter:
        async def call_contracted_notify_result(self, arguments, *, schema_fingerprint):
            assert len(schema_fingerprint) == 64
            raise PermissionError("phone is not connected")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_notify_result({
        "operation_id": _NOTIFY_OPERATION_ID,
        "text": "Reminder: call Simon",
    }))

    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "error": "glasses notification was unavailable before phone handoff",
    }
    assert "phone is not connected" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["acknowledged", "historical_acknowledgement"])
@pytest.mark.parametrize(
    ("payload", "expected_phone_arguments", "expected_lane"),
    [
        (
            {"operation_id": _WORK_TASK_OPERATION_ID, "title": "  Cafe\u0301  "},
            {"operation_id": _WORK_TASK_OPERATION_ID, "title": "Café"},
            "inbox",
        ),
        (
            {
                "operation_id": _WORK_TASK_OPERATION_ID,
                "title": "Email Simon about merger permissions",
                "lane": "today",
            },
            {
                "operation_id": _WORK_TASK_OPERATION_ID,
                "title": "Email Simon about merger permissions",
                "lane": "today",
            },
            "today",
        ),
    ],
)
async def test_work_task_add_routes_normalized_exact_payload_only_to_fixed_phone_tool(
    plugin_package,
    monkeypatch,
    status,
    payload,
    expected_phone_arguments,
    expected_lane,
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            calls.append((name, arguments))
            return _notify_mcp_result(
                _work_task_receipt_text(status=status, lane=expected_lane)
            )

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_work_task_add(payload))

    assert result == {
        "success": True,
        "receipt": {
            "status": status,
            "operation_id": _WORK_TASK_OPERATION_ID,
            "task_id": _WORK_TASK_ID,
            "lane": expected_lane,
            "board_revision": 7,
        },
    }
    assert calls == [("glasses.work_board.add_task", expected_phone_arguments)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mcp_result",
    [
        None,
        [],
        {},
        {"content": [{"type": "text", "text": _work_task_receipt_text()}]},
        _notify_mcp_result(_work_task_receipt_text(), is_error=True),
        {
            "content": [{"type": "text", "text": _work_task_receipt_text()}],
            "isError": 0,
        },
        {
            "content": [{"type": "text", "text": _work_task_receipt_text()}],
            "isError": False,
            "structuredContent": {},
        },
        {"content": [], "isError": False},
        {
            "content": [
                {"type": "text", "text": _work_task_receipt_text()},
                {"type": "text", "text": _work_task_receipt_text()},
            ],
            "isError": False,
        },
        {
            "content": [
                {"type": "text", "text": _work_task_receipt_text(), "extra": True}
            ],
            "isError": False,
        },
        _notify_mcp_result(""),
        _notify_mcp_result("not-json"),
        _notify_mcp_result(_work_task_receipt_text(status="created")),
        _notify_mcp_result(_work_task_receipt_text(operation_id="different-op")),
        _notify_mcp_result(_work_task_receipt_text(task_id="wt_NOT_HEX")),
        _notify_mcp_result(_work_task_receipt_text(lane="today")),
        _notify_mcp_result(_work_task_receipt_text(board_revision=0)),
        _notify_mcp_result(_work_task_receipt_text(board_revision=True)),
        _notify_mcp_result(_work_task_receipt_text(board_revision=7.0)),
        _notify_mcp_result(_work_task_receipt_text(board_revision=9_007_199_254_740_992)),
        _notify_mcp_result(_work_task_receipt_text(extra=True)),
        _notify_mcp_result(
            '{"status":"acknowledged","status":"historical_acknowledgement",'
            '"operation_id":"work-task.voice-42",'
            '"task_id":"wt_0123456789abcdef0123456789abcdef",'
            '"lane":"inbox","board_revision":7}'
        ),
        _notify_mcp_result((" " * 321) + _work_task_receipt_text()),
    ],
)
async def test_work_task_add_fails_closed_on_malformed_or_ambiguous_receipt(
    plugin_package, monkeypatch, mcp_result
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            calls.append((name, arguments))
            return mcp_result

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_work_task_add({
        "operation_id": _WORK_TASK_OPERATION_ID,
        "title": "Email Simon",
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _WORK_TASK_OPERATION_ID,
        "error": "Work Tasks did not return an exact acknowledgement receipt",
    }
    assert calls == [(
        "glasses.work_board.add_task",
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "Email Simon"},
    )]


@pytest.mark.asyncio
async def test_work_task_add_marks_transport_failure_as_ambiguous_for_same_id_retry(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    class OutcomeUnknown(ConnectionError):
        commit_state = "unknown"

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            assert name == "glasses.work_board.add_task"
            assert arguments["operation_id"] == _WORK_TASK_OPERATION_ID
            raise OutcomeUnknown("response lost")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_work_task_add({
        "operation_id": _WORK_TASK_OPERATION_ID,
        "title": "Email Simon",
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _WORK_TASK_OPERATION_ID,
        "error": "response lost",
    }


@pytest.mark.asyncio
async def test_work_task_add_marks_definite_pre_call_rejection_as_not_committed(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            raise PermissionError("active G2 turn required")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_work_task_add({
        "operation_id": _WORK_TASK_OPERATION_ID,
        "title": "Email Simon",
    }))

    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "error": "active G2 turn required",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"operation_id": _WORK_TASK_OPERATION_ID},
        {"title": "Email Simon"},
        {
            "operation_id": _WORK_TASK_OPERATION_ID,
            "title": "Email Simon",
            "tool": "glasses.show_alert",
        },
        {
            "operation_id": _WORK_TASK_OPERATION_ID,
            "title": "Email Simon",
            "arguments": {},
        },
        {"operation_id": "bad/id", "title": "Email Simon"},
        {"operation_id": "ø", "title": "Email Simon"},
        {"operation_id": "x" * 65, "title": "Email Simon"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": None},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": ""},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "   "},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "line one\nline two"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "control\u0085text"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "line\u2028break"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "paragraph\u2029break"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "arabic-mark\u061ctext"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "ltr-mark\u200etext"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "rtl-mark\u200ftext"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "bidi\u202etext"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "isolate\u2066text"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "surrogate\ud800"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "x" * 121},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "Email Simon", "lane": None},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "Email Simon", "lane": "done"},
        {"operation_id": _WORK_TASK_OPERATION_ID, "title": "Email Simon", "lane": "Inbox"},
    ],
)
async def test_work_task_add_rejects_generic_or_unsafe_payload_before_adapter(
    plugin_package, monkeypatch, payload
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def call_active(factory):
        calls.append(factory)
        raise AssertionError("invalid payload reached the active adapter")

    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_work_task_add(payload))

    assert result["success"] is False
    assert result["commit_state"] == "not_committed"
    assert calls == []


def test_work_task_title_unicode_boundaries_are_scalar_and_utf8_bounded(plugin_package):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    assert tools._normalize_work_task_title("  مرحبا بالعالم  ") == "مرحبا بالعالم"
    assert tools._normalize_work_task_title("😀" * 120) == "😀" * 120
    assert len(("😀" * 120).encode("utf-8")) == 480
    assert tools._normalize_work_task_title("😀" * 121) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["acknowledged", "historical_acknowledgement"])
async def test_clock_timer_routes_only_exact_normalized_payload_to_fixed_phone_tool(
    plugin_package, monkeypatch, status
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            calls.append((name, arguments))
            return _notify_mcp_result(_clock_receipt_text(status=status))

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_clock_set_timer({
        "operation_id": _CLOCK_OPERATION_ID,
        "duration_seconds": 600,
        "label": "  Cafe\u0301  ",
    }))

    assert result == {
        "success": True,
        "receipt": json.loads(_clock_receipt_text(status=status)),
    }
    assert calls == [(
        "glasses.clock.set_timer",
        {
            "operation_id": _CLOCK_OPERATION_ID,
            "duration_seconds": 600,
            "label": "Café",
        },
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "phone_arguments", "receipt_kwargs"),
    [
        (
            {
                "operation_id": _CLOCK_OPERATION_ID,
                "local_time": "07:30",
                "date": "2026-08-25",
                "label": "Wake up",
            },
            {
                "operation_id": _CLOCK_OPERATION_ID,
                "local_time": "07:30",
                "date": "2026-08-25",
                "label": "Wake up",
            },
            {"date": "2026-08-25", "repeat_days": []},
        ),
        (
            {
                "operation_id": _CLOCK_OPERATION_ID,
                "local_time": "07:30",
                "repeat_days": ["fri", "mon", "wed"],
            },
            {
                "operation_id": _CLOCK_OPERATION_ID,
                "local_time": "07:30",
                "repeat_days": ["mon", "wed", "fri"],
            },
            {"date": None, "repeat_days": ["mon", "wed", "fri"]},
        ),
        (
            {"operation_id": _CLOCK_OPERATION_ID, "local_time": "22:05"},
            {"operation_id": _CLOCK_OPERATION_ID, "local_time": "22:05"},
            {"local_time": "22:05", "date": "2026-08-25", "repeat_days": []},
        ),
    ],
)
async def test_clock_alarm_normalizes_phone_local_schedule_and_requires_exact_receipt(
    plugin_package, monkeypatch, payload, phone_arguments, receipt_kwargs
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []
    receipt_kwargs = {"kind": "alarm", **receipt_kwargs}

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            calls.append((name, arguments))
            return _notify_mcp_result(_clock_receipt_text(**receipt_kwargs))

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_clock_set_alarm(payload))

    assert result == {
        "success": True,
        "receipt": json.loads(_clock_receipt_text(**receipt_kwargs)),
    }
    assert calls == [("glasses.clock.set_alarm", phone_arguments)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "payload"),
    [
        ("_handle_clock_set_timer", {}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": 0}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": True}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": 604_801}),
        ("_handle_clock_set_timer", {"operation_id": "bad/id", "duration_seconds": 60}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": 60, "label": "line\nbreak"}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": 60, "label": "x" * 81}),
        ("_handle_clock_set_timer", {"operation_id": _CLOCK_OPERATION_ID, "duration_seconds": 60, "tool": "timer.set"}),
        ("_handle_clock_set_alarm", {}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "7:30"}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "24:00"}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "date": "2026-02-30"}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "repeat_days": []}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "repeat_days": ["mon", "mon"]}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "repeat_days": ["monday"]}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "date": "2026-08-25", "repeat_days": ["mon"]}),
        ("_handle_clock_set_alarm", {"operation_id": _CLOCK_OPERATION_ID, "local_time": "07:30", "arguments": {}}),
    ],
)
async def test_clock_wrappers_reject_unsafe_or_generic_payload_before_phone(
    plugin_package, monkeypatch, handler_name, payload
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def call_active(factory):
        calls.append(factory)
        raise AssertionError("invalid Clock payload reached phone adapter")

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await getattr(tools, handler_name)(payload))

    assert result["success"] is False
    assert result["commit_state"] == "not_committed"
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mcp_result",
    [
        None,
        {},
        _notify_mcp_result(_clock_receipt_text(), is_error=True),
        _notify_mcp_result(_clock_receipt_text(status="scheduled")),
        _notify_mcp_result(_clock_receipt_text(operation_id="different")),
        _notify_mcp_result(_clock_receipt_text(item_id="clock_bad")),
        _notify_mcp_result(_clock_receipt_text(kind="alarm")),
        _notify_mcp_result(_clock_receipt_text(duration_seconds=601)),
        _notify_mcp_result(_clock_receipt_text(clock_revision=True)),
        _notify_mcp_result(_clock_receipt_text(extra=True)),
        _notify_mcp_result(
            '{"status":"acknowledged","status":"historical_acknowledgement",'
            '"operation_id":"clock.voice-42","item_id":"clk_0123456789abcdef0123456789abcdef",'
            '"kind":"timer","next_fire_at_ms":1787664000000,"clock_revision":4,'
            '"duration_seconds":600}'
        ),
        _notify_mcp_result((" " * 641) + _clock_receipt_text()),
    ],
)
async def test_clock_wrapper_marks_malformed_post_phone_receipt_unknown(
    plugin_package, monkeypatch, mcp_result
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            assert name == "glasses.clock.set_timer"
            return mcp_result

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    result = json.loads(await tools._handle_clock_set_timer({
        "operation_id": _CLOCK_OPERATION_ID,
        "duration_seconds": 600,
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _CLOCK_OPERATION_ID,
        "error": "Clock did not return an exact acknowledgement receipt",
    }


@pytest.mark.asyncio
async def test_clock_wrappers_are_g2_only_and_preserve_unknown_outcome_identity(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def unreachable(_factory):
        calls.append("called")
        raise AssertionError("non-G2 Clock request reached adapter")

    monkeypatch.setattr(tools.runtime, "call_active", unreachable)
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "api_server")
    denied = json.loads(await tools._handle_clock_set_timer({
        "operation_id": _CLOCK_OPERATION_ID,
        "duration_seconds": 600,
    }))
    assert denied["commit_state"] == "not_committed"
    assert calls == []

    class OutcomeUnknown(ConnectionError):
        commit_state = "unknown"

    class Adapter:
        async def call_contracted_glasses_tool(
            self, name, arguments, *, schema_fingerprint
        ):
            assert len(schema_fingerprint) == 64
            raise OutcomeUnknown("private response-loss diagnostic")

    async def call_active(factory):
        return await factory(Adapter())

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools.runtime, "call_active", call_active)
    unknown = json.loads(await tools._handle_clock_set_alarm({
        "operation_id": _CLOCK_OPERATION_ID,
        "local_time": "07:30",
    }))
    assert unknown == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _CLOCK_OPERATION_ID,
        "error": "Clock scheduling outcome is unknown after phone handoff",
    }
    assert "diagnostic" not in json.dumps(unknown)


@pytest.mark.asyncio
async def test_schedule_reminder_commits_only_through_active_adapter_outbox(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    created = []
    authorization = object()

    async def authorize(expected=None):
        assert expected is None
        return authorization

    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

        def schedule_g2_reminder(self, operation_id, schedule, text):
            created.append((operation_id, schedule, text))
            return {
                "success": True,
                "status": "scheduled",
                "operation_id": operation_id,
                "reminder_id": "abc123",
                "due_at": "2026-08-24T14:00:00.000Z",
            }

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: Adapter())
    result = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "10m",
        "text": "Reminder: check the oven.",
    }))

    assert result == {
        "success": True,
        "status": "scheduled",
        "operation_id": _REMINDER_OPERATION_ID,
        "reminder_id": "abc123",
        "due_at": "2026-08-24T14:00:00.000Z",
    }
    assert created == [
        (_REMINDER_OPERATION_ID, "10m", "Reminder: check the oven.")
    ]


@pytest.mark.asyncio
async def test_schedule_reminder_is_g2_only_and_rejects_unsafe_input_before_outbox(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "api_server")
    denied = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "10m",
        "text": "Reminder: check the oven.",
    }))
    assert denied["success"] is False
    assert denied["commit_state"] == "not_committed"

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")

    authorization = object()

    async def authorize(expected=None):
        return authorization

    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

        def schedule_g2_reminder(self, *_args):
            calls.append(_args)
            raise AssertionError("unsafe input reached outbox")

    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: Adapter())
    for payload in [
        {"operation_id": _REMINDER_OPERATION_ID, "schedule": "10m"},
        {"operation_id": "bad/id", "schedule": "10m", "text": "Reminder: x"},
        {"operation_id": _REMINDER_OPERATION_ID, "schedule": "line\nbreak", "text": "Reminder: x"},
        {"operation_id": _REMINDER_OPERATION_ID, "schedule": "10m", "text": "line\nbreak"},
        {"operation_id": _REMINDER_OPERATION_ID, "schedule": "10m", "text": "https://example.test"},
    ]:
        rejected = json.loads(await tools._handle_schedule_reminder(payload))
        assert rejected["success"] is False
        assert rejected["commit_state"] == "not_committed"
    assert calls == []


@pytest.mark.asyncio
async def test_schedule_reminder_rechecks_exact_turn_immediately_before_mutation(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    initial = object()
    replacement = object()
    created = []

    async def authorize(expected=None):
        assert expected is None
        return initial

    class Adapter:
        def authorize_active_g2_turn(self):
            return replacement

        def schedule_g2_reminder(self, *args):
            created.append(args)

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: Adapter())

    result = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "10m",
        "text": "Reminder: check the oven.",
    }))

    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "error": "G2 reminder turn authority expired before scheduling",
    }
    assert created == []


@pytest.mark.asyncio
async def test_schedule_reminder_preserves_historical_result_and_conflict_contract(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    authorization = object()

    async def authorize(expected=None):
        return authorization

    class Adapter:
        calls = 0

        def authorize_active_g2_turn(self):
            return authorization

        def schedule_g2_reminder(self, operation_id, schedule, text):
            self.calls += 1
            if self.calls == 1:
                return {
                    "success": True,
                    "status": "historical_scheduled",
                    "operation_id": operation_id,
                    "reminder_id": "historic",
                    "due_at": "2026-08-24T14:00:00.000Z",
                }
            raise tools.ReminderConflictError(
                "operation_id is already bound to a different reminder"
            )

    adapter = Adapter()
    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: adapter)

    replay = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "10m",
        "text": "Reminder: check the oven.",
    }))
    conflict = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "20m",
        "text": "Reminder: check the oven.",
    }))
    assert replay["status"] == "historical_scheduled"
    assert replay["reminder_id"] == "historic"
    assert conflict["success"] is False
    assert conflict["commit_state"] == "not_committed"


@pytest.mark.asyncio
async def test_schedule_reminder_never_claims_unknown_outbox_write(
    plugin_package, monkeypatch
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    authorization = object()

    async def authorize(expected=None):
        return authorization

    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

        def schedule_g2_reminder(self, *_args):
            raise tools.ReminderStoreWriteError("injected")

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: Adapter())

    result = json.loads(await tools._handle_schedule_reminder({
        "operation_id": _REMINDER_OPERATION_ID,
        "schedule": "10m",
        "text": "Reminder: check the oven.",
    }))

    assert result == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _REMINDER_OPERATION_ID,
        "error": tools._REMINDER_CREATE_ERROR,
    }


def test_tools_module_has_only_the_fixed_private_relay_surface(plugin_package):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    source = Path(tools.__file__).read_text(encoding="utf-8")

    assert not hasattr(tools, "register_tools")
    assert not hasattr(tools, "_handle_list_tools")
    assert not hasattr(tools, "_handle_call")
    assert "register_tool" not in source
    assert set(tools._MCP_WORKFLOW_HANDLERS) == {
        "g2.notifications.deliver_final",
        "g2.reminders.create",
        "g2.work_tasks.add",
        "g2.clock.set_timer",
        "g2.clock.set_alarm",
        "g2.transit.read_departures",
        "g2.weather.read_forecast",
        "g2.context.present",
        "g2.device.apps.manage",
        "g2.device.media.control",
        "g2.device.navigation",
        "g2.device.notifications",
        "g2.device.health.summary",
        "g2.device.calendar.agenda",
    }
