"""Pinned phone MCP contracts and privacy-bounded result decoders.

The workflow MCP never receives a phone tool name from the model.  Native
handlers select one entry in this module, verify the connected phone's exact
MCP identity and structural input-schema fingerprint, then invoke that fixed
tool.  Results are reduced to typed, bounded values before crossing the local
workflow relay.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from datetime import date
from typing import Any


PHONE_MCP_PROTOCOL_VERSION = "2025-06-18"
PHONE_MCP_SERVER_NAME = "hermes-g2"
PHONE_MCP_SERVER_VERSION = "1.0.0"

MAX_PHONE_TEXT_BYTES = 24 * 1024
MAX_SUMMARY_CHARS = 2_000
MAX_NOTIFICATION_KEY_CHARS = 512
MAX_SAFE_INTEGER = 9_007_199_254_740_991

_BIDI_CONTROLS = frozenset({
    0x061C,
    0x200E,
    0x200F,
    0x202A,
    0x202B,
    0x202C,
    0x202D,
    0x202E,
    0x2066,
    0x2067,
    0x2068,
    0x2069,
})
_APP_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_CONTEXT_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CONTEXT_DASHBOARD_ID = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_CONTEXT_PRESENTATION_BLOCKED_MESSAGES = {
    "clock_alert_active": (
        "The glasses display is busy with an active Clock alert."
    ),
    "assistant_presentation_active": (
        "The glasses display is busy with another assistant presentation."
    ),
}
_WINDOW_LINE = re.compile(
    r'^- (?P<window>[A-Za-z0-9._:-]{1,128}) — "(?P<title>.*)" '
    r'\(app: (?P<app>[a-z0-9][a-z0-9._-]{0,63})\)'
    r'(?: \[(?P<marks>foreground|pinned|foreground, pinned)\])?$'
)
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


class DeviceContractError(RuntimeError):
    """The negotiated phone identity or advertised schema drifted."""


class DeviceResultError(RuntimeError):
    """The phone returned a malformed, oversized, or error result."""


def _object_schema(
    properties: dict[str, Any],
    required: tuple[str, ...] = (),
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = list(required)
    return schema


# This is the current launcher-visible set.  A phone release that adds or
# removes an app deliberately changes the apps.* schema fingerprint, forcing a
# matching workflow-package release instead of silently expanding authority.
LAUNCHABLE_APP_IDS = (
    "agent-cockpit",
    "blocks",
    "calendar",
    "clock",
    "compass",
    "conversate",
    "evenhub-local-counter",
    "files",
    "freecell",
    "minesweeper",
    "music",
    "navigate",
    "notifications",
    "pinball",
    "settings",
    "universal-search",
    "weather",
    "work-tasks",
)

_APP_ID_SCHEMA = {"type": "string", "enum": list(LAUNCHABLE_APP_IDS)}
_EMPTY_SCHEMA = _object_schema({})
_FIXED_OPERATION_ID_SCHEMA = {
    "type": "string",
    "minLength": 1,
    "maxLength": 64,
}

_CONTEXT_SUMMARY_SCHEMA = _object_schema(
    {
        "primary": {"type": "string", "minLength": 1, "maxLength": 64},
        "secondary": {"type": "string", "minLength": 1, "maxLength": 96},
        "tone": {
            "type": "string",
            "enum": ["neutral", "good", "warning", "critical"],
        },
        "uncertainty": {
            "type": "string",
            "enum": ["exact", "estimated", "unknown"],
        },
    },
    ("primary", "uncertainty"),
)
_CONTEXT_ROW_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "label": {"type": "string", "minLength": 1, "maxLength": 40},
        "value": {"type": "string", "minLength": 1, "maxLength": 64},
        "tone": {
            "type": "string",
            "enum": ["neutral", "good", "warning", "critical"],
        },
        "destination": {"type": "string", "minLength": 1, "maxLength": 40},
        "scheduled_departure_ms": {"type": "integer", "minimum": 0},
        "expected_departure_ms": {"type": "integer", "minimum": 0},
        "status": {
            "type": "string",
            "enum": ["on_time", "delayed", "cancelled", "unknown", "departed"],
        },
        "platform": {"type": "string", "minLength": 1, "maxLength": 8},
    },
    ("id",),
)
_CONTEXT_BAR_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "label": {"type": "string", "minLength": 1, "maxLength": 24},
        "value": {"type": "number", "minimum": 0, "maximum": 1_000_000_000},
        "max": {"type": "number", "minimum": 0, "maximum": 1_000_000_000},
        "unit": {"type": "string", "minLength": 1, "maxLength": 8},
    },
    ("id", "label", "value", "max"),
)
_CONTEXT_SECTION_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "order": {"type": "integer", "minimum": 0, "maximum": 3},
        "type": {
            "type": "string",
            "enum": ["departures", "status_grid", "bar_chart", "list", "message"],
        },
        "title": {"type": "string", "minLength": 1, "maxLength": 40},
        "load_state": {
            "type": "string",
            "enum": ["pending", "ready", "empty", "error"],
        },
        "source_ids": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": {"type": "string", "minLength": 1, "maxLength": 64},
        },
        "uncertainty": {
            "type": "string",
            "enum": ["exact", "estimated", "unknown"],
        },
        "note": {"type": "string", "minLength": 1, "maxLength": 64},
        "error_code": {
            "type": "string",
            "enum": [
                "timeout",
                "offline",
                "permission",
                "unavailable",
                "invalid_data",
                "unknown",
            ],
        },
        "rows": {"type": "array", "maxItems": 12, "items": _CONTEXT_ROW_SCHEMA},
        "bars": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": _CONTEXT_BAR_SCHEMA,
        },
        "items": {
            "type": "array",
            "maxItems": 8,
            "items": {"type": "string", "minLength": 1, "maxLength": 96},
        },
        "body": {"type": "string", "minLength": 1, "maxLength": 160},
    },
    ("id", "order", "type", "load_state", "source_ids", "uncertainty"),
)
_CONTEXT_SOURCE_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "label": {"type": "string", "minLength": 1, "maxLength": 40},
        "attribution_id": {"type": "string", "enum": ["open_meteo_ukmo"]},
        "observed_at_ms": {"type": "integer", "minimum": 0},
        "stale_after_seconds": {
            "type": "integer",
            "minimum": 30,
            "maximum": 86_400,
        },
        "status": {
            "type": "string",
            "enum": ["current", "stale", "unavailable", "unknown"],
        },
    },
    ("id", "label", "stale_after_seconds", "status"),
)
_CONTEXT_LOCAL_ACTION_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "kind": {
            "type": "string",
            "enum": ["refresh", "section", "follow_up"],
        },
        "label": {"type": "string", "minLength": 1, "maxLength": 24},
        "enabled": {"type": "boolean"},
    },
    ("id", "kind", "label", "enabled"),
)
_CONTEXT_ANNOUNCEMENT_SCHEMA = _object_schema(
    {
        "id": {"type": "string", "minLength": 1, "maxLength": 64},
        "text": {"type": "string", "minLength": 1, "maxLength": 160},
        "policy": {"type": "string", "enum": ["once_when_useful"]},
    },
    ("id", "text", "policy"),
)
_CONTEXT_FINAL_SPEC_SCHEMA = _object_schema(
    {
        "version": {"type": "integer", "minimum": 2, "maximum": 2},
        "presentation_mode": {"type": "string", "enum": ["single", "deck"]},
        "dashboard_key": {"type": "string", "minLength": 1, "maxLength": 64},
        "title": {"type": "string", "minLength": 1, "maxLength": 48},
        "state": {
            "type": "string",
            "enum": ["ready", "empty", "error", "offline"],
        },
        "privacy": {
            "type": "string",
            "enum": ["public", "private", "sensitive"],
        },
        "summary": _CONTEXT_SUMMARY_SCHEMA,
        "sections": {
            "type": "array",
            "minItems": 1,
            "maxItems": 4,
            "items": _CONTEXT_SECTION_SCHEMA,
        },
        "sources": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": _CONTEXT_SOURCE_SCHEMA,
        },
        "local_actions": {
            "type": "array",
            "maxItems": 3,
            "items": _CONTEXT_LOCAL_ACTION_SCHEMA,
        },
        "announcement": _CONTEXT_ANNOUNCEMENT_SCHEMA,
        "ttl_seconds": {"type": "integer", "minimum": 30, "maximum": 3_600},
    },
    (
        "version",
        "dashboard_key",
        "title",
        "state",
        "privacy",
        "summary",
        "sections",
        "sources",
        "local_actions",
        "ttl_seconds",
    ),
)
_CONTEXT_PRESENT_SCHEMA = _object_schema(
    {
        "operation_id": {"type": "string", "minLength": 1, "maxLength": 64},
        "intent": {"type": "string", "minLength": 1, "maxLength": 240},
        "refresh_policy": _object_schema(
            {
                "mode": {"type": "string", "enum": ["manual", "on_visible"]},
                "min_interval_seconds": {
                    "type": "integer",
                    "minimum": 30,
                    "maximum": 86_400,
                },
            },
            ("mode", "min_interval_seconds"),
        ),
        "regeneration": {
            "type": "string",
            "enum": ["self_contained_intent", "current_turn_only"],
        },
        "spec": _CONTEXT_FINAL_SPEC_SCHEMA,
    },
    ("operation_id", "intent", "refresh_policy", "regeneration", "spec"),
)

EXPECTED_PHONE_SCHEMAS: dict[str, dict[str, Any]] = {
    "glasses.notify_result": _object_schema(
        {
            "operation_id": _FIXED_OPERATION_ID_SCHEMA,
            "text": {"type": "string", "maxLength": 160},
        },
        ("operation_id", "text"),
    ),
    "glasses.work_board.add_task": _object_schema(
        {
            "operation_id": _FIXED_OPERATION_ID_SCHEMA,
            "title": {"type": "string", "minLength": 1, "maxLength": 240},
            "lane": {
                "type": "string",
                "enum": ["inbox", "today", "doing"],
            },
        },
        ("operation_id", "title"),
    ),
    "glasses.clock.set_timer": _object_schema(
        {
            "operation_id": _FIXED_OPERATION_ID_SCHEMA,
            "duration_seconds": {
                "type": "integer",
                "minimum": 1,
                "maximum": 604_800,
            },
            "label": {"type": "string", "minLength": 1, "maxLength": 160},
        },
        ("operation_id", "duration_seconds"),
    ),
    "glasses.clock.set_alarm": _object_schema(
        {
            "operation_id": _FIXED_OPERATION_ID_SCHEMA,
            "local_time": {
                "type": "string",
                "minLength": 5,
                "maxLength": 5,
            },
            "date": {
                "type": "string",
                "minLength": 10,
                "maxLength": 10,
            },
            "repeat_days": {
                "type": "array",
                "minItems": 1,
                "maxItems": 7,
                "items": {
                    "type": "string",
                    "enum": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
                },
            },
            "label": {"type": "string", "minLength": 1, "maxLength": 160},
        },
        ("operation_id", "local_time"),
    ),
    "apps.launch": _object_schema({"app_id": _APP_ID_SCHEMA}, ("app_id",)),
    "apps.list_windows": _EMPTY_SCHEMA,
    "apps.focus_window": _object_schema(
        {"window_id": {"type": "string"}}, ("window_id",)
    ),
    "apps.close_window": _object_schema(
        {"window_id": {"type": "string"}}, ("window_id",)
    ),
    "apps.list_folders": _EMPTY_SCHEMA,
    "apps.move_to_folder": _object_schema(
        {"app_id": _APP_ID_SCHEMA, "folder": {"type": "string"}},
        ("app_id", "folder"),
    ),
    "apps.remove_from_folder": _object_schema(
        {"app_id": _APP_ID_SCHEMA}, ("app_id",)
    ),
    "apps.disband_folder": _object_schema(
        {"folder": {"type": "string"}}, ("folder",)
    ),
    "media.now_playing": _EMPTY_SCHEMA,
    "media.play_pause": _EMPTY_SCHEMA,
    "media.next": _EMPTY_SCHEMA,
    "nav.start_navigation": _object_schema(
        {
            "destination": {"type": "string"},
            "profile": {
                "type": "string",
                "enum": ["driving", "walking", "cycling"],
            },
        },
        ("destination",),
    ),
    "nav.stop_navigation": _EMPTY_SCHEMA,
    "nav.route_status": _EMPTY_SCHEMA,
    "notifications.list": _object_schema({"max": {"type": "number"}}),
    "notifications.dismiss": _object_schema(
        {"key": {"type": "string"}}, ("key",)
    ),
    "health.get_ring_data": _object_schema(
        {
            "end_date": {"type": "string"},
            "days": {"type": "integer", "minimum": 1, "maximum": 31},
            "include_hourly": {"type": "boolean"},
        }
    ),
    "calendar.list_events": _object_schema(
        {
            "within_hours": {"type": "number"},
            "max_events": {"type": "number"},
        }
    ),
    "glasses.context_dashboard.present": _CONTEXT_PRESENT_SCHEMA,
}


def _without_descriptions(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_descriptions(item)
            for key, item in value.items()
            if key != "description"
        }
    if isinstance(value, list):
        return [_without_descriptions(item) for item in value]
    return value


def schema_fingerprint(schema: Any) -> str:
    """Hash the complete structural JSON Schema, excluding prose only."""
    try:
        encoded = json.dumps(
            _without_descriptions(schema),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DeviceContractError("phone tool schema is not canonical JSON") from exc
    if len(encoded) > 16 * 1024:
        raise DeviceContractError("phone tool schema exceeds the contract limit")
    return hashlib.sha256(encoded).hexdigest()


# Literal pins make every phone schema change a reviewable compatibility event.
# Tests also recompute these from EXPECTED_PHONE_SCHEMAS.
PHONE_SCHEMA_FINGERPRINTS: dict[str, str] = {
    "glasses.notify_result": "2cc3b606fdeb30b0c874b9729f02e15efdfc4102f61a89ab031e8c00dace06f0",
    "glasses.work_board.add_task": "e616031ce95de59b7685b55c57ba3248b746a90372500985043f25691f8bbc2e",
    "glasses.clock.set_timer": "c95d203dd8d6a8cf47d0afb48d01100ab05169567b208ff3bbbde978b4c04181",
    "glasses.clock.set_alarm": "28f9f251128c3f4cb89321cef6e839bfdbb023576e37bb861ac14157a6e6d21a",
    "apps.launch": "3c32e24581c1457a847a6734a4fadcf007ce30ebba2eebfbfecb5495e3e790b6",
    "apps.list_windows": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "apps.focus_window": "ed2ba34ffb82b550811ba124101392a17c185e9219743ebba8a3db70e042954f",
    "apps.close_window": "ed2ba34ffb82b550811ba124101392a17c185e9219743ebba8a3db70e042954f",
    "apps.list_folders": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "apps.move_to_folder": "29663de5fd9159330fe48958b7385bdefd19a2b8a645327991422685ba0a1cdc",
    "apps.remove_from_folder": "3c32e24581c1457a847a6734a4fadcf007ce30ebba2eebfbfecb5495e3e790b6",
    "apps.disband_folder": "c01de9ef641e692dbca90f3ef9ba6af412083c8ee25a0e330ab79ec2bbb2fd9c",
    "media.now_playing": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "media.play_pause": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "media.next": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "nav.start_navigation": "1e96bdf82e0854c292e118540450864d2dfdc61625df5747920dbf0da040e9af",
    "nav.stop_navigation": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "nav.route_status": "99334726611ccf58a148b0814696bfa6fe08c1b2d027e946beccf5a74331c9aa",
    "notifications.list": "edcc271841209e420f5cc67f8693c085a9f5939351dc57765308ac48350026b6",
    "notifications.dismiss": "673957d8495ab1abc9fe59e5c14179fdb14e45af68f636ff52db653221562195",
    "health.get_ring_data": "003de86aa5236094399dfc2a79db11f86dcc164915ca6854e042debf1edb734b",
    "calendar.list_events": "038d0c80ca9bb5f2363a052654a042a45b0e779b285e8b893fa3ef162100c51f",
    "glasses.context_dashboard.present": "99ff3cd3d5b9f409f9ef54814c9a8eb60957985c6382666e4a059da6e26c653b",
}

RAW_PHONE_TOOL_NAMES = frozenset(PHONE_SCHEMA_FINGERPRINTS)


def validate_phone_identity(
    protocol_version: Any,
    server_name: Any,
    server_version: Any,
) -> None:
    if (
        protocol_version != PHONE_MCP_PROTOCOL_VERSION
        or server_name != PHONE_MCP_SERVER_NAME
        or server_version != PHONE_MCP_SERVER_VERSION
    ):
        raise DeviceContractError("connected phone MCP identity is incompatible")


def validate_phone_tool(tool: Any, expected_name: str) -> None:
    expected = PHONE_SCHEMA_FINGERPRINTS.get(expected_name)
    if expected is None:
        raise DeviceContractError("phone tool is outside the fixed contract")
    if (
        not isinstance(tool, dict)
        or set(tool) != {"name", "description", "inputSchema"}
        or tool.get("name") != expected_name
        or not isinstance(tool.get("description"), str)
        or not 1 <= len(tool["description"].encode("utf-8")) <= 8_192
        or not isinstance(tool.get("inputSchema"), dict)
        or schema_fingerprint(tool["inputSchema"]) != expected
    ):
        raise DeviceContractError(
            f"connected phone tool contract drifted: {expected_name}"
        )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-finite number")


def _safe_text(
    value: Any,
    *,
    max_chars: int = MAX_SUMMARY_CHARS,
    allow_newlines: bool = False,
) -> str:
    if not isinstance(value, str):
        raise DeviceResultError("phone result text is missing")
    try:
        normalized = unicodedata.normalize("NFC", value)
        encoded = normalized.encode("utf-8")
    except (TypeError, UnicodeError) as exc:
        raise DeviceResultError("phone result text is invalid") from exc
    if len(normalized) > max_chars or len(encoded) > MAX_PHONE_TEXT_BYTES:
        raise DeviceResultError("phone result text exceeds the contract limit")
    for character in normalized:
        codepoint = ord(character)
        category = unicodedata.category(character)
        if (
            codepoint in _BIDI_CONTROLS
            or 0xD800 <= codepoint <= 0xDFFF
            or category in {"Cs", "Zl", "Zp"}
            or (
                category == "Cc"
                and not (allow_newlines and character == "\n")
            )
        ):
            raise DeviceResultError("phone result text contains unsafe controls")
    return normalized


def phone_result_text(result: Any, *, allow_newlines: bool = False) -> str:
    """Accept only the phone MCP's exact one-text-block success envelope."""
    if not isinstance(result, dict) or set(result) != {"content", "isError"}:
        raise DeviceResultError("phone result envelope is malformed")
    content = result.get("content")
    if result.get("isError") is not False:
        raise DeviceResultError("phone rejected the fixed device workflow")
    if not isinstance(content, list) or len(content) != 1:
        raise DeviceResultError("phone result content is malformed")
    item = content[0]
    if (
        not isinstance(item, dict)
        or set(item) != {"type", "text"}
        or item.get("type") != "text"
    ):
        raise DeviceResultError("phone result content is malformed")
    return _safe_text(
        item.get("text"),
        max_chars=MAX_PHONE_TEXT_BYTES,
        allow_newlines=allow_newlines,
    )


def mutation_result(result: Any) -> dict[str, Any]:
    phone_result_text(result)
    return {"success": True, "state": "completed"}


def context_present_result(
    result: Any,
    *,
    expected_operation_id: str,
    expected_dashboard_key: str,
) -> dict[str, Any]:
    """Bind one exact phone frame acknowledgement to the requested deck.

    The phone receipt deliberately contains only phone-owned presentation
    identity.  This fixed native wrapper adds the exact operation and
    dashboard key that were handed to that same contracted call, preventing a
    caller from treating an unrelated or merely non-error MCP result as proof
    that its deck reached the lenses.
    """
    if (
        not isinstance(expected_operation_id, str)
        or _CONTEXT_ID.fullmatch(expected_operation_id) is None
        or not isinstance(expected_dashboard_key, str)
        or _CONTEXT_ID.fullmatch(expected_dashboard_key) is None
    ):
        raise DeviceResultError("context presentation identity is malformed")
    if (
        not isinstance(result, dict)
        or set(result) != {"content", "isError"}
        or type(result.get("isError")) is not bool
    ):
        raise DeviceResultError("context presentation receipt is malformed")
    content = result.get("content")
    if not isinstance(content, list) or len(content) != 1:
        raise DeviceResultError("context presentation receipt is malformed")
    item = content[0]
    if (
        not isinstance(item, dict)
        or set(item) != {"type", "text"}
        or item.get("type") != "text"
    ):
        raise DeviceResultError("context presentation receipt is malformed")
    encoded = _safe_text(item.get("text"), max_chars=1_024)
    if len(encoded.encode("utf-8")) > 1_024:
        raise DeviceResultError("context presentation receipt is oversized")
    try:
        value = json.loads(
            encoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError) as exc:
        raise DeviceResultError("context presentation receipt is malformed") from exc
    if result["isError"] is True:
        error_code = value.get("error_code") if isinstance(value, dict) else None
        if (
            not isinstance(value, dict)
            or set(value) != {"status", "error_code", "error"}
            or value.get("status") != "rejected"
            or not isinstance(error_code, str)
            or error_code not in _CONTEXT_PRESENTATION_BLOCKED_MESSAGES
            or value.get("error")
            != _CONTEXT_PRESENTATION_BLOCKED_MESSAGES[error_code]
        ):
            raise DeviceResultError("context presentation receipt is malformed")
        return {
            "success": False,
            "commit_state": "not_committed",
            "operation_id": expected_operation_id,
            "error_code": error_code,
            "error": value["error"],
        }

    required = {
        "status",
        "dashboard_id",
        "presentation_generation",
        "refresh_generation",
        "revision",
        "frame_id",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value.get("status")
        not in {"acknowledged", "historical_acknowledgement"}
        or not isinstance(value.get("dashboard_id"), str)
        or _CONTEXT_DASHBOARD_ID.fullmatch(value["dashboard_id"]) is None
        or type(value.get("presentation_generation")) is not int
        or value["presentation_generation"] != 1
        or type(value.get("refresh_generation")) is not int
        or value["refresh_generation"] != 1
        or type(value.get("revision")) is not int
        or value["revision"] != 1
        or type(value.get("frame_id")) is not int
        or not 1 <= value["frame_id"] <= MAX_SAFE_INTEGER
    ):
        raise DeviceResultError("context presentation receipt is malformed")
    return {
        "success": True,
        "receipt": {
            "status": value["status"],
            "operation_id": expected_operation_id,
            "dashboard_key": expected_dashboard_key,
            "dashboard_id": value["dashboard_id"],
            "presentation_generation": 1,
            "refresh_generation": 1,
            "revision": 1,
            "frame_id": value["frame_id"],
        },
    }


def windows_result(result: Any) -> dict[str, Any]:
    text = phone_result_text(result, allow_newlines=True)
    if not text:
        return {"success": True, "state": "empty", "windows": []}
    lines = text.split("\n")
    if len(lines) > 32:
        raise DeviceResultError("phone returned too many windows")
    windows: list[dict[str, Any]] = []
    for line in lines:
        match = _WINDOW_LINE.fullmatch(line)
        if match is None:
            raise DeviceResultError("phone window result drifted")
        marks = set((match.group("marks") or "").split(", ")) - {""}
        windows.append({
            "window_id": match.group("window"),
            "title": _safe_text(match.group("title"), max_chars=160),
            "app_id": match.group("app"),
            "foreground": "foreground" in marks,
            "pinned": "pinned" in marks,
        })
    return {"success": True, "state": "available", "windows": windows}


def folders_result(result: Any) -> dict[str, Any]:
    text = phone_result_text(result, allow_newlines=True)
    lines = text.split("\n") if text else []
    if not lines or len(lines) > 40:
        raise DeviceResultError("phone folder result drifted")
    folders: list[dict[str, Any]] = []
    ungrouped: list[str] | None = None
    for index, line in enumerate(lines):
        if ": " not in line:
            raise DeviceResultError("phone folder result drifted")
        label, members_text = line.rsplit(": ", 1)
        label = _safe_text(label, max_chars=24)
        members = [] if members_text == "(none)" else members_text.split(", ")
        if len(members) > len(LAUNCHABLE_APP_IDS) or any(
            _APP_ID.fullmatch(member) is None for member in members
        ):
            raise DeviceResultError("phone folder members drifted")
        if index == len(lines) - 1:
            if label != "Ungrouped":
                raise DeviceResultError("phone folder result omitted ungrouped apps")
            ungrouped = members
        else:
            folders.append({"name": label, "app_ids": members})
    if ungrouped is None:
        raise DeviceResultError("phone folder result omitted ungrouped apps")
    return {
        "success": True,
        "state": "available" if folders else "empty",
        "folders": folders,
        "ungrouped_app_ids": ungrouped,
    }


def media_result(result: Any) -> dict[str, Any]:
    summary = _safe_text(phone_result_text(result), max_chars=MAX_SUMMARY_CHARS)
    if summary == "Nothing is currently playing.":
        state = "idle"
    elif summary == "Notification access isn't granted, so media state is unavailable.":
        state = "unavailable"
    elif summary.startswith("Playing: ") and summary.endswith("."):
        state = "playing"
    elif summary.startswith("Paused: ") and summary.endswith("."):
        state = "paused"
    else:
        raise DeviceResultError("phone media result drifted")
    return {"success": True, "state": state, "summary": summary}


def navigation_result(result: Any) -> dict[str, Any]:
    summary = _safe_text(phone_result_text(result), max_chars=MAX_SUMMARY_CHARS)
    if summary in {"Navigation is not active.", "Navigation was not active."}:
        state = "inactive"
    elif summary == "Navigation stopped.":
        state = "stopped"
    elif summary.startswith("Arrived at "):
        state = "arrived"
    elif summary.startswith(("Navigating to ", "Next: ")):
        state = "active"
    else:
        raise DeviceResultError("phone navigation result drifted")
    return {"success": True, "state": state, "summary": summary}


def notifications_result(result: Any) -> dict[str, Any]:
    text = phone_result_text(result, allow_newlines=True)
    if text == "No current notifications.":
        return {"success": True, "state": "empty", "notifications": []}
    lines = text.split("\n")
    if not lines or len(lines) > 20:
        raise DeviceResultError("phone notification result drifted")
    notifications: list[dict[str, str]] = []
    for line in lines:
        if not line.startswith("- [") or "] " not in line or " (key: " not in line or not line.endswith(")"):
            raise DeviceResultError("phone notification result drifted")
        app_end = line.find("] ", 3)
        if app_end < 0:
            raise DeviceResultError("phone notification app is malformed")
        app_name = _safe_text(line[3:app_end], max_chars=120)
        summary_and_key = line[app_end + 2 :]
        summary, key = summary_and_key.rsplit(" (key: ", 1)
        key = key[:-1]
        notifications.append({
            "key": _safe_text(key, max_chars=MAX_NOTIFICATION_KEY_CHARS),
            "app": app_name,
            "summary": _safe_text(summary, max_chars=500),
        })
    return {
        "success": True,
        "state": "available",
        "notifications": notifications,
    }


_DAILY_FIELDS = frozenset({
    "dateKey",
    "updatedAtMs",
    "restingHr",
    "hrMin",
    "hrMax",
    "hrvAvg",
    "spo2Avg",
    "steps",
    "sleepScore",
    "sleepDurationMin",
    "sleepDeepMin",
    "sleepRemMin",
    "bodyTempC",
    "readinessScore",
})


def _finite_or_none(value: Any, minimum: float, maximum: float) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DeviceResultError("health metric is malformed")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise DeviceResultError("health metric is malformed") from exc
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise DeviceResultError("health metric is outside safe bounds")
    return value


def _real_date(value: Any) -> str:
    if not isinstance(value, str) or _DATE.fullmatch(value) is None:
        raise DeviceResultError("health date is malformed")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise DeviceResultError("health date is malformed") from exc


def health_result(result: Any) -> dict[str, Any]:
    encoded = phone_result_text(result)
    try:
        value = json.loads(
            encoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError) as exc:
        raise DeviceResultError("phone health JSON is malformed") from exc
    required = {
        "source",
        "schemaVersion",
        "generatedAtMs",
        "retentionDays",
        "range",
        "hourlyIncluded",
        "history",
    }
    if (
        not isinstance(value, dict)
        or not required <= set(value)
        or set(value) - required - {"activityTotals"}
        or value.get("source") != "hermes-g2-ring"
        or value.get("schemaVersion") != 1
        or value.get("hourlyIncluded") is not False
        or type(value.get("generatedAtMs")) is not int
        or not 0 < value["generatedAtMs"] <= MAX_SAFE_INTEGER
        or type(value.get("retentionDays")) is not int
        or not 1 <= value["retentionDays"] <= 31
    ):
        raise DeviceResultError("phone health envelope drifted")
    range_value = value.get("range")
    if not isinstance(range_value, dict) or set(range_value) != {"startDate", "endDate"}:
        raise DeviceResultError("phone health range drifted")
    start_date = _real_date(range_value.get("startDate"))
    end_date = _real_date(range_value.get("endDate"))
    if start_date > end_date:
        raise DeviceResultError("phone health range is invalid")
    history = value.get("history")
    if not isinstance(history, list) or len(history) > 31:
        raise DeviceResultError("phone health history is malformed")
    latest: dict[str, Any] | None = None
    previous_date = ""
    for row in history:
        if not isinstance(row, dict) or set(row) != _DAILY_FIELDS:
            raise DeviceResultError("phone health daily row drifted")
        row_date = _real_date(row.get("dateKey"))
        if row_date <= previous_date or not start_date <= row_date <= end_date:
            raise DeviceResultError("phone health daily ordering drifted")
        previous_date = row_date
        if type(row.get("updatedAtMs")) is not int or not 0 < row["updatedAtMs"] <= MAX_SAFE_INTEGER:
            raise DeviceResultError("phone health timestamp is malformed")
        latest = {
            "date": row_date,
            "steps": _finite_or_none(row.get("steps"), 0, 1_000_000),
            "resting_hr_bpm": _finite_or_none(row.get("restingHr"), 20, 300),
            "hrv_ms": _finite_or_none(row.get("hrvAvg"), 0, 1_000),
            "spo2_percent": _finite_or_none(row.get("spo2Avg"), 0, 100),
            "sleep_score": _finite_or_none(row.get("sleepScore"), 0, 100),
            "sleep_minutes": _finite_or_none(row.get("sleepDurationMin"), 0, 1_440),
            "readiness_score": _finite_or_none(row.get("readinessScore"), 0, 100),
        }
        # Validate metrics omitted from the coarse projection too.
        _finite_or_none(row.get("hrMin"), 20, 300)
        _finite_or_none(row.get("hrMax"), 20, 300)
        _finite_or_none(row.get("sleepDeepMin"), 0, 1_440)
        _finite_or_none(row.get("sleepRemMin"), 0, 1_440)
        _finite_or_none(row.get("bodyTempC"), 20, 50)
    output: dict[str, Any] = {
        "success": True,
        "state": "available" if latest is not None else "empty",
        "range": {"start_date": start_date, "end_date": end_date},
        "days_with_data": len(history),
        "latest": latest,
    }
    activity = value.get("activityTotals")
    if activity is not None:
        if not isinstance(activity, dict) or set(activity) != {
            "dateKey", "totalSteps", "activeCalories", "totalCalories", "restingCalories"
        }:
            raise DeviceResultError("phone activity summary drifted")
        output["activity_today"] = {
            "date": _real_date(activity.get("dateKey")),
            "steps": _finite_or_none(activity.get("totalSteps"), 0, 1_000_000),
            "active_calories": _finite_or_none(activity.get("activeCalories"), 0, 100_000),
            "total_calories": _finite_or_none(activity.get("totalCalories"), 0, 100_000),
        }
        _finite_or_none(activity.get("restingCalories"), 0, 100_000)
    return output


def calendar_result(result: Any) -> dict[str, Any]:
    text = phone_result_text(result, allow_newlines=True)
    if text == "No upcoming events in that window.":
        return {"success": True, "state": "empty", "events": []}
    lines = text.split("\n")
    if not lines or len(lines) > 20 or any(not line.startswith("- ") for line in lines):
        raise DeviceResultError("phone calendar result drifted")
    events = [_safe_text(line[2:], max_chars=600) for line in lines]
    return {"success": True, "state": "available", "events": events}
