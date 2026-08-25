"""Private fixed-route handlers for the standalone G2 workflow MCP relay."""

from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import re
import threading
import time
import unicodedata
from datetime import date
from enum import Enum
from typing import Any

from . import runtime
from .reminder_scheduler import (
    ReminderCapacityError,
    ReminderConflictError,
    ReminderInputError,
    ReminderStoreWriteError,
)


logger = logging.getLogger(__name__)


_NOTIFY_RESULT_PHONE_TOOL = "glasses.notify_result"
_NOTIFY_OPERATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_NOTIFY_TEXT_MAX_CHARS = 160
_NOTIFY_RECEIPT_STATUSES = frozenset({
    "queued",
    "acknowledged",
    "historical_acknowledgement",
})
_NOTIFY_RECEIPT_MAX_JSON_BYTES = 160
_NOTIFY_RECEIPT_ERROR = (
    "glasses notification did not return an exact acknowledgement receipt"
)
_NOTIFY_NOT_COMMITTED_ERROR = (
    "glasses notification was unavailable before phone handoff"
)
_NOTIFY_OUTCOME_UNKNOWN_ERROR = (
    "glasses notification outcome is unknown after phone handoff"
)

_REMINDER_OPERATION_ID = _NOTIFY_OPERATION_ID
_REMINDER_SCHEDULE_MAX_CHARS = 128
_REMINDER_CREATE_ERROR = "G2 reminder could not be scheduled safely"

_WORK_TASK_PHONE_TOOL = "glasses.work_board.add_task"
_WORK_TASK_OPERATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_WORK_TASK_ID = re.compile(r"^wt_[a-f0-9]{32}$")
_WORK_TASK_LANES = frozenset({"inbox", "today", "doing"})
_WORK_TASK_TITLE_MAX_SCALARS = 120
_WORK_TASK_TITLE_MAX_BYTES = 480
_WORK_TASK_RECEIPT_MAX_JSON_BYTES = 320
_WORK_TASK_MAX_SAFE_REVISION = 9_007_199_254_740_991
_WORK_TASK_RECEIPT_STATUSES = frozenset({
    "acknowledged",
    "historical_acknowledgement",
})
_WORK_TASK_RECEIPT_ERROR = (
    "Work Tasks did not return an exact acknowledgement receipt"
)

_CLOCK_TIMER_PHONE_TOOL = "glasses.clock.set_timer"
_CLOCK_ALARM_PHONE_TOOL = "glasses.clock.set_alarm"
_CLOCK_OPERATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CLOCK_ITEM_ID = re.compile(r"^clk_[a-f0-9]{32}$")
_CLOCK_LOCAL_TIME = re.compile(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
_CLOCK_LOCAL_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_CLOCK_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_CLOCK_WEEKDAY_SET = frozenset(_CLOCK_WEEKDAYS)
_CLOCK_MAX_DURATION_SECONDS = 604_800
_CLOCK_LABEL_MAX_SCALARS = 80
_CLOCK_LABEL_MAX_BYTES = 320
_CLOCK_RECEIPT_MAX_JSON_BYTES = 640
_CLOCK_MAX_SAFE_REVISION = 9_007_199_254_740_991
_CLOCK_RECEIPT_STATUSES = frozenset({
    "acknowledged",
    "historical_acknowledgement",
})
_CLOCK_RECEIPT_ERROR = "Clock did not return an exact acknowledgement receipt"
_CLOCK_NOT_COMMITTED_ERROR = "Clock was unavailable before phone handoff"
_CLOCK_OUTCOME_UNKNOWN_ERROR = "Clock scheduling outcome is unknown after phone handoff"
_CONTEXT_PRESENT_PHONE_TOOL = "glasses.context_dashboard.present"
_CONTEXT_OPERATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CONTEXT_SPEC_REQUIRED_KEYS = frozenset({
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
})
_CONTEXT_SPEC_OPTIONAL_KEYS = frozenset({"presentation_mode", "announcement"})
_BIDI_CONTROL_CODEPOINTS = frozenset({
    0x061C,  # Arabic letter mark
    0x200E,  # left-to-right mark
    0x200F,  # right-to-left mark
    0x202A,  # left-to-right embedding
    0x202B,  # right-to-left embedding
    0x202C,  # pop directional formatting
    0x202D,  # left-to-right override
    0x202E,  # right-to-left override
    0x2066,  # left-to-right isolate
    0x2067,  # right-to-left isolate
    0x2068,  # first-strong isolate
    0x2069,  # pop directional isolate
})
_TRAIN_CRS = re.compile(r"^[A-Z0-9]{3}$")
_TRAIN_READ_TIMEOUT_SECONDS = 25.0
_WEATHER_READ_TIMEOUT_SECONDS = 15.0


class _PublicReadStage(Enum):
    """Fixed diagnostics for public-data workflows; values contain no payload."""

    TRAIN_ENTERED = "train.entered"
    TRAIN_PLATFORM_DENIED = "train.platform_denied"
    TRAIN_REQUEST_INVALID = "train.request_invalid"
    TRAIN_AUTHORIZATION_FAILED = "train.authorization_failed"
    TRAIN_AUTHORIZED = "train.authorized"
    TRAIN_READER_IMPORT_FAILED = "train.reader_import_failed"
    TRAIN_READER_STARTED = "train.reader_started"
    TRAIN_READER_COMPLETED = "train.reader_completed"
    TRAIN_READER_FAILED = "train.reader_failed"
    TRAIN_READER_UNEXPECTED = "train.reader_unexpected"
    TRAIN_TURN_REVALIDATION_FAILED = "train.turn_revalidation_failed"
    TRAIN_TURN_REVALIDATED = "train.turn_revalidated"
    TRAIN_CANCELLED = "train.cancelled"
    TRAIN_COMPLETED = "train.completed"
    WEATHER_ENTERED = "weather.entered"
    WEATHER_PLATFORM_DENIED = "weather.platform_denied"
    WEATHER_REQUEST_INVALID = "weather.request_invalid"
    WEATHER_READER_IMPORT_FAILED = "weather.reader_import_failed"
    WEATHER_AUTHORIZATION_FAILED = "weather.authorization_failed"
    WEATHER_AUTHORIZED = "weather.authorized"
    WEATHER_READER_STARTED = "weather.reader_started"
    WEATHER_READER_COMPLETED = "weather.reader_completed"
    WEATHER_LOCATION_AMBIGUOUS = "weather.location_ambiguous"
    WEATHER_LOCATION_NOT_FOUND = "weather.location_not_found"
    WEATHER_INPUT_INVALID = "weather.input_invalid"
    WEATHER_READER_FAILED = "weather.reader_failed"
    WEATHER_READER_UNEXPECTED = "weather.reader_unexpected"
    WEATHER_TURN_REVALIDATION_FAILED = "weather.turn_revalidation_failed"
    WEATHER_TURN_REVALIDATED = "weather.turn_revalidated"
    WEATHER_RESULT_MISSING = "weather.result_missing"
    WEATHER_CANCELLED = "weather.cancelled"
    WEATHER_COMPLETED = "weather.completed"


def _log_public_read_stage(stage: _PublicReadStage, *, failed: bool = False) -> None:
    """Log one allowlisted stage without formatting request or exception data."""

    if type(stage) is not _PublicReadStage:
        raise TypeError("public-data diagnostic stage must be allowlisted")
    logger.log(
        logging.WARNING if failed else logging.INFO,
        "G2 public-data workflow stage=%s",
        stage.value,
    )


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON object key")
        value[key] = item
    return value


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


def _normalize_notify_text(value: Any) -> str | None:
    """Mirror the phone's inert one-line display-text boundary."""
    if not isinstance(value, str):
        return None
    for character in value:
        codepoint = ord(character)
        category = unicodedata.category(character)
        if (
            codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or 0xD800 <= codepoint <= 0xDFFF
            or codepoint in _BIDI_CONTROL_CODEPOINTS
            or category in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    try:
        text = unicodedata.normalize("NFC", value).strip()
    except (TypeError, UnicodeError):
        return None
    if (
        not text
        or len(text) > _NOTIFY_TEXT_MAX_CHARS
        or any(marker in text for marker in ("<", ">", "`"))
        or re.search(r"(?:https?://|www\.)", text, re.IGNORECASE)
    ):
        return None
    return text


def _normalize_reminder_schedule(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    schedule = value.strip()
    if not schedule or len(schedule) > _REMINDER_SCHEDULE_MAX_CHARS:
        return None
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in schedule):
        return None
    return schedule


def _current_session_platform() -> str:
    try:
        from gateway.session_context import get_session_env

        return str(get_session_env("HERMES_SESSION_PLATFORM") or "")
    except Exception:
        return ""


def _decode_notify_receipt(
    result: Any,
    *,
    expected_operation_id: str,
) -> dict[str, str] | None:
    """Accept only the exact MCP and phone receipt contract for notify_result."""
    if (
        not isinstance(result, dict)
        or set(result) != {"content", "isError"}
        or result.get("isError") is not False
    ):
        return None
    content = result.get("content")
    if not isinstance(content, list) or len(content) != 1:
        return None
    item = content[0]
    if (
        not isinstance(item, dict)
        or set(item) != {"type", "text"}
        or item.get("type") != "text"
    ):
        return None
    encoded_receipt = item.get("text")
    if not isinstance(encoded_receipt, str) or not encoded_receipt:
        return None
    try:
        if len(encoded_receipt.encode("utf-8")) > _NOTIFY_RECEIPT_MAX_JSON_BYTES:
            return None
        receipt = json.loads(
            encoded_receipt,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, UnicodeError):
        return None
    if not isinstance(receipt, dict) or set(receipt) != {"status", "operation_id"}:
        return None
    status = receipt.get("status")
    operation_id = receipt.get("operation_id")
    if (
        not isinstance(status, str)
        or status not in _NOTIFY_RECEIPT_STATUSES
        or not isinstance(operation_id, str)
        or operation_id != expected_operation_id
    ):
        return None
    return {"status": status, "operation_id": operation_id}


def _normalize_work_task_title(value: Any) -> str | None:
    """Return a trimmed NFC task title containing only safe, one-line scalars."""
    if not isinstance(value, str):
        return None
    for character in value:
        codepoint = ord(character)
        category = unicodedata.category(character)
        if (
            codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or 0xD800 <= codepoint <= 0xDFFF
            or codepoint in _BIDI_CONTROL_CODEPOINTS
            or category in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    try:
        title = unicodedata.normalize("NFC", value).strip()
        encoded = title.encode("utf-8")
    except (TypeError, UnicodeError):
        return None
    if (
        not title
        or len(title) > _WORK_TASK_TITLE_MAX_SCALARS
        or len(encoded) > _WORK_TASK_TITLE_MAX_BYTES
    ):
        return None
    return title


def _normalize_clock_label(value: Any) -> str | None:
    """Return one inert, bounded NFC Clock label or None when invalid."""
    if not isinstance(value, str):
        return None
    for character in value:
        codepoint = ord(character)
        category = unicodedata.category(character)
        if (
            codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or 0xD800 <= codepoint <= 0xDFFF
            or codepoint in _BIDI_CONTROL_CODEPOINTS
            or category in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    try:
        label = unicodedata.normalize("NFC", value).strip()
        encoded = label.encode("utf-8")
    except (TypeError, UnicodeError):
        return None
    if (
        not label
        or len(label) > _CLOCK_LABEL_MAX_SCALARS
        or len(encoded) > _CLOCK_LABEL_MAX_BYTES
    ):
        return None
    return label


def _normalize_clock_date(value: Any) -> str | None:
    if not isinstance(value, str) or not _CLOCK_LOCAL_DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _normalize_clock_repeat_days(value: Any) -> list[str] | None:
    if not isinstance(value, list) or not 1 <= len(value) <= len(_CLOCK_WEEKDAYS):
        return None
    if any(not isinstance(day, str) or day not in _CLOCK_WEEKDAY_SET for day in value):
        return None
    if len(set(value)) != len(value):
        return None
    selected = set(value)
    return [day for day in _CLOCK_WEEKDAYS if day in selected]


def _decode_clock_receipt(
    result: Any,
    *,
    expected_operation_id: str,
    expected_kind: str,
    expected_duration_seconds: int | None = None,
    expected_local_time: str | None = None,
    expected_date: str | None = None,
    expected_repeat_days: list[str] | None = None,
    allow_resolved_date: bool = False,
) -> dict[str, Any] | None:
    """Accept only the exact durable Clock receipt for the requested schedule."""
    if (
        not isinstance(result, dict)
        or set(result) != {"content", "isError"}
        or result.get("isError") is not False
    ):
        return None
    content = result.get("content")
    if not isinstance(content, list) or len(content) != 1:
        return None
    item = content[0]
    if (
        not isinstance(item, dict)
        or set(item) != {"type", "text"}
        or item.get("type") != "text"
    ):
        return None
    encoded_receipt = item.get("text")
    if not isinstance(encoded_receipt, str) or not encoded_receipt:
        return None
    try:
        if len(encoded_receipt.encode("utf-8")) > _CLOCK_RECEIPT_MAX_JSON_BYTES:
            return None
        receipt = json.loads(
            encoded_receipt,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, UnicodeError):
        return None

    common_keys = {
        "status",
        "operation_id",
        "item_id",
        "kind",
        "next_fire_at_ms",
        "clock_revision",
    }
    kind_keys = (
        {"duration_seconds"}
        if expected_kind == "timer"
        else {"local_time", "date", "repeat_days"}
    )
    if not isinstance(receipt, dict) or set(receipt) != common_keys | kind_keys:
        return None

    status = receipt.get("status")
    operation_id = receipt.get("operation_id")
    item_id = receipt.get("item_id")
    kind = receipt.get("kind")
    next_fire_at_ms = receipt.get("next_fire_at_ms")
    clock_revision = receipt.get("clock_revision")
    if (
        not isinstance(status, str)
        or status not in _CLOCK_RECEIPT_STATUSES
        or operation_id != expected_operation_id
        or not isinstance(item_id, str)
        or not _CLOCK_ITEM_ID.fullmatch(item_id)
        or kind != expected_kind
        or type(next_fire_at_ms) is not int
        or not 1 <= next_fire_at_ms <= _CLOCK_MAX_SAFE_REVISION
        or type(clock_revision) is not int
        or not 1 <= clock_revision <= _CLOCK_MAX_SAFE_REVISION
    ):
        return None

    if expected_kind == "timer":
        if (
            type(receipt.get("duration_seconds")) is not int
            or receipt.get("duration_seconds") != expected_duration_seconds
        ):
            return None
    else:
        receipt_date = receipt.get("date")
        if (
            receipt.get("local_time") != expected_local_time
            or receipt.get("repeat_days") != (expected_repeat_days or [])
            or (
                _normalize_clock_date(receipt_date) is None
                if allow_resolved_date
                else receipt_date != expected_date
            )
        ):
            return None
    return receipt


def _decode_work_task_receipt(
    result: Any,
    *,
    expected_operation_id: str,
    expected_lane: str,
) -> dict[str, Any] | None:
    """Accept only the exact MCP and phone receipt contract for a task add."""
    if (
        not isinstance(result, dict)
        or set(result) != {"content", "isError"}
        or result.get("isError") is not False
    ):
        return None
    content = result.get("content")
    if not isinstance(content, list) or len(content) != 1:
        return None
    item = content[0]
    if (
        not isinstance(item, dict)
        or set(item) != {"type", "text"}
        or item.get("type") != "text"
    ):
        return None
    encoded_receipt = item.get("text")
    if not isinstance(encoded_receipt, str) or not encoded_receipt:
        return None
    try:
        if len(encoded_receipt.encode("utf-8")) > _WORK_TASK_RECEIPT_MAX_JSON_BYTES:
            return None
        receipt = json.loads(
            encoded_receipt,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, UnicodeError):
        return None
    if not isinstance(receipt, dict) or set(receipt) != {
        "status",
        "operation_id",
        "task_id",
        "lane",
        "board_revision",
    }:
        return None
    status = receipt.get("status")
    operation_id = receipt.get("operation_id")
    task_id = receipt.get("task_id")
    lane = receipt.get("lane")
    board_revision = receipt.get("board_revision")
    if (
        not isinstance(status, str)
        or status not in _WORK_TASK_RECEIPT_STATUSES
        or not isinstance(operation_id, str)
        or operation_id != expected_operation_id
        or not isinstance(task_id, str)
        or not _WORK_TASK_ID.fullmatch(task_id)
        or not isinstance(lane, str)
        or lane != expected_lane
        or type(board_revision) is not int
        or not 1 <= board_revision <= _WORK_TASK_MAX_SAFE_REVISION
    ):
        return None
    return {
        "status": status,
        "operation_id": operation_id,
        "task_id": task_id,
        "lane": lane,
        "board_revision": board_revision,
    }


async def _handle_notify_result(args: dict[str, Any], **_kwargs: Any) -> str:
    """Deliver one bounded final result through the fixed phone tool.

    This intentionally does not accept a phone-tool name or a nested arguments
    object. The adapter remains the authority for proactive policy, its exact
    allowlists, connection liveness, and the phone's strict delivery receipt.
    """
    if not isinstance(args, dict) or set(args) != {"operation_id", "text"}:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id and text are the only accepted fields",
        })
    operation_id = args.get("operation_id")
    text = _normalize_notify_text(args.get("text"))
    if not isinstance(operation_id, str) or not _NOTIFY_OPERATION_ID.fullmatch(operation_id):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id must be 1-64 ASCII letters, digits, dot, underscore, or hyphen",
        })
    if (
        text is None
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": f"text must be non-empty and at most {_NOTIFY_TEXT_MAX_CHARS} characters",
        })
    arguments = {"operation_id": operation_id, "text": text}
    try:
        from .device_voice_contract import PHONE_SCHEMA_FINGERPRINTS

        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_notify_result(
                arguments,
                schema_fingerprint=PHONE_SCHEMA_FINGERPRINTS[
                    _NOTIFY_RESULT_PHONE_TOOL
                ],
            )
        )
        receipt = _decode_notify_receipt(
            result,
            expected_operation_id=operation_id,
        )
        if receipt is None:
            return json.dumps({
                "success": False,
                "commit_state": "unknown",
                "operation_id": operation_id,
                "error": _NOTIFY_RECEIPT_ERROR,
            })
        return json.dumps({"success": True, "receipt": receipt})
    except Exception as exc:
        commit_state = (
            "unknown" if getattr(exc, "commit_state", None) == "unknown"
            else "not_committed"
        )
        response = {
            "success": False,
            "commit_state": commit_state,
            "error": (
                _NOTIFY_OUTCOME_UNKNOWN_ERROR
                if commit_state == "unknown"
                else _NOTIFY_NOT_COMMITTED_ERROR
            ),
        }
        if commit_state == "unknown":
            response["operation_id"] = operation_id
        return json.dumps(response)


async def _handle_schedule_reminder(args: dict[str, Any], **_kwargs: Any) -> str:
    """Durably enqueue one deterministic reminder during the exact active turn."""
    if _current_session_platform() != "g2":
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "G2 reminders are available only during an active G2 turn",
        })
    try:
        active_authorization = await _authorize_active_g2_read()
    except Exception:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "G2 reminders require the exact current phone turn",
        })
    if not isinstance(args, dict) or set(args) != {"operation_id", "schedule", "text"}:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id, schedule, and text are the only accepted fields",
        })
    operation_id = args.get("operation_id")
    schedule = _normalize_reminder_schedule(args.get("schedule"))
    text = _normalize_notify_text(args.get("text"))
    if not isinstance(operation_id, str) or not _REMINDER_OPERATION_ID.fullmatch(operation_id):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id must be 1-64 ASCII letters, digits, dot, underscore, or hyphen",
        })
    if schedule is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "schedule must be one bounded one-shot Hermes schedule",
        })
    if text is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "text must be one bounded inert reminder line",
        })

    # No await is permitted between this last-moment authority check and the
    # synchronous durable outbox mutation.  Trusted relay metadata identifies
    # only a candidate turn; the live adapter remains the final authority.
    adapter = runtime.get_active()
    try:
        current_authorization = (
            adapter.authorize_active_g2_turn() if adapter is not None else None
        )
    except Exception:
        current_authorization = None
    if current_authorization != active_authorization:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "G2 reminder turn authority expired before scheduling",
        })

    try:
        result = adapter.schedule_g2_reminder(operation_id, schedule, text)
        return json.dumps(result)
    except (ReminderInputError, ReminderConflictError, ReminderCapacityError) as exc:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": str(exc),
        })
    except ReminderStoreWriteError:
        return json.dumps({
            "success": False,
            "commit_state": "unknown",
            "operation_id": operation_id,
            "error": _REMINDER_CREATE_ERROR,
        })
    except Exception:
        return json.dumps({
            "success": False,
            "commit_state": "unknown",
            "operation_id": operation_id,
            "error": _REMINDER_CREATE_ERROR,
        })


async def _handle_work_task_add(args: dict[str, Any], **_kwargs: Any) -> str:
    """Add one local day-job task through a fixed active-turn phone tool."""
    if not isinstance(args, dict) or not {"operation_id", "title"} <= set(args) or not set(args) <= {
        "operation_id",
        "title",
        "lane",
    }:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id, title, and optional lane are the only accepted fields",
        })
    operation_id = args.get("operation_id")
    if (
        not isinstance(operation_id, str)
        or not _WORK_TASK_OPERATION_ID.fullmatch(operation_id)
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id must be 1-64 ASCII letters, digits, dot, underscore, or hyphen",
        })
    title = _normalize_work_task_title(args.get("title"))
    if title is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": (
                "title must be one safe line of 1-120 Unicode scalars and at most "
                f"{_WORK_TASK_TITLE_MAX_BYTES} UTF-8 bytes"
            ),
        })
    lane = args.get("lane", "inbox")
    if not isinstance(lane, str) or lane not in _WORK_TASK_LANES:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "lane must be inbox, today, or doing; omit it for inbox",
        })

    arguments: dict[str, Any] = {
        "operation_id": operation_id,
        "title": title,
    }
    if "lane" in args:
        arguments["lane"] = lane
    try:
        from .device_voice_contract import PHONE_SCHEMA_FINGERPRINTS

        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_glasses_tool(
                _WORK_TASK_PHONE_TOOL,
                arguments,
                schema_fingerprint=PHONE_SCHEMA_FINGERPRINTS[
                    _WORK_TASK_PHONE_TOOL
                ],
            )
        )
        receipt = _decode_work_task_receipt(
            result,
            expected_operation_id=operation_id,
            expected_lane=lane,
        )
        if receipt is None:
            return json.dumps({
                "success": False,
                "commit_state": "unknown",
                "operation_id": operation_id,
                "error": _WORK_TASK_RECEIPT_ERROR,
            })
        return json.dumps({"success": True, "receipt": receipt})
    except Exception as exc:
        commit_state = (
            "unknown" if getattr(exc, "commit_state", None) == "unknown"
            else "not_committed"
        )
        response = {
            "success": False,
            "commit_state": commit_state,
            "error": str(exc),
        }
        if commit_state == "unknown":
            response["operation_id"] = operation_id
        return json.dumps(response)


def _clock_failure(operation_id: str, exc: Exception) -> str:
    commit_state = (
        "unknown" if getattr(exc, "commit_state", None) == "unknown"
        else "not_committed"
    )
    response = {
        "success": False,
        "commit_state": commit_state,
        "error": (
            _CLOCK_OUTCOME_UNKNOWN_ERROR
            if commit_state == "unknown"
            else _CLOCK_NOT_COMMITTED_ERROR
        ),
    }
    if commit_state == "unknown":
        response["operation_id"] = operation_id
    return json.dumps(response)


async def _handle_clock_set_timer(args: dict[str, Any], **_kwargs: Any) -> str:
    """Create one durable phone-owned Clock timer during the exact G2 turn."""
    if _current_session_platform() != "g2":
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "Clock timers are available only during an active G2 turn",
        })
    if (
        not isinstance(args, dict)
        or not {"operation_id", "duration_seconds"} <= set(args)
        or not set(args) <= {"operation_id", "duration_seconds", "label"}
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id, duration_seconds, and optional label are the only accepted fields",
        })
    operation_id = args.get("operation_id")
    duration_seconds = args.get("duration_seconds")
    if not isinstance(operation_id, str) or not _CLOCK_OPERATION_ID.fullmatch(operation_id):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id must be 1-64 ASCII letters, digits, dot, underscore, or hyphen",
        })
    if (
        type(duration_seconds) is not int
        or not 1 <= duration_seconds <= _CLOCK_MAX_DURATION_SECONDS
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": f"duration_seconds must be an integer from 1 to {_CLOCK_MAX_DURATION_SECONDS}",
        })
    arguments: dict[str, Any] = {
        "operation_id": operation_id,
        "duration_seconds": duration_seconds,
    }
    if "label" in args:
        label = _normalize_clock_label(args.get("label"))
        if label is None:
            return json.dumps({
                "success": False,
                "commit_state": "not_committed",
                "error": "label must be one inert line of at most 80 Unicode characters",
            })
        arguments["label"] = label
    try:
        from .device_voice_contract import PHONE_SCHEMA_FINGERPRINTS

        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_glasses_tool(
                _CLOCK_TIMER_PHONE_TOOL,
                arguments,
                schema_fingerprint=PHONE_SCHEMA_FINGERPRINTS[
                    _CLOCK_TIMER_PHONE_TOOL
                ],
            )
        )
        receipt = _decode_clock_receipt(
            result,
            expected_operation_id=operation_id,
            expected_kind="timer",
            expected_duration_seconds=duration_seconds,
        )
        if receipt is None:
            return json.dumps({
                "success": False,
                "commit_state": "unknown",
                "operation_id": operation_id,
                "error": _CLOCK_RECEIPT_ERROR,
            })
        return json.dumps({"success": True, "receipt": receipt})
    except Exception as exc:
        return _clock_failure(operation_id, exc)


async def _handle_clock_set_alarm(args: dict[str, Any], **_kwargs: Any) -> str:
    """Create one durable phone-owned Clock alarm during the exact G2 turn."""
    if _current_session_platform() != "g2":
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "Clock alarms are available only during an active G2 turn",
        })
    accepted = {"operation_id", "local_time", "date", "repeat_days", "label"}
    if (
        not isinstance(args, dict)
        or not {"operation_id", "local_time"} <= set(args)
        or not set(args) <= accepted
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id, local_time, optional date or repeat_days, and optional label are the only accepted fields",
        })
    operation_id = args.get("operation_id")
    local_time = args.get("local_time")
    if not isinstance(operation_id, str) or not _CLOCK_OPERATION_ID.fullmatch(operation_id):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id must be 1-64 ASCII letters, digits, dot, underscore, or hyphen",
        })
    if not isinstance(local_time, str) or not _CLOCK_LOCAL_TIME.fullmatch(local_time):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "local_time must use exact 24-hour HH:MM format",
        })
    has_date = "date" in args
    has_repeat_days = "repeat_days" in args
    if has_date and has_repeat_days:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "date and repeat_days are mutually exclusive",
        })
    local_date: str | None = None
    repeat_days: list[str] = []
    if has_date:
        local_date = _normalize_clock_date(args.get("date"))
        if local_date is None:
            return json.dumps({
                "success": False,
                "commit_state": "not_committed",
                "error": "date must be one real calendar date in YYYY-MM-DD format",
            })
    elif has_repeat_days:
        normalized_days = _normalize_clock_repeat_days(args.get("repeat_days"))
        if normalized_days is None:
            return json.dumps({
                "success": False,
                "commit_state": "not_committed",
                "error": "repeat_days must contain one to seven unique weekday names",
            })
        repeat_days = normalized_days

    arguments: dict[str, Any] = {
        "operation_id": operation_id,
        "local_time": local_time,
    }
    if local_date is not None:
        arguments["date"] = local_date
    if repeat_days:
        arguments["repeat_days"] = repeat_days
    if "label" in args:
        label = _normalize_clock_label(args.get("label"))
        if label is None:
            return json.dumps({
                "success": False,
                "commit_state": "not_committed",
                "error": "label must be one inert line of at most 80 Unicode characters",
            })
        arguments["label"] = label
    try:
        from .device_voice_contract import PHONE_SCHEMA_FINGERPRINTS

        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_glasses_tool(
                _CLOCK_ALARM_PHONE_TOOL,
                arguments,
                schema_fingerprint=PHONE_SCHEMA_FINGERPRINTS[
                    _CLOCK_ALARM_PHONE_TOOL
                ],
            )
        )
        receipt = _decode_clock_receipt(
            result,
            expected_operation_id=operation_id,
            expected_kind="alarm",
            expected_local_time=local_time,
            expected_date=local_date,
            expected_repeat_days=repeat_days,
            allow_resolved_date=not has_date and not has_repeat_days,
        )
        if receipt is None:
            return json.dumps({
                "success": False,
                "commit_state": "unknown",
                "operation_id": operation_id,
                "error": _CLOCK_RECEIPT_ERROR,
            })
        return json.dumps({"success": True, "receipt": receipt})
    except Exception as exc:
        return _clock_failure(operation_id, exc)


async def _authorize_active_g2_read(expected: Any = None) -> Any:
    async def authorize(adapter):
        authorization = adapter.authorize_active_g2_turn()
        if expected is not None and authorization != expected:
            raise PermissionError("G2 turn changed during live read")
        return authorization

    return await runtime.call_active(authorize)


async def _handle_train_departures(args: dict[str, Any], **_kwargs: Any) -> str:
    """Read one generated National Rail route and return typed departures only."""
    _log_public_read_stage(_PublicReadStage.TRAIN_ENTERED)
    if _current_session_platform() != "g2":
        _log_public_read_stage(
            _PublicReadStage.TRAIN_PLATFORM_DENIED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "National Rail departures are available only during an active G2 turn",
        })
    if not isinstance(args, dict) or set(args) != {"origin_crs", "destination_crs"}:
        _log_public_read_stage(
            _PublicReadStage.TRAIN_REQUEST_INVALID,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "origin_crs and destination_crs are the only accepted fields",
        })
    origin = args.get("origin_crs")
    destination = args.get("destination_crs")
    if (
        not isinstance(origin, str)
        or not isinstance(destination, str)
        or not _TRAIN_CRS.fullmatch(origin)
        or not _TRAIN_CRS.fullmatch(destination)
        or origin == destination
    ):
        _log_public_read_stage(
            _PublicReadStage.TRAIN_REQUEST_INVALID,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "station codes must be two distinct uppercase three-character CRS codes",
        })
    try:
        authorization = await _authorize_active_g2_read()
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.TRAIN_AUTHORIZATION_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "the exact G2 turn is no longer active",
        })
    _log_public_read_stage(_PublicReadStage.TRAIN_AUTHORIZED)
    try:
        from .public_web import TrainReadError, read_train_departures
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.TRAIN_READER_IMPORT_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "the isolated National Rail reader is unavailable",
        })

    cancelled = threading.Event()
    deadline = time.monotonic() + _TRAIN_READ_TIMEOUT_SECONDS
    _log_public_read_stage(_PublicReadStage.TRAIN_READER_STARTED)
    try:
        departures = await asyncio.wait_for(
            asyncio.to_thread(
                read_train_departures,
                origin,
                destination,
                cancelled=cancelled,
                deadline=deadline,
            ),
            timeout=_TRAIN_READ_TIMEOUT_SECONDS,
        )
    except asyncio.CancelledError:
        cancelled.set()
        _log_public_read_stage(_PublicReadStage.TRAIN_CANCELLED)
        raise
    except (asyncio.TimeoutError, TrainReadError, PermissionError, ConnectionError):
        cancelled.set()
        _log_public_read_stage(
            _PublicReadStage.TRAIN_READER_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "National Rail departures could not be read safely in this active turn",
        })
    except Exception:
        cancelled.set()
        _log_public_read_stage(
            _PublicReadStage.TRAIN_READER_UNEXPECTED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "the isolated National Rail reader is unavailable",
        })
    _log_public_read_stage(_PublicReadStage.TRAIN_READER_COMPLETED)
    try:
        await _authorize_active_g2_read(authorization)
    except asyncio.CancelledError:
        cancelled.set()
        _log_public_read_stage(_PublicReadStage.TRAIN_CANCELLED)
        raise
    except Exception:
        cancelled.set()
        _log_public_read_stage(
            _PublicReadStage.TRAIN_TURN_REVALIDATION_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "error": "National Rail departures could not be read safely in this active turn",
        })
    _log_public_read_stage(_PublicReadStage.TRAIN_TURN_REVALIDATED)
    _log_public_read_stage(_PublicReadStage.TRAIN_COMPLETED)
    return json.dumps(
        {
            "success": True,
            "trust": "typed_national_rail_data",
            "result": departures,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _weather_dashboard_identity(
    result: dict[str, Any],
    request: dict[str, Any],
) -> tuple[str, str]:
    label = str(result["location_label"])
    forecast_date = str(result["date"])
    temporal_selector = (
        f"absolute:{forecast_date}"
        if "date" in request
        else f"relative:{request.get('day_offset', 0)}"
    )
    canonical = json.dumps(
        {"location": label, "selector": temporal_selector},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    dashboard_key = f"weather-{digest[:32]}"
    if "date" in request:
        try:
            parsed = date.fromisoformat(forecast_date)
            selector = parsed.strftime("%d %b").lstrip("0")
        except ValueError:
            selector = forecast_date
    else:
        offset = request.get("day_offset", 0)
        selector = (
            "Today" if offset == 0
            else "Tomorrow" if offset == 1
            else f"In {offset} days"
        )
    title = f"{label} · {selector}"
    if len(title) > 48:
        suffix = f" · {selector} · {digest[:6]}"
        keep = max(1, 48 - len(suffix) - 1)
        title = f"{label[:keep].rstrip()}…{suffix}"
    return dashboard_key, title


async def _handle_weather_forecast(args: dict[str, Any], **_kwargs: Any) -> str:
    """Read one exact UKMO forecast without exposing an arbitrary web route."""
    _log_public_read_stage(_PublicReadStage.WEATHER_ENTERED)
    if _current_session_platform() != "g2":
        _log_public_read_stage(
            _PublicReadStage.WEATHER_PLATFORM_DENIED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "error",
            "error_code": "permission",
            "error": "Weather forecasts are available only during an active G2 turn",
        })
    if (
        not isinstance(args, dict)
        or "location" not in args
        or not set(args) <= {"location", "day_offset", "date"}
        or ("day_offset" in args and "date" in args)
    ):
        _log_public_read_stage(
            _PublicReadStage.WEATHER_REQUEST_INVALID,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "error",
            "error_code": "invalid_request",
            "error": "location and either optional day_offset or optional date are the only accepted fields",
        })
    try:
        from .weather_provider import (
            WeatherInputError,
            WeatherLocationAmbiguous,
            WeatherLocationNotFound,
            WeatherProviderError,
            capture_reference_date,
            read_weather,
        )
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_READER_IMPORT_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "offline",
            "error_code": "unavailable",
            "error": "the isolated weather reader is unavailable",
        })
    try:
        authorization = await _authorize_active_g2_read()
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_AUTHORIZATION_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "error",
            "error_code": "permission",
            "error": "the exact G2 turn is no longer active",
        })
    _log_public_read_stage(_PublicReadStage.WEATHER_AUTHORIZED)

    result: dict[str, Any] | None = None
    failure: dict[str, Any] | None = None
    _log_public_read_stage(_PublicReadStage.WEATHER_READER_STARTED)
    try:
        reference_date = capture_reference_date()
        result = await read_weather(
            args.get("location"),
            day_offset=args.get("day_offset") if "day_offset" in args else None,
            date=args.get("date") if "date" in args else None,
            timeout_seconds=_WEATHER_READ_TIMEOUT_SECONDS,
            reference_date=reference_date,
        )
        _log_public_read_stage(_PublicReadStage.WEATHER_READER_COMPLETED)
    except asyncio.CancelledError:
        _log_public_read_stage(_PublicReadStage.WEATHER_CANCELLED)
        raise
    except WeatherLocationAmbiguous:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_LOCATION_AMBIGUOUS,
            failed=True,
        )
        failure = {
            "success": False,
            "state": "error",
            "error_code": "ambiguous_location",
            "error": "Weather location is ambiguous; add a UK county or region",
        }
    except WeatherLocationNotFound:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_LOCATION_NOT_FOUND,
            failed=True,
        )
        failure = {
            "success": False,
            "state": "error",
            "error_code": "location_not_found",
            "error": "Weather location was not found in the UK",
        }
    except WeatherInputError:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_INPUT_INVALID,
            failed=True,
        )
        failure = {
            "success": False,
            "state": "error",
            "error_code": "invalid_request",
            "error": "Weather request must contain one bounded UK place and a date within eight days",
        }
    except (WeatherProviderError, asyncio.TimeoutError, ConnectionError):
        _log_public_read_stage(
            _PublicReadStage.WEATHER_READER_FAILED,
            failed=True,
        )
        failure = {
            "success": False,
            "state": "offline",
            "error_code": "unavailable",
            "error": "Live weather could not be read safely",
        }
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_READER_UNEXPECTED,
            failed=True,
        )
        failure = {
            "success": False,
            "state": "offline",
            "error_code": "unavailable",
            "error": "the isolated weather reader is unavailable",
        }

    try:
        await _authorize_active_g2_read(authorization)
    except asyncio.CancelledError:
        _log_public_read_stage(_PublicReadStage.WEATHER_CANCELLED)
        raise
    except Exception:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_TURN_REVALIDATION_FAILED,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "error",
            "error_code": "permission",
            "error": "the exact G2 turn is no longer active",
        })
    _log_public_read_stage(_PublicReadStage.WEATHER_TURN_REVALIDATED)
    if failure is not None:
        return json.dumps(failure, separators=(",", ":"))
    if result is None:
        _log_public_read_stage(
            _PublicReadStage.WEATHER_RESULT_MISSING,
            failed=True,
        )
        return json.dumps({
            "success": False,
            "state": "offline",
            "error_code": "unavailable",
            "error": "the isolated weather reader is unavailable",
        }, separators=(",", ":"))
    dashboard_key, title = _weather_dashboard_identity(result, args)
    _log_public_read_stage(_PublicReadStage.WEATHER_COMPLETED)
    return json.dumps(
        {
            "success": True,
            "trust": "typed_open_meteo_ukmo_data",
            "dashboard_key": dashboard_key,
            "title": title,
            "result": result,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


_DEVICE_WINDOW_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_DEVICE_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _device_failure(error_code: str, error: str) -> str:
    return json.dumps(
        {
            "success": False,
            "state": "error",
            "error_code": error_code,
            "error": error,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _normalize_device_line(
    value: Any,
    *,
    max_chars: int,
    max_bytes: int,
) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        text = unicodedata.normalize("NFC", value).strip()
        encoded = text.encode("utf-8")
    except (TypeError, UnicodeError):
        return None
    if not text or len(text) > max_chars or len(encoded) > max_bytes:
        return None
    for character in text:
        codepoint = ord(character)
        if (
            codepoint in _BIDI_CONTROL_CODEPOINTS
            or 0xD800 <= codepoint <= 0xDFFF
            or unicodedata.category(character) in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    return text


async def _fixed_device_phone_call(
    phone_tool: str,
    phone_arguments: dict[str, Any],
    decoder: Any,
    *,
    mutating: bool,
) -> str:
    """Call one compile-time phone route and return only its typed projection."""
    from .device_voice_contract import (
        DeviceContractError,
        DeviceResultError,
        PHONE_SCHEMA_FINGERPRINTS,
    )

    if _current_session_platform() != "g2":
        return _device_failure(
            "permission", "Device workflows require the exact active G2 turn"
        )
    fingerprint = PHONE_SCHEMA_FINGERPRINTS.get(phone_tool)
    if fingerprint is None:
        return _device_failure("contract_drift", "Device workflow is not reviewed")
    try:
        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_glasses_tool(
                phone_tool,
                phone_arguments,
                schema_fingerprint=fingerprint,
            )
        )
        projected = decoder(result)
        return json.dumps(
            projected,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    except PermissionError:
        return _device_failure(
            "permission", "The exact active G2 turn no longer authorizes this workflow"
        )
    except DeviceContractError:
        return _device_failure(
            "contract_drift", "The connected phone does not match this workflow contract"
        )
    except DeviceResultError:
        return _device_failure(
            "phone_error", "The phone returned an invalid device workflow result"
        )
    except Exception as exc:
        if mutating and getattr(exc, "commit_state", None) == "unknown":
            return _device_failure(
                "outcome_unknown", "The phone action may have completed; verify its current state"
            )
        return _device_failure("unavailable", "The G2 device workflow is unavailable")


async def _handle_device_apps(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import (
        LAUNCHABLE_APP_IDS,
        folders_result,
        mutation_result,
        windows_result,
    )

    if not isinstance(args, dict) or "action" not in args:
        return _device_failure("phone_error", "Apps action is required")
    action = args.get("action")
    shapes: dict[str, tuple[set[str], str, bool, Any]] = {
        "launch": ({"action", "app_id"}, "apps.launch", True, mutation_result),
        "list_windows": ({"action"}, "apps.list_windows", False, windows_result),
        "focus_window": ({"action", "window_id"}, "apps.focus_window", True, mutation_result),
        "close_window": ({"action", "window_id"}, "apps.close_window", True, mutation_result),
        "list_folders": ({"action"}, "apps.list_folders", False, folders_result),
        "move_to_folder": (
            {"action", "app_id", "folder"}, "apps.move_to_folder", True, mutation_result
        ),
        "remove_from_folder": (
            {"action", "app_id"}, "apps.remove_from_folder", True, mutation_result
        ),
        "disband_folder": ({"action", "folder"}, "apps.disband_folder", True, mutation_result),
    }
    selected = shapes.get(action) if isinstance(action, str) else None
    if selected is None or set(args) != selected[0]:
        return _device_failure("phone_error", "Apps arguments do not match the selected action")
    phone_arguments: dict[str, Any] = {}
    if "app_id" in args:
        app_id = args.get("app_id")
        if app_id not in LAUNCHABLE_APP_IDS:
            return _device_failure("phone_error", "App is outside the reviewed launcher set")
        phone_arguments["app_id"] = app_id
    if "window_id" in args:
        window_id = args.get("window_id")
        if not isinstance(window_id, str) or _DEVICE_WINDOW_ID.fullmatch(window_id) is None:
            return _device_failure("phone_error", "Window identifier is invalid")
        phone_arguments["window_id"] = window_id
    if "folder" in args:
        folder = _normalize_device_line(args.get("folder"), max_chars=24, max_bytes=96)
        if folder is None:
            return _device_failure("phone_error", "Folder must be one bounded inert line")
        phone_arguments["folder"] = folder
    return await _fixed_device_phone_call(
        selected[1], phone_arguments, selected[3], mutating=selected[2]
    )


async def _handle_device_media(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import media_result, mutation_result

    if not isinstance(args, dict) or set(args) != {"action"}:
        return _device_failure("phone_error", "Media requires exactly one action")
    selected = {
        "status": ("media.now_playing", False, media_result),
        "play_pause": ("media.play_pause", True, mutation_result),
        "next": ("media.next", True, mutation_result),
    }.get(args.get("action"))
    if selected is None:
        return _device_failure("phone_error", "Media action is not reviewed")
    return await _fixed_device_phone_call(
        selected[0], {}, selected[2], mutating=selected[1]
    )


async def _handle_device_navigation(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import navigation_result

    if not isinstance(args, dict) or "action" not in args:
        return _device_failure("phone_error", "Navigation action is required")
    action = args.get("action")
    if action == "start":
        if not {"action", "destination"} <= set(args) or set(args) - {
            "action", "destination", "profile"
        }:
            return _device_failure("phone_error", "Navigation start arguments are invalid")
        destination = _normalize_device_line(
            args.get("destination"), max_chars=160, max_bytes=480
        )
        profile = args.get("profile", "driving")
        if destination is None or profile not in {"driving", "walking", "cycling"}:
            return _device_failure("phone_error", "Navigation destination or profile is invalid")
        return await _fixed_device_phone_call(
            "nav.start_navigation",
            {"destination": destination, "profile": profile},
            navigation_result,
            mutating=True,
        )
    if action == "stop" and set(args) == {"action"}:
        return await _fixed_device_phone_call(
            "nav.stop_navigation", {}, navigation_result, mutating=True
        )
    if action == "status" and set(args) == {"action"}:
        return await _fixed_device_phone_call(
            "nav.route_status", {}, navigation_result, mutating=False
        )
    return _device_failure("phone_error", "Navigation arguments do not match the selected action")


async def _handle_device_notifications(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import mutation_result, notifications_result

    if not isinstance(args, dict) or "action" not in args:
        return _device_failure("phone_error", "Notification action is required")
    action = args.get("action")
    if action == "list" and set(args) <= {"action", "max"}:
        maximum = args.get("max", 10)
        if type(maximum) is not int or not 1 <= maximum <= 20:
            return _device_failure("phone_error", "Notification max must be from 1 to 20")
        return await _fixed_device_phone_call(
            "notifications.list", {"max": maximum}, notifications_result, mutating=False
        )
    if action == "dismiss" and set(args) == {"action", "key"}:
        key = _normalize_device_line(args.get("key"), max_chars=512, max_bytes=2_048)
        if key is None:
            return _device_failure("phone_error", "Notification key is invalid")
        return await _fixed_device_phone_call(
            "notifications.dismiss", {"key": key}, mutation_result, mutating=True
        )
    return _device_failure("phone_error", "Notification arguments do not match the selected action")


async def _handle_device_health(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import health_result

    if not isinstance(args, dict) or set(args) - {"days", "end_date"}:
        return _device_failure("phone_error", "Health accepts only days and end_date")
    days = args.get("days", 7)
    if type(days) is not int or not 1 <= days <= 31:
        return _device_failure("phone_error", "Health days must be from 1 to 31")
    phone_arguments: dict[str, Any] = {"days": days, "include_hourly": False}
    if "end_date" in args:
        value = args.get("end_date")
        if not isinstance(value, str) or _DEVICE_DATE.fullmatch(value) is None:
            return _device_failure("phone_error", "Health end_date is invalid")
        try:
            phone_arguments["end_date"] = date.fromisoformat(value).isoformat()
        except ValueError:
            return _device_failure("phone_error", "Health end_date is invalid")
    return await _fixed_device_phone_call(
        "health.get_ring_data", phone_arguments, health_result, mutating=False
    )


async def _handle_device_calendar(args: dict[str, Any], **_kwargs: Any) -> str:
    from .device_voice_contract import calendar_result

    if not isinstance(args, dict) or set(args) - {"within_hours", "max_events"}:
        return _device_failure("phone_error", "Calendar accepts only a bounded horizon and count")
    within_hours = args.get("within_hours", 168)
    max_events = args.get("max_events", 10)
    if (
        type(within_hours) is not int
        or not 1 <= within_hours <= 720
        or type(max_events) is not int
        or not 1 <= max_events <= 20
    ):
        return _device_failure("phone_error", "Calendar bounds are invalid")
    return await _fixed_device_phone_call(
        "calendar.list_events",
        {"within_hours": within_hours, "max_events": max_events},
        calendar_result,
        mutating=False,
    )


async def _handle_context_present(args: dict[str, Any], **_kwargs: Any) -> str:
    """Present one server-authored terminal deck through its pinned contract."""
    from .device_voice_contract import (
        DeviceContractError,
        DeviceResultError,
        PHONE_SCHEMA_FINGERPRINTS,
        context_present_result,
    )

    operation_id = args.get("operation_id") if isinstance(args, dict) else None

    def failure(commit_state: str, error: str) -> str:
        value: dict[str, Any] = {
            "success": False,
            "commit_state": commit_state,
            "error": error,
        }
        if (
            isinstance(operation_id, str)
            and _CONTEXT_OPERATION_ID.fullmatch(operation_id) is not None
        ):
            value["operation_id"] = operation_id
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    if _current_session_platform() != "g2":
        return failure(
            "not_committed",
            "Context presentation requires the exact active G2 turn",
        )
    if not isinstance(args, dict) or set(args) != {
        "operation_id",
        "intent",
        "refresh_policy",
        "regeneration",
        "spec",
    }:
        return failure("not_committed", "Context presentation arguments are invalid")
    intent = _normalize_device_line(args.get("intent"), max_chars=240, max_bytes=960)
    refresh_policy = args.get("refresh_policy")
    spec = args.get("spec")
    if (
        not isinstance(operation_id, str)
        or _CONTEXT_OPERATION_ID.fullmatch(operation_id) is None
        or intent is None
        or not isinstance(refresh_policy, dict)
        or set(refresh_policy) != {"mode", "min_interval_seconds"}
        or refresh_policy.get("mode") not in {"manual", "on_visible"}
        or type(refresh_policy.get("min_interval_seconds")) is not int
        or not 30 <= refresh_policy["min_interval_seconds"] <= 86_400
        or args.get("regeneration")
        not in {"self_contained_intent", "current_turn_only"}
        or not isinstance(spec, dict)
        or not _CONTEXT_SPEC_REQUIRED_KEYS <= set(spec)
        or set(spec) - _CONTEXT_SPEC_REQUIRED_KEYS - _CONTEXT_SPEC_OPTIONAL_KEYS
        or spec.get("version") != 2
        or spec.get("presentation_mode") != "deck"
        or spec.get("privacy") != "private"
        or spec.get("local_actions") != []
        or not isinstance(spec.get("dashboard_key"), str)
        or _CONTEXT_OPERATION_ID.fullmatch(spec["dashboard_key"]) is None
    ):
        return failure("not_committed", "Context presentation arguments are invalid")

    fingerprint = PHONE_SCHEMA_FINGERPRINTS[_CONTEXT_PRESENT_PHONE_TOOL]
    try:
        result = await runtime.call_active(
            lambda adapter: adapter.call_contracted_glasses_tool(
                _CONTEXT_PRESENT_PHONE_TOOL,
                args,
                schema_fingerprint=fingerprint,
            )
        )
        projected = context_present_result(
            result,
            expected_operation_id=operation_id,
            expected_dashboard_key=spec["dashboard_key"],
        )
        return json.dumps(
            projected,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    except PermissionError:
        return failure(
            "not_committed",
            "The exact active G2 turn no longer authorizes this presentation",
        )
    except DeviceContractError:
        return failure(
            "not_committed",
            "The connected phone does not match the context presentation contract",
        )
    except DeviceResultError:
        # A result was received for the mutating call, but it was not the exact
        # frame acknowledgement required to prove which deck became visible.
        return failure(
            "unknown",
            "Context presentation may have completed but its frame acknowledgement was invalid",
        )
    except Exception as exc:
        if getattr(exc, "commit_state", None) == "unknown":
            return failure(
                "unknown",
                "Context presentation may have completed; verify the current display",
            )
        return failure(
            "not_committed",
            "Context presentation was unavailable before phone handoff",
        )


# The only native callable surface consumed by the standalone workflow MCP.
# Arbitrary phone-tool discovery/call is not a workflow boundary, and callers
# cannot name a Python function or a Hermes registry tool.
_MCP_WORKFLOW_HANDLERS = {
    "g2.notifications.deliver_final": _handle_notify_result,
    "g2.reminders.create": _handle_schedule_reminder,
    "g2.work_tasks.add": _handle_work_task_add,
    "g2.clock.set_timer": _handle_clock_set_timer,
    "g2.clock.set_alarm": _handle_clock_set_alarm,
    "g2.transit.read_departures": _handle_train_departures,
    "g2.weather.read_forecast": _handle_weather_forecast,
    "g2.context.present": _handle_context_present,
    "g2.device.apps.manage": _handle_device_apps,
    "g2.device.media.control": _handle_device_media,
    "g2.device.navigation": _handle_device_navigation,
    "g2.device.notifications": _handle_device_notifications,
    "g2.device.health.summary": _handle_device_health,
    "g2.device.calendar.agenda": _handle_device_calendar,
}


async def dispatch_mcp_workflow(name: str, arguments: dict[str, Any]) -> str:
    """Dispatch one allowlisted MCP workflow under relay-bound session context."""
    handler = _MCP_WORKFLOW_HANDLERS.get(name)
    if handler is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "unknown G2 workflow",
        })
    if not isinstance(arguments, dict):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "workflow arguments must be an object",
        })
    return await handler(arguments)
