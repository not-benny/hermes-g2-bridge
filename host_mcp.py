"""Host-owned MCP session carried over the authenticated G2 websocket.

The phone is already an MCP server for device tools on ``chan: mcp``.  This
module implements the opposite logical role on ``chan: host-mcp``: the phone
is an MCP client and the gateway is the server.  It exposes a long-running
voice-turn tool plus a bounded Cockpit projection and exact command dispatcher.
A successful voice call is not answered when accepted; its CallToolResult is
emitted only after the gateway reports the exact bound Hermes turn complete.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import math
import re
import secrets
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


HOST_MCP_CAPABILITY = "host-mcp-v1"
CONVERSATE_CUES_CAPABILITY = "conversate-cues-v1"
COCKPIT_FREE_TEXT_CAPABILITY = "cockpit-free-text-v1"
MCP_PROTOCOL_VERSION = "2025-06-18"
HOST_MCP_SERVER_VERSION = "1.0.0"
VOICE_TURN_TOOL = "hermes.voice.turn"
CONVERSATE_CUES_TOOL = "hermes.conversate.cues"
COCKPIT_COMMAND_TOOL = "hermes.cockpit.command"
HOST_STATUS_RESOURCE_URI = "hermes://session/status"
COCKPIT_STATE_RESOURCE_URI = "hermes://cockpit/state"

_MAX_SAFE_INTEGER = 9_007_199_254_740_991
_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_TURN_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_MAX_TRANSCRIPT_SCALARS = 4_096
_MAX_TRANSCRIPT_BYTES = 16_384
_MAX_CUE_TEXT_SCALARS = 160
_MAX_CONVERSATE_CUES = 3
_COCKPIT_REVIEW_TEXT_MAX_SCALARS = 64
_CONVERSATE_CUE_DEADLINE_SECONDS = 2.5
_CONVERSATE_CUE_CANCEL_GRACE_SECONDS = 0.05
_MAX_FINAL_TEXT_SCALARS = 16_384
_MAX_FINAL_TEXT_BYTES = 65_536
_MAX_REQUESTS_PER_SESSION = 4_096
_MAX_INITIALIZE_CAPABILITIES_BYTES = 4_096
_CONTROL_OR_BIDI = frozenset(
    {
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
    }
)
_SESSION_GENERATION = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_STOP_REASONS = frozenset({"end_turn", "cancelled", "error"})
_COCKPIT_ID = re.compile(r"^[A-Za-z0-9._-]{12,128}$")
_COCKPIT_STATES = frozenset(
    {"queued", "running", "waiting_human", "interrupting", "completed", "failed", "interrupted"}
)
_MAX_COCKPIT_SESSIONS = 8
_MAX_COCKPIT_TIMELINE = 40
_MAX_COCKPIT_PENDING = 8
_COCKPIT_INTERACTION_TTL_MS = 5 * 60 * 1000
# The phone independently rejects Cockpit resource text above 48 KiB and the
# resource is itself JSON-escaped inside a bounded websocket frame.  Keeping
# the inner snapshot at 24 KiB leaves deterministic room for that envelope.
_MAX_COCKPIT_SNAPSHOT_BYTES = 24 * 1024

JsonRpcId = str | int
SendMessage = Callable[[dict[str, Any]], Awaitable[None]]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HostVoiceTurnRequest:
    """Validated, inert input accepted from ``hermes.voice.turn``."""

    turn_id: str
    text: str
    context: dict[str, Any]


@dataclass(frozen=True)
class HostConversateCuesRequest:
    """Bounded recent text for the optional, tool-free Conversate side lane."""

    session_id: str
    revision: int
    transcript: str


@dataclass(frozen=True)
class HostTurnBinding:
    """Unforgeable in-process identity for one accepted host-MCP request."""

    request_id: JsonRpcId
    request_key: str
    session_generation: str
    call_generation: int
    turn_id: str


@dataclass
class _PendingTurn:
    binding: HostTurnBinding
    cancellation_requested: bool = False


@dataclass
class _PendingConversateCues:
    request_id: JsonRpcId
    request_key: str
    request: HostConversateCuesRequest
    task: asyncio.Task[None] | None = None


@dataclass
class _CockpitSessionRecord:
    binding: HostTurnBinding
    document: dict[str, Any]
    backends: dict[str, dict[str, Any]]


VOICE_TURN_TOOL_SPEC: dict[str, Any] = {
    "name": VOICE_TURN_TOOL,
    "description": (
        "Submit one wearer voice utterance to the authenticated Hermes G2 "
        "session and wait for its authoritative final result. The call is "
        "long-running: no thinking, tool activity, partial text, or progress "
        "is returned. Cancel it with MCP notifications/cancelled using this "
        "request's exact JSON-RPC id."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "turnId": {
                "type": "string",
                "pattern": r"^[A-Za-z0-9._-]{1,128}$",
                "description": "Phone-generated identity unique within this connection.",
            },
            "text": {
                "type": "string",
                "minLength": 1,
                "maxLength": _MAX_TRANSCRIPT_SCALARS,
                "description": "The inert, one-line wearer transcript.",
            },
            "context": {
                "type": "object",
                "properties": {
                    "foregroundApp": {"type": ["string", "null"], "maxLength": 128},
                    "foregroundTitle": {"type": ["string", "null"], "maxLength": 160},
                    "screenOn": {"type": "boolean"},
                    "localTime": {"type": "string", "maxLength": 128},
                    "headsetBattery": {
                        "type": ["number", "null"],
                        "minimum": 0,
                        "maximum": 100,
                    },
                },
                "additionalProperties": False,
            },
        },
        "required": ["turnId", "text"],
        "additionalProperties": False,
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "turnId": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{1,128}$"},
            "text": {"type": "string", "maxLength": _MAX_FINAL_TEXT_SCALARS},
            "stopReason": {"enum": ["end_turn", "cancelled", "error"]},
            "generation": {
                "type": "integer",
                "minimum": 1,
                "maximum": _MAX_SAFE_INTEGER,
            },
        },
        "required": ["turnId", "text", "stopReason", "generation"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
}

CONVERSATE_CUES_TOOL_SPEC: dict[str, Any] = {
    "name": CONVERSATE_CUES_TOOL,
    "description": (
        "Generate up to three short conversation cues from bounded recent transcript text. "
        "This is an optional low-latency auxiliary-model call: it has no agent session, "
        "chat history, workflow, device tools, progress stream, or side effects. A newer "
        "request supersedes the older request and every request has a 2.5 second deadline."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "sessionId": {
                "type": "string",
                "pattern": r"^[A-Za-z0-9._-]{1,128}$",
            },
            "revision": {
                "type": "integer",
                "minimum": 1,
                "maximum": _MAX_SAFE_INTEGER,
            },
            "transcript": {
                "type": "string",
                "minLength": 1,
                "maxLength": _MAX_TRANSCRIPT_SCALARS,
                "description": (
                    "Bounded recent transcript text, including live revisions; "
                    "never audio."
                ),
            },
        },
        "required": ["sessionId", "revision", "transcript"],
        "additionalProperties": False,
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "sessionId": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{1,128}$"},
            "revision": {"type": "integer", "minimum": 1, "maximum": _MAX_SAFE_INTEGER},
            "cues": {
                "type": "array",
                "maxItems": _MAX_CONVERSATE_CUES,
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"enum": ["question", "topic", "action"]},
                        "text": {"type": "string", "minLength": 1, "maxLength": _MAX_CUE_TEXT_SCALARS},
                    },
                    "required": ["kind", "text"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["sessionId", "revision", "cues"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
}

COCKPIT_COMMAND_TOOL_SPEC: dict[str, Any] = {
    "name": COCKPIT_COMMAND_TOOL,
    "description": (
        "Submit one exact command created from the authenticated Cockpit projection. "
        "The host revalidates connection, session generation, request nonce, choice, "
        "expiry, and live backend authority before performing any effect."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "v": {"const": 1},
            "chan": {"const": "cockpit"},
            "connection_generation": {"type": "string", "minLength": 12, "maxLength": 128},
            "type": {
                "enum": [
                    "answer",
                    "answer_text",
                    "permission_decide",
                    "steer",
                    "interrupt",
                ]
            },
            "command_id": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{12,128}$"},
            "session_id": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{12,128}$"},
            "generation": {"type": "integer", "minimum": 1, "maximum": _MAX_SAFE_INTEGER},
            "request_id": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{12,128}$"},
            "nonce": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{12,128}$"},
            "choice_id": {"type": "string", "pattern": r"^[A-Za-z0-9._-]{12,128}$"},
            "decision": {"enum": ["deny", "allow_once"]},
            "text": {
                "type": "string",
                "minLength": 1,
                "maxLength": _COCKPIT_REVIEW_TEXT_MAX_SCALARS,
                "pattern": r"^(?!.*[<>`])[\x21-\x7E](?:[\x20-\x7E]*[\x21-\x7E])?$",
            },
        },
        "required": [
            "v", "chan", "connection_generation", "type", "command_id", "session_id", "generation"
        ],
        "oneOf": [
            {
                "properties": {"type": {"const": "answer"}},
                "required": ["request_id", "nonce", "choice_id"],
                "not": {"anyOf": [{"required": ["decision"]}, {"required": ["text"]}]},
            },
            {
                "properties": {"type": {"const": "answer_text"}},
                "required": ["request_id", "nonce", "text"],
                "not": {
                    "anyOf": [
                        {"required": ["choice_id"]},
                        {"required": ["decision"]},
                    ]
                },
            },
            {
                "properties": {"type": {"const": "permission_decide"}},
                "required": ["request_id", "nonce", "decision"],
                "not": {"anyOf": [{"required": ["choice_id"]}, {"required": ["text"]}]},
            },
            {
                "properties": {"type": {"const": "steer"}},
                "required": ["text"],
                "not": {
                    "anyOf": [
                        {"required": ["request_id"]},
                        {"required": ["nonce"]},
                        {"required": ["choice_id"]},
                        {"required": ["decision"]},
                    ]
                },
            },
            {
                "properties": {"type": {"const": "interrupt"}},
                "not": {
                    "anyOf": [
                        {"required": ["request_id"]},
                        {"required": ["nonce"]},
                        {"required": ["choice_id"]},
                        {"required": ["decision"]},
                        {"required": ["text"]},
                    ]
                },
            },
        ],
        "additionalProperties": False,
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "v": {"const": 1},
            "chan": {"const": "cockpit"},
            "type": {"const": "command_receipt"},
            "sequence": {"type": "integer", "minimum": 0, "maximum": _MAX_SAFE_INTEGER},
            "command_id": {"type": "string"},
            "session_id": {"type": "string"},
            "generation": {"type": "integer"},
            "outcome": {"enum": ["accepted", "rejected", "duplicate", "outcome_unknown"]},
            "code": {"type": "string", "maxLength": 80},
        },
        "required": ["v", "chan", "type", "sequence", "command_id", "session_id", "generation", "outcome"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    },
}

HOST_STATUS_RESOURCE_SPEC: dict[str, Any] = {
    "uri": HOST_STATUS_RESOURCE_URI,
    "name": "hermes.session.status",
    "title": "Hermes Session Status",
    "description": (
        "Authenticated, bounded connection status for the G2 host session. "
        "It exposes no conversation history, transcript, device identity, "
        "or session-control authority."
    ),
    "mimeType": "application/json",
}

COCKPIT_STATE_RESOURCE_SPEC: dict[str, Any] = {
    "uri": COCKPIT_STATE_RESOURCE_URI,
    "name": "hermes.cockpit.state",
    "title": "Hermes Cockpit State",
    "description": (
        "Authenticated bounded projection of explicitly shared G2 work: session state, "
        "final timeline rows, and pending reviewed interactions."
    ),
    "mimeType": "application/json",
}


def _valid_request_id(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return 0 <= value <= _MAX_SAFE_INTEGER
    return isinstance(value, str) and bool(_REQUEST_ID.fullmatch(value))


def _request_key(value: JsonRpcId) -> str:
    return f"{'i' if isinstance(value, int) else 's'}:{value}"


def _safe_one_line(value: Any, *, max_scalars: int, allow_empty: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    for character in value:
        codepoint = ord(character)
        category = unicodedata.category(character)
        if (
            codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or 0xD800 <= codepoint <= 0xDFFF
            or codepoint in _CONTROL_OR_BIDI
            or category in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    try:
        normalized = unicodedata.normalize("NFC", value).strip()
    except (TypeError, UnicodeError):
        return None
    if (not normalized and not allow_empty) or len(normalized) > max_scalars:
        return None
    return normalized


def _normalize_optional_context_text(value: Any, maximum: int) -> str | None | object:
    if value is None:
        return None
    normalized = _safe_one_line(value, max_scalars=maximum, allow_empty=True)
    return normalized if normalized is not None else _INVALID


_INVALID = object()


def _normalize_context(value: Any) -> dict[str, Any] | None:
    if value is None:
        return {}
    if not isinstance(value, dict):
        return None
    allowed = {
        "foregroundApp",
        "foregroundTitle",
        "screenOn",
        "localTime",
        "headsetBattery",
    }
    if not set(value).issubset(allowed):
        return None
    normalized: dict[str, Any] = {}
    if "foregroundApp" in value:
        item = _normalize_optional_context_text(value["foregroundApp"], 128)
        if item is _INVALID:
            return None
        normalized["foregroundApp"] = item
    if "foregroundTitle" in value:
        item = _normalize_optional_context_text(value["foregroundTitle"], 160)
        if item is _INVALID:
            return None
        normalized["foregroundTitle"] = item
    if "screenOn" in value:
        if not isinstance(value["screenOn"], bool):
            return None
        normalized["screenOn"] = value["screenOn"]
    if "localTime" in value:
        local_time = _safe_one_line(
            value["localTime"], max_scalars=128, allow_empty=True
        )
        if local_time is None:
            return None
        normalized["localTime"] = local_time
    if "headsetBattery" in value:
        battery = value["headsetBattery"]
        if battery is not None and (
            isinstance(battery, bool)
            or not isinstance(battery, (int, float))
            or not math.isfinite(battery)
            or not 0 <= battery <= 100
        ):
            return None
        normalized["headsetBattery"] = battery
    return normalized


def _normalize_voice_turn(arguments: Any) -> HostVoiceTurnRequest | None:
    if not isinstance(arguments, dict) or set(arguments) - {"turnId", "text", "context"}:
        return None
    if set(arguments) < {"turnId", "text"}:
        return None
    turn_id = arguments.get("turnId")
    if not isinstance(turn_id, str) or not _TURN_ID.fullmatch(turn_id):
        return None
    text = _safe_one_line(arguments.get("text"), max_scalars=_MAX_TRANSCRIPT_SCALARS)
    if text is None:
        return None
    try:
        if len(text.encode("utf-8")) > _MAX_TRANSCRIPT_BYTES:
            return None
    except UnicodeError:
        return None
    context = _normalize_context(arguments.get("context"))
    if context is None:
        return None
    return HostVoiceTurnRequest(turn_id=turn_id, text=text, context=context)


def _normalize_conversate_cues(
    arguments: Any,
) -> HostConversateCuesRequest | None:
    if not isinstance(arguments, dict) or set(arguments) != {
        "sessionId",
        "revision",
        "transcript",
    }:
        return None
    session_id = arguments.get("sessionId")
    revision = arguments.get("revision")
    if not isinstance(session_id, str) or not _TURN_ID.fullmatch(session_id):
        return None
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or not 1 <= revision <= _MAX_SAFE_INTEGER
    ):
        return None
    transcript = _safe_one_line(
        arguments.get("transcript"), max_scalars=_MAX_TRANSCRIPT_SCALARS
    )
    if transcript is None:
        return None
    try:
        if len(transcript.encode("utf-8")) > _MAX_TRANSCRIPT_BYTES:
            return None
    except UnicodeError:
        return None
    return HostConversateCuesRequest(
        session_id=session_id,
        revision=revision,
        transcript=transcript,
    )


def _normalize_generated_cues(value: Any) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or len(value) > _MAX_CONVERSATE_CUES:
        return None
    normalized: list[dict[str, str]] = []
    for cue in value:
        if not isinstance(cue, dict) or set(cue) != {"kind", "text"}:
            return None
        kind = cue.get("kind")
        text = _safe_one_line(
            cue.get("text"), max_scalars=_MAX_CUE_TEXT_SCALARS
        )
        if kind not in {"question", "topic", "action"} or text is None:
            return None
        normalized.append({"kind": kind, "text": text})
    return normalized


def _normalize_final_text(value: Any) -> str:
    text = str(value or "")
    if len(text) > _MAX_FINAL_TEXT_SCALARS:
        text = text[:_MAX_FINAL_TEXT_SCALARS]
    while len(text.encode("utf-8", errors="replace")) > _MAX_FINAL_TEXT_BYTES:
        text = text[:-1]
    return text


def _cockpit_text(value: Any, maximum: int, fallback: str) -> str:
    """Collapse host text into the Cockpit protocol's inert one-line subset."""
    raw = unicodedata.normalize("NFC", str(value or ""))
    cleaned: list[str] = []
    for character in raw:
        codepoint = ord(character)
        if (
            character in "<>`"
            or codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or codepoint in _CONTROL_OR_BIDI
            or unicodedata.category(character) in {"Cc", "Cs", "Zl", "Zp"}
        ):
            cleaned.append(" ")
        else:
            cleaned.append(character)
    result = " ".join("".join(cleaned).split()).strip()
    if not result:
        result = fallback
    return result[:maximum]


def _cockpit_review_text(value: Any) -> str | None:
    """Return only unchanged text proven to fit the 640x480 review."""
    if not isinstance(value, str):
        return None
    # Never trim or normalize text after the wearer reviewed it. Leading or
    # trailing whitespace is rejected because the native renderer does not
    # make that authority-bearing difference reliably visible.
    if (
        not value
        or value != value.strip()
        or len(value) > _COCKPIT_REVIEW_TEXT_MAX_SCALARS
    ):
        return None
    if any(ord(character) < 0x20 or ord(character) > 0x7E for character in value):
        return None
    if any(character in "<>`" for character in value):
        return None
    return value


def _cockpit_id(prefix: str, seed: str | None = None) -> str:
    entropy = seed if seed is not None else secrets.token_hex(24)
    digest = hashlib.sha256(entropy.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}_{digest}"


def _normalize_cockpit_command(arguments: Any, session_generation: str) -> dict[str, Any] | None:
    if not isinstance(arguments, dict):
        return None
    base = {
        "v", "chan", "connection_generation", "type", "command_id", "session_id", "generation"
    }
    command_type = arguments.get("type")
    extras = {
        "answer": {"request_id", "nonce", "choice_id"},
        "answer_text": {"request_id", "nonce", "text"},
        "permission_decide": {"request_id", "nonce", "decision"},
        "steer": {"text"},
        "interrupt": set(),
    }.get(command_type)
    if extras is None or set(arguments) != base | extras:
        return None
    if (
        arguments.get("v") != 1
        or arguments.get("chan") != "cockpit"
        or arguments.get("connection_generation") != session_generation
        or not all(
            isinstance(arguments.get(name), str) and _COCKPIT_ID.fullmatch(arguments[name])
            for name in ("command_id", "session_id")
        )
        or isinstance(arguments.get("generation"), bool)
        or not isinstance(arguments.get("generation"), int)
        or not 1 <= arguments["generation"] <= _MAX_SAFE_INTEGER
    ):
        return None
    if command_type in {"answer", "answer_text", "permission_decide"}:
        if not all(
            isinstance(arguments.get(name), str) and _COCKPIT_ID.fullmatch(arguments[name])
            for name in ("request_id", "nonce")
        ):
            return None
    if command_type == "answer" and not (
        isinstance(arguments.get("choice_id"), str)
        and _COCKPIT_ID.fullmatch(arguments["choice_id"])
    ):
        return None
    if command_type == "permission_decide" and arguments.get("decision") not in {
        "deny", "allow_once"
    }:
        return None
    if command_type in {"answer_text", "steer"}:
        text = _cockpit_review_text(arguments.get("text"))
        if text is None:
            return None
        arguments = {**arguments, "text": text}
    return dict(arguments)


class HostSessionMcpServer:
    """One connection-scoped MCP server for phone-to-Hermes interactions."""

    def __init__(
        self,
        send: SendMessage,
        *,
        session_generation: str,
        on_voice_turn: Callable[
            [HostTurnBinding, HostVoiceTurnRequest], Awaitable[None]
        ],
        on_cancel: Callable[[HostTurnBinding], Awaitable[None]],
        on_conversate_cues: Callable[
            [HostConversateCuesRequest], Awaitable[list[dict[str, str]]]
        ] | None = None,
        on_fatal: Callable[[str], Awaitable[None]] | None = None,
        on_cockpit_command: Callable[
            [dict[str, Any], dict[str, Any] | None], Awaitable[tuple[str, str | None]]
        ] | None = None,
        cockpit_free_text_enabled: bool = False,
        profile: str | None = None,
    ) -> None:
        if not isinstance(session_generation, str) or not _SESSION_GENERATION.fullmatch(
            session_generation
        ):
            raise ValueError("host MCP session generation is invalid")
        if profile is not None and (
            not isinstance(profile, str) or not _TURN_ID.fullmatch(profile)
        ):
            raise ValueError("host MCP profile is invalid")
        if not isinstance(cockpit_free_text_enabled, bool):
            raise ValueError("Cockpit free-text negotiation is invalid")
        self._send = send
        self._session_generation = session_generation
        self._on_voice_turn = on_voice_turn
        self._on_cancel = on_cancel
        self._on_conversate_cues = on_conversate_cues
        self._on_fatal = on_fatal
        self._on_cockpit_command = on_cockpit_command
        self._cockpit_free_text_enabled = cockpit_free_text_enabled
        self._profile = profile
        self._lifecycle = "new"
        self._closed = False
        self._next_call_generation = 1
        self._pending: dict[str, _PendingTurn] = {}
        self._pending_conversate_cues: dict[str, _PendingConversateCues] = {}
        self._completed: set[str] = set()
        self._cockpit_sessions: dict[str, _CockpitSessionRecord] = {}
        self._cockpit_binding_sessions: dict[str, str] = {}
        self._cockpit_sequence = 0
        self._cockpit_subscribed = False
        self._cockpit_commands: set[str] = set()

    @property
    def initialized(self) -> bool:
        return self._lifecycle == "initialized" and not self._closed

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    async def handle_message(self, message: Any) -> None:
        if self._closed or not isinstance(message, dict):
            return
        request_id = message.get("id", _INVALID)
        valid_id = request_id is not _INVALID and _valid_request_id(request_id)
        if message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
            if valid_id:
                await self._error(request_id, -32600, "Invalid Request")
            return
        if request_id is not _INVALID and not valid_id:
            return
        if set(message) - {"jsonrpc", "id", "method", "params"}:
            if valid_id:
                await self._error(request_id, -32600, "Invalid Request")
            return

        method = message["method"]
        if request_id is _INVALID:
            if method == "notifications/initialized":
                params = message.get("params", {})
                if (
                    self._lifecycle == "initializing"
                    and message.keys() <= {"jsonrpc", "method", "params"}
                    and isinstance(params, dict)
                    and not params
                ):
                    self._lifecycle = "initialized"
                return
            if method == "notifications/cancelled":
                await self._handle_cancel_notification(message.get("params"))
            return

        key = _request_key(request_id)
        if (
            key in self._pending
            or key in self._pending_conversate_cues
            or key in self._completed
        ):
            await self._error(request_id, -32600, "Duplicate request id")
            return
        if len(self._completed) >= _MAX_REQUESTS_PER_SESSION:
            reason = "Host MCP request budget exhausted; reconnect before retrying"
            await self._error(
                request_id,
                -32005,
                reason,
            )
            # Completed request IDs are connection-scoped tombstones: evicting
            # one could let a delayed cancellation alias a reused ID. Make
            # exhaustion connection-fatal so the phone's normal reconnect path
            # establishes a fresh generation and a fresh bounded ID space.
            self.close()
            if self._on_fatal is not None:
                await self._on_fatal(reason)
            return

        if method == "initialize":
            if self._lifecycle != "new" or not self._valid_initialize(message.get("params")):
                await self._error(request_id, -32602, "Invalid initialize request")
                self._remember_completed(key)
                return
            self._lifecycle = "initializing"
            await self._result(
                request_id,
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {
                            "subscribe": True,
                            "listChanged": False,
                        },
                    },
                    "serverInfo": {
                        "name": "hermes-g2-host",
                        "version": HOST_MCP_SERVER_VERSION,
                    },
                },
            )
            self._remember_completed(key)
            return

        if method == "ping":
            params = message.get("params", {})
            if not isinstance(params, dict) or params:
                await self._error(request_id, -32602, "Invalid ping parameters")
            else:
                await self._result(request_id, {})
            self._remember_completed(key)
            return

        if self._lifecycle != "initialized":
            await self._error(request_id, -32002, "MCP server is not initialized")
            self._remember_completed(key)
            return

        if method == "tools/list":
            params = message.get("params", {})
            if not isinstance(params, dict) or params:
                await self._error(request_id, -32602, "Invalid tools/list parameters")
            else:
                tools = [VOICE_TURN_TOOL_SPEC, COCKPIT_COMMAND_TOOL_SPEC]
                if self._on_conversate_cues is not None:
                    tools.append(CONVERSATE_CUES_TOOL_SPEC)
                await self._result(
                    request_id,
                    {"tools": tools},
                )
            self._remember_completed(key)
            return

        if method == "resources/list":
            params = message.get("params", {})
            if not isinstance(params, dict) or params:
                await self._error(request_id, -32602, "Invalid resources/list parameters")
            else:
                await self._result(
                    request_id,
                    {
                        "resources": [
                            HOST_STATUS_RESOURCE_SPEC,
                            COCKPIT_STATE_RESOURCE_SPEC,
                        ]
                    },
                )
            self._remember_completed(key)
            return

        if method in {"resources/subscribe", "resources/unsubscribe"}:
            params = message.get("params")
            if (
                not isinstance(params, dict)
                or set(params) != {"uri"}
                or params.get("uri") != COCKPIT_STATE_RESOURCE_URI
            ):
                await self._error(request_id, -32602, f"Invalid {method} parameters")
            else:
                self._cockpit_subscribed = method == "resources/subscribe"
                await self._result(request_id, {})
            self._remember_completed(key)
            return

        if method == "resources/read":
            params = message.get("params")
            if not isinstance(params, dict) or set(params) != {"uri"}:
                await self._error(request_id, -32602, "Invalid resources/read parameters")
            elif params.get("uri") not in {
                HOST_STATUS_RESOURCE_URI,
                COCKPIT_STATE_RESOURCE_URI,
            }:
                await self._error(request_id, -32002, "Unknown host MCP resource")
            else:
                uri = params["uri"]
                document = (
                    self._status_document()
                    if uri == HOST_STATUS_RESOURCE_URI
                    else self._cockpit_snapshot()
                )
                await self._result(
                    request_id,
                    {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "application/json",
                                "text": json.dumps(
                                    document,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                ),
                            }
                        ]
                    },
                )
            self._remember_completed(key)
            return

        if method != "tools/call":
            await self._error(request_id, -32601, f"Method not supported: {method}")
            self._remember_completed(key)
            return

        params = message.get("params")
        if not isinstance(params, dict) or set(params) != {"name", "arguments"}:
            await self._error(request_id, -32602, "Invalid tools/call parameters")
            self._remember_completed(key)
            return
        tool_name = params.get("name")
        allowed_tools = {VOICE_TURN_TOOL, COCKPIT_COMMAND_TOOL}
        if self._on_conversate_cues is not None:
            allowed_tools.add(CONVERSATE_CUES_TOOL)
        if tool_name not in allowed_tools:
            await self._error(request_id, -32602, "Unknown host MCP tool")
            self._remember_completed(key)
            return
        if tool_name == COCKPIT_COMMAND_TOOL:
            command = _normalize_cockpit_command(
                params.get("arguments"), self._session_generation
            )
            if command is None:
                await self._error(
                    request_id, -32602, "Invalid hermes.cockpit.command arguments"
                )
            else:
                await self._handle_cockpit_command(request_id, command)
            self._remember_completed(key)
            return
        if tool_name == CONVERSATE_CUES_TOOL:
            cue_request = _normalize_conversate_cues(params.get("arguments"))
            if cue_request is None:
                await self._error(
                    request_id, -32602, "Invalid hermes.conversate.cues arguments"
                )
                self._remember_completed(key)
                return
            await self._start_conversate_cues(request_id, key, cue_request)
            return
        voice_turn = _normalize_voice_turn(params.get("arguments"))
        if voice_turn is None:
            await self._error(request_id, -32602, "Invalid hermes.voice.turn arguments")
            self._remember_completed(key)
            return
        if self._pending:
            await self._error(
                request_id,
                -32003,
                "Another voice turn is active; cancel its exact request id first",
            )
            self._remember_completed(key)
            return
        if self._next_call_generation > _MAX_SAFE_INTEGER:
            await self._error(request_id, -32004, "Host MCP generation exhausted")
            self._remember_completed(key)
            return

        binding = HostTurnBinding(
            request_id=request_id,
            request_key=key,
            session_generation=self._session_generation,
            call_generation=self._next_call_generation,
            turn_id=voice_turn.turn_id,
        )
        self._next_call_generation += 1
        self._pending[key] = _PendingTurn(binding=binding)
        try:
            await self._on_voice_turn(binding, voice_turn)
        except Exception:
            current = self._pending.get(key)
            if current is not None and current.binding == binding:
                self._pending.pop(key, None)
                self._remember_completed(key)
                await self._error(
                    request_id, -32603, "Hermes voice turn could not be started"
                )

    async def _start_conversate_cues(
        self,
        request_id: JsonRpcId,
        key: str,
        request: HostConversateCuesRequest,
    ) -> None:
        """Start an isolated latest-wins cue job without blocking frame intake."""
        for pending in list(self._pending_conversate_cues.values()):
            await self._cancel_conversate_cues(
                pending, "Superseded by newer Conversate transcript"
            )
        if self._closed or self._on_conversate_cues is None:
            await self._error(request_id, -32002, "Conversate cues are unavailable")
            self._remember_completed(key)
            return
        pending = _PendingConversateCues(
            request_id=request_id,
            request_key=key,
            request=request,
        )
        self._pending_conversate_cues[key] = pending
        pending.task = asyncio.create_task(self._run_conversate_cues(pending))
        pending.task.add_done_callback(self._consume_background_task)

    async def _run_conversate_cues(
        self, pending: _PendingConversateCues
    ) -> None:
        try:
            callback = self._on_conversate_cues
            if callback is None:
                raise RuntimeError("Conversate cues are unavailable")
            async with asyncio.timeout(_CONVERSATE_CUE_DEADLINE_SECONDS):
                raw_cues = await callback(pending.request)
            cues = _normalize_generated_cues(raw_cues)
            if cues is None:
                raise ValueError("auxiliary model returned an invalid cue schema")
            if not self._take_pending_conversate_cues(pending):
                return
            terminal = {
                "sessionId": pending.request.session_id,
                "revision": pending.request.revision,
                "cues": cues,
            }
            await self._result(
                pending.request_id,
                {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                terminal,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        }
                    ],
                    "structuredContent": copy.deepcopy(terminal),
                    "isError": False,
                },
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            await self._fail_conversate_cues(
                pending, "Conversate cue request timed out"
            )
        except Exception:
            # Provider exceptions can include request metadata. Keep this lane's
            # diagnostic fixed so transcript text cannot enter bridge logs.
            logger.debug("Conversate auxiliary cue generation failed safely")
            await self._fail_conversate_cues(
                pending, "Conversate cues are temporarily unavailable"
            )

    def _take_pending_conversate_cues(
        self, pending: _PendingConversateCues
    ) -> bool:
        current = self._pending_conversate_cues.get(pending.request_key)
        if self._closed or current is not pending:
            return False
        self._pending_conversate_cues.pop(pending.request_key, None)
        self._remember_completed(pending.request_key)
        return True

    async def _fail_conversate_cues(
        self, pending: _PendingConversateCues, message: str
    ) -> None:
        if not self._take_pending_conversate_cues(pending):
            return
        await self._result(
            pending.request_id,
            {
                "content": [{"type": "text", "text": message}],
                "isError": True,
            },
        )

    async def _cancel_conversate_cues(
        self, pending: _PendingConversateCues, reason: str
    ) -> None:
        if not self._take_pending_conversate_cues(pending):
            return
        task = pending.task
        if task is not None and not task.done():
            task.cancel()
            # Provider clients should cooperate with cancellation, but a buggy
            # coroutine may suppress CancelledError. Drain cooperative cleanup
            # briefly in the background and then detach; frame intake and the
            # latest request must never wait on that provider task.
            drain = asyncio.create_task(self._drain_cancelled_conversate_task(task))
            drain.add_done_callback(self._consume_background_task)
        await self._result(
            pending.request_id,
            {
                "content": [{"type": "text", "text": reason}],
                "isError": True,
            },
        )

    @staticmethod
    async def _drain_cancelled_conversate_task(task: asyncio.Task[None]) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=_CONVERSATE_CUE_CANCEL_GRACE_SECONDS,
            )
        except (asyncio.CancelledError, TimeoutError):
            pass
        except Exception:
            pass

    @staticmethod
    def _consume_background_task(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def _status_document(self) -> dict[str, Any]:
        pending = next(iter(self._pending.values()), None)
        voice_state = (
            "cancelling"
            if pending is not None and pending.cancellation_requested
            else "running"
            if pending is not None
            else "idle"
        )
        return {
            "schemaVersion": 1,
            "connectionGeneration": self._session_generation,
            "profile": self._profile,
            "transport": {"state": "online", "authenticated": True},
            "sessionMcp": {
                "state": "ready",
                "voiceTurnState": voice_state,
                "legacyChatFallback": False,
            },
            "cockpit": {
                "state": "online",
                "transport": "mcp-resource",
                "projection": "session-snapshot",
                "sharedSessions": len(self._cockpit_sessions),
                "commandsAvailable": bool(
                    self._on_cockpit_command
                    and any(
                        record.document["state"]
                        not in {"completed", "failed", "interrupted"}
                        for record in self._cockpit_sessions.values()
                    )
                ),
            },
            "companion": {
                "state": "unavailable",
                "reason": "backend-authority-absent",
                "commandsAvailable": False,
            },
        }

    def _cockpit_snapshot(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        changed = False
        for record in self._cockpit_sessions.values():
            pending = record.document["pending"]
            retained = [item for item in pending if item["expires_at_ms"] > now_ms]
            if len(retained) != len(pending):
                expired = {item["request_id"] for item in pending} - {
                    item["request_id"] for item in retained
                }
                for request_id in expired:
                    record.backends.pop(request_id, None)
                record.document["pending"] = retained
                if not retained and record.document["state"] == "waiting_human":
                    record.document["state"] = "running"
                record.document["revision"] += 1
                record.document["updated_at_ms"] = now_ms
                changed = True
        if changed:
            self._cockpit_sequence += 1
        snapshot = {
            "v": 1,
            "chan": "cockpit",
            "type": "snapshot",
            "connection_generation": self._session_generation,
            "sequence": self._cockpit_sequence,
            "sessions": [
                copy.deepcopy(record.document)
                for record in self._cockpit_sessions.values()
            ],
        }
        return self._bound_cockpit_snapshot(snapshot)

    @staticmethod
    def _cockpit_snapshot_size(snapshot: dict[str, Any]) -> int:
        return len(
            json.dumps(
                snapshot,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )

    @classmethod
    def _bound_cockpit_snapshot(
        cls, snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        """Fit the authoritative projection inside both MCP and WSS bounds."""
        if cls._cockpit_snapshot_size(snapshot) <= _MAX_COCKPIT_SNAPSHOT_BYTES:
            return snapshot

        sessions = snapshot["sessions"]
        # Timeline rows are informational, never authority. Discard the oldest
        # rows from terminal/old sessions first while retaining live requests.
        while cls._cockpit_snapshot_size(snapshot) > _MAX_COCKPIT_SNAPSHOT_BYTES:
            with_timeline = [item for item in sessions if item["timeline"]]
            if not with_timeline:
                break
            selected = min(
                with_timeline,
                key=lambda item: (
                    item["state"] not in {"completed", "failed", "interrupted"},
                    item["updated_at_ms"],
                    item["session_id"],
                ),
            )
            selected["timeline"].pop(0)

        # At the theoretical pending maximum, show the oldest reviewed work
        # first. Hidden requests retain no phone authority and become visible
        # after a sequence-changing resolution/expiry removes earlier work.
        while cls._cockpit_snapshot_size(snapshot) > _MAX_COCKPIT_SNAPSHOT_BYTES:
            with_pending = [item for item in sessions if item["pending"]]
            if not with_pending:
                break
            selected = max(
                with_pending,
                key=lambda item: (
                    len(item["pending"]),
                    -item["updated_at_ms"],
                    item["session_id"],
                ),
            )
            selected["pending"].pop()

        # Session metadata alone is far below the limit under the fixed field
        # bounds. This final fail-closed guard protects against future schema
        # growth without ever emitting a document the phone must reject.
        while (
            cls._cockpit_snapshot_size(snapshot) > _MAX_COCKPIT_SNAPSHOT_BYTES
            and sessions
        ):
            selected = min(
                sessions,
                key=lambda item: (
                    item["state"] not in {"completed", "failed", "interrupted"},
                    item["updated_at_ms"],
                    item["session_id"],
                ),
            )
            sessions.remove(selected)
        return snapshot

    async def open_cockpit_turn(
        self,
        binding: HostTurnBinding,
        *,
        generation: int,
        user_text: str,
    ) -> str | None:
        """Explicitly share one authenticated G2 turn with the phone Cockpit."""
        if (
            self._closed
            or binding.session_generation != self._session_generation
            or binding.request_key not in self._pending
            or not 1 <= generation <= _MAX_SAFE_INTEGER
        ):
            return None
        while len(self._cockpit_sessions) >= _MAX_COCKPIT_SESSIONS:
            removable = next(
                (
                    session_id
                    for session_id, record in self._cockpit_sessions.items()
                    if record.document["state"] in {"completed", "failed", "interrupted"}
                ),
                None,
            )
            if removable is None:
                return None
            stale = self._cockpit_sessions.pop(removable)
            self._cockpit_binding_sessions.pop(stale.binding.request_key, None)
        session_id = _cockpit_id(
            "session",
            f"{self._session_generation}:{binding.request_key}:{binding.call_generation}",
        )
        now_ms = int(time.time() * 1000)
        row_id = _cockpit_id("timeline", f"{session_id}:user")
        document = {
            "session_id": session_id,
            "generation": generation,
            "revision": 1,
            "title": _cockpit_text(user_text, 80, "G2 assistant request"),
            "state": "running",
            "updated_at_ms": now_ms,
            "timeline": [
                {
                    "id": row_id,
                    "kind": "user",
                    "text": _cockpit_text(user_text, 240, "Spoken G2 request"),
                    "status": "done",
                }
            ],
            "pending": [],
        }
        self._cockpit_sessions[session_id] = _CockpitSessionRecord(
            binding=binding,
            document=document,
            backends={},
        )
        self._cockpit_binding_sessions[binding.request_key] = session_id
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return session_id

    async def open_cockpit_question(
        self,
        binding: HostTurnBinding,
        *,
        clarify_id: str,
        session_key: str,
        question: str,
        choices: list[Any],
    ) -> bool:
        record = self._record_for_binding(binding)
        if record is None or not choices or len(choices) > 8:
            return False
        if len(record.document["pending"]) >= _MAX_COCKPIT_PENDING:
            return False
        request_id = _cockpit_id(
            "request", f"{record.document['session_id']}:question:{clarify_id}"
        )
        if request_id in record.backends:
            return True
        title = _cockpit_review_text(question)
        if title is None:
            return False
        projected_choices: list[dict[str, str]] = []
        choice_values: dict[str, str] = {}
        for index, raw_choice in enumerate(choices):
            original = str(raw_choice)
            label = _cockpit_review_text(original)
            if label is None:
                return False
            choice_id = _cockpit_id("choice", f"{request_id}:{index}")
            projected_choices.append(
                {
                    "id": choice_id,
                    "label": label,
                }
            )
            # label is an identity transform: the dispatched answer is exactly
            # what the wearer reviewed, never the pre-review provider value.
            choice_values[choice_id] = label
        interaction = {
            "request_id": request_id,
            "nonce": _cockpit_id("nonce"),
            "kind": "question",
            "title": title,
            "expires_at_ms": int(time.time() * 1000) + _COCKPIT_INTERACTION_TTL_MS,
            "choices": projected_choices,
        }
        record.backends[request_id] = {
            "kind": "question",
            "clarify_id": clarify_id,
            "session_key": session_key,
            "choice_values": choice_values,
        }
        record.document["pending"].append(interaction)
        self._touch_record(record, state="waiting_human")
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return True

    async def open_cockpit_text_question(
        self,
        binding: HostTurnBinding,
        *,
        clarify_id: str,
        session_key: str,
        question: str,
    ) -> bool:
        """Project one negotiated, bounded open clarification."""
        record = self._record_for_binding(binding)
        if (
            not self._cockpit_free_text_enabled
            or record is None
            or len(record.document["pending"]) >= _MAX_COCKPIT_PENDING
        ):
            return False
        request_id = _cockpit_id(
            "request", f"{record.document['session_id']}:text-question:{clarify_id}"
        )
        if request_id in record.backends:
            return True
        title = _cockpit_review_text(question)
        if title is None:
            return False
        interaction = {
            "request_id": request_id,
            "nonce": _cockpit_id("nonce"),
            "kind": "text_question",
            "title": title,
            "expires_at_ms": int(time.time() * 1000)
            + _COCKPIT_INTERACTION_TTL_MS,
            "max_length": _COCKPIT_REVIEW_TEXT_MAX_SCALARS,
        }
        record.backends[request_id] = {
            "kind": "text_question",
            "clarify_id": clarify_id,
            "session_key": session_key,
        }
        record.document["pending"].append(interaction)
        self._touch_record(record, state="waiting_human")
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return True

    async def open_cockpit_permission(
        self,
        binding: HostTurnBinding,
        *,
        session_key: str,
        approval_request_id: str,
        command: str,
        description: str,
        allow_once: bool = True,
    ) -> bool:
        record = self._record_for_binding(binding)
        if record is None or len(record.document["pending"]) >= _MAX_COCKPIT_PENDING:
            return False
        request_id = _cockpit_id(
            "request",
            f"{record.document['session_id']}:permission:{approval_request_id}",
        )
        if request_id in record.backends:
            return True
        target = _cockpit_review_text(command)
        effect = _cockpit_review_text(description)
        render_safe = target is not None and effect is not None
        interaction = {
            "request_id": request_id,
            "nonce": _cockpit_id("nonce"),
            "kind": "permission",
            "title": "Command approval required",
            "expires_at_ms": int(time.time() * 1000) + _COCKPIT_INTERACTION_TTL_MS,
            "action": "other_bounded",
            "target": target if render_safe else "Details unavailable",
            "effect": effect if render_safe else "Approval disabled: details exceed safe display bounds",
            "choices": ["deny", "allow_once"] if allow_once and render_safe else ["deny"],
        }
        record.backends[request_id] = {
            "kind": "permission",
            "session_key": session_key,
            "approval_request_id": approval_request_id,
        }
        record.document["pending"].append(interaction)
        self._touch_record(record, state="waiting_human")
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return True

    def has_cockpit_approval_backend(self, approval_request_id: str) -> bool:
        return any(
            backend.get("kind") == "permission"
            and backend.get("approval_request_id") == approval_request_id
            for record in self._cockpit_sessions.values()
            for backend in record.backends.values()
        )

    async def discard_cockpit_turn(self, binding: HostTurnBinding) -> bool:
        """Remove a projection whose bound gateway turn failed to start."""
        record = self._record_for_binding(binding)
        if record is None:
            return False
        session_id = record.document["session_id"]
        self._cockpit_sessions.pop(session_id, None)
        self._cockpit_binding_sessions.pop(binding.request_key, None)
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return True

    async def finish_cockpit_turn(
        self,
        binding: HostTurnBinding,
        *,
        text: str,
        stop_reason: str,
        error: str | None = None,
    ) -> bool:
        record = self._record_for_binding(binding)
        if record is None or stop_reason not in _STOP_REASONS:
            return False
        document = record.document
        document["pending"] = []
        record.backends.clear()
        if stop_reason == "end_turn":
            document["state"] = "completed"
            if text:
                document["timeline"].append(
                    {
                        "id": _cockpit_id(
                            "timeline", f"{document['session_id']}:assistant"
                        ),
                        "kind": "assistant",
                        "text": _cockpit_text(text, 240, "Hermes completed the request"),
                        "status": "done",
                    }
                )
        else:
            document["state"] = "interrupted" if stop_reason == "cancelled" else "failed"
            document["timeline"].append(
                {
                    "id": _cockpit_id(
                        "timeline", f"{document['session_id']}:{stop_reason}"
                    ),
                    "kind": "status",
                    "text": _cockpit_text(
                        error or ("Turn interrupted" if stop_reason == "cancelled" else "Turn failed"),
                        240,
                        "Turn ended",
                    ),
                    "status": "failed",
                }
            )
        if len(document["timeline"]) > _MAX_COCKPIT_TIMELINE:
            document["timeline"] = document["timeline"][-_MAX_COCKPIT_TIMELINE:]
        self._touch_record(record)
        self._cockpit_sequence += 1
        await self._notify_cockpit_updated()
        return True

    def _record_for_binding(
        self, binding: HostTurnBinding
    ) -> _CockpitSessionRecord | None:
        if binding.session_generation != self._session_generation:
            return None
        session_id = self._cockpit_binding_sessions.get(binding.request_key)
        record = self._cockpit_sessions.get(session_id or "")
        return record if record is not None and record.binding == binding else None

    @staticmethod
    def _touch_record(
        record: _CockpitSessionRecord, *, state: str | None = None
    ) -> None:
        if state is not None:
            if state not in _COCKPIT_STATES:
                raise ValueError("invalid Cockpit state")
            record.document["state"] = state
        record.document["revision"] += 1
        record.document["updated_at_ms"] = int(time.time() * 1000)

    async def _handle_cockpit_command(
        self, request_id: JsonRpcId, command: dict[str, Any]
    ) -> None:
        command_id = command["command_id"]
        record = self._cockpit_sessions.get(command["session_id"])
        backend: dict[str, Any] | None = None
        outcome = "rejected"
        code: str | None = None
        if command_id in self._cockpit_commands:
            outcome, code = "duplicate", "duplicate_command"
        elif len(self._cockpit_commands) >= _MAX_REQUESTS_PER_SESSION:
            outcome, code = "rejected", "command_budget_exhausted"
        else:
            self._cockpit_commands.add(command_id)
            code = self._validate_cockpit_authority(record, command)
            if code is None and self._on_cockpit_command is not None:
                if command["type"] in {
                    "answer",
                    "answer_text",
                    "permission_decide",
                }:
                    backend = record.backends.get(command["request_id"]) if record else None
                try:
                    outcome, code = await self._on_cockpit_command(command, backend)
                except Exception:
                    outcome, code = "outcome_unknown", "backend_dispatch_failed"
                if outcome not in {"accepted", "rejected", "duplicate", "outcome_unknown"}:
                    outcome, code = "outcome_unknown", "invalid_backend_receipt"
            elif code is None:
                code = "backend_authority_unavailable"
        if outcome == "accepted" and record is not None:
            if command["type"] in {
                "answer",
                "answer_text",
                "permission_decide",
            }:
                request = command["request_id"]
                record.document["pending"] = [
                    item for item in record.document["pending"] if item["request_id"] != request
                ]
                record.backends.pop(request, None)
                self._touch_record(
                    record,
                    state="waiting_human" if record.document["pending"] else "running",
                )
            elif command["type"] == "interrupt":
                self._touch_record(record, state="interrupting")
        self._cockpit_sequence += 1
        receipt: dict[str, Any] = {
            "v": 1,
            "chan": "cockpit",
            "type": "command_receipt",
            "sequence": self._cockpit_sequence,
            "command_id": command_id,
            "session_id": command["session_id"],
            "generation": command["generation"],
            "outcome": outcome,
        }
        if code:
            receipt["code"] = _cockpit_text(code, 80, "command_failed")
        await self._result(
            request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(receipt, ensure_ascii=False, separators=(",", ":")),
                    }
                ],
                "structuredContent": copy.deepcopy(receipt),
                "isError": False,
            },
        )
        await self._notify_cockpit_updated()

    def _validate_cockpit_authority(
        self,
        record: _CockpitSessionRecord | None,
        command: dict[str, Any],
    ) -> str | None:
        if record is None or record.document["generation"] != command["generation"]:
            return "stale_session"
        state = record.document["state"]
        if state in {"completed", "failed", "interrupted"}:
            return "session_terminal"
        command_type = command["type"]
        if command_type == "steer":
            return None if state == "running" else "session_not_running"
        if command_type == "interrupt":
            return None if state in {"running", "waiting_human"} else "session_not_interruptible"
        request = next(
            (
                item
                for item in record.document["pending"]
                if item["request_id"] == command["request_id"]
            ),
            None,
        )
        expected_kind = {
            "answer": "question",
            "answer_text": "text_question",
            "permission_decide": "permission",
        }.get(command_type)
        if request is None or request["kind"] != expected_kind:
            return "stale_request"
        if request["nonce"] != command["nonce"]:
            return "stale_nonce"
        if int(time.time() * 1000) >= request["expires_at_ms"]:
            return "request_expired"
        if command_type == "answer" and not any(
            choice["id"] == command["choice_id"] for choice in request["choices"]
        ):
            return "invalid_choice"
        if command_type == "answer_text" and (
            len(command["text"]) > request["max_length"]
            or _cockpit_review_text(command["text"]) != command["text"]
        ):
            return "invalid_text_answer"
        if command_type == "permission_decide" and command["decision"] not in request["choices"]:
            return "invalid_decision"
        return None

    async def _notify_cockpit_updated(self) -> None:
        if not self.initialized or not self._cockpit_subscribed:
            return
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/resources/updated",
                    "params": {"uri": COCKPIT_STATE_RESOURCE_URI},
                }
            )
        except Exception:
            # A resource notification is only an invalidation hint. The full
            # snapshot remains readable, and failure to send the hint must not
            # abort voice-turn start or suppress its authoritative terminal
            # CallToolResult.
            logger.warning("G2 Cockpit resource update notification failed safely")

    async def complete_turn(
        self,
        binding: HostTurnBinding,
        *,
        text: str,
        stop_reason: str,
        error: str | None = None,
    ) -> bool:
        """Resolve exactly one still-current request with its final turn state."""
        if stop_reason not in _STOP_REASONS:
            raise ValueError("invalid Hermes voice turn stop reason")
        if self._closed or binding.session_generation != self._session_generation:
            return False
        pending = self._pending.get(binding.request_key)
        if pending is None or pending.binding != binding:
            return False
        self._pending.pop(binding.request_key, None)
        self._remember_completed(binding.request_key)

        is_error = error is not None or stop_reason in {"error", "cancelled"}
        final_text = _normalize_final_text(
            error
            if error is not None
            else text
            if text
            else "Turn cancelled."
            if stop_reason == "cancelled"
            else ""
        )
        terminal = {
            "turnId": binding.turn_id,
            "text": final_text,
            "stopReason": stop_reason,
        }
        await self._result(
            binding.request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": final_text
                        if is_error
                        else json.dumps(
                            terminal,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    }
                ],
                "structuredContent": {
                    **terminal,
                    "generation": binding.call_generation,
                },
                "isError": is_error,
            },
        )
        return True

    async def _handle_cancel_notification(self, params: Any) -> None:
        if self._lifecycle != "initialized" or not isinstance(params, dict):
            return
        if set(params) - {"requestId", "reason"} or "requestId" not in params:
            return
        target_id = params.get("requestId")
        if not _valid_request_id(target_id):
            return
        reason = params.get("reason")
        if reason is not None and _safe_one_line(
            reason, max_scalars=160, allow_empty=True
        ) is None:
            return
        pending = self._pending.get(_request_key(target_id))
        if pending is None:
            cue_pending = self._pending_conversate_cues.get(
                _request_key(target_id)
            )
            if cue_pending is not None:
                await self._cancel_conversate_cues(
                    cue_pending, "Conversate cue request cancelled"
                )
            return
        if pending.cancellation_requested:
            return
        pending.cancellation_requested = True
        try:
            await self._on_cancel(pending.binding)
        except Exception:
            await self.complete_turn(
                pending.binding,
                text="",
                stop_reason="error",
                error="Hermes voice turn cancellation failed",
            )

    @staticmethod
    def _valid_initialize(params: Any) -> bool:
        if not isinstance(params, dict) or set(params) != {
            "protocolVersion",
            "capabilities",
            "clientInfo",
        }:
            return False
        if params.get("protocolVersion") != MCP_PROTOCOL_VERSION:
            return False
        if not isinstance(params.get("capabilities"), dict):
            return False
        try:
            if (
                len(
                    json.dumps(
                        params["capabilities"],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
                > _MAX_INITIALIZE_CAPABILITIES_BYTES
            ):
                return False
        except (TypeError, ValueError, UnicodeError):
            return False
        client = params.get("clientInfo")
        if not isinstance(client, dict) or set(client) != {"name", "version"}:
            return False
        name = _safe_one_line(client.get("name"), max_scalars=80)
        version = _safe_one_line(client.get("version"), max_scalars=40)
        return name is not None and version is not None

    def close(self) -> None:
        self._closed = True
        self._lifecycle = "closed"
        for pending in self._pending_conversate_cues.values():
            if pending.task is not None and not pending.task.done():
                pending.task.cancel()
        self._pending.clear()
        self._pending_conversate_cues.clear()
        self._completed.clear()
        self._cockpit_sessions.clear()
        self._cockpit_binding_sessions.clear()
        self._cockpit_commands.clear()
        self._cockpit_subscribed = False

    def _remember_completed(self, key: str) -> None:
        # Never evict a completed ID within the connection. A delayed standard
        # notifications/cancelled carries only requestId; allowing reuse would
        # let a stale cancellation target a newer call. The session-wide
        # request budget above keeps this exact-ID set strictly bounded.
        self._completed.add(key)

    async def _result(self, request_id: JsonRpcId, result: dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def _error(self, request_id: JsonRpcId, code: int, message: str) -> None:
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            }
        )
