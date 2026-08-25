"""Private fixed-route handlers for the standalone G2 workflow MCP relay."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import errno
import json
import hashlib
import logging
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import unicodedata
from datetime import date
from enum import Enum
from typing import Any, Callable

try:  # The G2 bridge host is POSIX; non-POSIX hosts fail closed at use time.
    import fcntl
except ImportError:  # pragma: no cover - exercised only on unsupported hosts
    fcntl = None  # type: ignore[assignment]

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

_KANBAN_OPERATION_ID = re.compile(r"^kanban\.[a-f0-9]{32}$")
_KANBAN_TASK_ID = re.compile(r"^t_[a-f0-9]{8}$")
_KANBAN_BOARD_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_KANBAN_TITLE_MAX_SCALARS = 120
_KANBAN_TITLE_MAX_BYTES = 480
_KANBAN_BODY_MAX_SCALARS = 2_000
_KANBAN_BODY_MAX_BYTES = 8_000
_KANBAN_BOARD_INPUT_MAX_SCALARS = 80
_KANBAN_BOARD_INPUT_MAX_BYTES = 320
_KANBAN_BOARD_LIST_LIMIT = 16
_KANBAN_CREATED_BY = "g2-workflows"
_KANBAN_IDEMPOTENCY_PREFIX = "g2-kanban:"
_KANBAN_LEDGER_SCHEMA_VERSION = 1
_KANBAN_LEDGER_LOCK_TIMEOUT_SECONDS = 0.5
_KANBAN_LEDGER_LOCK_POLL_SECONDS = 0.02
_KANBAN_BOARD_BUSY_TIMEOUT_MS = 25
_KANBAN_LEDGER_STATES = frozenset({"PREPARED", "MUTATING", "COMMITTED"})
_KANBAN_OPERATION_ERRORS = {
    "operation_conflict": (
        "Kanban operation identity is already bound to different arguments"
    ),
    "board_generation_changed": (
        "The selected Kanban board changed before the card was created"
    ),
    "operation_outcome_unknown": (
        "Kanban creation may have started but its exact outcome is unknown"
    ),
}

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


class _KanbanOperationError(RuntimeError):
    """One fixed, content-free Kanban operation failure."""

    def __init__(self, error_code: str, commit_state: str) -> None:
        if error_code not in _KANBAN_OPERATION_ERRORS:
            raise ValueError("unsupported Kanban operation error")
        if commit_state not in {"not_committed", "unknown", "committed"}:
            raise ValueError("unsupported Kanban commit state")
        super().__init__(_KANBAN_OPERATION_ERRORS[error_code])
        self.error_code = error_code
        self.commit_state = commit_state


class _KanbanBoardSelectionError(RuntimeError):
    """An exact board selection failed before an operation intent existed."""

    def __init__(
        self,
        error_code: str,
        available_boards: list[dict[str, str]],
        boards_truncated: bool,
    ) -> None:
        if error_code not in {"board_not_found", "board_ambiguous"}:
            raise ValueError("unsupported Kanban board selection error")
        super().__init__(error_code)
        self.error_code = error_code
        self.available_boards = available_boards
        self.boards_truncated = boards_truncated


@dataclass(frozen=True)
class _KanbanBoardGeneration:
    slug: str
    db_path: str
    fingerprint: str


@dataclass(frozen=True)
class _KanbanLedgerEntry:
    operation_id: str
    payload_digest: str
    board_slug: str
    board_db_path: str
    board_generation: str
    state: str
    task_id: str | None
    created_status: str | None


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


def _normalize_kanban_line(
    value: Any,
    *,
    max_scalars: int,
    max_bytes: int,
) -> str | None:
    """Return bounded NFC text that is inert when stored in a Kanban card."""
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
        encoded = text.encode("utf-8")
    except (TypeError, UnicodeError):
        return None
    if (
        not text
        or len(text) > max_scalars
        or len(encoded) > max_bytes
    ):
        return None
    return text


def _canonical_kanban_boards(
    kb: Any,
) -> tuple[list[dict[str, str]], list[dict[str, str]], bool]:
    """Return exact matching data plus a bounded presentation projection."""
    projected: list[dict[str, str]] = []
    for metadata in kb.list_boards(include_archived=False):
        if not isinstance(metadata, dict):
            continue
        slug = metadata.get("slug")
        if not isinstance(slug, str) or _KANBAN_BOARD_SLUG.fullmatch(slug) is None:
            continue
        name = _normalize_kanban_line(
            metadata.get("name"),
            max_scalars=_KANBAN_BOARD_INPUT_MAX_SCALARS,
            max_bytes=_KANBAN_BOARD_INPUT_MAX_BYTES,
        )
        projected.append({"slug": slug, "name": name or slug})
    projected.sort(key=lambda item: (item["slug"] != "default", item["slug"]))
    available = projected[:_KANBAN_BOARD_LIST_LIMIT]
    return projected, available, len(projected) > _KANBAN_BOARD_LIST_LIMIT


def _resolve_exact_kanban_board(
    requested: str,
    boards: list[dict[str, str]],
) -> tuple[str | None, str | None]:
    """Resolve one exact case-insensitive slug/display-name match."""
    identity = unicodedata.normalize("NFC", requested).casefold()
    matches = {
        board["slug"]
        for board in boards
        if identity
        in {
            unicodedata.normalize("NFC", board["slug"]).casefold(),
            unicodedata.normalize("NFC", board["name"]).casefold(),
        }
    }
    if not matches:
        return None, "board_not_found"
    if len(matches) != 1:
        return None, "board_ambiguous"
    return next(iter(matches)), None


def _kanban_payload_digest(
    *, title: str, body: str | None, board_input: str
) -> str:
    """Bind an operation to normalized arguments without storing their text."""
    canonical = json.dumps(
        {
            "version": 1,
            "title": title,
            "body": body,
            "board": board_input,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _kanban_ledger_directory() -> Path:
    """Return a private, profile-owned directory or fail closed."""
    if fcntl is None or not hasattr(os, "getuid"):
        raise RuntimeError("Kanban operation ledger requires POSIX flock")
    raw_home = str(os.environ.get("HERMES_HOME") or "").strip()
    profile = (
        Path(raw_home).expanduser() if raw_home else Path.home() / ".hermes"
    ).resolve()
    profile.mkdir(mode=0o700, parents=True, exist_ok=True)
    state_root = profile / "state"
    state_root.mkdir(mode=0o700, exist_ok=True)
    state_info = state_root.lstat()
    if (
        stat.S_ISLNK(state_info.st_mode)
        or not stat.S_ISDIR(state_info.st_mode)
        or state_info.st_uid != os.getuid()
    ):
        raise RuntimeError("Hermes profile state directory is unsafe")
    ledger_dir = state_root / "g2-workflows"
    ledger_dir.mkdir(mode=0o700, exist_ok=True)
    ledger_info = ledger_dir.lstat()
    if (
        stat.S_ISLNK(ledger_info.st_mode)
        or not stat.S_ISDIR(ledger_info.st_mode)
        or ledger_info.st_uid != os.getuid()
    ):
        raise RuntimeError("Kanban operation ledger directory is unsafe")
    os.chmod(ledger_dir, 0o700)
    return ledger_dir


def _secure_private_file(path: Path) -> int:
    """Open one owner-only, non-symlink regular file and return its fd."""
    required_flags = ("O_CLOEXEC", "O_NOFOLLOW")
    if any(not hasattr(os, name) for name in required_flags):
        raise RuntimeError("Secure Kanban operation files are unsupported")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
        ):
            raise RuntimeError("Kanban operation file is unsafe")
        os.fchmod(descriptor, 0o600)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


@contextlib.contextmanager
def _kanban_operation_lock(cancelled: threading.Event):
    """Acquire the sole profile operation lock with a short hard deadline."""
    ledger_dir = _kanban_ledger_directory()
    lock_path = ledger_dir / "kanban-operations.lock"
    descriptor = _secure_private_file(lock_path)
    acquired = False
    deadline = time.monotonic() + _KANBAN_LEDGER_LOCK_TIMEOUT_SECONDS
    try:
        while True:
            if cancelled.is_set():
                raise PermissionError("G2 turn was cancelled")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError("Kanban operation ledger is busy") from None
                time.sleep(_KANBAN_LEDGER_LOCK_POLL_SECONDS)
        yield ledger_dir
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _connect_kanban_ledger(ledger_dir: Path) -> sqlite3.Connection:
    """Open and verify the private durable ledger under the held flock."""
    db_path = ledger_dir / "kanban-operations.sqlite3"
    descriptor = _secure_private_file(db_path)
    before = os.fstat(descriptor)
    os.close(descriptor)
    conn = sqlite3.connect(
        db_path,
        isolation_level=None,
        timeout=0.1,
    )
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=100")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA trusted_schema=OFF")
        conn.execute("PRAGMA secure_delete=ON")
        after = db_path.lstat()
        if (
            stat.S_ISLNK(after.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or after.st_uid != os.getuid()
            or after.st_nlink != 1
            or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RuntimeError("Kanban operation ledger identity changed")
        version_row = conn.execute("PRAGMA user_version").fetchone()
        version = int(version_row[0]) if version_row is not None else -1
        if version not in {0, _KANBAN_LEDGER_SCHEMA_VERSION}:
            raise RuntimeError("Kanban operation ledger schema is unsupported")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS operations (
                operation_id TEXT PRIMARY KEY,
                payload_digest TEXT NOT NULL,
                board_slug TEXT NOT NULL,
                board_db_path TEXT NOT NULL,
                board_generation TEXT NOT NULL,
                state TEXT NOT NULL,
                task_id TEXT,
                created_status TEXT,
                created_assignee TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                CHECK (state IN ('PREPARED', 'MUTATING', 'COMMITTED')),
                CHECK (created_assignee IS NULL)
            );
            """
        )
        if version == 0:
            conn.execute(f"PRAGMA user_version={_KANBAN_LEDGER_SCHEMA_VERSION}")
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(operations)")
        }
        if columns != {
            "operation_id",
            "payload_digest",
            "board_slug",
            "board_db_path",
            "board_generation",
            "state",
            "task_id",
            "created_status",
            "created_assignee",
            "created_at",
            "updated_at",
        }:
            raise RuntimeError("Kanban operation ledger schema is malformed")
        check = conn.execute("PRAGMA quick_check").fetchone()
        if check is None or str(check[0]).lower() != "ok":
            raise RuntimeError("Kanban operation ledger failed integrity check")
        os.chmod(db_path, 0o600)
        return conn
    except Exception:
        conn.close()
        raise


@contextlib.contextmanager
def _ledger_write(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        with contextlib.suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _read_kanban_ledger_entry(
    conn: sqlite3.Connection, operation_id: str
) -> _KanbanLedgerEntry | None:
    row = conn.execute(
        "SELECT operation_id, payload_digest, board_slug, board_db_path, "
        "board_generation, state, task_id, created_status, created_assignee "
        "FROM operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    if row is None:
        return None
    entry = _KanbanLedgerEntry(
        operation_id=str(row["operation_id"]),
        payload_digest=str(row["payload_digest"]),
        board_slug=str(row["board_slug"]),
        board_db_path=str(row["board_db_path"]),
        board_generation=str(row["board_generation"]),
        state=str(row["state"]),
        task_id=str(row["task_id"]) if row["task_id"] is not None else None,
        created_status=(
            str(row["created_status"])
            if row["created_status"] is not None
            else None
        ),
    )
    if (
        entry.operation_id != operation_id
        or _KANBAN_OPERATION_ID.fullmatch(entry.operation_id) is None
        or re.fullmatch(r"[a-f0-9]{64}", entry.payload_digest) is None
        or _KANBAN_BOARD_SLUG.fullmatch(entry.board_slug) is None
        or re.fullmatch(r"[a-f0-9]{64}", entry.board_generation) is None
        or entry.state not in _KANBAN_LEDGER_STATES
        or row["created_assignee"] is not None
        or (
            entry.state == "COMMITTED"
            and (
                entry.task_id is None
                or _KANBAN_TASK_ID.fullmatch(entry.task_id) is None
                or entry.created_status != "blocked"
            )
        )
        or (
            entry.state != "COMMITTED"
            and (entry.task_id is not None or entry.created_status is not None)
        )
    ):
        raise RuntimeError("Kanban operation ledger row is malformed")
    return entry


def _insert_kanban_intent(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    payload_digest: str,
    generation: _KanbanBoardGeneration,
) -> _KanbanLedgerEntry:
    now = int(time.time())
    with _ledger_write(conn):
        conn.execute(
            "INSERT INTO operations (operation_id, payload_digest, board_slug, "
            "board_db_path, board_generation, state, task_id, created_status, "
            "created_assignee, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'PREPARED', NULL, NULL, NULL, ?, ?)",
            (
                operation_id,
                payload_digest,
                generation.slug,
                generation.db_path,
                generation.fingerprint,
                now,
                now,
            ),
        )
    entry = _read_kanban_ledger_entry(conn, operation_id)
    if entry is None:
        raise RuntimeError("Kanban operation intent was not persisted")
    return entry


def _mark_kanban_mutating(
    conn: sqlite3.Connection, operation_id: str
) -> _KanbanLedgerEntry:
    with _ledger_write(conn):
        cursor = conn.execute(
            "UPDATE operations SET state = 'MUTATING', updated_at = ? "
            "WHERE operation_id = ? AND state = 'PREPARED'",
            (int(time.time()), operation_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Kanban operation intent could not be advanced")
    entry = _read_kanban_ledger_entry(conn, operation_id)
    if entry is None or entry.state != "MUTATING":
        raise RuntimeError("Kanban operation mutation intent was not persisted")
    return entry


def _finalize_kanban_ledger(
    conn: sqlite3.Connection, *, operation_id: str, task_id: str
) -> None:
    with _ledger_write(conn):
        cursor = conn.execute(
            "UPDATE operations SET state = 'COMMITTED', task_id = ?, "
            "created_status = 'blocked', created_assignee = NULL, updated_at = ? "
            "WHERE operation_id = ? AND state = 'MUTATING'",
            (task_id, int(time.time()), operation_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Kanban operation receipt could not be finalized")


def _path_identity(path: Path, *, directory: bool) -> tuple[int, int]:
    info = path.lstat()
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if stat.S_ISLNK(info.st_mode) or not expected:
        raise RuntimeError("Kanban board path is unsafe")
    return int(info.st_dev), int(info.st_ino)


def _kanban_board_generation(
    kb: Any,
    board: str,
    *,
    expected_db_path: str | None = None,
) -> _KanbanBoardGeneration:
    """Fingerprint one active canonical board without following descendants."""
    root = Path(kb.kanban_home()).expanduser().resolve(strict=True)
    lexical_db_path = Path(kb.kanban_db_path(board)).expanduser()
    if not lexical_db_path.is_absolute():
        lexical_db_path = Path.cwd() / lexical_db_path
    lexical_db_path = Path(os.path.abspath(lexical_db_path))
    try:
        relative_parts = lexical_db_path.relative_to(root).parts
    except ValueError as exc:
        raise RuntimeError("Kanban board path escaped its canonical root") from exc
    current = root
    for index, part in enumerate(relative_parts):
        current = current / part
        _path_identity(current, directory=index < len(relative_parts) - 1)
    resolved_db_path = str(lexical_db_path.resolve(strict=True))
    if resolved_db_path != str(lexical_db_path):
        raise RuntimeError("Kanban board path contains a symlink")
    if expected_db_path is not None and resolved_db_path != expected_db_path:
        raise RuntimeError("Kanban board path changed")
    metadata = kb.read_board_metadata(board)
    if (
        not isinstance(metadata, dict)
        or metadata.get("slug") != board
        or metadata.get("archived") is not False
    ):
        raise RuntimeError("Kanban board is no longer active")
    metadata_path = Path(kb.board_metadata_path(board)).expanduser()
    metadata_identity: tuple[int, int] | None = None
    if metadata_path.exists() or metadata_path.is_symlink():
        metadata_identity = _path_identity(metadata_path, directory=False)
        if str(metadata_path.resolve(strict=True)) != str(
            Path(os.path.abspath(metadata_path))
        ):
            raise RuntimeError("Kanban board metadata path contains a symlink")
    created_at = metadata.get("created_at")
    if type(created_at) is not int or created_at < 0:
        created_at = None
    identity = {
        "version": 1,
        "slug": board,
        "db_path": resolved_db_path,
        "db_identity": _path_identity(lexical_db_path, directory=False),
        "directory_identity": _path_identity(
            lexical_db_path.parent, directory=True
        ),
        "metadata_identity": metadata_identity,
        "created_at": created_at,
    }
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return _KanbanBoardGeneration(board, resolved_db_path, fingerprint)


def _initialize_fresh_default_kanban_board(
    kb: Any, *, reauthorize: Callable[[], None]
) -> None:
    """Materialize only Hermes' special always-present default board."""
    path = Path(kb.kanban_db_path("default")).expanduser()
    if path.is_symlink():
        raise RuntimeError("Default Kanban DB path is a symlink")
    needs_init = not path.exists()
    if not needs_init:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("Default Kanban DB path is unsafe")
        needs_init = info.st_size == 0
    if not needs_init:
        return
    # Canonical Hermes advertises `default` even before its legacy DB exists.
    # Initialize that one special board before PREPARED/MUTATING exists; named
    # boards never take this path, so a missing archived/deleted generation is
    # still never recreated. Revalidate around initialization because it can
    # perform first-open schema work, but cannot create a card.
    reauthorize()
    kb.init_db(board="default")
    reauthorize()


def _open_existing_kanban_board(
    generation: _KanbanBoardGeneration,
) -> sqlite3.Connection:
    """Open the pinned DB in mode=rw so a missing board is never recreated."""
    path = Path(generation.db_path)
    uri = path.as_uri() + "?mode=rw"
    conn = sqlite3.connect(
        uri,
        uri=True,
        isolation_level=None,
        timeout=_KANBAN_BOARD_BUSY_TIMEOUT_MS / 1_000,
    )
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={_KANBAN_BOARD_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA trusted_schema=OFF")
        sentinel = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'tasks' LIMIT 1"
        ).fetchone()
        if sentinel is None:
            raise RuntimeError("Kanban board schema is unavailable")
        return conn
    except Exception:
        conn.close()
        raise


def _verify_parked_kanban_task(
    kb: Any,
    conn: sqlite3.Connection,
    *,
    task_id: str,
    idempotency_key: str,
    title: str,
    body: str | None,
) -> Any:
    task = kb.get_task(conn, task_id)
    created_events = conn.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'created' "
        "ORDER BY id ASC LIMIT 2",
        (task_id,),
    ).fetchall()
    try:
        created_payload = (
            json.loads(created_events[0]["payload"])
            if len(created_events) == 1 and created_events[0]["payload"]
            else None
        )
    except (TypeError, json.JSONDecodeError):
        created_payload = None
    if (
        task is None
        or task.idempotency_key != idempotency_key
        or task.created_by != _KANBAN_CREATED_BY
        or task.title != title
        or task.body != body
        or task.status != "blocked"
        or task.assignee is not None
        or _KANBAN_TASK_ID.fullmatch(task.id) is None
        or not isinstance(created_payload, dict)
        or created_payload.get("status") != "blocked"
        or created_payload.get("assignee") is not None
        or not kb._has_sticky_block(conn, task.id)
    ):
        raise _KanbanOperationError("operation_outcome_unknown", "unknown")
    return task


def _committed_kanban_receipt(
    entry: _KanbanLedgerEntry, *, historical: bool
) -> dict[str, Any]:
    if (
        entry.state != "COMMITTED"
        or entry.task_id is None
        or entry.created_status != "blocked"
    ):
        raise RuntimeError("Kanban operation receipt is incomplete")
    return {
        "status": (
            "historical_acknowledgement" if historical else "acknowledged"
        ),
        "operation_id": entry.operation_id,
        "task_id": entry.task_id,
        "created_status": "blocked",
        "created_assignee": None,
        "board": entry.board_slug,
    }


def _execute_kanban_task_create(
    kb: Any,
    *,
    operation_id: str,
    title: str,
    body: str | None,
    board_input: str,
    session_id: str | None,
    reauthorize: Callable[[], None],
    cancelled: threading.Event,
) -> dict[str, Any]:
    """Create/recover once under a durable global tombstone and board txn."""
    payload_digest = _kanban_payload_digest(
        title=title, body=body, board_input=board_input
    )
    idempotency_key = _KANBAN_IDEMPOTENCY_PREFIX + operation_id
    append_event = getattr(kb, "_append_event", None)
    has_sticky_block = getattr(kb, "_has_sticky_block", None)
    if not callable(append_event) or not callable(has_sticky_block):
        raise RuntimeError("Canonical sticky-block support is unavailable")

    with _kanban_operation_lock(cancelled) as ledger_dir:
        reauthorize()
        with contextlib.closing(_connect_kanban_ledger(ledger_dir)) as ledger:
            entry = _read_kanban_ledger_entry(ledger, operation_id)
            # Ledger open/integrity work is bounded but can still outlive the
            # exact turn. Recheck before returning any historical/conflict fact.
            reauthorize()
            if entry is not None and entry.payload_digest != payload_digest:
                commit_state = {
                    "PREPARED": "not_committed",
                    "MUTATING": "unknown",
                    "COMMITTED": "committed",
                }[entry.state]
                raise _KanbanOperationError("operation_conflict", commit_state)
            if entry is not None and entry.state == "COMMITTED":
                reauthorize()
                return _committed_kanban_receipt(entry, historical=True)
            if entry is None:
                matching, available, truncated = _canonical_kanban_boards(kb)
                board, board_error = _resolve_exact_kanban_board(
                    board_input, matching
                )
                if board_error is not None or board is None:
                    # Canonical display names can be private profile data. The
                    # listing may have outlived/released its active turn, so do
                    # not let bounded choices escape without one exact final
                    # cancellation/authority check under the still-held lock.
                    reauthorize()
                    raise _KanbanBoardSelectionError(
                        board_error or "board_not_found", available, truncated
                    )
                reauthorize()
                if board == "default":
                    _initialize_fresh_default_kanban_board(
                        kb, reauthorize=reauthorize
                    )
                generation = _kanban_board_generation(kb, board)
                reauthorize()
                entry = _insert_kanban_intent(
                    ledger,
                    operation_id=operation_id,
                    payload_digest=payload_digest,
                    generation=generation,
                )
            generation = _KanbanBoardGeneration(
                entry.board_slug,
                entry.board_db_path,
                entry.board_generation,
            )
            try:
                current_generation = _kanban_board_generation(
                    kb,
                    entry.board_slug,
                    expected_db_path=entry.board_db_path,
                )
            except Exception as exc:
                state = "unknown" if entry.state == "MUTATING" else "not_committed"
                raise _KanbanOperationError(
                    "operation_outcome_unknown"
                    if state == "unknown"
                    else "board_generation_changed",
                    state,
                ) from exc
            if current_generation.fingerprint != entry.board_generation:
                state = "unknown" if entry.state == "MUTATING" else "not_committed"
                raise _KanbanOperationError(
                    "operation_outcome_unknown"
                    if state == "unknown"
                    else "board_generation_changed",
                    state,
                )
            try:
                reauthorize()
            except Exception as exc:
                if entry.state == "MUTATING":
                    raise _KanbanOperationError(
                        "operation_outcome_unknown", "unknown"
                    ) from exc
                raise

            may_create = entry.state == "PREPARED"
            if may_create:
                entry = _mark_kanban_mutating(ledger, operation_id)

            created_now = False
            try:
                with contextlib.closing(
                    _open_existing_kanban_board(generation)
                ) as conn:
                    opened_generation = _kanban_board_generation(
                        kb,
                        entry.board_slug,
                        expected_db_path=entry.board_db_path,
                    )
                    if opened_generation.fingerprint != entry.board_generation:
                        raise _KanbanOperationError(
                            "operation_outcome_unknown", "unknown"
                        )
                    with kb.write_txn(conn):
                        locked_generation = _kanban_board_generation(
                            kb,
                            entry.board_slug,
                            expected_db_path=entry.board_db_path,
                        )
                        if locked_generation.fingerprint != entry.board_generation:
                            raise _KanbanOperationError(
                                "operation_outcome_unknown", "unknown"
                            )
                        try:
                            reauthorize()
                        except Exception as exc:
                            raise _KanbanOperationError(
                                "operation_outcome_unknown", "unknown"
                            ) from exc
                        existing = conn.execute(
                            "SELECT id FROM tasks WHERE idempotency_key = ? "
                            "ORDER BY created_at ASC, id ASC LIMIT 2",
                            (idempotency_key,),
                        ).fetchall()
                        if len(existing) > 1:
                            raise _KanbanOperationError(
                                "operation_outcome_unknown", "unknown"
                            )
                        if existing:
                            task_id = str(existing[0]["id"])
                        elif may_create:
                            task_id = kb.create_task(
                                conn,
                                title=title,
                                body=body,
                                assignee=None,
                                created_by=_KANBAN_CREATED_BY,
                                workspace_kind="scratch",
                                triage=False,
                                idempotency_key=idempotency_key,
                                initial_status="blocked",
                                session_id=session_id,
                                board=entry.board_slug,
                                # Keep this parked capture independent of a
                                # board-scoped Project lookup/worktree. Besides
                                # matching the no-auto-run contract, this avoids
                                # an unrelated projects DB wait inside the final
                                # authority-bound board transaction.
                                project_id="",
                            )
                            created_now = True
                        else:
                            raise _KanbanOperationError(
                                "operation_outcome_unknown", "unknown"
                            )
                        task = kb.get_task(conn, task_id)
                        if created_now and task is not None and not has_sticky_block(
                            conn, task.id
                        ):
                            append_event(
                                conn,
                                task.id,
                                "blocked",
                                {
                                    "reason": (
                                        "parked by explicit G2 Kanban creation"
                                    ),
                                    "kind": "needs_input",
                                    "source_status": "ready",
                                },
                            )
                        task = _verify_parked_kanban_task(
                            kb,
                            conn,
                            task_id=task_id,
                            idempotency_key=idempotency_key,
                            title=title,
                            body=body,
                        )
                        # Cancellation/revocation can arrive while canonical
                        # create/event verification is running in this worker.
                        # Recheck while the IMMEDIATE transaction is still
                        # rollback-capable; after COMMIT, complete the durable
                        # ledger tombstone even if the awaiting coroutine has
                        # already gone away.
                        try:
                            reauthorize()
                        except Exception as exc:
                            raise _KanbanOperationError(
                                "operation_outcome_unknown", "unknown"
                            ) from exc
            except _KanbanOperationError:
                raise
            except Exception as exc:
                raise _KanbanOperationError(
                    "operation_outcome_unknown", "unknown"
                ) from exc

            try:
                post_generation = _kanban_board_generation(
                    kb,
                    entry.board_slug,
                    expected_db_path=entry.board_db_path,
                )
            except Exception as exc:
                raise _KanbanOperationError(
                    "operation_outcome_unknown", "unknown"
                ) from exc
            if post_generation.fingerprint != entry.board_generation:
                raise _KanbanOperationError(
                    "operation_outcome_unknown", "unknown"
                )
            _finalize_kanban_ledger(
                ledger, operation_id=operation_id, task_id=task.id
            )
            committed = _read_kanban_ledger_entry(ledger, operation_id)
            if committed is None:
                raise RuntimeError("Kanban operation receipt disappeared")
            return _committed_kanban_receipt(
                committed, historical=not created_now
            )


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


async def _handle_kanban_task_create(args: dict[str, Any], **_kwargs: Any) -> str:
    """Create one blocked, unassigned card on one exact existing board."""
    if _current_session_platform() != "g2":
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "Hermes Kanban creation requires an active G2 turn",
        })
    if (
        not isinstance(args, dict)
        or not {"operation_id", "title", "board"} <= set(args)
        or not set(args) <= {"operation_id", "title", "body", "board"}
    ):
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": (
                "operation_id, title, board, and optional body are the only "
                "accepted fields"
            ),
        })
    operation_id = args.get("operation_id")
    title = _normalize_kanban_line(
        args.get("title"),
        max_scalars=_KANBAN_TITLE_MAX_SCALARS,
        max_bytes=_KANBAN_TITLE_MAX_BYTES,
    )
    board_input = _normalize_kanban_line(
        args.get("board"),
        max_scalars=_KANBAN_BOARD_INPUT_MAX_SCALARS,
        max_bytes=_KANBAN_BOARD_INPUT_MAX_BYTES,
    )
    body = None
    if "body" in args:
        body = _normalize_kanban_line(
            args.get("body"),
            max_scalars=_KANBAN_BODY_MAX_SCALARS,
            max_bytes=_KANBAN_BODY_MAX_BYTES,
        )
    if not isinstance(operation_id, str) or _KANBAN_OPERATION_ID.fullmatch(
        operation_id
    ) is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "operation_id is not a trusted Kanban operation identity",
        })
    if title is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "title must be one bounded inert line",
        })
    if board_input is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "board must be one bounded exact board slug or display name",
        })
    if "body" in args and body is None:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "body must be one bounded inert line",
        })

    try:
        active_authorization = await _authorize_active_g2_read()
    except Exception:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "Hermes Kanban requires the exact current G2 turn",
        })

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
            "error": "G2 turn authority expired before Kanban creation",
        })

    cancelled = threading.Event()

    def reauthorize() -> None:
        if cancelled.is_set() or runtime.get_active() is not adapter:
            raise PermissionError("G2 turn authority expired")
        try:
            authorization = adapter.authorize_active_g2_turn()
        except Exception as exc:
            raise PermissionError("G2 turn authority expired") from exc
        if authorization != active_authorization:
            raise PermissionError("G2 turn authority changed")

    try:
        from hermes_cli import kanban_db as kb
        from gateway.session_context import get_session_env

        if str(os.environ.get("HERMES_KANBAN_DB") or "").strip():
            raise RuntimeError("a pinned Kanban DB cannot prove board identity")
        session_id = str(get_session_env("HERMES_SESSION_ID") or "").strip() or None
        receipt = await asyncio.to_thread(
            _execute_kanban_task_create,
            kb,
            operation_id=operation_id,
            title=title,
            body=body,
            board_input=board_input,
            session_id=session_id,
            reauthorize=reauthorize,
            cancelled=cancelled,
        )
        return json.dumps(
            {"success": True, "receipt": receipt},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except asyncio.CancelledError:
        cancelled.set()
        raise
    except _KanbanBoardSelectionError as exc:
        return json.dumps(
            {
                "success": False,
                "commit_state": "not_committed",
                "error_code": exc.error_code,
                "error": (
                    "No active Hermes Kanban board exactly matches that name"
                    if exc.error_code == "board_not_found"
                    else (
                        "More than one active Hermes Kanban board has that "
                        "exact name"
                    )
                ),
                "available_boards": exc.available_boards,
                "boards_truncated": exc.boards_truncated,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except _KanbanOperationError as exc:
        return json.dumps(
            {
                "success": False,
                "commit_state": exc.commit_state,
                "operation_id": operation_id,
                "error_code": exc.error_code,
                "error": _KANBAN_OPERATION_ERRORS[exc.error_code],
            },
            separators=(",", ":"),
        )
    except TimeoutError:
        # Another same-profile operation may be creating or finalizing this
        # exact identity while it holds the global flock. Without reading its
        # ledger row we cannot truthfully claim `not_committed` (especially on
        # response-loss attempt 2), and lock contention is not an auth error.
        return json.dumps(
            {
                "success": False,
                "commit_state": "unknown",
                "operation_id": operation_id,
                "error_code": "operation_outcome_unknown",
                "error": _KANBAN_OPERATION_ERRORS[
                    "operation_outcome_unknown"
                ],
            },
            separators=(",", ":"),
        )
    except PermissionError:
        return json.dumps({
            "success": False,
            "commit_state": "not_committed",
            "error": "G2 turn authority expired before Kanban creation",
        })
    except Exception:
        return json.dumps({
            "success": False,
            "commit_state": "unknown",
            "operation_id": operation_id,
            "error": "Hermes Kanban creation outcome is unknown",
        })


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
    "g2.kanban.task.create": _handle_kanban_task_create,
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
