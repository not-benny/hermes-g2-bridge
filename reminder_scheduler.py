"""Deterministic, profile-local delivery for one-shot G2 reminders.

This module deliberately has no agent, prompt, cron, tool-registry, or session
dependency.  An authenticated active G2 turn may append one bounded reminder
to the durable outbox.  At its due instant the adapter-owned background worker
calls one fixed phone MCP method through the callback supplied at construction.

The phone operation ID is the exactly-once boundary.  We persist ``in_flight``
before handoff and replay the identical operation ID and inert text after an
unknown outcome or process restart.  A compliant phone then returns a normal
or historical acknowledgement.  Completed rows are reduced to content-free
tombstones so reminder text is not retained after delivery.

Pending and in-flight text and schedule metadata are necessarily plaintext in
the current store.
Hermes does not expose a gateway-safe OS-keyring encryption primitive, so this
module does not invent a local key or derive one from a rotatable transport
secret.  The file therefore fails closed on symlinks, non-owner permissions,
corruption, and unsafe metadata and is restricted to a ``0700`` directory and
``0600`` atomic file.  Processes with the same OS identity can still read it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import stat
import threading
import unicodedata
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

STORE_VERSION = 1
MAX_PENDING_REMINDERS = 256
MAX_TOMBSTONES = 768
MAX_STORE_BYTES = 1_048_576
MAX_ATTEMPT_COUNT = 9_007_199_254_740_991
MAX_DUE_PER_TICK = 16
MAX_FUTURE_SECONDS = 10 * 366 * 24 * 60 * 60

_OPERATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_INSTANT = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$"
)
_TEXT_MAX_CHARS = 160
_TEXT_MAX_BYTES = 640
_SCHEDULE_MAX_CHARS = 128
_SCHEDULE_MAX_BYTES = 256
_BIDI_CONTROLS = frozenset(
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
_RECEIPT_STATUSES = frozenset(
    {"queued", "acknowledged", "historical_acknowledgement"}
)
_RECEIPT_MAX_BYTES = 160

_PENDING_FIELDS = frozenset(
    {
        "operation_id",
        "operation_hash",
        "payload_digest",
        "schedule",
        "text",
        "due_at",
        "state",
        "created_at",
        "updated_at",
        "attempt_count",
        "last_attempt_at",
        "next_attempt_at",
    }
)
_TOMBSTONE_FIELDS = frozenset(
    {
        "operation_hash",
        "payload_digest",
        "state",
        "completed_at",
        "attempt_count",
        "receipt_status",
    }
)

Delivery = Callable[[str, str], Awaitable[Any]]
Clock = Callable[[], datetime]
ScheduleParser = Callable[[str], Mapping[str, Any]]


class ReminderError(RuntimeError):
    """Base class for safe reminder boundary failures."""


class ReminderInputError(ReminderError):
    """The requested reminder is not a bounded future one-shot."""

    commit_state = "not_committed"


class ReminderConflictError(ReminderError):
    """An operation ID was already bound to a different payload."""

    commit_state = "not_committed"


class ReminderCapacityError(ReminderError):
    """The bounded pending outbox has reached capacity."""

    commit_state = "not_committed"


class ReminderStoreCorrupt(ReminderError):
    """The durable outbox cannot be trusted and must not be overwritten."""


class ReminderStoreWriteError(ReminderError):
    """A durable write could not be confirmed."""

    commit_state = "unknown"


def _profile_home() -> Path:
    raw = os.environ.get("HERMES_HOME")
    return Path(raw).expanduser().resolve() if raw else (Path.home() / ".hermes").resolve()


def default_store_path() -> Path:
    # The shared profile state directory is conventionally 0755. Keep reminder
    # text in its own owner-only leaf instead of weakening the store's metadata
    # checks or requiring unrelated Hermes state to change permissions.
    return _profile_home() / "state" / "g2-reminders" / "outbox-v1.json"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("naive reminder instant")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _parse_instant(value: Any) -> datetime:
    if not isinstance(value, str) or not _INSTANT.fullmatch(value):
        raise ValueError("invalid reminder instant")
    parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    return parsed.astimezone(timezone.utc)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


def _safe_line(value: Any, *, max_chars: int, max_bytes: int) -> str | None:
    if not isinstance(value, str):
        return None
    for character in value:
        codepoint = ord(character)
        if (
            codepoint <= 0x1F
            or 0x7F <= codepoint <= 0x9F
            or 0xD800 <= codepoint <= 0xDFFF
            or codepoint in _BIDI_CONTROLS
            or unicodedata.category(character) in {"Cc", "Cs", "Zl", "Zp"}
        ):
            return None
    try:
        normalized = unicodedata.normalize("NFC", value).strip()
        encoded = normalized.encode("utf-8")
    except (TypeError, UnicodeError):
        return None
    if not normalized or len(normalized) > max_chars or len(encoded) > max_bytes:
        return None
    return normalized


def _safe_reminder_text(value: Any) -> str | None:
    text = _safe_line(value, max_chars=_TEXT_MAX_CHARS, max_bytes=_TEXT_MAX_BYTES)
    if (
        text is None
        or any(marker in text for marker in ("<", ">", "`"))
        or re.search(r"(?:https?://|www\.)", text, re.IGNORECASE)
    ):
        return None
    return text


def _default_schedule_parser(schedule: str) -> Mapping[str, Any]:
    # Reuse Hermes' timezone-aware parser, but never create or inspect a cron
    # job.  The exact kind check in ``_one_shot_due_at`` rejects interval and
    # cron schedules after parsing.
    from cron.jobs import parse_schedule

    relative = schedule[3:].strip() if schedule.lower().startswith("in ") else schedule
    return parse_schedule(relative)


def _normalized_schedule(schedule: Any) -> str:
    normalized = _safe_line(
        schedule,
        max_chars=_SCHEDULE_MAX_CHARS,
        max_bytes=_SCHEDULE_MAX_BYTES,
    )
    if normalized is None:
        raise ReminderInputError("schedule must be one bounded one-shot value")
    return normalized


def _one_shot_due_at(
    schedule: Any,
    *,
    now: datetime,
    parser: ScheduleParser,
) -> tuple[str, datetime]:
    normalized = _normalized_schedule(schedule)
    try:
        parsed = parser(normalized)
    except Exception as exc:
        raise ReminderInputError(
            "schedule must be a future one-shot relative duration or ISO timestamp"
        ) from exc
    if not isinstance(parsed, Mapping) or parsed.get("kind") != "once":
        raise ReminderInputError("recurring reminder schedules are not accepted")
    raw_due = parsed.get("run_at")
    if not isinstance(raw_due, str):
        raise ReminderInputError("one-shot reminder has no exact due instant")
    try:
        due = datetime.fromisoformat(raw_due.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReminderInputError("one-shot reminder due instant is invalid") from exc
    if due.tzinfo is None:
        raise ReminderInputError("one-shot reminder due instant must include a timezone")
    due = due.astimezone(timezone.utc)
    if due <= now or (due - now).total_seconds() > MAX_FUTURE_SECONDS:
        raise ReminderInputError("one-shot reminder due instant is outside the allowed future window")
    return normalized, due


def _operation_hash(operation_id: str) -> str:
    return hashlib.sha256(operation_id.encode("ascii")).hexdigest()


def _payload_digest(schedule: str, text: str) -> str:
    canonical = json.dumps(
        {"schedule": schedule, "text": text},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _decode_receipt(result: Any, operation_id: str) -> str | None:
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
    encoded = item.get("text")
    if not isinstance(encoded, str):
        return None
    try:
        if not encoded or len(encoded.encode("utf-8")) > _RECEIPT_MAX_BYTES:
            return None
        receipt = json.loads(
            encoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, UnicodeError):
        return None
    if (
        not isinstance(receipt, dict)
        or set(receipt) != {"status", "operation_id"}
        or receipt.get("status") not in _RECEIPT_STATUSES
        or receipt.get("operation_id") != operation_id
    ):
        return None
    return str(receipt["status"])


def _valid_attempt(value: Any) -> bool:
    return type(value) is int and 0 <= value <= MAX_ATTEMPT_COUNT


def _validate_entry(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("reminder entry is not an object")
    state = raw.get("state")
    if state in {"pending", "in_flight"}:
        if set(raw) != _PENDING_FIELDS:
            raise ValueError("pending reminder has unexpected fields")
        operation_id = raw.get("operation_id")
        text = _safe_reminder_text(raw.get("text"))
        schedule = _safe_line(
            raw.get("schedule"),
            max_chars=_SCHEDULE_MAX_CHARS,
            max_bytes=_SCHEDULE_MAX_BYTES,
        )
        if (
            not isinstance(operation_id, str)
            or not _OPERATION_ID.fullmatch(operation_id)
            or raw.get("operation_hash") != _operation_hash(operation_id)
            or text is None
            or text != raw.get("text")
            or schedule is None
            or schedule != raw.get("schedule")
            or not isinstance(raw.get("payload_digest"), str)
            or not _DIGEST.fullmatch(raw["payload_digest"])
            or raw["payload_digest"] != _payload_digest(schedule, text)
            or not _valid_attempt(raw.get("attempt_count"))
        ):
            raise ValueError("pending reminder fields are invalid")
        for key in ("due_at", "created_at", "updated_at", "next_attempt_at"):
            _parse_instant(raw.get(key))
        if raw.get("last_attempt_at") is not None:
            _parse_instant(raw.get("last_attempt_at"))
        return dict(raw)
    if state == "delivered":
        if set(raw) != _TOMBSTONE_FIELDS:
            raise ValueError("reminder tombstone has unexpected fields")
        if (
            not isinstance(raw.get("operation_hash"), str)
            or not _DIGEST.fullmatch(raw["operation_hash"])
            or not isinstance(raw.get("payload_digest"), str)
            or not _DIGEST.fullmatch(raw["payload_digest"])
            or not _valid_attempt(raw.get("attempt_count"))
            or raw.get("receipt_status") not in _RECEIPT_STATUSES
        ):
            raise ValueError("reminder tombstone fields are invalid")
        _parse_instant(raw.get("completed_at"))
        return dict(raw)
    raise ValueError("unknown reminder state")


def _serialize(entries: list[dict[str, Any]]) -> bytes:
    encoded = json.dumps(
        {"version": STORE_VERSION, "entries": entries},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > MAX_STORE_BYTES:
        raise ReminderCapacityError("reminder outbox exceeds its bounded store size")
    return encoded


def _open_private_directory(directory: Path, *, create: bool) -> int:
    if create:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(directory, flags)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            raise ReminderStoreCorrupt("reminder outbox directory metadata is unsafe")
        return fd
    except Exception:
        os.close(fd)
        raise


def _read_store(path: Path) -> list[dict[str, Any]]:
    try:
        directory_fd = _open_private_directory(path.parent, create=False)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ReminderStoreCorrupt("reminder outbox directory cannot be opened safely") from exc
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path.name, flags, dir_fd=directory_fd)
    except FileNotFoundError:
        os.close(directory_fd)
        return []
    except OSError as exc:
        os.close(directory_fd)
        raise ReminderStoreCorrupt("reminder outbox cannot be opened safely") from exc
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_size > MAX_STORE_BYTES
        ):
            raise ReminderStoreCorrupt("reminder outbox metadata is unsafe")
        chunks: list[bytes] = []
        remaining = MAX_STORE_BYTES + 1
        while remaining:
            chunk = os.read(fd, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    finally:
        os.close(fd)
        os.close(directory_fd)
    if len(raw) > MAX_STORE_BYTES:
        raise ReminderStoreCorrupt("reminder outbox is too large")
    try:
        document = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
        if (
            not isinstance(document, dict)
            or set(document) != {"version", "entries"}
            or type(document.get("version")) is not int
            or document["version"] != STORE_VERSION
            or not isinstance(document.get("entries"), list)
            or len(document["entries"]) > MAX_PENDING_REMINDERS + MAX_TOMBSTONES
        ):
            raise ValueError("invalid reminder outbox document")
        entries = [_validate_entry(item) for item in document["entries"]]
        hashes = [item["operation_hash"] for item in entries]
        if len(hashes) != len(set(hashes)):
            raise ValueError("duplicate reminder operation hash")
        pending_count = sum(item["state"] != "delivered" for item in entries)
        tombstone_count = len(entries) - pending_count
        if pending_count > MAX_PENDING_REMINDERS or tombstone_count > MAX_TOMBSTONES:
            raise ValueError("reminder outbox capacity exceeded")
        return entries
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ReminderStoreCorrupt("reminder outbox is corrupt") from exc


def _atomic_write(path: Path, encoded: bytes) -> None:
    directory = path.parent
    directory_fd = _open_private_directory(directory, create=True)
    temporary_name = f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = -1
    replaced = False
    try:
        fd = os.open(temporary_name, flags, 0o600, dir_fd=directory_fd)
        view = memoryview(encoded)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short reminder outbox write")
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        replaced = True
        os.fsync(directory_fd)
    except Exception:
        # Even an after-replace failure remains safe to retry: every caller uses
        # the same operation ID and payload.  The exception is classified as an
        # unknown durable commit by ``_commit``.
        raise
    finally:
        if fd >= 0:
            os.close(fd)
        if not replaced:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


class ReminderScheduler:
    """Durable one-shot reminder outbox and adapter-owned delivery worker."""

    def __init__(
        self,
        deliver: Delivery,
        *,
        store_path: Path | None = None,
        clock: Clock = _utc_now,
        schedule_parser: ScheduleParser = _default_schedule_parser,
        retry_base_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        poll_max_seconds: float = 30.0,
    ) -> None:
        self._deliver = deliver
        self._store_path = (store_path or default_store_path()).absolute()
        self._clock = clock
        self._schedule_parser = schedule_parser
        self._retry_base_seconds = max(0.05, float(retry_base_seconds))
        self._retry_max_seconds = max(
            self._retry_base_seconds, float(retry_max_seconds)
        )
        self._poll_max_seconds = max(0.05, float(poll_max_seconds))
        self._entries: list[dict[str, Any]] = []
        self._state_lock = threading.RLock()
        self._tick_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._wake: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loaded = False

    @property
    def store_path(self) -> Path:
        return self._store_path

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        entries = _read_store(self._store_path)
        # In-flight means the previous process could have handed off to the
        # phone.  Preserve it and retry the exact same operation after the
        # recorded backoff; phone-side idempotency provides the receipt.
        with self._state_lock:
            self._entries = entries
            self._loaded = True
        self._loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self._task = asyncio.create_task(
            self._run(), name="g2-deterministic-reminders"
        )

    async def stop(self) -> None:
        task, self._task = self._task, None
        wake, self._wake = self._wake, None
        if wake is not None:
            wake.set()
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._loop = None

    def _notify_worker(self) -> None:
        loop = self._loop
        wake = self._wake
        if loop is None or wake is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(wake.set)

    def _commit(self, entries: list[dict[str, Any]]) -> None:
        encoded = _serialize(entries)
        try:
            _atomic_write(self._store_path, encoded)
        except Exception as exc:
            raise ReminderStoreWriteError(
                "reminder outbox durable write could not be confirmed"
            ) from exc
        self._entries = entries

    def schedule(self, operation_id: Any, schedule: Any, text: Any) -> dict[str, Any]:
        """Synchronously commit a reminder; callers can recheck turn authority first."""
        if not self._loaded:
            raise ReminderStoreWriteError("reminder scheduler is not active")
        if not isinstance(operation_id, str) or not _OPERATION_ID.fullmatch(operation_id):
            raise ReminderInputError("operation_id is invalid")
        normalized_text = _safe_reminder_text(text)
        if normalized_text is None or normalized_text != text:
            raise ReminderInputError("reminder text must be one bounded inert line")
        now = self._clock().astimezone(timezone.utc)
        # Resolve the idempotency identity from bounded, canonical input before
        # applying the time-sensitive "future" check.  An absolute timestamp
        # necessarily becomes historical after its due instant, but an exact
        # retry must still resolve to the row already bound to this operation.
        normalized_schedule = _normalized_schedule(schedule)
        operation_hash = _operation_hash(operation_id)
        payload_digest = _payload_digest(normalized_schedule, normalized_text)

        with self._state_lock:
            existing = next(
                (
                    item
                    for item in self._entries
                    if item["operation_hash"] == operation_hash
                ),
                None,
            )
            if existing is not None:
                if existing["payload_digest"] != payload_digest:
                    raise ReminderConflictError(
                        "operation_id is already bound to a different reminder"
                    )
                if existing["state"] == "delivered":
                    return {
                        "success": True,
                        "status": "historical_delivered",
                        "operation_id": operation_id,
                        "receipt": {
                            "status": existing["receipt_status"],
                            "operation_id": operation_id,
                        },
                    }
                return {
                    "success": True,
                    "status": "historical_scheduled",
                    "operation_id": operation_id,
                    "reminder_id": operation_hash[:32],
                    "due_at": existing["due_at"],
                }

            normalized_schedule, due = _one_shot_due_at(
                normalized_schedule,
                now=now,
                parser=self._schedule_parser,
            )
            pending_count = sum(
                item["state"] != "delivered" for item in self._entries
            )
            if pending_count >= MAX_PENDING_REMINDERS:
                raise ReminderCapacityError("G2 reminder outbox is full")
            retained = list(self._entries)
            tombstones = sorted(
                (item for item in retained if item["state"] == "delivered"),
                key=lambda item: item["completed_at"],
            )
            while len(tombstones) >= MAX_TOMBSTONES:
                oldest = tombstones.pop(0)
                retained = [item for item in retained if item is not oldest]

            instant = _instant(now)
            entry = {
                "operation_id": operation_id,
                "operation_hash": operation_hash,
                "payload_digest": payload_digest,
                "schedule": normalized_schedule,
                "text": normalized_text,
                "due_at": _instant(due),
                "state": "pending",
                "created_at": instant,
                "updated_at": instant,
                "attempt_count": 0,
                "last_attempt_at": None,
                "next_attempt_at": _instant(due),
            }
            candidate = [*retained, entry]
            self._commit(candidate)

        self._notify_worker()
        return {
            "success": True,
            "status": "scheduled",
            "operation_id": operation_id,
            "reminder_id": operation_hash[:32],
            "due_at": entry["due_at"],
        }

    def _due_hashes(self, now: datetime) -> list[str]:
        with self._state_lock:
            due = [
                item
                for item in self._entries
                if item["state"] in {"pending", "in_flight"}
                and _parse_instant(item["next_attempt_at"]) <= now
            ]
            due.sort(key=lambda item: (item["next_attempt_at"], item["operation_hash"]))
            return [item["operation_hash"] for item in due[:MAX_DUE_PER_TICK]]

    def _retry_delay(self, attempt_count: int) -> float:
        exponent = min(max(attempt_count - 1, 0), 16)
        return min(self._retry_base_seconds * (2**exponent), self._retry_max_seconds)

    def _claim(self, operation_hash: str, now: datetime) -> tuple[str, str] | None:
        with self._state_lock:
            index = next(
                (
                    index
                    for index, item in enumerate(self._entries)
                    if item["operation_hash"] == operation_hash
                ),
                None,
            )
            if index is None:
                return None
            current = self._entries[index]
            if (
                current["state"] not in {"pending", "in_flight"}
                or _parse_instant(current["next_attempt_at"]) > now
            ):
                return None
            attempt_count = min(current["attempt_count"] + 1, MAX_ATTEMPT_COUNT)
            claimed = {
                **current,
                "state": "in_flight",
                "updated_at": _instant(now),
                "attempt_count": attempt_count,
                "last_attempt_at": _instant(now),
                "next_attempt_at": _instant(
                    now + timedelta(seconds=self._retry_delay(attempt_count))
                ),
            }
            candidate = list(self._entries)
            candidate[index] = claimed
            self._commit(candidate)
            return str(claimed["operation_id"]), str(claimed["text"])

    def _defer(self, operation_hash: str, now: datetime) -> None:
        with self._state_lock:
            index = next(
                (
                    index
                    for index, item in enumerate(self._entries)
                    if item["operation_hash"] == operation_hash
                ),
                None,
            )
            if index is None or self._entries[index]["state"] != "in_flight":
                return
            current = self._entries[index]
            deferred = {
                **current,
                "state": "pending",
                "updated_at": _instant(now),
            }
            candidate = list(self._entries)
            candidate[index] = deferred
            self._commit(candidate)

    def _complete(self, operation_hash: str, status: str, now: datetime) -> None:
        with self._state_lock:
            index = next(
                (
                    index
                    for index, item in enumerate(self._entries)
                    if item["operation_hash"] == operation_hash
                ),
                None,
            )
            if index is None or self._entries[index]["state"] != "in_flight":
                return
            current = self._entries[index]
            tombstone = {
                "operation_hash": current["operation_hash"],
                "payload_digest": current["payload_digest"],
                "state": "delivered",
                "completed_at": _instant(now),
                "attempt_count": current["attempt_count"],
                "receipt_status": status,
            }
            candidate = list(self._entries)
            candidate[index] = tombstone
            self._commit(candidate)

    async def run_due_once(self) -> int:
        """Attempt each reminder that was due at tick start no more than once."""
        async with self._tick_lock:
            now = self._clock().astimezone(timezone.utc)
            operation_hashes = self._due_hashes(now)
            delivered = 0
            for operation_hash in operation_hashes:
                try:
                    claimed = self._claim(operation_hash, now)
                except ReminderStoreWriteError:
                    logger.warning("G2 reminder claim could not be persisted")
                    continue
                if claimed is None:
                    continue
                operation_id, text = claimed
                status: str | None = None
                try:
                    result = await self._deliver(operation_id, text)
                    status = _decode_receipt(result, operation_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Offline, a definite pre-send failure, and an unknown
                    # post-send outcome all remain retryable with the same
                    # operation ID/text.  No alternative delivery route exists.
                    status = None
                completed_at = self._clock().astimezone(timezone.utc)
                try:
                    if status is None:
                        self._defer(operation_hash, completed_at)
                    else:
                        self._complete(operation_hash, status, completed_at)
                        delivered += 1
                except ReminderStoreWriteError:
                    # The durable in-flight row remains the source of truth.
                    # Its next retry asks the phone for a historical receipt.
                    logger.warning("G2 reminder outcome could not be persisted")
            return delivered

    def _seconds_until_next(self) -> float:
        now = self._clock().astimezone(timezone.utc)
        with self._state_lock:
            due_values = [
                _parse_instant(item["next_attempt_at"])
                for item in self._entries
                if item["state"] in {"pending", "in_flight"}
            ]
        if not due_values:
            return self._poll_max_seconds
        return max(0.0, min((min(due_values) - now).total_seconds(), self._poll_max_seconds))

    async def _run(self) -> None:
        while True:
            try:
                await self.run_due_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("G2 deterministic reminder tick failed safely")
            wake = self._wake
            if wake is None:
                return
            wake.clear()
            try:
                await asyncio.wait_for(wake.wait(), timeout=self._seconds_until_next())
            except asyncio.TimeoutError:
                pass
