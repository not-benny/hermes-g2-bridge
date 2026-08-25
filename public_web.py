"""Typed National Rail reader using an isolated headless Brave browser.

No caller supplies a URL and no page text crosses the tool boundary. The
reader generates one fixed National Rail route, pins every allowed hostname to
the public address resolved before launch, captures only typed service
responses, and returns a small validated departures model.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import socket
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit


class TrainReadError(RuntimeError):
    """A safe, deliberately non-diagnostic National Rail reader failure."""


_CRS = re.compile(r"^[A-Z0-9]{3}$")
_PLATFORM = re.compile(r"^[A-Za-z0-9]{1,8}$")
_POST_LOAD_SETTLE_MS = 3_000
_MAX_RESPONSE_BYTES = 1_048_576
_MAX_DEPARTURES = 6
_MAX_BROWSER_REQUESTS = 160
_MAX_CONCURRENT_READS = 2
_TRAIN_READ_SLOTS = threading.BoundedSemaphore(_MAX_CONCURRENT_READS)
_BRAVE_BINARY = Path("/opt/brave-bin/brave")
_ALLOWED_HOST_PATHS = {
    "www.nationalrail.co.uk": ("/",),
    "nreservices.nationalrail.co.uk": ("/live-info",),
    "jpservices.nationalrail.co.uk": (
        "/journey-planner",
        "/stations",
        "/fare-info",
    ),
    "stationpicker.nationalrail.co.uk": ("/stationPicker/",),
}


def _find_brave() -> str:
    """Use the real ELF, never the user-config-reading /usr/bin wrapper."""
    if (
        not _BRAVE_BINARY.is_file()
        or _BRAVE_BINARY.is_symlink()
        or not os.access(_BRAVE_BINARY, os.X_OK)
    ):
        raise TrainReadError("headless train browser unavailable")
    return str(_BRAVE_BINARY)


def _load_sync_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        raise TrainReadError("headless train browser unavailable") from None
    return sync_playwright


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _decode_response(response) -> Any | None:
    try:
        if response.status != 200:
            return None
        body = response.body()
        if not isinstance(body, bytes) or not body or len(body) > _MAX_RESPONSE_BYTES:
            return None
        return json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except Exception:
        return None


def _pinned_public_ipv4(host: str) -> str:
    try:
        addresses = {
            ipaddress.ip_address(info[4][0])
            for info in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        }
    except (OSError, ValueError):
        raise TrainReadError("National Rail host unavailable") from None
    candidates = sorted(
        str(address)
        for address in addresses
        if address.version == 4 and address.is_global
    )
    if not candidates:
        raise TrainReadError("National Rail host unavailable")
    return candidates[0]


def _host_resolver_rules() -> str:
    mappings = [
        f"MAP {host} {_pinned_public_ipv4(host)}"
        for host in sorted(_ALLOWED_HOST_PATHS)
    ]
    return ",".join([*mappings, "EXCLUDE localhost"])


def _request_is_allowed(url: object) -> bool:
    if not isinstance(url, str) or len(url) > 2_048:
        return False
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower().rstrip(".")
        prefixes = _ALLOWED_HOST_PATHS.get(host)
        return bool(
            parsed.scheme == "https"
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
            and prefixes
            and any(parsed.path.startswith(prefix) for prefix in prefixes)
        )
    except (TypeError, ValueError):
        return False


def _final_route_is_allowed(url: object, origin: str, destination: str) -> bool:
    if not isinstance(url, str) or len(url) > 2_048:
        return False
    try:
        parsed = urlsplit(url)
        common = bool(
            parsed.scheme == "https"
            and (parsed.hostname or "").lower().rstrip(".") == "www.nationalrail.co.uk"
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
        )
        if not common:
            return False
        if parsed.path.startswith("/live-trains/departures/"):
            return True
        if parsed.path != "/journey-planner/":
            return False
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        return bool(
            set(query) == {
                "type", "origin", "destination", "leavingType", "leavingDate",
                "leavingHour", "leavingMin", "adults", "extraTime", "fromLiveTrains",
            }
            and query.get("type") == ["single"]
            and query.get("origin") == [origin]
            and query.get("destination") == [destination]
            and query.get("leavingType") == ["departing"]
            and query.get("fromLiveTrains") == ["true"]
            and len(query.get("leavingDate", [])) == 1
            and re.fullmatch(r"[0-9]{6}", query["leavingDate"][0])
            and len(query.get("leavingHour", [])) == 1
            and re.fullmatch(r"(?:[01][0-9]|2[0-3])", query["leavingHour"][0])
            and len(query.get("leavingMin", [])) == 1
            and re.fullmatch(r"[0-5][0-9]", query["leavingMin"][0])
            and query.get("adults") == ["1"]
            and query.get("extraTime") == ["0"]
        )
    except (TypeError, ValueError):
        return False


class _RequestQuota:
    def __init__(self, limit: int) -> None:
        self._remaining = limit
        self._lock = threading.Lock()

    def consume(self) -> bool:
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            return True


def _route_request(
    route,
    request,
    cancelled: threading.Event,
    deadline: float,
    quota: _RequestQuota,
) -> None:
    if (
        cancelled.is_set()
        or time.monotonic() >= deadline
        or not quota.consume()
        or not _request_is_allowed(request.url)
        or request.resource_type in {"image", "media", "font"}
    ):
        route.abort()
        return
    route.continue_()


def _timestamp_ms(value: object) -> int | None:
    if not isinstance(value, str) or not 10 <= len(value) <= 64:
        return None
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if instant.tzinfo is None:
            return None
        timestamp = int(instant.timestamp() * 1_000)
    except (OverflowError, ValueError):
        return None
    return timestamp if 1_577_836_800_000 <= timestamp <= 4_102_444_800_000 else None


def _record(value: object) -> dict[str, Any] | None:
    return value if isinstance(value, dict) and all(isinstance(key, str) for key in value) else None


def _time_pair(value: object) -> tuple[int, int | None] | None:
    item = _record(value)
    if item is None:
        return None
    scheduled = _timestamp_ms(item.get("scheduled"))
    if scheduled is None:
        return None
    expected = _timestamp_ms(item.get("actual")) or _timestamp_ms(item.get("estimated"))
    return scheduled, expected


def _status(value: object, scheduled: int, expected: int | None, cancelled: bool) -> str:
    if cancelled:
        return "cancelled"
    item = _record(value) or {}
    provider = str(item.get("status") or "").lower()
    if "cancel" in provider:
        return "cancelled"
    if expected is None:
        return "unknown"
    if expected > scheduled + 30_000 or "delay" in provider:
        return "delayed"
    return "on_time"


def _optional_platform(value: object) -> str | None:
    return value if isinstance(value, str) and _PLATFORM.fullmatch(value) else None


def _parse_live(payload: object, origin: str, destination: str) -> dict[str, object] | None:
    root = _record(payload)
    data = _record(root.get("data")) if root else None
    board = _record(data.get("DepartureBoard")) if data else None
    departure_station = _record(board.get("departureStation")) if board else None
    filter_station = _record(board.get("filterStation")) if board else None
    services = board.get("services") if board else None
    observed_at_ms = _timestamp_ms(board.get("generatedAt")) if board else None
    if (
        board is None
        or departure_station is None
        or filter_station is None
        or departure_station.get("crs") != origin
        or filter_station.get("crs") != destination
        or observed_at_ms is None
        or not isinstance(services, list)
        or len(services) > 100
    ):
        return None
    departures: list[dict[str, object]] = []
    for service in services:
        item = _record(service)
        details = _record(item.get("journeyDetails")) if item else None
        departure = _time_pair(details.get("departureInfo")) if details else None
        arrival = _time_pair(details.get("arrivalInfo")) if details else None
        if departure is None or arrival is None:
            continue
        scheduled, expected = departure
        scheduled_arrival, expected_arrival = arrival
        if scheduled_arrival < scheduled:
            continue
        provider_status = item.get("status") if item else None
        cancelled = bool(item.get("isCancelled")) if item else False
        row: dict[str, object] = {
            "scheduled_departure_ms": scheduled,
            "scheduled_arrival_ms": scheduled_arrival,
            "status": _status(provider_status, scheduled, expected, cancelled),
        }
        if expected is not None:
            row["expected_departure_ms"] = expected
        if expected_arrival is not None:
            row["expected_arrival_ms"] = expected_arrival
        platform = _optional_platform(item.get("platform")) if item else None
        if platform is not None:
            row["platform"] = platform
        departures.append(row)
    departures.sort(key=lambda item: int(item["scheduled_departure_ms"]))
    return {
        "data_kind": "live" if departures else "no_live_services",
        "observed_at_ms": observed_at_ms,
        "departures": departures[:_MAX_DEPARTURES],
    }


def _parse_journeys(payload: object, origin: str, destination: str) -> list[dict[str, object]] | None:
    root = _record(payload)
    journeys = root.get("outwardJourneys") if root else None
    if not isinstance(journeys, list) or len(journeys) > 100:
        return None
    departures: list[dict[str, object]] = []
    for journey in journeys:
        item = _record(journey)
        route_origin = _record(item.get("origin")) if item else None
        route_destination = _record(item.get("destination")) if item else None
        timetable = _record(item.get("timetable")) if item else None
        scheduled = _record(timetable.get("scheduled")) if timetable else None
        realtime = _record(timetable.get("realtime")) if timetable else None
        if (
            item is None
            or route_origin is None
            or route_destination is None
            or route_origin.get("crsCode") != origin
            or route_destination.get("crsCode") != destination
            or scheduled is None
        ):
            continue
        scheduled_departure = _timestamp_ms(scheduled.get("departure"))
        scheduled_arrival = _timestamp_ms(scheduled.get("arrival"))
        expected_departure = _timestamp_ms(realtime.get("departure")) if realtime else None
        expected_arrival = _timestamp_ms(realtime.get("arrival")) if realtime else None
        if scheduled_departure is None or scheduled_arrival is None or scheduled_arrival < scheduled_departure:
            continue
        provider_status = item.get("status")
        cancelled = isinstance(provider_status, str) and "cancel" in provider_status.lower()
        legs = item.get("legs")
        changes = max(0, len(legs) - 1) if isinstance(legs, list) and len(legs) <= 8 else 0
        row: dict[str, object] = {
            "scheduled_departure_ms": scheduled_departure,
            "scheduled_arrival_ms": scheduled_arrival,
            "status": _status({"status": provider_status}, scheduled_departure, expected_departure, cancelled),
            "changes": changes,
        }
        if expected_departure is not None:
            row["expected_departure_ms"] = expected_departure
        if expected_arrival is not None:
            row["expected_arrival_ms"] = expected_arrival
        departures.append(row)
    departures.sort(key=lambda item: int(item["scheduled_departure_ms"]))
    return departures[:_MAX_DEPARTURES]


def _browser_env(temp_root: str) -> dict[str, str]:
    """Minimal environment: no user flags, profile, extension, or proxy state."""
    return {
        "HOME": temp_root,
        "XDG_CONFIG_HOME": str(Path(temp_root) / "config"),
        "XDG_CACHE_HOME": str(Path(temp_root) / "cache"),
        "TMPDIR": str(Path(temp_root) / "tmp"),
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "NO_PROXY": "*",
    }


def _read_train_departures_once(
    origin_crs: str,
    destination_crs: str,
    *,
    cancelled: threading.Event | None = None,
    deadline: float | None = None,
) -> dict[str, object]:
    """Return bounded typed departures for one generated National Rail route."""
    if (
        not isinstance(origin_crs, str)
        or not isinstance(destination_crs, str)
        or not _CRS.fullmatch(origin_crs)
        or not _CRS.fullmatch(destination_crs)
        or origin_crs == destination_crs
    ):
        raise TrainReadError("invalid station codes")
    stop = cancelled or threading.Event()
    expires = deadline if isinstance(deadline, (int, float)) else time.monotonic() + 25.0
    if stop.is_set() or time.monotonic() >= expires:
        raise TrainReadError("train read cancelled")

    try:
        brave = _find_brave()
        sync_playwright = _load_sync_playwright()
        resolver_rules = _host_resolver_rules()
        captured: dict[str, object] = {}
        request_quota = _RequestQuota(_MAX_BROWSER_REQUESTS)
        with tempfile.TemporaryDirectory(prefix="hermes-g2-trains-") as temp_root:
            Path(temp_root, "tmp").mkdir()
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    executable_path=brave,
                    headless=True,
                    chromium_sandbox=True,
                    env=_browser_env(temp_root),
                    args=[
                        f"--host-resolver-rules={resolver_rules}",
                        "--disable-extensions",
                        "--disable-component-extensions-with-background-pages",
                        "--no-proxy-server",
                        "--no-first-run",
                    ],
                )
                try:
                    context = browser.new_context(
                        service_workers="block",
                        accept_downloads=False,
                    )
                    try:
                        context.route(
                            "**/*",
                            lambda route, request: _route_request(
                                route,
                                request,
                                stop,
                                expires,
                                request_quota,
                            ),
                        )
                        context.route_web_socket("**/*", lambda websocket: websocket.close())
                        page = context.new_page()
                        page.on("dialog", lambda dialog: dialog.dismiss())

                        def close_popup(candidate) -> None:
                            if candidate is not page:
                                candidate.close()

                        context.on("page", close_popup)

                        def capture(response) -> None:
                            try:
                                parsed = urlsplit(response.url)
                                key = None
                                if parsed.hostname == "nreservices.nationalrail.co.uk" and parsed.path == "/live-info":
                                    key = "live"
                                elif parsed.hostname == "jpservices.nationalrail.co.uk" and parsed.path == "/journey-planner":
                                    key = "journeys"
                                if key and key not in captured:
                                    decoded = _decode_response(response)
                                    if decoded is not None:
                                        captured[key] = decoded
                            except Exception:
                                return

                        page.on("response", capture)
                        route_url = (
                            "https://www.nationalrail.co.uk/live-trains/departures/"
                            f"{origin_crs}/{destination_crs}/"
                        )
                        page.goto(route_url, wait_until="domcontentloaded", timeout=18_000)
                        page.wait_for_timeout(_POST_LOAD_SETTLE_MS)
                        if stop.is_set() or time.monotonic() >= expires:
                            raise TrainReadError("train read cancelled")
                        if not _final_route_is_allowed(page.url, origin_crs, destination_crs):
                            raise TrainReadError("National Rail route changed unexpectedly")
                    finally:
                        context.close()
                finally:
                    browser.close()

        live = _parse_live(captured.get("live"), origin_crs, destination_crs)
        if live is None:
            raise TrainReadError("National Rail response was invalid")
        if live["departures"]:
            result = live
        else:
            scheduled = _parse_journeys(captured.get("journeys"), origin_crs, destination_crs)
            result = {
                "data_kind": "next_scheduled" if scheduled else "no_services",
                "observed_at_ms": live["observed_at_ms"],
                "departures": scheduled or [],
            }
        return {
            "source": "National Rail",
            "origin_crs": origin_crs,
            "destination_crs": destination_crs,
            **result,
        }
    except TrainReadError:
        raise
    except Exception:
        raise TrainReadError("National Rail departures unavailable") from None


def read_train_departures(
    origin_crs: str,
    destination_crs: str,
    *,
    cancelled: threading.Event | None = None,
    deadline: float | None = None,
) -> dict[str, object]:
    """Run one typed read, rejecting excess simultaneous browser processes."""
    if not _TRAIN_READ_SLOTS.acquire(blocking=False):
        raise TrainReadError("train reader busy")
    try:
        return _read_train_departures_once(
            origin_crs,
            destination_crs,
            cancelled=cancelled,
            deadline=deadline,
        )
    finally:
        _TRAIN_READ_SLOTS.release()


__all__ = ["TrainReadError", "read_train_departures"]
