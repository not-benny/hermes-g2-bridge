"""Typed, zero-key UK weather reader backed by Open-Meteo and UKMO.

The public result is deliberately smaller than either upstream response: no
page text, provider labels, URLs, metadata, or error bodies cross the boundary.
The user-supplied place label is treated only as inert geocoding input and is
the only label returned.

UK Met Office data distributed by Open-Meteo is licensed under CC BY-SA 4.0.
Derived products must retain the same or a compatible licence and attribute
``Open-Meteo · UK Met Office data``.  Keep this notice with integrations that
surface results from this module.
"""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
import json
import math
import os
import socket
import tempfile
import threading
import time
import unicodedata
from datetime import date as calendar_date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypedDict
from urllib.parse import parse_qs, urlencode, urlsplit
from zoneinfo import ZoneInfo


SOURCE_ATTRIBUTION = "Open-Meteo · UK Met Office data"
DATA_LICENCE_NOTICE = (
    "UK Met Office data via Open-Meteo is CC BY-SA 4.0; derived products "
    "must retain the same or a compatible licence."
)


class WeatherProviderError(RuntimeError):
    """A safe provider failure that never includes upstream content or URLs."""


class WeatherInputError(ValueError):
    """A bounded validation failure for caller-controlled query fields."""


class WeatherLocationNotFound(WeatherProviderError):
    """The bounded UK geocoder returned no usable result."""


class WeatherLocationAmbiguous(WeatherProviderError):
    """More than one exact UK place matched; the caller must add a region."""


WeatherCondition = Literal[
    "clear",
    "partly_cloudy",
    "cloudy",
    "fog",
    "drizzle",
    "rain",
    "snow",
    "showers",
    "snow_showers",
    "thunderstorm",
]


class WeatherResult(TypedDict):
    location_label: str
    date: str
    weather_code: int
    condition: WeatherCondition
    temperature_min_c: float
    temperature_max_c: float
    precipitation_probability_max_pct: int | None
    precipitation_amount_mm: float | None
    wind_speed_max_kmh: float
    source: Literal["Open-Meteo · UK Met Office data"]
    observed_at_ms: int


_BRAVE_BINARY = Path("/opt/brave-bin/brave")
_GEOCODING_HOST = "geocoding-api.open-meteo.com"
_FORECAST_HOST = "api.open-meteo.com"
_GEOCODING_PATH = "/v1/search"
_FORECAST_PATH = "/v1/forecast"
_ALLOWED_HOST_PATHS = {
    _GEOCODING_HOST: _GEOCODING_PATH,
    _FORECAST_HOST: _FORECAST_PATH,
}
_FORECAST_MODEL = "ukmo_seamless"
_FORECAST_TIMEZONE = "Europe/London"
_LONDON = ZoneInfo(_FORECAST_TIMEZONE)
_DAILY_FIELDS = (
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_probability_max",
    "precipitation_sum",
    "wind_speed_10m_max",
)
_KNOWN_WEATHER_CODES = frozenset({
    0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
    71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99,
})
_CONDITION_BY_CODE: dict[int, WeatherCondition] = {
    0: "clear",
    1: "partly_cloudy",
    2: "partly_cloudy",
    3: "cloudy",
    45: "fog",
    48: "fog",
    51: "drizzle",
    53: "drizzle",
    55: "drizzle",
    56: "drizzle",
    57: "drizzle",
    61: "rain",
    63: "rain",
    65: "rain",
    66: "rain",
    67: "rain",
    71: "snow",
    73: "snow",
    75: "snow",
    77: "snow",
    80: "showers",
    81: "showers",
    82: "showers",
    85: "snow_showers",
    86: "snow_showers",
    95: "thunderstorm",
    96: "thunderstorm",
    99: "thunderstorm",
}
_LOCATION_PUNCTUATION = frozenset(" '-,.()&\u2019")
_UK_COUNTRY_QUALIFIERS = frozenset({
    "gb",
    "great britain",
    "uk",
    "united kingdom",
})
_MAX_LOCATION_CHARS = 80
_MAX_LOCATION_BYTES = 160
_MAX_URL_CHARS = 2_048
_MAX_GEOCODING_BYTES = 65_536
_MAX_FORECAST_BYTES = 65_536
_MAX_REQUESTS = 2
_MAX_REQUEST_ATTEMPTS = 8
_MAX_CONCURRENT_READS = 2
_TOTAL_TIMEOUT_SECONDS = 15.0
_CLOSE_TIMEOUT_SECONDS = 1.0
_READ_SLOTS = threading.BoundedSemaphore(_MAX_CONCURRENT_READS)


def _find_brave() -> str:
    """Use the real executable, never a wrapper that reads a user profile."""
    if (
        not _BRAVE_BINARY.is_file()
        or _BRAVE_BINARY.is_symlink()
        or not os.access(_BRAVE_BINARY, os.X_OK)
    ):
        raise WeatherProviderError("weather browser unavailable")
    return str(_BRAVE_BINARY)


def _load_async_playwright():
    try:
        from playwright.async_api import async_playwright
    except Exception:
        raise WeatherProviderError("weather browser unavailable") from None
    return async_playwright


def _normalise_location(value: object) -> str:
    if not isinstance(value, str):
        raise WeatherInputError("location must be text")
    normalised = unicodedata.normalize("NFC", value)
    if any(character.isspace() and character != " " for character in normalised):
        raise WeatherInputError("location contains unsupported whitespace")
    normalised = " ".join(normalised.split())
    if (
        not normalised
        or len(normalised) > _MAX_LOCATION_CHARS
        or len(normalised.encode("utf-8")) > _MAX_LOCATION_BYTES
    ):
        raise WeatherInputError("location is empty or too long")
    has_word_character = False
    for character in normalised:
        category = unicodedata.category(character)
        if category[0] in {"L", "N"}:
            has_word_character = True
            continue
        if category[0] == "M":
            continue
        if character in _LOCATION_PUNCTUATION:
            continue
        raise WeatherInputError("location contains unsupported characters")
    if not has_word_character:
        raise WeatherInputError("location must contain a letter or number")
    return normalised


def _local_today() -> calendar_date:
    return datetime.now(_LONDON).date()


def capture_reference_date() -> calendar_date:
    """Freeze the wearer's local calendar date for one authorised read."""
    return _local_today()


def _target_date(
    *,
    day_offset: int | None,
    requested_date: str | None,
    today: calendar_date | None = None,
) -> calendar_date:
    base = today or _local_today()
    if day_offset is not None and requested_date is not None:
        raise WeatherInputError("choose either day_offset or date")
    if requested_date is not None:
        if not isinstance(requested_date, str) or len(requested_date) != 10:
            raise WeatherInputError("date must be YYYY-MM-DD")
        try:
            target = calendar_date.fromisoformat(requested_date)
        except ValueError:
            raise WeatherInputError("date must be YYYY-MM-DD") from None
        if target.isoformat() != requested_date:
            raise WeatherInputError("date must be YYYY-MM-DD")
    else:
        offset = 0 if day_offset is None else day_offset
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= 7:
            raise WeatherInputError("day_offset must be from 0 to 7")
        target = base + timedelta(days=offset)
    if not base <= target <= base + timedelta(days=7):
        raise WeatherInputError("date must be within the next eight days")
    return target


def _pinned_public_ipv4(host: str) -> str:
    if host not in _ALLOWED_HOST_PATHS:
        raise WeatherProviderError("weather host unavailable")
    try:
        addresses = {
            ipaddress.ip_address(info[4][0])
            for info in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        }
    except (OSError, ValueError):
        raise WeatherProviderError("weather host unavailable") from None
    candidates = sorted(
        str(address)
        for address in addresses
        if address.version == 4 and address.is_global
    )
    if not candidates:
        raise WeatherProviderError("weather host unavailable")
    return candidates[0]


def _host_resolver_rules() -> str:
    mappings = [
        f"MAP {host} {_pinned_public_ipv4(host)}"
        for host in sorted(_ALLOWED_HOST_PATHS)
    ]
    # The fixed mappings preserve TLS hostname verification while preventing
    # Chromium (including browser-level background services) from resolving a
    # third-party hostname. Playwright's local control channel remains exempt.
    return ",".join([*mappings, "MAP * ~NOTFOUND", "EXCLUDE localhost"])


def _geocoding_url(location: str) -> str:
    search_name, _region = _location_parts(location)
    query = urlencode({
        "name": search_name,
        "count": "5",
        "language": "en",
        "format": "json",
        "countryCode": "GB",
    })
    return f"https://{_GEOCODING_HOST}{_GEOCODING_PATH}?{query}"


def _forecast_url(latitude: float, longitude: float, target: calendar_date) -> str:
    date_value = target.isoformat()
    query = urlencode({
        "latitude": f"{latitude:.6f}",
        "longitude": f"{longitude:.6f}",
        "daily": ",".join(_DAILY_FIELDS),
        "models": _FORECAST_MODEL,
        "timezone": _FORECAST_TIMEZONE,
        "start_date": date_value,
        "end_date": date_value,
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    })
    return f"https://{_FORECAST_HOST}{_FORECAST_PATH}?{query}"


def _request_is_allowed(url: object, expected_url: object) -> bool:
    if (
        not isinstance(url, str)
        or not isinstance(expected_url, str)
        or url != expected_url
        or len(url) > _MAX_URL_CHARS
    ):
        return False
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        if not (
            parsed.scheme == "https"
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
            and parsed.fragment == ""
            and _ALLOWED_HOST_PATHS.get(host) == parsed.path
        ):
            return False
        values = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        if any(len(items) != 1 for items in values.values()):
            return False
        if host == _GEOCODING_HOST:
            allowed = (
                set(values) == {"name", "count", "language", "format", "countryCode"}
                and values["count"] == ["5"]
                and values["language"] == ["en"]
                and values["format"] == ["json"]
                and values["countryCode"] == ["GB"]
            )
            if not allowed:
                return False
            try:
                return _normalise_location(values["name"][0]) == values["name"][0]
            except WeatherInputError:
                return False
        if host == _FORECAST_HOST:
            allowed = (
                set(values) == {
                    "latitude", "longitude", "daily", "models", "timezone",
                    "start_date", "end_date", "temperature_unit",
                    "wind_speed_unit", "precipitation_unit",
                }
                and values["daily"] == [",".join(_DAILY_FIELDS)]
                and values["models"] == [_FORECAST_MODEL]
                and values["timezone"] == [_FORECAST_TIMEZONE]
                and values["start_date"] == values["end_date"]
                and values["temperature_unit"] == ["celsius"]
                and values["wind_speed_unit"] == ["kmh"]
                and values["precipitation_unit"] == ["mm"]
            )
            if not allowed:
                return False
            latitude = _finite_number_from_text(values["latitude"][0], 49.0, 61.5)
            longitude = _finite_number_from_text(values["longitude"][0], -8.7, 2.2)
            try:
                parsed_date = calendar_date.fromisoformat(values["start_date"][0])
            except ValueError:
                return False
            return bool(
                latitude is not None
                and longitude is not None
                and parsed_date.isoformat() == values["start_date"][0]
            )
    except (TypeError, ValueError):
        return False
    return False


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _decode_json(body: object, maximum_bytes: int) -> dict[str, Any]:
    if not isinstance(body, bytes) or not body or len(body) > maximum_bytes:
        raise WeatherProviderError("weather data unavailable")
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError):
        raise WeatherProviderError("weather data unavailable") from None
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise WeatherProviderError("weather data unavailable")
    return value


def _finite_number(value: object, minimum: float, maximum: float) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and minimum <= number <= maximum else None


def _finite_number_from_text(value: object, minimum: float, maximum: float) -> float | None:
    if not isinstance(value, str) or not value or len(value) > 24:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) and minimum <= number <= maximum else None


def _safe_provider_place(value: object) -> str | None:
    try:
        return _normalise_location(value)
    except WeatherInputError:
        return None


def _location_parts(location: str) -> tuple[str, str | None]:
    parts = [part.strip() for part in location.split(",")]
    if len(parts) > 2 or not parts[0] or (len(parts) == 2 and not parts[1]):
        raise WeatherInputError("location may contain at most one region qualifier")
    region = parts[1] if len(parts) == 2 else None
    # Every generated request is already pinned to countryCode=GB.  Models and
    # people commonly add a harmless country suffix (for example
    # ``Liverpool, UK``); do not misinterpret that suffix as a county and then
    # reject an otherwise exact UK place.  Real county/region qualifiers remain
    # significant for disambiguation.
    if region is not None and region.casefold() in _UK_COUNTRY_QUALIFIERS:
        region = None
    return parts[0], region


def _parse_geocoding(payload: object, location: str) -> tuple[float, float, str]:
    if not isinstance(payload, dict):
        raise WeatherProviderError("weather data unavailable")
    results = payload.get("results")
    if not isinstance(results, list):
        raise WeatherLocationNotFound("UK weather location not found")
    requested_name, requested_region = _location_parts(location)
    requested_name_folded = requested_name.casefold()
    requested_region_folded = requested_region.casefold() if requested_region else None
    matches: list[tuple[float, float, str]] = []
    for item in results[:5]:
        if not isinstance(item, dict) or item.get("country_code") != "GB":
            continue
        canonical_name = _safe_provider_place(item.get("name"))
        if canonical_name is None or canonical_name.casefold() != requested_name_folded:
            continue
        canonical_region: str | None = None
        if requested_region_folded is not None:
            for key in ("admin3", "admin2", "admin1", "country"):
                candidate = _safe_provider_place(item.get(key))
                if candidate is not None and candidate.casefold() == requested_region_folded:
                    canonical_region = candidate
                    break
            if canonical_region is None:
                continue
        latitude = _finite_number(item.get("latitude"), 49.0, 61.5)
        longitude = _finite_number(item.get("longitude"), -8.7, 2.2)
        if latitude is not None and longitude is not None:
            canonical_label = (
                f"{canonical_name}, {canonical_region}"
                if canonical_region is not None
                else canonical_name
            )
            candidate = (latitude, longitude, canonical_label)
            if candidate not in matches:
                matches.append(candidate)
    if not matches:
        raise WeatherLocationNotFound("UK weather location not found")
    if len(matches) > 1:
        raise WeatherLocationAmbiguous(
            "UK weather location is ambiguous; add a county or region"
        )
    return matches[0]


def _single_value(container: dict[str, Any], key: str) -> object:
    values = container.get(key)
    if not isinstance(values, list) or len(values) != 1:
        raise WeatherProviderError("weather data unavailable")
    return values[0]


def _unit(units: dict[str, Any], key: str, expected: str) -> None:
    if units.get(key) != expected:
        raise WeatherProviderError("weather data unavailable")


def _optional_probability(daily: dict[str, Any], units: dict[str, Any]) -> int | None:
    raw = _single_value(daily, "precipitation_probability_max")
    if raw is None:
        if units.get("precipitation_probability_max") not in {"%", "undefined"}:
            raise WeatherProviderError("weather data unavailable")
        return None
    probability = _finite_number(raw, 0.0, 100.0)
    if probability is None or not probability.is_integer():
        raise WeatherProviderError("weather data unavailable")
    _unit(units, "precipitation_probability_max", "%")
    return int(probability)


def _parse_forecast(
    payload: object,
    *,
    location_label: str,
    target: calendar_date,
    observed_at_ms: int,
) -> WeatherResult:
    if not isinstance(payload, dict):
        raise WeatherProviderError("weather data unavailable")
    if payload.get("timezone") != _FORECAST_TIMEZONE:
        raise WeatherProviderError("weather data unavailable")
    daily = payload.get("daily")
    units = payload.get("daily_units")
    if not isinstance(daily, dict) or not isinstance(units, dict):
        raise WeatherProviderError("weather data unavailable")
    if _single_value(daily, "time") != target.isoformat():
        raise WeatherProviderError("weather data unavailable")
    _unit(units, "time", "iso8601")
    _unit(units, "weather_code", "wmo code")
    _unit(units, "temperature_2m_max", "°C")
    _unit(units, "temperature_2m_min", "°C")
    _unit(units, "precipitation_sum", "mm")
    _unit(units, "wind_speed_10m_max", "km/h")

    raw_code = _single_value(daily, "weather_code")
    if isinstance(raw_code, bool) or not isinstance(raw_code, int) or raw_code not in _KNOWN_WEATHER_CODES:
        raise WeatherProviderError("weather data unavailable")
    minimum = _finite_number(_single_value(daily, "temperature_2m_min"), -90.0, 65.0)
    maximum = _finite_number(_single_value(daily, "temperature_2m_max"), -90.0, 65.0)
    precipitation = _finite_number(_single_value(daily, "precipitation_sum"), 0.0, 2_000.0)
    wind = _finite_number(_single_value(daily, "wind_speed_10m_max"), 0.0, 500.0)
    probability = _optional_probability(daily, units)
    if minimum is None or maximum is None or minimum > maximum or precipitation is None or wind is None:
        raise WeatherProviderError("weather data unavailable")

    return {
        "location_label": location_label,
        "date": target.isoformat(),
        "weather_code": raw_code,
        "condition": _CONDITION_BY_CODE[raw_code],
        "temperature_min_c": round(minimum, 1),
        "temperature_max_c": round(maximum, 1),
        "precipitation_probability_max_pct": probability,
        "precipitation_amount_mm": round(precipitation, 1),
        "wind_speed_max_kmh": round(wind, 1),
        "source": SOURCE_ATTRIBUTION,
        "observed_at_ms": observed_at_ms,
    }


def _observed_at_ms() -> int:
    """Return host retrieval completion time, never an upstream model run time."""
    return int(time.time() * 1_000)


def _remaining_ms(deadline: float) -> int:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise WeatherProviderError("weather request timed out")
    return max(1, min(5_000, int(remaining * 1_000)))


def _scrubbed_browser_env(root: Path) -> dict[str, str]:
    home = root / "home"
    config = root / "config"
    cache = root / "cache"
    downloads = root / "downloads"
    for directory in (home, config, cache, downloads):
        directory.mkdir(mode=0o700)
    return {
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(config),
        "XDG_CACHE_HOME": str(cache),
        "TMPDIR": str(root),
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": _FORECAST_TIMEZONE,
    }


class _RouteGuard:
    def __init__(self) -> None:
        self.expected_url: str | None = None
        self.remaining = _MAX_REQUESTS
        self.attempts_remaining = _MAX_REQUEST_ATTEMPTS

    async def handle(self, route, request) -> None:
        allowed = False
        if self.attempts_remaining > 0:
            self.attempts_remaining -= 1
            try:
                allowed = bool(
                    self.remaining > 0
                    and request.method == "GET"
                    and request.resource_type == "document"
                    and request.is_navigation_request()
                    and _request_is_allowed(request.url, self.expected_url)
                )
            except Exception:
                allowed = False
        if not allowed:
            await route.abort("blockedbyclient")
            return
        self.remaining -= 1
        await route.continue_(headers={
            "Accept": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
        })


async def _block_websocket(websocket_route) -> None:
    await websocket_route.close(code=1008, reason="blocked")


def _content_length(headers: object) -> int | None:
    if not isinstance(headers, dict):
        return None
    raw = headers.get("content-length")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.isdigit():
        raise WeatherProviderError("weather data unavailable")
    value = int(raw)
    if value < 0:
        raise WeatherProviderError("weather data unavailable")
    return value


async def _fetch_json(page, guard: _RouteGuard, url: str, maximum_bytes: int, deadline: float) -> dict[str, Any]:
    guard.expected_url = url
    try:
        response = await page.goto(url, wait_until="commit", timeout=_remaining_ms(deadline))
    finally:
        guard.expected_url = None
    if response is None or response.status != 200 or response.url != url:
        raise WeatherProviderError("weather data unavailable")
    content_type = response.headers.get("content-type", "")
    if not isinstance(content_type, str) or content_type.split(";", 1)[0].strip().lower() != "application/json":
        raise WeatherProviderError("weather data unavailable")
    announced = _content_length(response.headers)
    if announced is not None and announced > maximum_bytes:
        raise WeatherProviderError("weather data unavailable")
    body = await response.body()
    return _decode_json(body, maximum_bytes)


def _schedule_rejection(awaitable, tasks: set[asyncio.Task[Any]]) -> None:
    if not inspect.isawaitable(awaitable):
        return
    task = asyncio.create_task(awaitable)
    tasks.add(task)
    task.add_done_callback(tasks.discard)


async def _close_safely(instance: object, method_name: str = "close") -> None:
    method = getattr(instance, method_name, None)
    if not callable(method):
        return
    try:
        result = method()
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, _CLOSE_TIMEOUT_SECONDS)
    except (Exception, asyncio.CancelledError):
        pass


async def _acquire_slot(deadline: float) -> None:
    while not _READ_SLOTS.acquire(blocking=False):
        if time.monotonic() >= deadline:
            raise WeatherProviderError("weather provider busy")
        await asyncio.sleep(0.025)


async def _browser_read(location: str, target: calendar_date, deadline: float) -> WeatherResult:
    resolver_rules = await asyncio.to_thread(_host_resolver_rules)
    browser = None
    context = None
    page = None
    playwright = None
    background_tasks: set[asyncio.Task[Any]] = set()
    with tempfile.TemporaryDirectory(prefix="hermes-weather-") as profile:
        root = Path(profile)
        environment = _scrubbed_browser_env(root)
        manager = _load_async_playwright()()
        try:
            playwright = await manager.start()
            browser = await playwright.chromium.launch(
                executable_path=_find_brave(),
                headless=True,
                chromium_sandbox=True,
                downloads_path=str(root / "downloads"),
                env=environment,
                timeout=_remaining_ms(deadline),
                args=[
                    f"--host-resolver-rules={resolver_rules}",
                    "--disable-background-networking",
                    "--disable-breakpad",
                    "--disable-client-side-phishing-detection",
                    "--disable-component-update",
                    "--disable-default-apps",
                    "--disable-domain-reliability",
                    "--disable-sync",
                    "--metrics-recording-only",
                    "--no-default-browser-check",
                    "--no-first-run",
                    "--no-proxy-server",
                ],
            )
            context = await browser.new_context(
                accept_downloads=False,
                service_workers="block",
                java_script_enabled=False,
                ignore_https_errors=False,
                bypass_csp=False,
                locale="en-GB",
                timezone_id=_FORECAST_TIMEZONE,
                viewport={"width": 800, "height": 600},
            )
            guard = _RouteGuard()
            await context.route("**/*", guard.handle)
            await context.route_web_socket("**/*", _block_websocket)
            page = await context.new_page()

            def reject_page(extra_page) -> None:
                if extra_page is not page:
                    _schedule_rejection(extra_page.close(), background_tasks)

            def reject_download(download) -> None:
                _schedule_rejection(download.cancel(), background_tasks)

            context.on("page", reject_page)
            page.on("popup", reject_page)
            page.on("download", reject_download)

            geocoding = await _fetch_json(
                page,
                guard,
                _geocoding_url(location),
                _MAX_GEOCODING_BYTES,
                deadline,
            )
            latitude, longitude, canonical_label = _parse_geocoding(geocoding, location)
            forecast = await _fetch_json(
                page,
                guard,
                _forecast_url(latitude, longitude, target),
                _MAX_FORECAST_BYTES,
                deadline,
            )
            return _parse_forecast(
                forecast,
                location_label=canonical_label,
                target=target,
                observed_at_ms=_observed_at_ms(),
            )
        finally:
            if background_tasks:
                for task in tuple(background_tasks):
                    task.cancel()
                await asyncio.gather(*background_tasks, return_exceptions=True)
            if page is not None:
                await _close_safely(page)
            if context is not None:
                await _close_safely(context)
            if browser is not None:
                await _close_safely(browser)
            if playwright is not None:
                await _close_safely(playwright, "stop")


async def _run_bounded(
    location: str,
    target: calendar_date,
    deadline: float,
) -> WeatherResult:
    acquired = False
    try:
        await _acquire_slot(deadline)
        acquired = True
        return await _browser_read(location, target, deadline)
    finally:
        if acquired:
            _READ_SLOTS.release()


async def _cancel_task(task: asyncio.Task[Any]) -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except (Exception, asyncio.CancelledError):
        pass


async def read_weather(
    location: str,
    *,
    day_offset: int | None = None,
    date: str | None = None,
    cancel_event: asyncio.Event | None = None,
    timeout_seconds: float = _TOTAL_TIMEOUT_SECONDS,
    reference_date: calendar_date | None = None,
) -> WeatherResult:
    """Return one bounded UKMO daily forecast without keys or browser UI.

    ``date`` is an ISO local date and is mutually exclusive with
    ``day_offset``.  Both are limited to today through seven days ahead.
    Cancelling this coroutine, or setting ``cancel_event``, tears down the
    ephemeral browser before propagating cancellation.
    """
    label = _normalise_location(location)
    _location_parts(label)
    if reference_date is not None and type(reference_date) is not calendar_date:
        raise WeatherInputError("reference_date must be a local calendar date")
    target = _target_date(
        day_offset=day_offset,
        requested_date=date,
        today=reference_date,
    )
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(float(timeout_seconds))
        or not 0.1 <= float(timeout_seconds) <= _TOTAL_TIMEOUT_SECONDS
    ):
        raise WeatherInputError("timeout is out of range")
    if cancel_event is not None and not isinstance(cancel_event, asyncio.Event):
        raise WeatherInputError("cancel_event must be an asyncio.Event")
    deadline = time.monotonic() + float(timeout_seconds)
    operation = asyncio.create_task(_run_bounded(label, target, deadline))
    cancellation = asyncio.create_task(cancel_event.wait()) if cancel_event is not None else None
    try:
        waiters: set[asyncio.Task[Any]] = {operation}
        if cancellation is not None:
            waiters.add(cancellation)
        done, _pending = await asyncio.wait(
            waiters,
            timeout=max(0.0, deadline - time.monotonic()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if operation in done:
            return operation.result()
        if cancellation is not None and cancellation in done:
            await _cancel_task(operation)
            raise asyncio.CancelledError
        await _cancel_task(operation)
        raise WeatherProviderError("weather request timed out")
    except asyncio.CancelledError:
        await _cancel_task(operation)
        raise
    except (WeatherProviderError, WeatherInputError):
        raise
    except Exception:
        await _cancel_task(operation)
        raise WeatherProviderError("weather data unavailable") from None
    finally:
        if cancellation is not None:
            await _cancel_task(cancellation)


__all__ = [
    "DATA_LICENCE_NOTICE",
    "SOURCE_ATTRIBUTION",
    "WeatherInputError",
    "WeatherCondition",
    "WeatherLocationAmbiguous",
    "WeatherLocationNotFound",
    "WeatherProviderError",
    "WeatherResult",
    "capture_reference_date",
    "read_weather",
]
