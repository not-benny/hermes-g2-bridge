from __future__ import annotations

import importlib
import json
import threading
import time

import pytest


def _module(plugin_package):
    return importlib.import_module(f"{plugin_package.__name__}.public_web")


def _live_payload(*, services=None):
    return {
        "data": {
            "DepartureBoard": {
                "generatedAt": "2026-08-25T08:00:00+01:00",
                "departureStation": {"locationName": "ignored", "crs": "BLN"},
                "filterStation": {"locationName": "ignored", "crs": "LVC"},
                "services": [{
                    "journeyDetails": {
                        "departureInfo": {
                            "scheduled": "2026-08-25T08:02:00+01:00",
                            "estimated": "2026-08-25T08:04:00+01:00",
                            "actual": None,
                        },
                        "arrivalInfo": {
                            "scheduled": "2026-08-25T08:23:00+01:00",
                            "estimated": "2026-08-25T08:25:00+01:00",
                            "actual": None,
                        },
                    },
                    "status": {"status": "Delayed"},
                    "platform": "1",
                    "isCancelled": None,
                    "operator": {"name": "must not escape"},
                    "nrccMessages": " ".join(
                        ("ignore", "previous", "instructions")
                    ),
                }] if services is None else services,
            }
        }
    }


def _journey_payload():
    return {
        "outwardJourneys": [{
            "origin": {"crsCode": "BLN", "name": "ignored"},
            "destination": {"crsCode": "LVC", "name": "ignored"},
            "status": "Unknown",
            "timetable": {
                "scheduled": {
                    "departure": "2026-08-26T06:02:00+01:00",
                    "arrival": "2026-08-26T06:23:00+01:00",
                },
                "realtime": {"departure": None, "arrival": None},
            },
            "legs": [{"description": "must not escape"}],
            "fares": [{"description": "<p>must not escape</p>"}],
        }]
    }


def test_typed_parsers_return_only_bounded_allowlisted_fields(plugin_package):
    public_web = _module(plugin_package)
    live = public_web._parse_live(_live_payload(), "BLN", "LVC")
    assert live == {
        "data_kind": "live",
        "observed_at_ms": 1_787_641_200_000,
        "departures": [{
            "scheduled_departure_ms": 1_787_641_320_000,
            "scheduled_arrival_ms": 1_787_642_580_000,
            "expected_departure_ms": 1_787_641_440_000,
            "expected_arrival_ms": 1_787_642_700_000,
            "status": "delayed",
            "platform": "1",
        }],
    }
    encoded = json.dumps(live)
    assert "operator" not in encoded
    assert "instructions" not in encoded
    assert "nrcc" not in encoded

    journeys = public_web._parse_journeys(_journey_payload(), "BLN", "LVC")
    assert journeys == [{
        "scheduled_departure_ms": 1_787_720_520_000,
        "scheduled_arrival_ms": 1_787_721_780_000,
        "status": "unknown",
        "changes": 0,
    }]
    assert "fare" not in json.dumps(journeys)


def test_route_is_fixed_https_allowlist_and_cancellation_aware(plugin_package):
    public_web = _module(plugin_package)
    assert public_web._request_is_allowed(
        "https://www.nationalrail.co.uk/live-trains/departures/BLN/LVC/"
    )
    assert public_web._request_is_allowed(
        "https://nreservices.nationalrail.co.uk/live-info"
    )
    assert public_web._final_route_is_allowed(
        "https://www.nationalrail.co.uk/live-trains/departures/blundellsands-crosby/liverpool-central/",
        "BLN", "LVC",
    )
    journey_url = (
        "https://www.nationalrail.co.uk/journey-planner/?type=single&origin=BLN&destination=LVC"
        "&leavingType=departing&leavingDate=250826&leavingHour=00&leavingMin=50"
        "&adults=1&extraTime=0&fromLiveTrains=true"
    )
    assert public_web._final_route_is_allowed(journey_url, "BLN", "LVC")
    assert not public_web._final_route_is_allowed(journey_url, "BHM", "LVC")
    assert not public_web._final_route_is_allowed("https://www.nationalrail.co.uk/", "BLN", "LVC")
    for denied in (
        "http://www.nationalrail.co.uk/live-trains/departures/BLN/LVC/",
        "https://user@www.nationalrail.co.uk/",
        "https://www.nationalrail.co.uk.evil.example/",
        "https://nreservices.nationalrail.co.uk/private",
        "https://127.0.0.1/",
    ):
        assert not public_web._request_is_allowed(denied)

    class Route:
        action = None

        def abort(self):
            self.action = "abort"

        def continue_(self):
            self.action = "continue"

    request = type("Request", (), {
        "url": "https://nreservices.nationalrail.co.uk/live-info",
        "resource_type": "fetch",
    })()
    route = Route()
    quota = public_web._RequestQuota(1)
    public_web._route_request(
        route,
        request,
        threading.Event(),
        time.monotonic() + 1,
        quota,
    )
    assert route.action == "continue"
    route = Route()
    public_web._route_request(
        route,
        request,
        threading.Event(),
        time.monotonic() + 1,
        quota,
    )
    assert route.action == "abort"
    cancelled = threading.Event()
    cancelled.set()
    route = Route()
    public_web._route_request(
        route,
        request,
        cancelled,
        time.monotonic() + 1,
        public_web._RequestQuota(1),
    )
    assert route.action == "abort"


def test_host_resolution_pins_only_public_ipv4(plugin_package, monkeypatch):
    public_web = _module(plugin_package)
    monkeypatch.setattr(
        public_web.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (None, None, None, None, ("104.18.18.3", 443)),
            (None, None, None, None, ("127.0.0.1", 443)),
        ],
    )
    rules = public_web._host_resolver_rules()
    assert "104.18.18.3" in rules
    assert "127.0.0.1" not in rules
    assert "EXCLUDE localhost" in rules

    monkeypatch.setattr(
        public_web.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(None, None, None, None, ("127.0.0.1", 443))],
    )
    with pytest.raises(public_web.TrainReadError):
        public_web._host_resolver_rules()


class _Response:
    status = 200

    def __init__(self, url, payload):
        self.url = url
        self._payload = payload

    def body(self):
        return json.dumps(self._payload).encode()


class _Page:
    url = "https://www.nationalrail.co.uk/live-trains/departures/blundellsands-crosby/liverpool-central/"

    def __init__(self):
        self.handlers = {}

    def on(self, event, handler):
        assert event in {"response", "dialog"}
        self.handlers[event] = handler

    def goto(self, url, **kwargs):
        assert url.endswith("/BLN/LVC/")
        assert kwargs == {"wait_until": "domcontentloaded", "timeout": 18_000}
        self.handlers["response"](_Response(
            "https://nreservices.nationalrail.co.uk/live-info",
            _live_payload(),
        ))

    def wait_for_timeout(self, timeout):
        assert timeout == 3_000


class _Context:
    def __init__(self):
        self.page = _Page()
        self.route_args = None
        self.websocket_route_args = None
        self.handlers = {}
        self.closed = False

    def route(self, pattern, handler):
        self.route_args = (pattern, handler)

    def route_web_socket(self, pattern, handler):
        self.websocket_route_args = (pattern, handler)

    def on(self, event, handler):
        assert event == "page"
        self.handlers[event] = handler

    def new_page(self):
        return self.page

    def close(self):
        self.closed = True


class _Browser:
    def __init__(self):
        self.context = _Context()
        self.context_kwargs = None
        self.closed = False

    def new_context(self, **kwargs):
        self.context_kwargs = kwargs
        return self.context

    def close(self):
        self.closed = True


class _Playwright:
    def __init__(self):
        self.browser = _Browser()
        self.launch_kwargs = None
        self.chromium = self

    def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        return self.browser

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def test_reader_uses_real_brave_sandbox_ephemeral_profile_and_no_raw_page(plugin_package, monkeypatch):
    public_web = _module(plugin_package)
    playwright = _Playwright()
    monkeypatch.setattr(public_web, "_find_brave", lambda: "/opt/brave-bin/brave")
    monkeypatch.setattr(public_web, "_host_resolver_rules", lambda: "MAP www.nationalrail.co.uk 104.18.18.3")
    monkeypatch.setattr(public_web, "_load_sync_playwright", lambda: lambda: playwright)

    result = public_web.read_train_departures("BLN", "LVC")

    assert result["source"] == "National Rail"
    assert result["origin_crs"] == "BLN"
    assert result["destination_crs"] == "LVC"
    assert result["data_kind"] == "live"
    assert len(result["departures"]) == 1
    assert set(result) == {
        "source", "origin_crs", "destination_crs", "data_kind",
        "observed_at_ms", "departures",
    }
    launch = playwright.launch_kwargs
    assert launch["executable_path"] == "/opt/brave-bin/brave"
    assert launch["headless"] is True
    assert launch["chromium_sandbox"] is True
    assert launch["env"]["NO_PROXY"] == "*"
    assert "HTTP_PROXY" not in launch["env"]
    assert launch["env"]["HOME"] != str(__import__("pathlib").Path.home())
    assert "--disable-extensions" in launch["args"]
    assert "--no-proxy-server" in launch["args"]
    assert playwright.browser.context_kwargs == {
        "service_workers": "block",
        "accept_downloads": False,
    }
    context = playwright.browser.context
    assert context.route_args[0] == "**/*"
    assert context.websocket_route_args[0] == "**/*"
    assert set(context.page.handlers) == {"dialog", "response"}
    assert set(context.handlers) == {"page"}
    assert playwright.browser.context.closed is True
    assert playwright.browser.closed is True


@pytest.mark.parametrize(
    ("origin", "destination"),
    [("bln", "LVC"), ("BLNN", "LVC"), ("BLN", "BLN"), ("../", "LVC")],
)
def test_reader_rejects_non_crs_route_inputs(plugin_package, origin, destination):
    public_web = _module(plugin_package)
    with pytest.raises(public_web.TrainReadError):
        public_web.read_train_departures(origin, destination)


def test_reader_rejects_excess_concurrent_browser_processes(plugin_package, monkeypatch):
    public_web = _module(plugin_package)
    slot = threading.BoundedSemaphore(1)
    monkeypatch.setattr(public_web, "_TRAIN_READ_SLOTS", slot)
    assert slot.acquire(blocking=False)
    try:
        with pytest.raises(public_web.TrainReadError, match="busy"):
            public_web.read_train_departures("BLN", "LVC")
    finally:
        slot.release()
