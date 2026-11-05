import os
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(None, -1)


@pytest.fixture(scope="module")
def downloader():
    return load(("nb_http_download", ALL_CELLS))


@pytest.fixture(scope="module")
def sources():
    return load(("nb_download_sources", DEFINITION_CELLS))


@pytest.fixture(scope="module")
def openaq():
    return load(("nb_http_download", ALL_CELLS), ("nb_ingest_openaq", DEFINITION_CELLS))


class FakeSession:
    def __init__(self, head_status, get_status=200):
        self.head_status = head_status
        self.get_status = get_status
        self.calls = []

    def head(self, url, **kwargs):
        self.calls.append("HEAD")
        return SimpleNamespace(status_code=self.head_status)

    @contextmanager
    def get(self, url, **kwargs):
        self.calls.append("GET")
        yield SimpleNamespace(status_code=self.get_status)


@pytest.mark.parametrize(
    "url",
    ["https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", "  http://example.com/data.json  "],
)
def test_validator_accepts_http_urls(downloader, url):
    assert downloader["UrlValidator"]().validate(url) == url.strip()


@pytest.mark.parametrize("url", ["ftp://example.com/file.csv", "https://", "not a url", "", "https://exa mple.com"])
def test_validator_rejects_invalid_urls(downloader, url):
    with pytest.raises(downloader["InvalidUrlError"]):
        downloader["UrlValidator"]().validate(url)


@pytest.mark.parametrize(
    ("head_status", "get_status", "expected_calls"),
    [(200, 200, ["HEAD"]), (405, 200, ["HEAD", "GET"])],
)
def test_existence_checker_accepts_reachable_urls(downloader, head_status, get_status, expected_calls):
    session = FakeSession(head_status, get_status)
    downloader["UrlExistenceChecker"](session, 5).ensure_exists("https://example.com/a.csv", {})
    assert session.calls == expected_calls


@pytest.mark.parametrize(("head_status", "get_status"), [(404, 200), (403, 403)])
def test_existence_checker_rejects_missing_urls(downloader, head_status, get_status):
    with pytest.raises(downloader["UrlNotFoundError"]):
        downloader["UrlExistenceChecker"](FakeSession(head_status, get_status), 5).ensure_exists("https://example.com/a", {})


@pytest.mark.parametrize(
    ("url", "content_type", "expected"),
    [
        ("https://host/trip-data/yellow_tripdata_2024-01.parquet", "binary/octet-stream", "yellow_tripdata_2024-01.parquet"),
        ("https://host/misc/taxi_zone_lookup.csv", "text/csv", "taxi_zone_lookup.csv"),
        ("https://host/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata", "text/csv", "D.USD.EUR.SP00.A.csv"),
        ("https://host/v2/country/USA/indicator/NY.GDP.PCAP.CD?format=json", "application/json;charset=utf-8", "NY.GDP.PCAP.CD.json"),
        ("https://host/archive.bin", "application/octet-stream", "archive.bin"),
    ],
)
def test_file_name_resolver(downloader, url, content_type, expected):
    assert downloader["FileNameResolver"]().resolve(url, content_type) == expected


def test_downloader_saves_resolved_name_and_removes_temp_file(downloader, tmp_path):
    local_file = tmp_path / "payload"
    local_file.write_bytes(b"a,b\n1,2\n")
    saved = {}

    class Fetcher:
        def fetch(self, url, headers):
            return downloader["DownloadedFile"](str(local_file), "text/csv", 8)

    class Checker:
        def ensure_exists(self, url, headers):
            saved["checked"] = url

    class Storage:
        def save(self, local_path, destination):
            saved["destination"] = destination

    result = downloader["UrlDownloader"](
        downloader["UrlValidator"](), Checker(), Fetcher(), downloader["FileNameResolver"](), Storage()
    ).download(downloader["DownloadRequest"]("https://host/data/rates?x=1", "Files/ECB_FX/"))

    assert saved == {"checked": "https://host/data/rates?x=1", "destination": "Files/ECB_FX/rates.csv"}
    assert result.destination == "Files/ECB_FX/rates.csv"
    assert not os.path.exists(local_file)


def test_parse_headers_rejects_non_objects(downloader):
    assert downloader["parse_headers"]('{"X-API-Key": 1}') == {"X-API-Key": "1"}
    with pytest.raises(ValueError):
        downloader["parse_headers"]("[1]")


def test_batch_downloader_reports_each_outcome(sources):
    class Runner:
        def run(self, source):
            if "bad" in source.url:
                raise RuntimeError("HTTP 404")
            return {"destination": source.target_folder}

    report = sources["BatchDownloader"](Runner()).download_all(
        sources["parse_sources"]('[{"url": "https://ok", "target_folder": "Files/a"}, {"url": "https://bad", "target_folder": "Files/b"}]')
    )

    assert [outcome.succeeded for outcome in report.outcomes] == [True, False]
    assert report.failures[0].detail == {"error": "HTTP 404"}


def test_default_sources_land_in_locked_scope_folders(sources):
    parsed = sources["parse_sources"](sources["sources_json"])
    assert {source.target_folder for source in parsed} == {"Files/ECB_FX", "Files/NY_GDP"}
    assert len({source.file_name for source in parsed}) == len(parsed)
    assert all("2016" in source.url for source in parsed)


class FakeApi:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, path, params):
        self.calls.append((path, params["page"]))
        return {"results": self.pages.get((path, params["page"]), [])}


def test_paginator_stops_on_short_page(openaq):
    api = FakeApi({("locations", 1): [1, 2], ("locations", 2): [3]})
    assert openaq["Paginator"](api, page_limit=2).fetch_all("locations", {}) == [1, 2, 3]
    assert api.calls == [("locations", 1), ("locations", 2)]


def test_paginator_stops_on_empty_page(openaq):
    api = FakeApi({("locations", 1): [1, 2]})
    assert openaq["Paginator"](api, page_limit=2).fetch_all("locations", {}) == [1, 2]
    assert api.calls == [("locations", 1), ("locations", 2)]


def test_rate_limiter_spaces_calls(openaq):
    clock = SimpleNamespace(now=100.0)
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    limiter = openaq["RateLimiter"](60, clock=lambda: clock.now, sleep=sleep)
    limiter.wait()
    limiter.wait()
    clock.now += 5
    limiter.wait()
    assert sleeps == [1.0]


def test_extractor_filters_locations_and_sensors(openaq):
    locations = [
        {
            "id": 1,
            "datetimeFirst": {"utc": "2020-01-01T00:00:00Z"},
            "datetimeLast": {"utc": "2025-01-01T00:00:00Z"},
            "sensors": [{"id": 10, "parameter": {"name": "pm25"}}, {"id": 11, "parameter": {"name": "temperature"}}],
        },
        {"id": 2, "datetimeFirst": {"utc": "2010-01-01T00:00:00Z"}, "datetimeLast": {"utc": "2015-01-01T00:00:00Z"}, "sensors": []},
        {"id": 3, "datetimeFirst": None, "datetimeLast": None, "sensors": []},
    ]
    api = FakeApi({("locations", 1): locations, ("sensors/10/days", 1): [{"value": 7.5}]})
    extractor = openaq["NycAirQualityExtractor"](
        openaq["Paginator"](api, 1000), "bbox", openaq["DateWindow"]("2024-01-01", "2024-12-31"), frozenset({"pm25"})
    )

    active = extractor.locations()
    sensors = extractor.sensors(active)
    days = extractor.sensor_days(sensors)

    assert [location["id"] for location in active] == [1]
    assert [sensor["id"] for sensor in sensors] == [10]
    assert days == [{"value": 7.5, "sensorId": 10, "locationId": 1}]
    assert ("sensors/10/days", 1) in api.calls


def test_window_defaults_end_to_today(openaq):
    from datetime import date

    window = openaq["resolve_window"]("2016-03-01", "", date(2026, 11, 5))
    assert window == openaq["DateWindow"]("2016-03-01", "2026-11-05")


@pytest.mark.parametrize(("start", "end"), [("2016-02-29", "2020-01-01"), ("2020-01-02", "2020-01-01")])
def test_window_rejects_out_of_scope_ranges(openaq, start, end):
    from datetime import date

    with pytest.raises(ValueError):
        openaq["resolve_window"](start, end, date(2026, 11, 5))


def test_pipeline_months_respect_locked_scope():
    import json

    from notebook_loader import WORKSPACE

    pipeline = json.loads((WORKSPACE / "pl_ingest_taxi.DataPipeline" / "pipeline-content.json").read_text(encoding="utf-8"))
    months = pipeline["properties"]["parameters"]["months"]["defaultValue"]
    assert months[0] == pipeline["properties"]["variables"]["locked_scope_start"]["defaultValue"] == "2016-03"
    assert months == sorted(set(months))


def test_columns_cast_nested_values(openaq):
    day = {
        "sensorId": 10,
        "value": 7,
        "period": {"datetimeFrom": {"local": "2024-03-01T00:00:00-05:00", "utc": "2024-03-01T05:00:00Z"}},
        "flagInfo": {"hasFlags": False},
    }
    row = {column.name: column.extract(day) for column in openaq["SENSOR_DAY_COLUMNS"]}
    assert row["value"] == 7.0 and isinstance(row["value"], float)
    assert str(row["date_local"]) == "2024-03-01"
    assert row["has_flags"] is False
    assert row["min"] is None
