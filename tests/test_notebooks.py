import os
import zipfile
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
def openaq():
    return load(("nb_http_download", ALL_CELLS), ("nb_ingest_openaq", DEFINITION_CELLS))


@pytest.fixture(scope="module")
def compaction():
    return load(("nb_compact_bronze_tables", DEFINITION_CELLS))


@pytest.fixture(scope="module")
def zip_members():
    return load(("nb_http_download", ALL_CELLS), ("nb_download_zip_members", DEFINITION_CELLS))


def write_taxi_zones_zip(path):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("taxi_zones/", "")
        archive.writestr("taxi_zones/taxi_zones.shp", b"shp-bytes")
        archive.writestr("taxi_zones/taxi_zones.dbf", b"dbf-bytes")
    return path


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


def test_zip_extractor_keeps_only_requested_members(zip_members, tmp_path):
    archive = write_taxi_zones_zip(tmp_path / "taxi_zones.zip")
    output = tmp_path / "out"
    output.mkdir()

    extracted = zip_members["ZipMemberExtractor"]().extract(str(archive), ("taxi_zones.shp",), str(output))

    assert [(member.name, member.size_bytes) for member in extracted] == [("taxi_zones.shp", 9)]
    assert [path.name for path in output.iterdir()] == ["taxi_zones.shp"]


def test_zip_extractor_rejects_missing_member(zip_members, tmp_path):
    archive = write_taxi_zones_zip(tmp_path / "taxi_zones.zip")
    with pytest.raises(zip_members["MissingArchiveMemberError"], match="taxi_zones.prj"):
        zip_members["ZipMemberExtractor"]().extract(str(archive), ("taxi_zones.prj",), str(tmp_path))


def test_zip_members_downloader_saves_members_and_removes_archive(zip_members, tmp_path):
    archive = write_taxi_zones_zip(tmp_path / "download.tmp")
    saved = {}

    class Fetcher:
        def fetch(self, url, headers):
            return zip_members["DownloadedFile"](str(archive), "application/zip", os.path.getsize(archive))

    class Checker:
        def ensure_exists(self, url, headers):
            pass

    class Storage:
        def save(self, local_path, destination):
            with open(local_path, "rb") as handle:
                saved[destination] = handle.read()

    result = zip_members["ZipMembersDownloader"](
        zip_members["UrlValidator"](), Checker(), Fetcher(), zip_members["ZipMemberExtractor"](), Storage()
    ).download(zip_members["ZipMembersRequest"]("https://host/misc/taxi_zones.zip", "Files/nyctaxi_yellow/taxi_zones/", ("taxi_zones.shp",)))

    assert saved == {"Files/nyctaxi_yellow/taxi_zones/taxi_zones.shp": b"shp-bytes"}
    assert [member.destination for member in result] == ["Files/nyctaxi_yellow/taxi_zones/taxi_zones.shp"]
    assert not os.path.exists(archive)


def test_zip_members_defaults_target_taxi_zones_shapefile(zip_members):
    assert zip_members["url"] == "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    assert zip_members["parse_member_names"](zip_members["members_json"]) == (
        "taxi_zones.shp",
        "taxi_zones.shx",
        "taxi_zones.dbf",
        "taxi_zones.prj",
    )


@pytest.mark.parametrize("raw", ["[]", '"taxi_zones.shp"', ""])
def test_parse_member_names_rejects_empty_or_non_lists(zip_members, raw):
    with pytest.raises(ValueError):
        zip_members["parse_member_names"](raw)


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
            "sensors": [
                {"id": 10, "parameter": {"name": "pm25"}},
                {"id": 11, "parameter": {"name": "temperature"}},
                {"id": 12, "parameter": {"name": "um003"}},
                {"id": 13, "parameter": {"name": "relativehumidity"}},
            ],
        },
        {"id": 2, "datetimeFirst": {"utc": "2010-01-01T00:00:00Z"}, "datetimeLast": {"utc": "2015-01-01T00:00:00Z"}, "sensors": []},
        {"id": 3, "datetimeFirst": None, "datetimeLast": None, "sensors": []},
    ]
    api = FakeApi({("locations", 1): locations, ("sensors/10/days", 1): [{"value": 7.5}]})
    extractor = openaq["NycAirQualityExtractor"](
        openaq["Paginator"](api, 1000),
        "bbox",
        openaq["DateWindow"]("2024-01-01", "2024-12-31"),
        frozenset({"temperature", "relativehumidity"}),
    )

    active = extractor.locations()
    sensors = extractor.sensors(active)
    days = extractor.sensor_days(sensors)

    assert [location["id"] for location in active] == [1]
    assert [sensor["id"] for sensor in sensors] == [10, 12]
    assert days == [{"value": 7.5, "sensorId": 10, "locationId": 1}]
    assert ("sensors/10/days", 1) in api.calls


def test_single_day_window_requests_non_empty_day_range(openaq):
    requested = []

    class RecordingApi:
        def get(self, path, params):
            requested.append((path, params["date_from"], params["date_to"]))
            return {"results": []}

    extractor = openaq["NycAirQualityExtractor"](
        openaq["Paginator"](RecordingApi(), 1000),
        "bbox",
        openaq["DateWindow"]("2026-11-14", "2026-11-14"),
        frozenset(),
    )

    extractor.sensor_days([{"id": 671, "locationId": 1}])

    assert requested == [("sensors/671/days", "2026-11-14", "2026-11-15")]


def test_rate_limiter_enforces_hourly_budget(openaq):
    clock = SimpleNamespace(now=0.0)
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    limiter = openaq["RateLimiter"](60, 2, clock=lambda: clock.now, sleep=sleep)
    limiter.wait()
    limiter.wait()
    limiter.wait()
    assert sleeps == [1.0, 3599.0]


def utc(text):
    from datetime import UTC, datetime

    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def test_month_windows_follow_calendar_months(openaq):
    windows = openaq["month_windows"](utc("2016-12-15"), utc("2017-02-10"))
    assert [(openaq["to_api_datetime"](w.start), openaq["to_api_datetime"](w.end)) for w in windows] == [
        ("2016-12-15T00:00:00Z", "2017-01-01T00:00:00Z"),
        ("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"),
        ("2017-02-01T00:00:00Z", "2017-02-10T00:00:00Z"),
    ]
    assert openaq["month_windows"](utc("2018-01-01"), utc("2018-01-01")) == []


def measurement(start_utc, value=1.0):
    return {"value": value, "period": {"label": "raw", "datetimeFrom": {"utc": start_utc}}, "flagInfo": {"hasFlags": False}}


class WindowApi:
    def __init__(self, by_window, timeouts=frozenset()):
        self.by_window = by_window
        self.timeouts = timeouts
        self.calls = []

    def get(self, path, params):
        window = (params["datetime_from"], params["datetime_to"])
        self.calls.append((path, window))
        if window in self.timeouts:
            raise self.timeout_error(path)
        return {"results": self.by_window.get(window, []) if params["page"] == 1 else []}


ACTIVE_2016_2017 = {"id": 1, "datetimeFirst": {"utc": "2016-12-01T00:00:00Z"}, "datetimeLast": {"utc": "2017-03-01T00:00:00Z"}}
SENSOR_10 = {"id": 10, "locationId": 1}


def measurement_extractor(openaq, api, start="2016-03-01", end="2017-12-31"):
    api.timeout_error = openaq["OpenAQTimeoutError"]
    return openaq["NycAirQualityExtractor"](openaq["Paginator"](api, 1000), "bbox", openaq["DateWindow"](start, end), frozenset())


def test_measurements_clip_to_location_activity_and_drop_boundary_repeats(openaq):
    api = WindowApi(
        {
            ("2016-12-01T00:00:00Z", "2017-01-01T00:00:00Z"): [measurement("2016-12-31T23:00:00Z"), measurement("2017-01-01T00:00:00Z")],
            ("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"): [measurement("2017-01-01T00:00:00Z"), measurement("2017-01-01T01:00:00Z")],
        }
    )
    chunks = list(measurement_extractor(openaq, api).sensor_measurements(SENSOR_10, ACTIVE_2016_2017))

    assert [window for _, window in api.calls] == [
        ("2016-12-01T00:00:00Z", "2017-01-01T00:00:00Z"),
        ("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"),
        ("2017-02-01T00:00:00Z", "2017-03-01T00:00:00Z"),
        ("2017-03-01T00:00:00Z", "2017-03-02T00:00:00Z"),
    ]
    keys = [[record["period"]["datetimeFrom"]["utc"] for record in records] for _, records in chunks]
    assert keys == [["2016-12-31T23:00:00Z", "2017-01-01T00:00:00Z"], ["2017-01-01T01:00:00Z"], [], []]
    assert chunks[1][1][0]["sensorId"] == 10 and chunks[1][1][0]["locationId"] == 1


def test_measurements_resume_after_watermark(openaq):
    api = WindowApi({("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"): [measurement("2017-01-01T00:00:00Z"), measurement("2017-01-01T01:00:00Z")]})
    chunks = list(measurement_extractor(openaq, api).sensor_measurements(SENSOR_10, ACTIVE_2016_2017, "2017-01-01T00:00:00Z"))

    assert api.calls[0] == ("sensors/10/measurements", ("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"))
    assert [[record["period"]["datetimeFrom"]["utc"] for record in records] for _, records in chunks] == [["2017-01-01T01:00:00Z"], [], []]


def test_measurements_split_window_on_timeout(openaq):
    location = {"id": 1, "datetimeFirst": {"utc": "2017-01-01T00:00:00Z"}, "datetimeLast": {"utc": "2017-01-02T00:00:00Z"}}
    api = WindowApi(
        {
            ("2017-01-01T00:00:00Z", "2017-01-02T00:00:00Z"): [measurement("2017-01-01T05:00:00Z")],
            ("2017-01-02T00:00:00Z", "2017-01-03T00:00:00Z"): [measurement("2017-01-02T05:00:00Z")],
        },
        timeouts={("2017-01-01T00:00:00Z", "2017-01-03T00:00:00Z")},
    )
    chunks = list(measurement_extractor(openaq, api, end="2017-01-02").sensor_measurements(SENSOR_10, location))

    assert [len(records) for _, records in chunks] == [2]
    assert [window for _, window in api.calls][1:] == [
        ("2017-01-01T00:00:00Z", "2017-01-02T00:00:00Z"),
        ("2017-01-02T00:00:00Z", "2017-01-03T00:00:00Z"),
    ]


def test_measurements_timeout_on_smallest_window_raises(openaq):
    location = {"id": 1, "datetimeFirst": {"utc": "2017-01-01T00:00:00Z"}, "datetimeLast": {"utc": "2017-01-01T00:00:00Z"}}
    api = WindowApi({}, timeouts={("2017-01-01T00:00:00Z", "2017-01-02T00:00:00Z")})
    with pytest.raises(openaq["OpenAQTimeoutError"]):
        list(measurement_extractor(openaq, api, end="2017-01-01").sensor_measurements(SENSOR_10, location))


def test_ingestion_lands_each_window_and_appends_once_per_sensor(openaq):
    location = {**ACTIVE_2016_2017, "sensors": [{"id": 10, "parameter": {"name": "o3"}}]}

    class Api(WindowApi):
        def get(self, path, params):
            if path == "locations":
                return {"results": [location] if params["page"] == 1 else []}
            if path.endswith("/days"):
                return {"results": [day("2017-02-28"), day("2018-01-01")] if params["page"] == 1 else []}
            return super().get(path, params)

    def day(local_date):
        return {"value": 1.0, "period": {"datetimeFrom": {"local": f"{local_date}T00:00:00-05:00"}}}

    api = Api(
        {
            ("2017-01-01T00:00:00Z", "2017-02-01T00:00:00Z"): [measurement("2017-01-01T01:00:00Z")],
            ("2017-02-01T00:00:00Z", "2017-03-01T00:00:00Z"): [measurement("2017-02-01T01:00:00Z")],
        }
    )
    landed, written, resets = [], [], []

    class Landing:
        def folder(self, entity):
            return f"Files/OpenAQ/{entity}/run_date=2026-11-10"

        def land(self, entity, records, part=None):
            landed.append((entity, part, len(records)))
            return f"{self.folder(entity)}/{part or entity}.jsonl"

    class Writer:
        def table_name(self, entity):
            return f"bronze_openaq_{entity}"

        def write(self, entity, records, columns, mode="overwrite", replace_where=None):
            written.append((entity, mode, len(records), replace_where))
            return self.table_name(entity)

    class Watermarks:
        def latest_by_sensor(self):
            return {10: "2016-12-31T23:00:00Z"}

        def reset(self):
            resets.append(True)

    summary = openaq["OpenAQIngestion"](measurement_extractor(openaq, api), Landing(), Writer(), Watermarks()).run()

    assert ("sensor_measurements", "sensor_id=10/20170101T000000Z", 1) in landed
    assert ("sensor_measurements", "sensor_id=10/20170201T000000Z", 1) in landed
    assert [write for write in written if write[0] == "sensor_measurements"] == [("sensor_measurements", "append", 2, None)]
    assert ("sensor_days", "overwrite", 1, "date_local >= '2016-03-01' AND date_local <= '2017-12-31'") in written
    assert ("locations", "overwrite", 1, None) in written
    assert summary["sensor_measurements"] == {
        "raw_folder": "Files/OpenAQ/sensor_measurements/run_date=2026-11-10",
        "table": "bronze_openaq_sensor_measurements",
        "rows": 2,
        "parts": 2,
    }
    assert resets == []


class RecordingDeltaSession:
    def __init__(self, existing_tables):
        self.existing_tables = set(existing_tables)
        self.calls = []
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.existing_tables)

    def createDataFrame(self, rows, schema):
        session = self

        class Builder:
            def __getattr__(self, method):
                def record(*arguments):
                    session.calls.append((method, *arguments))
                    return self

                return record

        class Frame:
            write = Builder()

            def withColumn(self, name, column):
                return self

            def writeTo(self, table_name):
                session.calls.append(("writeTo", table_name))
                return Builder()

        return Frame()


def test_openaq_writer_creates_tables_dataflow_gen2_can_append_to(openaq, monkeypatch):
    monkeypatch.setitem(openaq, "F", SimpleNamespace(current_timestamp=lambda: "now"))
    session = RecordingDeltaSession(existing_tables=[])

    openaq["DeltaTableWriter"](session, "bronze_openaq_").write("sensor_days", [], openaq["SENSOR_DAY_COLUMNS"], replace_where="date_local >= '2016-03-01'")

    assert session.calls == [
        ("writeTo", "bronze_openaq_sensor_days"),
        ("using", "delta"),
        ("tableProperty", "delta.enableDeletionVectors", "false"),
        ("tableProperty", "delta.minReaderVersion", "1"),
        ("tableProperty", "delta.minWriterVersion", "2"),
        ("create",),
    ]


def test_openaq_writer_keeps_existing_table_protocol(openaq, monkeypatch):
    monkeypatch.setitem(openaq, "F", SimpleNamespace(current_timestamp=lambda: "now"))
    session = RecordingDeltaSession(existing_tables=["bronze_openaq_sensor_days"])

    openaq["DeltaTableWriter"](session, "bronze_openaq_").write("sensor_days", [], openaq["SENSOR_DAY_COLUMNS"], replace_where="date_local >= '2016-03-01'")

    assert session.calls == [
        ("mode", "overwrite"),
        ("format", "delta"),
        ("option", "replaceWhere", "date_local >= '2016-03-01'"),
        ("saveAsTable", "bronze_openaq_sensor_days"),
    ]


class FakeSparkSql:
    def __init__(self):
        self.conf = SimpleNamespace(set=lambda key, value: self.settings.update({key: value}))
        self.settings = {}
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(collect=lambda: [{"metrics": {"numFilesRemoved": 40, "numFilesAdded": 2}}])


def test_compaction_packs_small_files_up_to_target_size(compaction):
    session = FakeSparkSql()
    compaction["set_target_file_size"](session, 1048576)

    assert session.settings == {
        "spark.databricks.delta.optimize.minFileSize": "1048576",
        "spark.databricks.delta.optimize.maxFileSize": "1048576",
    }
    with pytest.raises(ValueError):
        compaction["set_target_file_size"](session, 0)


def test_compaction_optimizes_then_vacuums_each_table(compaction):
    session = FakeSparkSql()
    report = compaction["compact_table"](session, "bronze_openaq_sensor_days", 168)

    assert session.statements == ["OPTIMIZE bronze_openaq_sensor_days", "VACUUM bronze_openaq_sensor_days RETAIN 168 HOURS"]
    assert report == {"files_removed": 40, "files_added": 2}


def test_compaction_defaults_target_openaq_fact_tables(compaction):
    assert compaction["parse_table_names"](compaction["table_names"]) == ["bronze_openaq_sensor_days", "bronze_openaq_sensor_measurements"]
    assert compaction["target_file_size_bytes"] == 1024 * 1024
    with pytest.raises(ValueError):
        compaction["parse_table_names"](" , ")


def test_window_defaults_end_to_today(openaq):
    from datetime import date

    window = openaq["resolve_window"]("2016-03-01", "", date(2026, 11, 5))
    assert window == openaq["DateWindow"]("2016-03-01", "2026-11-05")


@pytest.mark.parametrize(("start", "end"), [("2016-02-29", "2020-01-01"), ("2020-01-02", "2020-01-01")])
def test_window_rejects_out_of_scope_ranges(openaq, start, end):
    from datetime import date

    with pytest.raises(ValueError):
        openaq["resolve_window"](start, end, date(2026, 11, 5))


def test_taxi_pipeline_backfills_from_locked_scope_then_loads_latest_published_month():
    import json

    from notebook_loader import item_folder

    properties = json.loads((item_folder("pl_ingest_taxi.DataPipeline") / "pipeline-content.json").read_text(encoding="utf-8"))["properties"]
    activities = {activity["name"]: activity for activity in properties["activities"]}
    assert "months" not in properties["parameters"]
    assert properties["parameters"]["publish_lag_months"] == {"type": "int", "defaultValue": 2}
    assert properties["variables"]["locked_scope_start"]["defaultValue"] == "2016-03"
    assert properties["concurrency"] == 1

    latest = activities["Set latest published month"]["typeProperties"]
    assert latest["variableName"] == "latest_month"
    assert latest["value"]["value"] == "@addToTime(utcNow(), mul(-1, pipeline().parameters.publish_lag_months), 'Month', 'yyyy-MM')"

    check = activities["Check trips folder"]["typeProperties"]
    trip_copy = activities["ForEach month"]["typeProperties"]["activities"][0]
    trip_folder = trip_copy["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]["folderPath"]["value"]
    assert check["fieldList"] == ["exists"]
    taxi_folder = "@concat('trips/nyctaxi_', pipeline().parameters.taxi_type"
    assert check["datasetSettings"]["typeProperties"]["location"]["folderPath"]["value"] == f"{taxi_folder})"
    assert trip_folder.startswith(f"{taxi_folder}, '/'")

    first = activities["Backfill or new month"]["typeProperties"]
    assert first["value"]["value"] == "@if(activity('Check trips folder').output.exists, variables('latest_month'), variables('locked_scope_start'))"

    list_months = activities["List months"]
    (append,) = list_months["typeProperties"]["activities"]
    assert list_months["typeProperties"]["isSequential"] is True
    assert append["typeProperties"]["variableName"] == "months"
    assert activities["ForEach month"]["typeProperties"]["items"]["value"] == "@variables('months')"
    assert activities["ForEach month"]["dependsOn"][0]["activity"] == "List months"


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


def test_measurement_columns_keep_period_bounds(openaq):
    raw = {
        "sensorId": 671,
        "locationId": 384,
        "value": 0.02,
        "parameter": {"name": "o3", "units": "ppm"},
        "period": {
            "label": "raw",
            "interval": "01:00:00",
            "datetimeFrom": {"utc": "2017-01-03T03:00:00Z", "local": "2017-01-02T22:00:00-05:00"},
            "datetimeTo": {"utc": "2017-01-03T04:00:00Z", "local": "2017-01-02T23:00:00-05:00"},
        },
        "coordinates": None,
        "flagInfo": {"hasFlags": False},
    }
    row = {column.name: column.extract(raw) for column in openaq["SENSOR_MEASUREMENT_COLUMNS"]}
    assert row["datetime_to_utc"] == "2017-01-03T04:00:00Z"
    assert row["period_interval"] == "01:00:00"
    assert row["value"] == 0.02
    assert "latitude" not in row and "longitude" not in row


def test_items_reference_lakehouse_by_logical_id():
    import json
    import re

    from notebook_loader import WORKSPACE, item_folder

    lakehouse_id = json.loads((item_folder("lh_bronze.Lakehouse") / ".platform").read_text(encoding="utf-8"))["config"]["logicalId"]
    zero_workspace = "00000000-0000-0000-0000-000000000000"
    pipeline = json.loads((item_folder("pl_ingest_taxi.DataPipeline") / "pipeline-content.json").read_text(encoding="utf-8"))
    sinks = re.findall(r'"workspaceId": "([^"]+)",\s*"artifactId": "([^"]+)"', json.dumps(pipeline, indent=2))
    assert sinks and set(sinks) == {(zero_workspace, lakehouse_id)}

    for notebook in WORKSPACE.rglob("*.Notebook/notebook-content.py"):
        content = notebook.read_text(encoding="utf-8")
        [default_lakehouse_name] = re.findall(r'"default_lakehouse_name": "([^"]+)"', content)
        default_lakehouse = item_folder(f"{default_lakehouse_name}.Lakehouse")
        default_lakehouse_id = json.loads((default_lakehouse / ".platform").read_text(encoding="utf-8"))["config"]["logicalId"]
        assert f'"default_lakehouse": "{default_lakehouse_id}"' in content, notebook
        assert f'"default_lakehouse_workspace_id": "{zero_workspace}"' in content, notebook

    for mashup in WORKSPACE.rglob("*.Dataflow/mashup.pq"):
        content = mashup.read_text(encoding="utf-8")
        lakehouse_navigations = content.count("Lakehouse.Contents(")
        assert lakehouse_navigations >= content.count("_DataDestination = let") > 0, mashup
        assert content.count('workspaceId = "."') == content.count('lakehouseName = "lh_bronze"') == lakehouse_navigations, mashup

    leftovers = [path for path in WORKSPACE.rglob("*") if path.is_file() and path.name != "parameter.yml" and re.search(r"11111111-|22222222-|33333333-", path.read_text(encoding="utf-8"))]
    assert leftovers == []


def test_openaq_pipeline_reads_key_vault_into_variable_for_dataflow():
    import json
    import re

    from notebook_loader import item_folder

    def logical_id(item):
        return json.loads((item_folder(item) / ".platform").read_text(encoding="utf-8"))["config"]["logicalId"]

    pipeline = json.loads((item_folder("pl_ingest_openaq.DataPipeline") / "pipeline-content.json").read_text(encoding="utf-8"))
    branch = next(activity for activity in pipeline["properties"]["activities"] if activity["type"] == "IfCondition")
    activities = {activity["name"]: activity for activity in branch["typeProperties"]["ifFalseActivities"]}
    get_key, set_key, run_dataflow = activities["Get OpenAQ key"], activities["Set OpenAQ key"], activities["Run df_openaq_nyc"]
    assert (get_key["type"], set_key["type"], run_dataflow["type"]) == ("WebActivity", "SetVariable", "RefreshDataflow")
    assert get_key["typeProperties"]["relativeUrl"] == "secrets/openaq-api-key?api-version=7.4"
    assert re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", get_key["externalReferences"]["connection"])
    assert get_key["policy"]["secureOutput"] and set_key["policy"]["secureOutput"] and run_dataflow["policy"]["secureInput"]
    assert set_key["typeProperties"]["value"]["value"] == f"@activity('{get_key['name']}').output.value"

    variable = set_key["typeProperties"]["variableName"]
    assert variable in pipeline["properties"]["variables"]
    assert run_dataflow["typeProperties"]["dataflowId"] == logical_id("df_openaq_nyc.Dataflow")
    parameters = run_dataflow["typeProperties"]["parameters"]
    assert parameters["OpenAQApiKey"]["value"]["value"] == f"@variables('{variable}')"
    assert parameters["LookbackHours"]["value"]["value"] == "@string(pipeline().parameters.lookback_hours)"
    assert pipeline["properties"]["parameters"]["lookback_hours"] == {"type": "int", "defaultValue": 3}
    assert "DateFrom" not in parameters and "DateTo" not in parameters

    mashup = (item_folder("df_openaq_nyc.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    assert 'shared OpenAQApiKey = "" meta [IsParameterQuery = true' in mashup
    assert "vault.azure.net" not in mashup
    assert "StatusOf = (response as binary) as nullable number => Record.FieldOrDefault(" in mashup
    measurements = mashup[mashup.index("shared bronze_openaq_sensor_measurements = let"):mashup.index("shared bronze_openaq_locations_DataDestination")]
    assert "coordinates" not in measurements and "latitude" not in measurements
    assert re.findall(r"shared (\w+) = .*IsParameterQueryRequired = true", mashup) == ["OpenAQApiKey"]
    assert json.loads((item_folder("df_openaq_nyc.Dataflow") / "queryMetadata.json").read_text(encoding="utf-8"))["parametric"] is True


def test_openaq_pipeline_runs_dataflow_daily_and_notebook_only_for_backfill():
    import json

    from notebook_loader import item_folder

    def logical_id(item):
        return json.loads((item_folder(item) / ".platform").read_text(encoding="utf-8"))["config"]["logicalId"]

    properties = json.loads((item_folder("pl_ingest_openaq.DataPipeline") / "pipeline-content.json").read_text(encoding="utf-8"))["properties"]
    parameters = properties["parameters"]
    assert parameters["run_backfill"] == {"type": "bool", "defaultValue": False}
    assert parameters["backfill_date_from"]["defaultValue"] == "2016-03-01"
    assert parameters["day"]["defaultValue"] == ""
    assert properties["concurrency"] == 1

    set_day, branch, compaction = properties["activities"]
    assert set_day["typeProperties"]["variableName"] == "load_day"
    assert "addDays(utcNow(), -1)" in set_day["typeProperties"]["value"]["value"]
    assert branch["typeProperties"]["expression"]["value"] == "@pipeline().parameters.run_backfill"

    (notebook,) = branch["typeProperties"]["ifTrueActivities"]
    assert notebook["type"] == "TridentNotebook"
    assert notebook["typeProperties"]["notebookId"] == logical_id("nb_ingest_openaq.Notebook")
    notebook_parameters = notebook["typeProperties"]["parameters"]
    assert notebook_parameters["date_from"]["value"]["value"] == "@pipeline().parameters.backfill_date_from"
    assert notebook_parameters["date_to"]["value"]["value"] == "@variables('load_day')"

    daily = branch["typeProperties"]["ifFalseActivities"]
    assert [activity["type"] for activity in daily] == ["WebActivity", "SetVariable", "RefreshDataflow"]
    assert daily[-1]["policy"]["retry"] >= 1 and daily[-1]["policy"]["retryIntervalInSeconds"] >= 600

    assert parameters["compact_tables"] == {"type": "bool", "defaultValue": False}
    assert compaction["dependsOn"] == [{"activity": branch["name"], "dependencyConditions": ["Succeeded"]}]
    assert compaction["typeProperties"]["expression"]["value"] == "@or(pipeline().parameters.run_backfill, pipeline().parameters.compact_tables)"
    (compact,) = compaction["typeProperties"]["ifTrueActivities"]
    assert compact["typeProperties"]["notebookId"] == logical_id("nb_compact_bronze_tables.Notebook")


def test_openaq_notebook_and_dataflow_write_the_same_columns():
    import re

    from notebook_loader import item_folder

    notebook = (item_folder("nb_ingest_openaq.Notebook") / "notebook-content.py").read_text(encoding="utf-8")
    mashup = (item_folder("df_openaq_nyc.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    for entity, columns in (
        ("locations", "LOCATION_COLUMNS"),
        ("sensors", "SENSOR_COLUMNS"),
        ("sensor_days", "SENSOR_DAY_COLUMNS"),
        ("sensor_measurements", "SENSOR_MEASUREMENT_COLUMNS"),
    ):
        block = notebook[notebook.index(f"{columns} = ["):]
        written = set(re.findall(r'Column\("(\w+)"', block[:block.index("]\n")])) | {"ingested_at"}
        annotation = next(line for line in mashup.splitlines() if f'QueryName = "bronze_openaq_{entity}_DataDestination"' in line)
        assert written == set(re.findall(r'DestinationColumnName = "(\w+)"', annotation)), entity


def test_openaq_dataflow_appends_only_missing_rows():
    import json
    import re

    from notebook_loader import item_folder

    mashup = (item_folder("df_openaq_nyc.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    destinations = {}
    for match in re.finditer(r'^\[DataDestinations = ([^\n]*)\]\nshared (\w+) = let\n(.*?)\nin\n', mashup, re.S | re.M):
        annotation, query, body = match.groups()
        destinations[query] = "Append" if 'UpdateMethod = [Kind = "Append"]' in annotation else "Replace"
        if destinations[query] == "Append":
            mapped = set(re.findall(r'SourceColumnName = "(\w+)"', annotation))
            typed = set(re.findall(r'\{"(\w+)", (?:Int64\.Type|type nullable \w+)\}', body))
            added = set(re.findall(r'Table\.AddColumn\(\w+, "(\w+)", each [^\n]*, type ', body))
            assert mapped == typed | added, (query, mapped ^ (typed | added))
    assert destinations == {
        "bronze_openaq_locations": "Append",
        "bronze_openaq_sensors": "Append",
        "bronze_openaq_sensor_days": "Append",
        "bronze_openaq_sensor_measurements": "Append",
    }
    for query, key in (("bronze_openaq_locations", '{"location_id"}'), ("bronze_openaq_sensors", '{"sensor_id"}')):
        body = mashup[mashup.index(f"shared {query} = let"):mashup.index("\nin\n", mashup.index(f"shared {query} = let"))]
        assert f'NewRows = OpenAQWithoutExistingKeys(Typed, OpenAQBronzeTable("{query}"), {key})' in body, query
        assert 'Table.AddColumn(NewRows, "ingested_at"' in body, query
    plan = mashup[mashup.index("shared OpenAQFetchPlan"):mashup.index("shared OpenAQOnlyNewRows")]
    assert "openaq_sensors_raw" in plan and "bronze_openaq_sensors" not in plan, "the plan needs every sensor, not only new ones"
    assert "OpenAQRunWindow" not in mashup[mashup.index("shared openaq_locations_raw"):mashup.index("shared bronze_openaq_locations = let")]
    for query in ("bronze_openaq_sensor_days", "bronze_openaq_sensor_measurements"):
        body = mashup[mashup.index(f"shared {query} = let"):mashup.index("\nin\n", mashup.index(f"shared {query} = let"))]
        assert f'TableName = "{query}"' in body, query
        assert "OpenAQFetchPlan(TableName," in body, query
        assert "NewRows = OpenAQOnlyNewRows(TableName, Typed, Plan)" in body, query
        assert 'Table.AddColumn(NewRows, "ingested_at"' in body, query
    assert 'Key = {"sensor_id", "datetime_from_utc"}' in mashup and "JoinKind.LeftAnti" in mashup
    assert 'each List.Max([datetime_to_utc])' in mashup
    assert "Table.Buffer(" not in mashup, "buffering forces every API call during ApplyChanges schema evaluation"
    names = set(re.findall(r"^shared (\w+) =", mashup, re.M))
    metadata = json.loads((item_folder("df_openaq_nyc.Dataflow") / "queryMetadata.json").read_text(encoding="utf-8"))
    assert names == set(metadata["queriesMetadata"])


@pytest.mark.parametrize(
    ("dataflow", "query", "key"),
    [
        ("df_ecb_exchange_rates.Dataflow", "bronze_ecb_exchange_rates", '{"key", "time_period"}'),
        ("df_worldbank_indicators.Dataflow", "bronze_worldbank_indicators", '{"indicator_id", "country_iso3", "period"}'),
    ],
)
def test_reference_dataflows_append_only_missing_keys(dataflow, query, key):
    import json
    import re

    from notebook_loader import item_folder

    mashup = (item_folder(dataflow) / "mashup.pq").read_text(encoding="utf-8")
    annotation, body = re.search(rf'^\[DataDestinations = ([^\n]*)\]\nshared {query} = let\n(.*?)\nin\n', mashup, re.S | re.M).groups()
    assert 'Settings = [Kind = "Manual"' in annotation and 'UpdateMethod = [Kind = "Append"]' in annotation
    assert "DynamicSchema = false" in annotation
    assert f"Key = {key}," in body
    assert "Existing = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = true])" in body
    assert f'{{[Id = "{query}", ItemKind = "Table"]}}?[Data]?' in body
    assert "JoinKind.LeftAnti" in body and 'Table.AddColumn(NewRows, "ingested_at"' in body
    listed = re.search(r"Columns = \{([^}]*)\}", body)
    produced = set(re.findall(r'"(\w+)"', listed.group(1))) if listed else set(re.findall(r'\{"(\w+)", (?:Int64\.Type|type nullable \w+)\}', body))
    assert set(re.findall(r'SourceColumnName = "(\w+)"', annotation)) == produced | {"ingested_at"}
    assert json.loads((item_folder(dataflow) / "queryMetadata.json").read_text(encoding="utf-8"))["fastCombine"] is True


def test_worldbank_dataflow_waits_for_published_values():
    from notebook_loader import item_folder

    mashup = (item_folder("df_worldbank_indicators.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    assert "Published = Table.SelectRows(Typed, each [value] <> null)" in mashup
    assert "Table.NestedJoin(Published, Key," in mashup


def test_openaq_dataflow_retries_server_errors_without_cache():
    from notebook_loader import item_folder

    mashup = (item_folder("df_openaq_nyc.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    assert "RetryableStatuses = {429, 500, 502, 503, 504}" in mashup
    assert "ManualStatusHandling = RetryableStatuses" in mashup
    assert "IsRetry = isRetry" in mashup and "Request(page, attempt > 1)" in mashup
    assert "@GetResults(page, attempt + 1," in mashup
    assert '"OpenAQ.ServerError"' in mashup


def test_openaq_dataflow_requests_measurements_in_month_windows():
    import json

    from notebook_loader import item_folder

    mashup = (item_folder("df_openaq_nyc.Dataflow") / "mashup.pq").read_text(encoding="utf-8")
    assert "each OpenAQMonthWindows(" in mashup
    assert "Date.StartOfMonth(Date.AddMonths(" in mashup
    assert "YearWindows" not in mashup
    metadata = json.loads((item_folder("df_openaq_nyc.Dataflow") / "queryMetadata.json").read_text(encoding="utf-8"))
    assert "OpenAQMonthWindows" in metadata["queriesMetadata"]


def test_dataflows_bind_web_source_and_lakehouse_connections():
    import json
    import re

    from notebook_loader import WORKSPACE

    for metadata_path in WORKSPACE.rglob("*.Dataflow/queryMetadata.json"):
        connections = json.loads(metadata_path.read_text(encoding="utf-8"))["connections"]
        assert sorted(connection["kind"] for connection in connections) == ["Lakehouse", "Web"], metadata_path
        for connection in connections:
            assert set(json.loads(connection["connectionId"])) == {"ClusterId", "DatasourceId"}, metadata_path

        mashup = (metadata_path.parent / "mashup.pq").read_text(encoding="utf-8")
        web_host = re.search(r'Web\.Contents\(\s*"([^"]+)"', mashup).group(1)
        web_path = next(connection["path"] for connection in connections if connection["kind"] == "Web")
        assert web_path.rstrip("/") == web_host, metadata_path


def test_dataflow_columns_accept_nulls():
    import re

    from notebook_loader import WORKSPACE

    for mashup in WORKSPACE.rglob("*.Dataflow/mashup.pq"):
        content = mashup.read_text(encoding="utf-8")
        assert re.findall(r'", type (?!nullable )\w+\}', content) == [], mashup


def test_item_descriptions_fit_fabric_limit():
    import json

    from notebook_loader import WORKSPACE

    fabric_description_limit = 256
    descriptions = {
        platform.parent.name: json.loads(platform.read_text(encoding="utf-8"))["metadata"].get("description", "")
        for platform in WORKSPACE.rglob(".platform")
    }
    assert descriptions
    assert {item: len(text) for item, text in descriptions.items() if len(text) > fabric_description_limit} == {}


def test_item_logical_ids_are_unique():
    import json
    from collections import Counter

    from notebook_loader import WORKSPACE

    logical_ids = Counter(
        json.loads(platform.read_text(encoding="utf-8"))["config"]["logicalId"] for platform in WORKSPACE.rglob(".platform")
    )
    assert {logical_id: count for logical_id, count in logical_ids.items() if count > 1} == {}
