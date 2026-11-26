# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8e27bc1-d18a-4eb0-b5cb-a0d3f492a089",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_ingest_openaq
#
# Notebook fallback for `df_openaq_nyc`. Rationale: `docs/openaq_ingestion.md`.
#
# NYC locations (bbox) -> their sensors -> per sensor `/v3/sensors/{id}/days` and raw `/v3/sensors/{id}/measurements`,
# paginated on `page` with `limit=1000`. Measurements are requested in calendar-month UTC windows (split further on HTTP 408;
# OpenAQ times out on deep pages of a year window) and appended incrementally: each sensor resumes after the latest
# `datetime_from_utc` already in the table, whoever wrote it (this notebook or `df_openaq_nyc`). One append per sensor,
# so the table gets one commit per sensor instead of one per sensor-month; `nb_compact_bronze_tables` packs the rest.
# `bronze_openaq_sensor_days` is replaced only inside `date_from`..`date_to`, so days outside the window stay.
# Locked scope starts 2016-03-01; empty `date_to` means today (UTC). `pl_ingest_openaq` passes yesterday.
# Raw results land as JSON Lines in `Files/OpenAQ/<entity>/run_date=<date>/`, typed rows in `bronze_openaq_<entity>` tables.
# HTTP session, URL validation and OneLake storage come from `nb_http_download`.

# CELL ********************

%run nb_http_download

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

openaq_key_vault_url = "https://fabric-project-secrets.vault.azure.net/"
openaq_secret_name = "openaq-api-key"
openaq_api_key = ""
nyc_bbox = "-74.2591,40.4774,-73.7002,40.9162"
date_from = "2016-03-01"
date_to = ""
excluded_parameters = "temperature,relativehumidity"
page_limit = 1000
requests_per_minute = 50
requests_per_hour = 1900
measurements_full_reload = False
raw_root_folder = "Files/OpenAQ"
table_prefix = "bronze_openaq_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable, Iterator, Optional, Protocol

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
LOCKED_SCOPE_START = date(2016, 3, 1)


@dataclass(frozen=True)
class DateWindow:
    start: str
    end: str


def resolve_window(raw_start: str, raw_end: str, today: date) -> DateWindow:
    start = date.fromisoformat(raw_start)
    end = date.fromisoformat(raw_end) if raw_end else today
    if start < LOCKED_SCOPE_START:
        raise ValueError(f"date_from {start} is before the locked scope start {LOCKED_SCOPE_START}")
    if start > end:
        raise ValueError(f"date_from {start} is after date_to {end}")
    return DateWindow(start.isoformat(), end.isoformat())


@dataclass(frozen=True)
class UtcWindow:
    start: datetime
    end: datetime

    def split(self) -> list:
        middle = self.start + (self.end - self.start) / 2
        return [UtcWindow(self.start, middle), UtcWindow(middle, self.end)]


def parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_api_datetime(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_windows(start: datetime, end: datetime) -> list:
    windows = []
    while start < end:
        boundary = datetime(start.year + start.month // 12, start.month % 12 + 1, 1, tzinfo=timezone.utc)
        windows.append(UtcWindow(start, min(boundary, end)))
        start = boundary
    return windows


def pick(record: dict, path: str):
    value = record
    for key in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


PYTHON_CASTS = {
    StringType: str,
    LongType: int,
    DoubleType: float,
    BooleanType: bool,
    DateType: lambda value: date.fromisoformat(str(value)[:10]),
}


@dataclass(frozen=True)
class Column:
    name: str
    path: str
    spark_type: DataType

    def extract(self, record: dict):
        value = pick(record, self.path)
        return None if value is None else PYTHON_CASTS[type(self.spark_type)](value)


LOCATION_COLUMNS = [
    Column("location_id", "id", LongType()),
    Column("location_name", "name", StringType()),
    Column("locality", "locality", StringType()),
    Column("timezone", "timezone", StringType()),
    Column("country_code", "country.code", StringType()),
    Column("country_name", "country.name", StringType()),
    Column("provider_name", "provider.name", StringType()),
    Column("owner_name", "owner.name", StringType()),
    Column("is_mobile", "isMobile", BooleanType()),
    Column("is_monitor", "isMonitor", BooleanType()),
    Column("latitude", "coordinates.latitude", DoubleType()),
    Column("longitude", "coordinates.longitude", DoubleType()),
    Column("datetime_first_utc", "datetimeFirst.utc", StringType()),
    Column("datetime_last_utc", "datetimeLast.utc", StringType()),
]

SENSOR_COLUMNS = [
    Column("sensor_id", "id", LongType()),
    Column("sensor_name", "name", StringType()),
    Column("location_id", "locationId", LongType()),
    Column("parameter_id", "parameter.id", LongType()),
    Column("parameter_name", "parameter.name", StringType()),
    Column("parameter_units", "parameter.units", StringType()),
    Column("parameter_display_name", "parameter.displayName", StringType()),
]

SENSOR_DAY_COLUMNS = [
    Column("sensor_id", "sensorId", LongType()),
    Column("location_id", "locationId", LongType()),
    Column("date_local", "period.datetimeFrom.local", DateType()),
    Column("value", "value", DoubleType()),
    Column("parameter_name", "parameter.name", StringType()),
    Column("parameter_units", "parameter.units", StringType()),
    Column("datetime_from_utc", "period.datetimeFrom.utc", StringType()),
    Column("datetime_from_local", "period.datetimeFrom.local", StringType()),
    Column("datetime_to_utc", "period.datetimeTo.utc", StringType()),
    Column("min", "summary.min", DoubleType()),
    Column("q25", "summary.q25", DoubleType()),
    Column("median", "summary.median", DoubleType()),
    Column("q75", "summary.q75", DoubleType()),
    Column("max", "summary.max", DoubleType()),
    Column("avg", "summary.avg", DoubleType()),
    Column("sd", "summary.sd", DoubleType()),
    Column("observed_count", "coverage.observedCount", LongType()),
    Column("expected_count", "coverage.expectedCount", LongType()),
    Column("percent_complete", "coverage.percentComplete", DoubleType()),
    Column("has_flags", "flagInfo.hasFlags", BooleanType()),
]

SENSOR_MEASUREMENT_COLUMNS = [
    Column("sensor_id", "sensorId", LongType()),
    Column("location_id", "locationId", LongType()),
    Column("value", "value", DoubleType()),
    Column("parameter_name", "parameter.name", StringType()),
    Column("parameter_units", "parameter.units", StringType()),
    Column("period_label", "period.label", StringType()),
    Column("period_interval", "period.interval", StringType()),
    Column("datetime_from_utc", "period.datetimeFrom.utc", StringType()),
    Column("datetime_from_local", "period.datetimeFrom.local", StringType()),
    Column("datetime_to_utc", "period.datetimeTo.utc", StringType()),
    Column("datetime_to_local", "period.datetimeTo.local", StringType()),
    Column("has_flags", "flagInfo.hasFlags", BooleanType()),
]

DATAFLOW_COMPATIBLE_TABLE_PROPERTIES = {
    "delta.enableDeletionVectors": "false",
    "delta.minReaderVersion": "1",
    "delta.minWriterVersion": "2",
}


class RateLimiter:
    def __init__(
        self,
        requests_per_minute: int,
        requests_per_hour: Optional[int] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._interval_seconds = 60.0 / requests_per_minute
        self._requests_per_hour = requests_per_hour
        self._clock = clock
        self._sleep = sleep
        self._next_slot = 0.0
        self._last_hour = deque()

    def wait(self) -> None:
        now = self._clock()
        wake_at = max(self._next_slot, self._hourly_slot(now))
        if now < wake_at:
            self._sleep(wake_at - now)
            now = wake_at
        self._next_slot = now + self._interval_seconds
        if self._requests_per_hour:
            self._last_hour.append(now)

    def _hourly_slot(self, now: float) -> float:
        if not self._requests_per_hour:
            return now
        while self._last_hour and self._last_hour[0] <= now - 3600:
            self._last_hour.popleft()
        return self._last_hour[0] + 3600 if len(self._last_hour) >= self._requests_per_hour else now


class OpenAQTimeoutError(DownloadError):
    pass


class JsonApi(Protocol):
    def get(self, path: str, params: dict) -> dict: ...


class OpenAQClient:
    def __init__(
        self,
        session: requests.Session,
        api_key: str,
        rate_limiter: RateLimiter,
        timeout_seconds: float = 60,
        base_url: str = OPENAQ_BASE_URL,
    ):
        self._session = session
        self._headers = {"X-API-Key": api_key, "Accept": "application/json"}
        self._rate_limiter = rate_limiter
        self._timeout_seconds = timeout_seconds
        self._base_url = UrlValidator().validate(base_url).rstrip("/")

    def get(self, path: str, params: dict) -> dict:
        self._rate_limiter.wait()
        response = self._session.get(
            f"{self._base_url}/{path}", params=params, headers=self._headers, timeout=self._timeout_seconds
        )
        if response.status_code == 408:
            raise OpenAQTimeoutError(f"{response.url} answered HTTP 408")
        if response.status_code >= 400:
            raise DownloadError(f"{response.url} answered HTTP {response.status_code}: {response.text[:300]}")
        return response.json()


class Paginator:
    def __init__(self, api: JsonApi, page_limit: int = 1000):
        self._api = api
        self._page_limit = page_limit

    def fetch_all(self, path: str, params: dict) -> list:
        records, page = [], 1
        while True:
            results = self._api.get(path, {**params, "limit": self._page_limit, "page": page}).get("results") or []
            records.extend(results)
            if len(results) < self._page_limit:
                return records
            page += 1


class NycAirQualityExtractor:
    def __init__(
        self,
        paginator: Paginator,
        bbox: str,
        window: DateWindow,
        excluded_parameter_names: frozenset,
        smallest_measurement_window: timedelta = timedelta(days=1),
    ):
        self._paginator = paginator
        self._bbox = bbox
        self.window = window
        self._date_from = window.start
        self._date_to = window.end
        self._days_end_exclusive = (date.fromisoformat(window.end) + timedelta(days=1)).isoformat()
        self._utc_start = datetime.fromisoformat(window.start).replace(tzinfo=timezone.utc)
        self._utc_end = datetime.fromisoformat(window.end).replace(tzinfo=timezone.utc) + timedelta(days=1)
        self._excluded_parameter_names = excluded_parameter_names
        self._smallest_measurement_window = smallest_measurement_window

    def locations(self) -> list:
        return [location for location in self._paginator.fetch_all("locations", {"bbox": self._bbox}) if self._is_active(location)]

    def sensors(self, locations: Iterable[dict]) -> list:
        return [
            {**sensor, "locationId": location["id"]}
            for location in locations
            for sensor in location.get("sensors") or []
            if self._is_wanted(sensor)
        ]

    def sensor_days(self, sensors: Iterable[dict]) -> list:
        return [
            {**day, "sensorId": sensor["id"], "locationId": sensor["locationId"]}
            for sensor in sensors
            for day in self._paginator.fetch_all(
                f"sensors/{sensor['id']}/days", {"date_from": self._date_from, "date_to": self._days_end_exclusive}
            )
        ]

    def sensor_measurements(self, sensor: dict, location: dict, after_utc: Optional[str] = None) -> Iterator[tuple]:
        previous_keys = set()
        for window in self._measurement_windows(location, after_utc):
            current_keys, records = set(), []
            for record in self._fetch_measurements(sensor["id"], window):
                key = pick(record, "period.datetimeFrom.utc")
                if key is not None and (key in previous_keys or key in current_keys or (after_utc is not None and key <= after_utc)):
                    continue
                current_keys.add(key)
                records.append({**record, "sensorId": sensor["id"], "locationId": sensor["locationId"]})
            previous_keys = current_keys
            yield window, records

    def _measurement_windows(self, location: dict, after_utc: Optional[str]) -> list:
        starts = [self._utc_start, parse_utc(pick(location, "datetimeFirst.utc"))]
        if after_utc:
            starts.append(parse_utc(after_utc))
        end = min(self._utc_end, parse_utc(pick(location, "datetimeLast.utc")) + timedelta(days=1))
        return month_windows(max(starts), end)

    def _fetch_measurements(self, sensor_id: int, window: UtcWindow) -> list:
        try:
            return self._paginator.fetch_all(
                f"sensors/{sensor_id}/measurements",
                {"datetime_from": to_api_datetime(window.start), "datetime_to": to_api_datetime(window.end)},
            )
        except OpenAQTimeoutError:
            if window.end - window.start <= self._smallest_measurement_window:
                raise
            return [record for half in window.split() for record in self._fetch_measurements(sensor_id, half)]

    def _is_active(self, location: dict) -> bool:
        first_seen = pick(location, "datetimeFirst.utc")
        last_seen = pick(location, "datetimeLast.utc")
        return bool(first_seen and last_seen) and first_seen[:10] <= self._date_to and last_seen[:10] >= self._date_from

    def _is_wanted(self, sensor: dict) -> bool:
        return pick(sensor, "parameter.name") not in self._excluded_parameter_names


class JsonLinesLanding:
    def __init__(self, storage: FileStorage, root_folder: str, run_date: str):
        self._storage = storage
        self._root_folder = root_folder.rstrip("/")
        self._run_date = run_date

    def folder(self, entity: str) -> str:
        return f"{self._root_folder}/{entity}/run_date={self._run_date}"

    def land(self, entity: str, records: list, part: Optional[str] = None) -> str:
        destination = f"{self.folder(entity)}/{part or entity}.jsonl"
        local_path = self._write_local(records)
        try:
            self._storage.save(local_path, destination)
        finally:
            os.remove(local_path)
        return destination

    @staticmethod
    def _write_local(records: list) -> str:
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as handle:
            handle.writelines(json.dumps(record) + "\n" for record in records)
            return handle.name


class DeltaTableWriter:
    def __init__(self, spark_session, table_prefix: str):
        self._spark = spark_session
        self._table_prefix = table_prefix

    def table_name(self, entity: str) -> str:
        return f"{self._table_prefix}{entity}"

    def write(self, entity: str, records: list, columns: list, mode: str = "overwrite", replace_where: Optional[str] = None) -> str:
        table_name = self.table_name(entity)
        schema = StructType([StructField(column.name, column.spark_type, True) for column in columns])
        rows = [tuple(column.extract(record) for column in columns) for record in records]
        frame = self._spark.createDataFrame(rows, schema).withColumn("ingested_at", F.current_timestamp())
        if not self._spark.catalog.tableExists(table_name):
            self._create(frame, table_name)
            return table_name
        writer = frame.write.mode(mode).format("delta")
        if replace_where:
            writer = writer.option("replaceWhere", replace_where)
        elif mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(table_name)
        return table_name

    @staticmethod
    def _create(frame, table_name: str) -> None:
        creator = frame.writeTo(table_name).using("delta")
        for key, value in DATAFLOW_COMPATIBLE_TABLE_PROPERTIES.items():
            creator = creator.tableProperty(key, value)
        creator.create()


class MeasurementWatermarks:
    def __init__(self, spark_session, table_name: str):
        self._spark = spark_session
        self._table_name = table_name

    def latest_by_sensor(self) -> dict:
        if not self._spark.catalog.tableExists(self._table_name):
            return {}
        latest = self._spark.table(self._table_name).groupBy("sensor_id").agg(F.max("datetime_from_utc").alias("latest"))
        return {row["sensor_id"]: row["latest"] for row in latest.collect()}

    def reset(self) -> None:
        self._spark.sql(f"DROP TABLE IF EXISTS {self._table_name}")


MEASUREMENTS = "sensor_measurements"


class OpenAQIngestion:
    def __init__(
        self,
        extractor: NycAirQualityExtractor,
        landing: JsonLinesLanding,
        writer: DeltaTableWriter,
        watermarks: MeasurementWatermarks,
        full_reload: bool = False,
    ):
        self._extractor = extractor
        self._landing = landing
        self._writer = writer
        self._watermarks = watermarks
        self._full_reload = full_reload

    def run(self) -> dict:
        locations = self._extractor.locations()
        sensors = self._extractor.sensors(locations)
        window = self._extractor.window
        sensor_days = [
            day for day in self._extractor.sensor_days(sensors)
            if window.start <= str(pick(day, "period.datetimeFrom.local") or "")[:10] <= window.end
        ]
        days_in_window = f"date_local >= '{window.start}' AND date_local <= '{window.end}'"
        return {
            "locations": self._persist("locations", locations, LOCATION_COLUMNS),
            "sensors": self._persist("sensors", sensors, SENSOR_COLUMNS),
            "sensor_days": self._persist("sensor_days", sensor_days, SENSOR_DAY_COLUMNS, days_in_window),
            MEASUREMENTS: self._append_measurements(locations, sensors),
        }

    def _persist(self, entity: str, records: list, columns: list, replace_where: Optional[str] = None) -> dict:
        return {
            "raw_file": self._landing.land(entity, records),
            "table": self._writer.write(entity, records, columns, replace_where=replace_where),
            "rows": len(records),
        }

    def _append_measurements(self, locations: list, sensors: list) -> dict:
        if self._full_reload:
            self._watermarks.reset()
        latest = self._watermarks.latest_by_sensor()
        locations_by_id = {location["id"]: location for location in locations}
        rows = parts = 0
        for sensor in sensors:
            landed = self._land_measurement_windows(sensor, locations_by_id[sensor["locationId"]], latest.get(sensor["id"]))
            sensor_records = [record for records in landed for record in records]
            if sensor_records:
                self._writer.write(MEASUREMENTS, sensor_records, SENSOR_MEASUREMENT_COLUMNS, mode="append")
            rows += len(sensor_records)
            parts += len(landed)
        return {
            "raw_folder": self._landing.folder(MEASUREMENTS),
            "table": self._writer.table_name(MEASUREMENTS),
            "rows": rows,
            "parts": parts,
        }

    def _land_measurement_windows(self, sensor: dict, location: dict, after_utc: Optional[str]) -> list:
        landed = []
        for window, records in self._extractor.sensor_measurements(sensor, location, after_utc):
            if records:
                self._landing.land(MEASUREMENTS, records, f"sensor_id={sensor['id']}/{window.start:%Y%m%dT%H%M%SZ}")
                landed.append(records)
        return landed


def resolve_api_key(key_vault_url: str, secret_name: str, plain_key: str) -> str:
    if key_vault_url:
        return notebookutils.credentials.getSecret(key_vault_url, secret_name)
    if plain_key:
        return plain_key
    raise ValueError("Set openaq_key_vault_url and openaq_secret_name, or openaq_api_key")


def parse_parameter_names(raw: str) -> frozenset:
    return frozenset(name.strip() for name in raw.split(",") if name.strip())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

openaq_api = OpenAQClient(
    build_session(),
    resolve_api_key(openaq_key_vault_url, openaq_secret_name, openaq_api_key),
    RateLimiter(requests_per_minute, requests_per_hour),
)
run_date = datetime.now(timezone.utc).date()
openaq_writer = DeltaTableWriter(spark, table_prefix)
openaq_ingestion = OpenAQIngestion(
    extractor=NycAirQualityExtractor(
        Paginator(openaq_api, page_limit),
        nyc_bbox,
        resolve_window(date_from, date_to, run_date),
        parse_parameter_names(excluded_parameters),
    ),
    landing=JsonLinesLanding(OneLakeStorage(), raw_root_folder, run_date.isoformat()),
    writer=openaq_writer,
    watermarks=MeasurementWatermarks(spark, openaq_writer.table_name(MEASUREMENTS)),
    full_reload=measurements_full_reload,
)
ingestion_summary = openaq_ingestion.run()
print(json.dumps(ingestion_summary, indent=2))
runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(ingestion_summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
