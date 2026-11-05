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
# NYC locations (bbox) -> their sensors -> `/v3/sensors/{id}/days` per sensor, paginated on `page` with `limit=1000`.
# Locked scope starts 2016-03-01; empty `date_to` means today (UTC).
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

openaq_key_vault_url = ""
openaq_secret_name = "openaq-api-key"
openaq_api_key = ""
nyc_bbox = "-74.2591,40.4774,-73.7002,40.9162"
date_from = "2016-03-01"
date_to = ""
pollutant_parameters = "pm25,pm10,no2,o3,so2,co"
page_limit = 1000
requests_per_minute = 50
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
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Iterable, Protocol

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


class RateLimiter:
    def __init__(
        self,
        requests_per_minute: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._interval_seconds = 60.0 / requests_per_minute
        self._clock = clock
        self._sleep = sleep
        self._next_slot = 0.0

    def wait(self) -> None:
        now = self._clock()
        if now < self._next_slot:
            self._sleep(self._next_slot - now)
        self._next_slot = max(now, self._next_slot) + self._interval_seconds


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
    def __init__(self, paginator: Paginator, bbox: str, window: DateWindow, parameter_names: frozenset):
        self._paginator = paginator
        self._bbox = bbox
        self._date_from = window.start
        self._date_to = window.end
        self._parameter_names = parameter_names

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
                f"sensors/{sensor['id']}/days", {"date_from": self._date_from, "date_to": self._date_to}
            )
        ]

    def _is_active(self, location: dict) -> bool:
        first_seen = pick(location, "datetimeFirst.utc")
        last_seen = pick(location, "datetimeLast.utc")
        return bool(first_seen and last_seen) and first_seen[:10] <= self._date_to and last_seen[:10] >= self._date_from

    def _is_wanted(self, sensor: dict) -> bool:
        return not self._parameter_names or pick(sensor, "parameter.name") in self._parameter_names


class JsonLinesLanding:
    def __init__(self, storage: FileStorage, root_folder: str, run_date: str):
        self._storage = storage
        self._root_folder = root_folder.rstrip("/")
        self._run_date = run_date

    def land(self, entity: str, records: list) -> str:
        destination = f"{self._root_folder}/{entity}/run_date={self._run_date}/{entity}.jsonl"
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

    def write(self, entity: str, records: list, columns: list) -> str:
        table_name = f"{self._table_prefix}{entity}"
        schema = StructType([StructField(column.name, column.spark_type, True) for column in columns])
        rows = [tuple(column.extract(record) for column in columns) for record in records]
        frame = self._spark.createDataFrame(rows, schema).withColumn("ingested_at", F.current_timestamp())
        frame.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)
        return table_name


class OpenAQIngestion:
    def __init__(self, extractor: NycAirQualityExtractor, landing: JsonLinesLanding, writer: DeltaTableWriter):
        self._extractor = extractor
        self._landing = landing
        self._writer = writer

    def run(self) -> dict:
        locations = self._extractor.locations()
        sensors = self._extractor.sensors(locations)
        sensor_days = self._extractor.sensor_days(sensors)
        return {
            "locations": self._persist("locations", locations, LOCATION_COLUMNS),
            "sensors": self._persist("sensors", sensors, SENSOR_COLUMNS),
            "sensor_days": self._persist("sensor_days", sensor_days, SENSOR_DAY_COLUMNS),
        }

    def _persist(self, entity: str, records: list, columns: list) -> dict:
        return {
            "raw_file": self._landing.land(entity, records),
            "table": self._writer.write(entity, records, columns),
            "rows": len(records),
        }


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
    RateLimiter(requests_per_minute),
)
run_date = datetime.now(timezone.utc).date()
openaq_ingestion = OpenAQIngestion(
    extractor=NycAirQualityExtractor(
        Paginator(openaq_api, page_limit),
        nyc_bbox,
        resolve_window(date_from, date_to, run_date),
        parse_parameter_names(pollutant_parameters),
    ),
    landing=JsonLinesLanding(OneLakeStorage(), raw_root_folder, run_date.isoformat()),
    writer=DeltaTableWriter(spark, table_prefix),
)
ingestion_summary = openaq_ingestion.run()
print(json.dumps(ingestion_summary, indent=2))
notebookutils.notebook.exit(json.dumps(ingestion_summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
