# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "80bcc451-4b85-4233-acd9-cd92e59035bf",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_silver_air_quality
#
# Builds `silver_air_quality_hourly`, `silver_air_quality_daily` and `silver_air_quality_sensors` in `lh_silver` from
# the OpenAQ tables in `lh_bronze`.
#
# 1. Normalize: keep only columns with information, fixed types, readable names.
#    - Hourly drops `period_label` (always `raw`), `period_interval` (always 1 hour), `datetime_to_utc` and
#      `datetime_to_local` (start + 1 hour). `datetime_from_utc` becomes `measured_at_utc`; `datetime_from_local` becomes
#      `measured_at`, the NYC wall-clock time, same convention as the taxi `pickup_datetime`.
#    - Daily drops `value` (`avg` rounded to 3 digits), `datetime_from_*`/`datetime_to_utc` (same day as `date_local`),
#      `observed_count`/`expected_count` (kept as `percent_complete`), `q25`, `q75`, `sd`.
#    - Sensors: one row per sensor with its location joined in. Drops `sensor_name` (parameter + unit), `parameter_id`
#      (one per `parameter_name`), `timezone`, `country_*` and `is_mobile` (constant for the NYC bbox), `locality`
#      (mostly empty), `owner_name` (personal nicknames). `is_monitor` becomes `is_reference_monitor`: regulatory
#      monitor (AirNow) versus low-cost sensor (AirGradient, Clarity).
# 2. Drop invalid rows, see `invalid_reading_rules` and `invalid_sensor_rules`. Units are checked against
#    `SUPPORTED_UNITS`, so every parameter has exactly one unit in silver. `um003` (particles >= 0.3 um) is a count,
#    not a mass, so it does not compare with `pm1`/`pm25`/`pm10`. Sensors outside every taxi zone are dropped
#    (`outside_taxi_zones`), and so are the readings of sensors missing from `silver_air_quality_sensors`
#    (`unknown_sensor`). Readings of the current hour (hourly) or the current NYC day (daily) are dropped as
#    `unfinished_period`: silver never rewrites a reading, so it only takes finished periods.
# 3. Keep one row per key, latest `ingested_at` wins: `df_openaq_nyc` and `nb_ingest_openaq` both write to bronze.
# 4. Add date attributes (`measured_date`, smallint `measured_year`, tinyint `measured_month` and
#    `measured_day_of_week`, `is_weekend`), plus tinyint `measured_hour` for hourly.
#    Sensors get `taxi_zone_id`: the TLC `LocationID` of the `silver_taxi_zones` boundary that contains their location
#    (ray casting on the GeoJSON); a sensor outside every zone (New Jersey, water) is skipped. Taxi trips only carry
#    zone ids, so the zone is what links pollution to mobility; run `nb_silver_taxi_zones` first. Hourly and daily
#    readings carry their sensor's `taxi_zone_id` instead of the OpenAQ `location_id` (still in the sensors table,
#    reachable through `sensor_id`), so sensors are written first.
# 5. Write, see `load_mode` in `nb_silver_delta_tables`. With `append_missing` (default, `pl_silver_air_quality`):
#    - Sensors: only bronze sensors whose `sensor_id` is not in the silver table yet get a zone and are appended;
#      sensors already there keep their row, a skipped sensor is checked again on every run.
#    - Hourly and daily: per sensor, only readings after its latest `measured_at_utc` (hourly) or `measured_date`
#      (daily) in silver are appended, so each run adds the missing hours or days and never rewrites one. A sensor new
#      to silver gets all its readings.
#    `rebuild` (`pl_silver_startup`) overwrites every selected table from all of bronze; use it after new zone
#    boundaries, rule changes or a schema change (an outdated schema fails `append_missing`), so hourly and daily also
#    drop the readings of removed sensors. Hourly and daily are partitioned by `measured_year`.
#    Tables are created from `nb_silver_delta_tables`: `sensor_id` and `location_id` are int, `taxi_zone_id` smallint,
#    `parameter_name` `VARCHAR(5)`, `parameter_units` and `parameter_display_name` `VARCHAR(15)`. A longer value fails
#    the write.
#    Each batch and its cleaned rows are cached, so bronze is read once for the rule counts, the row count and the
#    write.
# 6. `VACUUM` the tables of this run after a `rebuild` (see `vacuum_after_load`); an append leaves nothing to delete.
#    Overwrite only unlists the old parquet files, Delta keeps them for time travel; vacuum deletes those older than
#    `vacuum_retention_hours`. Below 168 hours Delta refuses unless
#    `spark.databricks.delta.retentionDurationCheck.enabled` is false.
#
# `tables`: comma separated subset of `sensors,hourly,daily`, empty means all. Schedule `hourly` and `daily` apart if
# needed; they read the current `silver_air_quality_sensors` for zones.
#
# Exits with one report per table: rows read, rows dropped per rule, duplicates removed, rows written.
# Days with low `percent_complete` stay; the coverage threshold is a gold decision (EPA uses 75%).

# PARAMETERS CELL ********************

bronze_lakehouse = "lh_bronze"
bronze_measurements_table = "bronze_openaq_sensor_measurements"
bronze_days_table = "bronze_openaq_sensor_days"
bronze_sensors_table = "bronze_openaq_sensors"
bronze_locations_table = "bronze_openaq_locations"
silver_hourly_table = "silver_air_quality_hourly"
silver_daily_table = "silver_air_quality_daily"
silver_sensors_table = "silver_air_quality_sensors"
silver_taxi_zones_table = "silver_taxi_zones"
tables = "sensors,hourly,daily"
load_mode = "append_missing"
vacuum_retention_hours = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_silver_delta_tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import operator
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import reduce
from itertools import pairwise

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

MICROGRAMS_PER_CUBIC_METER = "µg/m³"
PARTS_PER_MILLION = "ppm"
PARTICLES_PER_CUBIC_CENTIMETER = "particles/cm³"
SUPPORTED_UNITS = {
    "pm1": MICROGRAMS_PER_CUBIC_METER,
    "pm25": MICROGRAMS_PER_CUBIC_METER,
    "pm10": MICROGRAMS_PER_CUBIC_METER,
    "um003": PARTICLES_PER_CUBIC_CENTIMETER,
    "no": PARTS_PER_MILLION,
    "no2": PARTS_PER_MILLION,
    "nox": PARTS_PER_MILLION,
    "o3": PARTS_PER_MILLION,
    "co": PARTS_PER_MILLION,
    "so2": PARTS_PER_MILLION,
}
LOCAL_DATETIME_LENGTH = len("2024-01-15T08:00:00")
NYC_TIME_ZONE = "America/New_York"
TABLE_LOADS = ("sensors", "hourly", "daily")
READING_PARTITION_COLUMN = "measured_year"
READING_TEXT_TYPES = {"parameter_name": "VARCHAR(5)", "parameter_units": "VARCHAR(15)"}
SENSOR_TEXT_TYPES = {**READING_TEXT_TYPES, "parameter_display_name": "VARCHAR(15)"}
LOCATION_ZONE_SCHEMA = "location_id int, taxi_zone_id smallint"


def select_tables(raw: str) -> list:
    requested = {name.strip() for name in raw.split(",") if name.strip()}
    unknown = requested - set(TABLE_LOADS)
    if unknown:
        raise ValueError(f"Unknown tables {sorted(unknown)}, expected a subset of {list(TABLE_LOADS)}")
    return [name for name in TABLE_LOADS if not requested or name in requested]


def local_wall_clock(column: str) -> Column:
    return F.to_timestamp(F.substring(column, 1, LOCAL_DATETIME_LENGTH), "yyyy-MM-dd'T'HH:mm:ss")


def _sensor_columns() -> list:
    return [
        F.col("sensor_id").cast("int").alias("sensor_id"),
        F.col("parameter_name").cast("string").alias("parameter_name"),
        F.col("parameter_units").cast("string").alias("parameter_units"),
    ]


def normalize_measurements(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        *_sensor_columns(),
        F.col("value").cast("double").alias("value"),
        F.col("has_flags").cast("boolean").alias("has_flags"),
        F.to_timestamp("datetime_from_utc").alias("measured_at_utc"),
        local_wall_clock("datetime_from_local").alias("measured_at"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def normalize_days(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        *_sensor_columns(),
        F.col("date_local").cast("date").alias("measured_date"),
        F.col("avg").cast("double").alias("avg_value"),
        F.col("min").cast("double").alias("min_value"),
        F.col("median").cast("double").alias("median_value"),
        F.col("max").cast("double").alias("max_value"),
        F.col("percent_complete").cast("double").alias("percent_complete"),
        F.col("has_flags").cast("boolean").alias("has_flags"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def normalize_sensors(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        *_sensor_columns(),
        F.col("location_id").cast("int").alias("location_id"),
        F.col("parameter_display_name").cast("string").alias("parameter_display_name"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def normalize_locations(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("location_id").cast("int").alias("location_id"),
        F.col("location_name").cast("string").alias("location_name"),
        F.col("provider_name").cast("string").alias("provider_name"),
        F.col("is_monitor").cast("boolean").alias("is_reference_monitor"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.to_timestamp("datetime_first_utc").alias("first_measured_at_utc"),
        F.to_timestamp("datetime_last_utc").alias("last_measured_at_utc"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def add_date_attributes(readings: DataFrame) -> DataFrame:
    day = F.col("measured_date")
    return readings.withColumns(
        {
            "measured_year": F.year(day).cast("smallint"),
            "measured_month": F.month(day).cast("tinyint"),
            "measured_day_of_week": (F.weekday(day) + 1).cast("tinyint"),
            "is_weekend": F.weekday(day) >= 5,
        }
    )


def add_hour_attributes(readings: DataFrame) -> DataFrame:
    measured_at = F.col("measured_at")
    with_date = readings.withColumns({"measured_date": F.to_date(measured_at), "measured_hour": F.hour(measured_at).cast("tinyint")})
    return add_date_attributes(with_date)


def _is_supported_unit() -> Column:
    pairs = [(F.col("parameter_name") == name) & (F.col("parameter_units") == unit) for name, unit in SUPPORTED_UNITS.items()]
    return F.coalesce(reduce(operator.or_, pairs), F.lit(False))


def _any_null(columns: tuple) -> Column:
    return reduce(operator.or_, [F.col(name).isNull() for name in columns])


def _never_null(conditions: dict) -> dict:
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def is_unfinished_hour() -> Column:
    return F.col("measured_at_utc") >= F.date_trunc("hour", F.current_timestamp())


def is_unfinished_day() -> Column:
    return F.col("measured_date") >= F.to_date(F.from_utc_timestamp(F.current_timestamp(), NYC_TIME_ZONE))


def invalid_reading_rules(required_columns: tuple, value_column: str, is_unfinished: Column) -> dict:
    value = F.col(value_column)
    return _never_null(
        {
            "missing_key": _any_null(required_columns),
            "missing_value": value.isNull(),
            "negative_value": value < 0,
            "unsupported_parameter_unit": ~_is_supported_unit(),
            "unknown_sensor": F.col("taxi_zone_id").isNull(),
            "unfinished_period": is_unfinished,
        }
    )


def invalid_sensor_rules() -> dict:
    return _never_null(
        {
            "missing_key": _any_null(("sensor_id", "location_id")),
            "missing_location": _any_null(("latitude", "longitude")),
            "unsupported_parameter_unit": ~_is_supported_unit(),
            "outside_taxi_zones": F.col("taxi_zone_id").isNull(),
        }
    )


def join_locations(sensors: DataFrame, locations: DataFrame) -> DataFrame:
    latest_locations = keep_latest_ingestion(locations, ("location_id",)).drop("ingested_at")
    return sensors.join(latest_locations, "location_id", "left")


def new_sensors(sensors: DataFrame, existing: DataFrame) -> DataFrame:
    return sensors.join(existing.select("sensor_id"), "sensor_id", "left_anti")


def clean_sensors(sensors_with_zones: DataFrame, rules: dict) -> DataFrame:
    valid = sensors_with_zones.filter(~is_invalid(rules))
    return keep_latest_ingestion(valid, ("sensor_id",)).drop("ingested_at")


def _is_inside_polygon(longitude: float, latitude: float, polygon: list) -> bool:
    crossings = 0
    for ring in polygon:
        for (x1, y1), (x2, y2) in pairwise(ring):
            if (y1 > latitude) != (y2 > latitude) and longitude < x1 + (latitude - y1) * (x2 - x1) / (y2 - y1):
                crossings += 1
    return crossings % 2 == 1


def locate_taxi_zone(longitude: float, latitude: float, polygons_by_zone: dict) -> int | None:
    containing = (
        zone_id
        for zone_id, polygons in polygons_by_zone.items()
        if any(_is_inside_polygon(longitude, latitude, polygon) for polygon in polygons)
    )
    return next(containing, None)


def join_taxi_zones(session: SparkSession, sensors: DataFrame, location_zones: list) -> DataFrame:
    return sensors.join(session.createDataFrame(location_zones, LOCATION_ZONE_SCHEMA), "location_id", "left")


def add_taxi_zones(session: SparkSession, sensors: DataFrame, taxi_zones: DataFrame) -> DataFrame:
    boundaries = taxi_zones.where(F.col("boundary_geojson").isNotNull()).select("location_id", "boundary_geojson").collect()
    polygons_by_zone = {row.location_id: json.loads(row.boundary_geojson)["coordinates"] for row in boundaries}
    placed = sensors.where(F.col("longitude").isNotNull() & F.col("latitude").isNotNull())
    locations = placed.select("location_id", "longitude", "latitude").distinct().collect()
    location_zones = [(row.location_id, locate_taxi_zone(row.longitude, row.latitude, polygons_by_zone)) for row in locations]
    return join_taxi_zones(session, sensors, location_zones)


def sensor_table_definition(session: SparkSession, table: str, sensors: DataFrame) -> TableDefinition:
    cleaned = clean_sensors(join_taxi_zones(session, sensors, []), invalid_sensor_rules())
    return TableDefinition.from_frame(table, cleaned, text_types=SENSOR_TEXT_TYPES)


def add_reading_taxi_zones(readings: DataFrame, sensor_zones: DataFrame) -> DataFrame:
    with_zone = readings.join(F.broadcast(sensor_zones.select("sensor_id", "taxi_zone_id")), "sensor_id", "left")
    return with_zone.select("sensor_id", "taxi_zone_id", *[name for name in readings.columns if name != "sensor_id"])


def move_ingested_at_last(frame: DataFrame) -> DataFrame:
    return frame.select(*[name for name in frame.columns if name != "ingested_at"], "ingested_at")


@dataclass(frozen=True)
class ReadingSpec:
    normalize: Callable[[DataFrame], DataFrame]
    add_time_attributes: Callable[[DataFrame], DataFrame]
    is_unfinished: Callable[[], Column]
    key: tuple
    required_columns: tuple
    value_column: str

    @property
    def period_column(self) -> str:
        return self.key[-1]

    def rules(self) -> dict:
        return invalid_reading_rules(self.required_columns, self.value_column, self.is_unfinished())

    def prepare(self, bronze: DataFrame, sensor_zones: DataFrame) -> DataFrame:
        return add_reading_taxi_zones(self.normalize(bronze), sensor_zones)

    def clean(self, readings: DataFrame) -> DataFrame:
        valid = readings.filter(~is_invalid(self.rules()))
        latest = self.add_time_attributes(keep_latest_ingestion(valid, self.key))
        return move_ingested_at_last(latest)

    def table_definition(self, table: str, bronze: DataFrame, sensor_zones: DataFrame) -> TableDefinition:
        readings = self.clean(self.prepare(bronze, sensor_zones))
        return TableDefinition.from_frame(table, readings, (READING_PARTITION_COLUMN,), READING_TEXT_TYPES)


HOURLY_READINGS = ReadingSpec(
    normalize=normalize_measurements,
    add_time_attributes=add_hour_attributes,
    is_unfinished=is_unfinished_hour,
    key=("sensor_id", "measured_at_utc"),
    required_columns=("sensor_id", "measured_at_utc", "measured_at"),
    value_column="value",
)
DAILY_READINGS = ReadingSpec(
    normalize=normalize_days,
    add_time_attributes=add_date_attributes,
    is_unfinished=is_unfinished_day,
    key=("sensor_id", "measured_date"),
    required_columns=("sensor_id", "measured_date"),
    value_column="avg_value",
)


def write_table(session: SparkSession, frame: DataFrame, definition: TableDefinition) -> int:
    rows = frame.count()
    overwrite_table(session, frame, definition)
    return rows


@contextmanager
def cached(frame: DataFrame) -> Iterator[DataFrame]:
    frame.persist()
    try:
        yield frame
    finally:
        frame.unpersist()


def read_delta(session: SparkSession, path: str) -> DataFrame:
    return session.read.format("delta").load(path)


def load_readings(
    session: SparkSession, spec: ReadingSpec, bronze_table_path: str, sensor_zones: DataFrame, table: str, load_mode: str
) -> TableReport:
    bronze = read_delta(session, bronze_table_path)
    definition = spec.table_definition(table, bronze, sensor_zones)
    is_append = appends_to_current_table(session, definition, load_mode)
    readings = spec.prepare(bronze, sensor_zones)
    batch = rows_after_latest_loaded(readings, session.table(table), ("sensor_id",), spec.period_column) if is_append else readings
    with cached(batch), cached(spec.clean(batch)) as cleaned:
        rule_hits = count_rule_hits(batch, spec.rules())
        rows_written = append_rows(cleaned, table) if is_append else write_table(session, cleaned, definition)
    return build_table_report(table, rule_hits, rows_written)


def read_sensors(session: SparkSession, sensors_path: str, locations_path: str) -> DataFrame:
    locations = normalize_locations(read_delta(session, locations_path))
    return join_locations(normalize_sensors(read_delta(session, sensors_path)), locations)


def load_sensors(session: SparkSession, sensors: DataFrame, taxi_zones: DataFrame, table: str, load_mode: str) -> TableReport:
    definition = sensor_table_definition(session, table, sensors)
    is_append = appends_to_current_table(session, definition, load_mode)
    batch = new_sensors(sensors, session.table(table)) if is_append else sensors
    sensors_with_zones = add_taxi_zones(session, batch, taxi_zones)
    rules = invalid_sensor_rules()
    with cached(sensors_with_zones), cached(clean_sensors(sensors_with_zones, rules)) as cleaned:
        rule_hits = count_rule_hits(sensors_with_zones, rules)
        rows_written = append_rows(cleaned, table) if is_append else write_table(session, cleaned, definition)
    return build_table_report(table, rule_hits, rows_written)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

silver_tables = {"sensors": silver_sensors_table, "hourly": silver_hourly_table, "daily": silver_daily_table}
reading_loads = {
    "hourly": (HOURLY_READINGS, bronze_measurements_table),
    "daily": (DAILY_READINGS, bronze_days_table),
}
selected_tables = select_tables(tables)
reports = []
if "sensors" in selected_tables:
    sensors = read_sensors(
        spark,
        bronze_table_path(bronze_lakehouse, bronze_sensors_table),
        bronze_table_path(bronze_lakehouse, bronze_locations_table),
    )
    reports.append(load_sensors(spark, sensors, spark.table(silver_taxi_zones_table), silver_sensors_table, load_mode))
for name in selected_tables:
    if name in reading_loads:
        spec, bronze_table = reading_loads[name]
        bronze_path = bronze_table_path(bronze_lakehouse, bronze_table)
        reports.append(load_readings(spark, spec, bronze_path, spark.table(silver_sensors_table), silver_tables[name], load_mode))
for name in selected_tables:
    vacuum_after_load(spark, silver_tables[name], vacuum_retention_hours, load_mode)
table_reports = [asdict(report) for report in reports]
print(json.dumps(table_reports, indent=2))

runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(table_reports))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
