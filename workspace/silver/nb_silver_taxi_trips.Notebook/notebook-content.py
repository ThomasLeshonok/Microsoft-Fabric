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

# # nb_silver_taxi_trips
#
# Builds `silver_taxi_trips` in `lh_silver` from the monthly yellow trip files in `lh_bronze`, one file at a time.
#
# 1. Normalize: every file is read straight from `lh_bronze` and mapped to one schema (snake_case names, fixed
#    types, name lookup ignores case). Of the amounts only `fare_amount` and `total_amount` are kept; the fee
#    components stay in bronze. Codes are tinyint (`vendor_id`, `passenger_count`, `rate_code_id`, `payment_type`),
#    zones smallint (`pickup_location_id`, `dropoff_location_id`); `try_cast` turns a value outside that range into
#    null instead of wrapping it around.
# 2. Drop invalid trips, see `invalid_trip_rules`. A trip with one NYC zone (not 264/265) is kept.
# 3. `abs()` both amounts, then keep one row per trip (`TRIP_KEY`). A refund reversal mirrors its original trip with
#    negative amounts, so after `abs()` both share the key; the original row wins.
# 4. Add pickup date/time attributes (smallint `pickup_year`, tinyint `pickup_month`, `pickup_hour` and
#    `pickup_day_of_week`) and `trip_duration_minutes`, drop `store_and_fwd_flag`.
# 5. Write per `load_mode`. The table is created from `nb_silver_delta_tables` before the first month loads.
#    - `overwrite_months` (default): overwrite the partition (`pickup_year`, `pickup_month`) of each month in `months`,
#      so reruns are idempotent. When the table does not exist or its schema differs from the one this notebook writes,
#      it is recreated empty and every month in bronze is loaded, whatever `months` says.
#    - `append_missing` (`pl_silver_taxi_trips`): append only the bronze months after the latest month in silver;
#      months already in silver are never rewritten. A missing table loads every month, an outdated schema fails.
#    - `rebuild` (`pl_silver_startup`): recreate the table empty and load every month in bronze.
# 6. After all months, `VACUUM` the table, except after `append_missing` (see `vacuum_after_load`): an append leaves
#    nothing to delete. Overwrite only unlists the old parquet files, Delta keeps them for time travel; vacuum deletes
#    those older than `vacuum_retention_hours`. Below 168 hours Delta refuses unless
#    `spark.databricks.delta.retentionDurationCheck.enabled` is false.
#
# `months`: comma separated `yyyy-MM` list for `overwrite_months`, empty means every month found in bronze.
# `max_parallel_months`: months loaded at once. The first month runs alone: the first write into a new table also
# changes its Delta metadata, so a concurrent write fails with `DELTA_METADATA_CHANGED`. The others overwrite
# disjoint partitions, so their Delta commits do not conflict.
# Exits with one report per month: rows read, rows dropped per rule, duplicates removed, rows written.

# PARAMETERS CELL ********************

load_mode = "overwrite_months"
months = ""
bronze_lakehouse = "lh_bronze"
trips_folder = "Files/trips/trips/nyctaxi_yellow"
silver_table = "silver_taxi_trips"
max_parallel_months = 4
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
import re
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TRIP_FILE_PATTERN = re.compile(r"_tripdata_(\d{4})-(\d{2})\.parquet$")
NON_NYC_LOCATION_IDS = (264, 265)
FLEX_FARE_PAYMENT_TYPE = 0
VOIDED_PAYMENT_TYPE = 6
MAX_TRIP_DISTANCE_MILES = 100
PARTITION_COLUMNS = ("pickup_year", "pickup_month")
DROPPED_COLUMNS = ("store_and_fwd_flag",)
OVERWRITE_MONTHS = "overwrite_months"
TRIP_LOAD_MODES = (OVERWRITE_MONTHS, APPEND_MISSING, REBUILD)


@dataclass(frozen=True)
class ColumnSpec:
    source: str
    target: str
    data_type: str


TRIP_COLUMNS = (
    ColumnSpec("VendorID", "vendor_id", "tinyint"),
    ColumnSpec("tpep_pickup_datetime", "pickup_datetime", "timestamp"),
    ColumnSpec("tpep_dropoff_datetime", "dropoff_datetime", "timestamp"),
    ColumnSpec("passenger_count", "passenger_count", "tinyint"),
    ColumnSpec("trip_distance", "trip_distance", "double"),
    ColumnSpec("RatecodeID", "rate_code_id", "tinyint"),
    ColumnSpec("store_and_fwd_flag", "store_and_fwd_flag", "string"),
    ColumnSpec("PULocationID", "pickup_location_id", "smallint"),
    ColumnSpec("DOLocationID", "dropoff_location_id", "smallint"),
    ColumnSpec("payment_type", "payment_type", "tinyint"),
    ColumnSpec("fare_amount", "fare_amount", "double"),
    ColumnSpec("total_amount", "total_amount", "double"),
)
AMOUNT_COLUMNS = ("fare_amount", "total_amount")
TRIP_KEY = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_distance",
    "total_amount",
)


@dataclass(frozen=True)
class TripFile:
    path: str
    year: int
    month: int

    @classmethod
    def from_path(cls, path: str) -> "TripFile":
        match = TRIP_FILE_PATTERN.search(path)
        if not match:
            raise ValueError(f"Not a monthly trip file: {path}")
        return cls(path, int(match.group(1)), int(match.group(2)))

    @property
    def month_label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class MonthReport:
    month: str
    rows_read: int
    rows_dropped_by_rule: dict
    duplicates_removed: int
    rows_written: int


def normalize_columns(raw: DataFrame) -> DataFrame:
    source_names = {name.lower(): name for name in raw.columns}
    return raw.select([_normalized_column(spec, source_names) for spec in TRIP_COLUMNS])


def _normalized_column(spec: ColumnSpec, source_names: dict) -> Column:
    source = source_names.get(spec.source.lower())
    if not source:
        return F.lit(None).cast(spec.data_type).alias(spec.target)
    return F.expr(f"try_cast(`{source}` AS {spec.data_type})").alias(spec.target)


def invalid_trip_rules(trip_file: TripFile) -> dict:
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    pickup_zone, dropoff_zone = F.col("pickup_location_id"), F.col("dropoff_location_id")
    distance, flag = F.col("trip_distance"), F.col("store_and_fwd_flag")
    conditions = {
        "missing_timestamp": pickup.isNull() | dropoff.isNull(),
        "no_nyc_zone": pickup_zone.isin(*NON_NYC_LOCATION_IDS) & dropoff_zone.isin(*NON_NYC_LOCATION_IDS),
        "unrealistic_flex_fare_distance": (F.col("payment_type") == FLEX_FARE_PAYMENT_TYPE) & (distance > MAX_TRIP_DISTANCE_MILES),
        "stationary_trip": (pickup == dropoff) & (pickup_zone == dropoff_zone) & (distance == 0) & (flag.isNull() | (flag == "N")),
        "zero_distance_and_amount": (distance == 0) & (F.col("total_amount") == 0),
        "voided_payment": F.col("payment_type") == VOIDED_PAYMENT_TYPE,
        "pickup_outside_file_month": (F.year(pickup) != trip_file.year) | (F.month(pickup) != trip_file.month),
        "dropoff_outside_file_year": F.year(dropoff) != trip_file.year,
        "dropoff_before_pickup": dropoff < pickup,
    }
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def drop_invalid_trips(trips: DataFrame, rules: dict) -> DataFrame:
    return trips.filter(~is_invalid(rules))


def make_amounts_positive(trips: DataFrame) -> DataFrame:
    is_reversal = F.coalesce(F.least(*AMOUNT_COLUMNS) < 0, F.lit(False))
    return trips.withColumn("is_reversal", is_reversal).withColumns({name: F.abs(name) for name in AMOUNT_COLUMNS})


def drop_duplicate_trips(trips: DataFrame) -> DataFrame:
    originals_first = Window.partitionBy(*TRIP_KEY).orderBy("is_reversal")
    ranked = trips.withColumn("trip_rank", F.row_number().over(originals_first))
    return ranked.filter(F.col("trip_rank") == 1).drop("trip_rank", "is_reversal")


def add_time_attributes(trips: DataFrame) -> DataFrame:
    pickup = F.col("pickup_datetime")
    duration_seconds = F.unix_timestamp("dropoff_datetime") - F.unix_timestamp(pickup)
    return trips.withColumns(
        {
            "pickup_date": F.to_date(pickup),
            "pickup_year": F.year(pickup).cast("smallint"),
            "pickup_month": F.month(pickup).cast("tinyint"),
            "pickup_hour": F.hour(pickup).cast("tinyint"),
            "pickup_day_of_week": (F.weekday(pickup) + 1).cast("tinyint"),
            "is_weekend": F.weekday(pickup) >= 5,
            "trip_duration_minutes": F.round(duration_seconds / 60, 2),
        }
    )


def clean_trips(trips: DataFrame, rules: dict) -> DataFrame:
    valid = drop_invalid_trips(trips, rules)
    deduplicated = drop_duplicate_trips(make_amounts_positive(valid))
    return add_time_attributes(deduplicated).drop(*DROPPED_COLUMNS)


def build_month_report(trip_file: TripFile, rule_hits: dict, rows_written: int) -> MonthReport:
    return MonthReport(
        month=trip_file.month_label,
        rows_read=rule_hits["rows_read"],
        rows_dropped_by_rule=rows_dropped_by_rule(rule_hits),
        duplicates_removed=rule_hits["rows_valid"] - rows_written,
        rows_written=rows_written,
    )


def select_trip_files(trip_files: list, months: str) -> list:
    requested = {month.strip() for month in months.split(",") if month.strip()}
    if not trip_files:
        raise FileNotFoundError("No bronze trip files found")
    if not requested:
        return trip_files
    missing = requested - {trip_file.month_label for trip_file in trip_files}
    if missing:
        raise FileNotFoundError(f"No bronze trip file for months: {sorted(missing)}")
    return [trip_file for trip_file in trip_files if trip_file.month_label in requested]


def list_trip_files(root: str) -> list:
    return sorted(_find_trip_files(root), key=lambda trip_file: trip_file.month_label)


def _find_trip_files(folder: str) -> Iterator[TripFile]:
    for entry in notebookutils.fs.ls(folder):
        if entry.isDir:
            yield from _find_trip_files(entry.path)
        elif TRIP_FILE_PATTERN.search(entry.name):
            yield TripFile.from_path(entry.path)


def select_missing_trip_files(trip_files: list, latest_loaded_month: str | None) -> list:
    return [trip_file for trip_file in trip_files if latest_loaded_month is None or trip_file.month_label > latest_loaded_month]


def latest_loaded_month(session: SparkSession, table: str) -> str | None:
    month_label = F.format_string("%04d-%02d", *PARTITION_COLUMNS)
    return session.table(table).agg(F.max(month_label)).first()[0]


def month_partition(trip_file: TripFile) -> str:
    return f"pickup_year = {trip_file.year} AND pickup_month = {trip_file.month}"


def overwrite_month(trips: DataFrame, table: str, trip_file: TripFile) -> None:
    trips.writeTo(table).overwrite(F.expr(month_partition(trip_file)))


def append_month(trips: DataFrame, table: str, trip_file: TripFile) -> None:
    trips.writeTo(table).append()


MonthWriter = Callable[[DataFrame, str, TripFile], None]


def month_writer(load_mode: str) -> MonthWriter:
    return append_month if load_mode == APPEND_MISSING else overwrite_month


def load_month(session: SparkSession, trip_file: TripFile, table: str, write: MonthWriter) -> MonthReport:
    trips = normalize_columns(session.read.parquet(trip_file.path))
    rules = invalid_trip_rules(trip_file)
    rule_hits = count_rule_hits(trips, rules)
    write(clean_trips(trips, rules), table, trip_file)
    rows_written = session.table(table).where(month_partition(trip_file)).count()
    return build_month_report(trip_file, rule_hits, rows_written)


def load_months(session: SparkSession, trip_files: list, table: str, max_parallel_months: int, write: MonthWriter) -> list:
    if not trip_files:
        return []
    first_report = load_month(session, trip_files[0], table, write)
    with ThreadPoolExecutor(max_workers=max_parallel_months) as pool:
        later_reports = list(pool.map(lambda trip_file: load_month(session, trip_file, table, write), trip_files[1:]))
    return [first_report, *later_reports]


def table_definition(session: SparkSession, trip_file: TripFile, table: str) -> TableDefinition:
    trips = normalize_columns(session.read.parquet(trip_file.path))
    return TableDefinition.from_frame(table, clean_trips(trips, invalid_trip_rules(trip_file)), PARTITION_COLUMNS)


def prepare_table_for_months(session: SparkSession, trip_files: list, months: str, table: str) -> list:
    requested = select_trip_files(trip_files, months)
    definition = table_definition(session, requested[0], table)
    if is_current(session, definition):
        return requested
    replace_table(session, definition)
    return trip_files


def prepare_table_and_select_files(session: SparkSession, trip_files: list, months: str, load_mode: str, table: str) -> list:
    require_load_mode(load_mode, TRIP_LOAD_MODES)
    if load_mode == OVERWRITE_MONTHS:
        return prepare_table_for_months(session, trip_files, months, table)
    if months.strip():
        raise ValueError(f"months applies only to load_mode '{OVERWRITE_MONTHS}', got load_mode '{load_mode}'")
    definition = table_definition(session, select_trip_files(trip_files, "")[0], table)
    if appends_to_current_table(session, definition, load_mode):
        return select_missing_trip_files(trip_files, latest_loaded_month(session, table))
    replace_table(session, definition)
    return trip_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

available_trip_files = list_trip_files(lakehouse_path(bronze_lakehouse, trips_folder))
trip_files = prepare_table_and_select_files(spark, available_trip_files, months, load_mode, silver_table)
reports = load_months(spark, trip_files, silver_table, max_parallel_months, month_writer(load_mode))
month_reports = [asdict(report) for report in reports]
vacuum_after_load(spark, silver_table, vacuum_retention_hours, load_mode)
print(json.dumps(month_reports, indent=2))

runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(month_reports))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
