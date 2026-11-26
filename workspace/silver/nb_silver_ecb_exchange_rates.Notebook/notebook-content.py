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

# # nb_silver_ecb_exchange_rates
#
# Builds `silver_ecb_exchange_rates` in `lh_silver` from `bronze_ecb_exchange_rates` in `lh_bronze`: one row per
# currency pair and calendar day (for example `EUR`, `USD`, 2026-11-16, 1.1601 US$ per euro).
#
# 1. Normalize: keep only columns with information, fixed types, readable names.
#    - `currency_denom` becomes `base_currency`, `currency` becomes `quote_currency`, `obs_value` becomes
#      `exchange_rate` (units of quote currency per 1 unit of base currency).
#    - Drops `key`, `exr_type`, `exr_suffix`, `time_format`, `unit` (repeat the series key or `currency`), the
#      constant SDMX attributes (`obs_status`, `obs_conf`, `collection`, `decimals`, `unit_mult`, `title`, ...) and the
#      empty ones (`obs_com`, `breaks`, `compilation`, ...).
# 2. Drop invalid rows, see `invalid_rate_rules`. Only daily (`freq = D`) series fit one row per day.
# 3. Keep one row per `RATE_KEY`, latest `ingested_at` wins.
# 4. Fill calendar gaps: the ECB publishes on TARGET business days only, but taxis run every day. Weekends and holidays
#    get the last published rate, with `is_published = false`, so gold can convert any day's revenue.
# 5. Add date attributes (`rate_year` smallint, `rate_month` and `rate_day_of_week` tinyint, `is_weekend`).
# 6. Write per `load_mode` (see `nb_silver_delta_tables`). `rebuild` (default, `pl_silver_startup`) overwrites the
#    table from all of bronze, so reruns are idempotent. `append_missing` (`pl_silver_ecb_exchange_rates`) appends, per
#    currency pair, only the days after its latest day in silver; the gap up to the first new published day is filled
#    from the last rate already in silver, days already in silver are never rewritten. The table is created from
#    `nb_silver_delta_tables`, so `base_currency` and `quote_currency` are `CHAR(3)` (ISO 4217 codes).
# 7. `VACUUM` the table after a `rebuild` (see `vacuum_after_load`); an append leaves nothing to delete. Overwrite only
#    unlists the old parquet files, Delta keeps them for time travel; vacuum deletes those older than
#    `vacuum_retention_hours`. Below 168 hours Delta refuses unless
#    `spark.databricks.delta.retentionDurationCheck.enabled` is false.
#
# Exits with a report: rows read, rows dropped per rule, duplicates removed, days carried forward, rows written.

# PARAMETERS CELL ********************

bronze_lakehouse = "lh_bronze"
bronze_table = "bronze_ecb_exchange_rates"
silver_table = "silver_ecb_exchange_rates"
load_mode = "rebuild"
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
from dataclasses import asdict, dataclass
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DAILY_FREQUENCY = "D"
CURRENCY_PAIR = ("base_currency", "quote_currency")
RATE_KEY = (*CURRENCY_PAIR, "rate_date")
TEXT_TYPES = {"base_currency": "CHAR(3)", "quote_currency": "CHAR(3)"}


def normalize_rates(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.upper(F.trim("currency_denom")).alias("base_currency"),
        F.upper(F.trim("currency")).alias("quote_currency"),
        F.col("time_period").cast("date").alias("rate_date"),
        F.col("obs_value").cast("double").alias("exchange_rate"),
        F.upper(F.trim("freq")).alias("frequency"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def invalid_rate_rules() -> dict:
    conditions = {
        "missing_key": reduce(operator.or_, [F.col(column).isNull() for column in RATE_KEY]),
        "non_daily_frequency": F.col("frequency") != DAILY_FREQUENCY,
        "missing_rate": F.col("exchange_rate").isNull(),
        "non_positive_rate": F.col("exchange_rate") <= 0,
    }
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def latest_valid_rates(rates: DataFrame, rules: dict) -> DataFrame:
    valid = rates.filter(~is_invalid(rules))
    return keep_latest_ingestion(valid, RATE_KEY).drop("ingested_at", "frequency")


def fill_calendar_gaps(published: DataFrame) -> DataFrame:
    calendar = published.groupBy(*CURRENCY_PAIR).agg(
        F.explode(F.sequence(F.min("rate_date"), F.max("rate_date"))).alias("rate_date")
    )
    days_so_far = Window.partitionBy(*CURRENCY_PAIR).orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
    daily = calendar.join(published.withColumn("is_published", F.lit(True)), list(RATE_KEY), "left")
    return daily.select(
        *RATE_KEY,
        F.last("exchange_rate", ignorenulls=True).over(days_so_far).alias("exchange_rate"),
        F.coalesce("is_published", F.lit(False)).alias("is_published"),
    )


def add_date_attributes(rates: DataFrame) -> DataFrame:
    day = F.col("rate_date")
    return rates.withColumns(
        {
            "rate_year": F.year(day).cast("smallint"),
            "rate_month": F.month(day).cast("tinyint"),
            "rate_day_of_week": (F.weekday(day) + 1).cast("tinyint"),
            "is_weekend": F.weekday(day) >= 5,
        }
    )


def clean_rates(rates: DataFrame, rules: dict) -> DataFrame:
    return add_date_attributes(fill_calendar_gaps(latest_valid_rates(rates, rules)))


def latest_silver_rates(silver: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy(*CURRENCY_PAIR).orderBy(F.col("rate_date").desc())
    ranked = silver.withColumn("date_rank", F.row_number().over(latest_first))
    return ranked.filter(F.col("date_rank") == 1).select(*RATE_KEY, "exchange_rate")


def clean_new_rates(rates: DataFrame, rules: dict, latest_rates: DataFrame) -> DataFrame:
    filled = fill_calendar_gaps(latest_valid_rates(rates, rules).unionByName(latest_rates))
    return add_date_attributes(filled.join(latest_rates.select(*RATE_KEY), list(RATE_KEY), "left_anti"))


@dataclass(frozen=True)
class ExchangeRateReport:
    table: str
    rows_read: int
    rows_dropped_by_rule: dict
    duplicates_removed: int
    days_carried_forward: int
    rows_written: int


def count_written_days(cleaned: DataFrame) -> dict:
    [totals] = cleaned.agg(
        F.count(F.lit(1)).alias("rows_written"),
        F.sum((~F.col("is_published")).cast("int")).alias("days_carried_forward"),
    ).collect()
    return {name: int(value or 0) for name, value in totals.asDict().items()}


def build_exchange_rate_report(table: str, rule_hits: dict, written_days: dict) -> ExchangeRateReport:
    days_published = written_days["rows_written"] - written_days["days_carried_forward"]
    return ExchangeRateReport(
        table=table,
        rows_read=rule_hits["rows_read"],
        rows_dropped_by_rule=rows_dropped_by_rule(rule_hits),
        duplicates_removed=rule_hits["rows_valid"] - days_published,
        days_carried_forward=written_days["days_carried_forward"],
        rows_written=written_days["rows_written"],
    )


def write_table(session: SparkSession, rates: DataFrame, definition: TableDefinition) -> dict:
    overwrite_table(session, rates, definition)
    return count_written_days(session.table(definition.name))


def append_missing_days(session: SparkSession, rates: DataFrame, rules: dict, table: str) -> ExchangeRateReport:
    silver = session.table(table)
    batch = rows_after_latest_loaded(rates, silver, CURRENCY_PAIR, "rate_date")
    rule_hits = count_rule_hits(batch, rules)
    new_days = clean_new_rates(batch, rules, latest_silver_rates(silver))
    written_days = count_written_days(new_days)
    new_days.writeTo(table).append()
    return build_exchange_rate_report(table, rule_hits, written_days)


def load_rates(session: SparkSession, bronze_path: str, table: str, load_mode: str) -> ExchangeRateReport:
    rates = normalize_rates(session.read.format("delta").load(bronze_path))
    rules = invalid_rate_rules()
    definition = TableDefinition.from_frame(table, clean_rates(rates, rules), text_types=TEXT_TYPES)
    if appends_to_current_table(session, definition, load_mode):
        return append_missing_days(session, rates, rules, table)
    rule_hits = count_rule_hits(rates, rules)
    written_days = write_table(session, clean_rates(rates, rules), definition)
    return build_exchange_rate_report(table, rule_hits, written_days)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

report = asdict(load_rates(spark, bronze_table_path(bronze_lakehouse, bronze_table), silver_table, load_mode))
vacuum_after_load(spark, silver_table, vacuum_retention_hours, load_mode)
print(json.dumps(report, indent=2))

runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(report))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
