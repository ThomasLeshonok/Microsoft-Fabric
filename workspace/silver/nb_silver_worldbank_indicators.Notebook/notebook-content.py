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

# # nb_silver_worldbank_indicators
#
# Builds `silver_worldbank_indicators` in `lh_silver` from `bronze_worldbank_indicators` in `lh_bronze`: one row per
# indicator, country and year (for example `NY.GDP.MKTP.CD`, `USA`, 2024, GDP in current US$).
#
# Reference implementation for a new API source (`docs/adding_a_dataset.md`): this notebook holds only the source's
# transform functions and the `WORLDBANK_INDICATORS` spec (`SilverDataset`); `load_dataset` in
# `nb_silver_delta_tables` reads bronze, counts the rules, creates the table and applies `load_mode`.
#
# 1. Normalize: keep only columns with information, fixed types, readable names.
#    - Drops `country_id` (ISO2, same country as `country_iso3`), `unit` and `obs_status` (empty for every row) and
#      `decimals` (display hint, always 0).
#    - `period` becomes the smallint `year`. Only annual periods (`yyyy`) are kept; the World Bank also serves quarterly
#      (`2024Q1`) and monthly (`2024M01`) periods for some indicators, which do not fit one row per year.
#    - The currency moves out of `indicator_name` into `currency_name` as an ISO 4217 code: `GDP (current US$)`
#      becomes `GDP` and `USD`. Symbols map through `CURRENCY_CODES`; names without `(current ...)` or
#      `(constant <year> ...)`, such as `GDP growth (annual %)`, keep their text and get no currency.
# 2. Drop invalid rows, see `invalid_indicator_rules`. The API returns `value = null` for years not published yet.
#    Negative values stay: growth indicators can be negative. A currency symbol missing from `CURRENCY_CODES` is
#    dropped as `unknown_currency`; add it there.
# 3. Keep one row per `INDICATOR_KEY`, latest `ingested_at` wins.
# 4. Add `period_start_date` and `period_end_date` (1 January, 31 December), so gold can join yearly values to daily
#    facts by date range.
# 5. Write per `load_mode` (see `nb_silver_delta_tables`). `rebuild` (default, `pl_silver_startup`) overwrites the
#    table from all of bronze, so reruns are idempotent. `append_missing` (`pl_silver_worldbank_indicators`) appends,
#    per indicator and country (`series_key`), only the years (`period_column`) after its latest year in silver; years
#    already in silver are never rewritten, so later World Bank revisions of them need a rebuild. The table is created
#    from `nb_silver_delta_tables`:
#    `indicator_name`, `currency_name` and `country_iso3` are `CHAR(3)`, so an indicator whose name without currency
#    is longer than 3 characters (`GDP per capita`) fails the write; widen `TEXT_TYPES` before loading one.
# 6. `VACUUM` the table after a `rebuild` (see `vacuum_after_load`); an append leaves nothing to delete. Overwrite only
#    unlists the old parquet files, Delta keeps them for time travel; vacuum deletes those older than
#    `vacuum_retention_hours`. Below 168 hours Delta refuses unless
#    `spark.databricks.delta.retentionDurationCheck.enabled` is false.
#
# Exits with a report: rows read, rows dropped per rule, duplicates removed, rows written.

# PARAMETERS CELL ********************

bronze_lakehouse = "lh_bronze"
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
from dataclasses import asdict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ANNUAL_PERIOD_PATTERN = r"^\d{4}$"
INDICATOR_SERIES = ("indicator_id", "country_iso3")
INDICATOR_KEY = (*INDICATOR_SERIES, "year")
NAME_WITH_CURRENCY_PATTERN = r"^(.+?)\s*\((?:current|constant \d{4}) (.+)\)$"
CURRENCY_CODES = {"US$": "USD"}
TEXT_TYPES = {"indicator_name": "CHAR(3)", "currency_name": "CHAR(3)", "country_iso3": "CHAR(3)"}


def _extracted(name: Column, group: int) -> Column:
    return F.nullif(F.regexp_extract(name, NAME_WITH_CURRENCY_PATTERN, group), F.lit(""))


def _currency_code(symbol: Column) -> Column:
    codes = F.create_map(*[F.lit(value) for pair in CURRENCY_CODES.items() for value in pair])
    return codes[symbol]


def normalize_indicators(bronze: DataFrame) -> DataFrame:
    period = F.trim("period")
    name = F.trim("indicator_name")
    parsed = bronze.withColumns({"name_without_currency": _extracted(name, 1), "currency_symbol": _extracted(name, 2)})
    return parsed.select(
        F.col("indicator_id").cast("string").alias("indicator_id"),
        F.coalesce("name_without_currency", name).alias("indicator_name"),
        "currency_symbol",
        _currency_code(F.col("currency_symbol")).alias("currency_name"),
        F.col("country_iso3").cast("string").alias("country_iso3"),
        F.col("country_name").cast("string").alias("country_name"),
        F.when(period.rlike(ANNUAL_PERIOD_PATTERN), period.cast("smallint")).alias("year"),
        F.col("value").cast("double").alias("value"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )


def invalid_indicator_rules() -> dict:
    conditions = {
        "missing_key": F.col("indicator_id").isNull() | F.col("country_iso3").isNull(),
        "non_annual_period": F.col("year").isNull(),
        "missing_value": F.col("value").isNull(),
        "unknown_currency": F.col("currency_symbol").isNotNull() & F.col("currency_name").isNull(),
    }
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def add_period_dates(indicators: DataFrame) -> DataFrame:
    year = F.col("year")
    return indicators.withColumns(
        {
            "period_start_date": F.make_date(year, F.lit(1), F.lit(1)),
            "period_end_date": F.make_date(year, F.lit(12), F.lit(31)),
        }
    )


def clean_indicators(indicators: DataFrame, rules: dict) -> DataFrame:
    valid = indicators.filter(~is_invalid(rules)).drop("currency_symbol")
    return add_period_dates(keep_latest_ingestion(valid, INDICATOR_KEY).drop("ingested_at"))


WORLDBANK_INDICATORS = SilverDataset(
    bronze_table="bronze_worldbank_indicators",
    silver_table="silver_worldbank_indicators",
    normalize=normalize_indicators,
    rules=invalid_indicator_rules,
    clean=clean_indicators,
    series_key=INDICATOR_SERIES,
    period_column="year",
    text_types=TEXT_TYPES,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

bronze_path = bronze_table_path(bronze_lakehouse, WORLDBANK_INDICATORS.bronze_table)
report = asdict(load_dataset(spark, WORLDBANK_INDICATORS, bronze_path, load_mode))
vacuum_after_load(spark, WORLDBANK_INDICATORS.silver_table, vacuum_retention_hours, load_mode)
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
