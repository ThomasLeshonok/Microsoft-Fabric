import os
import sys
from datetime import date, datetime

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(1, -1)
BRONZE_SCHEMA = (
    "key string, freq string, currency string, currency_denom string, exr_type string, time_period date, "
    "obs_value double, obs_status string, obs_conf string, title string, unit string, ingested_at timestamp"
)
FIRST_INGESTION = datetime(2026, 11, 16, 18, 17)
LATER_INGESTION = datetime(2026, 11, 17, 6, 0)
FRIDAY = date(2026, 11, 13)
MONDAY = date(2026, 11, 16)


@pytest.fixture(scope="module")
def silver():
    return load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_ecb_exchange_rates", DEFINITION_CELLS))


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def bronze_rate(**overrides):
    rate = {
        "key": "EXR.D.USD.EUR.SP00.A",
        "freq": "D",
        "currency": "USD",
        "currency_denom": "EUR",
        "exr_type": "SP00",
        "time_period": MONDAY,
        "obs_value": 1.1601,
        "obs_status": "A",
        "obs_conf": "F",
        "title": "US dollar/Euro ECB reference exchange rate",
        "unit": "USD",
        "ingested_at": FIRST_INGESTION,
    }
    return {**rate, **overrides}


def normalize(silver, spark, *rates):
    return silver["normalize_rates"](spark.createDataFrame(list(rates), BRONZE_SCHEMA))


def clean(silver, spark, *rates):
    cleaned = silver["clean_rates"](normalize(silver, spark, *rates), silver["invalid_rate_rules"]())
    return cleaned.orderBy("rate_date").collect()


def test_rate_keeps_informative_columns_and_gains_date_attributes(silver, spark):
    [rate] = clean(silver, spark, bronze_rate())

    assert rate.asDict() == {
        "base_currency": "EUR",
        "quote_currency": "USD",
        "rate_date": MONDAY,
        "exchange_rate": 1.1601,
        "is_published": True,
        "rate_year": 2026,
        "rate_month": 11,
        "rate_day_of_week": 1,
        "is_weekend": False,
    }


def test_table_schema_uses_currency_codes_and_narrow_date_parts(silver, spark):
    rates = silver["clean_rates"](normalize(silver, spark, bronze_rate()), silver["invalid_rate_rules"]())

    definition = silver["TableDefinition"].from_frame("silver_ecb_exchange_rates", rates, text_types=silver["TEXT_TYPES"])

    assert definition.columns == (
        "base_currency CHAR(3), quote_currency CHAR(3), rate_date date, exchange_rate double, is_published boolean, "
        "rate_year smallint, rate_month tinyint, rate_day_of_week tinyint, is_weekend boolean"
    )


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"currency": None}, id="missing_quote_currency"),
        pytest.param({"currency_denom": None}, id="missing_base_currency"),
        pytest.param({"time_period": None}, id="missing_date"),
        pytest.param({"freq": "M"}, id="monthly_series"),
        pytest.param({"obs_value": None}, id="missing_rate"),
        pytest.param({"obs_value": 0.0}, id="zero_rate"),
        pytest.param({"obs_value": -1.16}, id="negative_rate"),
    ],
)
def test_invalid_rates_are_dropped(silver, spark, overrides):
    assert clean(silver, spark, bronze_rate(**overrides)) == []


def test_currency_codes_are_trimmed_and_upper_cased(silver, spark):
    [rate] = clean(silver, spark, bronze_rate(currency=" usd ", currency_denom="eur", freq=" d"))
    assert (rate["base_currency"], rate["quote_currency"]) == ("EUR", "USD")


def test_duplicate_keeps_latest_ingestion(silver, spark):
    first = bronze_rate(obs_value=1.15)
    later = bronze_rate(obs_value=1.1601, ingested_at=LATER_INGESTION)

    [rate] = clean(silver, spark, later, first)

    assert rate["exchange_rate"] == 1.1601


def test_weekend_carries_forward_last_published_rate(silver, spark):
    rates = clean(silver, spark, bronze_rate(time_period=FRIDAY, obs_value=1.1588), bronze_rate())

    assert [(row["rate_date"], row["exchange_rate"], row["is_published"], row["is_weekend"]) for row in rates] == [
        (FRIDAY, 1.1588, True, False),
        (date(2026, 11, 14), 1.1588, False, True),
        (date(2026, 11, 15), 1.1588, False, True),
        (MONDAY, 1.1601, True, False),
    ]


def test_calendar_gaps_are_filled_per_currency_pair(silver, spark):
    usd_friday = bronze_rate(time_period=FRIDAY, obs_value=1.1588)
    gbp_monday = bronze_rate(currency="GBP", obs_value=0.8812)

    rates = clean(silver, spark, usd_friday, bronze_rate(), gbp_monday)

    gbp_days = [row["rate_date"] for row in rates if row["quote_currency"] == "GBP"]
    usd_days = [row["rate_date"] for row in rates if row["quote_currency"] == "USD"]
    assert (gbp_days, len(usd_days)) == ([MONDAY], 4)


def test_table_report_counts_rules_duplicates_and_carried_forward_days(silver, spark):
    rates = normalize(
        silver,
        spark,
        bronze_rate(time_period=FRIDAY),
        bronze_rate(),
        bronze_rate(),
        bronze_rate(time_period=date(2026, 11, 17), obs_value=None),
    )
    rules = silver["invalid_rate_rules"]()

    rule_hits = silver["count_rule_hits"](rates, rules)
    written_days = silver["count_written_days"](silver["clean_rates"](rates, rules))
    report = silver["build_exchange_rate_report"]("silver_ecb_exchange_rates", rule_hits, written_days)

    assert (report.rows_read, report.duplicates_removed, report.days_carried_forward, report.rows_written) == (4, 1, 2, 4)
    assert report.rows_dropped_by_rule == {
        "missing_key": 0,
        "non_daily_frequency": 0,
        "missing_rate": 1,
        "non_positive_rate": 0,
    }


def test_append_fills_gap_from_last_silver_rate_and_keeps_existing_days(silver, spark):
    rules = silver["invalid_rate_rules"]()
    friday = bronze_rate(time_period=FRIDAY, obs_value=1.15)
    loaded = silver["clean_rates"](normalize(silver, spark, friday), rules)
    rates = normalize(silver, spark, friday, bronze_rate())

    batch = silver["rows_after_latest_loaded"](rates, loaded, silver["CURRENCY_PAIR"], "rate_date")
    new_days = silver["clean_new_rates"](batch, rules, silver["latest_silver_rates"](loaded))

    assert new_days.columns == loaded.columns
    assert [(day.rate_date, day.exchange_rate, day.is_published) for day in new_days.orderBy("rate_date").collect()] == [
        (date(2026, 11, 14), 1.15, False),
        (date(2026, 11, 15), 1.15, False),
        (MONDAY, 1.1601, True),
    ]


def test_append_without_new_published_day_adds_nothing(silver, spark):
    rules = silver["invalid_rate_rules"]()
    loaded = silver["clean_rates"](normalize(silver, spark, bronze_rate()), rules)
    rates = normalize(silver, spark, bronze_rate(ingested_at=LATER_INGESTION))

    batch = silver["rows_after_latest_loaded"](rates, loaded, silver["CURRENCY_PAIR"], "rate_date")

    assert silver["clean_new_rates"](batch, rules, silver["latest_silver_rates"](loaded)).count() == 0
