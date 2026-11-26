import os
import sys
from datetime import date, datetime

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(1, -1)
BRONZE_SCHEMA = (
    "indicator_id string, indicator_name string, country_id string, country_name string, country_iso3 string, "
    "period string, value double, unit string, obs_status string, decimals bigint, ingested_at timestamp"
)
FIRST_INGESTION = datetime(2026, 11, 16, 18, 17)
LATER_INGESTION = datetime(2026, 11, 17, 6, 0)


@pytest.fixture(scope="module")
def silver():
    return load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_worldbank_indicators", DEFINITION_CELLS))


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


def bronze_indicator(**overrides):
    indicator = {
        "indicator_id": "NY.GDP.MKTP.CD",
        "indicator_name": "GDP (current US$)",
        "country_id": "US",
        "country_name": "United States",
        "country_iso3": "USA",
        "period": "2024",
        "value": 29298013000000.0,
        "unit": "",
        "obs_status": "",
        "decimals": 0,
        "ingested_at": FIRST_INGESTION,
    }
    return {**indicator, **overrides}


def normalize(silver, spark, *indicators):
    return silver["normalize_indicators"](spark.createDataFrame(list(indicators), BRONZE_SCHEMA))


def clean(silver, spark, *indicators):
    return silver["clean_indicators"](normalize(silver, spark, *indicators), silver["invalid_indicator_rules"]()).collect()


def test_indicator_keeps_informative_columns_and_gains_period_dates(silver, spark):
    [indicator] = clean(silver, spark, bronze_indicator())

    assert indicator.asDict() == {
        "indicator_id": "NY.GDP.MKTP.CD",
        "indicator_name": "GDP",
        "currency_name": "USD",
        "country_iso3": "USA",
        "country_name": "United States",
        "year": 2024,
        "value": 29298013000000.0,
        "period_start_date": date(2024, 1, 1),
        "period_end_date": date(2024, 12, 31),
    }


def test_table_schema_uses_fixed_length_codes_and_small_year(silver, spark):
    indicators = silver["clean_indicators"](normalize(silver, spark, bronze_indicator()), silver["invalid_indicator_rules"]())

    definition = silver["TableDefinition"].from_frame("silver_worldbank_indicators", indicators, text_types=silver["TEXT_TYPES"])

    assert definition.columns == (
        "indicator_id string, indicator_name CHAR(3), currency_name CHAR(3), country_iso3 CHAR(3), country_name string, "
        "year smallint, value double, period_start_date date, period_end_date date"
    )


@pytest.mark.parametrize(
    ("indicator_name", "expected"),
    [
        pytest.param("GDP (current US$)", ("GDP", "USD"), id="current_dollars"),
        pytest.param(" GDP (constant 2015 US$) ", ("GDP", "USD"), id="constant_dollars"),
        pytest.param("GDP growth (annual %)", ("GDP growth (annual %)", None), id="no_currency"),
    ],
)
def test_currency_moves_out_of_indicator_name(silver, spark, indicator_name, expected):
    [indicator] = clean(silver, spark, bronze_indicator(indicator_name=indicator_name))
    assert (indicator["indicator_name"], indicator["currency_name"]) == expected


def test_unknown_currency_symbol_is_dropped(silver, spark):
    indicators = normalize(silver, spark, bronze_indicator(indicator_name="GDP (current LCU)"))
    rules = silver["invalid_indicator_rules"]()

    assert silver["count_rule_hits"](indicators, rules)["unknown_currency"] == 1
    assert silver["clean_indicators"](indicators, rules).count() == 0


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"indicator_id": None}, id="missing_indicator"),
        pytest.param({"country_iso3": None}, id="missing_country"),
        pytest.param({"period": None}, id="missing_period"),
        pytest.param({"period": "2024Q1"}, id="quarterly_period"),
        pytest.param({"period": "2024M01"}, id="monthly_period"),
        pytest.param({"value": None}, id="unpublished_year"),
    ],
)
def test_invalid_indicators_are_dropped(silver, spark, overrides):
    assert clean(silver, spark, bronze_indicator(**overrides)) == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"period": " 2024 "}, id="padded_period"),
        pytest.param({"indicator_id": "NY.GDP.MKTP.KD.ZG", "value": -2.2}, id="negative_growth"),
        pytest.param({"value": 0.0}, id="zero_value"),
    ],
)
def test_valid_edge_indicators_are_kept(silver, spark, overrides):
    assert len(clean(silver, spark, bronze_indicator(**overrides))) == 1


def test_duplicate_keeps_latest_ingestion(silver, spark):
    first = bronze_indicator(value=29000000000000.0)
    later = bronze_indicator(value=29298013000000.0, ingested_at=LATER_INGESTION)
    previous_year = bronze_indicator(period="2023", value=27811520000000.0)

    indicators = clean(silver, spark, later, first, previous_year)

    assert sorted((row["year"], row["value"]) for row in indicators) == [(2023, 27811520000000.0), (2024, 29298013000000.0)]


def test_same_year_for_other_country_or_indicator_is_not_a_duplicate(silver, spark):
    germany = bronze_indicator(country_id="DE", country_name="Germany", country_iso3="DEU")
    gdp_growth = bronze_indicator(indicator_id="NY.GDP.MKTP.KD.ZG", indicator_name="GDP growth (annual %)", value=2.8)

    assert len(clean(silver, spark, bronze_indicator(), germany, gdp_growth)) == 3


def test_table_report_counts_rules_and_duplicates(silver, spark):
    indicators = normalize(silver, spark, bronze_indicator(), bronze_indicator(), bronze_indicator(period="2026", value=None))
    rules = silver["invalid_indicator_rules"]()

    rule_hits = silver["count_rule_hits"](indicators, rules)
    report = silver["build_table_report"]("silver_worldbank_indicators", rule_hits, silver["clean_indicators"](indicators, rules).count())

    assert (report.rows_read, report.duplicates_removed, report.rows_written) == (3, 1, 1)
    assert report.rows_dropped_by_rule == {"missing_key": 0, "non_annual_period": 0, "missing_value": 1, "unknown_currency": 0}


def test_vacuum_table_keeps_files_inside_retention(silver):
    statements = []
    session = type("FakeSession", (), {"sql": lambda self, statement: statements.append(statement)})()

    silver["vacuum_table"](session, "silver_worldbank_indicators", 168)

    assert statements == ["VACUUM silver_worldbank_indicators RETAIN 168 HOURS"]


def test_dataset_spec_appends_years_per_indicator_and_country(silver):
    dataset = silver["WORLDBANK_INDICATORS"]

    assert (dataset.bronze_table, dataset.silver_table) == ("bronze_worldbank_indicators", "silver_worldbank_indicators")
    assert (dataset.series_key, dataset.period_column, dataset.text_types) == (silver["INDICATOR_SERIES"], "year", silver["TEXT_TYPES"])


def test_append_keeps_only_years_after_latest_year_of_each_country(silver, spark):
    rules = silver["invalid_indicator_rules"]()
    loaded = silver["clean_indicators"](normalize(silver, spark, bronze_indicator(period="2023")), rules)
    indicators = normalize(
        silver,
        spark,
        bronze_indicator(period="2023", value=1.0),
        bronze_indicator(period="2024"),
        bronze_indicator(country_iso3="DEU", country_name="Germany", period="2020"),
    )

    batch = silver["rows_after_latest_loaded"](indicators, loaded, silver["INDICATOR_SERIES"], "year")

    assert sorted((row.country_iso3, row.year) for row in silver["clean_indicators"](batch, rules).collect()) == [("DEU", 2020), ("USA", 2024)]
