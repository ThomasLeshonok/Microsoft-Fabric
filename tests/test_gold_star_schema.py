import os
import sys
from datetime import date, datetime, timezone

import pytest
from notebook_loader import load

DEFINITION_CELLS = slice(1, -1)
MONDAY = date(2026, 11, 16)
SATURDAY = date(2026, 11, 14)
TRIPS_SCHEMA = (
    "pickup_datetime timestamp, dropoff_datetime timestamp, pickup_location_id int, dropoff_location_id int, "
    "passenger_count int, trip_distance double, trip_duration_minutes double, fare_amount double, total_amount double"
)
PICKUP_KEY, DROPOFF_KEY = 1, 2
RATES_SCHEMA = "base_currency string, quote_currency string, rate_date date, exchange_rate double, is_published boolean"
ZONES_SCHEMA = "location_id int, borough string, zone string, service_zone string, boundary_geojson string"
READINGS_SCHEMA = (
    "sensor_id bigint, taxi_zone_id int, parameter_name string, parameter_units string, measured_date date, "
    "avg_value double, min_value double, max_value double, percent_complete double"
)
HOURLY_READINGS_SCHEMA = (
    "sensor_id bigint, taxi_zone_id int, parameter_name string, parameter_units string, measured_date date, "
    "measured_hour int, value double"
)
INDICATORS_SCHEMA = "indicator_id string, country_iso3 string, country_name string, year int, value double"
SILVER_HOURLY_READINGS_SCHEMA = f"{HOURLY_READINGS_SCHEMA}, measured_at_utc timestamp"
SETTLED_BEFORE = datetime(2026, 11, 16, 10)


@pytest.fixture(scope="module")
def gold():
    return load(("nb_gold_star_schema", DEFINITION_CELLS))


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


def zones(spark):
    return spark.createDataFrame(
        [
            (161, "Manhattan", "Midtown Center", "Yellow Zone", '{"type": "MultiPolygon"}'),
            (1, "EWR", "Newark Airport", "EWR", None),
            (132, "Queens", "JFK Airport", "Airports", None),
            (264, "Unknown", "N/A", "N/A", None),
        ],
        ZONES_SCHEMA,
    )


def rates(spark):
    return spark.createDataFrame([("EUR", "USD", MONDAY, 1.25, True), ("EUR", "USD", SATURDAY, 1.2, False)], RATES_SCHEMA)


def rows(frame, *order):
    return [row.asDict() for row in frame.orderBy(*order).collect()]


def at(day, hour, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)


def gold_inputs(gold, spark, calendar, zones_frame, rates_frame):
    silver_tables = {
        "SILVER_TRIPS_TABLE": spark.createDataFrame([], TRIPS_SCHEMA),
        "SILVER_ZONES_TABLE": zones_frame,
        "SILVER_AIR_QUALITY_DAILY_TABLE": spark.createDataFrame([], READINGS_SCHEMA),
        "SILVER_AIR_QUALITY_HOURLY_TABLE": spark.createDataFrame([], SILVER_HOURLY_READINGS_SCHEMA),
        "SILVER_EXCHANGE_RATES_TABLE": rates_frame,
        "SILVER_INDICATORS_TABLE": spark.createDataFrame([], INDICATORS_SCHEMA),
    }
    for constant, frame in silver_tables.items():
        frame.createOrReplaceTempView(gold[constant])
    return gold["GoldInputs"](spark, calendar, 75.0, SETTLED_BEFORE)


def test_dim_date_covers_whole_calendar_with_day_attributes(gold, spark):
    calendar = gold["calendar_span"](2016, MONDAY)

    dim_date = gold["build_dim_date"](spark, calendar)
    [saturday] = dim_date.where(dim_date.DateKey == 20261114).collect()

    assert calendar == (date(2016, 1, 1), date(2026, 12, 31))
    assert dim_date.count() == (date(2026, 12, 31) - date(2016, 1, 1)).days + 1
    assert saturday.asDict() == {
        "DateKey": 20261114,
        "Date": SATURDAY,
        "Year": 2026,
        "Quarter": 4,
        "Month": 11,
        "MonthName": "November",
        "MonthStartDate": date(2026, 11, 1),
        "DayOfMonth": 14,
        "DayOfWeek": 6,
        "DayName": "Saturday",
        "WeekOfYear": 46,
        "IsWeekend": True,
    }


def test_dim_zone_groups_airports_rolls_boroughs_up_to_city_and_adds_unknown_member(gold, spark):
    dim_zone = gold["build_dim_zone"](zones(spark))

    assert rows(dim_zone, "ZoneKey") == [
        {"ZoneKey": -1, "Borough": "Unknown", "Zone": "Unknown", "ServiceZone": "Unknown", "City": "Unknown"},
        {"ZoneKey": 1, "Borough": "Airports", "Zone": "Newark Airport", "ServiceZone": "EWR", "City": "Outside New York City"},
        {"ZoneKey": 132, "Borough": "Airports", "Zone": "JFK Airport", "ServiceZone": "Airports", "City": "New York City"},
        {"ZoneKey": 161, "Borough": "Manhattan", "Zone": "Midtown Center", "ServiceZone": "Yellow Zone", "City": "New York City"},
        {"ZoneKey": 264, "Borough": "Unknown", "Zone": "N/A", "ServiceZone": "N/A", "City": "Outside New York City"},
    ]


def daily_trips(spark):
    return spark.createDataFrame(
        [
            (at(MONDAY, 8), at(MONDAY, 8, 20), 161, 132, 1, 2.0, 10.0, 10.0, 15.0),
            (at(MONDAY, 8, 10), at(MONDAY, 8, 40), 161, 161, 3, 4.0, 20.0, 20.0, 25.0),
            (at(MONDAY, 8, 30), at(MONDAY, 8, 50), 999, None, 1, 1.0, 5.0, 7.0, 10.0),
            (at(MONDAY, 9), at(MONDAY, 9, 30), None, 161, None, 1.0, 5.0, 3.0, 5.0),
        ],
        TRIPS_SCHEMA,
    )


def test_fact_taxi_daily_sums_and_averages_per_day_and_pickup_zone(gold, spark):
    dim_zone = gold["build_dim_zone"](zones(spark))

    fact = gold["build_fact_taxi_daily"](daily_trips(spark), dim_zone, rates(spark))

    assert rows(fact.where(fact.TripEndKey == PICKUP_KEY), "ZoneKey") == [
        {
            "DateKey": 20261116,
            "ZoneKey": -1,
            "TripEndKey": PICKUP_KEY,
            "TripCount": 2,
            "PassengerCount": 1,
            "TripDistanceMiles": 2.0,
            "TripDurationMinutes": 10.0,
            "FareAmountUsd": 10.0,
            "TotalAmountUsd": 15.0,
            "FareAmountEur": 8.0,
            "TotalAmountEur": 12.0,
            "AvgFareAmountUsd": 5.0,
            "AvgTotalAmountUsd": 7.5,
            "AvgTotalAmountEur": 6.0,
            "AvgTripDistanceMiles": 1.0,
            "AvgTripDurationMinutes": 5.0,
        },
        {
            "DateKey": 20261116,
            "ZoneKey": 161,
            "TripEndKey": PICKUP_KEY,
            "TripCount": 2,
            "PassengerCount": 4,
            "TripDistanceMiles": 6.0,
            "TripDurationMinutes": 30.0,
            "FareAmountUsd": 30.0,
            "TotalAmountUsd": 40.0,
            "FareAmountEur": 24.0,
            "TotalAmountEur": 32.0,
            "AvgFareAmountUsd": 15.0,
            "AvgTotalAmountUsd": 20.0,
            "AvgTotalAmountEur": 16.0,
            "AvgTripDistanceMiles": 3.0,
            "AvgTripDurationMinutes": 15.0,
        },
    ]


def test_fact_taxi_daily_sums_the_same_trips_per_dropoff_zone(gold, spark):
    dim_zone = gold["build_dim_zone"](zones(spark))

    fact = gold["build_fact_taxi_daily"](daily_trips(spark), dim_zone, rates(spark))
    dropoffs = fact.where(fact.TripEndKey == DROPOFF_KEY).select(
        "ZoneKey", "TripCount", "PassengerCount", "FareAmountUsd", "TotalAmountEur"
    )

    assert rows(dropoffs, "ZoneKey") == [
        {"ZoneKey": -1, "TripCount": 1, "PassengerCount": 1, "FareAmountUsd": 7.0, "TotalAmountEur": 8.0},
        {"ZoneKey": 132, "TripCount": 1, "PassengerCount": 1, "FareAmountUsd": 10.0, "TotalAmountEur": 12.0},
        {"ZoneKey": 161, "TripCount": 2, "PassengerCount": 3, "FareAmountUsd": 23.0, "TotalAmountEur": 24.0},
    ]


def test_fact_taxi_daily_counts_dropoffs_on_their_day_until_the_last_pickup_day(gold, spark):
    trips = spark.createDataFrame(
        [
            (at(SATURDAY, 23, 50), at(date(2026, 11, 15), 0, 10), 161, 161, 1, 2.0, 20.0, 10.0, 15.0),
            (at(MONDAY, 8), at(MONDAY, 8, 20), 161, 161, 1, 2.0, 20.0, 10.0, 15.0),
            (at(MONDAY, 23, 55), at(date(2026, 11, 17), 0, 5), 161, 161, 1, 2.0, 10.0, 10.0, 15.0),
        ],
        TRIPS_SCHEMA,
    )

    fact = gold["build_fact_taxi_daily"](trips, gold["build_dim_zone"](zones(spark)), rates(spark))

    assert rows(fact.select("TripEndKey", "DateKey", "TripCount"), "TripEndKey", "DateKey") == [
        {"TripEndKey": PICKUP_KEY, "DateKey": 20261114, "TripCount": 1},
        {"TripEndKey": PICKUP_KEY, "DateKey": 20261116, "TripCount": 2},
        {"TripEndKey": DROPOFF_KEY, "DateKey": 20261115, "TripCount": 1},
        {"TripEndKey": DROPOFF_KEY, "DateKey": 20261116, "TripCount": 1},
    ]


def test_fact_taxi_daily_leaves_eur_empty_without_a_rate(gold, spark):
    tuesday = date(2026, 11, 17)
    trips = spark.createDataFrame([(at(tuesday, 8), at(tuesday, 8, 20), 161, 161, 1, 2.0, 10.0, 10.0, 15.0)], TRIPS_SCHEMA)

    fact = gold["build_fact_taxi_daily"](trips, gold["build_dim_zone"](zones(spark)), rates(spark))

    assert {(row.TotalAmountUsd, row.TotalAmountEur, row.AvgTotalAmountEur) for row in fact.collect()} == {(15.0, None, None)}


def test_fact_air_quality_daily_aggregates_covered_sensor_days_per_zone_and_pollutant(gold, spark):
    readings = spark.createDataFrame(
        [
            (1, 161, "pm25", "µg/m³", MONDAY, 8.0, 2.0, 20.0, 100.0),
            (2, 161, "pm25", "µg/m³", MONDAY, 12.0, 4.0, 30.0, 75.0),
            (2, 161, "pm25", "µg/m³", SATURDAY, 50.0, 40.0, 60.0, 74.9),
            (3, None, "no2", "ppm", MONDAY, 0.02, 0.01, 0.04, 90.0),
            (4, None, "no2", "ppm", MONDAY, 0.03, 0.02, 0.05, 90.0),
        ],
        READINGS_SCHEMA,
    )

    fact = gold["build_fact_air_quality_daily"](readings, 75.0)

    assert rows(fact, "ZoneKey") == [
        {"DateKey": 20261116, "ZoneKey": -1, "Pollutant": "no2", "Unit": "ppm", "SensorCount": 2, "AvgValue": pytest.approx(0.025), "MinValue": 0.01, "MaxValue": 0.05},
        {"DateKey": 20261116, "ZoneKey": 161, "Pollutant": "pm25", "Unit": "µg/m³", "SensorCount": 2, "AvgValue": 10.0, "MinValue": 2.0, "MaxValue": 30.0},
    ]


def test_dim_hour_has_every_hour_with_its_day_part(gold, spark):
    dim_hour = rows(gold["build_dim_hour"](spark), "HourKey")

    assert len(dim_hour) == 24
    assert [dim_hour[hour] for hour in (0, 5, 6, 10, 16, 19, 20, 23)] == [
        {"HourKey": 0, "HourLabel": "00:00", "DayPart": "Night", "DayPartOrder": 1},
        {"HourKey": 5, "HourLabel": "05:00", "DayPart": "Night", "DayPartOrder": 1},
        {"HourKey": 6, "HourLabel": "06:00", "DayPart": "Morning rush", "DayPartOrder": 2},
        {"HourKey": 10, "HourLabel": "10:00", "DayPart": "Midday", "DayPartOrder": 3},
        {"HourKey": 16, "HourLabel": "16:00", "DayPart": "Evening rush", "DayPartOrder": 4},
        {"HourKey": 19, "HourLabel": "19:00", "DayPart": "Evening rush", "DayPartOrder": 4},
        {"HourKey": 20, "HourLabel": "20:00", "DayPart": "Evening", "DayPartOrder": 5},
        {"HourKey": 23, "HourLabel": "23:00", "DayPart": "Evening", "DayPartOrder": 5},
    ]


def test_fact_taxi_hourly_counts_trips_per_hour_and_zone_of_each_trip_end_until_the_last_pickup_hour(gold, spark):
    trips = spark.createDataFrame(
        [
            (at(MONDAY, 8, 10), at(MONDAY, 8, 40), 161, 132, 1, 2.0, 30.0, 10.0, 15.0),
            (at(MONDAY, 8, 30), at(MONDAY, 9, 5), 161, 161, 3, 4.0, 35.0, 20.0, 25.0),
            (at(MONDAY, 17), at(MONDAY, 17, 20), 161, 999, 1, 1.0, 20.0, 7.0, 10.0),
            (at(MONDAY, 17, 50), at(MONDAY, 18, 10), 999, 161, 1, 1.0, 20.0, 7.0, 10.0),
        ],
        TRIPS_SCHEMA,
    )

    fact = gold["build_fact_taxi_hourly"](trips, gold["build_dim_zone"](zones(spark)))

    assert rows(fact, "TripEndKey", "ZoneKey", "HourKey") == [
        {"DateKey": 20261116, "HourKey": 17, "ZoneKey": -1, "TripEndKey": PICKUP_KEY, "TripCount": 1},
        {"DateKey": 20261116, "HourKey": 8, "ZoneKey": 161, "TripEndKey": PICKUP_KEY, "TripCount": 2},
        {"DateKey": 20261116, "HourKey": 17, "ZoneKey": 161, "TripEndKey": PICKUP_KEY, "TripCount": 1},
        {"DateKey": 20261116, "HourKey": 17, "ZoneKey": -1, "TripEndKey": DROPOFF_KEY, "TripCount": 1},
        {"DateKey": 20261116, "HourKey": 8, "ZoneKey": 132, "TripEndKey": DROPOFF_KEY, "TripCount": 1},
        {"DateKey": 20261116, "HourKey": 9, "ZoneKey": 161, "TripEndKey": DROPOFF_KEY, "TripCount": 1},
    ]


def test_dim_trip_end_has_pickup_and_dropoff(gold, spark):
    assert rows(gold["build_dim_trip_end"](spark), "TripEndKey") == [
        {"TripEndKey": PICKUP_KEY, "TripEnd": "Pickup"},
        {"TripEndKey": DROPOFF_KEY, "TripEnd": "Dropoff"},
    ]


def test_fact_air_quality_hourly_aggregates_sensors_per_hour_zone_and_pollutant(gold, spark):
    readings = spark.createDataFrame(
        [
            (1, 161, "pm25", "µg/m³", MONDAY, 8, 10.0),
            (2, 161, "pm25", "µg/m³", MONDAY, 8, 20.0),
            (1, 161, "pm25", "µg/m³", MONDAY, 9, 5.0),
            (3, None, "no2", "ppm", MONDAY, 8, 0.02),
        ],
        HOURLY_READINGS_SCHEMA,
    )

    fact = gold["build_fact_air_quality_hourly"](readings)

    assert rows(fact, "ZoneKey", "HourKey") == [
        {"DateKey": 20261116, "HourKey": 8, "ZoneKey": -1, "Pollutant": "no2", "Unit": "ppm", "SensorCount": 1, "AvgValue": 0.02, "MaxValue": 0.02},
        {"DateKey": 20261116, "HourKey": 8, "ZoneKey": 161, "Pollutant": "pm25", "Unit": "µg/m³", "SensorCount": 2, "AvgValue": 15.0, "MaxValue": 20.0},
        {"DateKey": 20261116, "HourKey": 9, "ZoneKey": 161, "Pollutant": "pm25", "Unit": "µg/m³", "SensorCount": 1, "AvgValue": 5.0, "MaxValue": 5.0},
    ]


def test_dim_gdp_keeps_gdp_and_computes_growth_only_against_the_previous_year(gold, spark):
    indicators = spark.createDataFrame(
        [
            ("NY.GDP.MKTP.CD", "USA", "United States", 2022, 100.0),
            ("NY.GDP.MKTP.CD", "USA", "United States", 2023, 110.0),
            ("NY.GDP.MKTP.CD", "USA", "United States", 2025, 120.0),
            ("NY.GDP.PCAP.CD", "USA", "United States", 2023, 80000.0),
        ],
        INDICATORS_SCHEMA,
    )

    dim_gdp = gold["build_dim_gdp"](indicators)

    assert rows(dim_gdp, "Year") == [
        {"Year": 2022, "CountryIso3": "USA", "CountryName": "United States", "GdpUsd": 100.0, "GdpGrowthPercent": None},
        {"Year": 2023, "CountryIso3": "USA", "CountryName": "United States", "GdpUsd": 110.0, "GdpGrowthPercent": 10.0},
        {"Year": 2025, "CountryIso3": "USA", "CountryName": "United States", "GdpUsd": 120.0, "GdpGrowthPercent": None},
    ]


def test_dim_fx_keys_each_rate_by_day(gold, spark):
    assert rows(gold["build_dim_fx"](rates(spark)), "DateKey") == [
        {"DateKey": 20261114, "BaseCurrency": "EUR", "QuoteCurrency": "USD", "ExchangeRate": 1.2, "IsPublished": False},
        {"DateKey": 20261116, "BaseCurrency": "EUR", "QuoteCurrency": "USD", "ExchangeRate": 1.25, "IsPublished": True},
    ]


def test_star_schema_has_the_ten_gold_tables(gold, spark):
    inputs = gold_inputs(gold, spark, (MONDAY, MONDAY), zones(spark), rates(spark))

    tables = gold["build_tables"](gold["GOLD_TABLES"], inputs)

    assert list(tables) == [
        "DimDate",
        "DimZone",
        "DimFX",
        "DimGDP",
        "FactTaxiDaily",
        "FactAirQualityDaily",
        "DimHour",
        "DimTripEnd",
        "FactTaxiHourly",
        "FactAirQualityHourly",
    ]
    assert {name: table.count() for name, table in tables.items()} == {
        "DimDate": 1,
        "DimZone": 5,
        "DimFX": 2,
        "DimGDP": 0,
        "FactTaxiDaily": 0,
        "FactAirQualityDaily": 0,
        "DimHour": 24,
        "DimTripEnd": 2,
        "FactTaxiHourly": 0,
        "FactAirQualityHourly": 0,
    }


def test_hourly_air_quality_keeps_only_hours_started_before_the_settle_cutoff(gold, spark):
    readings = spark.createDataFrame(
        [(datetime(2026, 11, 16, 9), 10.0), (datetime(2026, 11, 16, 10), 20.0)],
        "measured_at_utc timestamp, value double",
    )

    settled = gold["settled_hours"](readings, SETTLED_BEFORE)

    assert [row.value for row in settled.collect()] == [10.0]


def registry(gold, *names):
    return {name: gold["GOLD_TABLES"][name] for name in names}


def test_select_tables_keeps_star_schema_order_and_rejects_unknown_tables(gold):
    tables = registry(gold, "DimDate", "DimZone", "FactTaxiDaily")

    assert list(gold["select_tables"](tables, " FactTaxiDaily, DimDate ", "rebuild")) == ["DimDate", "FactTaxiDaily"]
    assert list(gold["select_tables"](tables, "", "rebuild")) == ["DimDate", "DimZone", "FactTaxiDaily"]
    with pytest.raises(ValueError, match="Unknown gold tables"):
        gold["select_tables"](tables, "FactTrips", "rebuild")
    with pytest.raises(ValueError, match="Unknown load_mode"):
        gold["select_tables"](tables, "", "overwrite")


def test_append_missing_rejects_tables_without_a_period(gold):
    tables = registry(gold, "DimDate", "DimZone", "DimHour")

    with pytest.raises(ValueError, match=r"\['DimHour', 'DimZone'\] cannot be appended"):
        gold["select_tables"](tables, "", "append_missing")
    assert list(gold["select_tables"](tables, "DimDate", "append_missing")) == ["DimDate"]


def test_only_selected_tables_are_built(gold):
    def unselected(inputs):
        raise AssertionError("an unselected table was built")

    tables = {
        "FactNew": gold["GoldTable"](lambda inputs: f"built from {inputs}", gold["AppendRule"]("DateKey")),
        "DimOther": gold["GoldTable"](unselected),
    }

    selected = gold["select_tables"](tables, "FactNew", "append_missing")

    assert gold["build_tables"](selected, "inputs") == {"FactNew": "built from inputs"}


def append_rule(gold, table):
    return gold["GOLD_TABLES"][table].append_rule


def test_latest_loaded_query_groups_by_the_rule_keys(gold):
    assert gold["latest_loaded_query"]("FactTaxiDaily", append_rule(gold, "FactTaxiDaily")) == (
        "SELECT MAX([DateKey]) AS [latest_loaded_period] FROM [dbo].[FactTaxiDaily]"
    )
    assert gold["latest_loaded_query"]("DimGDP", append_rule(gold, "DimGDP")) == (
        "SELECT [CountryIso3], MAX([Year]) AS [latest_loaded_period] FROM [dbo].[DimGDP] GROUP BY [CountryIso3]"
    )
    assert gold["latest_loaded_query"]("FactAirQualityHourly", append_rule(gold, "FactAirQualityHourly")) == (
        "SELECT MAX([DateKey] * 100 + [HourKey]) AS [latest_loaded_period] FROM [dbo].[FactAirQualityHourly]"
    )


def test_append_keeps_only_days_after_the_latest_loaded_day(gold, spark):
    fact = spark.createDataFrame([(20261114, 1), (20261115, 2), (20261116, 3)], "DateKey int, TripCount int")
    rule = append_rule(gold, "FactTaxiDaily")

    def latest_loaded(latest_period):
        return spark.createDataFrame([(latest_period,)], "latest_loaded_period int")

    after_saturday = gold["rows_after_latest_loaded"](fact, latest_loaded(20261114), rule)
    into_empty_table = gold["rows_after_latest_loaded"](fact, latest_loaded(None), rule)

    assert rows(after_saturday, "DateKey") == [{"DateKey": 20261115, "TripCount": 2}, {"DateKey": 20261116, "TripCount": 3}]
    assert into_empty_table.count() == 3


def test_append_keeps_only_hours_after_the_latest_loaded_hour(gold, spark):
    fact = spark.createDataFrame(
        [(20261115, 23, 1), (20261116, 7, 2), (20261116, 8, 3), (20261116, 9, 4)], "DateKey int, HourKey int, SensorCount int"
    )
    latest = spark.createDataFrame([(2026111608,)], "latest_loaded_period int")

    appended = gold["rows_after_latest_loaded"](fact, latest, append_rule(gold, "FactAirQualityHourly"))

    assert rows(appended, "HourKey") == [{"DateKey": 20261116, "HourKey": 9, "SensorCount": 4}]


def test_append_keeps_only_years_after_the_latest_loaded_year_per_country(gold, spark):
    dim_gdp = spark.createDataFrame(
        [("USA", 2024, 1.0), ("USA", 2025, 2.0), ("DEU", 2024, 3.0), ("FRA", 2024, 4.0)],
        "CountryIso3 string, Year int, GdpUsd double",
    )
    latest = spark.createDataFrame([("USA", 2024), ("DEU", 2024)], "CountryIso3 string, latest_loaded_period int")

    appended = gold["rows_after_latest_loaded"](dim_gdp, latest, append_rule(gold, "DimGDP"))

    assert rows(appended, "CountryIso3") == [
        {"CountryIso3": "FRA", "Year": 2024, "GdpUsd": 4.0},
        {"CountryIso3": "USA", "Year": 2025, "GdpUsd": 2.0},
    ]


def test_missing_rows_loads_every_row_into_a_table_not_in_the_warehouse(gold, spark):
    fact = spark.createDataFrame([(20261116, 3)], "DateKey int, TripCount int")

    def read_query(query):
        raise AssertionError(f"no watermark query expected, got {query}")

    assert gold["missing_rows"](read_query, "FactTaxiDaily", fact, {"DimDate"}) is fact
