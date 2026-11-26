import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(1, -1)
MICROGRAMS = "µg/m³"
BRONZE_MEASUREMENT_SCHEMA = (
    "sensor_id bigint, location_id bigint, value double, parameter_name string, parameter_units string, "
    "period_label string, period_interval string, datetime_from_utc string, datetime_from_local string, "
    "datetime_to_utc string, datetime_to_local string, has_flags boolean, ingested_at timestamp"
)
BRONZE_DAY_SCHEMA = (
    "sensor_id bigint, location_id bigint, value double, parameter_name string, parameter_units string, "
    "datetime_from_utc string, datetime_from_local string, datetime_to_utc string, min double, q25 double, "
    "median double, q75 double, max double, avg double, sd double, observed_count bigint, expected_count bigint, "
    "percent_complete double, has_flags boolean, date_local date, ingested_at timestamp"
)
BRONZE_SENSOR_SCHEMA = (
    "location_id bigint, sensor_id bigint, sensor_name string, parameter_id bigint, parameter_name string, "
    "parameter_units string, parameter_display_name string, ingested_at timestamp"
)
BRONZE_LOCATION_SCHEMA = (
    "location_id bigint, location_name string, locality string, timezone string, country_code string, "
    "country_name string, provider_name string, owner_name string, is_mobile boolean, is_monitor boolean, "
    "latitude double, longitude double, datetime_first_utc string, datetime_last_utc string, ingested_at timestamp"
)
TAXI_ZONES_SCHEMA = "location_id int, boundary_geojson string"
SENSOR_ZONES_SCHEMA = "sensor_id int, taxi_zone_id smallint"
HOURLY_SENSOR_ZONE = 166
DAILY_SENSOR_ZONE = 107
HARLEM_SQUARE_WITH_HOLE = [
    [[-74.0, 40.8], [-74.0, 40.9], [-73.9, 40.9], [-73.9, 40.8], [-74.0, 40.8]],
    [[-73.96, 40.84], [-73.94, 40.84], [-73.94, 40.86], [-73.96, 40.86], [-73.96, 40.84]],
]
FIRST_INGESTION = datetime(2026, 11, 11, 16, 0)
LATER_INGESTION = datetime(2026, 11, 12, 4, 0)


@pytest.fixture(scope="module")
def silver():
    return load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_air_quality", DEFINITION_CELLS))


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


def bronze_measurement(**overrides):
    measurement = {
        "sensor_id": 1099,
        "location_id": 626,
        "value": 19.6,
        "parameter_name": "pm10",
        "parameter_units": MICROGRAMS,
        "period_label": "raw",
        "period_interval": "01:00:00",
        "datetime_from_utc": "2020-09-26T14:00:00Z",
        "datetime_from_local": "2020-09-26T10:00:00-04:00",
        "datetime_to_utc": "2020-09-26T15:00:00Z",
        "datetime_to_local": "2020-09-26T11:00:00-04:00",
        "has_flags": False,
        "ingested_at": FIRST_INGESTION,
    }
    return {**measurement, **overrides}


def bronze_day(**overrides):
    day = {
        "sensor_id": 3950,
        "location_id": 971,
        "value": 0.0327,
        "parameter_name": "o3",
        "parameter_units": "ppm",
        "datetime_from_utc": "2024-03-07T05:00:00Z",
        "datetime_from_local": "2024-03-07T00:00:00-05:00",
        "datetime_to_utc": "2024-03-08T05:00:00Z",
        "min": 0.02,
        "q25": 0.03,
        "median": 0.03,
        "q75": 0.035,
        "max": 0.047,
        "avg": 0.032667,
        "sd": 0.006,
        "observed_count": 23,
        "expected_count": 24,
        "percent_complete": 96.0,
        "has_flags": False,
        "date_local": date(2024, 3, 7),
        "ingested_at": FIRST_INGESTION,
    }
    return {**day, **overrides}


def bronze_sensor(**overrides):
    sensor = {
        "location_id": 384,
        "sensor_id": 673,
        "sensor_name": f"pm25 {MICROGRAMS}",
        "parameter_id": 2,
        "parameter_name": "pm25",
        "parameter_units": MICROGRAMS,
        "parameter_display_name": "PM2.5",
        "ingested_at": FIRST_INGESTION,
    }
    return {**sensor, **overrides}


def bronze_location(**overrides):
    location = {
        "location_id": 384,
        "location_name": "CCNY",
        "locality": "New York-Northern New Jersey-Long Island",
        "timezone": "America/New_York",
        "country_code": "US",
        "country_name": "United States",
        "provider_name": "AirNow",
        "owner_name": "Unknown Governmental Organization",
        "is_mobile": False,
        "is_monitor": True,
        "latitude": 40.8197,
        "longitude": -73.9481,
        "datetime_first_utc": "2016-03-12T09:00:00Z",
        "datetime_last_utc": "2026-11-12T02:00:00Z",
        "ingested_at": FIRST_INGESTION,
    }
    return {**location, **overrides}


def sensors_with_locations(silver, spark, sensors, locations):
    bronze_sensors = spark.createDataFrame(sensors, BRONZE_SENSOR_SCHEMA)
    bronze_locations = spark.createDataFrame(locations, BRONZE_LOCATION_SCHEMA)
    return silver["join_locations"](silver["normalize_sensors"](bronze_sensors), silver["normalize_locations"](bronze_locations))


def harlem_taxi_zones(spark):
    return spark.createDataFrame(
        [(166, json.dumps({"type": "MultiPolygon", "coordinates": [HARLEM_SQUARE_WITH_HOLE]})), (264, None)],
        TAXI_ZONES_SCHEMA,
    )


def sensors_with_zones(silver, spark, sensors, locations):
    joined = sensors_with_locations(silver, spark, sensors, locations)
    return silver["add_taxi_zones"](spark, joined, harlem_taxi_zones(spark))


def clean_sensors(silver, spark, sensors, locations):
    located = sensors_with_zones(silver, spark, sensors, locations)
    return silver["clean_sensors"](located, silver["invalid_sensor_rules"]()).collect()


def sensor_zones(spark):
    return spark.createDataFrame([(1099, HOURLY_SENSOR_ZONE), (3950, DAILY_SENSOR_ZONE)], SENSOR_ZONES_SCHEMA)


def clean_hourly(silver, spark, *measurements):
    spec = silver["HOURLY_READINGS"]
    readings = spec.prepare(spark.createDataFrame(list(measurements), BRONZE_MEASUREMENT_SCHEMA), sensor_zones(spark))
    return spec.clean(readings).collect()


def clean_daily(silver, spark, *days):
    spec = silver["DAILY_READINGS"]
    readings = spec.prepare(spark.createDataFrame(list(days), BRONZE_DAY_SCHEMA), sensor_zones(spark))
    return spec.clean(readings).collect()


def test_hourly_keeps_only_informative_columns(silver, spark):
    [reading] = clean_hourly(silver, spark, bronze_measurement())

    assert list(reading.asDict()) == [
        "sensor_id",
        "taxi_zone_id",
        "parameter_name",
        "parameter_units",
        "value",
        "has_flags",
        "measured_at_utc",
        "measured_at",
        "measured_date",
        "measured_hour",
        "measured_year",
        "measured_month",
        "measured_day_of_week",
        "is_weekend",
        "ingested_at",
    ]


@pytest.mark.parametrize(
    ("spec_name", "bronze", "schema", "columns"),
    [
        pytest.param(
            "HOURLY_READINGS",
            bronze_measurement,
            BRONZE_MEASUREMENT_SCHEMA,
            "sensor_id int, taxi_zone_id smallint, parameter_name VARCHAR(5), parameter_units VARCHAR(15), value double, "
            "has_flags boolean, measured_at_utc timestamp, measured_at timestamp, measured_date date, measured_hour tinyint, "
            "measured_year smallint, measured_month tinyint, measured_day_of_week tinyint, is_weekend boolean, "
            "ingested_at timestamp",
            id="hourly",
        ),
        pytest.param(
            "DAILY_READINGS",
            bronze_day,
            BRONZE_DAY_SCHEMA,
            "sensor_id int, taxi_zone_id smallint, parameter_name VARCHAR(5), parameter_units VARCHAR(15), "
            "measured_date date, avg_value double, min_value double, median_value double, max_value double, "
            "percent_complete double, has_flags boolean, measured_year smallint, measured_month tinyint, "
            "measured_day_of_week tinyint, is_weekend boolean, ingested_at timestamp",
            id="daily",
        ),
    ],
)
def test_reading_table_schema_uses_narrow_types_partitioned_by_year(silver, spark, spec_name, bronze, schema, columns):
    bronze_readings = spark.createDataFrame([bronze()], schema)

    definition = silver[spec_name].table_definition("silver_readings", bronze_readings, sensor_zones(spark))

    assert (definition.columns, definition.partition_columns) == (columns, ("measured_year",))


def test_reading_carries_its_sensor_taxi_zone_instead_of_openaq_location(silver, spark):
    [reading] = clean_hourly(silver, spark, bronze_measurement())

    assert reading["taxi_zone_id"] == HOURLY_SENSOR_ZONE
    assert "location_id" not in reading.asDict()


@pytest.mark.parametrize(
    ("clean", "bronze"),
    [(clean_hourly, bronze_measurement), (clean_daily, bronze_day)],
    ids=["hourly", "daily"],
)
def test_readings_of_sensors_missing_from_sensor_table_are_dropped(silver, spark, clean, bronze):
    assert clean(silver, spark, bronze(sensor_id=1100)) == []


def test_hourly_timestamps_hold_utc_instant_and_nyc_wall_clock(silver, spark):
    spec = silver["HOURLY_READINGS"]
    readings = spec.normalize(spark.createDataFrame([bronze_measurement()], BRONZE_MEASUREMENT_SCHEMA))

    [timestamps] = readings.selectExpr("string(measured_at_utc)", "string(measured_at)").collect()

    assert tuple(timestamps) == ("2020-09-26 14:00:00", "2020-09-26 10:00:00")


def test_hourly_uses_local_wall_clock_for_time_attributes(silver, spark):
    [reading] = clean_hourly(silver, spark, bronze_measurement())

    assert (reading["measured_date"], reading["measured_hour"]) == (date(2020, 9, 26), 10)
    assert (reading["measured_year"], reading["measured_month"]) == (2020, 9)
    assert (reading["measured_day_of_week"], reading["is_weekend"]) == (6, True)


def test_hourly_local_date_differs_from_utc_date_late_evening(silver, spark):
    [reading] = clean_hourly(
        silver,
        spark,
        bronze_measurement(datetime_from_utc="2024-01-16T02:00:00Z", datetime_from_local="2024-01-15T21:00:00-05:00"),
    )
    assert (reading["measured_date"], reading["measured_hour"], reading["is_weekend"]) == (date(2024, 1, 15), 21, False)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"sensor_id": None}, id="missing_sensor"),
        pytest.param({"datetime_from_utc": None}, id="missing_utc_time"),
        pytest.param({"datetime_from_local": "not a time"}, id="unparsable_local_time"),
        pytest.param({"value": None}, id="missing_value"),
        pytest.param({"value": -999.0}, id="negative_value"),
        pytest.param({"parameter_units": "ppm"}, id="unexpected_unit"),
        pytest.param({"parameter_name": "bc", "parameter_units": MICROGRAMS}, id="unsupported_parameter"),
        pytest.param({"parameter_name": "um003", "parameter_units": MICROGRAMS}, id="particle_count_as_mass"),
        pytest.param({"parameter_name": "nox", "parameter_units": MICROGRAMS}, id="nox_in_mass_unit"),
        pytest.param({"parameter_name": None}, id="missing_parameter"),
    ],
)
def test_invalid_measurements_are_dropped(silver, spark, overrides):
    assert clean_hourly(silver, spark, bronze_measurement(**overrides)) == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"value": 0.0}, id="zero_value"),
        pytest.param({"has_flags": True}, id="flagged"),
        pytest.param({"parameter_name": "pm1", "value": 4.4}, id="pm1"),
        pytest.param({"parameter_name": "pm25", "value": 6.8}, id="pm25"),
        pytest.param({"parameter_name": "um003", "parameter_units": "particles/cm³", "value": 812.0}, id="um003_count"),
    ],
)
def test_valid_edge_measurements_are_kept(silver, spark, overrides):
    assert len(clean_hourly(silver, spark, bronze_measurement(**overrides))) == 1


def test_hourly_duplicate_keeps_latest_ingestion(silver, spark):
    first = bronze_measurement(value=19.0)
    later = bronze_measurement(value=19.6, ingested_at=LATER_INGESTION)
    next_hour = bronze_measurement(datetime_from_utc="2020-09-26T15:00:00Z", datetime_from_local="2020-09-26T11:00:00-04:00")

    readings = clean_hourly(silver, spark, later, first, next_hour)

    assert sorted((r["measured_hour"], r["value"]) for r in readings) == [(10, 19.6), (11, 19.6)]


def test_daily_keeps_summary_without_redundant_columns(silver, spark):
    [day] = clean_daily(silver, spark, bronze_day())

    assert day.asDict() == {
        "sensor_id": 3950,
        "taxi_zone_id": DAILY_SENSOR_ZONE,
        "parameter_name": "o3",
        "parameter_units": "ppm",
        "measured_date": date(2024, 3, 7),
        "avg_value": 0.032667,
        "min_value": 0.02,
        "median_value": 0.03,
        "max_value": 0.047,
        "percent_complete": 96.0,
        "has_flags": False,
        "measured_year": 2024,
        "measured_month": 3,
        "measured_day_of_week": 4,
        "is_weekend": False,
        "ingested_at": FIRST_INGESTION,
    }


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"date_local": None}, id="missing_date"),
        pytest.param({"avg": None}, id="missing_avg"),
        pytest.param({"avg": -1.0}, id="negative_avg"),
        pytest.param({"parameter_units": MICROGRAMS}, id="unexpected_unit"),
    ],
)
def test_invalid_days_are_dropped(silver, spark, overrides):
    assert clean_daily(silver, spark, bronze_day(**overrides)) == []


def test_low_coverage_day_is_kept(silver, spark):
    [day] = clean_daily(silver, spark, bronze_day(observed_count=6, percent_complete=25.0))
    assert day["percent_complete"] == 25.0


def test_daily_duplicate_keeps_latest_ingestion(silver, spark):
    first = bronze_day(avg=0.03)
    later = bronze_day(avg=0.04, ingested_at=LATER_INGESTION)

    [day] = clean_daily(silver, spark, first, later)

    assert (day["avg_value"], day["ingested_at"]) == (0.04, LATER_INGESTION)


@pytest.mark.parametrize(
    ("raw", "selected"),
    [("", ["sensors", "hourly", "daily"]), ("daily, hourly", ["hourly", "daily"]), ("hourly", ["hourly"])],
    ids=["empty_means_all", "fixed_order", "single"],
)
def test_select_tables_keeps_load_order(silver, raw, selected):
    assert silver["select_tables"](raw) == selected


def test_select_tables_rejects_unknown_table(silver):
    with pytest.raises(ValueError, match="weekly"):
        silver["select_tables"]("hourly,weekly")


def test_reading_of_current_hour_is_dropped_as_unfinished(silver, spark):
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    current_hour = bronze_measurement(value=1.0, datetime_from_utc=now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    spec = silver["HOURLY_READINGS"]
    readings = spec.prepare(spark.createDataFrame([current_hour, bronze_measurement()], BRONZE_MEASUREMENT_SCHEMA), sensor_zones(spark))

    rule_hits = silver["count_rule_hits"](readings, spec.rules())

    assert rule_hits["unfinished_period"] == 1
    assert [row.value for row in spec.clean(readings).collect()] == [19.6]


def test_day_still_running_in_new_york_is_dropped_as_unfinished(silver, spark):
    new_york_today = datetime.now(ZoneInfo("America/New_York")).date()
    yesterday = new_york_today - timedelta(days=1)

    readings = clean_daily(silver, spark, bronze_day(date_local=new_york_today), bronze_day(date_local=yesterday))

    assert [reading.measured_date for reading in readings] == [yesterday]


@pytest.mark.parametrize(("spec_name", "period_column"), [("HOURLY_READINGS", "measured_at_utc"), ("DAILY_READINGS", "measured_date")])
def test_append_watermark_is_the_reading_period(silver, spec_name, period_column):
    assert silver[spec_name].period_column == period_column


def test_append_batch_keeps_only_hours_after_latest_hour_of_each_sensor(silver, spark):
    spec = silver["HOURLY_READINGS"]
    loaded_hour = bronze_measurement()
    next_hour = bronze_measurement(datetime_from_utc="2020-09-26T15:00:00Z", datetime_from_local="2020-09-26T11:00:00-04:00")
    loaded = spec.clean(spec.prepare(spark.createDataFrame([loaded_hour], BRONZE_MEASUREMENT_SCHEMA), sensor_zones(spark)))
    readings = spec.prepare(spark.createDataFrame([loaded_hour, next_hour], BRONZE_MEASUREMENT_SCHEMA), sensor_zones(spark))

    batch = silver["rows_after_latest_loaded"](readings, loaded, ("sensor_id",), spec.period_column)

    assert [reading.measured_hour for reading in spec.clean(batch).collect()] == [11]


def test_table_report_counts_rules_and_duplicates(silver, spark):
    spec = silver["HOURLY_READINGS"]
    bronze = spark.createDataFrame([bronze_measurement(), bronze_measurement(), bronze_measurement(value=None)], BRONZE_MEASUREMENT_SCHEMA)
    readings = spec.prepare(bronze, sensor_zones(spark))

    rule_hits = silver["count_rule_hits"](readings, spec.rules())
    report = silver["build_table_report"]("silver_air_quality_hourly", rule_hits, spec.clean(readings).count())

    assert (report.rows_read, report.duplicates_removed, report.rows_written) == (3, 1, 1)
    assert report.rows_dropped_by_rule["missing_value"] == 1
    assert sum(report.rows_dropped_by_rule.values()) == 1


def test_cached_frame_is_released_after_the_load(silver, spark):
    frame = spark.createDataFrame([(1,)], "sensor_id int")

    with silver["cached"](frame) as rows:
        assert rows.is_cached
    assert not frame.is_cached


def test_write_table_counts_rows_then_overwrites(silver, monkeypatch):
    calls = []
    frame = SimpleNamespace(count=lambda: calls.append("count") or 4)
    monkeypatch.setitem(silver, "overwrite_table", lambda session, frame, definition: calls.append("overwrite"))

    assert silver["write_table"](None, frame, None) == 4
    assert calls == ["count", "overwrite"]


def test_sensor_gains_location_without_redundant_columns(silver, spark):
    [sensor] = clean_sensors(silver, spark, [bronze_sensor()], [bronze_location()])

    assert sensor.asDict() == {
        "location_id": 384,
        "sensor_id": 673,
        "parameter_name": "pm25",
        "parameter_units": MICROGRAMS,
        "parameter_display_name": "PM2.5",
        "location_name": "CCNY",
        "provider_name": "AirNow",
        "is_reference_monitor": True,
        "latitude": 40.8197,
        "longitude": -73.9481,
        "first_measured_at_utc": sensor["first_measured_at_utc"],
        "last_measured_at_utc": sensor["last_measured_at_utc"],
        "taxi_zone_id": 166,
    }


def test_location_activity_period_is_parsed_as_utc(silver, spark):
    joined = sensors_with_locations(silver, spark, [bronze_sensor()], [bronze_location()])

    [period] = joined.selectExpr("string(first_measured_at_utc)", "string(last_measured_at_utc)").collect()

    assert tuple(period) == ("2016-03-12 09:00:00", "2026-11-12 02:00:00")


@pytest.mark.parametrize(
    "sensor_overrides, location_overrides",
    [
        pytest.param({"location_id": 999}, {}, id="unknown_location"),
        pytest.param({}, {"latitude": None}, id="missing_coordinates"),
        pytest.param({"sensor_id": None}, {}, id="missing_sensor"),
        pytest.param({"parameter_name": "temperature", "parameter_units": "c"}, {}, id="unsupported_parameter"),
        pytest.param({}, {"latitude": 40.7, "longitude": -74.1}, id="outside_taxi_zones"),
    ],
)
def test_invalid_sensors_are_dropped(silver, spark, sensor_overrides, location_overrides):
    assert clean_sensors(silver, spark, [bronze_sensor(**sensor_overrides)], [bronze_location(**location_overrides)]) == []


def test_low_cost_sensor_is_kept_and_marked(silver, spark):
    location = bronze_location(location_id=2903996, provider_name="AirGradient", is_monitor=False)

    [sensor] = clean_sensors(silver, spark, [bronze_sensor(location_id=2903996)], [location])

    assert (sensor["provider_name"], sensor["is_reference_monitor"]) == ("AirGradient", False)


def test_sensor_and_location_duplicates_keep_latest_ingestion(silver, spark):
    sensors = [bronze_sensor(), bronze_sensor(parameter_display_name="PM 2.5", ingested_at=LATER_INGESTION)]
    locations = [bronze_location(), bronze_location(location_name="CCNY Harlem", ingested_at=LATER_INGESTION)]

    [sensor] = clean_sensors(silver, spark, sensors, locations)

    assert (sensor["parameter_display_name"], sensor["location_name"]) == ("PM 2.5", "CCNY Harlem")


def test_sensor_report_counts_rules_and_duplicates(silver, spark):
    sensors = [bronze_sensor(), bronze_sensor(), bronze_sensor(sensor_id=674, location_id=999)]
    located = sensors_with_zones(silver, spark, sensors, [bronze_location()])
    rules = silver["invalid_sensor_rules"]()

    rule_hits = silver["count_rule_hits"](located, rules)
    report = silver["build_table_report"]("silver_air_quality_sensors", rule_hits, silver["clean_sensors"](located, rules).count())

    assert (report.rows_read, report.duplicates_removed, report.rows_written) == (3, 1, 1)
    assert report.rows_dropped_by_rule == {
        "missing_key": 0,
        "missing_location": 1,
        "unsupported_parameter_unit": 0,
        "outside_taxi_zones": 1,
    }


@pytest.mark.parametrize(
    ("longitude", "latitude", "taxi_zone_id"),
    [(-73.9481, 40.8197, 166), (-73.95, 40.85, None), (-73.5, 40.85, None)],
    ids=["inside", "in_hole", "outside"],
)
def test_location_is_placed_in_the_taxi_zone_containing_it(silver, longitude, latitude, taxi_zone_id):
    assert silver["locate_taxi_zone"](longitude, latitude, {166: [HARLEM_SQUARE_WITH_HOLE]}) == taxi_zone_id


def test_every_sensor_gains_the_taxi_zone_of_its_location(silver, spark):
    sensors = [
        bronze_sensor(),
        bronze_sensor(sensor_id=674),
        bronze_sensor(sensor_id=675, location_id=385),
        bronze_sensor(sensor_id=676, location_id=999),
    ]
    locations = [bronze_location(), bronze_location(location_id=385, latitude=40.7, longitude=-74.1)]

    located = sensors_with_zones(silver, spark, sensors, locations)

    assert {row.sensor_id: row.taxi_zone_id for row in located.collect()} == {673: 166, 674: 166, 675: None, 676: None}


def test_new_sensors_skip_sensors_already_in_silver(silver, spark):
    sensors = sensors_with_locations(silver, spark, [bronze_sensor(), bronze_sensor(sensor_id=674)], [bronze_location()])
    existing = spark.createDataFrame([(673,)], "sensor_id int")

    assert [row.sensor_id for row in silver["new_sensors"](sensors, existing).collect()] == [674]


def test_sensor_table_definition_matches_cleaned_sensors(silver, spark):
    sensors = sensors_with_locations(silver, spark, [bronze_sensor()], [bronze_location()])
    cleaned = silver["clean_sensors"](silver["add_taxi_zones"](spark, sensors, harlem_taxi_zones(spark)), silver["invalid_sensor_rules"]())

    definition = silver["sensor_table_definition"](spark, "silver_sensors", sensors)

    assert definition == silver["TableDefinition"].from_frame("silver_sensors", cleaned, text_types=silver["SENSOR_TEXT_TYPES"])


def test_sensor_table_schema_uses_narrow_types(silver, spark):
    sensors = sensors_with_locations(silver, spark, [bronze_sensor()], [bronze_location()])

    definition = silver["sensor_table_definition"](spark, "silver_sensors", sensors)

    assert definition.columns == (
        "location_id int, sensor_id int, parameter_name VARCHAR(5), parameter_units VARCHAR(15), "
        "parameter_display_name VARCHAR(15), location_name string, provider_name string, is_reference_monitor boolean, "
        "latitude double, longitude double, first_measured_at_utc timestamp, last_measured_at_utc timestamp, "
        "taxi_zone_id smallint"
    )
