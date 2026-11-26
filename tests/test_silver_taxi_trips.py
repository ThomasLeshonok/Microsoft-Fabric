import os
import sys
from datetime import datetime

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(1, -1)
JANUARY_2024_FILE = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/trips/nyctaxi_yellow/2024/01/yellow_tripdata_2024-01.parquet"
RAW_2024_SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, passenger_count bigint, "
    "trip_distance double, RatecodeID bigint, store_and_fwd_flag string, PULocationID int, DOLocationID int, "
    "payment_type bigint, fare_amount double, extra double, mta_tax double, tip_amount double, tolls_amount double, "
    "improvement_surcharge double, congestion_surcharge double, Airport_fee double, total_amount double"
)
AMOUNTS = {
    "fare_amount": 17.7,
    "extra": 1.0,
    "mta_tax": 0.5,
    "tip_amount": 4.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "congestion_surcharge": 2.5,
    "Airport_fee": 0.0,
    "total_amount": 26.7,
}


@pytest.fixture(scope="module")
def silver():
    return load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_taxi_trips", DEFINITION_CELLS))


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


def raw_trip(**overrides):
    trip = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 20),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,
        "DOLocationID": 237,
        "payment_type": 1,
        **AMOUNTS,
    }
    return {**trip, **overrides}


def negated_amounts():
    return {name: -value for name, value in AMOUNTS.items()}


def clean(silver, spark, *raw_trips):
    trip_file = silver["TripFile"].from_path(JANUARY_2024_FILE)
    trips = silver["normalize_columns"](spark.createDataFrame(list(raw_trips), RAW_2024_SCHEMA))
    rules = silver["invalid_trip_rules"](trip_file)
    return silver["clean_trips"](trips, rules).collect()


def test_trip_file_reads_month_from_name(silver):
    trip_file = silver["TripFile"].from_path(JANUARY_2024_FILE)
    assert (trip_file.year, trip_file.month, trip_file.month_label) == (2024, 1, "2024-01")


def test_normalize_maps_every_year_to_one_schema(silver, spark):
    raw = spark.createDataFrame([raw_trip()], RAW_2024_SCHEMA)

    normalized = silver["normalize_columns"](raw)

    expected = [(spec.target, spec.data_type) for spec in silver["TRIP_COLUMNS"]]
    assert normalized.dtypes == expected
    assert "airport_fee" not in normalized.columns


def test_normalize_turns_values_out_of_narrow_range_into_null(silver, spark):
    raw = spark.createDataFrame([raw_trip(passenger_count=200, PULocationID=40000)], RAW_2024_SCHEMA)

    [trip] = silver["normalize_columns"](raw).collect()

    assert (trip["passenger_count"], trip["pickup_location_id"], trip["dropoff_location_id"]) == (None, None, 237)


def test_table_schema_uses_narrow_codes_partitioned_by_pickup_month(silver, spark):
    trips = silver["normalize_columns"](spark.createDataFrame([raw_trip()], RAW_2024_SCHEMA))
    cleaned = silver["clean_trips"](trips, silver["invalid_trip_rules"](silver["TripFile"].from_path(JANUARY_2024_FILE)))

    definition = silver["TableDefinition"].from_frame("silver_taxi_trips", cleaned, silver["PARTITION_COLUMNS"])

    assert definition.columns == (
        "vendor_id tinyint, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count tinyint, "
        "trip_distance double, rate_code_id tinyint, pickup_location_id smallint, dropoff_location_id smallint, "
        "payment_type tinyint, fare_amount double, total_amount double, pickup_date date, pickup_year smallint, "
        "pickup_month tinyint, pickup_hour tinyint, pickup_day_of_week tinyint, is_weekend boolean, "
        "trip_duration_minutes double"
    )
    assert definition.partition_columns == ("pickup_year", "pickup_month")


def test_valid_trip_keeps_values_and_gains_time_attributes(silver, spark):
    [trip] = clean(silver, spark, raw_trip())

    assert trip["pickup_location_id"] == 161
    assert trip["total_amount"] == 26.7
    assert trip["pickup_date"].isoformat() == "2024-01-15"
    assert (trip["pickup_year"], trip["pickup_month"], trip["pickup_hour"]) == (2024, 1, 8)
    assert (trip["pickup_day_of_week"], trip["is_weekend"]) == (1, False)
    assert trip["trip_duration_minutes"] == 20.0
    assert "store_and_fwd_flag" not in trip.asDict()


def test_weekend_pickup_is_flagged(silver, spark):
    [trip] = clean(silver, spark, raw_trip(tpep_pickup_datetime=datetime(2024, 1, 13, 23, 50), tpep_dropoff_datetime=datetime(2024, 1, 14, 0, 5)))
    assert (trip["pickup_day_of_week"], trip["is_weekend"]) == (6, True)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"tpep_pickup_datetime": None}, id="missing_timestamp"),
        pytest.param({"PULocationID": 264, "DOLocationID": 264}, id="unknown_pickup_and_dropoff_zone"),
        pytest.param({"PULocationID": 265, "DOLocationID": 265}, id="outside_nyc_pickup_and_dropoff_zone"),
        pytest.param({"PULocationID": 264, "DOLocationID": 265}, id="unknown_pickup_outside_nyc_dropoff"),
        pytest.param({"payment_type": 0, "trip_distance": 1500.0}, id="unrealistic_flex_fare_distance"),
        pytest.param(
            {"tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 0), "DOLocationID": 161, "trip_distance": 0.0},
            id="stationary_trip",
        ),
        pytest.param(
            {"tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 0), "DOLocationID": 161, "trip_distance": 0.0, "store_and_fwd_flag": None},
            id="stationary_trip_without_flag",
        ),
        pytest.param({"trip_distance": 0.0, **{name: 0.0 for name in AMOUNTS}}, id="zero_distance_and_amount"),
        pytest.param({"payment_type": 6}, id="voided_payment"),
        pytest.param(
            {"tpep_pickup_datetime": datetime(2023, 12, 31, 23, 0), "tpep_dropoff_datetime": datetime(2023, 12, 31, 23, 20)},
            id="pickup_outside_file_year",
        ),
        pytest.param(
            {"tpep_pickup_datetime": datetime(2024, 2, 1, 8, 0), "tpep_dropoff_datetime": datetime(2024, 2, 1, 8, 20)},
            id="pickup_outside_file_month",
        ),
        pytest.param({"tpep_dropoff_datetime": datetime(2088, 1, 15, 8, 20)}, id="dropoff_outside_file_year"),
        pytest.param({"tpep_dropoff_datetime": datetime(2024, 1, 15, 7, 50)}, id="dropoff_before_pickup"),
    ],
)
def test_invalid_trips_are_dropped(silver, spark, overrides):
    assert clean(silver, spark, raw_trip(**overrides)) == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"DOLocationID": 161}, id="same_zone_round_trip"),
        pytest.param({"trip_distance": 0.0}, id="zero_distance_with_fare"),
        pytest.param({"PULocationID": 264}, id="only_pickup_zone_unknown"),
        pytest.param({"DOLocationID": 265}, id="only_dropoff_zone_outside_nyc"),
        pytest.param({"payment_type": 0, "trip_distance": 100.0}, id="flex_fare_at_distance_limit"),
        pytest.param({"payment_type": 1, "trip_distance": 1500.0}, id="long_distance_card_payment"),
        pytest.param(
            {"tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 0), "DOLocationID": 161, "trip_distance": 0.0, "store_and_fwd_flag": "Y"},
            id="stationary_trip_stored_and_forwarded",
        ),
        pytest.param({"payment_type": 3}, id="no_charge_payment"),
    ],
)
def test_suspicious_but_valid_trips_are_kept(silver, spark, overrides):
    assert len(clean(silver, spark, raw_trip(**overrides))) == 1


def test_refund_reversal_collapses_into_original_trip(silver, spark):
    original = raw_trip(payment_type=1)
    reversal = raw_trip(payment_type=4, **negated_amounts())

    [trip] = clean(silver, spark, reversal, original)

    assert (trip["payment_type"], trip["fare_amount"], trip["total_amount"]) == (1, 17.7, 26.7)


def test_lone_negative_trip_becomes_positive(silver, spark):
    [trip] = clean(silver, spark, raw_trip(payment_type=4, **negated_amounts()))
    assert (trip["fare_amount"], trip["total_amount"]) == (17.7, 26.7)


def test_exact_duplicates_are_removed_but_distinct_trips_stay(silver, spark):
    later_trip = raw_trip(tpep_pickup_datetime=datetime(2024, 1, 15, 9, 0), tpep_dropoff_datetime=datetime(2024, 1, 15, 9, 20))
    assert len(clean(silver, spark, raw_trip(), raw_trip(), later_trip)) == 2


def test_month_report_counts_rules_and_duplicates(silver, spark):
    trip_file = silver["TripFile"].from_path(JANUARY_2024_FILE)
    raw = spark.createDataFrame([raw_trip(), raw_trip(), raw_trip(payment_type=6)], RAW_2024_SCHEMA)
    trips = silver["normalize_columns"](raw)
    rules = silver["invalid_trip_rules"](trip_file)

    rule_hits = silver["count_rule_hits"](trips, rules)
    report = silver["build_month_report"](trip_file, rule_hits, silver["clean_trips"](trips, rules).count())

    assert (report.rows_read, report.duplicates_removed, report.rows_written) == (3, 1, 1)
    assert report.rows_dropped_by_rule["voided_payment"] == 1
    assert sum(report.rows_dropped_by_rule.values()) == 1


def test_select_trip_files_keeps_requested_months(silver):
    trip_file = silver["TripFile"].from_path
    files = [trip_file(JANUARY_2024_FILE), trip_file(JANUARY_2024_FILE.replace("01", "02"))]

    assert silver["select_trip_files"](files, "") == files
    assert [f.month_label for f in silver["select_trip_files"](files, " 2024-02 ")] == ["2024-02"]


def test_select_trip_files_fails_without_bronze_files(silver):
    with pytest.raises(FileNotFoundError):
        silver["select_trip_files"]([], "")


def test_select_trip_files_fails_on_unknown_month(silver):
    with pytest.raises(FileNotFoundError, match="2024-03"):
        silver["select_trip_files"]([silver["TripFile"].from_path(JANUARY_2024_FILE)], "2024-03")


def test_load_months_writes_first_month_alone_then_keeps_order():
    silver = load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_taxi_trips", DEFINITION_CELLS))
    trip_file = silver["TripFile"].from_path
    files = [trip_file(JANUARY_2024_FILE.replace("01", month)) for month in ("01", "02", "03", "04")]
    loaded = []

    def fake_load_month(session, trip_file, table, write):
        loaded.append(trip_file.month_label)
        return trip_file.month_label

    silver["load_month"] = fake_load_month

    reports = silver["load_months"](None, files, "silver_taxi_trips", 2, silver["overwrite_month"])

    assert loaded[0] == "2024-01"
    assert sorted(loaded) == reports == ["2024-01", "2024-02", "2024-03", "2024-04"]


@pytest.mark.parametrize(
    ("is_current", "expected_months", "expected_replaced"),
    [
        pytest.param(True, ["2024-02"], [], id="current_schema_loads_requested_months"),
        pytest.param(False, ["2024-01", "2024-02"], ["silver_taxi_trips"], id="outdated_schema_rebuilds_every_month"),
    ],
)
def test_prepare_table_rebuilds_outdated_table_from_every_month(is_current, expected_months, expected_replaced):
    silver = load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_taxi_trips", DEFINITION_CELLS))
    files = [silver["TripFile"].from_path(JANUARY_2024_FILE.replace("01", month)) for month in ("01", "02")]
    replaced = []
    silver["table_definition"] = lambda session, trip_file, table: silver["TableDefinition"](table, "pickup_year smallint")
    silver["is_current"] = lambda session, definition: is_current
    silver["replace_table"] = lambda session, definition: replaced.append(definition.name)

    trip_files = silver["prepare_table_and_select_files"](None, files, "2024-02", "overwrite_months", "silver_taxi_trips")

    assert [trip_file.month_label for trip_file in trip_files] == expected_months
    assert replaced == expected_replaced


def test_load_months_without_missing_month_loads_nothing(silver):
    assert silver["load_months"](None, [], "silver_taxi_trips", 2, silver["append_month"]) == []


@pytest.mark.parametrize(
    ("latest_loaded_month", "expected_months"),
    [
        pytest.param(None, ["2024-01", "2024-02", "2024-03"], id="empty_table_loads_every_month"),
        pytest.param("2024-01", ["2024-02", "2024-03"], id="months_after_latest"),
        pytest.param("2024-03", [], id="nothing_missing"),
    ],
)
def test_select_missing_trip_files_keeps_months_after_latest_loaded(silver, latest_loaded_month, expected_months):
    files = [silver["TripFile"].from_path(JANUARY_2024_FILE.replace("01", month)) for month in ("01", "02", "03")]

    missing = silver["select_missing_trip_files"](files, latest_loaded_month)

    assert [trip_file.month_label for trip_file in missing] == expected_months


@pytest.mark.parametrize(("load_mode", "writer"), [("append_missing", "append_month"), ("overwrite_months", "overwrite_month"), ("rebuild", "overwrite_month")])
def test_only_append_missing_appends_months(silver, load_mode, writer):
    assert silver["month_writer"](load_mode) is silver[writer]


@pytest.mark.parametrize(
    ("load_mode", "is_append", "expected_months", "expected_replaced"),
    [
        pytest.param("append_missing", True, ["2024-02"], [], id="append_loads_months_after_latest"),
        pytest.param("append_missing", False, ["2024-01", "2024-02"], ["silver_taxi_trips"], id="append_to_missing_table_loads_all"),
        pytest.param("rebuild", False, ["2024-01", "2024-02"], ["silver_taxi_trips"], id="rebuild_recreates_and_loads_all"),
    ],
)
def test_prepare_table_for_append_or_rebuild(load_mode, is_append, expected_months, expected_replaced):
    silver = load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_taxi_trips", DEFINITION_CELLS))
    files = [silver["TripFile"].from_path(JANUARY_2024_FILE.replace("01", month)) for month in ("01", "02")]
    replaced = []
    silver["table_definition"] = lambda session, trip_file, table: silver["TableDefinition"](table, "pickup_year smallint")
    silver["appends_to_current_table"] = lambda session, definition, mode: is_append
    silver["latest_loaded_month"] = lambda session, table: "2024-01"
    silver["replace_table"] = lambda session, definition: replaced.append(definition.name)

    trip_files = silver["prepare_table_and_select_files"](None, files, "", load_mode, "silver_taxi_trips")

    assert [trip_file.month_label for trip_file in trip_files] == expected_months
    assert replaced == expected_replaced


@pytest.mark.parametrize(("load_mode", "error"), [("append_missing", "months applies only"), ("merge", "Unknown load_mode")])
def test_prepare_table_rejects_months_outside_overwrite_and_unknown_modes(silver, load_mode, error):
    files = [silver["TripFile"].from_path(JANUARY_2024_FILE)]

    with pytest.raises(ValueError, match=error):
        silver["prepare_table_and_select_files"](None, files, "2024-01", load_mode, "silver_taxi_trips")


def test_latest_loaded_month_reads_highest_partition(silver, spark):
    spark.createDataFrame([(2024, 12), (2025, 2), (2025, 1)], "pickup_year smallint, pickup_month tinyint").createOrReplaceTempView("loaded_trips")

    assert silver["latest_loaded_month"](spark, "loaded_trips") == "2025-02"
