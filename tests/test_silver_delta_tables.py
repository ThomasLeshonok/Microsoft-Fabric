import os
import sys
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
TRIPS_LAYOUT = "(pickup_year smallint, pickup_month tinyint, zone VARCHAR(15)) PARTITIONED BY (pickup_year, pickup_month)"


@pytest.fixture(scope="module")
def tables():
    return load(("nb_silver_delta_tables", ALL_CELLS))


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield session
    session.stop()


def trips_definition(tables, spark):
    frame = spark.createDataFrame([], "pickup_year smallint, pickup_month tinyint, zone string")
    return tables["TableDefinition"].from_frame("silver_trips", frame, ("pickup_year", "pickup_month"), {"zone": "VARCHAR(15)"})


class FakeSession:
    def __init__(self, existing_tables: dict):
        self.existing_tables = existing_tables
        self.statements = []
        self.catalog = SimpleNamespace(tableExists=lambda name: name in existing_tables)

    def sql(self, statement):
        self.statements.append(statement)
        properties = self.existing_tables.get(statement.removeprefix("SHOW TBLPROPERTIES "), {})
        return SimpleNamespace(collect=lambda: [SimpleNamespace(key=key, value=value) for key, value in properties.items()])


def test_definition_keeps_integer_widths_and_applies_text_types(tables, spark):
    definition = trips_definition(tables, spark)

    assert definition.columns == "pickup_year smallint, pickup_month tinyint, zone VARCHAR(15)"
    assert definition.layout == TRIPS_LAYOUT


def test_definition_rejects_text_type_for_missing_column(tables, spark):
    frame = spark.createDataFrame([], "zone string")

    with pytest.raises(ValueError, match="borough"):
        tables["TableDefinition"].from_frame("silver_zones", frame, text_types={"borough": "VARCHAR(15)"})


def test_replace_statement_creates_partitioned_delta_table_with_schema_property(tables, spark):
    statement = trips_definition(tables, spark).replace_statement()

    assert statement == (
        "CREATE OR REPLACE TABLE silver_trips (pickup_year smallint, pickup_month tinyint, zone VARCHAR(15)) "
        "USING DELTA PARTITIONED BY (pickup_year, pickup_month) "
        f"TBLPROPERTIES ('silver.schema' = '{TRIPS_LAYOUT}')"
    )
    spark._jsparkSession.sessionState().sqlParser().parsePlan(statement)


def test_unpartitioned_statement_has_no_partition_clause(tables, spark):
    frame = spark.createDataFrame([], "base_currency string")
    definition = tables["TableDefinition"].from_frame("silver_rates", frame, text_types={"base_currency": "CHAR(3)"})

    assert definition.replace_statement() == (
        "CREATE OR REPLACE TABLE silver_rates (base_currency CHAR(3)) USING DELTA "
        "TBLPROPERTIES ('silver.schema' = '(base_currency CHAR(3))')"
    )


@pytest.mark.parametrize(
    ("existing_tables", "expected"),
    [
        pytest.param({}, False, id="missing_table"),
        pytest.param({"silver_trips": {}}, False, id="written_before_schema_property"),
        pytest.param({"silver_trips": {"silver.schema": "(pickup_year int)"}}, False, id="other_schema"),
        pytest.param({"silver_trips": {"silver.schema": TRIPS_LAYOUT}}, True, id="same_schema"),
    ],
)
def test_is_current_compares_schema_property(tables, spark, existing_tables, expected):
    assert tables["is_current"](FakeSession(existing_tables), trips_definition(tables, spark)) is expected


def test_overwrite_table_replaces_table_then_appends_by_name(tables, spark):
    session = FakeSession({})
    appended = []
    frame = SimpleNamespace(writeTo=lambda name: SimpleNamespace(append=lambda: appended.append(name)))

    tables["overwrite_table"](session, frame, trips_definition(tables, spark))

    assert session.statements[0].startswith("CREATE OR REPLACE TABLE silver_trips ")
    assert appended == ["silver_trips"]


def test_require_load_mode_rejects_unknown_mode(tables):
    with pytest.raises(ValueError, match="merge"):
        tables["require_load_mode"]("merge")


@pytest.mark.parametrize(
    ("load_mode", "existing_tables", "expected"),
    [
        pytest.param("rebuild", {"silver_trips": {"silver.schema": TRIPS_LAYOUT}}, False, id="rebuild_current_table"),
        pytest.param("append_missing", {}, False, id="append_to_missing_table_loads_everything"),
        pytest.param("append_missing", {"silver_trips": {"silver.schema": TRIPS_LAYOUT}}, True, id="append_to_current_table"),
    ],
)
def test_appends_only_to_existing_current_table(tables, spark, load_mode, existing_tables, expected):
    definition = trips_definition(tables, spark)

    assert tables["appends_to_current_table"](FakeSession(existing_tables), definition, load_mode) is expected


def test_append_to_outdated_table_fails_instead_of_rebuilding(tables, spark):
    session = FakeSession({"silver_trips": {"silver.schema": "(pickup_year int)"}})

    with pytest.raises(tables["OutdatedTableError"], match="rebuild"):
        tables["appends_to_current_table"](session, trips_definition(tables, spark), "append_missing")


def test_rows_after_latest_loaded_keeps_newer_periods_per_key_and_new_keys(tables, spark):
    schema = "sensor_id int, measured_date date, value double"
    loaded = spark.createDataFrame([(1, date(2026, 11, 10), 1.0), (1, date(2026, 11, 12), 1.0), (2, date(2026, 11, 8), 2.0)], schema)
    rows = spark.createDataFrame(
        [
            (1, date(2026, 11, 12), 9.0),
            (1, date(2026, 11, 13), 9.0),
            (2, date(2026, 11, 9), 9.0),
            (3, date(2026, 11, 1), 9.0),
            (None, None, 9.0),
        ],
        schema,
    )

    newer = tables["rows_after_latest_loaded"](rows, loaded, ("sensor_id",), "measured_date")

    assert newer.columns == rows.columns
    assert {tuple(row) for row in newer.collect()} == {
        (1, date(2026, 11, 13), 9.0),
        (2, date(2026, 11, 9), 9.0),
        (3, date(2026, 11, 1), 9.0),
        (None, None, 9.0),
    }


def test_append_rows_counts_then_appends(tables):
    appended = []
    frame = SimpleNamespace(count=lambda: 3, writeTo=lambda name: SimpleNamespace(append=lambda: appended.append(name)))

    assert tables["append_rows"](frame, "silver_trips") == 3
    assert appended == ["silver_trips"]


@pytest.mark.parametrize(
    ("load_mode", "statements"),
    [
        ("rebuild", ["VACUUM silver_trips RETAIN 168 HOURS"]),
        ("overwrite_months", ["VACUUM silver_trips RETAIN 168 HOURS"]),
        ("append_missing", []),
    ],
)
def test_vacuum_after_load_skips_appends(tables, load_mode, statements):
    executed = []
    session = SimpleNamespace(sql=executed.append)

    tables["vacuum_after_load"](session, "silver_trips", 168, load_mode)

    assert executed == statements


def value_rules():
    from pyspark.sql import functions as F

    conditions = {"missing_value": F.col("value").isNull(), "negative_value": F.col("value") < 0}
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def test_rule_hits_count_rows_read_valid_rows_and_each_rule(tables, spark):
    frame = spark.createDataFrame([(1.0,), (None,), (-1.0,), (2.0,)], "value double")
    rules = value_rules()

    rule_hits = tables["count_rule_hits"](frame, rules)

    assert rule_hits == {"rows_read": 4, "rows_valid": 2, "missing_value": 1, "negative_value": 1}
    assert tables["rows_dropped_by_rule"](rule_hits) == {"missing_value": 1, "negative_value": 1}
    assert [row.value for row in frame.filter(~tables["is_invalid"](rules)).collect()] == [1.0, 2.0]


def test_table_report_counts_valid_rows_not_written_as_duplicates(tables):
    report = tables["build_table_report"]("silver_readings", {"rows_read": 5, "rows_valid": 4, "missing_value": 1}, 3)

    assert report == tables["TableReport"]("silver_readings", 5, {"missing_value": 1}, 1, 3)


def test_keep_latest_ingestion_keeps_newest_row_per_key(tables, spark):
    frame = spark.createDataFrame(
        [("A", 1.0, datetime(2026, 11, 16)), ("A", 2.0, datetime(2026, 11, 17)), ("B", 3.0, datetime(2026, 11, 16))],
        "station string, value double, ingested_at timestamp",
    )

    latest = tables["keep_latest_ingestion"](frame, ("station",))

    assert latest.columns == frame.columns
    assert sorted((row.station, row.value) for row in latest.collect()) == [("A", 2.0), ("B", 3.0)]


READINGS_SCHEMA = "station string, day date, value double, ingested_at timestamp"
FIRST_INGESTION = datetime(2026, 11, 16)
LATER_INGESTION = datetime(2026, 11, 17)


def readings_dataset(tables):
    from pyspark.sql import functions as F

    def normalize(bronze):
        return bronze.select(F.trim("station").alias("station"), "day", "value", "ingested_at")

    def clean(readings, rules):
        valid = readings.filter(~tables["is_invalid"](rules))
        return tables["keep_latest_ingestion"](valid, ("station", "day")).drop("ingested_at")

    return tables["SilverDataset"](
        bronze_table="bronze_readings",
        silver_table="silver_readings",
        normalize=normalize,
        rules=value_rules,
        clean=clean,
        series_key=("station",),
        period_column="day",
        text_types={"station": "CHAR(3)"},
    )


class DatasetSession(FakeSession):
    def __init__(self, existing_tables, bronze, silver=None):
        super().__init__(existing_tables)
        self.read = SimpleNamespace(format=lambda source: SimpleNamespace(load=lambda path: bronze))
        self.silver = silver

    def table(self, name):
        return self.silver


def record_writes(tables, monkeypatch):
    writes = {}

    def append_rows(frame, table):
        writes["append"] = (table, {tuple(row) for row in frame.collect()})
        return frame.count()

    def overwrite_rows(session, frame, definition):
        writes["overwrite"] = (definition, {tuple(row) for row in frame.collect()})
        return frame.count()

    monkeypatch.setitem(tables, "append_rows", append_rows)
    monkeypatch.setitem(tables, "overwrite_rows", overwrite_rows)
    return writes


def test_load_dataset_rebuild_overwrites_with_clean_rows_and_reports_rules(tables, spark, monkeypatch):
    bronze = spark.createDataFrame(
        [
            (" ABC ", date(2026, 11, 1), 1.0, FIRST_INGESTION),
            ("ABC", date(2026, 11, 1), 2.0, LATER_INGESTION),
            ("ABC", date(2026, 11, 2), None, FIRST_INGESTION),
            ("XYZ", date(2026, 11, 1), 3.0, FIRST_INGESTION),
        ],
        READINGS_SCHEMA,
    )
    writes = record_writes(tables, monkeypatch)

    report = tables["load_dataset"](DatasetSession({}, bronze), readings_dataset(tables), "bronze-path", "rebuild")

    definition, written = writes["overwrite"]
    assert definition.columns == "station CHAR(3), day date, value double"
    assert written == {("ABC", date(2026, 11, 1), 2.0), ("XYZ", date(2026, 11, 1), 3.0)}
    assert report == tables["TableReport"]("silver_readings", 4, {"missing_value": 1, "negative_value": 0}, 1, 2)


def test_load_dataset_append_missing_appends_only_periods_after_latest_per_series(tables, spark, monkeypatch):
    dataset = readings_dataset(tables)
    silver = spark.createDataFrame([("ABC", date(2026, 11, 2), 2.0)], "station string, day date, value double")
    bronze = spark.createDataFrame(
        [
            ("ABC", date(2026, 11, 2), 9.0, LATER_INGESTION),
            ("ABC", date(2026, 11, 3), 4.0, FIRST_INGESTION),
            ("XYZ", date(2026, 11, 1), 5.0, FIRST_INGESTION),
        ],
        READINGS_SCHEMA,
    )
    layout = tables["TableDefinition"].from_frame("silver_readings", silver, text_types=dataset.text_types).layout
    session = DatasetSession({"silver_readings": {"silver.schema": layout}}, bronze, silver)
    writes = record_writes(tables, monkeypatch)

    report = tables["load_dataset"](session, dataset, "bronze-path", "append_missing")

    assert writes == {"append": ("silver_readings", {("ABC", date(2026, 11, 3), 4.0), ("XYZ", date(2026, 11, 1), 5.0)})}
    assert (report.rows_read, report.duplicates_removed, report.rows_written) == (2, 0, 2)


def test_overwrite_rows_replaces_table_then_counts_it(tables, spark):
    session = FakeSession({})
    session.table = lambda name: SimpleNamespace(count=lambda: 5)
    frame = SimpleNamespace(writeTo=lambda name: SimpleNamespace(append=lambda: None))

    assert tables["overwrite_rows"](session, frame, trips_definition(tables, spark)) == 5
    assert session.statements[0].startswith("CREATE OR REPLACE TABLE silver_trips ")


def test_bronze_table_path_points_at_lakehouse_tables_folder(tables, monkeypatch):
    runtime = SimpleNamespace(context={"currentWorkspaceId": "workspace-id"})
    monkeypatch.setitem(tables, "notebookutils", SimpleNamespace(runtime=runtime, lakehouse=SimpleNamespace(get=lambda name: {"id": f"{name}-id"})))

    assert tables["bronze_table_path"]("lh_bronze", "bronze_readings") == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lh_bronze-id/Tables/bronze_readings"
    )
