import importlib.util
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pytest
from notebook_loader import WORKSPACE, item_folder
from test_gold_star_schema import RATES_SCHEMA, ZONES_SCHEMA, gold, gold_inputs, spark

BUILD_REPORT_SCRIPT = WORKSPACE.parent / "scripts" / "build_report.py"
MODEL_DEFINITION = item_folder("sm_gold.SemanticModel") / "definition"
REPORT_FOLDER = item_folder("rpt_nyc_insights.Report")
TMDL_DATA_TYPES = {
    "short": "int64",
    "integer": "int64",
    "long": "int64",
    "double": "double",
    "decimal": "decimal",
    "string": "string",
    "date": "dateTime",
    "boolean": "boolean",
}
TABLE_COLUMN_REFERENCE = re.compile(r"(\w+)\[([^\]]+)\]")
MEASURE_REFERENCE = re.compile(r"(?<![\w\]])\[([^\]@]+)\]")


@dataclass
class ModelTable:
    columns: dict = field(default_factory=dict)
    measures: dict = field(default_factory=dict)
    entity: str = ""


def unquoted(name: str) -> str:
    return name.strip().strip("'")


def parse_table(text: str) -> tuple:
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("///")]
    name, table = unquoted(lines[0].removeprefix("table ")), ModelTable()
    current_column, current_measure = None, None
    for line in lines[1:]:
        stripped = line.strip()
        if line.startswith("\tmeasure "):
            measure_name, _, expression = line.removeprefix("\tmeasure ").partition("=")
            current_measure, current_column = unquoted(measure_name), None
            table.measures[current_measure] = expression
        elif line.startswith("\tcolumn "):
            current_column, current_measure = unquoted(line.removeprefix("\tcolumn ")), None
            table.columns[current_column] = {}
        elif line.startswith("\t") and not line.startswith("\t\t"):
            current_column, current_measure = None, None
        elif line.startswith("\t\t\t") and current_measure:
            table.measures[current_measure] += "\n" + line
        elif line.startswith("\t\t") and not line.startswith("\t\t\t"):
            current_measure = None
            if current_column and ": " in stripped:
                key, _, value = stripped.partition(": ")
                table.columns[current_column][key] = value
        if stripped.startswith("entityName: "):
            table.entity = stripped.removeprefix("entityName: ")
    return name, table


@pytest.fixture(scope="module")
def model():
    return dict(parse_table(path.read_text(encoding="utf-8")) for path in (MODEL_DEFINITION / "tables").glob("*.tmdl"))


@pytest.fixture(scope="module")
def report_builder():
    spec = importlib.util.spec_from_file_location("build_report", BUILD_REPORT_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gold_tables(gold, spark):
    calendar = (date(2026, 1, 1), date(2026, 1, 2))
    inputs = gold_inputs(gold, spark, calendar, spark.createDataFrame([], ZONES_SCHEMA), spark.createDataFrame([], RATES_SCHEMA))
    return gold["build_tables"](gold["GOLD_TABLES"], inputs)


def report_fields(report_builder) -> list:
    return [
        reference
        for page in report_builder.PAGES
        for visual in page.visuals
        for references in visual.roles.values()
        for reference in references
    ]


def visual_fields(visual) -> list:
    return [reference for references in visual.roles.values() for reference in references]


def overlaps(first, second) -> bool:
    (x1, y1, w1, h1), (x2, y2, w2, h2) = first.position, second.position
    return x1 < x2 + w2 and x2 < x1 + w1 and y1 < y2 + h2 and y2 < y1 + h1


def test_committed_report_matches_generator(report_builder):
    expected = {
        relative_path: report_builder.render(document)
        for relative_path, document in report_builder.report_files(report_builder.PAGES).items()
    }
    committed = {
        path.relative_to(REPORT_FOLDER).as_posix(): path.read_text(encoding="utf-8")
        for path in REPORT_FOLDER.rglob("*")
        if path.is_file() and path.relative_to(REPORT_FOLDER).parts[0] in {"definition", "definition.pbir"}
    }

    assert committed == expected


def test_report_has_the_phase_4_dashboards_and_time_of_day(report_builder):
    assert [page.display_name for page in report_builder.PAGES] == [
        "Mobility",
        "Air Quality",
        "Mobility vs Air Quality",
        "Time of Day",
        "NO2",
        "Economic Impact",
    ]


def test_visual_names_are_unique_per_page(report_builder):
    for page in report_builder.PAGES:
        names = [visual.name for visual in page.visuals]
        assert len(names) == len(set(names)), page.name


def test_visuals_fit_on_page(report_builder):
    for page in report_builder.PAGES:
        for visual in page.visuals:
            x, y, width, height = visual.position
            assert x + width <= report_builder.PAGE_WIDTH and y + height <= report_builder.PAGE_HEIGHT, visual.name


def test_visuals_do_not_overlap(report_builder):
    for page in report_builder.PAGES:
        for index, first in enumerate(page.visuals):
            for second in page.visuals[index + 1 :]:
                assert not overlaps(first, second), f"{page.name}: {first.name} / {second.name}"


def test_no2_fields_only_on_no2_page(report_builder):
    for page in report_builder.PAGES:
        for visual in page.visuals:
            uses_no2 = any("NO2" in reference.name for reference in visual_fields(visual))
            assert uses_no2 == (page is report_builder.NO2) or visual.visual_type == "slicer", f"{page.name}: {visual.name}"


def test_trip_pages_slice_by_trip_end(report_builder):
    slicers = {page.name: visual for page in report_builder.PAGES for visual in page.visuals if visual.name == "trip_end_slicer"}

    assert set(slicers) == {"mobility", "mobility_vs_air_quality", "time_of_day"}
    for visual in slicers.values():
        assert visual.objects["selection"] == report_builder.FORCED_SINGLE_SELECTION["selection"], "one trip end at a time"
        [default] = visual.objects["general"][0]["properties"]["filter"]["filter"]["Where"]
        assert default["Condition"]["In"]["Values"] == [[{"Literal": {"Value": "'Pickup'"}}]]


def test_daily_trend_lines_are_smoothed(report_builder):
    visuals = {visual.name: visual for page in report_builder.PAGES for visual in page.visuals}
    for name in ("trips_by_day", "pm25_trend", "trips_and_pm25"):
        assert visuals[name].objects == report_builder.SMOOTH_LINES, name


def test_report_fields_exist_in_semantic_model(report_builder, model):
    for reference in report_fields(report_builder):
        table = model[reference.table]
        members = table.measures if reference.kind == "Measure" else table.columns
        assert reference.name in members, f"{reference.table}.{reference.name}"


def test_dax_references_resolve(model):
    measures = {name for table in model.values() for name in table.measures}
    for table_name, table in model.items():
        for measure_name, expression in table.measures.items():
            for referenced_table, referenced_column in TABLE_COLUMN_REFERENCE.findall(expression):
                assert referenced_column in model[referenced_table].columns, f"{measure_name}: {referenced_table}[{referenced_column}]"
            for referenced_measure in MEASURE_REFERENCE.findall(expression):
                assert referenced_measure in measures, f"{table_name}.{measure_name}: [{referenced_measure}]"


def test_relationships_join_model_columns(model):
    text = (MODEL_DEFINITION / "relationships.tmdl").read_text(encoding="utf-8")
    for table_name, column_name in re.findall(r"(?:from|to)Column: (\w+)\.(\w+)", text):
        assert column_name in model[table_name].columns, f"{table_name}.{column_name}"


def test_model_reads_every_gold_table_with_its_columns_and_types(model, gold_tables):
    assert {table.entity for table in model.values()} == set(gold_tables)
    for name, table in model.items():
        gold_types = {column.name: TMDL_DATA_TYPES[column.dataType.typeName()] for column in gold_tables[table.entity].schema}
        model_types = {properties["sourceColumn"]: properties["dataType"] for properties in table.columns.values()}
        assert model_types == gold_types, name


def test_model_tables_are_all_referenced_by_model_file(model):
    text = (MODEL_DEFINITION / "model.tmdl").read_text(encoding="utf-8")
    assert set(re.findall(r"^ref table (\w+)$", text, flags=re.M)) == set(model)


def test_direct_lake_source_points_at_wh_gold_logical_id():
    warehouse_platform = (item_folder("wh_gold.Warehouse") / ".platform").read_text(encoding="utf-8")
    [logical_id] = re.findall(r'"logicalId": "([^"]+)"', warehouse_platform)

    assert f'"{logical_id}"' in (MODEL_DEFINITION / "expressions.tmdl").read_text(encoding="utf-8")
