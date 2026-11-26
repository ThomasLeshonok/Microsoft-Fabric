import pytest
from notebook_loader import load
from pipeline_loader import (
    CURRENT_WORKSPACE_ID,
    logical_id,
    notebook_activities,
    notebook_parameters,
    parameter_values,
    pipeline_properties,
)

GOLD_NOTEBOOK = "nb_gold_star_schema"
HOURLY_TABLES = {"FactAirQualityHourly"}
DAILY_TABLES = {"DimDate", "DimFX", "FactAirQualityDaily"}
MONTHLY_TABLES = {"FactTaxiDaily", "FactTaxiHourly", "DimGDP"}
GOLD_PIPELINES = {"pl_gold_hourly": HOURLY_TABLES, "pl_gold_daily": DAILY_TABLES, "pl_gold_monthly": MONTHLY_TABLES}


def gold_activity(pipeline: str) -> dict:
    activities = notebook_activities(pipeline)
    assert list(activities) == [GOLD_NOTEBOOK]
    return activities[GOLD_NOTEBOOK]


@pytest.mark.parametrize(("pipeline", "tables"), GOLD_PIPELINES.items())
def test_gold_pipeline_appends_only_missing_periods_of_its_tables(pipeline, tables):
    activity = gold_activity(pipeline)
    parameters = parameter_values(activity)

    assert set(parameters) <= notebook_parameters(GOLD_NOTEBOOK)
    assert parameters["load_mode"] == "append_missing"
    assert set(parameters["tables"].split(",")) == tables
    assert activity["typeProperties"]["workspaceId"] == CURRENT_WORKSPACE_ID
    assert pipeline_properties(pipeline)["concurrency"] == 1


def test_gold_pipeline_tables_can_all_be_appended():
    gold_tables = load((GOLD_NOTEBOOK, slice(1, -1)))["GOLD_TABLES"]
    appendable = {name for name, table in gold_tables.items() if table.append_rule}

    scheduled = [*GOLD_PIPELINES.values()]
    assert set().union(*scheduled) <= appendable
    assert sum(len(tables) for tables in scheduled) == len(set().union(*scheduled))


def test_gold_pipelines_have_unique_logical_ids():
    ids = {logical_id(f"{pipeline}.DataPipeline") for pipeline in GOLD_PIPELINES}

    assert len(ids) == len(GOLD_PIPELINES)
