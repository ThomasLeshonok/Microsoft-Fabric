import pytest
from pipeline_loader import (
    CURRENT_WORKSPACE_ID,
    logical_id,
    notebook_activities,
    notebook_parameters,
    parameter_values,
    pipeline_properties,
)

INCREMENTAL_PIPELINES = {
    "pl_silver_air_quality": "nb_silver_air_quality",
    "pl_silver_taxi_trips": "nb_silver_taxi_trips",
    "pl_silver_ecb_exchange_rates": "nb_silver_ecb_exchange_rates",
    "pl_silver_worldbank_indicators": "nb_silver_worldbank_indicators",
}
REBUILT_NOTEBOOKS = {
    "nb_silver_taxi_zones",
    "nb_silver_air_quality",
    "nb_silver_taxi_trips",
    "nb_silver_ecb_exchange_rates",
    "nb_silver_worldbank_indicators",
}


@pytest.mark.parametrize("pipeline", [*INCREMENTAL_PIPELINES, "pl_silver_startup"])
def test_pipeline_notebook_parameters_exist_in_notebook(pipeline):
    for notebook, activity in notebook_activities(pipeline).items():
        assert set(parameter_values(activity)) <= notebook_parameters(notebook), notebook
        assert activity["typeProperties"]["workspaceId"] == CURRENT_WORKSPACE_ID


@pytest.mark.parametrize(("pipeline", "notebook"), INCREMENTAL_PIPELINES.items())
def test_incremental_pipeline_appends_only_missing_periods(pipeline, notebook):
    activities = notebook_activities(pipeline)

    assert list(activities) == [notebook]
    assert parameter_values(activities[notebook])["load_mode"] == "append_missing"
    assert pipeline_properties(pipeline)["concurrency"] == 1


def test_taxi_pipeline_appends_without_explicit_months():
    assert parameter_values(notebook_activities("pl_silver_taxi_trips")["nb_silver_taxi_trips"])["months"] == ""


def test_air_quality_pipeline_adds_daily_only_in_daily_load_hour():
    properties = pipeline_properties("pl_silver_air_quality")
    [select_tables] = [activity for activity in properties["activities"] if activity["type"] == "SetVariable"]

    assert select_tables["typeProperties"]["value"]["value"] == (
        "@if(equals(int(convertFromUtc(utcNow(), 'Eastern Standard Time', 'HH')), pipeline().parameters.daily_load_hour), "
        "'sensors,hourly,daily', 'sensors,hourly')"
    )
    tables = parameter_values(notebook_activities("pl_silver_air_quality")["nb_silver_air_quality"])["tables"]
    assert tables == {"value": "@variables('tables')", "type": "Expression"}


def test_startup_pipeline_rebuilds_every_silver_table_with_zones_first():
    activities = notebook_activities("pl_silver_startup")

    assert set(activities) == REBUILT_NOTEBOOKS
    for notebook in REBUILT_NOTEBOOKS - {"nb_silver_taxi_zones"}:
        assert parameter_values(activities[notebook])["load_mode"] == "rebuild", notebook
    assert parameter_values(activities["nb_silver_air_quality"])["tables"] == "sensors,hourly,daily"
    zone_activity = activities["nb_silver_taxi_zones"]["name"]
    assert [dependency["activity"] for dependency in activities["nb_silver_air_quality"]["dependsOn"]] == [zone_activity]


def test_silver_pipelines_have_unique_logical_ids():
    pipelines = [*INCREMENTAL_PIPELINES, "pl_silver_startup"]
    ids = [logical_id(f"{pipeline}.DataPipeline") for pipeline in pipelines]

    assert len(set(ids)) == len(pipelines)
