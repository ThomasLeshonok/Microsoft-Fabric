import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path

REPORT_FOLDER = Path(__file__).resolve().parents[1] / "workspace" / "analytics" / "rpt_nyc_insights.Report"
SEMANTIC_MODEL_PATH = "../sm_gold.SemanticModel"
SCHEMA_ROOT = "https://developer.microsoft.com/json-schemas/fabric/item/report"
BASE_THEME = "CY24SU10"
PAGE_WIDTH = 1280
PAGE_HEIGHT = 720

TAXI = "FactTaxiDaily"
AIR = "FactAirQualityDaily"
TAXI_HOURLY = "FactTaxiHourly"
AIR_HOURLY = "FactAirQualityHourly"
HOUR = "DimHour"
TRIP_END = "DimTripEnd"
DATE = "DimDate"
ZONE = "DimZone"
FX = "DimFX"
GDP = "DimGDP"


@dataclass(frozen=True)
class Field:
    kind: str
    table: str
    name: str


@dataclass(frozen=True)
class Visual:
    name: str
    visual_type: str
    title: str
    position: tuple
    roles: dict
    sort_descending_by: Field = None
    objects: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    name: str
    display_name: str
    visuals: tuple


def column(table: str, name: str) -> Field:
    return Field("Column", table, name)


def measure(table: str, name: str) -> Field:
    return Field("Measure", table, name)


def literal(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


def text_literal(text: str) -> dict:
    return literal("'" + text.replace("'", "''") + "'")


SMOOTH_LINES = {"lineStyles": [{"properties": {"lineChartType": text_literal("smooth")}}]}
EXCHANGE_RATE_LABELS = {"labels": [{"properties": {"labelPrecision": literal("3L")}}]}


def card(name: str, value: Field, position: tuple, objects: dict = None) -> Visual:
    return Visual(name, "card", value.name, position, {"Values": (value,)}, objects=objects or {})


FORCED_SINGLE_SELECTION = {"selection": [{"properties": {"singleSelect": literal("true"), "strictSingleSelect": literal("true")}}]}
DEFAULT_TRIP_END = "Pickup"


def selected_value(reference: Field, value: str) -> dict:
    source = "s"
    selected_column = {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": reference.name}}
    condition = {"In": {"Expressions": [selected_column], "Values": [[text_literal(value)["expr"]]]}}
    query_filter = {"Version": 2, "From": [{"Name": source, "Entity": reference.table, "Type": 0}], "Where": [{"Condition": condition}]}
    return {"general": [{"properties": {"filter": {"filter": query_filter}}}]}


def slicer(name: str, title: str, values: Field, mode: str, position: tuple, objects: dict = None) -> Visual:
    slicer_objects = {
        "data": [{"properties": {"mode": text_literal(mode)}}],
        "header": [{"properties": {"show": literal("false")}}],
        **(objects or {}),
    }
    return Visual(name, "slicer", title, position, {"Values": (values,)}, objects=slicer_objects)


def date_slicer(position: tuple) -> Visual:
    return slicer("date_slicer", "Date", column(DATE, "Date"), "Between", position)


def borough_slicer(position: tuple) -> Visual:
    return slicer("borough_slicer", "Borough", column(ZONE, "Borough"), "Dropdown", position)


def trip_end_slicer(position: tuple) -> Visual:
    trip_end = column(TRIP_END, "TripEnd")
    objects = {**FORCED_SINGLE_SELECTION, **selected_value(trip_end, DEFAULT_TRIP_END)}
    return slicer("trip_end_slicer", "Trip end", trip_end, "Dropdown", position, objects)


TRIP_SLICERS = (
    date_slicer((16, 16, 260, 80)),
    borough_slicer((292, 16, 160, 80)),
    trip_end_slicer((468, 16, 160, 80)),
)


MOBILITY = Page(
    "mobility",
    "Mobility",
    (
        *TRIP_SLICERS,
        card("trips", measure(TAXI, "Trips"), (644, 16, 143, 80)),
        card("trips_per_day", measure(TAXI, "Trips per Day"), (803, 16, 143, 80)),
        card("avg_fare", measure(TAXI, "Avg Fare USD"), (962, 16, 143, 80)),
        card("trips_yoy", measure(TAXI, "Trips YoY %"), (1121, 16, 143, 80)),
        Visual(
            "trips_by_day",
            "lineChart",
            "Trips per day",
            (16, 112, 828, 280),
            {"Category": (column(DATE, "Date"),), "Y": (measure(TAXI, "Trips"),)},
            objects=SMOOTH_LINES,
        ),
        Visual(
            "trips_by_weekday",
            "clusteredColumnChart",
            "Trips per day by weekday",
            (860, 112, 404, 280),
            {"Category": (column(DATE, "DayName"),), "Y": (measure(TAXI, "Trips per Day"),)},
        ),
        Visual(
            "busiest_zones",
            "tableEx",
            "Busiest zones",
            (16, 408, 828, 296),
            {
                "Values": (
                    column(ZONE, "Zone"),
                    column(ZONE, "Borough"),
                    measure(TAXI, "Trips"),
                    measure(TAXI, "Avg Fare USD"),
                    measure(TAXI, "Avg Trip Distance (mi)"),
                    measure(TAXI, "Avg Trip Duration (min)"),
                )
            },
            sort_descending_by=measure(TAXI, "Trips"),
        ),
        Visual(
            "trips_by_month",
            "clusteredColumnChart",
            "Seasonality: trips per day by month",
            (860, 408, 404, 296),
            {"Category": (column(DATE, "MonthName"),), "Y": (measure(TAXI, "Trips per Day"),)},
        ),
    ),
)

AIR_QUALITY = Page(
    "air_quality",
    "Air Quality",
    (
        date_slicer((16, 16, 300, 80)),
        borough_slicer((332, 16, 200, 80)),
        card("pm25", measure(AIR, "PM2.5 (µg/m³)"), (548, 16, 230, 80)),
        card("peak_pm25", measure(AIR, "Peak PM2.5 (µg/m³)"), (794, 16, 230, 80)),
        card("days_above_standard", measure(AIR, "PM2.5 Days Above EPA Standard"), (1040, 16, 224, 80)),
        Visual(
            "pm25_trend",
            "lineChart",
            "PM2.5 daily and 30-day average",
            (16, 112, 828, 280),
            {
                "Category": (column(DATE, "Date"),),
                "Y": (measure(AIR, "PM2.5 (µg/m³)"), measure(AIR, "PM2.5 30-Day Avg")),
            },
            objects=SMOOTH_LINES,
        ),
        Visual(
            "pm25_by_month",
            "clusteredColumnChart",
            "Seasonality: PM2.5 by month",
            (860, 112, 404, 280),
            {"Category": (column(DATE, "MonthName"),), "Y": (measure(AIR, "PM2.5 (µg/m³)"),)},
        ),
        Visual(
            "pm25_hotspots",
            "clusteredBarChart",
            "PM2.5 hotspots by zone",
            (16, 408, 1248, 296),
            {"Category": (column(ZONE, "Zone"),), "Y": (measure(AIR, "PM2.5 (µg/m³)"),)},
            sort_descending_by=measure(AIR, "PM2.5 (µg/m³)"),
        ),
    ),
)

CORRELATION = Page(
    "mobility_vs_air_quality",
    "Mobility vs Air Quality",
    (
        *TRIP_SLICERS,
        card("trips_pm25_correlation", measure(TAXI_HOURLY, "Trips vs PM2.5 Correlation"), (644, 16, 302, 80)),
        card("pm25_days_above", measure(AIR, "PM2.5 Days Above EPA Standard"), (962, 16, 302, 80)),
        Visual(
            "trips_and_pm25",
            "lineClusteredColumnComboChart",
            "Daily trips (columns) and PM2.5 (line)",
            (16, 112, 1248, 280),
            {"Category": (column(DATE, "Date"),), "Y": (measure(TAXI, "Trips"),), "Y2": (measure(AIR, "PM2.5 (µg/m³)"),)},
            objects=SMOOTH_LINES,
        ),
        Visual(
            "trips_vs_pm25_scatter",
            "scatterChart",
            "Each day: trips vs PM2.5",
            (16, 408, 520, 296),
            {"Category": (column(DATE, "Date"),), "X": (measure(TAXI, "Trips"),), "Y": (measure(AIR, "PM2.5 (µg/m³)"),)},
        ),
        Visual(
            "correlation_by_zone",
            "pivotTable",
            "Trips vs pollution correlation by zone and pollutant",
            (552, 408, 712, 296),
            {
                "Rows": (column(ZONE, "Zone"),),
                "Columns": (column(AIR_HOURLY, "Pollutant"),),
                "Values": (measure(TAXI_HOURLY, "Trips vs Pollution Correlation"),),
            },
        ),
    ),
)

TIME_OF_DAY = Page(
    "time_of_day",
    "Time of Day",
    (
        *TRIP_SLICERS,
        card("busiest_hour", measure(TAXI_HOURLY, "Busiest Hour"), (644, 16, 302, 80)),
        card("most_polluted_hour", measure(AIR_HOURLY, "Most Polluted Hour (PM2.5)"), (962, 16, 302, 80)),
        Visual(
            "trips_and_pm25_by_hour",
            "lineClusteredColumnComboChart",
            "Trips per day (columns) and PM2.5 (line) by hour",
            (16, 112, 1248, 280),
            {
                "Category": (column(HOUR, "HourLabel"),),
                "Y": (measure(TAXI_HOURLY, "Trips per Day in Hour"),),
                "Y2": (measure(AIR_HOURLY, "Hourly PM2.5 (µg/m³)"),),
            },
        ),
        Visual(
            "trips_by_hour_weekend",
            "lineChart",
            "Trips per day by hour, weekend vs weekday",
            (16, 408, 826, 296),
            {
                "Category": (column(HOUR, "HourLabel"),),
                "Series": (column(DATE, "IsWeekend"),),
                "Y": (measure(TAXI_HOURLY, "Trips per Day in Hour"),),
            },
        ),
        Visual(
            "correlation_by_day_part",
            "pivotTable",
            "Trips vs pollution correlation by day part and pollutant",
            (858, 408, 406, 296),
            {
                "Rows": (column(HOUR, "DayPart"),),
                "Columns": (column(AIR_HOURLY, "Pollutant"),),
                "Values": (measure(TAXI_HOURLY, "Trips vs Pollution Correlation"),),
            },
        ),
    ),
)

NO2 = Page(
    "no2",
    "NO2",
    (
        date_slicer((16, 16, 300, 80)),
        borough_slicer((332, 16, 200, 80)),
        card("no2", measure(AIR, "NO2 (ppm)"), (548, 16, 350, 80)),
        card("trips_no2_correlation", measure(TAXI_HOURLY, "Trips vs NO2 Correlation"), (914, 16, 350, 80)),
        Visual(
            "no2_by_borough",
            "lineChart",
            "NO2 by borough",
            (16, 112, 1248, 280),
            {"Category": (column(DATE, "MonthStartDate"),), "Series": (column(ZONE, "Borough"),), "Y": (measure(AIR, "NO2 (ppm)"),)},
        ),
        Visual(
            "no2_by_hour_weekend",
            "lineChart",
            "NO2 by hour, weekend vs weekday",
            (16, 408, 1248, 296),
            {
                "Category": (column(HOUR, "HourLabel"),),
                "Series": (column(DATE, "IsWeekend"),),
                "Y": (measure(AIR_HOURLY, "Hourly NO2 (ppm)"),),
            },
        ),
    ),
)

ECONOMY = Page(
    "economic_impact",
    "Economic Impact",
    (
        date_slicer((16, 16, 300, 80)),
        card("revenue_usd", measure(TAXI, "Revenue USD"), (332, 16, 180, 80)),
        card("revenue_eur", measure(TAXI, "Revenue EUR"), (528, 16, 180, 80)),
        card("avg_revenue_per_trip_eur", measure(TAXI, "Avg Revenue per Trip EUR"), (724, 16, 180, 80)),
        card("avg_usd_per_eur", measure(FX, "Avg USD per EUR"), (920, 16, 160, 80), EXCHANGE_RATE_LABELS),
        card("fx_effect", measure(TAXI, "FX Effect EUR"), (1096, 16, 168, 80)),
        Visual(
            "revenue_usd_vs_eur",
            "lineChart",
            "Monthly revenue in USD and EUR",
            (16, 112, 620, 280),
            {"Category": (column(DATE, "MonthStartDate"),), "Y": (measure(TAXI, "Revenue USD"), measure(TAXI, "Revenue EUR"))},
        ),
        Visual(
            "fx_effect_by_month",
            "lineClusteredColumnComboChart",
            "FX effect on EUR revenue (columns) and US$ per euro (line)",
            (652, 112, 612, 280),
            {"Category": (column(DATE, "MonthStartDate"),), "Y": (measure(TAXI, "FX Effect EUR"),), "Y2": (measure(FX, "Avg USD per EUR"),)},
        ),
        Visual(
            "revenue_vs_gdp",
            "lineClusteredColumnComboChart",
            "Taxi revenue (columns) and US GDP (line) by year",
            (16, 408, 520, 296),
            {"Category": (column(DATE, "Year"),), "Y": (measure(TAXI, "Revenue USD"),), "Y2": (measure(GDP, "US GDP (USD)"),)},
        ),
        Visual(
            "growth_by_year",
            "tableEx",
            "Growth vs environment by year",
            (552, 408, 712, 296),
            {
                "Values": (
                    column(DATE, "Year"),
                    measure(TAXI, "Trips YoY %"),
                    measure(TAXI, "Revenue YoY %"),
                    measure(GDP, "US GDP Growth %"),
                    measure(TAXI, "Revenue per US$1bn GDP"),
                    measure(AIR, "PM2.5 (µg/m³)"),
                    measure(AIR, "PM2.5 YoY %"),
                )
            },
        ),
    ),
)

PAGES = (MOBILITY, AIR_QUALITY, CORRELATION, TIME_OF_DAY, NO2, ECONOMY)


def field_expression(reference: Field) -> dict:
    return {reference.kind: {"Expression": {"SourceRef": {"Entity": reference.table}}, "Property": reference.name}}


def projection(reference: Field) -> dict:
    return {
        "field": field_expression(reference),
        "queryRef": f"{reference.table}.{reference.name}",
        "nativeQueryRef": reference.name,
    }


def visual_query(visual: Visual) -> dict:
    query = {
        "queryState": {
            role: {"projections": [projection(reference) for reference in references]}
            for role, references in visual.roles.items()
        }
    }
    if visual.sort_descending_by:
        query["sortDefinition"] = {
            "sort": [{"field": field_expression(visual.sort_descending_by), "direction": "Descending"}],
            "isDefaultSort": False,
        }
    return query


def visual_json(visual: Visual, order: int) -> dict:
    x, y, width, height = visual.position
    body = {
        "visualType": visual.visual_type,
        "query": visual_query(visual),
        "visualContainerObjects": {"title": [{"properties": {"show": literal("true"), "text": text_literal(visual.title)}}]},
        "drillFilterOtherVisuals": True,
    }
    if visual.objects:
        body["objects"] = visual.objects
    return {
        "$schema": f"{SCHEMA_ROOT}/definition/visualContainer/2.0.0/schema.json",
        "name": visual.name,
        "position": {"x": x, "y": y, "z": order, "width": width, "height": height, "tabOrder": order},
        "visual": body,
    }


def page_json(page: Page) -> dict:
    return {
        "$schema": f"{SCHEMA_ROOT}/definition/page/2.0.0/schema.json",
        "name": page.name,
        "displayName": page.display_name,
        "displayOption": "FitToPage",
        "height": PAGE_HEIGHT,
        "width": PAGE_WIDTH,
    }


def report_json() -> dict:
    return {
        "$schema": f"{SCHEMA_ROOT}/definition/report/3.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": BASE_THEME,
                "reportVersionAtImport": {"visual": "1.8.0", "report": "2.0.0", "page": "1.3.0"},
                "type": "SharedResources",
            }
        },
        "resourcePackages": [
            {
                "name": "SharedResources",
                "type": "SharedResources",
                "items": [{"name": BASE_THEME, "path": f"BaseThemes/{BASE_THEME}.json", "type": "BaseTheme"}],
            }
        ],
    }


def report_files(pages: tuple) -> dict:
    files = {
        "definition.pbir": {
            "$schema": f"{SCHEMA_ROOT}/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {"byPath": {"path": SEMANTIC_MODEL_PATH}},
        },
        "definition/version.json": {"$schema": f"{SCHEMA_ROOT}/definition/versionMetadata/1.0.0/schema.json", "version": "2.0.0"},
        "definition/report.json": report_json(),
        "definition/pages/pages.json": {
            "$schema": f"{SCHEMA_ROOT}/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": [page.name for page in pages],
            "activePageName": pages[0].name,
        },
    }
    for page in pages:
        page_folder = f"definition/pages/{page.name}"
        files[f"{page_folder}/page.json"] = page_json(page)
        for order, visual in enumerate(page.visuals):
            files[f"{page_folder}/visuals/{visual.name}/visual.json"] = visual_json(visual, order)
    return files


def render(document: dict) -> str:
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def write_report(folder: Path, files: dict) -> None:
    shutil.rmtree(folder / "definition", ignore_errors=True)
    for relative_path, document in files.items():
        path = folder / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(document), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    write_report(REPORT_FOLDER, report_files(PAGES))
    print(f"Wrote {REPORT_FOLDER}")
