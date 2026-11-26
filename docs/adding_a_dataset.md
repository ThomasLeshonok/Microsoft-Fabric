# Adding a dataset

A checklist for bringing a new open dataset (weather, traffic sensors, energy) through bronze, silver and gold into
the report. The worked example is a daily weather feed for New York City: one row per weather station and day with
temperature and precipitation. Names follow the existing sources; replace `weather` with your source.

`nb_silver_worldbank_indicators` is the reference implementation for silver: copy it, there is no template notebook.

## Files the weather example touches

| Layer | File | Change |
|---|---|---|
| Bronze | `workspace/bronze/df_weather.Dataflow/.platform`, `mashup.pq`, `queryMetadata.json` | new |
| Bronze | `workspace/parameter.yml` | connection ids for other environments |
| Silver | `workspace/silver/nb_silver_weather.Notebook/.platform`, `notebook-content.py` | new |
| Silver | `workspace/silver/pl_silver_weather.DataPipeline/.platform`, `pipeline-content.json` | new |
| Silver | `workspace/silver/pl_silver_startup.DataPipeline/pipeline-content.json` | one rebuild activity |
| Silver | `workspace/silver/lh_silver.Lakehouse/.platform` | table in the description |
| Gold | `workspace/gold/nb_gold_star_schema.Notebook/notebook-content.py` | `build_*` function, `GOLD_TABLES` entry, markdown table row |
| Gold | `workspace/gold/pl_gold_daily.DataPipeline/pipeline-content.json` (and `.platform` description) | table in `tables` |
| Gold | `workspace/gold/wh_gold.Warehouse/.platform` | table in the description |
| Semantic model | `workspace/analytics/sm_gold.SemanticModel/definition/tables/FactWeatherDaily.tmdl` | new |
| Semantic model | `.../definition/model.tmdl`, `.../definition/relationships.tmdl` | `ref table`, relationships |
| Report | `scripts/build_report.py`, then the regenerated `workspace/analytics/rpt_nyc_insights.Report/` | visuals |
| Tests | `tests/test_silver_weather.py`; `INCREMENTAL_PIPELINES` and `REBUILT_NOTEBOOKS` in `tests/test_silver_pipelines.py`; `DAILY_TABLES` in `tests/test_gold_pipelines.py`; `gold_inputs` in `tests/test_gold_star_schema.py` | new and extended |
| Docs | `README.md` (items table, *Silver column types*, star schema diagram, *Run order*), `docs/data_dictionary.md`, `docs/lineage.md`, `docs/governance.md` (licence, quality rules, refresh) | rows for the new items |

Every new item needs a `.platform` with a new random `logicalId` (UUID4), for example
`python -c "import uuid; print(uuid.uuid4())"`. Item descriptions stay under 256 characters
(`test_item_descriptions_fit_fabric_limit`).

## 1. Bronze

- [ ] **Pick the ingestion item.** An API goes through a Dataflow Gen2 `df_<source>`, like `df_ecb_exchange_rates`,
  `df_worldbank_indicators` and `df_openaq_nyc`. Files go through a pipeline Copy, like `pl_ingest_taxi`. The weather
  API is a JSON API, so: `df_weather`.
- [ ] **Name the table `bronze_<source>_<entity>`**: `bronze_weather_daily`.
- [ ] **Append only, with `ingested_at`.** Copy the query shape of `df_worldbank_indicators`: read the API, type every
  column as `type nullable ...` (`test_dataflow_columns_accept_nulls`), drop the rows whose key is already in the table
  (`Table.NestedJoin(..., JoinKind.LeftAnti)` against `Lakehouse.Contents(...)`), then add
  `ingested_at = DateTimeZone.RemoveZone(DateTimeZone.UtcNow())`. The destination is `Kind = "Manual"`,
  `UpdateMethod = [Kind = "Append"]`, `DynamicSchema = false`. Bronze never updates a row; silver keeps the latest
  `ingested_at` per key.
- [ ] **Destination by name, not GUID**: `workspaceId = "."` and `lakehouseName = "lh_bronze"`
  (`test_items_reference_lakehouse_by_logical_id`).
- [ ] **Connections.** Add one Anonymous Web connection for the API host and the OAuth2 Lakehouse connection to
  `connections` in `queryMetadata.json` (`ClusterId` = the connection's `gatewayId`, `DatasourceId` = its id, from
  `GET /v1/connections`). The Web connection's `path` must equal the host in `Web.Contents`
  (`test_dataflows_bind_web_source_and_lakehouse_connections`). Add a `find_replace` entry in `workspace/parameter.yml`
  for each other environment.
- [ ] **Secrets.** An API key goes in Key Vault, never in the mashup: see how `pl_ingest_openaq` passes the OpenAQ key
  to `df_openaq_nyc` as a public parameter.
- [ ] **Locked scope.** Start at `2016-03-01`, like every other source.

## 2. Silver

- [ ] **Notebook `nb_silver_weather`**, copied from `nb_silver_worldbank_indicators`. Keep the cell layout: markdown,
  parameters (`bronze_lakehouse`, `load_mode`, `vacuum_retention_hours`), `%run nb_silver_delta_tables`, definitions,
  runtime. Rewrite the markdown cell for the new source. The source specific part is the transform functions and one
  `SilverDataset`; `load_dataset` in `nb_silver_delta_tables` does the reading, rule counts, table creation,
  `rebuild`/`append_missing` and the exit report.

  ```python
  WEATHER_SERIES = ("station_id",)
  WEATHER_KEY = (*WEATHER_SERIES, "observed_date")
  TEXT_TYPES = {"station_id": "VARCHAR(11)"}


  def normalize_weather(bronze: DataFrame) -> DataFrame:
      return bronze.select(
          F.trim("station").alias("station_id"),
          F.col("date").cast("date").alias("observed_date"),
          F.col("tmax").cast("double").alias("max_temperature_celsius"),
          F.col("prcp").cast("double").alias("precipitation_millimetres"),
          F.col("ingested_at").cast("timestamp").alias("ingested_at"),
      )


  def invalid_weather_rules() -> dict:
      conditions = {
          "missing_key": F.col("station_id").isNull() | F.col("observed_date").isNull(),
          "negative_precipitation": F.col("precipitation_millimetres") < 0,
      }
      return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


  def clean_weather(weather: DataFrame, rules: dict) -> DataFrame:
      valid = weather.filter(~is_invalid(rules))
      return keep_latest_ingestion(valid, WEATHER_KEY).drop("ingested_at")


  WEATHER_DAILY = SilverDataset(
      bronze_table="bronze_weather_daily",
      silver_table="silver_weather_daily",
      normalize=normalize_weather,
      rules=invalid_weather_rules,
      clean=clean_weather,
      series_key=WEATHER_SERIES,
      period_column="observed_date",
      text_types=TEXT_TYPES,
  )
  ```

  The runtime cell is the World Bank one with `WEATHER_DAILY`:
  `load_dataset(spark, WEATHER_DAILY, bronze_table_path(bronze_lakehouse, WEATHER_DAILY.bronze_table), load_mode)`,
  then `vacuum_after_load` and the exit cell.
- [ ] **What the spec means.** `series_key` + `period_column` drive `append_missing`: per series, only rows after its
  latest period in silver are appended, a row already in silver is never rewritten. `text_types` gives text columns a
  `CHAR(n)`/`VARCHAR(n)` type (`CHAR` for fixed length codes only); integer widths come from the casts in
  `normalize_*`. Add `partition_columns` for a large table.
- [ ] **When the spec does not fit, write the load yourself** with the same shared helpers: ECB carries the last
  silver rate into an append, taxi trips load file by file and overwrite by month, air quality writes three tables and
  joins zones. Don't bend `SilverDataset` to cover them.
- [ ] **Pipeline `pl_silver_weather`**, copied from `pl_silver_worldbank_indicators`: one `TridentNotebook` activity
  with `notebookId` = the notebook's logicalId, `workspaceId` = `00000000-0000-0000-0000-000000000000`, parameter
  `load_mode = append_missing`, `concurrency: 1`.
- [ ] **`pl_silver_startup`**: add an activity `Rebuild weather` with `load_mode = rebuild`. It has no dependency unless
  the notebook reads another silver table (air quality depends on taxi zones).
- [ ] **README**: a row in the items table for each new item, and a row in the *Silver column types* table.

## 3. Gold

- [ ] **A `build_*` function** in `nb_gold_star_schema` that takes silver DataFrames and returns the gold table, plus
  one `GOLD_TABLES` entry that wires it to its inputs and append rule:

  ```python
  SILVER_WEATHER_TABLE = "silver_weather_daily"


  def build_fact_weather_daily(weather: DataFrame) -> DataFrame:
      daily = weather.groupBy("observed_date").agg(
          F.countDistinct("station_id").alias("StationCount"),
          F.max("max_temperature_celsius").alias("MaxTemperatureCelsius"),
          F.avg("precipitation_millimetres").alias("AvgPrecipitationMillimetres"),
      )
      return daily.select(date_key("observed_date").alias("DateKey"), "StationCount", "MaxTemperatureCelsius", "AvgPrecipitationMillimetres")


  GOLD_TABLES = {
      ...,
      "FactWeatherDaily": GoldTable(lambda inputs: build_fact_weather_daily(inputs.silver(SILVER_WEATHER_TABLE)), AppendRule("DateKey")),
  }
  ```

  `append_rule = None` makes a table rebuild only, like `DimZone`, `DimHour` and `DimTripEnd`.
- [ ] **Conformed keys**, so the new table slices with the others: `DateKey` (`yyyyMMdd` int, `date_key`), `ZoneKey`
  (TLC `LocationID`, `-1` = unknown, `UNKNOWN_ZONE_KEY`), `HourKey` (0 to 23, NYC wall clock). A point source (a
  station, a sensor) gets its `ZoneKey` in silver, the way `nb_silver_air_quality` places sensors in
  `silver_taxi_zones` boundaries.
- [ ] **Schedule it**: add the table to `tables` of the `pl_gold_*` pipeline with the matching cadence
  (`pl_gold_daily` for a daily table) and to its `.platform` description. Each table belongs to one gold pipeline
  (`test_gold_pipeline_tables_can_all_be_appended`).
- [ ] **First run**: the connector creates the Warehouse table on the first write. After a later column change, drop
  the table in `wh_gold` once before the next run (`overwrite` keeps the old definition).

## 4. Semantic model

- [ ] `tables/FactWeatherDaily.tmdl`: one `column` per gold column with `sourceColumn`, a `dataType` matching gold,
  `summarizeBy`, keys hidden (`isHidden`), and the `partition` in Direct Lake mode pointing at
  `entityName: FactWeatherDaily` (copy an existing fact table).
- [ ] `model.tmdl`: `ref table FactWeatherDaily`.
- [ ] `relationships.tmdl`: `FactWeatherDaily.DateKey` to `DimDate.DateKey` (and `ZoneKey`/`HourKey` if it has them),
  each relationship with a new UUID.
- [ ] Measures in display folders, described with `///`.
- [ ] `tests/test_power_bi.py` fails when the model's columns or types differ from what `nb_gold_star_schema` writes,
  when a table is not referenced in `model.tmdl`, or when a relationship joins a missing column.

## 5. Report

- [ ] Add the visuals in `scripts/build_report.py` (pages, visuals, positions), then regenerate:
  `.venv/Scripts/python scripts/build_report.py`. Never edit the report in Power BI only: the next generation
  overwrites it, and `test_committed_report_matches_generator` fails.

## 6. Tests

- [ ] `tests/test_silver_weather.py`, following `tests/test_silver_worldbank_indicators.py`: fixture
  `load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_weather", DEFINITION_CELLS))`, one test per rule, the latest
  ingestion wins, the table schema (`TableDefinition.from_frame` with `TEXT_TYPES`), and the append keeping only
  periods after the latest one per series.
- [ ] Add the pipeline and notebook to `INCREMENTAL_PIPELINES` and `REBUILT_NOTEBOOKS` in
  `tests/test_silver_pipelines.py`, and the gold table to the pipeline's set in `tests/test_gold_pipelines.py`.
- [ ] Register the new silver table in `gold_inputs` in `tests/test_gold_star_schema.py`, and test the `build_*`
  function like the other gold builders.
- [ ] Run everything: `.venv/Scripts/python -m pytest tests -q`.

## 7. Schedules

Schedules are set in the Fabric UI, not in the repo. Write the intended cadence in the pipeline's `.platform`
description (for example "Schedule daily after df_weather."), the way the existing `pl_silver_*` and `pl_gold_*`
pipelines do, and add it to *Run order* in the README.
