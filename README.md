# fabric-analytics

Medallion analytics on Microsoft Fabric for NYC taxi trips, ECB exchange rates, World Bank indicators and OpenAQ air
quality: bronze ingestion into `lh_bronze`, cleaned Delta tables in `lh_silver`, and a gold star schema in the `wh_gold`
Warehouse.

`workspace/` uses the Fabric Git integration format (`<name>.<Type>/.platform` + definition files), so you can sync it
with workspace Git integration or publish it with `fabric-cicd` (`scripts/deploy.py`).

## Locked scope

Data scope starts at **2016-03**. The pipeline drops months before `2016-03`, the OpenAQ notebook and dataflow fail if
the start date is earlier than `2016-03-01`, ECB uses `startPeriod=2016-03-01`, and World Bank uses `date=2016:<current year>`.

## `lh_bronze` Files layout

```
Files/
├── nyctaxi_yellow/
│   ├── taxi_zone_lookup.csv
│   ├── taxi_zones/        taxi_zones.shp, .shx, .dbf, .prj
│   ├── 2016/03/yellow_tripdata_2016-03.parquet
│   ├── ...
│   └── 2026/09/yellow_tripdata_2026-09.parquet
└── OpenAQ/            <entity>/run_date=<date>/<entity>.jsonl   (locations, sensors, sensor_days; sensor_measurements split per sensor_id=<id>/<window>.jsonl)
```

## Items

| Item | What it does | Output |
|---|---|---|
| `lh_bronze` (Lakehouse) | landing zone | `Files/...` above, `bronze_*` tables |
| `nb_http_download` | validates a URL (http/https, host, no whitespace), checks it exists (HEAD, falling back to a streamed GET when HEAD is refused), then streams any file type to OneLake | one file |
| `nb_download_zip_members` | downloads a ZIP with the `nb_http_download` classes (via `%run`) and lands only the members named in `members_json`, matched by base name; by default `taxi_zones.shp`, `.shx`, `.dbf` (`LocationID`, `zone`, `borough`) and `.prj` (EPSG:2263) from `misc/taxi_zones.zip` | `Files/nyctaxi_yellow/taxi_zones/` |
| `pl_ingest_taxi` | `Get Metadata` checks the trips folder: missing means backfill from `locked_scope_start` (2016-03), present means only the latest published month (now minus `publish_lag_months`, default 2); then `ForEach` HTTP Copy, plus `taxi_zone_lookup.csv` | `Files/nyctaxi_yellow/yyyy/MM/`, `Files/nyctaxi_yellow/taxi_zone_lookup.csv` |
| `df_ecb_exchange_rates` | ECB `EXR` CSV (`D.USD.EUR.SP00.A`, from 2016-03-01); appends only days whose `key`, `time_period` are not in the table yet | `bronze_ecb_exchange_rates` |
| `df_worldbank_indicators` | World Bank JSON with `per_page=1000`, every page, second array element expanded; appends only published values (`value` not null) whose `indicator_id`, `country_iso3`, `period` are not in the table yet | `bronze_worldbank_indicators` |
| `df_openaq_nyc` | NYC locations (bbox), then their sensors, then `/days` and raw `/measurements` (monthly windows) per sensor, paginated with `List.Generate` (`page`, `limit=1000`); appends only locations and sensors not in the table yet | `bronze_openaq_*` |
| `nb_ingest_openaq` | notebook fallback for `df_openaq_nyc`; reuses `nb_http_download` via `%run` | same `bronze_openaq_*` tables + raw JSONL in `Files/OpenAQ` |
| `nb_compact_bronze_tables` | `OPTIMIZE` packs parquet files smaller than `target_file_size_bytes` (1 MB) into files of at most that size, then `VACUUM` (168 h); run by `pl_ingest_openaq` after a backfill or with `compact_tables = true` | `bronze_openaq_sensor_days`, `bronze_openaq_sensor_measurements` |
| `lh_silver` (Lakehouse) | silver layer, fixed schemas | `silver_*` tables |
| `nb_silver_delta_tables` | loaded by the silver notebooks with `%run`; creates each table with `CREATE OR REPLACE TABLE` DDL built from the DataFrame schema plus `CHAR(n)`/`VARCHAR(n)` overrides (Spark DataFrames turn both into `string`), stores the schema in the table property `silver.schema` and rebuilds a table whose schema changed; writes go through `writeTo`, so a too long text or an out-of-range integer fails the write; holds the shared `load_mode` logic: `rebuild` recreates a table from all of bronze, `append_missing` appends only rows newer than the latest period in silver for their key (never rewrites a row; loads everything into a missing table, raises `OutdatedTableError` on an outdated schema); also holds what every silver notebook shares: rule checks and counts (`is_invalid`, `count_rule_hits`), the exit report (`TableReport`, `build_table_report`), `keep_latest_ingestion` and the OneLake paths (`bronze_table_path`, `lakehouse_path`); `SilverDataset` + `load_dataset` load a simple API source from one spec (tables, transform functions, series key and period column, text types) | - |
| `nb_silver_taxi_trips` | reads each bronze trip file straight from `lh_bronze` (no raw copy in silver), normalizes columns to one schema (amounts: `fare_amount`, `total_amount` only), drops invalid trips (rules in `invalid_trip_rules`), `abs()` on amounts, one row per trip (`TRIP_KEY`, original wins over its refund reversal), adds pickup date/hour/weekday and `trip_duration_minutes`; `load_mode` = `overwrite_months` (default: overwrites each month in `months` = `yyyy-MM,...` or empty for all; a table with an outdated schema is recreated and every month reloaded), `append_missing` (appends only bronze months after the latest month in silver) or `rebuild`; after the first month, `max_parallel_months` (default 4) months load at once; exits with per-month rule counts | `silver_taxi_trips` partitioned by `pickup_year`, `pickup_month` |
| `nb_silver_air_quality` | reads the `bronze_openaq_*` tables from `lh_bronze`, drops redundant columns (constant `period_*`, period ends, day `value` = rounded `avg`, coverage counts, `q25`/`q75`/`sd`, `sensor_name`, `parameter_id`, constant location columns, `locality`, `owner_name`), drops invalid rows (rules in `invalid_reading_rules`, `invalid_sensor_rules`; one unit per parameter in `SUPPORTED_UNITS`), keeps the latest `ingested_at` per key, drops the current hour and the current NYC day (`unfinished_period`), adds `measured_at` (NYC wall clock) and date/hour/weekday attributes, joins each sensor to its location (`is_reference_monitor`, coordinates) and to the `silver_taxi_zones` boundary containing it (`taxi_zone_id`; a sensor outside every zone is skipped), writes sensors first (only sensors not in silver yet are appended, so a skipped sensor is checked again every run), then gives every hourly and daily reading its sensor's `taxi_zone_id` in place of the OpenAQ `location_id` (still in the sensors table via `sensor_id`) and drops readings of sensors missing from the sensors table; with `load_mode = append_missing` (default) hourly and daily append, per sensor, only readings after its latest `measured_at_utc` / `measured_date` in silver; `rebuild` overwrites every selected table; `tables` = subset of `sensors,hourly,daily`, empty for all; exits with per-table rule counts | `silver_air_quality_hourly`, `silver_air_quality_daily` partitioned by `measured_year`, `silver_air_quality_sensors` |
| `nb_silver_worldbank_indicators` | reference implementation of a `SilverDataset` (`WORLDBANK_INDICATORS`, loaded by `load_dataset`), see [docs/adding_a_dataset.md](docs/adding_a_dataset.md); reads `bronze_worldbank_indicators` from `lh_bronze`, drops redundant columns (`country_id`, empty `unit`/`obs_status`, `decimals`), turns `period` into smallint `year` (annual periods only), moves the currency out of `indicator_name` into `currency_name` (`GDP (current US$)` becomes `GDP` and `USD`, symbols in `CURRENCY_CODES`), drops invalid rows (rules in `invalid_indicator_rules`), keeps the latest `ingested_at` per indicator, country and year, adds `period_start_date`/`period_end_date`; `load_mode` = `rebuild` (default, overwrites the table) or `append_missing` (appends per indicator and country only the years after its latest year in silver); exits with rule counts | `silver_worldbank_indicators` |
| `nb_silver_ecb_exchange_rates` | reads `bronze_ecb_exchange_rates` from `lh_bronze`, keeps `base_currency` (`currency_denom`), `quote_currency` (`currency`), `rate_date` and `exchange_rate` (quote units per 1 base unit, e.g. US$ per euro) and drops the series-key repeats and constant/empty SDMX attributes, drops invalid rows (rules in `invalid_rate_rules`; daily series only), keeps the latest `ingested_at` per pair and day, fills weekends and TARGET holidays with the last published rate (`is_published = false`), adds date/weekday attributes; `load_mode` = `rebuild` (default, overwrites the table) or `append_missing` (appends per pair only the days after its latest day in silver, carrying the last silver rate into the gap); exits with rule counts and days carried forward | `silver_ecb_exchange_rates` |
| `nb_silver_taxi_zones` | reads the `taxi_zone_lookup` table and the `taxi_zones` shapefile from `lh_bronze`, keeps one row per `location_id` (rules in `invalid_zone_rules`), converts the EPSG:2263 polygons to a longitude/latitude GeoJSON `MultiPolygon` per zone (own `.shp`/`.dbf` reader and Lambert conformal conic inverse from the `.prj`, because the Fabric runtime has no geo libraries), overwrites the table; exits with rule counts and zones without boundary | `silver_taxi_zones` |
| `pl_silver_startup` | builds silver from scratch, every notebook with `load_mode = rebuild`: `nb_silver_taxi_zones` then `nb_silver_air_quality`; `nb_silver_taxi_trips`, `nb_silver_ecb_exchange_rates`, `nb_silver_worldbank_indicators` in parallel | every `silver_*` table |
| `pl_silver_air_quality` | hourly schedule; `nb_silver_air_quality` with `append_missing` and `tables` = `sensors,hourly`, plus `daily` in the run whose NYC hour is `daily_load_hour` (default 2) | missing finished hours and days |
| `pl_silver_taxi_trips` | monthly schedule; `nb_silver_taxi_trips` with `append_missing` | missing months |
| `pl_silver_ecb_exchange_rates` | daily schedule; `nb_silver_ecb_exchange_rates` with `append_missing` | missing days |
| `pl_silver_worldbank_indicators` | yearly (or monthly) schedule; `nb_silver_worldbank_indicators` with `append_missing` | missing years |
| `wh_gold` (Warehouse) | gold layer, star schema for reporting | `DimDate`, `DimZone`, `DimFX`, `DimGDP`, `FactTaxiDaily`, `FactAirQualityDaily` |
| `nb_gold_star_schema` | reads the `lh_silver` tables, builds the star schema below (one `GOLD_TABLES` entry per table: its build function and append rule; only the selected tables are built) and writes the tables in `tables` (empty for all) to `wh_gold` with the Spark connector for Fabric Data Warehouse (`synapsesql`, Runtime 1.3); `load_mode` = `rebuild` (default, overwrites each table) or `append_missing` (appends only rows newer than the latest period in `wh_gold`: `DateKey`, `DateKey` and `HourKey` for `FactAirQualityHourly`, `Year` per country for `DimGDP`; the watermark is read with a T-SQL query; `air_quality_settle_hours` (default 3) keeps the newest hourly air quality hours back until late OpenAQ readings are in silver); exits with rows written per table | `wh_gold.dbo.*` |
| `pl_gold_hourly` | hourly schedule; `nb_gold_star_schema` with `append_missing` and `tables` = `FactAirQualityHourly` | missing settled hours |
| `pl_gold_daily` | daily schedule; `nb_gold_star_schema` with `append_missing` and `tables` = `DimDate,DimFX,FactAirQualityDaily` | missing days |
| `pl_gold_monthly` | monthly schedule; `nb_gold_star_schema` with `append_missing` and `tables` = `FactTaxiDaily,FactTaxiHourly,DimGDP` | missing taxi days, missing GDP years |
| `sm_gold` (SemanticModel) | Direct Lake model (TMDL) on `wh_gold`: relationships, `DimDate` marked as date table, hidden fact columns, measures in display folders | measures below |
| `rpt_nyc_insights` (Report) | PBIR report on `sm_gold`, generated by `scripts/build_report.py`; five pages, one per Phase 4 dashboard plus Time of Day | pages below |

## Silver column types

Set in the silver notebooks, applied by `nb_silver_delta_tables`. Columns not listed keep `string`, `double`, `date`,
`timestamp` or `boolean`.

| Table | `tinyint` | `smallint` | `int` | `CHAR(3)` | `VARCHAR(n)` |
|---|---|---|---|---|---|
| `silver_taxi_trips` | `vendor_id`, `passenger_count`, `rate_code_id`, `payment_type`, `pickup_month`, `pickup_hour`, `pickup_day_of_week` | `pickup_year`, `pickup_location_id`, `dropoff_location_id` | | | |
| `silver_air_quality_sensors` | | `taxi_zone_id` | `location_id`, `sensor_id` | | `parameter_name` (5), `parameter_units` (15), `parameter_display_name` (15) |
| `silver_air_quality_hourly`, `_daily` | `measured_hour` (hourly), `measured_month`, `measured_day_of_week` | `taxi_zone_id`, `measured_year` | `sensor_id` | | `parameter_name` (5), `parameter_units` (15) |
| `silver_worldbank_indicators` | | `year` | | `indicator_name`, `currency_name`, `country_iso3` | |
| `silver_ecb_exchange_rates` | `rate_month`, `rate_day_of_week` | `rate_year` | | `base_currency`, `quote_currency` | |

* Parquet has no `CHAR`/`VARCHAR`: text is always UTF-8 bytes, the length lives in the Delta schema. `tinyint` and
  `smallint` are INT32 with an `INT(8)`/`INT(16)` annotation.
* `CHAR(n)` pads shorter values with spaces, so it holds fixed length codes only; names and units are `VARCHAR(n)`.
* The SQL analytics endpoint shows `tinyint` as `smallint` (SQL `tinyint` is unsigned); Power BI reads every integer
  as Int64.
* `silver_worldbank_indicators` fails its write for an indicator whose name without currency is longer than 3
  characters (`GDP per capita`); widen `TEXT_TYPES` before loading one.

## Gold star schema (`wh_gold`)

Tables, grain and every column with its type and source: [docs/data_dictionary.md](docs/data_dictionary.md#gold-wh_gold).

```
DimDate ──< FactTaxiDaily >── DimZone
   │  └───< FactAirQualityDaily >──┘
   ├────< FactTaxiHourly >────────┤
   │         ├── DimHour           │
   │         └── DimTripEnd        │
   ├────< FactAirQualityHourly >──┘
   ├── DimFX   (DateKey)
   └── DimGDP  (Year)
```

* Hourly facts use the NYC wall-clock hour of silver (trip end time, `measured_hour`). The hour repeated when daylight
  saving time ends is one row holding both hours; hourly air quality has no coverage threshold (each row is one reading).

* EUR amounts divide each day's US$ total by that day's ECB rate; a day without a rate has empty EUR columns.
* Air quality keeps sensor days with `percent_complete >= min_percent_complete` (default 75, as EPA). Its zone is the
  reading's `taxi_zone_id` from `silver_air_quality_daily`. Taxi trips carry only zone ids, no coordinates, so both facts slice by the same
  `DimZone`; a sensor outside every zone (New Jersey, water) gets `ZoneKey = -1`.
* Taxi facts hold each trip twice, once per `DimTripEnd`: at its pickup zone and time, and at its dropoff zone and
  time. A trip end whose zone is missing from `DimZone` gets `ZoneKey = -1`. Trip ends after the last pickup day
  (hour for `FactTaxiHourly`) in silver wait for the next run, so `append_missing` never skips the dropoffs of trips
  still to come.
* Warehouse constraints are not enforced in Fabric and the connector creates the tables, so the relationships above
  belong in the semantic model.
* `overwrite` keeps the existing table definition. After a column change in `nb_gold_star_schema`, drop that table in
  `wh_gold` before the next run.
* `FactAirQualityHourly` holds settled hours only: hours that started at least `air_quality_settle_hours` (default 3,
  the hourly OpenAQ ingestion's `lookback_hours`) before the current hour, so late readings are in silver first.
  Its append watermark is `DateKey * 100 + HourKey`; the second copy of the hour repeated when daylight saving time
  ends arrives after that hour is loaded, so it waits for the next `rebuild`.
* `append_missing` never rewrites a row: silver rows that arrive for a day (or GDP year) already in `wh_gold` are
  skipped until the next `rebuild`. `DimZone`, `DimHour` and `DimTripEnd` have no period and only rebuild.

## Power BI (`sm_gold`, `rpt_nyc_insights`)

`sm_gold` reads the `wh_gold` tables in Direct Lake mode (no import, no refresh copy). Relationships:
the four facts to `DimDate` (`DateKey`) and `DimZone` (`ZoneKey`), the hourly facts also to `DimHour` (`HourKey`),
the taxi facts also to `DimTripEnd` (`TripEndKey`), `DimFX` to `DimDate`. `DimGDP` has no relationship (`DimDate[Year]` is not unique); its measures filter it with `TREATAS`.

| Measure | Rule |
|---|---|
| `Trips`, `Trips per Day`, `Trips YoY %` | sum of `TripCount` at the selected trip end, Pickup unless exactly one end is selected (a trip never counts twice); mean over days; versus `SAMEPERIODLASTYEAR` |
| `Avg Fare USD`, `Avg Trip Distance (mi)`, `Avg Trip Duration (min)` | totals divided by `Trips` (weighted, never an average of daily averages) |
| `Revenue USD`, `Revenue EUR`, `Avg Revenue per Trip USD`/`EUR`, `Revenue YoY %` | `TotalAmountUsd`/`TotalAmountEur` (daily ECB rate); same trip end rule as `Trips` |
| `FX Effect EUR` | `Revenue EUR` minus `Revenue USD` converted at the mean rate of the selected period: the part of EUR revenue caused by the rate moving |
| `Avg USD per EUR` | mean EUR/USD rate of the days in context |
| `US GDP (USD)`, `US GDP Growth %`, `Revenue per US$1bn GDP` | `DimGDP` for `USA`, years from `DimDate` |
| `Avg Concentration`, `PM2.5 (µg/m³)`, `NO2 (ppm)` | mean of zone daily averages weighted by `SensorCount`; one pollutant only (units differ) |
| `Peak PM2.5 (µg/m³)`, `PM2.5 30-Day Avg`, `PM2.5 Days Above EPA Standard`, `PM2.5 YoY %` | highest sensor maximum; trailing 30 days; days whose mean is above 35 µg/m³ (EPA 24 h) |
| `Hourly Trips`, `Trips per Day in Hour`, `Busiest Hour` | `FactTaxiHourly`: sum, same trip end rule as `Trips`; trips divided by selected days with trips; hour with most trips |
| `Hourly Avg Concentration`, `Hourly PM2.5 (µg/m³)`, `Hourly NO2 (ppm)`, `Most Polluted Hour (PM2.5)` | `FactAirQualityHourly`, weighted by `SensorCount` |
| `Trips vs Pollution Correlation` (+ `PM2.5`, `NO2` variants) | on the hourly facts: Pearson correlation of trips and mean concentration, one point per day; under an hour or day part filter each day counts only those hours; blank below 30 days or for several pollutants at once |

| Page | Visuals |
|---|---|
| Mobility | date, borough and trip end (single select, Pickup or Dropoff) slicers; trips, trips per day, average fare, trips YoY cards; trips per day line; trips per day by weekday and by month (seasonality); busiest zones table sorted by trips |
| Air Quality | PM2.5, NO2, peak PM2.5, days above EPA standard cards; daily PM2.5 with 30-day average; PM2.5 by month; NO2 by borough per month; PM2.5 hotspots by zone |
| Mobility vs Air Quality | date, borough and trip end slicers; correlation cards; daily trips (columns) with PM2.5 (line); trips vs PM2.5 scatter per day; correlation matrix by zone and pollutant |
| Time of Day | date, borough and trip end slicers; busiest hour, most polluted hour cards; trips per day (columns) with PM2.5 (line) by hour; trips and NO2 by hour, weekend vs weekday; correlation matrix by day part and pollutant |
| Economic Impact | revenue USD/EUR, revenue per trip EUR, rate and FX effect cards; monthly revenue USD vs EUR; FX effect with rate; revenue vs US GDP by year; growth vs environment table by year |

* Edit the report in `scripts/build_report.py` (pages, visuals, positions), then regenerate:
  ```bash
  .venv/Scripts/python scripts/build_report.py
  ```
  `tests/test_power_bi.py` fails when the committed report differs from the script, when a visual uses a field missing
  from `sm_gold`, when a DAX reference does not resolve, or when `sm_gold` columns and types differ from what
  `nb_gold_star_schema` writes. Changes made in the Power BI editor and committed from the workspace are overwritten
  by the next generation; port them to the script.
* `expressions.tmdl` points Direct Lake at `wh_gold`: the database is the `wh_gold` logicalId, which fabric-cicd
  replaces with the deployed Warehouse id; the server placeholder `wh-gold-sql-endpoint.datawarehouse.fabric.microsoft.com`
  is replaced from `workspace/parameter.yml` with `$items.Warehouse.wh_gold.$sqlendpoint`. With Fabric Git integration
  neither is replaced: set both in `expressions.tmdl` to the SQL connection string and item id of `wh_gold`
  (Warehouse **Settings → SQL endpoint**, id from its URL), commit, then **Update all**.
* If `sm_gold` reports missing credentials after the first deploy, open its **Settings → Gateway and cloud
  connections**, edit the `wh_gold` data source and sign in (OAuth2).

## Documentation

* [docs/adding_a_dataset.md](docs/adding_a_dataset.md): checklist for a new open dataset (weather, traffic sensors,
  energy) through bronze, silver, gold, the semantic model and the report, with every file it touches.
* [docs/lineage.md](docs/lineage.md): source to report page, as a Mermaid flowchart.
* [docs/data_dictionary.md](docs/data_dictionary.md): every bronze, silver and gold column with its type, meaning and
  source column.
* [docs/governance.md](docs/governance.md): classification, licences, secrets, access, retention, quality rules,
  change management and refresh.
* [docs/openaq_ingestion.md](docs/openaq_ingestion.md): why the OpenAQ fallback exists, and when to switch.

## Using the downloader

```python
notebookutils.notebook.run("nb_http_download", 1800, {
    "url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "target_folder": "Files/nyctaxi_yellow",
    "file_name": "",
    "headers_json": "{}",
})
```

It exits with `{"url", "destination", "content_type", "size_bytes"}`. If `file_name` is empty, the name comes from the
URL path, and an extension is added from `Content-Type` when the path has none
(`.../EXR/D.USD.EUR.SP00.A?format=csvdata` becomes `D.USD.EUR.SP00.A.csv`). An invalid URL raises `InvalidUrlError`;
an HTTP status of 400 or above, or an unreachable host, raises `UrlNotFoundError`.

## References between items

Every item points to `lh_bronze` without hard-coded workspace or lakehouse GUIDs:

| Item | Reference to `lh_bronze` |
|---|---|
| notebooks (`META dependencies.lakehouse`) | `default_lakehouse` = `lh_bronze` logicalId `d8e27bc1-d18a-4eb0-b5cb-a0d3f492a089`, `default_lakehouse_workspace_id` = `00000000-0000-0000-0000-000000000000` (current workspace) |
| pipeline sinks | `artifactId` = the same logicalId, `workspaceId` = `00000000-0000-0000-0000-000000000000` |
| dataflow destinations | relative reference: `workspaceId = "."` and `lakehouseName = "lh_bronze"` |

Fabric Git sync and fabric-cicd both resolve logicalId + zero workspace to the item in the target workspace. Keep
`lh_bronze.Lakehouse/.platform` in the repo: its logicalId is what the others depend on. If you create `lh_bronze` by
hand in the workspace before syncing, the logicalIds won't match.

### HTTP connection

The two Copy sources in `pl_ingest_taxi` ship **without** a connection (no `externalReferences`). Connections are
tenant objects, not workspace items, and Fabric rejects a Git update whose pipeline references a connection the user
cannot access (`RequestValidationFailed: User does not have access to the connection used in the Pipeline`). A
placeholder id therefore can't work.

Once, after the first sync:

1. **Settings → Manage connections and gateways → New → Cloud**: type **HTTP**,
   URL `https://d37ci6vzurychx.cloudfront.net/`, authentication **Anonymous**.
2. Open `pl_ingest_taxi`, select that connection in **Copy trip file** (inside the ForEach) and in
   **Copy taxi zone lookup**, then save.
3. **Source control → Commit**. Git then holds the real connection id, and later syncs keep it.

## Deploy

### Fabric Git integration

1. Connect the workspace to this repo with folder `workspace`, then **Update all**.
2. Set the HTTP connection (see above).
3. Open each dataflow once, bind credentials, and **Save**. Fabric requires a manual save after Git sync.

### fabric-cicd

1. Make sure the pipeline in Git holds a connection you can access (see above). For per-environment connections,
   set `find_value` in `workspace/parameter.yml` to the connection id committed in the pipeline, and `DEV` (and other
   environments) to the id each environment should use.
2. Sign in to the Azure CLI with the **same account and tenant** you use in the Fabric portal. That account needs the
   Contributor, Member or Admin role on the workspace. The tenant id is the `ctid` in the Fabric portal URL, or under
   **? → About Microsoft Fabric → Tenant URL**.
   ```bash
   az login --tenant <fabric-tenant-id> --allow-no-subscriptions
   ```
3. Publish:
   ```bash
   .venv/Scripts/python scripts/deploy.py --workspace-id <workspace-guid> --tenant-id <fabric-tenant-id> --environment DEV
   ```
   `403 InsufficientPrivileges` on `GET /v1/workspaces/<id>` means the CLI identity can't see that workspace: wrong
   account, wrong tenant, or no workspace role.
4. Dataflow connections are bound from Git: `connections` in each `queryMetadata.json` points at an existing Fabric
   connection (`ClusterId` = the connection's `gatewayId`, `DatasourceId` = its id, both from `GET /v1/connections`).
   Each dataflow needs one Anonymous Web connection for its host (`data-api.ecb.europa.eu`, `api.worldbank.org`,
   `api.openaq.org`) and one OAuth2 Lakehouse connection for the `lh_bronze` destination. The ids committed are the
   DEV ones; for another environment add `find_replace` entries for them in `workspace/parameter.yml`.
5. `df_openaq_nyc` runs in public parameter mode (`"parametric": true` in `queryMetadata.json`), so the API key is
   never typed in. `pl_ingest_openaq` reads it from Key Vault with a Web activity (secure output), keeps it in the
   `openaq_api_key` variable (secure output) and passes `@variables('openaq_api_key')` to the dataflow activity (secure
   input). The Web activity uses a Web v2 connection to `https://fabric-project-secrets.vault.azure.net/` with token
   audience `https://vault.azure.net`; the identity behind that connection needs **Key Vault Secrets User** on the vault.

## Run order

1. `pl_ingest_taxi`: the first run (no `nyctaxi_<taxi_type>` trips folder yet) copies every month from 2016-03 to the
   latest published month, now minus `publish_lag_months` (default 2, TLC publishes about two months late). Then
   schedule it monthly with no parameters: each run copies only that latest month. Schedule it late in the month
   (TLC usually publishes mid-month), otherwise the Copy fails with 404. A month missed by a skipped run is not
   filled in later; rerun after deleting the trips folder, or copy that month by hand.
2. `df_ecb_exchange_rates` and `df_worldbank_indicators` (tables)
3. `pl_ingest_openaq`: run it once with `run_backfill = true`, which runs only `nb_ingest_openaq` from
   `backfill_date_from` (default `2016-03-01`) to yesterday, resuming each sensor after the rows already loaded.
   Then schedule it hourly (`lookback_hours = 3`) and daily (`lookback_hours = 48`, `compact_tables = true`): each run starts only
   `df_openaq_nyc` with the Key Vault key, which resumes each sensor from its watermark and appends only rows not
   loaded yet. See [docs/openaq_ingestion.md](docs/openaq_ingestion.md#pipeline-pl_ingest_openaq).
   A dataflow with a required public parameter can't be refreshed on its own schedule; schedule the pipeline instead.
4. `pl_silver_startup`: run once after bronze is loaded (and again after changing rules, columns or zone boundaries).
   It rebuilds every silver table from all of bronze: `nb_silver_taxi_zones` first (sensors read `silver_taxi_zones`
   for `taxi_zone_id`), then `nb_silver_air_quality`; trips, exchange rates and World Bank indicators in parallel.
5. Then schedule the incremental pipelines. Each runs its notebook with `load_mode = append_missing`, appends only the
   periods missing at the end of silver and never rewrites a row; a period that reaches bronze after a newer one was
   loaded for the same key is skipped until the next `pl_silver_startup`. An outdated table schema fails the run with
   `OutdatedTableError`: run `pl_silver_startup`.
   * `pl_silver_air_quality`: hourly, after `pl_ingest_openaq`, never while bronze OpenAQ tables are being written.
     Every run adds new sensors and the finished hours missing per sensor; the run in NYC hour `daily_load_hour` also adds
     the finished days missing per sensor. Set `daily_load_hour` after the daily `pl_ingest_openaq` run.
   * `pl_silver_taxi_trips`: monthly, after `pl_ingest_taxi`; appends the bronze months after the latest silver month.
   * `pl_silver_ecb_exchange_rates`: daily, after `df_ecb_exchange_rates`.
   * `pl_silver_worldbank_indicators`: after `df_worldbank_indicators` (the World Bank publishes once a year).
   * `nb_silver_taxi_zones` has no incremental mode; rerun `pl_silver_startup` when zones change.
6. `nb_silver_taxi_trips` keeps `load_mode = overwrite_months` for repairs: `months = 2026-09` overwrites that month.
7. `nb_gold_star_schema`: no parameters, rebuilds every `wh_gold` table from `lh_silver`; run it once after
   `pl_silver_startup` (and again after changing a gold column or rule, or to pick up late silver rows). The workspace
   must use Runtime 1.3 for the Warehouse connector to write. `DimDate` gained `MonthStartDate`: drop
   `wh_gold.dbo.DimDate` once before the next run, `overwrite` keeps the old definition. Money columns became
   `decimal(18,2)` (fares and totals in `FactTaxiDaily`, `GdpUsd` in `DimGDP`): drop `wh_gold.dbo.FactTaxiDaily` and
   `wh_gold.dbo.DimGDP` once before the next run too. `DimHour`, `FactTaxiHourly`
   and `FactAirQualityHourly` are new tables, so the first run creates them. The taxi facts gained trip ends
   (`PickupZoneKey` became `ZoneKey`, plus `TripEndKey`): drop `wh_gold.dbo.FactTaxiDaily` and
   `wh_gold.dbo.FactTaxiHourly` once before the next run; `DimTripEnd` is new. `sm_gold` reads the new rows directly
   (Direct Lake), no refresh needed unless automatic updates are off.
   Then schedule the gold pipelines; each appends only the periods after the latest one in `wh_gold`, so a skipped run
   is caught up by the next one:
   * `pl_gold_hourly`: hourly, after each `pl_silver_air_quality` run (for example at minute 45).
   * `pl_gold_daily`: daily, after `pl_silver_ecb_exchange_rates` and after the `pl_silver_air_quality` run in NYC hour
     `daily_load_hour` (for example 03:30 NYC). `DimDate` is in it so the new year's days appear on 1 January.
   * `pl_gold_monthly`: monthly, after `pl_silver_taxi_trips` and `pl_silver_worldbank_indicators`.
8. Every `nb_silver_*` notebook ends with `VACUUM`: overwrite leaves the old parquet files in `Tables/` for time
   travel, vacuum deletes those older than `vacuum_retention_hours` (default 168, the Delta minimum).

## Local tests

The notebook cells run locally with a fake `notebookutils`:

```bash
.venv/Scripts/python -m pytest tests -q
```
