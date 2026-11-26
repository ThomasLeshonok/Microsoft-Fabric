# Governance

Policies for the data in this workspace: what it is, who may use it and how, where secrets live, how long data is
kept, how quality is enforced, how changes ship and when data refreshes. Tables and columns:
[data_dictionary.md](data_dictionary.md); item and table flow: [lineage.md](lineage.md).

## Data classification

| Class | Applies to | Handling |
|---|---|---|
| Public open data | every table and file in `lh_bronze`, `lh_silver`, `wh_gold`, `sm_gold`, `rpt_nyc_insights` | no access restriction needed beyond the workspace; keep the source attribution (see *Licences*) on anything published |
| Secret | the OpenAQ API key | Key Vault only, see *Secrets* |

* **No PII is processed for analysis.** Taxi trip records carry times, taxi zones, distances and amounts, no passenger
  or driver identity. Exchange rates, GDP and air quality readings describe currencies, countries and sensors.
* One bronze column is close to personal data: `bronze_openaq_locations.owner_name` (owners of low-cost sensors, some
  are personal nicknames). `nb_silver_air_quality` drops it, so it never reaches silver, gold or the report. Don't add
  it back.
* Sensor coordinates (`latitude`, `longitude`) describe a monitoring site, not a person, and are already public on
  OpenAQ.

## Licences

Checked on the providers' sites on 2026-11-25. Recheck before publishing outside the organisation: terms change.

| Source | Data | Terms | Our handling |
|---|---|---|---|
| NYC Taxi & Limousine Commission | yellow trip records, `taxi_zone_lookup.csv`, `taxi_zones.zip` | The TLC trip data page names no open licence; the NYC.gov terms of use apply (content provided "as is", no warranty). TLC states the trip data comes from the technology providers and makes no representation as to its accuracy. NYC publishes agency data open by default under its Open Data Law. | Credit "NYC Taxi & Limousine Commission"; treat accuracy as unwarranted (the silver rules exist for that). Before redistributing the data outside the organisation, confirm the terms on the dataset's NYC Open Data page. |
| OpenAQ | locations, sensors, daily and hourly measurements | No single licence: OpenAQ aggregates openly licensed data, and each provider's own licence applies (the API lists them under `licenses`). Data is offered as is, without warranty; an API key is required, scraping is forbidden, and the hosted API may not be used to build a product that substantially duplicates OpenAQ. | Credit OpenAQ and the original provider (`provider_name`); check the licence of each provider before redistributing. |
| World Bank | `NY.GDP.MKTP.CD` (GDP, current US$) | Creative Commons Attribution 4.0 (CC BY 4.0) unless a dataset is labelled otherwise, plus the World Bank dataset terms: no implied endorsement, no use of the World Bank name or logos; datasets and APIs provided without warranty. | Credit in the form "The World Bank: [dataset name]: [data source]", here World Development Indicators, GDP (current US$). |
| European Central Bank | `EXR` `D.USD.EUR.SP00.A` reference rates | ESCB policy on the reuse of statistics: free reuse, commercial or not, if the source is credited and the statistics and metadata are not modified; third-party data is excluded. | Credit "Source: ECB statistics". Published rates are kept unchanged; the rates silver fills in for weekends and holidays are derived values and are marked `is_published = false`. |

Pages read: [nyc.gov TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
[NYC.gov terms of use](https://www.nyc.gov/home/terms-of-use.page),
[OpenAQ terms of use](https://docs.openaq.org/about/terms),
[World Bank dataset terms](https://www.worldbank.org/ext/en/legal/terms-conditions/datasets),
[ECB reuse policy](https://www.ecb.europa.eu/stats/ecb_statistics/governance_and_quality_framework/html/usage_policy.en.html).

## Secrets

* The only secret is the OpenAQ API key: secret `openaq-api-key` in the Key Vault
  `https://fabric-project-secrets.vault.azure.net/`.
* `pl_ingest_openaq` reads it with a Web activity over a Web v2 connection (token audience `https://vault.azure.net`),
  with secure output, keeps it in the `openaq_api_key` variable (secure output) and passes it to `df_openaq_nyc` as a
  public parameter (secure input). The key is never typed into the dataflow and never stored in Git.
* `nb_ingest_openaq` reads it with `notebookutils.credentials.getSecret(openaq_key_vault_url, openaq_secret_name)`.
  Its `openaq_api_key` parameter is for local debugging only; leave it empty in Fabric.
* The identity behind the Key Vault connection needs **Key Vault Secrets User** on the vault and nothing more.
* Connections (HTTP, Web, Lakehouse) are tenant objects bound in Fabric, not secrets in the repo. `parameter.yml`
  holds only connection ids and endpoints per environment.

## Access

* Access is by Fabric workspace role (Admin, Member, Contributor, Viewer). Grant Viewer to report consumers,
  Contributor to people who run or edit pipelines and notebooks, Member or Admin only to workspace owners.
* The identity that deploys with `fabric-cicd` needs Contributor, Member or Admin on the target workspace.
* `sm_gold` reads `wh_gold` in Direct Lake mode through its data source credentials (OAuth2), so report users see the
  whole star schema their role allows.
* **Row-level security is not implemented.** Every report user sees every zone, day and pollutant. All data is public
  (see *Data classification*), so no role-specific views are required today.

## Retention

| Data | Kept | Mechanism |
|---|---|---|
| Bronze raw files (trip parquet, taxi zone shapefile, OpenAQ JSONL) | indefinitely | never deleted by any item; they allow every silver table to be rebuilt |
| Bronze tables | indefinitely, append-only | no item deletes or updates bronze rows; `nb_compact_bronze_tables` rewrites small files (`OPTIMIZE`) without changing rows |
| Old Delta file versions (bronze OpenAQ facts, every silver table) | 168 hours | `VACUUM ... RETAIN 168 HOURS` in `nb_compact_bronze_tables` and after every silver load except `append_missing` (`vacuum_after_load`); 168 hours is the Delta minimum, so time travel reaches back 7 days |
| Silver tables | as long as bronze | rebuilt from bronze by `pl_silver_startup` |
| Gold tables | as long as silver | rebuilt from silver by `nb_gold_star_schema` with `load_mode = rebuild` |

## Quality rules

Every silver notebook names its rules in `invalid_*_rules`: a dict of rule name to a never-null condition. A row hit by
any rule is dropped (`is_invalid` in `nb_silver_delta_tables`); then one row per key is kept. Every run exits
(`notebookutils.notebook.exit`, visible in the pipeline run output) with a report per table or month: `rows_read`,
`rows_dropped_by_rule` (hits per rule; a row can hit several), `duplicates_removed` (valid rows not written) and
`rows_written`.

| Notebook | Rules | Deduplication | Extra report fields |
|---|---|---|---|
| `nb_silver_taxi_trips` | `invalid_trip_rules`: `missing_timestamp`, `no_nyc_zone` (both zones 264/265), `unrealistic_flex_fare_distance` (flex fare over 100 miles), `stationary_trip`, `zero_distance_and_amount`, `voided_payment`, `pickup_outside_file_month`, `dropoff_outside_file_year`, `dropoff_before_pickup` | one row per `TRIP_KEY`; the original trip wins over its refund reversal | one report per month (`month`) |
| `nb_silver_taxi_zones` | `invalid_zone_rules`: `missing_location_id` | one row per `location_id` | `zones_without_boundary` |
| `nb_silver_ecb_exchange_rates` | `invalid_rate_rules`: `missing_key`, `non_daily_frequency`, `missing_rate`, `non_positive_rate` | latest `ingested_at` per pair and day | `days_carried_forward` |
| `nb_silver_worldbank_indicators` | `invalid_indicator_rules`: `missing_key`, `non_annual_period`, `missing_value`, `unknown_currency` | latest `ingested_at` per indicator, country and year | - |
| `nb_silver_air_quality` | `invalid_reading_rules` (hourly and daily): `missing_key`, `missing_value`, `negative_value`, `unsupported_parameter_unit` (`SUPPORTED_UNITS`), `unknown_sensor`, `unfinished_period`; `invalid_sensor_rules`: `missing_key`, `missing_location`, `unsupported_parameter_unit`, `outside_taxi_zones` | latest `ingested_at` per sensor and hour, sensor and day, sensor, location | one report per table |

Enforced as well:

* **Schema.** Silver tables are created from typed DDL (`nb_silver_delta_tables`): a too long `CHAR(n)`/`VARCHAR(n)`
  value or an out-of-range integer fails the write instead of being cut. An outdated table schema fails
  `append_missing` with `OutdatedTableError` until `pl_silver_startup` rebuilds it.
* **Bronze.** Dataflows append only keys not loaded yet; the World Bank dataflow waits for published values
  (`value` not null).
* **Gold.** Air quality sensor days below `min_percent_complete` (75%, as EPA) are left out of `FactAirQualityDaily`;
  `FactAirQualityHourly` takes settled hours only (`air_quality_settle_hours`). Unknown zones map to `ZoneKey = -1`
  instead of being dropped.

## Change management

* **Git is the source of truth.** Every item lives in `workspace/` in the Fabric Git format. Work happens on `dev`;
  `master` is the main branch. A change made in the Fabric UI is committed from the workspace (**Source control →
  Commit**) or ported to the repo; the report is only edited through `scripts/build_report.py`.
* **Deploy** with Fabric Git integration (**Update all**) or `fabric-cicd` (`scripts/deploy.py`, environment values in
  `workspace/parameter.yml`). Items reference each other by `logicalId`, never by a workspace GUID, so the same commit
  deploys to any workspace.
* **Tests are the gate.** Run `.venv/Scripts/python -m pytest tests -q` before every commit and deploy; there is no CI
  server. The suite runs the notebook code on local Spark (rules, cleaning, load modes, gold builders), checks pipeline
  parameters against notebook parameters, item references by `logicalId`, dataflow append keys and connections, item
  description length, that `sm_gold` columns and types match what `nb_gold_star_schema` writes, and that the committed
  report equals the generator's output.
* **Schema changes.** A silver schema change needs `pl_silver_startup` (rebuild). A gold column change needs the
  Warehouse table dropped once before the next run (`overwrite` keeps the old definition), and the TMDL updated.

## Refresh

Schedules are configured in the Fabric UI, not in the repo. The intended cadence, from each item's `.platform`
description and the README *Run order*:

| Item | Cadence | After |
|---|---|---|
| `pl_ingest_taxi` | monthly, late in the month | TLC publication (about two months late) |
| `df_ecb_exchange_rates` | daily | ECB publication (TARGET business days) |
| `df_worldbank_indicators` | yearly (or monthly) | World Bank publication |
| `pl_ingest_openaq` | hourly (`lookback_hours = 3`) and daily (`lookback_hours = 48`, `compact_tables = true`) | - |
| `pl_silver_air_quality` | hourly | `pl_ingest_openaq`; adds days in NYC hour `daily_load_hour` |
| `pl_silver_taxi_trips` | monthly | `pl_ingest_taxi` |
| `pl_silver_ecb_exchange_rates` | daily | `df_ecb_exchange_rates` |
| `pl_silver_worldbank_indicators` | yearly (or monthly) | `df_worldbank_indicators` |
| `pl_gold_hourly` | hourly | `pl_silver_air_quality` |
| `pl_gold_daily` | daily | `pl_silver_ecb_exchange_rates` and the daily `pl_silver_air_quality` run |
| `pl_gold_monthly` | monthly | `pl_silver_taxi_trips`, `pl_silver_worldbank_indicators` |
| `pl_silver_startup`, `nb_gold_star_schema` (`rebuild`) | on demand | after rule or schema changes |

`sm_gold` is Direct Lake: it reads new `wh_gold` rows without a refresh unless automatic updates are turned off.
