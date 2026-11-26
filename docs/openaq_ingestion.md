# OpenAQ ingestion: Dataflow Gen2 first, notebook fallback

Two implementations write the **same four tables**, so anything downstream does not care which one ran:

| Table | Grain |
|---|---|
| `bronze_openaq_locations` | one row per NYC location in the bounding box |
| `bronze_openaq_sensors` | one row per sensor of those locations, filtered to pollutant parameters |
| `bronze_openaq_sensor_days` | one row per sensor per day (`/v3/sensors/{id}/days`), OpenAQ's own daily aggregate |
| `bronze_openaq_sensor_measurements` | one row per raw reading (`/v3/sensors/{id}/measurements`), as reported by the provider |

Coordinates live on `bronze_openaq_locations` only. OpenAQ returns `coordinates: null` on every measurement and day of
a fixed monitor, so the reading tables carry `location_id` instead: join on it for the position.

Silver builds its own hourly/daily aggregates from `bronze_openaq_sensor_measurements`. `bronze_openaq_sensor_days`
stays as a reconciliation check against OpenAQ's numbers.

### Reading the timestamps

Every row carries its period: `datetime_from_*` (start) and `datetime_to_*` (end). OpenAQ labels an hourly reading by the
**end** of the hour, and the OpenAQ Explorer CSV export shows `datetime_to` as `datetimeUtc`/`datetimeLocal`. Example:
the reading exported as `2017-01-02T23:00:00-05:00` covers 22:00-23:00 local time.

A row in `/days` for local day D summarizes the readings whose period ends between D 01:00 and D+1 00:00, local time.
Its `datetime_from_utc` is local midnight (05:00Z for New York in winter), so it is not an hourly timestamp.

## Call pattern (both implementations)

1. `GET /v3/locations?bbox=-74.2591,40.4774,-73.7002,40.9162&limit=1000&page=n`
   (`bbox` is minX,minY,maxX,maxY; the alternative is `coordinates=40.7128,-74.0060&radius=25000`, max radius 25 km).
2. Sensors come from the `sensors` array that each location already returns. Calling
   `/v3/locations/{id}/sensors` would cost one request per location for the same sensor ids.
3. `GET /v3/sensors/{id}/days?date_from=...&date_to=...&limit=1000&page=n` per sensor.
   The full locked scope (2016-03-01 to today) is about 3,900 days, so at most 4 pages per sensor.
4. `GET /v3/sensors/{id}/measurements?datetime_from=...&datetime_to=...&limit=1000&page=n` per sensor and per
   calendar-year UTC window (`2017-01-01T00:00:00Z` to `2018-01-01T00:00:00Z`). The pagination guide asks for
   "a single year or less" on `/measurements`. Each window is clipped to the location's
   `datetimeFirst`/`datetimeLast`, and a reading that shows up in two adjacent windows is kept once
   (key: `sensor_id` + `datetime_from_utc`).
   Volume depends on the provider's interval: a reference monitor (AirNow, hourly) is about 8,760 rows per year,
   so about 9 pages per sensor-year and about 95 for the full scope. Low-cost sensors that report every few minutes
   produce 10-30 times more.

Pagination stops when a page returns fewer than `limit` rows, so a sensor with ≤ 1000 days costs one call.

Requests per run ≈ `1 + Σ ceil(days_with_data / 1000) + Σ over sensor-years ceil(readings / 1000)`.
The daily part is at most `1 + 4 × number_of_sensors`. The raw part dominates: 300 hourly sensors over the full scope
are about 28,000 requests. At OpenAQ's 2,000 requests/hour limit, that is roughly 15 hours for the first backfill.
Later runs are incremental (notebook) and only fetch new readings. Locations that were not active in the
window are dropped before any sensor calls, and sensors listed in `ExcludedParameters` (temperature, humidity) are
skipped. Every other parameter (pm1, pm25, pm10, um003, no2, o3, ...) is loaded.

## Dataflow Gen2 (`df_openaq_nyc`)

- `OpenAQGetAllPages(relativePath, query)` uses `List.Generate` over `page`, with `limit=1000`.
- Every request goes through `Function.InvokeAfter` with a 1.2 s delay, which keeps it to about 50 requests a minute.
  HTTP 429, 500, 502, 503 and 504 are handled manually (`ManualStatusHandling`) and a page gets up to 4 attempts:
  60 s wait after a 429, 15/30/45 s after a 5xx. OpenAQ answers sporadic 500s on pages that succeed when asked again.
  Later attempts set `IsRetry = true` so `Web.Contents` does not replay its cached failed response. If the last attempt
  still fails, the refresh fails with `OpenAQ.RateLimited` (429) or `OpenAQ.ServerError` (5xx).
- `Web.Contents` always uses the static base URL plus `RelativePath`/`Query`, so service refresh is allowed.
- `fastCombine` is `true` because every `bronze_openaq_*` query references another query *and* reads `lh_bronze`
  or calls `Web.Contents`. `df_ecb_exchange_rates` and `df_worldbank_indicators` set it too: each query combines
  `Web.Contents` with its own `lh_bronze` table.
- `bronze_openaq_sensor_measurements` builds calendar-month windows with `OpenAQMonthWindows(fetch_from, fetch_to)` and
  pages each one. Months, not years: OpenAQ pages by offset and its query times out after about 6 s, so on a year
  window page 5 and later (offset 4000+) of a dense hourly sensor always answer HTTP 408. A month of hourly data
  (at most 744 rows) fits in one page.
  HTTP 408 is not retried (the same request times out again) and the dataflow does not split windows, so a 408
  on a sensor-month fails the refresh.
- `bronze_openaq_locations` and `bronze_openaq_sensors` append only locations (`location_id`) and sensors (`sensor_id`)
  not in the table yet (`OpenAQWithoutExistingKeys`). `OpenAQFetchPlan` reads every sensor from `openaq_sensors_raw`,
  not from `bronze_openaq_sensors`, which holds only the new ones of a run.
- Every destination uses Manual settings with
  `UpdateMethod = [Kind = "Append"]` and explicit `ColumnSettings` mappings. Rows already in the table are never
  rewritten.

### Incremental load (`LookbackHours`)

Each refresh decides per sensor what to fetch, from the table it writes to. There is no date parameter.

1. **Watermark.** `OpenAQFetchPlan` reads `max(datetime_to_utc)` per `sensor_id` from the destination table
   (`OpenAQBronzeTable`, the same `lh_bronze` with folding on, so the group-by runs in the SQL analytics endpoint).
2. **Window.** Measurements: from watermark − `LookbackHours` to now. Days: from the watermark to now −
   `LookbackHours`, and only once a whole day fits, so a local day is loaded after it has been closed for
   `LookbackHours`. The start is never earlier than 30 days back (`CatchUpLimit`). A sensor with no rows yet starts
   30 days back too; its older history comes from the notebook backfill.
3. **Skip.** A sensor is not called when its location's `datetime_last_utc` is not newer than the sensor's watermark.
   Dead locations cost no request.
4. **Insert only new keys.** `OpenAQOnlyNewRows` anti-joins the fetched rows with the keys already in the table
   (`sensor_id`, `datetime_from_utc`, read from the earliest fetched timestamp on). That is
   `INSERT ... ON CONFLICT DO NOTHING`: overlap from the lookback, a rerun or a pipeline retry adds nothing.

Consequences:

- A missed run or an outage up to 30 days heals itself on the next run. Longer outages leave a gap: run the notebook
  with `measurements_full_reload = True`.
- Late readings are picked up while they are within `LookbackHours` of the sensor's newest loaded reading.
- Source corrections of a row already loaded are not picked up (DO NOTHING). A Lakehouse destination of Dataflow
  Gen2 only does Append or Replace, so an upsert (DO UPDATE on changed hash) would need the notebook and a Delta
  `MERGE`.
- The day aggregate is frozen when first loaded. With hourly runs at `LookbackHours = 3` a day is loaded 3 h after
  local midnight, even if the daily run uses a longer lookback.
- The SQL analytics endpoint can lag a fresh Delta commit by a few minutes. A rerun inside that lag may append the
  same keys again; silver still deduplicates on (`sensor_id`, `datetime_from_utc`).

## Pipeline (`pl_ingest_openaq`)

One pipeline, two modes, chosen by `run_backfill`:

1. Set Variable `load_day` = `day`, or yesterday (UTC) when `day` is empty. Only the backfill uses it.
2. **Incremental** (`run_backfill = false`, the default): a Web activity reads the key from Key Vault, Set Variable
   stores it in `openaq_api_key` (secure output), and `df_openaq_nyc` runs once with
   `LookbackHours = lookback_hours` (secure input). A failed refresh is retried twice, 30 min apart; retries add no
   duplicates. The notebook is not started.
3. **Backfill** (`run_backfill = true`): only `nb_ingest_openaq` runs, from `backfill_date_from` to `load_day`
   (24 h activity timeout). It reads the key from Key Vault itself. The dataflow is not started, so `load_day` is
   not loaded twice.
4. **Compaction** (after a backfill, or when `compact_tables = true`): `nb_compact_bronze_tables` runs `OPTIMIZE`
   on `bronze_openaq_sensor_days` and `bronze_openaq_sensor_measurements`, packing files smaller than 1 MB
   (`target_file_size_bytes`) into files of at most 1 MB, then `VACUUM` with 168 h retention. Every dataflow refresh
   appends a few KB per table, so without it the tables grow into thousands of tiny parquet files. It starts a Spark
   session, so hourly runs skip it.
5. `concurrency` is 1, so scheduled incremental runs cannot overlap each other or a backfill.

| Parameter | Default | Meaning |
|---|---|---|
| `run_backfill` | `false` | `true` runs the notebook backfill instead of the incremental dataflow |
| `backfill_date_from` | `2016-03-01` | first day of the backfill |
| `day` | `""` | last day of the backfill; empty means yesterday |
| `lookback_hours` | `3` | incremental runs: how far behind each sensor's watermark to fetch again (`LookbackHours`) |
| `compact_tables` | `false` | `true` compacts the two fact tables after the load; a backfill always compacts |

- **First use:** run once with `run_backfill = true`. Measurements already in the table (for example from earlier
  dataflow runs) are not fetched again: each sensor resumes after its latest `datetime_from_utc`. `sensor_days` is
  replaced only inside `backfill_date_from`..`day`, so a later `backfill_date_from` (for example `2024-01-01`) keeps
  the older days.
- **After that:** schedule the pipeline hourly with `lookback_hours = 3`, and daily with `lookback_hours = 48` and
  `compact_tables = true` to catch late readings and compact the day's small files. Both only append rows not loaded yet. One sensor-window is one request whatever the lookback, so a
  single hourly schedule with `lookback_hours = 48` costs about the same API calls.
- **Missed runs:** nothing to do for outages up to 30 days; the next run resumes from each sensor's watermark.

### Limits

Sizing (NYC bbox, 2016-03 to 2026-11): 74 locations, 31 to 68 sensors per month in the six pollutants, 5,377
sensor-months: one `/measurements` request per sensor-month, one `/days` request per active sensor and run.

- **OpenAQ:** 60 requests/minute and 2,000/hour per key. The notebook keeps a sliding budget of 1,900/hour, so the
  full backfill (about 7,500 requests) takes about 4 hours. An incremental run is about one request per sensor with new readings, plus one `/days` request per sensor and day.
- **Fabric capacity:** an Fn capacity has n CU, so n × 86,400 CU-seconds per day (the DEV trial is `FTL4`:
  345,600). Background jobs are smoothed over 24 hours; once the booked usage goes past 24 hours of capacity, every
  new job is rejected with `CapacityLimitExceeded` until it burns down. A dataflow refresh is billed for every second
  its queries run, waiting on OpenAQ included, which is why a backfill through the dataflow (about 10,000 CU-seconds
  per 3 months) exhausted `FTL4`. The notebook pays for its Spark session instead, and an incremental dataflow run for a few
  hours is a few thousand CU-seconds. Check real costs in the Fabric Capacity Metrics app.

## Why the notebook fallback exists (`nb_ingest_openaq`)

Power Query can do this, but it pushes back in these places:

1. **Rate limits across parallel evaluation.** OpenAQ allows 60 requests/min and 2,000/hour per key.
   `Function.InvokeAfter` only spaces out calls inside one evaluation chain. Dataflow Gen2 evaluates queries in parallel,
   and it can evaluate the same query more than once (validation, staging, destination write). Call volume therefore
   grows faster than the throttle assumes, and 429s show up on bigger windows.
2. **Retry semantics.** The retries built into `Web.Contents` are opaque: you cannot set the count, the backoff,
   or which headers it reads. Taking control (`ManualStatusHandling`) means writing "wait and try again" by hand in M. The notebook gets retry with backoff and `Retry-After` from a
   `urllib3.Retry` session (`build_session` in `nb_http_download`), plus its own rate limiter shared by every call.
3. **Formula.Firewall.** A query that references another query and also reads a web source raises
   `Formula.Firewall: Query references other queries or steps, so it may not directly access a data source`
   unless privacy-level combining is allowed for the whole dataflow.
4. **Secret handling.** OpenAQ wants the key in an `X-API-Key` header. The Web connector's API-key credential cannot
   be mapped to a custom header, so the key ends up as a dataflow parameter stored in the item definition (and in Git). The notebook reads it from Azure Key Vault with `notebookutils.credentials.getSecret`.
5. **Debuggability and replay.** The notebook lands every raw result as JSON Lines under
   `Files/OpenAQ/<entity>/run_date=<date>/`, so a run can be rebuilt without calling the API again.
   Its pagination, filtering and casting logic is unit tested (`tests/test_notebooks.py`).
6. **Raw measurement volume.** Only the notebook loads `bronze_openaq_sensor_measurements` incrementally.
   It lands each sensor-window as raw JSON Lines, appends one Delta commit per sensor, keeps the client under
   2,000 requests/hour, and splits a window in half when OpenAQ answers HTTP 408 (down to one day). A run that
   stops halfway resumes after the last sensor it appended.

### Decision rule

`pl_ingest_openaq` uses both: the notebook for the one-time history (resumable, 408 splitting, hourly budget, no
dataflow compute while it waits on the API) and the dataflow for the incremental runs. Schedule the notebook daily instead
of the pipeline only if the dataflow keeps failing with `OpenAQ.RateLimited` or `Formula.Firewall`.

## Running the notebook

| Parameter | Default | Meaning |
|---|---|---|
| `openaq_key_vault_url` / `openaq_secret_name` | `https://fabric-project-secrets.vault.azure.net/` / `openaq-api-key` | Key Vault holding the API key (preferred) |
| `openaq_api_key` | `""` | plain key, used only when no Key Vault URL is given |
| `nyc_bbox` | `-74.2591,40.4774,-73.7002,40.9162` | minX,minY,maxX,maxY |
| `date_from` / `date_to` | `2016-03-01` / `""` | window for `/days` and `/measurements`; earlier than 2016-03-01 is rejected, empty `date_to` means today |
| `raw_root_folder` | `Files/OpenAQ` | raw JSON Lines landing |
| `excluded_parameters` | `temperature,relativehumidity` | sensors with these parameters are skipped; empty string keeps every sensor |
| `requests_per_minute` | `50` | client-side throttle, under the 60/min limit |
| `requests_per_hour` | `1900` | sliding one-hour budget, under the 2,000/hour limit |
| `measurements_full_reload` | `False` | `True` drops `bronze_openaq_sensor_measurements` and loads the whole window again |

`bronze_openaq_locations` and `bronze_openaq_sensors` are overwritten on each run (the dataflow only appends new ones).
`bronze_openaq_sensor_days` is replaced only for `date_local` inside `date_from`..`date_to` (Delta `replaceWhere`);
days outside the window, for example those the incremental dataflow appended, stay. The notebook writes the same columns
the dataflow's Append destinations map, so both can write the two fact tables.

Dataflow Gen2 only writes Delta tables on the legacy protocol (reader 1, writer 2). Fabric Spark enables deletion
vectors on every table it creates, which moves the table to table features (reader 3, writer 7, with `appendOnly`
and `invariants` listed explicitly), and the next refresh fails with `Lakehouse036`: "We can't write to this table
because we don't support these features: invariants." The notebook therefore creates missing tables with
`DATAFLOW_COMPATIBLE_TABLE_PROPERTIES` (deletion vectors off, protocol 1/2). Writes into an existing table keep its
protocol. A protocol cannot be downgraded in place, so a table Spark already created with table features has to be
rebuilt. In a notebook attached to `lh_bronze`, per table (`bronze_openaq_sensor_days`, then
`bronze_openaq_sensor_measurements`), with no pipeline running:

```sql
CREATE TABLE bronze_openaq_sensor_days_rebuild AS SELECT * FROM bronze_openaq_sensor_days;
-- compare SELECT COUNT(*) of both tables before going on
DROP TABLE bronze_openaq_sensor_days;
CREATE TABLE bronze_openaq_sensor_days
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2')
AS SELECT * FROM bronze_openaq_sensor_days_rebuild;
DROP TABLE bronze_openaq_sensor_days_rebuild;
DESCRIBE DETAIL bronze_openaq_sensor_days;  -- minReaderVersion 1, minWriterVersion 2
```

`bronze_openaq_sensor_measurements` is append-only. For each sensor, the run reads the latest `datetime_from_utc`
already in the table and asks for readings from that point. Readings at or before it are dropped. Raw pages land as
`Files/OpenAQ/sensor_measurements/run_date=<date>/sensor_id=<id>/<window start>.jsonl`; the table gets one append
per sensor with all its windows. A sensor interrupted halfway is fetched again from its watermark.
Moving `date_from` earlier does not backfill sensors that already have rows; use `measurements_full_reload = True`.
Bronze does not deduplicate beyond that. Readings that OpenAQ revises later are not picked up again.
Silver should still deduplicate on `sensor_id` + `datetime_from_utc`.

The first full backfill runs for hours; the pipeline's Notebook activity has a 24 h timeout. If it times out or
fails, run it again: it resumes per sensor.
