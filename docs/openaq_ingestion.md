# OpenAQ ingestion: Dataflow Gen2 first, notebook fallback

Two implementations write the **same three tables**, so anything downstream does not care which one ran:

| Table | Grain |
|---|---|
| `bronze_openaq_locations` | one row per NYC location active in the window (locked scope start 2016-03-01 up to `DateTo`, today when empty) |
| `bronze_openaq_sensors` | one row per sensor of those locations, filtered to pollutant parameters |
| `bronze_openaq_sensor_days` | one row per sensor per day (`/v3/sensors/{id}/days`) |

## Call pattern (both implementations)

1. `GET /v3/locations?bbox=-74.2591,40.4774,-73.7002,40.9162&limit=1000&page=n`
   (`bbox` is minX,minY,maxX,maxY; the alternative is `coordinates=40.7128,-74.0060&radius=25000`, max radius 25 km).
2. Sensors come from the `sensors` array that each location already returns. Calling
   `/v3/locations/{id}/sensors` would cost one request per location for the same sensor ids.
3. `GET /v3/sensors/{id}/days?date_from=...&date_to=...&limit=1000&page=n` per sensor.
   The full locked scope (2016-03-01 to today) is about 3,900 days, so at most 4 pages per sensor.
   The hourly endpoint would need about 94 pages per sensor for the same window.

Pagination stops when a page returns fewer than `limit` rows, so a sensor with ≤ 1000 days costs one call.

Requests per run ≈ `1 + Σ ceil(days_with_data / 1000)` over sensors, at most `1 + 4 × number_of_sensors` for the full scope.
At 50 requests/min, 300 sensors take about 24 minutes. Locations that were not active in the
window are dropped before any sensor calls, and sensors outside `PollutantParameters` are skipped.

## Dataflow Gen2 (`df_openaq_nyc`)

- `OpenAQGetAllPages(relativePath, query)` uses `List.Generate` over `page`, with `limit=1000`.
- Every request goes through `Function.InvokeAfter` with a 1.2 s delay, which keeps it to about 50 requests a minute.
  An HTTP 429 gets one retry after 60 s (`ManualStatusHandling = {429}`, `IsRetry = true`). If that also returns 429,
  the refresh fails with `OpenAQ.RateLimited`.
- `Web.Contents` always uses the static base URL plus `RelativePath`/`Query`, so service refresh is allowed.
- `fastCombine` is `true` because `bronze_openaq_sensor_days` references another query *and* calls `Web.Contents`.

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

### Decision rule

Run `df_openaq_nyc` first. Switch to `nb_ingest_openaq` (for example, schedule the notebook instead of the
dataflow) if any of these happens:

- the refresh fails with `OpenAQ.RateLimited` or HTTP 429,
- it fails with `Formula.Firewall` even with privacy combining allowed,
- refresh time is not acceptable for the date window you need,
- policy forbids storing the API key in the dataflow definition.

## Running the notebook

| Parameter | Default | Meaning |
|---|---|---|
| `openaq_key_vault_url` / `openaq_secret_name` | `""` / `openaq-api-key` | Key Vault holding the API key (preferred) |
| `openaq_api_key` | `""` | plain key, used only when no Key Vault URL is given |
| `nyc_bbox` | `-74.2591,40.4774,-73.7002,40.9162` | minX,minY,maxX,maxY |
| `date_from` / `date_to` | `2016-03-01` / `""` | window for `/days`; earlier than 2016-03-01 is rejected, empty `date_to` means today |
| `raw_root_folder` | `Files/OpenAQ` | raw JSON Lines landing |
| `pollutant_parameters` | `pm25,pm10,no2,o3,so2,co` | empty string keeps every sensor |
| `requests_per_minute` | `50` | client-side throttle, under the 60/min limit |

Tables are overwritten on each run, which matches the dataflow's Replace destination.
