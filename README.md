# fabric-analytics

Bronze ingestion for Microsoft Fabric: NYC taxi trips, ECB exchange rates, World Bank indicators and OpenAQ air quality.

`workspace/` uses the Fabric Git integration format (`<name>.<Type>/.platform` + definition files), so you can sync it
with workspace Git integration or publish it with `fabric-cicd` (`scripts/deploy.py`).

## Locked scope

Everything starts at **2016-03**. The pipeline drops months before `2016-03`, the OpenAQ notebook and dataflow fail if
the start date is earlier than `2016-03-01`, ECB uses `startPeriod=2016-03-01`, and World Bank uses `date=2016:<current year>`.

## `lh_bronze` Files layout

```
Files/
├── ECB_FX/            exr_daily_eur_reference_rates.csv
├── NY_GDP/            NY.GDP.PCAP.CD_page1..3.json
├── nyctaxi_yellow/
│   ├── taxi_zone_lookup.csv
│   ├── 2016/03/yellow_tripdata_2016-03.parquet
│   ├── ...
│   └── 2026/09/yellow_tripdata_2026-09.parquet
└── OpenAQ/            <entity>/run_date=<date>/<entity>.jsonl   (locations, sensors, sensor_days)
```

## Items

| Item | What it does | Output |
|---|---|---|
| `lh_bronze` (Lakehouse) | landing zone | `Files/...` above, `bronze_*` tables |
| `nb_http_download` | validates a URL (http/https, host, no whitespace), checks it exists (HEAD, falling back to a streamed GET when HEAD is refused), then streams any file type to OneLake | one file |
| `nb_download_sources` | calls `nb_http_download` via `notebookutils.notebook.run` for each entry in `sources_json`, reports every outcome, fails if any failed; by default lands the raw ECB CSV and World Bank pages | `Files/ECB_FX`, `Files/NY_GDP` |
| `pl_ingest_taxi` | `Filter` keeps the months in `months` (`yyyy-MM`, default 2016-03 … 2026-09) that fall in the locked scope, then `ForEach` HTTP Copy, plus `taxi_zone_lookup.csv` | `Files/nyctaxi_yellow/yyyy/MM/`, `Files/nyctaxi_yellow/taxi_zone_lookup.csv` |
| `df_ecb_exchange_rates` | ECB `EXR` CSV (`D..EUR.SP00.A`, from 2016-03-01) | `bronze_ecb_exchange_rates` |
| `df_worldbank_indicators` | World Bank JSON with `per_page=1000`, every page, second array element expanded | `bronze_worldbank_indicators` |
| `df_openaq_nyc` | NYC locations (bbox), then their sensors, then `/days` per sensor, paginated with `List.Generate` (`page`, `limit=1000`) | `bronze_openaq_*` |
| `nb_ingest_openaq` | notebook fallback for `df_openaq_nyc`; reuses `nb_http_download` via `%run` | same `bronze_openaq_*` tables + raw JSONL in `Files/OpenAQ` |

Why the OpenAQ fallback exists, and when to switch: [docs/openaq_ingestion.md](docs/openaq_ingestion.md).

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

## Placeholders

| Placeholder | Meaning | Where |
|---|---|---|
| `11111111-1111-1111-1111-111111111111` | workspace id | notebooks, pipeline, dataflow destinations |
| `22222222-2222-2222-2222-222222222222` | `lh_bronze` lakehouse id | notebooks, pipeline, dataflow destinations |
| `33333333-3333-3333-3333-333333333333` | HTTP connection id, base URL `https://d37ci6vzurychx.cloudfront.net/`, Anonymous | pipeline |

`workspace/parameter.yml` replaces them when you publish with fabric-cicd. With plain Git sync, re-attach `lh_bronze`
in each notebook, pick the connection in both Copy activities, and re-select the destination in each dataflow.

## Deploy

1. In Fabric, create an HTTP connection to `https://d37ci6vzurychx.cloudfront.net/` (Anonymous) and put its id in
   `parameter.yml` in place of `<http-connection-id-...>`.
2. Publish:
   ```bash
   az login
   ```
   ```bash
   .venv/Scripts/python scripts/deploy.py --workspace-id <workspace-guid> --environment DEV
   ```
3. Open each dataflow once and bind credentials (Anonymous for `data-api.ecb.europa.eu`, `api.worldbank.org`,
   `api.openaq.org`). In `df_openaq_nyc`, set the `OpenAQApiKey` parameter.
4. For `nb_ingest_openaq`, store the key in Key Vault and set `openaq_key_vault_url`.

## Run order

1. `pl_ingest_taxi`: the default `months` covers 2016-03 … 2026-09. Pass a shorter list for incremental loads
   (for example `["2026-10"]` once TLC publishes it). Months before 2016-03 are filtered out.
2. `nb_download_sources` (raw files in `Files/ECB_FX` and `Files/NY_GDP`), then `df_ecb_exchange_rates` and `df_worldbank_indicators` (tables)
3. `df_openaq_nyc`, or `nb_ingest_openaq` if the dataflow trips over rate limits or the firewall

## Local tests

The notebook cells run locally with a fake `notebookutils`:

```bash
.venv/Scripts/python -m pytest tests -q
```
