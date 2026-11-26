# Data dictionary

Every column of every bronze, silver and gold table, with its type, meaning and source column. Types are Delta (Spark
SQL) types for bronze and silver and the types `nb_gold_star_schema` writes to `wh_gold` for gold. How the tables
connect: [lineage.md](lineage.md). Which rows are dropped on the way: the quality rules in
[governance.md](governance.md#quality-rules).

Conventions:

* Bronze is append-only. Every table loaded by a dataflow or by `nb_ingest_openaq` has `ingested_at`
  (`timestamp`, UTC time of the load); silver keeps the row with the latest `ingested_at` per key.
* Times called `_utc` are UTC. `pickup_datetime` and `measured_at` are the NYC wall clock (no offset), and so are
  the dates and hours derived from them.
* Day of week is `1` = Monday to `7` = Sunday everywhere.
* In silver, `tinyint`, `smallint`, `CHAR(n)` and `VARCHAR(n)` are the declared Delta types (see *Silver column types*
  in the README); Power BI reads every integer as Int64.

## Bronze (`lh_bronze`)

### Files

| Path | Written by | Content |
|---|---|---|
| `Files/trips/nyctaxi_yellow/yyyy/MM/yellow_tripdata_yyyy-MM.parquet` | `pl_ingest_taxi` | TLC yellow taxi trip records, one file per month, as published (`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, the fare components, `total_amount`). The column set and types vary by year; `nb_silver_taxi_trips` maps them to one schema. |
| `Files/nyctaxi_yellow/taxi_zones/taxi_zones.shp`, `.shx`, `.dbf`, `.prj` | `nb_download_zip_members` | TLC taxi zone polygons (NY State Plane, EPSG:2263, US feet); the `.dbf` holds `LocationID`, `zone`, `borough`. |
| `Files/OpenAQ/<entity>/run_date=<date>/<entity>.jsonl` | `nb_ingest_openaq` | raw OpenAQ API responses of a backfill run, one JSON object per line; `sensor_measurements` split per `sensor_id=<id>/<window>.jsonl`. |

### `taxi_zone_lookup`

Written by `pl_ingest_taxi` (Copy, append) from `misc/taxi_zone_lookup.csv`. No `ingested_at`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `LocationID` | string | TLC taxi zone id | CSV `LocationID` |
| `Borough` | string | borough (`EWR` for Newark, `Unknown`, `N/A`) | CSV `Borough` |
| `Zone` | string | zone name | CSV `Zone` |
| `service_zone` | string | service area (`Yellow Zone`, `Boro Zone`, `Airports`, `EWR`) | CSV `service_zone` |

### `bronze_ecb_exchange_rates`

Written by `df_ecb_exchange_rates` from the ECB `EXR` series `D.USD.EUR.SP00.A`, `format=csvdata`. Every column is the
CSV column of the same name in upper case (`KEY`, `FREQ`, ...). Key: `key`, `time_period`.

| Column | Type | Meaning |
|---|---|---|
| `key` | string | SDMX series key, `EXR.D.USD.EUR.SP00.A` |
| `freq` | string | frequency, `D` = daily |
| `currency` | string | quoted currency, `USD` |
| `currency_denom` | string | base currency, `EUR` |
| `exr_type` | string | exchange rate type, `SP00` = spot |
| `exr_suffix` | string | series variation, `A` |
| `time_period` | date | observation day |
| `obs_value` | double | rate: units of `currency` per 1 `currency_denom` |
| `obs_status` | string | observation status |
| `obs_conf` | string | observation confidentiality |
| `obs_pre_break` | string | value before a series break |
| `obs_com` | string | observation comment |
| `time_format` | string | time format code |
| `breaks` | string | series break note |
| `collection` | string | collection indicator |
| `compiling_org` | string | compiling organisation |
| `diss_org` | string | disseminating organisation |
| `dom_ser_ids` | string | domestic series ids |
| `publ_ecb` | string | ECB publication reference |
| `publ_mu` | string | euro area publication reference |
| `publ_public` | string | public publication reference |
| `unit_index_base` | string | index base |
| `compilation` | string | compilation note |
| `coverage` | string | coverage note |
| `decimals` | string | display decimals |
| `nat_title` | string | title in the national language |
| `source_agency` | string | source agency |
| `source_pub` | string | publication source |
| `title` | string | series title |
| `title_compl` | string | title complement |
| `unit` | string | unit, `USD` |
| `unit_mult` | string | unit multiplier |
| `ingested_at` | timestamp | load time |

### `bronze_worldbank_indicators`

Written by `df_worldbank_indicators` from `v2/country/USA/indicator/NY.GDP.MKTP.CD` (JSON, second array element).
Only published values (`value` not null). Key: `indicator_id`, `country_iso3`, `period`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `indicator_id` | string | indicator code, `NY.GDP.MKTP.CD` | `indicator.id` |
| `indicator_name` | string | indicator name, `GDP (current US$)` | `indicator.value` |
| `country_id` | string | ISO 3166-1 alpha-2 country | `country.id` |
| `country_name` | string | country name | `country.value` |
| `country_iso3` | string | ISO 3166-1 alpha-3 country | `countryiso3code` |
| `period` | string | period, `yyyy` for annual values | `date` |
| `value` | double | indicator value | `value` |
| `unit` | string | unit (empty) | `unit` |
| `obs_status` | string | observation status (empty) | `obs_status` |
| `decimals` | bigint | display decimals | `decimal` |
| `ingested_at` | timestamp | load time | - |

### `bronze_openaq_locations`

Written by `df_openaq_nyc` (or `nb_ingest_openaq`) from `/v3/locations` in the NYC bounding box. Key: `location_id`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `location_id` | bigint | OpenAQ location id | `id` |
| `location_name` | string | location name | `name` |
| `locality` | string | locality | `locality` |
| `timezone` | string | IANA time zone | `timezone` |
| `country_code` | string | ISO 3166-1 alpha-2 country | `country.code` |
| `country_name` | string | country name | `country.name` |
| `provider_name` | string | data provider (AirNow, AirGradient, Clarity, ...) | `provider.name` |
| `owner_name` | string | sensor owner | `owner.name` |
| `is_mobile` | boolean | mobile location | `isMobile` |
| `is_monitor` | boolean | regulatory reference monitor (versus low-cost sensor) | `isMonitor` |
| `latitude` | double | WGS84 latitude | `coordinates.latitude` |
| `longitude` | double | WGS84 longitude | `coordinates.longitude` |
| `datetime_first_utc` | string | first measurement, ISO 8601 UTC | `datetimeFirst.utc` |
| `datetime_last_utc` | string | latest measurement, ISO 8601 UTC | `datetimeLast.utc` |
| `ingested_at` | timestamp | load time | - |

### `bronze_openaq_sensors`

From the `sensors` of each location. Key: `sensor_id`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `sensor_id` | bigint | OpenAQ sensor id | `id` |
| `sensor_name` | string | parameter and unit, `pm25 µg/m³` | `name` |
| `location_id` | bigint | location of the sensor | `locationId` (the parent location) |
| `parameter_id` | bigint | OpenAQ parameter id | `parameter.id` |
| `parameter_name` | string | pollutant code, `pm25`, `no2`, ... | `parameter.name` |
| `parameter_units` | string | unit, `µg/m³`, `ppm`, ... | `parameter.units` |
| `parameter_display_name` | string | display name, `PM2.5` | `parameter.displayName` |
| `ingested_at` | timestamp | load time | - |

### `bronze_openaq_sensor_days`

From `/v3/sensors/{id}/days`. Key: `sensor_id`, `date_local`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `sensor_id` | bigint | sensor | `sensorId` (the requested sensor) |
| `location_id` | bigint | location | `locationId` (the sensor's location) |
| `date_local` | date | local day | `period.datetimeFrom.local` (date part) |
| `value` | double | daily mean, rounded | `value` |
| `parameter_name` | string | pollutant code | `parameter.name` |
| `parameter_units` | string | unit | `parameter.units` |
| `datetime_from_utc` | string | day start, UTC | `period.datetimeFrom.utc` |
| `datetime_from_local` | string | day start, local with offset | `period.datetimeFrom.local` |
| `datetime_to_utc` | string | day end, UTC | `period.datetimeTo.utc` |
| `min`, `q25`, `median`, `q75`, `max`, `avg`, `sd` | double | daily statistics of the hourly values | `summary.*` |
| `observed_count` | bigint | hours with a value | `coverage.observedCount` |
| `expected_count` | bigint | hours expected | `coverage.expectedCount` |
| `percent_complete` | double | `observed_count / expected_count` in percent | `coverage.percentComplete` |
| `has_flags` | boolean | the provider flagged the value | `flagInfo.hasFlags` |
| `ingested_at` | timestamp | load time | - |

### `bronze_openaq_sensor_measurements`

From `/v3/sensors/{id}/measurements` in monthly windows. Key: `sensor_id`, `datetime_from_utc`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `sensor_id` | bigint | sensor | `sensorId` (the requested sensor) |
| `location_id` | bigint | location | `locationId` (the sensor's location) |
| `value` | double | measured value | `value` |
| `parameter_name` | string | pollutant code | `parameter.name` |
| `parameter_units` | string | unit | `parameter.units` |
| `period_label` | string | aggregation, always `raw` | `period.label` |
| `period_interval` | string | period length, `01:00:00` | `period.interval` |
| `datetime_from_utc` | string | hour start, UTC | `period.datetimeFrom.utc` |
| `datetime_from_local` | string | hour start, local with offset | `period.datetimeFrom.local` |
| `datetime_to_utc` | string | hour end, UTC | `period.datetimeTo.utc` |
| `datetime_to_local` | string | hour end, local with offset | `period.datetimeTo.local` |
| `has_flags` | boolean | the provider flagged the value | `flagInfo.hasFlags` |
| `ingested_at` | timestamp | load time | - |

## Silver (`lh_silver`)

Built by the `nb_silver_*` notebooks; column lists come from each notebook's `normalize_*` function and casts.
"Derived" means computed in silver, not copied.

### `silver_taxi_trips`

`nb_silver_taxi_trips` from the bronze trip files. One row per trip (`TRIP_KEY`). Partitioned by `pickup_year`,
`pickup_month`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `vendor_id` | tinyint | TPEP technology provider code | `VendorID` |
| `pickup_datetime` | timestamp | meter engaged, NYC wall clock | `tpep_pickup_datetime` |
| `dropoff_datetime` | timestamp | meter disengaged, NYC wall clock | `tpep_dropoff_datetime` |
| `passenger_count` | tinyint | passengers, as entered by the driver | `passenger_count` |
| `trip_distance` | double | trip distance in miles | `trip_distance` |
| `rate_code_id` | tinyint | final rate code | `RatecodeID` |
| `pickup_location_id` | smallint | TLC pickup zone | `PULocationID` |
| `dropoff_location_id` | smallint | TLC drop-off zone | `DOLocationID` |
| `payment_type` | tinyint | payment code (`0` flex fare, `6` voided) | `payment_type` |
| `fare_amount` | double | time and distance fare in US$, made positive | `fare_amount` (`abs`) |
| `total_amount` | double | amount charged in US$ (without cash tips), made positive | `total_amount` (`abs`) |
| `pickup_date` | date | pickup day | derived from `pickup_datetime` |
| `pickup_year` | smallint | pickup year | derived |
| `pickup_month` | tinyint | pickup month | derived |
| `pickup_hour` | tinyint | pickup hour, 0 to 23 | derived |
| `pickup_day_of_week` | tinyint | 1 = Monday | derived |
| `is_weekend` | boolean | Saturday or Sunday | derived |
| `trip_duration_minutes` | double | drop-off minus pickup, 2 decimals | derived |

Dropped from bronze: `store_and_fwd_flag` and the fare components (`extra`, `mta_tax`, `tip_amount`,
`tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `Airport_fee`, ...).

### `silver_taxi_zones`

`nb_silver_taxi_zones` from `taxi_zone_lookup` and the `taxi_zones` shapefile. One row per `location_id`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `location_id` | int | TLC taxi zone id | `taxi_zone_lookup.LocationID` |
| `borough` | string | borough | `taxi_zone_lookup.Borough` (trimmed) |
| `zone` | string | zone name | `taxi_zone_lookup.Zone` (trimmed) |
| `service_zone` | string | service area | `taxi_zone_lookup.service_zone` (trimmed) |
| `boundary_geojson` | string | zone boundary as a GeoJSON `MultiPolygon` in longitude/latitude; null for 264 and 265 | `taxi_zones.shp` polygons, joined on `.dbf` `LocationID`, projected with the `.prj` |

### `silver_ecb_exchange_rates`

`nb_silver_ecb_exchange_rates` from `bronze_ecb_exchange_rates`. One row per currency pair and calendar day.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `base_currency` | CHAR(3) | ISO 4217 base currency, `EUR` | `currency_denom` (trimmed, upper case) |
| `quote_currency` | CHAR(3) | ISO 4217 quoted currency, `USD` | `currency` (trimmed, upper case) |
| `rate_date` | date | calendar day | `time_period`, plus the days in between |
| `exchange_rate` | double | units of quote currency per 1 base currency (US$ per euro) | `obs_value`; on unpublished days the last published value |
| `is_published` | boolean | false = weekend or TARGET holiday carried forward | derived |
| `rate_year` | smallint | year | derived |
| `rate_month` | tinyint | month | derived |
| `rate_day_of_week` | tinyint | 1 = Monday | derived |
| `is_weekend` | boolean | Saturday or Sunday | derived |

### `silver_worldbank_indicators`

`nb_silver_worldbank_indicators` from `bronze_worldbank_indicators`. One row per indicator, country and year.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `indicator_id` | string | indicator code | `indicator_id` |
| `indicator_name` | CHAR(3) | indicator name without its currency, `GDP` | `indicator_name` before `(current ...)` |
| `currency_name` | CHAR(3) | ISO 4217 currency of the value, `USD` | `indicator_name` symbol through `CURRENCY_CODES` |
| `country_iso3` | CHAR(3) | ISO 3166-1 alpha-3 country | `country_iso3` |
| `country_name` | string | country name | `country_name` |
| `year` | smallint | year of the value | `period` (annual periods only) |
| `value` | double | indicator value | `value` |
| `period_start_date` | date | 1 January of `year` | derived |
| `period_end_date` | date | 31 December of `year` | derived |

### `silver_air_quality_sensors`

`nb_silver_air_quality` from `bronze_openaq_sensors` joined to `bronze_openaq_locations`. One row per sensor inside a
taxi zone.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `location_id` | int | OpenAQ location | `sensors.location_id` |
| `sensor_id` | int | OpenAQ sensor | `sensors.sensor_id` |
| `parameter_name` | VARCHAR(5) | pollutant code | `sensors.parameter_name` |
| `parameter_units` | VARCHAR(15) | unit, one per pollutant (`SUPPORTED_UNITS`) | `sensors.parameter_units` |
| `parameter_display_name` | VARCHAR(15) | display name | `sensors.parameter_display_name` |
| `location_name` | string | location name | `locations.location_name` |
| `provider_name` | string | data provider | `locations.provider_name` |
| `is_reference_monitor` | boolean | regulatory monitor (AirNow) versus low-cost sensor | `locations.is_monitor` |
| `latitude` | double | WGS84 latitude | `locations.latitude` |
| `longitude` | double | WGS84 longitude | `locations.longitude` |
| `first_measured_at_utc` | timestamp | first measurement | `locations.datetime_first_utc` |
| `last_measured_at_utc` | timestamp | latest measurement | `locations.datetime_last_utc` |
| `taxi_zone_id` | smallint | TLC zone whose boundary contains the location | derived from `silver_taxi_zones.boundary_geojson` |

### `silver_air_quality_hourly`

`nb_silver_air_quality` from `bronze_openaq_sensor_measurements`. One row per sensor and hour. Partitioned by
`measured_year`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `sensor_id` | int | sensor | `sensor_id` |
| `taxi_zone_id` | smallint | the sensor's taxi zone | `silver_air_quality_sensors.taxi_zone_id` |
| `parameter_name` | VARCHAR(5) | pollutant code | `parameter_name` |
| `parameter_units` | VARCHAR(15) | unit | `parameter_units` |
| `value` | double | measured value | `value` |
| `has_flags` | boolean | the provider flagged the value | `has_flags` |
| `measured_at_utc` | timestamp | hour start, UTC | `datetime_from_utc` |
| `measured_at` | timestamp | hour start, NYC wall clock | `datetime_from_local` without its offset |
| `measured_date` | date | NYC day | derived from `measured_at` |
| `measured_hour` | tinyint | NYC hour, 0 to 23 | derived |
| `measured_year` | smallint | year | derived |
| `measured_month` | tinyint | month | derived |
| `measured_day_of_week` | tinyint | 1 = Monday | derived |
| `is_weekend` | boolean | Saturday or Sunday | derived |
| `ingested_at` | timestamp | bronze load time of the kept row | `ingested_at` |

### `silver_air_quality_daily`

`nb_silver_air_quality` from `bronze_openaq_sensor_days`. One row per sensor and day. Partitioned by
`measured_year`.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `sensor_id` | int | sensor | `sensor_id` |
| `taxi_zone_id` | smallint | the sensor's taxi zone | `silver_air_quality_sensors.taxi_zone_id` |
| `parameter_name` | VARCHAR(5) | pollutant code | `parameter_name` |
| `parameter_units` | VARCHAR(15) | unit | `parameter_units` |
| `measured_date` | date | NYC day | `date_local` |
| `avg_value` | double | daily mean | `avg` |
| `min_value` | double | daily minimum | `min` |
| `median_value` | double | daily median | `median` |
| `max_value` | double | daily maximum | `max` |
| `percent_complete` | double | share of hours with a value, percent | `percent_complete` |
| `has_flags` | boolean | the provider flagged the value | `has_flags` |
| `measured_year` | smallint | year | derived |
| `measured_month` | tinyint | month | derived |
| `measured_day_of_week` | tinyint | 1 = Monday | derived |
| `is_weekend` | boolean | Saturday or Sunday | derived |
| `ingested_at` | timestamp | bronze load time of the kept row | `ingested_at` |

## Gold (`wh_gold`)

Built by `nb_gold_star_schema`, one `GOLD_TABLES` entry per table. Relationships, notes and load rules: *Gold star
schema* in the [README](../README.md#gold-star-schema-wh_gold).

| Table | Grain | Built from |
|---|---|---|
| `DimDate` | calendar day, 2016-01-01 to 31 December of the current year | generated |
| `DimHour` | hour of the day, 0 to 23 | generated |
| `DimTripEnd` | trip end: Pickup or Dropoff | generated |
| `DimZone` | TLC taxi zone, plus `ZoneKey = -1` Unknown | `silver_taxi_zones` |
| `DimFX` | currency pair and day | `silver_ecb_exchange_rates` |
| `DimGDP` | country and year, `NY.GDP.MKTP.CD` | `silver_worldbank_indicators` |
| `FactTaxiDaily` | day, zone and trip end | `silver_taxi_trips`, `silver_ecb_exchange_rates` |
| `FactTaxiHourly` | day, hour, zone and trip end | `silver_taxi_trips` |
| `FactAirQualityDaily` | day, zone and pollutant | `silver_air_quality_daily` |
| `FactAirQualityHourly` | day, hour, zone and pollutant | `silver_air_quality_hourly` |

### `DimDate`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | `yyyyMMdd` | generated day |
| `Date` | date | day | generated |
| `Year` | int | year | `Date` |
| `Quarter` | int | quarter, 1 to 4 | `Date` |
| `Month` | int | month, 1 to 12 | `Date` |
| `MonthName` | string | `January` ... | `Date` |
| `MonthStartDate` | date | first day of the month (monthly report axes) | `Date` |
| `DayOfMonth` | int | day of the month | `Date` |
| `DayOfWeek` | int | 1 = Monday | `Date` |
| `DayName` | string | `Monday` ... | `Date` |
| `WeekOfYear` | int | ISO week | `Date` |
| `IsWeekend` | boolean | Saturday or Sunday | `Date` |

### `DimHour`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `HourKey` | int | hour, 0 to 23 | generated |
| `HourLabel` | string | `08:00` | `HourKey` |
| `DayPart` | string | Night 0-5, Morning rush 6-9, Midday 10-15, Evening rush 16-19, Evening 20-23 | `DAY_PARTS` |
| `DayPartOrder` | int | sort order of `DayPart`, 1 to 5 | `DAY_PARTS` |

### `DimTripEnd`

Each trip is in the taxi facts twice, once per end: Pickup counts trips by where and when they start, Dropoff by where
and when they end. Summed over both ends every trip counts twice, so the `sm_gold` trip measures use Pickup unless
exactly one end is selected.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `TripEndKey` | int | `1` Pickup, `2` Dropoff | `TRIP_ENDS` |
| `TripEnd` | string | `Pickup` or `Dropoff` | `TRIP_ENDS` |

### `DimZone`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `ZoneKey` | int | TLC `LocationID`; `-1` = Unknown | `location_id` |
| `Borough` | string | borough; `Airports` for Newark 1, JFK 132, LaGuardia 138 | `borough` |
| `Zone` | string | zone name | `zone` |
| `ServiceZone` | string | service area | `service_zone` |
| `City` | string | `New York City` for the five boroughs, else `Outside New York City` | `borough` |

### `DimFX`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | day | `rate_date` |
| `BaseCurrency` | string | `EUR` | `base_currency` |
| `QuoteCurrency` | string | `USD` | `quote_currency` |
| `ExchangeRate` | double | US$ per euro | `exchange_rate` |
| `IsPublished` | boolean | false = carried forward from the last ECB publication | `is_published` |

### `DimGDP`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `Year` | smallint | year | `year` |
| `CountryIso3` | string | ISO 3166-1 alpha-3 country | `country_iso3` |
| `CountryName` | string | country name | `country_name` |
| `GdpUsd` | decimal(18,2) | GDP, current US$ | `value` where `indicator_id = NY.GDP.MKTP.CD` |
| `GdpGrowthPercent` | double | change versus the previous year in percent; null without the previous year | `value` |

### `FactTaxiDaily`

Trip ends after the last pickup day in silver wait for the next run, so `append_missing` never skips a day.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | day of the trip end | `pickup_datetime` or `dropoff_datetime` |
| `ZoneKey` | int | zone of the trip end, `-1` when not in `DimZone` | `pickup_location_id` or `dropoff_location_id` |
| `TripEndKey` | int | `1` Pickup, `2` Dropoff | `TRIP_ENDS` |
| `TripCount` | bigint | trips | count of rows |
| `PassengerCount` | bigint | passengers | sum of `passenger_count` |
| `TripDistanceMiles` | double | total miles | sum of `trip_distance` |
| `TripDurationMinutes` | double | total minutes | sum of `trip_duration_minutes` |
| `FareAmountUsd` | decimal(18,2) | total fares, US$ | sum of `fare_amount` |
| `TotalAmountUsd` | decimal(18,2) | total charged, US$ | sum of `total_amount` |
| `FareAmountEur` | decimal(18,2) | total fares in euro at the rate of the trip end's day; null without a rate | `fare_amount`, `exchange_rate` (EUR/USD) |
| `TotalAmountEur` | decimal(18,2) | total charged in euro at that day's rate | `total_amount`, `exchange_rate` |
| `AvgFareAmountUsd` | decimal(18,2) | fare per trip | `FareAmountUsd / TripCount` |
| `AvgTotalAmountUsd` | decimal(18,2) | charge per trip | `TotalAmountUsd / TripCount` |
| `AvgTotalAmountEur` | decimal(18,2) | charge per trip in euro | `TotalAmountEur / TripCount` |
| `AvgTripDistanceMiles` | double | miles per trip | `TripDistanceMiles / TripCount` |
| `AvgTripDurationMinutes` | double | minutes per trip | `TripDurationMinutes / TripCount` |

### `FactTaxiHourly`

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | day of the trip end | `pickup_datetime` or `dropoff_datetime` |
| `HourKey` | int | hour of the trip end, NYC wall clock | `pickup_datetime` or `dropoff_datetime` |
| `ZoneKey` | int | zone of the trip end, `-1` when not in `DimZone` | `pickup_location_id` or `dropoff_location_id` |
| `TripEndKey` | int | `1` Pickup, `2` Dropoff | `TRIP_ENDS` |
| `TripCount` | bigint | trips | count of rows |

### `FactAirQualityDaily`

Sensor days with `percent_complete >= min_percent_complete` (default 75) only.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | day | `measured_date` |
| `ZoneKey` | int | taxi zone, `-1` for a sensor in no zone | `taxi_zone_id` |
| `Pollutant` | string | pollutant code | `parameter_name` |
| `Unit` | string | unit | `parameter_units` |
| `SensorCount` | bigint | sensors reporting | distinct `sensor_id` |
| `AvgValue` | double | mean of the sensors' daily means | `avg_value` |
| `MinValue` | double | lowest sensor minimum | `min_value` |
| `MaxValue` | double | highest sensor maximum | `max_value` |

### `FactAirQualityHourly`

Settled hours only: hours that started at least `air_quality_settle_hours` (default 3) before the current hour.

| Column | Type | Meaning | Source |
|---|---|---|---|
| `DateKey` | int | day | `measured_date` |
| `HourKey` | int | hour, NYC wall clock | `measured_hour` |
| `ZoneKey` | int | taxi zone, `-1` for a sensor in no zone | `taxi_zone_id` |
| `Pollutant` | string | pollutant code | `parameter_name` |
| `Unit` | string | unit | `parameter_units` |
| `SensorCount` | bigint | sensors reporting | distinct `sensor_id` |
| `AvgValue` | double | mean of the sensors' hourly values | `value` |
| `MaxValue` | double | highest hourly value | `value` |
