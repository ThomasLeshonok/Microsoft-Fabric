# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "80bcc451-4b85-4233-acd9-cd92e59035bf",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_gold_star_schema
#
# Builds the gold star schema from the `lh_silver` tables and overwrites it in the `wh_gold` Warehouse with the Spark
# connector for Fabric Data Warehouse (`synapsesql`, Runtime 1.3).
#
# | Table | Grain | Source |
# |---|---|---|
# | `DimDate` | one calendar day, `calendar_first_year`-01-01 to 31 December of the current year | generated |
# | `DimZone` | one TLC taxi zone, plus `ZoneKey = -1` (Unknown) | `silver_taxi_zones` |
# | `DimFX` | one currency pair and day | `silver_ecb_exchange_rates` |
# | `DimGDP` | one country and year of `NY.GDP.MKTP.CD` (GDP, current US$) | `silver_worldbank_indicators` |
# | `FactTaxiDaily` | one day, zone and trip end | `silver_taxi_trips`, `silver_ecb_exchange_rates` |
# | `FactAirQualityDaily` | one day, zone and pollutant | `silver_air_quality_daily` |
# | `DimHour` | one hour of the day, 0 to 23, with its day part | generated |
# | `DimTripEnd` | one trip end: Pickup or Dropoff | generated |
# | `FactTaxiHourly` | one day, hour, zone and trip end | `silver_taxi_trips` |
# | `FactAirQualityHourly` | one day, hour, zone and pollutant | `silver_air_quality_hourly` |
#
# * Keys: `DateKey` is `yyyyMMdd`, `ZoneKey` is the TLC `LocationID`. Facts reference `DimDate` and `DimZone`;
#   `DimFX` joins `DimDate` on `DateKey`, `DimGDP` joins `DimDate` on `Year`.
# * `DimZone`: `Borough` is `Airports` for the airport zones (`AIRPORT_ZONE_KEYS`: Newark, JFK, LaGuardia); `City`
#   still follows the TLC borough, so Newark stays outside New York City.
# * Money (fares, totals, `GdpUsd`) is `MONEY_TYPE`, fixed point, so sums never pick up floating point error.
# * Taxi facts hold each trip twice, once per trip end (`DimTripEnd`): at its pickup zone and pickup time, and at its
#   dropoff zone and dropoff time. Filter one end to count trips starting or ending in a zone: summed over both ends
#   every trip counts twice. A trip end whose zone is not in `DimZone` gets `ZoneKey = -1`. Trip ends after the last
#   pickup period in silver are left out until silver has every trip of that period, so `append_missing` never skips
#   the dropoffs of trips still to come.
# * `FactTaxiDaily`: daily totals (trips, passengers, miles, minutes, fare and total amount) and averages per trip.
#   EUR amounts use that day's ECB rate (silver carries weekends and holidays forward).
# * `FactAirQualityDaily`: sensor days below `min_percent_complete` coverage are left out (EPA uses 75%). The zone is the
#   reading's `taxi_zone_id` from silver; a sensor in no zone gets `ZoneKey = -1`. Measures:
#   sensors reporting, mean of their daily averages, lowest minimum, highest maximum.
# * Hourly facts use the NYC wall-clock hour of silver (trip end time, `measured_hour`), so both line up on `HourKey`;
#   the repeated hour when daylight saving time ends is one hour with both readings. `FactTaxiHourly`: trips.
#   `FactAirQualityHourly`: sensors reporting, mean and highest hourly value.
# * `FactAirQualityHourly` holds settled hours only: hours that started at least `air_quality_settle_hours` before the
#   current UTC hour, so OpenAQ readings that arrive late (the hourly ingestion looks back 3 hours) are in silver
#   before the hour is appended.
# * `GOLD_TABLES` is the registry: each gold table is a `GoldTable` with its `build` function, which reads the silver
#   tables it needs through `GoldInputs.silver`, and its `append_rule` (`None` = rebuild only). Only the selected
#   tables are built. A new gold table is one `build_*` function plus one `GOLD_TABLES` entry; outside this notebook it
#   also needs a TMDL table in `sm_gold` and a place in the `tables` parameter of a `pl_gold_*` pipeline.
# * `tables` = comma separated gold tables to load, empty for all. `load_mode`:
#   * `rebuild` (default) overwrites each selected table. `overwrite` keeps the existing Warehouse table definition, so
#     after a column change drop that table in `wh_gold` before the next run.
#   * `append_missing` appends only rows newer than the latest period already in `wh_gold` (the table's
#     `append_rule`: `DateKey`, `DateKey` and `HourKey` for `FactAirQualityHourly`, or `Year` per country for
#     `DimGDP`) and never rewrites a row; a missing table gets every row. `DimZone`, `DimHour` and `DimTripEnd` have
#     no period, so they only rebuild. Silver rows that arrive for a day already in gold are skipped until the next
#     `rebuild`.
#
# Exits with one report per table: rows written (rows appended for `append_missing`).

# PARAMETERS CELL ********************

warehouse = "wh_gold"
calendar_first_year = 2016
min_percent_complete = 75.0
air_quality_settle_hours = 3
tables = ""
load_mode = "rebuild"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

WAREHOUSE_SCHEMA = "dbo"
SILVER_TRIPS_TABLE = "silver_taxi_trips"
SILVER_ZONES_TABLE = "silver_taxi_zones"
SILVER_AIR_QUALITY_DAILY_TABLE = "silver_air_quality_daily"
SILVER_AIR_QUALITY_HOURLY_TABLE = "silver_air_quality_hourly"
SILVER_EXCHANGE_RATES_TABLE = "silver_ecb_exchange_rates"
SILVER_INDICATORS_TABLE = "silver_worldbank_indicators"
NYC_TIME_ZONE = "America/New_York"
REBUILD = "rebuild"
APPEND_MISSING = "append_missing"
LATEST_LOADED_PERIOD = "latest_loaded_period"
EXISTING_TABLES_QUERY = f"SELECT name FROM sys.tables WHERE SCHEMA_NAME(schema_id) = '{WAREHOUSE_SCHEMA}'"
UNKNOWN_ZONE_KEY = -1
UNKNOWN = "Unknown"
NEW_YORK_CITY = "New York City"
OUTSIDE_NEW_YORK_CITY = "Outside New York City"
NEW_YORK_CITY_BOROUGHS = ("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island")
AIRPORTS = "Airports"
AIRPORT_ZONE_KEYS = (1, 132, 138)
EURO = "EUR"
US_DOLLAR = "USD"
GDP_INDICATOR_ID = "NY.GDP.MKTP.CD"
DECIMALS = 2
MONEY_TYPE = "decimal(18,2)"
HOURS_PER_DAY = 24
HOUR_KEY_SHIFT = 100
DAY_PARTS = (
    (0, "Night"),
    (6, "Morning rush"),
    (10, "Midday"),
    (16, "Evening rush"),
    (20, "Evening"),
)
TRIP_END_MEASURES = ("passenger_count", "trip_distance", "trip_duration_minutes", "fare_amount", "total_amount")


@dataclass(frozen=True)
class GoldInputs:
    session: SparkSession
    calendar: tuple
    min_percent_complete: float
    settled_before: datetime

    def silver(self, table: str) -> DataFrame:
        return self.session.table(table)


@dataclass(frozen=True)
class AppendRule:
    period_column: str
    key_columns: tuple = ()
    hour_column: str = ""

    def period_sql(self) -> str:
        if self.hour_column:
            return f"[{self.period_column}] * {HOUR_KEY_SHIFT} + [{self.hour_column}]"
        return f"[{self.period_column}]"

    def period(self) -> Column:
        if self.hour_column:
            return F.col(self.period_column) * HOUR_KEY_SHIFT + F.col(self.hour_column)
        return F.col(self.period_column)


@dataclass(frozen=True)
class GoldTable:
    build: Callable[[GoldInputs], DataFrame]
    append_rule: AppendRule | None = None


@dataclass(frozen=True)
class TripEnd:
    key: int
    name: str
    location_column: str
    time_column: str


PICKUP = TripEnd(1, "Pickup", "pickup_location_id", "pickup_datetime")
DROPOFF = TripEnd(2, "Dropoff", "dropoff_location_id", "dropoff_datetime")
TRIP_ENDS = (PICKUP, DROPOFF)


def date_key(day: str) -> Column:
    return F.date_format(day, "yyyyMMdd").cast("int")


def _rounded(value: Column, name: str) -> Column:
    return F.round(value, DECIMALS).alias(name)


def _money(value: Column, name: str) -> Column:
    return F.round(value, DECIMALS).cast(MONEY_TYPE).alias(name)


def build_dim_date(session: SparkSession, calendar: tuple) -> DataFrame:
    first_day, last_day = calendar
    day = F.col("Date")
    days = session.range(1).select(F.explode(F.sequence(F.lit(first_day), F.lit(last_day))).alias("Date"))
    return days.select(
        date_key("Date").alias("DateKey"),
        day,
        F.year(day).alias("Year"),
        F.quarter(day).alias("Quarter"),
        F.month(day).alias("Month"),
        F.date_format(day, "MMMM").alias("MonthName"),
        F.trunc(day, "month").alias("MonthStartDate"),
        F.dayofmonth(day).alias("DayOfMonth"),
        (F.weekday(day) + 1).alias("DayOfWeek"),
        F.date_format(day, "EEEE").alias("DayName"),
        F.weekofyear(day).alias("WeekOfYear"),
        (F.weekday(day) >= 5).alias("IsWeekend"),
    )


def day_part_order(hour: int) -> int:
    return max(order for order, (first_hour, _) in enumerate(DAY_PARTS, start=1) if hour >= first_hour)


def build_dim_hour(session: SparkSession) -> DataFrame:
    hours = []
    for hour in range(HOURS_PER_DAY):
        order = day_part_order(hour)
        hours.append((hour, f"{hour:02d}:00", DAY_PARTS[order - 1][1], order))
    return session.createDataFrame(hours, "HourKey int, HourLabel string, DayPart string, DayPartOrder int")


def build_dim_trip_end(session: SparkSession) -> DataFrame:
    return session.createDataFrame([(end.key, end.name) for end in TRIP_ENDS], "TripEndKey int, TripEnd string")


def build_dim_zone(zones: DataFrame) -> DataFrame:
    borough = F.col("borough")
    zone_key = F.col("location_id")
    city = F.when(borough.isin(*NEW_YORK_CITY_BOROUGHS), NEW_YORK_CITY).otherwise(OUTSIDE_NEW_YORK_CITY)
    known = zones.select(
        zone_key.alias("ZoneKey"),
        F.when(zone_key.isin(*AIRPORT_ZONE_KEYS), AIRPORTS).otherwise(borough).alias("Borough"),
        F.col("zone").alias("Zone"),
        F.col("service_zone").alias("ServiceZone"),
        city.alias("City"),
    )
    unknown = known.sparkSession.createDataFrame([(UNKNOWN_ZONE_KEY, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN)], known.schema)
    return known.unionByName(unknown)


def build_dim_fx(rates: DataFrame) -> DataFrame:
    return rates.select(
        date_key("rate_date").alias("DateKey"),
        F.col("base_currency").alias("BaseCurrency"),
        F.col("quote_currency").alias("QuoteCurrency"),
        F.col("exchange_rate").alias("ExchangeRate"),
        F.col("is_published").alias("IsPublished"),
    )


def build_dim_gdp(indicators: DataFrame) -> DataFrame:
    gdp = indicators.where(F.col("indicator_id") == GDP_INDICATOR_ID)
    earlier_years = Window.partitionBy("country_iso3").orderBy("year")
    is_previous_year = F.lag("year").over(earlier_years) == F.col("year") - 1
    growth = (F.col("value") / F.lag("value").over(earlier_years) - 1) * 100
    return gdp.select(
        F.col("year").alias("Year"),
        F.col("country_iso3").alias("CountryIso3"),
        F.col("country_name").alias("CountryName"),
        _money(F.col("value"), "GdpUsd"),
        F.when(is_previous_year, F.round(growth, DECIMALS)).alias("GdpGrowthPercent"),
    )


def _usd_per_eur(rates: DataFrame) -> DataFrame:
    euro_dollar = (F.col("base_currency") == EURO) & (F.col("quote_currency") == US_DOLLAR)
    return rates.where(euro_dollar).select(F.col("rate_date").alias("end_date"), F.col("exchange_rate").alias("usd_per_eur"))


TripPeriod = Callable[[str], Column]


def _hour_start(time_column: str) -> Column:
    return F.date_trunc("hour", time_column)


def _trip_end_rows(trips: DataFrame, end: TripEnd, period: TripPeriod) -> DataFrame:
    return trips.select(
        F.lit(end.key).alias("TripEndKey"),
        F.col(end.location_column).alias("location_id"),
        period(end.time_column).alias("period"),
        *TRIP_END_MEASURES,
    )


def trip_ends(trips: DataFrame, period: TripPeriod) -> DataFrame:
    last_pickup = trips.agg(F.max(period(PICKUP.time_column)).alias("last_pickup_period"))
    ends = reduce(DataFrame.unionByName, (_trip_end_rows(trips, end, period) for end in TRIP_ENDS))
    complete = ends.crossJoin(F.broadcast(last_pickup)).where(F.col("period") <= F.col("last_pickup_period"))
    return complete.drop("last_pickup_period")


def _with_zone_key(trip_ends: DataFrame, dim_zone: DataFrame) -> DataFrame:
    zone_keys = dim_zone.select(F.col("ZoneKey").alias("location_id"), "ZoneKey")
    with_zone = trip_ends.join(F.broadcast(zone_keys), "location_id", "left")
    return with_zone.withColumn("ZoneKey", F.coalesce("ZoneKey", F.lit(UNKNOWN_ZONE_KEY)))


def build_fact_taxi_daily(trips: DataFrame, dim_zone: DataFrame, rates: DataFrame) -> DataFrame:
    ends = _with_zone_key(trip_ends(trips, F.to_date), dim_zone)
    daily = ends.groupBy(F.col("period").alias("end_date"), "ZoneKey", "TripEndKey").agg(
        F.count(F.lit(1)).alias("TripCount"),
        F.sum("passenger_count").alias("PassengerCount"),
        F.sum("trip_distance").alias("distance"),
        F.sum("trip_duration_minutes").alias("duration"),
        F.sum("fare_amount").alias("fare_usd"),
        F.sum("total_amount").alias("total_usd"),
    )
    trip_count, usd_per_eur = F.col("TripCount"), F.col("usd_per_eur")
    fare_usd, total_usd, distance, duration = F.col("fare_usd"), F.col("total_usd"), F.col("distance"), F.col("duration")
    return daily.join(_usd_per_eur(rates), "end_date", "left").select(
        date_key("end_date").alias("DateKey"),
        "ZoneKey",
        "TripEndKey",
        "TripCount",
        "PassengerCount",
        _rounded(distance, "TripDistanceMiles"),
        _rounded(duration, "TripDurationMinutes"),
        _money(fare_usd, "FareAmountUsd"),
        _money(total_usd, "TotalAmountUsd"),
        _money(fare_usd / usd_per_eur, "FareAmountEur"),
        _money(total_usd / usd_per_eur, "TotalAmountEur"),
        _money(fare_usd / trip_count, "AvgFareAmountUsd"),
        _money(total_usd / trip_count, "AvgTotalAmountUsd"),
        _money(total_usd / usd_per_eur / trip_count, "AvgTotalAmountEur"),
        _rounded(distance / trip_count, "AvgTripDistanceMiles"),
        _rounded(duration / trip_count, "AvgTripDurationMinutes"),
    )


def build_fact_air_quality_daily(readings: DataFrame, min_percent_complete: float) -> DataFrame:
    covered = readings.where(F.col("percent_complete") >= min_percent_complete)
    zone_key = F.coalesce("taxi_zone_id", F.lit(UNKNOWN_ZONE_KEY)).alias("ZoneKey")
    daily = covered.groupBy("measured_date", zone_key, "parameter_name", "parameter_units").agg(
        F.countDistinct("sensor_id").alias("SensorCount"),
        F.avg("avg_value").alias("AvgValue"),
        F.min("min_value").alias("MinValue"),
        F.max("max_value").alias("MaxValue"),
    )
    return daily.select(
        date_key("measured_date").alias("DateKey"),
        "ZoneKey",
        F.col("parameter_name").alias("Pollutant"),
        F.col("parameter_units").alias("Unit"),
        "SensorCount",
        "AvgValue",
        "MinValue",
        "MaxValue",
    )


def build_fact_taxi_hourly(trips: DataFrame, dim_zone: DataFrame) -> DataFrame:
    ends = _with_zone_key(trip_ends(trips, _hour_start), dim_zone)
    hourly = ends.groupBy("period", "ZoneKey", "TripEndKey").agg(F.count(F.lit(1)).alias("TripCount"))
    return hourly.select(
        date_key("period").alias("DateKey"),
        F.hour("period").alias("HourKey"),
        "ZoneKey",
        "TripEndKey",
        "TripCount",
    )


def build_fact_air_quality_hourly(readings: DataFrame) -> DataFrame:
    zone_key = F.coalesce("taxi_zone_id", F.lit(UNKNOWN_ZONE_KEY)).alias("ZoneKey")
    hourly = readings.groupBy("measured_date", "measured_hour", zone_key, "parameter_name", "parameter_units").agg(
        F.countDistinct("sensor_id").alias("SensorCount"),
        F.avg("value").alias("AvgValue"),
        F.max("value").alias("MaxValue"),
    )
    return hourly.select(
        date_key("measured_date").alias("DateKey"),
        F.col("measured_hour").cast("int").alias("HourKey"),
        "ZoneKey",
        F.col("parameter_name").alias("Pollutant"),
        F.col("parameter_units").alias("Unit"),
        "SensorCount",
        "AvgValue",
        "MaxValue",
    )


def calendar_span(first_year: int, today: date) -> tuple:
    return (date(first_year, 1, 1), date(today.year, 12, 31))


def settled_hours(readings: DataFrame, settled_before: datetime) -> DataFrame:
    return readings.where(F.col("measured_at_utc") < F.lit(settled_before))


def _dim_zone(inputs: GoldInputs) -> DataFrame:
    return build_dim_zone(inputs.silver(SILVER_ZONES_TABLE))


def _fact_taxi_daily(inputs: GoldInputs) -> DataFrame:
    rates = inputs.silver(SILVER_EXCHANGE_RATES_TABLE)
    return build_fact_taxi_daily(inputs.silver(SILVER_TRIPS_TABLE), _dim_zone(inputs), rates)


def _fact_air_quality_daily(inputs: GoldInputs) -> DataFrame:
    return build_fact_air_quality_daily(inputs.silver(SILVER_AIR_QUALITY_DAILY_TABLE), inputs.min_percent_complete)


def _fact_taxi_hourly(inputs: GoldInputs) -> DataFrame:
    return build_fact_taxi_hourly(inputs.silver(SILVER_TRIPS_TABLE), _dim_zone(inputs))


def _fact_air_quality_hourly(inputs: GoldInputs) -> DataFrame:
    readings = settled_hours(inputs.silver(SILVER_AIR_QUALITY_HOURLY_TABLE), inputs.settled_before)
    return build_fact_air_quality_hourly(readings)


GOLD_TABLES = {
    "DimDate": GoldTable(lambda inputs: build_dim_date(inputs.session, inputs.calendar), AppendRule("DateKey")),
    "DimZone": GoldTable(_dim_zone),
    "DimFX": GoldTable(lambda inputs: build_dim_fx(inputs.silver(SILVER_EXCHANGE_RATES_TABLE)), AppendRule("DateKey")),
    "DimGDP": GoldTable(lambda inputs: build_dim_gdp(inputs.silver(SILVER_INDICATORS_TABLE)), AppendRule("Year", ("CountryIso3",))),
    "FactTaxiDaily": GoldTable(_fact_taxi_daily, AppendRule("DateKey")),
    "FactAirQualityDaily": GoldTable(_fact_air_quality_daily, AppendRule("DateKey")),
    "DimHour": GoldTable(lambda inputs: build_dim_hour(inputs.session)),
    "DimTripEnd": GoldTable(lambda inputs: build_dim_trip_end(inputs.session)),
    "FactTaxiHourly": GoldTable(_fact_taxi_hourly, AppendRule("DateKey")),
    "FactAirQualityHourly": GoldTable(_fact_air_quality_hourly, AppendRule("DateKey", hour_column="HourKey")),
}


def require_load_mode(load_mode: str) -> None:
    allowed = (REBUILD, APPEND_MISSING)
    if load_mode not in allowed:
        raise ValueError(f"Unknown load_mode {load_mode!r}, expected one of {list(allowed)}")


def select_tables(tables: dict, names: str, load_mode: str) -> dict:
    require_load_mode(load_mode)
    selected = {name.strip() for name in names.split(",") if name.strip()} or set(tables)
    unknown = sorted(selected - set(tables))
    if unknown:
        raise ValueError(f"Unknown gold tables {unknown}, expected some of {list(tables)}")
    without_rule = sorted(name for name in selected if tables[name].append_rule is None)
    if load_mode == APPEND_MISSING and without_rule:
        raise ValueError(f"{without_rule} cannot be appended, load them with load_mode = '{REBUILD}'")
    return {name: table for name, table in tables.items() if name in selected}


def build_tables(tables: dict, inputs: GoldInputs) -> dict:
    return {name: table.build(inputs) for name, table in tables.items()}


def latest_loaded_query(table: str, rule: AppendRule) -> str:
    keys = [f"[{column}]" for column in rule.key_columns]
    columns = ", ".join([*keys, f"MAX({rule.period_sql()}) AS [{LATEST_LOADED_PERIOD}]"])
    group_by = f" GROUP BY {', '.join(keys)}" if keys else ""
    return f"SELECT {columns} FROM [{WAREHOUSE_SCHEMA}].[{table}]{group_by}"


def _rows_after_latest_overall(rows: DataFrame, latest: DataFrame, rule: AppendRule) -> DataFrame:
    [latest_row] = latest.collect()
    latest_period = latest_row[LATEST_LOADED_PERIOD]
    return rows if latest_period is None else rows.where(rule.period() > F.lit(latest_period))


def _rows_after_latest_per_key(rows: DataFrame, latest: DataFrame, rule: AppendRule) -> DataFrame:
    is_newer = F.coalesce(rule.period() > F.col(LATEST_LOADED_PERIOD), F.lit(True))
    return rows.join(F.broadcast(latest), list(rule.key_columns), "left").where(is_newer).select(*rows.columns)


def rows_after_latest_loaded(rows: DataFrame, latest: DataFrame, rule: AppendRule) -> DataFrame:
    if rule.key_columns:
        return _rows_after_latest_per_key(rows, latest, rule)
    return _rows_after_latest_overall(rows, latest, rule)


def missing_rows(read_query: Callable[[str], DataFrame], name: str, table: DataFrame, existing_tables: set) -> DataFrame:
    if name not in existing_tables:
        return table
    rule = GOLD_TABLES[name].append_rule
    return rows_after_latest_loaded(table, read_query(latest_loaded_query(name, rule)), rule)


def write_warehouse_table(session: SparkSession, table: DataFrame, name: str) -> int:
    table.write.mode("overwrite").synapsesql(name)
    return session.read.synapsesql(name).count()


def append_warehouse_table(rows: DataFrame, name: str) -> int:
    row_count = rows.count()
    if row_count:
        rows.write.mode("append").synapsesql(name)
    return row_count


def warehouse_table(warehouse: str, name: str) -> str:
    return f"{warehouse}.{WAREHOUSE_SCHEMA}.{name}"


def write_star_schema(session: SparkSession, tables: dict, warehouse: str) -> list:
    return [
        {"table": name, "rows_written": write_warehouse_table(session, table, warehouse_table(warehouse, name))}
        for name, table in tables.items()
    ]


def append_star_schema(read_query: Callable[[str], DataFrame], tables: dict, warehouse: str) -> list:
    existing_tables = {row.name for row in read_query(EXISTING_TABLES_QUERY).collect()}
    return [
        {
            "table": name,
            "rows_written": append_warehouse_table(
                missing_rows(read_query, name, table, existing_tables), warehouse_table(warehouse, name)
            ),
        }
        for name, table in tables.items()
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

spark.conf.set("spark.sql.session.timeZone", "UTC")


def read_warehouse_query(query: str) -> DataFrame:
    return spark.read.option(Constants.DatabaseName, warehouse).synapsesql(query)


nyc_today = datetime.now(ZoneInfo(NYC_TIME_ZONE)).date()
current_utc_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
settled_before = current_utc_hour - timedelta(hours=air_quality_settle_hours)
inputs = GoldInputs(spark, calendar_span(calendar_first_year, nyc_today), min_percent_complete, settled_before)
selected_tables = build_tables(select_tables(GOLD_TABLES, tables, load_mode), inputs)
if load_mode == APPEND_MISSING:
    table_reports = append_star_schema(read_warehouse_query, selected_tables, warehouse)
else:
    table_reports = write_star_schema(spark, selected_tables, warehouse)
print(json.dumps(table_reports, indent=2))

runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(table_reports))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
