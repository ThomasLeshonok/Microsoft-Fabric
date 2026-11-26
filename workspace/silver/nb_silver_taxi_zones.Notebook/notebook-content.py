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

# # nb_silver_taxi_zones
#
# Builds `silver_taxi_zones` in `lh_silver`: one row per TLC taxi zone (`location_id`, `borough`, `zone`,
# `service_zone`) with its boundary as a GeoJSON `MultiPolygon` in longitude/latitude (`boundary_geojson`). Gold uses
# the boundaries to place each OpenAQ sensor in a taxi zone, so mobility and air quality share one zone dimension.
#
# 1. Normalize the `taxi_zone_lookup` table from `lh_bronze` (`pl_ingest_taxi` appends it on every run).
# 2. Drop invalid rows, see `invalid_zone_rules`, then keep one row per `location_id`.
# 3. Read `taxi_zones.shp`, `.dbf` and `.prj` from `lh_bronze`. The Fabric runtime has no geo libraries, so the
#    notebook reads the polygon records and the `LocationID` column itself and converts the NY State Plane
#    coordinates (Lambert conformal conic, US feet) to longitude/latitude with the parameters in the `.prj` file.
#    A zone with several shapefile records (islands) gets all of them in one `MultiPolygon`. Clockwise rings are
#    outer rings, counter-clockwise rings are holes of the outer ring before them (shapefile convention).
# 4. Join boundaries to the lookup. `264` (Unknown) and `265` (Outside of NYC) have no boundary.
# 5. Overwrite the table, so reruns are idempotent, then `VACUUM` it (see `vacuum_retention_hours`).
#
# Rule counts, `vacuum_table` and the OneLake paths come from `nb_silver_delta_tables` (`%run`).
#
# Exits with a report: rows read, rows dropped per rule, duplicates removed, zones without boundary, rows written.

# PARAMETERS CELL ********************

bronze_lakehouse = "lh_bronze"
bronze_lookup_table = "taxi_zone_lookup"
bronze_shapefile = "Files/nyctaxi_yellow/taxi_zones/taxi_zones"
silver_table = "silver_taxi_zones"
vacuum_retention_hours = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_silver_delta_tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import math
import re
import struct
from collections import defaultdict
from dataclasses import asdict, dataclass
from itertools import pairwise

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SHAPEFILE_FILE_CODE = 9994
SHAPEFILE_HEADER_BYTES = 100
RECORD_HEADER_BYTES = 8
BYTES_PER_WORD = 2
NULL_SHAPE_TYPE = 0
POLYGON_SHAPE_TYPE = 5
DBF_DESCRIPTOR_BYTES = 32
DBF_FIELD_NAME_BYTES = 11
DBF_FIELD_LENGTH_OFFSET = 16
DBF_HEADER_TERMINATOR = 0x0D
DBF_DELETION_FLAG_BYTES = 1
LOCATION_ID_FIELD = "LocationID"
LAMBERT_CONFORMAL_CONIC = "Lambert_Conformal_Conic"
LATITUDE_ITERATIONS = 10
COORDINATE_DECIMALS = 6
BOUNDARY_SCHEMA = "location_id int, boundary_geojson string"


def _lambert_m(latitude: float, eccentricity: float) -> float:
    return math.cos(latitude) / math.sqrt(1 - (eccentricity * math.sin(latitude)) ** 2)


def _lambert_t(latitude: float, eccentricity: float) -> float:
    eccentric_sine = eccentricity * math.sin(latitude)
    return math.tan(math.pi / 4 - latitude / 2) / ((1 - eccentric_sine) / (1 + eccentric_sine)) ** (eccentricity / 2)


def _latitude_from_t(t: float, eccentricity: float) -> float:
    latitude = math.pi / 2 - 2 * math.atan(t)
    for _ in range(LATITUDE_ITERATIONS):
        eccentric_sine = eccentricity * math.sin(latitude)
        latitude = math.pi / 2 - 2 * math.atan(t * ((1 - eccentric_sine) / (1 + eccentric_sine)) ** (eccentricity / 2))
    return latitude


def _prj_value(pattern: str, prj: str) -> str:
    match = re.search(pattern, prj)
    if not match:
        raise ValueError(f"Projection file has no {pattern}: {prj}")
    return match.group(1)


@dataclass(frozen=True)
class LambertConformalConic:
    semi_major_axis: float
    eccentricity: float
    central_meridian: float
    false_easting: float
    false_northing: float
    metres_per_unit: float
    cone_constant: float
    scale: float
    origin_radius: float

    @classmethod
    def from_prj(cls, prj: str) -> "LambertConformalConic":
        projection = _prj_value(r'PROJECTION\["([^"]+)"\]', prj)
        if projection != LAMBERT_CONFORMAL_CONIC:
            raise ValueError(f"Only {LAMBERT_CONFORMAL_CONIC} is supported, got {projection}")
        parameters = {name.lower(): float(value) for name, value in re.findall(r'PARAMETER\["([^"]+)",\s*([-+\d.eE]+)\]', prj)}
        semi_major_axis = float(_prj_value(r'SPHEROID\["[^"]*",\s*([\d.]+)', prj))
        inverse_flattening = float(_prj_value(r'SPHEROID\["[^"]*",\s*[\d.]+,\s*([\d.]+)', prj))
        metres_per_unit = float(re.findall(r'UNIT\["[^"]*",\s*([\d.eE+-]+)\]', prj)[-1])
        flattening = 1 / inverse_flattening
        eccentricity = math.sqrt(flattening * (2 - flattening))
        first_parallel = math.radians(parameters["standard_parallel_1"])
        second_parallel = math.radians(parameters["standard_parallel_2"])
        origin_latitude = math.radians(parameters["latitude_of_origin"])
        m1, m2 = _lambert_m(first_parallel, eccentricity), _lambert_m(second_parallel, eccentricity)
        t1, t2 = _lambert_t(first_parallel, eccentricity), _lambert_t(second_parallel, eccentricity)
        cone_constant = (math.log(m1) - math.log(m2)) / (math.log(t1) - math.log(t2))
        scale = m1 / (cone_constant * t1**cone_constant)
        return cls(
            semi_major_axis=semi_major_axis,
            eccentricity=eccentricity,
            central_meridian=math.radians(parameters["central_meridian"]),
            false_easting=parameters["false_easting"] * metres_per_unit,
            false_northing=parameters["false_northing"] * metres_per_unit,
            metres_per_unit=metres_per_unit,
            cone_constant=cone_constant,
            scale=scale,
            origin_radius=semi_major_axis * scale * _lambert_t(origin_latitude, eccentricity) ** cone_constant,
        )

    def to_longitude_latitude(self, x: float, y: float) -> tuple:
        cone_sign = math.copysign(1, self.cone_constant)
        easting = x * self.metres_per_unit - self.false_easting
        northing = self.origin_radius - (y * self.metres_per_unit - self.false_northing)
        radius = cone_sign * math.hypot(easting, northing)
        t = (radius / (self.semi_major_axis * self.scale)) ** (1 / self.cone_constant)
        angle = math.atan2(cone_sign * easting, cone_sign * northing)
        longitude = angle / self.cone_constant + self.central_meridian
        latitude = _latitude_from_t(t, self.eccentricity)
        return (round(math.degrees(longitude), COORDINATE_DECIMALS), round(math.degrees(latitude), COORDINATE_DECIMALS))


def read_polygon_records(shp: bytes) -> list:
    (file_code,) = struct.unpack_from(">i", shp, 0)
    (shape_type,) = struct.unpack_from("<i", shp, 32)
    if file_code != SHAPEFILE_FILE_CODE:
        raise ValueError(f"Not a shapefile: file code {file_code}")
    if shape_type != POLYGON_SHAPE_TYPE:
        raise ValueError(f"Only polygon shapefiles are supported, got shape type {shape_type}")
    records, offset = [], SHAPEFILE_HEADER_BYTES
    while offset < len(shp):
        (_, content_words) = struct.unpack_from(">ii", shp, offset)
        records.append(_polygon_rings(shp, offset + RECORD_HEADER_BYTES))
        offset += RECORD_HEADER_BYTES + content_words * BYTES_PER_WORD
    return records


def _polygon_rings(shp: bytes, offset: int) -> list:
    (record_type,) = struct.unpack_from("<i", shp, offset)
    if record_type == NULL_SHAPE_TYPE:
        return []
    part_count, point_count = struct.unpack_from("<ii", shp, offset + 36)
    starts = struct.unpack_from(f"<{part_count}i", shp, offset + 44)
    coordinates = struct.unpack_from(f"<{2 * point_count}d", shp, offset + 44 + 4 * part_count)
    points = list(zip(coordinates[0::2], coordinates[1::2]))
    ends = (*starts[1:], point_count)
    return [points[start:end] for start, end in zip(starts, ends)]


def read_dbf_column(dbf: bytes, field_name: str) -> list:
    record_count, header_length, record_length = struct.unpack_from("<IHH", dbf, 4)
    fields, field_start, position = {}, DBF_DELETION_FLAG_BYTES, DBF_DESCRIPTOR_BYTES
    while dbf[position] != DBF_HEADER_TERMINATOR:
        name = dbf[position : position + DBF_FIELD_NAME_BYTES].split(b"\x00")[0].decode("ascii")
        length = dbf[position + DBF_FIELD_LENGTH_OFFSET]
        fields[name] = (field_start, length)
        field_start += length
        position += DBF_DESCRIPTOR_BYTES
    if field_name not in fields:
        raise KeyError(f"DBF has no field {field_name}; fields: {sorted(fields)}")
    start, length = fields[field_name]
    records = (dbf[header_length + index * record_length :][:record_length] for index in range(record_count))
    return [record[start : start + length].decode("latin-1").strip() for record in records]


def _signed_area(ring: list) -> float:
    return sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in pairwise(ring)) / 2


def group_rings(rings: list) -> list:
    polygons = []
    for ring in rings:
        is_outer_ring = _signed_area(ring) < 0
        if is_outer_ring or not polygons:
            polygons.append([ring])
        else:
            polygons[-1].append(ring)
    return polygons


def zone_boundaries(shp: bytes, dbf: bytes, prj: str) -> dict:
    projection = LambertConformalConic.from_prj(prj)
    polygons_by_zone = defaultdict(list)
    for location_id, rings in zip(read_dbf_column(dbf, LOCATION_ID_FIELD), read_polygon_records(shp)):
        projected = [[projection.to_longitude_latitude(x, y) for x, y in ring] for ring in rings]
        polygons_by_zone[int(float(location_id))].extend(group_rings(projected))
    return {
        location_id: json.dumps({"type": "MultiPolygon", "coordinates": polygons})
        for location_id, polygons in polygons_by_zone.items()
        if polygons
    }


def normalize_lookup(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim("Borough").alias("borough"),
        F.trim("Zone").alias("zone"),
        F.trim("service_zone").alias("service_zone"),
    )


def invalid_zone_rules() -> dict:
    conditions = {"missing_location_id": F.col("location_id").isNull()}
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in conditions.items()}


def clean_zones(zones: DataFrame, rules: dict, boundaries: DataFrame) -> DataFrame:
    valid = zones.filter(~is_invalid(rules)).dropDuplicates(["location_id"])
    return valid.join(boundaries, "location_id", "left")


@dataclass(frozen=True)
class TaxiZoneReport:
    table: str
    rows_read: int
    rows_dropped_by_rule: dict
    duplicates_removed: int
    zones_without_boundary: int
    rows_written: int


def count_written_zones(cleaned: DataFrame) -> dict:
    [totals] = cleaned.agg(
        F.count(F.lit(1)).alias("rows_written"),
        F.sum(F.col("boundary_geojson").isNull().cast("int")).alias("zones_without_boundary"),
    ).collect()
    return {name: int(value or 0) for name, value in totals.asDict().items()}


def build_taxi_zone_report(table: str, rule_hits: dict, written_zones: dict) -> TaxiZoneReport:
    return TaxiZoneReport(
        table=table,
        rows_read=rule_hits["rows_read"],
        rows_dropped_by_rule=rows_dropped_by_rule(rule_hits),
        duplicates_removed=rule_hits["rows_valid"] - written_zones["rows_written"],
        zones_without_boundary=written_zones["zones_without_boundary"],
        rows_written=written_zones["rows_written"],
    )


def write_table(session: SparkSession, zones: DataFrame, table: str) -> dict:
    zones.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    return count_written_zones(session.table(table))


def read_binary_file(session: SparkSession, path: str) -> bytes:
    return bytes(session.read.format("binaryFile").load(path).select("content").head()[0])


def read_boundaries(session: SparkSession, shapefile_path: str) -> DataFrame:
    shp, dbf, prj = (read_binary_file(session, f"{shapefile_path}.{extension}") for extension in ("shp", "dbf", "prj"))
    boundaries = zone_boundaries(shp, dbf, prj.decode("ascii"))
    return session.createDataFrame(sorted(boundaries.items()), BOUNDARY_SCHEMA)


def load_zones(session: SparkSession, lookup_path: str, shapefile_path: str, table: str) -> TaxiZoneReport:
    zones = normalize_lookup(session.read.format("delta").load(lookup_path))
    rules = invalid_zone_rules()
    rule_hits = count_rule_hits(zones, rules)
    written_zones = write_table(session, clean_zones(zones, rules, read_boundaries(session, shapefile_path)), table)
    return build_taxi_zone_report(table, rule_hits, written_zones)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report = asdict(
    load_zones(
        spark,
        bronze_table_path(bronze_lakehouse, bronze_lookup_table),
        lakehouse_path(bronze_lakehouse, bronze_shapefile),
        silver_table,
    )
)
vacuum_table(spark, silver_table, vacuum_retention_hours)
print(json.dumps(report, indent=2))

runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(report))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
