import json
import os
import struct
import sys

import pytest
from notebook_loader import load

ALL_CELLS = slice(None)
DEFINITION_CELLS = slice(1, -1)
TAXI_ZONES_PRJ = (
    'PROJCS["NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104_Feet",GEOGCS["GCS_North_American_1983",'
    'DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",984250.0],'
    'PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-74.0],'
    'PARAMETER["Standard_Parallel_1",40.66666666666666],PARAMETER["Standard_Parallel_2",41.03333333333333],'
    'PARAMETER["Latitude_Of_Origin",40.16666666666666],UNIT["Foot_US",0.3048006096012192]]'
)
PYPROJ_EPSG_2263_TO_4326 = {
    (988000.0, 215000.0): (-73.98646427239088, 40.75680099734238),
    (1010000.0, 180000.0): (-73.90718855789628, 40.660697648959626),
    (935000.0, 145000.0): (-74.17725769630636, 40.56453054274751),
}
LOOKUP_SCHEMA = "LocationID string, Borough string, Zone string, service_zone string"


@pytest.fixture(scope="module")
def silver():
    return load(("nb_silver_delta_tables", ALL_CELLS), ("nb_silver_taxi_zones", DEFINITION_CELLS))


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def clockwise_square(x, y, size):
    return [(x, y), (x, y + size), (x + size, y + size), (x + size, y), (x, y)]


def counter_clockwise_square(x, y, size):
    return list(reversed(clockwise_square(x, y, size)))


def polygon_record(rings):
    points = [point for ring in rings for point in ring]
    starts = [sum(len(ring) for ring in rings[:index]) for index in range(len(rings))]
    xs, ys = [x for x, _ in points], [y for _, y in points]
    return (
        struct.pack("<i4d2i", 5, min(xs), min(ys), max(xs), max(ys), len(rings), len(points))
        + struct.pack(f"<{len(starts)}i", *starts)
        + struct.pack(f"<{2 * len(points)}d", *[coordinate for point in points for coordinate in point])
    )


def shapefile(records, shape_type=5):
    contents = [polygon_record(rings) if rings else struct.pack("<i", 0) for rings in records]
    body = b"".join(struct.pack(">2i", number, len(content) // 2) + content for number, content in enumerate(contents, 1))
    header = struct.pack(">i20xi", 9994, (100 + len(body)) // 2) + struct.pack("<2i64x", 1000, shape_type)
    return header + body


def dbf(columns):
    fields = [(name, kind, width) for name, kind, width, _ in columns]
    rows = list(zip(*[values for *_, values in columns]))
    header_length = 32 + 32 * len(fields) + 1
    record_length = 1 + sum(width for *_, width in fields)
    header = struct.pack("<4BIHH20x", 3, 126, 1, 1, len(rows), header_length, record_length)
    descriptors = b"".join(name.encode().ljust(11, b"\x00") + kind.encode() + bytes(4) + bytes([width, 0]) + bytes(14) for name, kind, width in fields)
    records = b"".join(b" " + b"".join(str(value).rjust(width).encode() for value, (_, _, width) in zip(row, fields)) for row in rows)
    return header + descriptors + b"\r" + records + b"\x1a"


def zones_dbf(location_ids):
    return dbf([("zone", "C", 20, ["z"] * len(location_ids)), ("LocationID", "N", 9, location_ids)])


def test_projection_matches_pyproj_for_epsg_2263(silver):
    projection = silver["LambertConformalConic"].from_prj(TAXI_ZONES_PRJ)

    for (x, y), (longitude, latitude) in PYPROJ_EPSG_2263_TO_4326.items():
        assert projection.to_longitude_latitude(x, y) == pytest.approx((longitude, latitude), abs=1e-6)


def test_projection_rejects_other_projections(silver):
    with pytest.raises(ValueError, match="Lambert_Conformal_Conic"):
        silver["LambertConformalConic"].from_prj(TAXI_ZONES_PRJ.replace("Lambert_Conformal_Conic", "Transverse_Mercator"))


def test_records_of_one_zone_merge_into_one_multipolygon(silver):
    shp = shapefile([[clockwise_square(988000, 215000, 1000)], [clockwise_square(990000, 215000, 1000)], []])

    boundaries = silver["zone_boundaries"](shp, zones_dbf([103, 103, 264]), TAXI_ZONES_PRJ)

    assert list(boundaries) == [103]
    geometry = json.loads(boundaries[103])
    assert geometry["type"] == "MultiPolygon"
    assert len(geometry["coordinates"]) == 2
    assert geometry["coordinates"][0][0][0] == pytest.approx([-73.98646427239088, 40.75680099734238], abs=1e-6)


def test_counter_clockwise_ring_is_a_hole_of_the_outer_ring_before_it(silver):
    rings = [clockwise_square(0, 0, 10), counter_clockwise_square(2, 2, 2), clockwise_square(20, 0, 10)]

    polygons = silver["group_rings"](rings)

    assert [len(polygon) for polygon in polygons] == [2, 1]


def test_reader_rejects_non_polygon_shapefiles(silver):
    with pytest.raises(ValueError, match="shape type 1"):
        silver["read_polygon_records"](shapefile([], shape_type=1))


def test_dbf_reader_names_available_fields_when_one_is_missing(silver):
    with pytest.raises(KeyError, match="LocationID"):
        silver["read_dbf_column"](dbf([("zone", "C", 20, ["z"])]), "LocationID")


def test_zones_are_deduplicated_and_joined_to_their_boundary(silver, spark):
    lookup = spark.createDataFrame(
        [
            (" 4 ", "Manhattan", "Alphabet City", "Yellow Zone"),
            ("4", "Manhattan", "Alphabet City", "Yellow Zone"),
            ("264", "Unknown", "N/A", "N/A"),
            (None, "Queens", "Nowhere", "Boro Zone"),
        ],
        LOOKUP_SCHEMA,
    )
    boundaries = spark.createDataFrame([(4, '{"type": "MultiPolygon"}')], silver["BOUNDARY_SCHEMA"])
    zones = silver["normalize_lookup"](lookup)
    rules = silver["invalid_zone_rules"]()

    cleaned = silver["clean_zones"](zones, rules, boundaries).orderBy("location_id").collect()
    report = silver["build_taxi_zone_report"](
        "silver_taxi_zones", silver["count_rule_hits"](zones, rules), silver["count_written_zones"](spark.createDataFrame(cleaned))
    )

    assert [row.asDict() for row in cleaned] == [
        {"location_id": 4, "borough": "Manhattan", "zone": "Alphabet City", "service_zone": "Yellow Zone", "boundary_geojson": '{"type": "MultiPolygon"}'},
        {"location_id": 264, "borough": "Unknown", "zone": "N/A", "service_zone": "N/A", "boundary_geojson": None},
    ]
    assert report.rows_dropped_by_rule == {"missing_location_id": 1}
    assert (report.rows_read, report.duplicates_removed, report.zones_without_boundary, report.rows_written) == (4, 1, 1, 2)
