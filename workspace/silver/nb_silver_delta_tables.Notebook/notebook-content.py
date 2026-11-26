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

# # nb_silver_delta_tables
#
# Loaded by the silver notebooks with `%run nb_silver_delta_tables`; defines classes and functions only.
#
# * Spark DataFrames have no `CHAR(n)` or `VARCHAR(n)` (Spark 4 keeps them only with
#   `spark.sql.preserveCharVarcharTypeInfo`, off by default): `cast("varchar(3)")` returns `string`, so
#   `saveAsTable` always writes `string`. `TableDefinition` builds the column DDL from the DataFrame schema (integer
#   widths such as `tinyint` and `smallint` come from its casts) plus `text_types` for the text columns, and
#   `CREATE OR REPLACE TABLE` creates the table with it.
# * Writes go through `DataFrame.writeTo`, which keeps the table types: a too long value for a `CHAR(n)` or
#   `VARCHAR(n)` column, or an integer out of a column's range, fails the write instead of being cut.
# * `CHAR(n)` pads shorter values with spaces; use it for fixed length codes only (ISO 3166 country, ISO 4217
#   currency), `VARCHAR(n)` for everything else.
# * The table property `silver.schema` stores the columns and partitioning the table was created with. `is_current`
#   compares it with the definition; tables written before this notebook have no property, so they count as outdated
#   and get rebuilt.
# * Parquet stores `tinyint`/`smallint` as INT32 with an `INT(8)`/`INT(16)` annotation and text as UTF-8 whatever its
#   Delta type. The SQL analytics endpoint shows `tinyint` as `smallint` (SQL `tinyint` is unsigned); Power BI reads
#   every integer as Int64.
# * `load_mode` of the silver notebooks: `rebuild` recreates the table from all of bronze (`pl_silver_startup`);
#   `append_missing` appends only rows newer than the latest period already in silver for their key, existing rows are
#   never rewritten. `append_missing` on a missing table loads everything; on an outdated schema it raises
#   `OutdatedTableError`, because only a rebuild can change the schema.
# * `vacuum_after_load` runs `VACUUM` after every load mode but `append_missing`: an append only adds parquet files, so
#   there is nothing to delete, and vacuum still lists every file of the table. Files unlisted by an overwrite or a
#   rebuild are deleted by the next vacuum once older than `vacuum_retention_hours`.
# * Data quality, shared by every silver notebook: each notebook names its rules in an `invalid_*_rules` dict (rule
#   name to a never-null boolean column). `is_invalid` drops a row hit by any rule, `count_rule_hits` counts rows read,
#   valid rows and the hits per rule, and `build_table_report` turns them into the exit report: rows dropped per rule,
#   and duplicates removed = valid rows not written. A notebook whose report has an extra field keeps its own report
#   dataclass and builds it with `rows_dropped_by_rule`, so the exit JSON stays flat.
# * `keep_latest_ingestion` keeps one row per key, latest `ingested_at` wins; `ingested_at` stays, callers drop it.
# * `bronze_table_path` and `lakehouse_path` build the OneLake `abfss://` paths of the bronze tables and files.
# * `SilverDataset` + `load_dataset`: the whole load of a simple API source (one bronze table to one silver table,
#   appended per series and period). The spec names the tables and holds the source's own functions: `normalize`
#   (bronze columns to silver names and types), `rules` (its `invalid_*_rules`) and `clean` (drop invalid rows, keep the
#   latest ingestion, add derived columns), plus `series_key` and `period_column` for `append_missing`, and
#   `text_types`/`partition_columns` for the table. `load_dataset` reads bronze, creates the table, applies
#   `load_mode` (`rebuild` overwrites, `append_missing` appends per series only the periods after its latest one in
#   silver) and returns the `TableReport`. `nb_silver_worldbank_indicators` is the reference implementation, see
#   `docs/adding_a_dataset.md`. ECB (carries the last silver rate into an append), taxi trips (file based, overwrite by
#   month) and air quality (three tables and a zone join) keep their own load functions.

# CELL ********************

import operator
from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SCHEMA_PROPERTY = "silver.schema"
REBUILD = "rebuild"
APPEND_MISSING = "append_missing"
RULE_TOTALS = ("rows_read", "rows_valid")


class OutdatedTableError(RuntimeError):
    pass


@dataclass(frozen=True)
class TableDefinition:
    name: str
    columns: str
    partition_columns: tuple = ()

    @classmethod
    def from_frame(
        cls, name: str, frame: DataFrame, partition_columns: tuple = (), text_types: dict | None = None
    ) -> "TableDefinition":
        text_types = text_types or {}
        unknown = set(text_types) - set(frame.columns)
        if unknown:
            raise ValueError(f"Text types for columns not in {name}: {sorted(unknown)}")
        columns = ", ".join(f"{field.name} {text_types.get(field.name, field.dataType.simpleString())}" for field in frame.schema)
        return cls(name, columns, tuple(partition_columns))

    @property
    def partition_clause(self) -> str:
        return f"PARTITIONED BY ({', '.join(self.partition_columns)}) " if self.partition_columns else ""

    @property
    def layout(self) -> str:
        return f"({self.columns}) {self.partition_clause}".strip()

    def replace_statement(self) -> str:
        return (
            f"CREATE OR REPLACE TABLE {self.name} ({self.columns}) USING DELTA {self.partition_clause}"
            f"TBLPROPERTIES ('{SCHEMA_PROPERTY}' = '{self.layout}')"
        )


def is_current(session: SparkSession, definition: TableDefinition) -> bool:
    if not session.catalog.tableExists(definition.name):
        return False
    properties = {row.key: row.value for row in session.sql(f"SHOW TBLPROPERTIES {definition.name}").collect()}
    return properties.get(SCHEMA_PROPERTY) == definition.layout


def replace_table(session: SparkSession, definition: TableDefinition) -> None:
    session.sql(definition.replace_statement())


def overwrite_table(session: SparkSession, frame: DataFrame, definition: TableDefinition) -> None:
    replace_table(session, definition)
    frame.writeTo(definition.name).append()


def require_load_mode(load_mode: str, allowed: tuple = (REBUILD, APPEND_MISSING)) -> None:
    if load_mode not in allowed:
        raise ValueError(f"Unknown load_mode {load_mode!r}, expected one of {list(allowed)}")


def appends_to_current_table(session: SparkSession, definition: TableDefinition, load_mode: str) -> bool:
    require_load_mode(load_mode)
    if load_mode == REBUILD or not session.catalog.tableExists(definition.name):
        return False
    if not is_current(session, definition):
        raise OutdatedTableError(f"{definition.name} has an outdated schema, rebuild it with load_mode = '{REBUILD}'")
    return True


def rows_after_latest_loaded(rows: DataFrame, loaded: DataFrame, key_columns: tuple, period_column: str) -> DataFrame:
    latest = loaded.groupBy(*key_columns).agg(F.max(period_column).alias("latest_loaded_period"))
    is_newer = F.coalesce(F.col(period_column) > F.col("latest_loaded_period"), F.lit(True))
    return rows.join(latest, list(key_columns), "left").where(is_newer).select(*rows.columns)


def append_rows(frame: DataFrame, table: str) -> int:
    rows = frame.count()
    frame.writeTo(table).append()
    return rows


def overwrite_rows(session: SparkSession, frame: DataFrame, definition: TableDefinition) -> int:
    overwrite_table(session, frame, definition)
    return session.table(definition.name).count()


def vacuum_table(session: SparkSession, table: str, retention_hours: int) -> None:
    session.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")


def vacuum_after_load(session: SparkSession, table: str, retention_hours: int, load_mode: str) -> None:
    if load_mode != APPEND_MISSING:
        vacuum_table(session, table, retention_hours)


def is_invalid(rules: dict) -> Column:
    return reduce(operator.or_, rules.values())


def count_rule_hits(frame: DataFrame, rules: dict) -> dict:
    aggregates = [F.count(F.lit(1)).alias("rows_read"), F.sum((~is_invalid(rules)).cast("int")).alias("rows_valid")]
    aggregates += [F.sum(rule.cast("int")).alias(name) for name, rule in rules.items()]
    [totals] = frame.agg(*aggregates).collect()
    return {name: int(value or 0) for name, value in totals.asDict().items()}


def rows_dropped_by_rule(rule_hits: dict) -> dict:
    return {name: hits for name, hits in rule_hits.items() if name not in RULE_TOTALS}


def keep_latest_ingestion(frame: DataFrame, key: tuple) -> DataFrame:
    latest_first = Window.partitionBy(*key).orderBy(F.col("ingested_at").desc())
    ranked = frame.withColumn("ingestion_rank", F.row_number().over(latest_first))
    return ranked.filter(F.col("ingestion_rank") == 1).drop("ingestion_rank")


@dataclass(frozen=True)
class TableReport:
    table: str
    rows_read: int
    rows_dropped_by_rule: dict
    duplicates_removed: int
    rows_written: int


def build_table_report(table: str, rule_hits: dict, rows_written: int) -> TableReport:
    return TableReport(
        table=table,
        rows_read=rule_hits["rows_read"],
        rows_dropped_by_rule=rows_dropped_by_rule(rule_hits),
        duplicates_removed=rule_hits["rows_valid"] - rows_written,
        rows_written=rows_written,
    )


@dataclass(frozen=True)
class SilverDataset:
    bronze_table: str
    silver_table: str
    normalize: Callable[[DataFrame], DataFrame]
    rules: Callable[[], dict]
    clean: Callable[[DataFrame, dict], DataFrame]
    series_key: tuple
    period_column: str
    text_types: dict
    partition_columns: tuple = ()


def load_dataset(session: SparkSession, dataset: SilverDataset, bronze_path: str, load_mode: str) -> TableReport:
    rows = dataset.normalize(session.read.format("delta").load(bronze_path))
    rules = dataset.rules()
    table = dataset.silver_table
    definition = TableDefinition.from_frame(table, dataset.clean(rows, rules), dataset.partition_columns, dataset.text_types)
    is_append = appends_to_current_table(session, definition, load_mode)
    batch = rows_after_latest_loaded(rows, session.table(table), dataset.series_key, dataset.period_column) if is_append else rows
    rule_hits = count_rule_hits(batch, rules)
    cleaned = dataset.clean(batch, rules)
    rows_written = append_rows(cleaned, table) if is_append else overwrite_rows(session, cleaned, definition)
    return build_table_report(table, rule_hits, rows_written)


def lakehouse_path(lakehouse: str, relative_path: str) -> str:
    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
    lakehouse_id = notebookutils.lakehouse.get(lakehouse)["id"]
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{relative_path}"


def bronze_table_path(lakehouse: str, table: str) -> str:
    return lakehouse_path(lakehouse, f"Tables/{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
