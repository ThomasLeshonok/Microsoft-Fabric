# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8e27bc1-d18a-4eb0-b5cb-a0d3f492a089",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_compact_bronze_tables
#
# Compacts the small parquet files that frequent appends leave in `lh_bronze` Delta tables (each `df_openaq_nyc`
# refresh adds a few KB per table).
#
# 1. `OPTIMIZE` every table in `table_names` (comma-separated). Files smaller than `target_file_size_bytes` are
#    bin-packed into files of at most that size (`spark.databricks.delta.optimize.minFileSize` and `maxFileSize`).
#    Files already at the target are not rewritten, so a rerun without new appends changes nothing.
# 2. `VACUUM` each table. `OPTIMIZE` only unlists the small files, Delta keeps them for time travel; vacuum deletes
#    those older than `vacuum_retention_hours`. Below 168 hours Delta refuses unless
#    `spark.databricks.delta.retentionDurationCheck.enabled` is false.
#
# Exits with files removed and added per table.

# PARAMETERS CELL ********************

table_names = "bronze_openaq_sensor_days,bronze_openaq_sensor_measurements"
target_file_size_bytes = 1048576
vacuum_retention_hours = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

OPTIMIZE_MIN_FILE_SIZE = "spark.databricks.delta.optimize.minFileSize"
OPTIMIZE_MAX_FILE_SIZE = "spark.databricks.delta.optimize.maxFileSize"


def parse_table_names(raw: str) -> list:
    names = [name.strip() for name in raw.split(",") if name.strip()]
    if not names:
        raise ValueError("Set table_names to a comma-separated list of Delta tables")
    return names


def set_target_file_size(session, size_bytes: int) -> None:
    if size_bytes <= 0:
        raise ValueError(f"target_file_size_bytes must be positive, got {size_bytes}")
    session.conf.set(OPTIMIZE_MIN_FILE_SIZE, str(size_bytes))
    session.conf.set(OPTIMIZE_MAX_FILE_SIZE, str(size_bytes))


def compact_table(session, table: str, retention_hours: int) -> dict:
    metrics = session.sql(f"OPTIMIZE {table}").collect()[0]["metrics"]
    session.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
    return {"files_removed": metrics["numFilesRemoved"], "files_added": metrics["numFilesAdded"]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

set_target_file_size(spark, target_file_size_bytes)
compaction_report = {table: compact_table(spark, table, vacuum_retention_hours) for table in parse_table_names(table_names)}
print(json.dumps(compaction_report, indent=2))
runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(json.dumps(compaction_report))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
