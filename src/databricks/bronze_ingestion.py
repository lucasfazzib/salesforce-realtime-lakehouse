import argparse
import re

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value}")
    return value


def volume_root(catalog: str, schema: str, volume: str) -> str:
    return f"/Volumes/{catalog}/{schema}/{volume}"


def bronze_schema_ddl() -> str:
    return """
        replay_id STRING,
        schema_id STRING,
        record_ids ARRAY<STRING>,
        change_type STRING,
        changed_fields ARRAY<STRING>,
        commit_timestamp TIMESTAMP,
        received_at TIMESTAMP,
        topic STRING,
        payload VARIANT,
        _rescued_data STRING
    """


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Salesforce CDC into Bronze")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume", required=True)
    parser.add_argument("--table", required=True)
    return parser.parse_args()


def run(catalog: str, schema: str, volume: str, table: str) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp

    catalog = validate_identifier(catalog)
    schema = validate_identifier(schema)
    volume = validate_identifier(volume)
    table = validate_identifier(table)

    root = volume_root(catalog, schema, volume)
    source_path = f"{root}/salesforce/opportunity"
    schema_path = f"{root}/_state/opportunity_bronze_schema"
    checkpoint_path = f"{root}/_state/opportunity_bronze_checkpoint"
    target_table = f"{catalog}.{schema}.{table}"

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    bronze_events = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .schema(bronze_schema_ddl())
        .load(source_path)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    query = (
        bronze_events.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    print(f"Bronze ingestion completed: {target_table}")


if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments.catalog, arguments.schema, arguments.volume, arguments.table)