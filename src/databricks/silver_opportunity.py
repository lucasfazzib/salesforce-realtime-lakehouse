import argparse
import re

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value}")
    return value

BUSINESS_FIELDS = {
    "name": ("Name", "payload:Name::STRING"),
    "stage_name": ("StageName", "payload:StageName::STRING"),
    "amount": ("Amount", "TRY_CAST(payload:Amount::STRING AS DECIMAL(18, 2))"),
    "close_date": (
        "CloseDate",
        "COALESCE("
        "TRY_CAST(payload:CloseDate::STRING AS DATE), "
        "DATE_ADD(DATE '1970-01-01', TRY_CAST(payload:CloseDate::STRING AS INT))"
        ")",
    ),
    "last_modified_date": (
        "LastModifiedDate",
        "COALESCE("
        "TIMESTAMP_MILLIS(TRY_CAST(payload:LastModifiedDate::STRING AS BIGINT)), "
        "TRY_CAST(payload:LastModifiedDate::STRING AS TIMESTAMP)"
        ")",
    ),
}


def silver_table_ddl(table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            opportunity_id STRING,
            name STRING,
            stage_name STRING,
            amount DECIMAL(18, 2),
            close_date DATE,
            last_modified_date TIMESTAMP,
            is_deleted BOOLEAN,
            deleted_at TIMESTAMP,
            last_change_type STRING,
            last_commit_timestamp TIMESTAMP,
            last_commit_number BIGINT,
            last_sequence_number BIGINT,
            last_transaction_key STRING,
            last_replay_id STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """


def merge_sql(target_table: str, source_view: str) -> str:
    newer_source = """
        source.last_commit_timestamp > target.last_commit_timestamp
        OR (
            source.last_commit_timestamp = target.last_commit_timestamp
            AND source.last_commit_number > target.last_commit_number
        )
        OR (
            source.last_commit_timestamp = target.last_commit_timestamp
            AND source.last_commit_number = target.last_commit_number
            AND source.last_sequence_number > target.last_sequence_number
        )
    """
    business_updates = ",\n".join(
        f"            target.{column} = CASE "
        f"WHEN source.has_{column}_change THEN source.{column} "
        f"ELSE target.{column} END"
        for column in BUSINESS_FIELDS
    )
    business_columns = ", ".join(BUSINESS_FIELDS)
    business_values = ", ".join(f"source.{column}" for column in BUSINESS_FIELDS)
    return f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
        ON target.opportunity_id = source.opportunity_id
        WHEN MATCHED AND ({newer_source}) THEN UPDATE SET
{business_updates},
            target.is_deleted = source.is_deleted,
            target.deleted_at = source.deleted_at,
            target.last_change_type = source.last_change_type,
            target.last_commit_timestamp = source.last_commit_timestamp,
            target.last_commit_number = source.last_commit_number,
            target.last_sequence_number = source.last_sequence_number,
            target.last_transaction_key = source.last_transaction_key,
            target.last_replay_id = source.last_replay_id,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            opportunity_id, {business_columns}, is_deleted, deleted_at,
            last_change_type, last_commit_timestamp, last_commit_number,
            last_sequence_number, last_transaction_key, last_replay_id, updated_at
        ) VALUES (
            source.opportunity_id, {business_values}, source.is_deleted,
            source.deleted_at, source.last_change_type,
            source.last_commit_timestamp, source.last_commit_number,
            source.last_sequence_number, source.last_transaction_key,
            source.last_replay_id, source.updated_at
        )
    """


def build_current_state(bronze_events):
    from pyspark.sql import Window
    from pyspark.sql.functions import (
        array_contains,
        coalesce,
        col,
        current_timestamp,
        explode,
        expr,
        first,
        lit,
        row_number,
        struct,
        when,
    )

    events = bronze_events.select(
        explode("record_ids").alias("opportunity_id"),
        "change_type",
        "changed_fields",
        "commit_timestamp",
        "replay_id",
        "payload",
        "_ingested_at",
        expr("payload:ChangeEventHeader:commitNumber::BIGINT").alias(
            "commit_number"
        ),
        expr("payload:ChangeEventHeader:sequenceNumber::BIGINT").alias(
            "sequence_number"
        ),
        expr("payload:ChangeEventHeader:transactionKey::STRING").alias(
            "transaction_key"
        ),
    )

    for column, (salesforce_field, value_expression) in BUSINESS_FIELDS.items():
        changed = (col("change_type") == "CREATE") | coalesce(
            array_contains("changed_fields", salesforce_field), lit(False)
        )
        events = events.withColumn(
            f"_{column}_update",
            when(
                changed,
                struct(
                    lit(True).alias("is_set"),
                    expr(value_expression).alias("value"),
                ),
            ),
        )

    event_order = Window.partitionBy("opportunity_id").orderBy(
        col("commit_timestamp").desc_nulls_last(),
        col("commit_number").desc_nulls_last(),
        col("sequence_number").desc_nulls_last(),
        col("_ingested_at").desc_nulls_last(),
        col("replay_id").desc_nulls_last(),
    )
    full_history = event_order.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    for column in BUSINESS_FIELDS:
        events = events.withColumn(
            f"_{column}_latest",
            first(f"_{column}_update", ignorenulls=True).over(full_history),
        )

    latest = events.withColumn("_event_rank", row_number().over(event_order)).where(
        col("_event_rank") == 1
    )

    projections = [col("opportunity_id")]
    for column in BUSINESS_FIELDS:
        projections.extend(
            [
                col(f"_{column}_latest").isNotNull().alias(
                    f"has_{column}_change"
                ),
                col(f"_{column}_latest.value").alias(column),
            ]
        )

    return latest.select(
        *projections,
        (col("change_type") == "DELETE").alias("is_deleted"),
        when(col("change_type") == "DELETE", col("commit_timestamp")).alias(
            "deleted_at"
        ),
        col("change_type").alias("last_change_type"),
        col("commit_timestamp").alias("last_commit_timestamp"),
        col("commit_number").alias("last_commit_number"),
        col("sequence_number").alias("last_sequence_number"),
        col("transaction_key").alias("last_transaction_key"),
        col("replay_id").alias("last_replay_id"),
        current_timestamp().alias("updated_at"),
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Maintain the Opportunity Silver current-state table"
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--silver-table", required=True)
    return parser.parse_args()


def run(
    catalog: str,
    bronze_schema: str,
    bronze_table: str,
    silver_schema: str,
    silver_table: str,
) -> None:
    from pyspark.sql import SparkSession

    identifiers = [
        catalog,
        bronze_schema,
        bronze_table,
        silver_schema,
        silver_table,
    ]
    catalog, bronze_schema, bronze_table, silver_schema, silver_table = [
        validate_identifier(identifier) for identifier in identifiers
    ]

    source_table = f"`{catalog}`.`{bronze_schema}`.`{bronze_table}`"
    target_table = f"`{catalog}`.`{silver_schema}`.`{silver_table}`"
    source_view = "opportunity_current_state_source"

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{silver_schema}`")
    spark.sql(silver_table_ddl(target_table))

    current_state = build_current_state(spark.table(source_table))
    current_state.createOrReplaceTempView(source_view)
    spark.sql(merge_sql(target_table, source_view))

    print(f"Silver merge completed: {catalog}.{silver_schema}.{silver_table}")


if __name__ == "__main__":
    arguments = parse_arguments()
    run(
        arguments.catalog,
        arguments.bronze_schema,
        arguments.bronze_table,
        arguments.silver_schema,
        arguments.silver_table,
    )