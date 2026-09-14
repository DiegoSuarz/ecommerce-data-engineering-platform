from audit import upsert_cdc_checkpoint
from db import get_postgres_connection
from load import insert_change_events
from logger import get_logger
from transform import (
    build_date_dimension_rows,
    calculate_scd2_hash,
)
import os
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from audit import (
    complete_cdc_batch,
    complete_etl_run,
    fail_etl_run,
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    start_cdc_batch,
    start_etl_run,
    upsert_cdc_checkpoint,
    update_cdc_checkpoint,
)

from cdc import (
    build_primary_key_columns_by_table,
    create_cdc_stream,
    get_current_binlog_coordinate,
    is_binlog_coordinate_ahead,
    iter_committed_transactions,
)

from db import (
    get_mysql_cdc_settings,
    get_postgres_connection,
)
from load import insert_change_events
from logger import get_logger


logger = get_logger("cdc_load")

PIPELINE_NAME = "change_data_capture"
CHECKPOINT_NAME = "mysql_sales_binlog"
SOURCE_SCHEMA = "sales"
DEFAULT_CDC_SERVER_ID = 100

TRANSFORMED_EVENT_COLUMNS = (
    "transformed_event_id",
    "batch_id",
    "raw_event_id",
    "event_key",
    "operation",
    "source_schema",
    "source_table",
    "primary_key",
    "before_values",
    "after_values",
    "binlog_file",
    "event_end_position",
    "row_index",
    "transaction_id",
    "commit_position",
    "event_timestamp",
    "commit_timestamp",
)

def persist_cdc_transaction(
    batch_id,
    transaction,
    pipeline_name,
    checkpoint_name,
):
    change_events = transaction[
        "change_events"
    ]

    binlog_file = transaction[
        "binlog_file"
    ]

    commit_position = transaction[
        "commit_position"
    ]

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                rows_inserted = (
                    insert_change_events(
                        cursor,
                        batch_id,
                        change_events,
                    )
                )

                upsert_cdc_checkpoint(
                    cursor,
                    pipeline_name,
                    checkpoint_name,
                    binlog_file,
                    commit_position,
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    logger.info(
        "CDC transaction persisted. "
        "transaction_id=%s "
        "events_received=%s "
        "events_inserted=%s "
        "checkpoint=%s:%s",
        transaction["transaction_id"],
        len(change_events),
        rows_inserted,
        binlog_file,
        commit_position,
    )

    return rows_inserted

def run_cdc_batch(
    orchestrator=None,
    orchestrator_run_id=None,
    orchestrator_task_id=None,
    orchestrator_try_number=None,
):
    run_id = start_etl_run(
        PIPELINE_NAME,
        orchestrator=orchestrator,
        orchestrator_run_id=orchestrator_run_id,
        orchestrator_task_id=orchestrator_task_id,
        orchestrator_try_number=(
            orchestrator_try_number
        ),
    )

    try:
        cdc_settings = (
            get_mysql_cdc_settings()
        )

        checkpoint = get_cdc_checkpoint(
            PIPELINE_NAME,
            CHECKPOINT_NAME,
        )

        if checkpoint is None:
            current_coordinate = (
                get_current_binlog_coordinate(
                    cdc_settings
                )
            )

            checkpoint = (
                initialize_cdc_checkpoint(
                    PIPELINE_NAME,
                    CHECKPOINT_NAME,
                    current_coordinate[0],
                    current_coordinate[1],
                )
            )

        (
            start_binlog_file,
            start_binlog_position,
        ) = checkpoint

        batch_id = start_cdc_batch(
            run_id=run_id,
            start_binlog_file=(
                start_binlog_file
            ),
            start_binlog_position=(
                start_binlog_position
            ),
        )

        primary_key_columns_by_table = (
            build_primary_key_columns_by_table(
                SOURCE_SCHEMA
            )
        )

        server_id = int(
            os.getenv(
                "MYSQL_CDC_SERVER_ID",
                DEFAULT_CDC_SERVER_ID,
            )
        )

        stream = create_cdc_stream(
            connection_settings=cdc_settings,
            source_schema=SOURCE_SCHEMA,
            log_file=start_binlog_file,
            log_position=start_binlog_position,
            server_id=server_id,
        )

        transactions_processed = 0
        events_processed = 0
        events_inserted = 0

        insert_events = 0
        update_events = 0
        delete_events = 0

        end_binlog_file = (
            start_binlog_file
        )
        end_binlog_position = (
            start_binlog_position
        )

        safe_read_coordinate = None

        try:
            for transaction in (
                iter_committed_transactions(
                    stream,
                    primary_key_columns_by_table,
                )
            ):
                rows_inserted = (
                    persist_cdc_transaction(
                        batch_id=batch_id,
                        transaction=transaction,
                        pipeline_name=PIPELINE_NAME,
                        checkpoint_name=(
                            CHECKPOINT_NAME
                        ),
                    )
                )

                transactions_processed += 1
                events_processed += len(
                    transaction["change_events"]
                )
                events_inserted += rows_inserted

                for change_event in (
                    transaction["change_events"]
                ):
                    operation = change_event[
                        "operation"
                    ]

                    if operation == "INSERT":
                        insert_events += 1
                    elif operation == "UPDATE":
                        update_events += 1
                    elif operation == "DELETE":
                        delete_events += 1

                end_binlog_file = transaction[
                    "binlog_file"
                ]
                end_binlog_position = transaction[
                    "commit_position"
                ]

            # This code is reached ONLY after normal
            # exhaustion of the non-blocking stream.
            safe_read_coordinate = (
                stream.log_file,
                stream.log_pos,
            )

        finally:
            stream.close()

        if (
            safe_read_coordinate
            != (
                end_binlog_file,
                end_binlog_position,
            )
        ):
            (
                safe_read_file,
                safe_read_position,
            ) = safe_read_coordinate

            update_cdc_checkpoint(
                PIPELINE_NAME,
                CHECKPOINT_NAME,
                safe_read_file,
                safe_read_position,
            )

            end_binlog_file = safe_read_file
            end_binlog_position = (
                safe_read_position
            )

            logger.info(
                "CDC checkpoint advanced to "
                "safe read cursor. "
                "checkpoint=%s:%s",
                end_binlog_file,
                end_binlog_position,
            )

        complete_cdc_batch(
            batch_id=batch_id,
            end_binlog_file=end_binlog_file,
            end_binlog_position=(
                end_binlog_position
            ),
            transactions_processed=(
                transactions_processed
            ),
            events_processed=events_processed,
            insert_events=insert_events,
            update_events=update_events,
            delete_events=delete_events,
        )

        complete_etl_run(
            run_id,
            rows_extracted=events_processed,
            rows_loaded=events_inserted,
            rows_rejected=0,
        )

        logger.info(
            "CDC batch completed successfully. "
            "transactions=%s "
            "events=%s "
            "inserted=%s "
            "checkpoint=%s:%s",
            transactions_processed,
            events_processed,
            events_inserted,
            end_binlog_file,
            end_binlog_position,
        )

        return {
            "run_id": run_id,
            "batch_id": batch_id,
            "transactions_processed": (
                transactions_processed
            ),
            "events_processed": (
                events_processed
            ),
            "events_inserted": (
                events_inserted
            ),
            "insert_events": insert_events,
            "update_events": update_events,
            "delete_events": delete_events,
            "end_binlog_file": (
                end_binlog_file
            ),
            "end_binlog_position": (
                end_binlog_position
            ),
        }

    except Exception as exc:
        fail_etl_run(
            run_id,
            str(exc),
        )

        logger.exception(
            "CDC batch failed."
        )

        raise

def fetch_transformed_events(
    cursor,
    batch_id,
):
    cursor.execute(
        """
        SELECT
            transformed_event_id,
            batch_id,
            raw_event_id,
            event_key,
            operation,
            source_schema,
            source_table,
            primary_key,
            before_values,
            after_values,
            binlog_file,
            event_end_position,
            row_index,
            transaction_id,
            commit_position,
            event_timestamp,
            commit_timestamp
        FROM cdc.transformed_event
        WHERE batch_id = %s
        ORDER BY transformed_event_id
        """,
        (batch_id,),
    )

    return [
        dict(
            zip(
                TRANSFORMED_EVENT_COLUMNS,
                row,
            )
        )
        for row in cursor.fetchall()
    ]

def validate_batch_ready_for_load(
    cursor,
    batch_id,
):
    cursor.execute(
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM cdc.raw_change_event
                WHERE batch_id = %s
            ) AS raw_count,
            (
                SELECT COUNT(*)
                FROM cdc.transformed_event
                WHERE batch_id = %s
            ) AS transformed_count
        """,
        (
            batch_id,
            batch_id,
        ),
    )

    raw_count, transformed_count = (
        cursor.fetchone()
    )

    if raw_count != transformed_count:
        raise RuntimeError(
            "CDC batch is not ready for LOAD: "
            f"batch_id={batch_id} "
            f"raw={raw_count} "
            f"transformed={transformed_count}."
        )

    return {
        "raw_events": raw_count,
        "transformed_events": (
            transformed_count
        ),
    }

def parse_cdc_timestamp(
    value,
    field_name,
):
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00",
                )
            )
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid CDC timestamp for "
                f"{field_name}: {value!r}."
            ) from exc
    else:
        raise RuntimeError(
            f"Invalid CDC timestamp type for "
            f"{field_name}: "
            f"{type(value).__name__}."
        )

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return timestamp.astimezone(
        timezone.utc
    )


def require_category_values(values):
    required_fields = (
        "category_id",
        "category_code",
        "category_name",
        "department",
        "updated_at",
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in values
    ]

    if missing_fields:
        raise RuntimeError(
            "Category CDC event is missing "
            "required fields: "
            f"{missing_fields}."
        )


def fetch_category_versions_for_update(
    cursor,
    category_id,
):
    cursor.execute(
        """
        SELECT
            category_key,
            category_code,
            category_name,
            department,
            effective_from,
            effective_to,
            is_current
        FROM dw.dim_category
        WHERE category_id = %s
        ORDER BY effective_from
        FOR UPDATE;
        """,
        (category_id,),
    )

    columns = (
        "category_key",
        "category_code",
        "category_name",
        "department",
        "effective_from",
        "effective_to",
        "is_current",
    )

    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def get_current_category_version(
    versions,
    category_id,
):
    current_versions = [
        version
        for version in versions
        if version["is_current"]
    ]

    if len(current_versions) > 1:
        raise RuntimeError(
            "Multiple current category "
            "versions detected for "
            f"category_id={category_id}."
        )

    if not current_versions:
        return None

    return current_versions[0]


def apply_category_insert(
    cursor,
    event,
):
    values = event["after_values"]

    if values is None:
        raise RuntimeError(
            "Category INSERT requires "
            "after_values."
        )

    require_category_values(values)

    category_id = values[
        "category_id"
    ]

    effective_from = (
        parse_cdc_timestamp(
            values["updated_at"],
            "categories.updated_at",
        )
    )

    versions = (
        fetch_category_versions_for_update(
            cursor,
            category_id,
        )
    )

    current = (
        get_current_category_version(
            versions,
            category_id,
        )
    )

    if current is not None:
        if (
            current["effective_from"]
            == effective_from
            and current["category_code"]
            == values["category_code"]
            and current["category_name"]
            == values["category_name"]
            and current["department"]
            == values["department"]
        ):
            return 0

        raise RuntimeError(
            "Category INSERT conflicts "
            "with an existing current "
            "dimension version for "
            f"category_id={category_id}."
        )

    for version in versions:
        if (
            version["effective_from"]
            == effective_from
            and version["category_code"]
            == values["category_code"]
            and version["department"]
            == values["department"]
        ):
            return 0

    cursor.execute(
        """
        INSERT INTO dw.dim_category
        (
            category_id,
            category_code,
            category_name,
            department,
            effective_from,
            effective_to,
            is_current
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            NULL,
            TRUE
        );
        """,
        (
            category_id,
            values["category_code"],
            values["category_name"],
            values["department"],
            effective_from,
        ),
    )

    return 1


def apply_category_update(
    cursor,
    event,
):
    before_values = event[
        "before_values"
    ]
    after_values = event[
        "after_values"
    ]

    if (
        before_values is None
        or after_values is None
    ):
        raise RuntimeError(
            "Category UPDATE requires "
            "before_values and "
            "after_values."
        )

    require_category_values(
        before_values
    )
    require_category_values(
        after_values
    )

    if (
        before_values["category_id"]
        != after_values["category_id"]
    ):
        raise RuntimeError(
            "category_id is an immutable "
            "Category business key."
        )

    if (
        before_values["category_code"]
        != after_values["category_code"]
    ):
        raise RuntimeError(
            "category_code is SCD0 and "
            "cannot be changed."
        )

    category_id = after_values[
        "category_id"
    ]

    event_category_id = event[
        "primary_key"
    ].get(
        "category_id"
    )

    if event_category_id != category_id:
        raise RuntimeError(
            "Category CDC primary key does "
            "not match after_values."
        )

    effective_from = (
        parse_cdc_timestamp(
            after_values["updated_at"],
            "categories.updated_at",
        )
    )

    versions = (
        fetch_category_versions_for_update(
            cursor,
            category_id,
        )
    )

    current = (
        get_current_category_version(
            versions,
            category_id,
        )
    )

    if current is None:
        raise RuntimeError(
            "Category UPDATE cannot be "
            "applied because no current "
            "dimension version exists for "
            f"category_id={category_id}."
        )

    if (
        current["category_code"]
        != after_values["category_code"]
    ):
        raise RuntimeError(
            "category_code is SCD0 and "
            "does not match the current "
            "dimension version."
        )

    if (
        current["effective_from"]
        == effective_from
        and current["category_code"]
        == after_values[
            "category_code"
        ]
        and current["category_name"]
        == after_values[
            "category_name"
        ]
        and current["department"]
        == after_values[
            "department"
        ]
    ):
        return 0

    department_changed = (
        current["department"]
        != after_values["department"]
    )

    if department_changed:
        if (
            effective_from
            <= current["effective_from"]
        ):
            raise RuntimeError(
                "Category SCD2 effective "
                "timestamp must be newer "
                "than the current version."
            )

        cursor.execute(
            """
            UPDATE dw.dim_category
            SET
                effective_to = %s,
                is_current = FALSE
            WHERE category_key = %s;
            """,
            (
                effective_from,
                current["category_key"],
            ),
        )

        cursor.execute(
            """
            INSERT INTO dw.dim_category
            (
                category_id,
                category_code,
                category_name,
                department,
                effective_from,
                effective_to,
                is_current
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                NULL,
                TRUE
            );
            """,
            (
                category_id,
                after_values[
                    "category_code"
                ],
                after_values[
                    "category_name"
                ],
                after_values[
                    "department"
                ],
                effective_from,
            ),
        )

    cursor.execute(
        """
        UPDATE dw.dim_category
        SET category_name = %s
        WHERE category_id = %s
          AND category_name
              IS DISTINCT FROM %s;
        """,
        (
            after_values[
                "category_name"
            ],
            category_id,
            after_values[
                "category_name"
            ],
        ),
    )

    return 1


def apply_category_delete(
    cursor,
    event,
):
    before_values = event[
        "before_values"
    ]

    if before_values is None:
        raise RuntimeError(
            "Category DELETE requires "
            "before_values."
        )

    category_id = before_values[
        "category_id"
    ]

    event_category_id = event[
        "primary_key"
    ].get(
        "category_id"
    )

    if event_category_id != category_id:
        raise RuntimeError(
            "Category CDC primary key does "
            "not match before_values."
        )

    deleted_at = parse_cdc_timestamp(
        event["commit_timestamp"],
        "commit_timestamp",
    )

    versions = (
        fetch_category_versions_for_update(
            cursor,
            category_id,
        )
    )

    current = (
        get_current_category_version(
            versions,
            category_id,
        )
    )

    if current is None:
        for version in versions:
            if (
                version["effective_to"]
                == deleted_at
                and not version[
                    "is_current"
                ]
            ):
                return 0

        raise RuntimeError(
            "Category DELETE cannot be "
            "applied because no current "
            "dimension version exists for "
            f"category_id={category_id}."
        )

    if deleted_at <= current[
        "effective_from"
    ]:
        raise RuntimeError(
            "Category DELETE timestamp "
            "must be newer than the "
            "current version."
        )

    cursor.execute(
        """
        UPDATE dw.dim_category
        SET
            effective_to = %s,
            is_current = FALSE
        WHERE category_key = %s;
        """,
        (
            deleted_at,
            current["category_key"],
        ),
    )

    return 1


def apply_category_change_event(
    cursor,
    event,
):
    if event["source_schema"] != SOURCE_SCHEMA:
        raise RuntimeError(
            "Unsupported CDC source schema: "
            f"{event['source_schema']!r}."
        )

    if event["source_table"] != "categories":
        raise RuntimeError(
            "Category handler received "
            "an event for source table "
            f"{event['source_table']!r}."
        )

    operation = event["operation"]

    if operation == "INSERT":
        return apply_category_insert(
            cursor,
            event,
        )

    if operation == "UPDATE":
        return apply_category_update(
            cursor,
            event,
        )

    if operation == "DELETE":
        return apply_category_delete(
            cursor,
            event,
        )

    raise RuntimeError(
        "Unsupported Category CDC "
        f"operation: {operation!r}."
    )


def require_country_values(values):
    required_fields = (
        "country_id",
        "country_code",
        "country_name",
        "sales_region",
        "market_segment",
        "updated_at",
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in values
    ]

    if missing_fields:
        raise RuntimeError(
            "Country CDC event is missing "
            "required fields: "
            f"{missing_fields}."
        )


def fetch_country_versions_for_update(
    cursor,
    country_id,
):
    cursor.execute(
        """
        SELECT
            country_key,
            country_code,
            country_name,
            sales_region,
            market_segment,
            row_hash,
            effective_from,
            effective_to,
            is_current
        FROM dw.dim_country
        WHERE country_id = %s
        ORDER BY effective_from
        FOR UPDATE;
        """,
        (country_id,),
    )

    columns = (
        "country_key",
        "country_code",
        "country_name",
        "sales_region",
        "market_segment",
        "row_hash",
        "effective_from",
        "effective_to",
        "is_current",
    )

    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def get_current_country_version(
    versions,
    country_id,
):
    current_versions = [
        version
        for version in versions
        if version["is_current"]
    ]

    if len(current_versions) > 1:
        raise RuntimeError(
            "Multiple current country "
            "versions detected for "
            f"country_id={country_id}."
        )

    if not current_versions:
        return None

    return current_versions[0]


def apply_country_insert(
    cursor,
    event,
):
    values = event["after_values"]

    if values is None:
        raise RuntimeError(
            "Country INSERT requires "
            "after_values."
        )

    require_country_values(values)

    country_id = values["country_id"]

    event_country_id = (
        event["primary_key"].get(
            "country_id"
        )
    )

    if event_country_id != country_id:
        raise RuntimeError(
            "Country CDC primary key does "
            "not match after_values."
        )

    effective_from = (
        parse_cdc_timestamp(
            values["updated_at"],
            "countries.updated_at",
        )
    )

    row_hash = calculate_scd2_hash(
        values["sales_region"],
        values["market_segment"],
    )

    versions = (
        fetch_country_versions_for_update(
            cursor,
            country_id,
        )
    )

    current = (
        get_current_country_version(
            versions,
            country_id,
        )
    )

    if current is not None:
        if (
            current["effective_from"]
            == effective_from
            and current["country_code"]
            == values["country_code"]
            and current["country_name"]
            == values["country_name"]
            and current["sales_region"]
            == values["sales_region"]
            and current["market_segment"]
            == values["market_segment"]
            and current["row_hash"]
            == row_hash
        ):
            return 0

        raise RuntimeError(
            "Country INSERT conflicts "
            "with an existing current "
            "dimension version for "
            f"country_id={country_id}."
        )

    for version in versions:
        if (
            version["effective_from"]
            == effective_from
            and version["country_code"]
            == values["country_code"]
            and version["country_name"]
            == values["country_name"]
            and version["sales_region"]
            == values["sales_region"]
            and version["market_segment"]
            == values["market_segment"]
            and version["row_hash"]
            == row_hash
        ):
            return 0

    cursor.execute(
        """
        INSERT INTO dw.dim_country
        (
            country_id,
            country_code,
            country_name,
            sales_region,
            market_segment,
            row_hash,
            effective_from,
            effective_to,
            is_current
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            NULL,
            TRUE
        );
        """,
        (
            country_id,
            values["country_code"],
            values["country_name"],
            values["sales_region"],
            values["market_segment"],
            row_hash,
            effective_from,
        ),
    )

    return 1


def apply_country_update(
    cursor,
    event,
):
    before_values = event[
        "before_values"
    ]
    after_values = event[
        "after_values"
    ]

    if (
        before_values is None
        or after_values is None
    ):
        raise RuntimeError(
            "Country UPDATE requires "
            "before_values and "
            "after_values."
        )

    require_country_values(
        before_values
    )
    require_country_values(
        after_values
    )

    if (
        before_values["country_id"]
        != after_values["country_id"]
    ):
        raise RuntimeError(
            "country_id is an immutable "
            "Country business key."
        )

    if (
        before_values["country_code"]
        != after_values["country_code"]
    ):
        raise RuntimeError(
            "country_code is SCD0 and "
            "cannot be changed."
        )

    country_id = after_values[
        "country_id"
    ]

    event_country_id = (
        event["primary_key"].get(
            "country_id"
        )
    )

    if event_country_id != country_id:
        raise RuntimeError(
            "Country CDC primary key does "
            "not match after_values."
        )

    effective_from = (
        parse_cdc_timestamp(
            after_values["updated_at"],
            "countries.updated_at",
        )
    )

    new_hash = calculate_scd2_hash(
        after_values["sales_region"],
        after_values[
            "market_segment"
        ],
    )

    versions = (
        fetch_country_versions_for_update(
            cursor,
            country_id,
        )
    )

    current = (
        get_current_country_version(
            versions,
            country_id,
        )
    )

    if current is None:
        raise RuntimeError(
            "Country UPDATE cannot be "
            "applied because no current "
            "dimension version exists for "
            f"country_id={country_id}."
        )

    if (
        current["country_code"]
        != after_values["country_code"]
    ):
        raise RuntimeError(
            "country_code is SCD0 and "
            "does not match the current "
            "dimension version."
        )

    if (
        current["effective_from"]
        == effective_from
        and current["country_code"]
        == after_values["country_code"]
        and current["country_name"]
        == after_values["country_name"]
        and current["sales_region"]
        == after_values["sales_region"]
        and current["market_segment"]
        == after_values[
            "market_segment"
        ]
        and current["row_hash"]
        == new_hash
    ):
        return 0

    scd2_changed = (
        current["row_hash"]
        != new_hash
    )

    if (
        not scd2_changed
        and current["country_name"]
        == after_values["country_name"]
    ):
        return 0

    if scd2_changed:
        if (
            effective_from
            <= current["effective_from"]
        ):
            raise RuntimeError(
                "Country SCD2 effective "
                "timestamp must be newer "
                "than the current version."
            )

        cursor.execute(
            """
            UPDATE dw.dim_country
            SET
                effective_to = %s,
                is_current = FALSE
            WHERE country_key = %s;
            """,
            (
                effective_from,
                current["country_key"],
            ),
        )

        cursor.execute(
            """
            INSERT INTO dw.dim_country
            (
                country_id,
                country_code,
                country_name,
                sales_region,
                market_segment,
                row_hash,
                effective_from,
                effective_to,
                is_current
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                NULL,
                TRUE
            );
            """,
            (
                country_id,
                after_values[
                    "country_code"
                ],
                after_values[
                    "country_name"
                ],
                after_values[
                    "sales_region"
                ],
                after_values[
                    "market_segment"
                ],
                new_hash,
                effective_from,
            ),
        )

    cursor.execute(
        """
        UPDATE dw.dim_country
        SET country_name = %s
        WHERE country_id = %s
          AND country_name
              IS DISTINCT FROM %s;
        """,
        (
            after_values[
                "country_name"
            ],
            country_id,
            after_values[
                "country_name"
            ],
        ),
    )

    return 1


def apply_country_delete(
    cursor,
    event,
):
    before_values = event[
        "before_values"
    ]

    if before_values is None:
        raise RuntimeError(
            "Country DELETE requires "
            "before_values."
        )

    country_id = before_values[
        "country_id"
    ]

    event_country_id = (
        event["primary_key"].get(
            "country_id"
        )
    )

    if event_country_id != country_id:
        raise RuntimeError(
            "Country CDC primary key does "
            "not match before_values."
        )

    deleted_at = parse_cdc_timestamp(
        event["commit_timestamp"],
        "commit_timestamp",
    )

    versions = (
        fetch_country_versions_for_update(
            cursor,
            country_id,
        )
    )

    current = (
        get_current_country_version(
            versions,
            country_id,
        )
    )

    if current is None:
        for version in versions:
            if (
                version["effective_to"]
                == deleted_at
                and not version[
                    "is_current"
                ]
            ):
                return 0

        raise RuntimeError(
            "Country DELETE cannot be "
            "applied because no current "
            "dimension version exists for "
            f"country_id={country_id}."
        )

    if (
        deleted_at
        <= current["effective_from"]
    ):
        raise RuntimeError(
            "Country DELETE timestamp "
            "must be newer than the "
            "current version."
        )

    cursor.execute(
        """
        UPDATE dw.dim_country
        SET
            effective_to = %s,
            is_current = FALSE
        WHERE country_key = %s;
        """,
        (
            deleted_at,
            current["country_key"],
        ),
    )

    return 1


def apply_country_change_event(
    cursor,
    event,
):
    if event["source_schema"] != SOURCE_SCHEMA:
        raise RuntimeError(
            "Unsupported CDC source schema: "
            f"{event['source_schema']!r}."
        )

    if event["source_table"] != "countries":
        raise RuntimeError(
            "Country handler received "
            "an event for source table "
            f"{event['source_table']!r}."
        )

    operation = event["operation"]

    if operation == "INSERT":
        return apply_country_insert(
            cursor,
            event,
        )

    if operation == "UPDATE":
        return apply_country_update(
            cursor,
            event,
        )

    if operation == "DELETE":
        return apply_country_delete(
            cursor,
            event,
        )

    raise RuntimeError(
        "Unsupported Country CDC "
        f"operation: {operation!r}."
    )


def require_order_values(values):
    required_fields = (
        "order_id",
        "order_date",
        "country_id",
        "category_id",
        "amount",
        "updated_at",
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in values
    ]

    if missing_fields:
        raise RuntimeError(
            "Order CDC event is missing "
            "required fields: "
            f"{missing_fields}."
        )


def parse_cdc_date(
    value,
    field_name,
):
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            return date.fromisoformat(
                value
            )
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid CDC date for "
                f"{field_name}: {value!r}."
            ) from exc

    raise RuntimeError(
        f"Invalid CDC date type for "
        f"{field_name}: "
        f"{type(value).__name__}."
    )


def parse_order_amount(value):
    try:
        amount = Decimal(str(value))
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ) as exc:
        raise RuntimeError(
            "Invalid order amount: "
            f"{value!r}."
        ) from exc

    if amount <= 0:
        raise RuntimeError(
            "Order amount must be "
            "greater than zero."
        )

    return amount


def ensure_order_date_dimension(
    cursor,
    order_date,
):
    date_row = (
        build_date_dimension_rows(
            [order_date]
        )[0]
    )

    cursor.execute(
        """
        INSERT INTO dw.dim_date
        (
            date_key,
            full_date,
            year,
            quarter,
            quarter_name,
            month,
            month_name,
            day,
            weekday,
            weekday_name
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (date_key)
        DO NOTHING;
        """,
        date_row,
    )

    return date_row[0]


def resolve_historical_country_key(
    cursor,
    country_id,
    order_date,
):
    order_timestamp = datetime.combine(
        order_date,
        datetime.min.time(),
        tzinfo=timezone.utc,
    )

    cursor.execute(
        """
        SELECT country_key
        FROM dw.dim_country
        WHERE country_id = %s
          AND %s >= effective_from
          AND (
                effective_to IS NULL
                OR %s < effective_to
          )
        ORDER BY effective_from DESC;
        """,
        (
            country_id,
            order_timestamp,
            order_timestamp,
        ),
    )

    rows = cursor.fetchall()

    if len(rows) != 1:
        raise RuntimeError(
            "Historical country dimension "
            "resolution failed for "
            f"country_id={country_id} "
            f"order_date={order_date}: "
            f"matches={len(rows)}."
        )

    return rows[0][0]


def resolve_historical_category_key(
    cursor,
    category_id,
    order_date,
):
    order_timestamp = datetime.combine(
        order_date,
        datetime.min.time(),
        tzinfo=timezone.utc,
    )

    cursor.execute(
        """
        SELECT category_key
        FROM dw.dim_category
        WHERE category_id = %s
          AND %s >= effective_from
          AND (
                effective_to IS NULL
                OR %s < effective_to
          )
        ORDER BY effective_from DESC;
        """,
        (
            category_id,
            order_timestamp,
            order_timestamp,
        ),
    )

    rows = cursor.fetchall()

    if len(rows) != 1:
        raise RuntimeError(
            "Historical category dimension "
            "resolution failed for "
            f"category_id={category_id} "
            f"order_date={order_date}: "
            f"matches={len(rows)}."
        )

    return rows[0][0]


def apply_order_upsert(
    cursor,
    event,
):
    values = event["after_values"]

    if values is None:
        raise RuntimeError(
            "Order INSERT/UPDATE requires "
            "after_values."
        )

    require_order_values(values)

    order_id = values["order_id"]

    event_order_id = (
        event["primary_key"].get(
            "order_id"
        )
    )

    if event_order_id != order_id:
        raise RuntimeError(
            "Order CDC primary key does "
            "not match after_values."
        )

    if event["operation"] == "UPDATE":
        before_values = event[
            "before_values"
        ]

        if before_values is None:
            raise RuntimeError(
                "Order UPDATE requires "
                "before_values."
            )

        require_order_values(
            before_values
        )

        if (
            before_values["order_id"]
            != order_id
        ):
            raise RuntimeError(
                "order_id is an immutable "
                "Order business key."
            )

    order_date = parse_cdc_date(
        values["order_date"],
        "orders.order_date",
    )

    source_updated_at = (
        parse_cdc_timestamp(
            values["updated_at"],
            "orders.updated_at",
        )
    )

    amount = parse_order_amount(
        values["amount"]
    )

    country_key = (
        resolve_historical_country_key(
            cursor,
            values["country_id"],
            order_date,
        )
    )

    category_key = (
        resolve_historical_category_key(
            cursor,
            values["category_id"],
            order_date,
        )
    )

    date_key = (
        ensure_order_date_dimension(
            cursor,
            order_date,
        )
    )

    cursor.execute(
        """
        INSERT INTO dw.fact_sales
        (
            order_id,
            date_key,
            country_key,
            category_key,
            amount,
            source_updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (order_id)
        DO UPDATE
        SET
            date_key =
                EXCLUDED.date_key,
            country_key =
                EXCLUDED.country_key,
            category_key =
                EXCLUDED.category_key,
            amount =
                EXCLUDED.amount,
            source_updated_at =
                EXCLUDED.source_updated_at
        WHERE
            dw.fact_sales.date_key
                IS DISTINCT FROM
                EXCLUDED.date_key
            OR
            dw.fact_sales.country_key
                IS DISTINCT FROM
                EXCLUDED.country_key
            OR
            dw.fact_sales.category_key
                IS DISTINCT FROM
                EXCLUDED.category_key
            OR
            dw.fact_sales.amount
                IS DISTINCT FROM
                EXCLUDED.amount
            OR
            dw.fact_sales.source_updated_at
                IS DISTINCT FROM
                EXCLUDED.source_updated_at
        RETURNING sales_key;
        """,
        (
            order_id,
            date_key,
            country_key,
            category_key,
            amount,
            source_updated_at,
        ),
    )

    if cursor.fetchone() is None:
        return 0

    return 1


def apply_order_delete(
    cursor,
    event,
):
    before_values = event[
        "before_values"
    ]

    if before_values is None:
        raise RuntimeError(
            "Order DELETE requires "
            "before_values."
        )

    require_order_values(
        before_values
    )

    order_id = before_values[
        "order_id"
    ]

    event_order_id = (
        event["primary_key"].get(
            "order_id"
        )
    )

    if event_order_id != order_id:
        raise RuntimeError(
            "Order CDC primary key does "
            "not match before_values."
        )

    cursor.execute(
        """
        DELETE FROM dw.fact_sales
        WHERE order_id = %s
        RETURNING sales_key;
        """,
        (order_id,),
    )

    if cursor.fetchone() is None:
        return 0

    return 1


def apply_order_change_event(
    cursor,
    event,
):
    if event["source_schema"] != SOURCE_SCHEMA:
        raise RuntimeError(
            "Unsupported CDC source schema: "
            f"{event['source_schema']!r}."
        )

    if event["source_table"] != "orders":
        raise RuntimeError(
            "Order handler received "
            "an event for source table "
            f"{event['source_table']!r}."
        )

    operation = event["operation"]

    if operation in (
        "INSERT",
        "UPDATE",
    ):
        return apply_order_upsert(
            cursor,
            event,
        )

    if operation == "DELETE":
        return apply_order_delete(
            cursor,
            event,
        )

    raise RuntimeError(
        "Unsupported Order CDC "
        f"operation: {operation!r}."
    )


def apply_change_events_to_dw(
    cursor,
    change_events,
):
    """
    Apply CDC events to the analytical DW
    in their original transformed order.
    """

    events_applied = 0

    for event in change_events:
        if (
            event["source_schema"]
            != SOURCE_SCHEMA
        ):
            raise RuntimeError(
                "Unsupported CDC source "
                "schema: "
                f"{event['source_schema']!r}."
            )

        source_table = event[
            "source_table"
        ]

        if source_table == "categories":
            events_applied += (
                apply_category_change_event(
                    cursor,
                    event,
                )
            )
            continue

        if source_table == "countries":
            events_applied += (
                apply_country_change_event(
                    cursor,
                    event,
                )
            )
            continue

        if source_table == "orders":
            events_applied += (
                apply_order_change_event(
                    cursor,
                    event,
                )
            )
            continue

        raise NotImplementedError(
            "CDC DW apply handler is not "
            "implemented yet for "
            f"{SOURCE_SCHEMA}."
            f"{source_table}."
        )

    return events_applied


def load_transformed_cdc_batch(
    batch_id,
    end_binlog_file,
    end_binlog_position,
    pipeline_name=PIPELINE_NAME,
    checkpoint_name=CHECKPOINT_NAME,
):
    """
    Load one fully transformed CDC batch.

    Final ChangeEvents and the APPLY
    checkpoint are committed atomically.
    """

    target_coordinate = (
        end_binlog_file,
        end_binlog_position,
    )

    current_checkpoint = (
        get_cdc_checkpoint(
            pipeline_name,
            checkpoint_name,
        )
    )

    if current_checkpoint is None:
        raise RuntimeError(
            "CDC APPLY checkpoint does "
            "not exist."
        )

    if (
        target_coordinate
        != current_checkpoint
        and not is_binlog_coordinate_ahead(
            target_coordinate,
            current_checkpoint,
        )
    ):
        raise RuntimeError(
            "CDC APPLY checkpoint "
            "regression rejected: "
            f"current={current_checkpoint} "
            f"target={target_coordinate}."
        )

    if target_coordinate == current_checkpoint:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                validation = (
                    validate_batch_ready_for_load(
                        cursor=cursor,
                        batch_id=batch_id,
                    )
                )

        durable_metrics = (
            get_loaded_batch_metrics(
                batch_id
            )
        )

        if (
            validation["transformed_events"]
            != durable_metrics["event_count"]
        ):
            raise RuntimeError(
                "CDC LOAD replay reconciliation "
                "failed: "
                f"batch_id={batch_id} "
                "transformed_events="
                f"{validation['transformed_events']} "
                "durable_final_events="
                f"{durable_metrics['event_count']}."
            )

        result = {
            "batch_id": batch_id,
            "raw_events": validation[
                "raw_events"
            ],
            "transformed_events": validation[
                "transformed_events"
            ],
            "events_loaded": validation[
                "transformed_events"
            ],
            "events_inserted": 0,
            "events_applied": 0,
            "end_binlog_file": (
                end_binlog_file
            ),
            "end_binlog_position": (
                end_binlog_position
            ),
        }

        logger.info(
            "CDC LOAD committed replay "
            "detected. batch_id=%s "
            "events=%s "
            "apply_checkpoint=%s:%s",
            batch_id,
            validation[
                "transformed_events"
            ],
            end_binlog_file,
            end_binlog_position,
        )

        return result

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                validation = (
                    validate_batch_ready_for_load(
                        cursor=cursor,
                        batch_id=batch_id,
                    )
                )

                transformed_events = (
                    fetch_transformed_events(
                        cursor=cursor,
                        batch_id=batch_id,
                    )
                )

                rows_inserted = (
                    insert_change_events(
                        cursor=cursor,
                        batch_id=batch_id,
                        change_events=(
                            transformed_events
                        ),
                    )
                )

                events_applied = (
                    apply_change_events_to_dw(
                        cursor=cursor,
                        change_events=(
                            transformed_events
                        ),
                    )
                )

                upsert_cdc_checkpoint(
                    cursor,
                    pipeline_name,
                    checkpoint_name,
                    end_binlog_file,
                    end_binlog_position,
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    result = {
        "batch_id": batch_id,
        "raw_events": validation[
            "raw_events"
        ],
        "transformed_events": validation[
            "transformed_events"
        ],
        "events_loaded": len(
            transformed_events
        ),
        "events_inserted": rows_inserted,
        "events_applied": events_applied,
        "end_binlog_file": (
            end_binlog_file
        ),
        "end_binlog_position": (
            end_binlog_position
        ),
    }

    logger.info(
        "CDC LOAD completed. "
        "batch_id=%s events=%s "
        "inserted=%s "
        "apply_checkpoint=%s:%s",
        batch_id,
        len(transformed_events),
        rows_inserted,
        end_binlog_file,
        end_binlog_position,
    )

    return result

def get_transformed_batch_metrics(
    batch_id,
):
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(
                        DISTINCT (
                            binlog_file,
                            transaction_id,
                            commit_position
                        )
                    ),
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE operation = 'INSERT'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'UPDATE'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'DELETE'
                    )
                FROM cdc.transformed_event
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            (
                transaction_count,
                event_count,
                insert_count,
                update_count,
                delete_count,
            ) = cursor.fetchone()

    return {
        "transaction_count": (
            transaction_count
        ),
        "event_count": event_count,
        "insert_count": insert_count,
        "update_count": update_count,
        "delete_count": delete_count,
    }

def get_loaded_batch_metrics(
    batch_id,
):
    """
    Read durable FINAL CDC metrics for a
    batch from cdc.change_event.

    These metrics describe committed batch
    state, not work performed by one retry.
    """
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE operation = 'INSERT'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'UPDATE'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'DELETE'
                    )
                FROM cdc.change_event
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            (
                event_count,
                insert_count,
                update_count,
                delete_count,
            ) = cursor.fetchone()

    return {
        "event_count": event_count,
        "insert_count": insert_count,
        "update_count": update_count,
        "delete_count": delete_count,
    }


if __name__ == "__main__":
    run_cdc_batch()
