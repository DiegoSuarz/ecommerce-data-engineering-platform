import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from db import (
    get_mysql_connection,
    get_postgres_connection,
)
from extract import extract_incremental_orders


pytestmark = pytest.mark.integration


def test_extract_composite_watermark_tie_breaker():
    test_timestamp = datetime(
        2037,
        1,
        1,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    first_order_id = 900001
    second_order_id = 900002

    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM orders
                WHERE order_id IN (%s, %s);
                """,
                (
                    first_order_id,
                    second_order_id,
                ),
            )

            cursor.executemany(
                """
                INSERT INTO orders
                (
                    order_id,
                    order_date,
                    country_id,
                    category_id,
                    amount,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                [
                    (
                        first_order_id,
                        "2022-01-10",
                        1,
                        1,
                        100.00,
                        test_timestamp,
                    ),
                    (
                        second_order_id,
                        "2022-01-10",
                        2,
                        2,
                        200.00,
                        test_timestamp,
                    ),
                ],
            )

        connection.commit()

    try:
        extracted = []

        for batch in extract_incremental_orders(
            test_timestamp,
            first_order_id,
            batch_size=10000,
        ):
            extracted.extend(batch)

        extracted_order_ids = [
            row[0]
            for row in extracted
            if row[0] in (
                first_order_id,
                second_order_id,
            )
        ]

        assert extracted_order_ids == [
            second_order_id
        ]

    finally:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM orders
                    WHERE order_id IN (%s, %s);
                    """,
                    (
                        first_order_id,
                        second_order_id,
                    ),
                )

            connection.commit()


def test_old_order_update_has_newer_composite_watermark():
    test_order_id = 150000

    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    amount,
                    updated_at
                FROM orders
                WHERE order_id = %s;
                """,
                (test_order_id,),
            )

            original_row = cursor.fetchone()

    assert original_row is not None

    original_amount = original_row[0]
    original_updated_at = original_row[1].replace(
        tzinfo=timezone.utc
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    watermark_timestamp,
                    watermark_order_id
                FROM audit.pipeline_watermark
                WHERE pipeline_name = 'incremental_load'
                  AND watermark_name =
                      'orders_updated_at_order_id';
                """
            )

            watermark = cursor.fetchone()

    assert watermark is not None

    watermark_timestamp = watermark[0]
    watermark_order_id = watermark[1]

    new_amount = original_amount + 1

    try:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE orders
                    SET amount = %s
                    WHERE order_id = %s;
                    """,
                    (
                        new_amount,
                        test_order_id,
                    ),
                )

                cursor.execute(
                    """
                    SELECT
                        updated_at
                    FROM orders
                    WHERE order_id = %s;
                    """,
                    (test_order_id,),
                )

                new_updated_at = cursor.fetchone()[0]
                new_updated_at = new_updated_at.replace(
                    tzinfo=timezone.utc
                )

            connection.commit()

        candidate_watermark = (
            new_updated_at,
            test_order_id,
        )

        current_watermark = (
            watermark_timestamp,
            watermark_order_id,
        )

        assert new_updated_at > original_updated_at

        assert (
            candidate_watermark
            > current_watermark
        )

        extracted = []

        for batch in extract_incremental_orders(
            watermark_timestamp,
            watermark_order_id,
            batch_size=10000,
        ):
            extracted.extend(batch)

        extracted_order_ids = [
            row[0] for row in extracted
        ]

        assert test_order_id in extracted_order_ids

    finally:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE orders
                    SET
                        amount = %s,
                        updated_at = %s
                    WHERE order_id = %s;
                    """,
                    (
                        original_amount,
                        original_updated_at,
                        test_order_id,
                    ),
                )

            connection.commit()
