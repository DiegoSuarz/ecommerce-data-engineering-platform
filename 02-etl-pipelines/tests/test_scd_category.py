import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from db import get_postgres_connection
from load import load_dim_category


pytestmark = pytest.mark.integration

TEST_CATEGORY_ID = 910001
TYPE1_CATEGORY_ID = 910002
TYPE2_CATEGORY_ID = 910003
TYPE2_IDEMPOTENT_CATEGORY_ID = 910004
TEMPORAL_CATEGORY_ID = 910005
INITIAL_BOUNDARY_CATEGORY_ID = 920006
INITIAL_BOUNDARY_COUNTRY_ID = 920006

INITIAL_BOUNDARY = datetime(
    1900,
    1,
    1,
    0,
    0,
    0,
    tzinfo=timezone.utc,
)

INITIAL_BOUNDARY = datetime(
    1900,
    1,
    1,
    0,
    0,
    0,
    tzinfo=timezone.utc,
)

TEST_EFFECTIVE_FROM = datetime(
    2035,
    1,
    1,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)

TYPE2_CHANGE_TIMESTAMP = datetime(
    2035,
    1,
    2,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)

TEMPORAL_START = datetime(
    2035,
    1,
    1,
    0,
    0,
    0,
    tzinfo=timezone.utc,
)

TEMPORAL_CHANGE = datetime(
    2035,
    2,
    1,
    0,
    0,
    0,
    tzinfo=timezone.utc,
)

def cleanup_test_category(category_id):
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM dw.dim_category
                WHERE category_id = %s;
                """,
                (category_id,),
            )

            cursor.execute(
                """
                DELETE FROM staging.categories
                WHERE category_id = %s;
                """,
                (category_id,),
            )

        connection.commit()


def test_scd_type0_preserves_original_value():
    cleanup_test_category(TEST_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.categories
                    (
                        category_id,
                        category_code,
                        category_name,
                        department,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        TEST_CATEGORY_ID,
                        "TEST-A",
                        "Test Category",
                        "Test Department",
                        TEST_EFFECTIVE_FROM,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        category_code,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None

        original_category_key = original_row[0]

        assert original_row[1] == "TEST-A"
        assert original_row[2] is True

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.categories
                    SET
                        category_code = %s,
                        updated_at = %s
                    WHERE category_id = %s;
                    """,
                    (
                        "TEST-B",
                        datetime(
                            2035,
                            1,
                            2,
                            12,
                            0,
                            0,
                            tzinfo=timezone.utc,
                        ),
                        TEST_CATEGORY_ID,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        category_code,
                        is_current,
                        COUNT(*) OVER ()
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                final_row = cursor.fetchone()

        assert final_row is not None
        assert final_row[0] == original_category_key
        assert final_row[1] == "TEST-A"
        assert final_row[2] is True
        assert final_row[3] == 1

    finally:
        cleanup_test_category(TEST_CATEGORY_ID)


def test_scd_type1_overwrites_attribute():
    cleanup_test_category(TYPE1_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.categories
                    (
                        category_id,
                        category_code,
                        category_name,
                        department,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE1_CATEGORY_ID,
                        "T1",
                        "Original Name",
                        "Stable Department",
                        TEST_EFFECTIVE_FROM,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        category_code,
                        category_name,
                        department,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TYPE1_CATEGORY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None

        original_category_key = original_row[0]

        assert original_row[1] == "T1"
        assert original_row[2] == "Original Name"
        assert original_row[3] == "Stable Department"
        assert original_row[4] is True

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.categories
                    SET
                        category_name = %s,
                        updated_at = %s
                    WHERE category_id = %s;
                    """,
                    (
                        "Updated Name",
                        datetime(
                            2035,
                            1,
                            2,
                            12,
                            0,
                            0,
                            tzinfo=timezone.utc,
                        ),
                        TYPE1_CATEGORY_ID,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        category_code,
                        category_name,
                        department,
                        is_current,
                        COUNT(*) OVER ()
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TYPE1_CATEGORY_ID,),
                )

                final_row = cursor.fetchone()

        assert final_row is not None

        assert final_row[0] == original_category_key
        assert final_row[1] == "T1"
        assert final_row[2] == "Updated Name"
        assert final_row[3] == "Stable Department"
        assert final_row[4] is True
        assert final_row[5] == 1

    finally:
        cleanup_test_category(TYPE1_CATEGORY_ID)


def test_scd_type2_creates_new_version():
    cleanup_test_category(TYPE2_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.categories
                    (
                        category_id,
                        category_code,
                        category_name,
                        department,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE2_CATEGORY_ID,
                        "T2-ORIGINAL",
                        "Type 2 Category",
                        "Department A",
                        TEST_EFFECTIVE_FROM,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT category_key
                    FROM dw.dim_category
                    WHERE category_id = %s
                      AND is_current = TRUE;
                    """,
                    (TYPE2_CATEGORY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None
        original_category_key = original_row[0]

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.categories
                    SET
                        category_code = %s,
                        department = %s,
                        updated_at = %s
                    WHERE category_id = %s;
                    """,
                    (
                        "T2-CHANGED",
                        "Department B",
                        TYPE2_CHANGE_TIMESTAMP,
                        TYPE2_CATEGORY_ID,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                    ORDER BY effective_from;
                    """,
                    (TYPE2_CATEGORY_ID,),
                )

                versions = cursor.fetchall()

        assert len(versions) == 2

        historical_row = versions[0]
        current_row = versions[1]

        assert historical_row[0] == original_category_key
        assert current_row[0] != original_category_key

        assert historical_row[1] == "T2-ORIGINAL"
        assert current_row[1] == "T2-ORIGINAL"

        assert historical_row[2] == "Type 2 Category"
        assert current_row[2] == "Type 2 Category"

        assert historical_row[3] == "Department A"
        assert current_row[3] == "Department B"

        assert historical_row[4] == TEST_EFFECTIVE_FROM
        assert historical_row[5] == TYPE2_CHANGE_TIMESTAMP
        assert historical_row[6] is False

        assert current_row[4] == TYPE2_CHANGE_TIMESTAMP
        assert current_row[5] is None
        assert current_row[6] is True

        assert (
            historical_row[5]
            == current_row[4]
        )

    finally:
        cleanup_test_category(TYPE2_CATEGORY_ID)

def test_scd_type2_is_idempotent():
    cleanup_test_category(TYPE2_IDEMPOTENT_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.categories
                    (
                        category_id,
                        category_code,
                        category_name,
                        department,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE2_IDEMPOTENT_CATEGORY_ID,
                        "T2-IDEMP",
                        "Idempotent Category",
                        "Department A",
                        TEST_EFFECTIVE_FROM,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.categories
                    SET
                        department = %s,
                        updated_at = %s
                    WHERE category_id = %s;
                    """,
                    (
                        "Department B",
                        TYPE2_CHANGE_TIMESTAMP,
                        TYPE2_IDEMPOTENT_CATEGORY_ID,
                    ),
                )

            connection.commit()

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        department,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s
                    ORDER BY effective_from;
                    """,
                    (TYPE2_IDEMPOTENT_CATEGORY_ID,),
                )

                versions_after_first_change = cursor.fetchall()

        assert len(versions_after_first_change) == 2

        first_version_keys = [
            row[0]
            for row in versions_after_first_change
        ]

        load_dim_category()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        category_key,
                        department,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s
                    ORDER BY effective_from;
                    """,
                    (TYPE2_IDEMPOTENT_CATEGORY_ID,),
                )

                versions_after_second_load = cursor.fetchall()

        assert len(versions_after_second_load) == 2

        second_version_keys = [
            row[0]
            for row in versions_after_second_load
        ]

        assert second_version_keys == first_version_keys

        historical_row = versions_after_second_load[0]
        current_row = versions_after_second_load[1]

        assert historical_row[1] == "Department A"
        assert historical_row[2] == TEST_EFFECTIVE_FROM
        assert historical_row[3] == TYPE2_CHANGE_TIMESTAMP
        assert historical_row[4] is False

        assert current_row[1] == "Department B"
        assert current_row[2] == TYPE2_CHANGE_TIMESTAMP
        assert current_row[3] is None
        assert current_row[4] is True

    finally:
        cleanup_test_category(TYPE2_IDEMPOTENT_CATEGORY_ID)

def test_initial_category_uses_explicit_effective_from():
    cleanup_test_category(INITIAL_BOUNDARY_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.categories
                    (
                        category_id,
                        category_code,
                        category_name,
                        department,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        INITIAL_BOUNDARY_CATEGORY_ID,
                        "INITIAL",
                        "Initial Boundary Category",
                        "Test Department",
                        TEST_EFFECTIVE_FROM,
                    ),
                )

            connection.commit()

        load_dim_category(
            initial_effective_from=INITIAL_BOUNDARY,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (INITIAL_BOUNDARY_CATEGORY_ID,),
                )

                row = cursor.fetchone()

        assert row is not None
        assert row[0] == INITIAL_BOUNDARY
        assert row[0] != TEST_EFFECTIVE_FROM
        assert row[1] is None
        assert row[2] is True

    finally:
        cleanup_test_category(INITIAL_BOUNDARY_CATEGORY_ID)

def test_scd_temporal_lookup_resolves_correct_version():
    cleanup_test_category(TEMPORAL_CATEGORY_ID)

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                    VALUES
                    (%s, %s, %s, %s, %s, %s, FALSE),
                    (%s, %s, %s, %s, %s, NULL, TRUE)
                    RETURNING
                        category_key,
                        effective_from;
                    """,
                    (
                        TEMPORAL_CATEGORY_ID,
                        "TEMP",
                        "Temporal Category",
                        "Department A",
                        TEMPORAL_START,
                        TEMPORAL_CHANGE,
                        TEMPORAL_CATEGORY_ID,
                        "TEMP",
                        "Temporal Category",
                        "Department B",
                        TEMPORAL_CHANGE,
                    ),
                )

                inserted_versions = cursor.fetchall()

            connection.commit()

        historical_key = inserted_versions[0][0]
        current_key = inserted_versions[1][0]

        historical_event = datetime(
            2035,
            1,
            15,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )

        current_event = datetime(
            2035,
            2,
            15,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT category_key
                    FROM dw.dim_category
                    WHERE category_id = %s
                      AND %s >= effective_from
                      AND (
                          effective_to IS NULL
                          OR %s < effective_to
                      );
                    """,
                    (
                        TEMPORAL_CATEGORY_ID,
                        historical_event,
                        historical_event,
                    ),
                )

                historical_match = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT category_key
                    FROM dw.dim_category
                    WHERE category_id = %s
                      AND %s >= effective_from
                      AND (
                          effective_to IS NULL
                          OR %s < effective_to
                      );
                    """,
                    (
                        TEMPORAL_CATEGORY_ID,
                        current_event,
                        current_event,
                    ),
                )

                current_match = cursor.fetchone()

        assert historical_match is not None
        assert current_match is not None

        assert historical_match[0] == historical_key
        assert current_match[0] == current_key

        assert historical_match[0] != current_match[0]

    finally:
        cleanup_test_category(TEMPORAL_CATEGORY_ID)
