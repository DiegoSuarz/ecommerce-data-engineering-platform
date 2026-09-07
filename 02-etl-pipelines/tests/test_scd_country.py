import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from db import get_postgres_connection
from load import load_dim_country
from transform import calculate_scd2_hash


pytestmark = pytest.mark.integration


TYPE0_COUNTRY_ID = 920001
TYPE1_COUNTRY_ID = 920002
TYPE2_COUNTRY_ID = 920003
TYPE2_IDEMPOTENT_COUNTRY_ID = 920004
TEMPORAL_COUNTRY_ID = 920005
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

TEST_EFFECTIVE_FROM = datetime(
    2035,
    1,
    1,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)

TYPE0_CHANGE_TIMESTAMP = datetime(
    2035,
    1,
    2,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)

TYPE1_CHANGE_TIMESTAMP = datetime(
    2035,
    1,
    2,
    13,
    0,
    0,
    tzinfo=timezone.utc,
)

TYPE2_CHANGE_TIMESTAMP = datetime(
    2035,
    1,
    2,
    14,
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

def cleanup_test_country(country_id):
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM dw.dim_country
                WHERE country_id = %s;
                """,
                (country_id,),
            )

            cursor.execute(
                """
                DELETE FROM staging.countries
                WHERE country_id = %s;
                """,
                (country_id,),
            )

        connection.commit()


def test_scd_type0_preserves_country_code():
    cleanup_test_country(TYPE0_COUNTRY_ID)

    original_hash = calculate_scd2_hash(
        "TEST_REGION",
        "STANDARD",
    )

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.countries
                    (
                        country_id,
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        updated_at,
                        row_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE0_COUNTRY_ID,
                        "X1",
                        "Type 0 Test Country",
                        "TEST_REGION",
                        "STANDARD",
                        TEST_EFFECTIVE_FROM,
                        original_hash,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        country_key,
                        country_code,
                        row_hash,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TYPE0_COUNTRY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None

        original_country_key = original_row[0]

        assert original_row[1] == "X1"
        assert original_row[2] == original_hash
        assert original_row[3] is True

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.countries
                    SET
                        country_code = %s,
                        updated_at = %s
                    WHERE country_id = %s;
                    """,
                    (
                        "X2",
                        TYPE0_CHANGE_TIMESTAMP,
                        TYPE0_COUNTRY_ID,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        country_key,
                        country_code,
                        row_hash,
                        is_current,
                        COUNT(*) OVER ()
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TYPE0_COUNTRY_ID,),
                )

                final_row = cursor.fetchone()

        assert final_row is not None
        assert final_row[0] == original_country_key
        assert final_row[1] == "X1"
        assert final_row[2] == original_hash
        assert final_row[3] is True
        assert final_row[4] == 1

    finally:
        cleanup_test_country(TYPE0_COUNTRY_ID)

def test_initial_country_uses_explicit_effective_from():
    cleanup_test_country(INITIAL_BOUNDARY_COUNTRY_ID)

    row_hash = calculate_scd2_hash(
        "TEST_REGION",
        "STANDARD",
    )

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.countries
                    (
                        country_id,
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        updated_at,
                        row_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        INITIAL_BOUNDARY_COUNTRY_ID,
                        "IB",
                        "Initial Boundary Country",
                        "TEST_REGION",
                        "STANDARD",
                        TEST_EFFECTIVE_FROM,
                        row_hash,
                    ),
                )

            connection.commit()

        load_dim_country(
            initial_effective_from=INITIAL_BOUNDARY,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        row_hash,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (INITIAL_BOUNDARY_COUNTRY_ID,),
                )

                row = cursor.fetchone()

        assert row is not None
        assert row[0] == row_hash
        assert row[1] == INITIAL_BOUNDARY
        assert row[1] != TEST_EFFECTIVE_FROM
        assert row[2] is None
        assert row[3] is True

    finally:
        cleanup_test_country(INITIAL_BOUNDARY_COUNTRY_ID)

def test_scd_type1_overwrites_country_name():
    cleanup_test_country(TYPE1_COUNTRY_ID)

    original_hash = calculate_scd2_hash(
        "TEST_REGION",
        "GROWTH",
    )

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.countries
                    (
                        country_id,
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        updated_at,
                        row_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE1_COUNTRY_ID,
                        "Y1",
                        "Original Country Name",
                        "TEST_REGION",
                        "GROWTH",
                        TEST_EFFECTIVE_FROM,
                        original_hash,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TYPE1_COUNTRY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None

        original_country_key = original_row[0]

        assert original_row[1] == "Y1"
        assert original_row[2] == "Original Country Name"
        assert original_row[3] == "TEST_REGION"
        assert original_row[4] == "GROWTH"
        assert original_row[5] == original_hash
        assert original_row[6] == TEST_EFFECTIVE_FROM
        assert original_row[7] is True

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.countries
                    SET
                        country_name = %s,
                        updated_at = %s
                    WHERE country_id = %s;
                    """,
                    (
                        "Updated Country Name",
                        TYPE1_CHANGE_TIMESTAMP,
                        TYPE1_COUNTRY_ID,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                        is_current,
                        COUNT(*) OVER ()
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TYPE1_COUNTRY_ID,),
                )

                final_row = cursor.fetchone()

        assert final_row is not None

        assert final_row[0] == original_country_key
        assert final_row[1] == "Y1"
        assert final_row[2] == "Updated Country Name"
        assert final_row[3] == "TEST_REGION"
        assert final_row[4] == "GROWTH"
        assert final_row[5] == original_hash
        assert final_row[6] == TEST_EFFECTIVE_FROM
        assert final_row[7] is True
        assert final_row[8] == 1

    finally:
        cleanup_test_country(TYPE1_COUNTRY_ID)

def test_scd_type2_creates_new_version():
    cleanup_test_country(TYPE2_COUNTRY_ID)

    original_hash = calculate_scd2_hash(
        "REGION_A",
        "STRATEGIC",
    )

    changed_hash = calculate_scd2_hash(
        "REGION_B",
        "STRATEGIC",
    )

    assert original_hash != changed_hash

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.countries
                    (
                        country_id,
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        updated_at,
                        row_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE2_COUNTRY_ID,
                        "Z1",
                        "Type 2 Test Country",
                        "REGION_A",
                        "STRATEGIC",
                        TEST_EFFECTIVE_FROM,
                        original_hash,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT country_key
                    FROM dw.dim_country
                    WHERE country_id = %s
                      AND is_current = TRUE;
                    """,
                    (TYPE2_COUNTRY_ID,),
                )

                original_row = cursor.fetchone()

        assert original_row is not None
        original_country_key = original_row[0]

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.countries
                    SET
                        country_code = %s,
                        sales_region = %s,
                        updated_at = %s,
                        row_hash = %s
                    WHERE country_id = %s;
                    """,
                    (
                        "Z2",
                        "REGION_B",
                        TYPE2_CHANGE_TIMESTAMP,
                        changed_hash,
                        TYPE2_COUNTRY_ID,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                    ORDER BY effective_from;
                    """,
                    (TYPE2_COUNTRY_ID,),
                )

                versions = cursor.fetchall()

        assert len(versions) == 2

        historical_row = versions[0]
        current_row = versions[1]

        assert historical_row[0] == original_country_key
        assert current_row[0] != original_country_key

        # Type 0 must be preserved in both versions.
        assert historical_row[1] == "Z1"
        assert current_row[1] == "Z1"

        # Type 1 did not change.
        assert historical_row[2] == "Type 2 Test Country"
        assert current_row[2] == "Type 2 Test Country"

        # Type 2 changed.
        assert historical_row[3] == "REGION_A"
        assert current_row[3] == "REGION_B"

        assert historical_row[4] == "STRATEGIC"
        assert current_row[4] == "STRATEGIC"

        assert historical_row[5] == original_hash
        assert current_row[5] == changed_hash

        # Temporal history must be continuous and half-open.
        assert historical_row[6] == TEST_EFFECTIVE_FROM
        assert historical_row[7] == TYPE2_CHANGE_TIMESTAMP
        assert historical_row[8] is False

        assert current_row[6] == TYPE2_CHANGE_TIMESTAMP
        assert current_row[7] is None
        assert current_row[8] is True

        assert historical_row[7] == current_row[6]

    finally:
        cleanup_test_country(TYPE2_COUNTRY_ID)

def test_scd_type2_is_idempotent():
    cleanup_test_country(TYPE2_IDEMPOTENT_COUNTRY_ID)

    original_hash = calculate_scd2_hash(
        "REGION_A",
        "STANDARD",
    )

    changed_hash = calculate_scd2_hash(
        "REGION_B",
        "STANDARD",
    )

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.countries
                    (
                        country_id,
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        updated_at,
                        row_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        TYPE2_IDEMPOTENT_COUNTRY_ID,
                        "W1",
                        "Idempotent Country",
                        "REGION_A",
                        "STANDARD",
                        TEST_EFFECTIVE_FROM,
                        original_hash,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.countries
                    SET
                        sales_region = %s,
                        updated_at = %s,
                        row_hash = %s
                    WHERE country_id = %s;
                    """,
                    (
                        "REGION_B",
                        TYPE2_CHANGE_TIMESTAMP,
                        changed_hash,
                        TYPE2_IDEMPOTENT_COUNTRY_ID,
                    ),
                )

            connection.commit()

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        country_key,
                        sales_region,
                        row_hash,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s
                    ORDER BY effective_from;
                    """,
                    (TYPE2_IDEMPOTENT_COUNTRY_ID,),
                )

                versions_after_first_change = cursor.fetchall()

        assert len(versions_after_first_change) == 2

        first_version_keys = [
            row[0]
            for row in versions_after_first_change
        ]

        load_dim_country()

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        country_key,
                        sales_region,
                        row_hash,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s
                    ORDER BY effective_from;
                    """,
                    (TYPE2_IDEMPOTENT_COUNTRY_ID,),
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

        assert historical_row[1] == "REGION_A"
        assert historical_row[2] == original_hash
        assert historical_row[3] == TEST_EFFECTIVE_FROM
        assert historical_row[4] == TYPE2_CHANGE_TIMESTAMP
        assert historical_row[5] is False

        assert current_row[1] == "REGION_B"
        assert current_row[2] == changed_hash
        assert current_row[3] == TYPE2_CHANGE_TIMESTAMP
        assert current_row[4] is None
        assert current_row[5] is True

    finally:
        cleanup_test_country(TYPE2_IDEMPOTENT_COUNTRY_ID)

def test_scd_temporal_lookup_resolves_correct_version():
    cleanup_test_country(TEMPORAL_COUNTRY_ID)

    historical_hash = calculate_scd2_hash(
        "REGION_A",
        "GROWTH",
    )

    current_hash = calculate_scd2_hash(
        "REGION_B",
        "GROWTH",
    )

    try:
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
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
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, FALSE),
                    (%s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
                    RETURNING
                        country_key,
                        effective_from;
                    """,
                    (
                        TEMPORAL_COUNTRY_ID,
                        "V1",
                        "Temporal Country",
                        "REGION_A",
                        "GROWTH",
                        historical_hash,
                        TEMPORAL_START,
                        TEMPORAL_CHANGE,

                        TEMPORAL_COUNTRY_ID,
                        "V1",
                        "Temporal Country",
                        "REGION_B",
                        "GROWTH",
                        current_hash,
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
                    SELECT country_key
                    FROM dw.dim_country
                    WHERE country_id = %s
                      AND %s >= effective_from
                      AND (
                          effective_to IS NULL
                          OR %s < effective_to
                      );
                    """,
                    (
                        TEMPORAL_COUNTRY_ID,
                        historical_event,
                        historical_event,
                    ),
                )

                historical_match = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT country_key
                    FROM dw.dim_country
                    WHERE country_id = %s
                      AND %s >= effective_from
                      AND (
                          effective_to IS NULL
                          OR %s < effective_to
                      );
                    """,
                    (
                        TEMPORAL_COUNTRY_ID,
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
        cleanup_test_country(TEMPORAL_COUNTRY_ID)
