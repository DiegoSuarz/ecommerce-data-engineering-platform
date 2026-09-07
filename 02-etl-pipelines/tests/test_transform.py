import sys
from datetime import date
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))


from transform import (
    build_date_dimension_rows,
    calculate_scd2_hash,
    transform_country_rows,
)

def test_build_date_dimension_quarter_boundaries():
    rows = build_date_dimension_rows(
        [
            date(2021, 3, 31),
            date(2021, 4, 1),
        ]
    )

    assert rows[0][3] == 1
    assert rows[0][4] == "Q1"

    assert rows[1][3] == 2
    assert rows[1][4] == "Q2"


def test_build_date_dimension_sorts_dates():
    rows = build_date_dimension_rows(
        [
            date(2021, 12, 31),
            date(2019, 1, 1),
            date(2020, 6, 15),
        ]
    )

    assert rows[0][1] == date(2019, 1, 1)
    assert rows[1][1] == date(2020, 6, 15)
    assert rows[2][1] == date(2021, 12, 31)



def test_calculate_scd2_hash_is_deterministic():
    hash_1 = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    hash_2 = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    assert hash_1 == hash_2


def test_calculate_scd2_hash_changes_when_sales_region_changes():
    original_hash = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    changed_hash = calculate_scd2_hash(
        "LATAM_SOUTH",
        "STRATEGIC",
    )

    assert original_hash != changed_hash


def test_calculate_scd2_hash_changes_when_market_segment_changes():
    original_hash = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    changed_hash = calculate_scd2_hash(
        "LATAM",
        "GROWTH",
    )

    assert original_hash != changed_hash


def test_calculate_scd2_hash_ignores_outer_whitespace():
    clean_hash = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    padded_hash = calculate_scd2_hash(
        "  LATAM  ",
        "  STRATEGIC  ",
    )

    assert clean_hash == padded_hash


def test_calculate_scd2_hash_handles_none_deterministically():
    hash_1 = calculate_scd2_hash(
        None,
        "STRATEGIC",
    )

    hash_2 = calculate_scd2_hash(
        None,
        "STRATEGIC",
    )

    assert hash_1 == hash_2


def test_calculate_scd2_hash_returns_sha256_hexadecimal():
    row_hash = calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )

    assert len(row_hash) == 64

    int(row_hash, 16)


def test_transform_country_rows_adds_scd2_hash():
    rows = [
        (
            32,
            "PE",
            "Peru",
            "LATAM",
            "STRATEGIC",
            "2026-08-29 10:00:00",
        )
    ]

    transformed_rows = transform_country_rows(rows)

    assert len(transformed_rows) == 1

    transformed_row = transformed_rows[0]

    assert transformed_row[:6] == rows[0]

    assert transformed_row[6] == calculate_scd2_hash(
        "LATAM",
        "STRATEGIC",
    )
