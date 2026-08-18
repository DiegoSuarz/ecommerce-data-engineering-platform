import sys
from datetime import date
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from transform import build_date_dimension_rows


def test_build_date_dimension_row():
    rows = build_date_dimension_rows(
        [
            date(2021, 12, 31),
        ]
    )

    assert len(rows) == 1

    row = rows[0]

    assert row[0] == 20211231
    assert row[1] == date(2021, 12, 31)
    assert row[2] == 2021
    assert row[3] == 4
    assert row[4] == "Q4"
    assert row[5] == 12
    assert row[6] == "December"
    assert row[7] == 31
    assert row[8] == 5
    assert row[9] == "Friday"


def test_build_date_dimension_removes_duplicates():
    rows = build_date_dimension_rows(
        [
            date(2019, 1, 1),
            date(2019, 1, 1),
            date(2019, 1, 2),
        ]
    )

    assert len(rows) == 2


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
