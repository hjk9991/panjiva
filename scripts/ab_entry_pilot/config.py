"""Immutable scope and schema configuration for the AB-entry pilot."""

from pathlib import Path

import pandas as pd


VERSION = "1.0.0"
START_QUARTER = "2015Q1"
END_QUARTER = "2025Q4"
OUTPUT_ROOT = r"C:\panjiva\data\staging\ab_entry_pilot_v1"
OUT = Path(OUTPUT_ROOT)

SECTORS = {
    "auto_8703": {"kind": "prefix", "hs6": "8703"},
    "refrigerator_841810": {"kind": "exact", "hs6": "841810"},
}

FIN_ITEMS = {
    28: "revenue",
    34: "cogs",
    1007: "assets",
    1004: "ppent",
    2021: "capex",
    4371: "employees",
}

WEEK_START = "2024-03-01"
WEEK_END = "2024-03-08"
MAX_FIN_AGE_DAYS = 730


def iter_quarters():
    """Yield the 44 quarterly labels in the approved pilot window."""

    return (
        str(quarter)
        for quarter in pd.period_range(START_QUARTER, END_QUARTER, freq="Q")
    )
