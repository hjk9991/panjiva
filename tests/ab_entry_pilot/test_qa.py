import pandas as pd
import pytest

from scripts.ab_entry_pilot.qa import (
    GateFailure,
    check_conservation,
    check_finance_asof,
    check_keys,
    check_shares,
)


def test_duplicate_primary_key_fails():
    frame = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 1],
            "sector_id": ["auto_8703"] * 2,
            "origin_country": ["JP"] * 2,
            "year_quarter": ["2024Q1"] * 2,
        }
    )
    with pytest.raises(GateFailure, match="duplicate"):
        check_keys(frame)


def test_future_financial_period_fails():
    frame = pd.DataFrame(
        {
            "quarter_start": pd.to_datetime(["2024-01-01"]),
            "fin_period_end": pd.to_datetime(["2024-01-01"]),
            "fin_age_days": [0],
            "has_financials": [1],
        }
    )
    with pytest.raises(GateFailure, match="future"):
        check_finance_asof(frame)


def test_source_shares_must_sum_to_one():
    frame = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 1],
            "sector_id": ["auto_8703"] * 2,
            "year_quarter": ["2024Q1"] * 2,
            "sourcing_share_value": [0.4, 0.4],
            "value_usd": [40.0, 40.0],
        }
    )
    with pytest.raises(GateFailure, match="share"):
        check_shares(frame)


def test_source_and_firm_totals_must_be_conserved():
    source = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 1],
            "sector_id": ["auto_8703"] * 2,
            "year_quarter": ["2024Q1"] * 2,
            "value_usd": [60.0, 40.0],
            "weight_kg": [6.0, 4.0],
            "teu": [0.6, 0.4],
        }
    )
    firm = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1],
            "sector_id": ["auto_8703"],
            "year_quarter": ["2024Q1"],
            "value_usd": [98.0],
            "weight_kg": [10.0],
            "teu": [1.0],
        }
    )
    with pytest.raises(GateFailure, match="conservation"):
        check_conservation(source, firm)
