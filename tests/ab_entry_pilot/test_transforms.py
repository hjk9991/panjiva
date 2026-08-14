import pandas as pd
import pytest

from scripts.ab_entry_pilot.transforms import (
    add_activity,
    add_transitions,
    attach_financials_asof,
    build_firm_panel,
)


def source_fixture():
    return pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 1, 1, 1],
            "sector_id": ["auto_8703"] * 4,
            "origin_country": ["JP", "MX", "JP", "MX"],
            "year_quarter": ["2024Q1", "2024Q1", "2024Q2", "2024Q2"],
            "shipment_count": [4, 1, 5, 0],
            "shipment_equivalent": [4.0, 1.0, 5.0, 0.0],
            "value_usd": [2_000_000.0, 50_000.0, 3_000_000.0, 0.0],
            "weight_kg": [20.0, 10.0, 30.0, 0.0],
            "teu": [2.0, 1.0, 3.0, 0.0],
        }
    )


def test_activity_thresholds_are_flags_not_filters():
    out = add_activity(source_fixture())
    assert len(out) == 4
    assert out["link_active_raw"].tolist() == [1, 1, 1, 0]
    assert out["link_active_100k"].tolist() == [1, 0, 1, 0]
    assert out["link_active_core"].tolist() == [1, 0, 1, 0]


def test_sourcing_and_industry_import_shares_are_separate():
    firm, source = build_firm_panel(add_activity(source_fixture()))
    q1 = source[source.year_quarter.eq("2024Q1")]
    assert abs(q1.sourcing_share_value.sum() - 1.0) < 1e-12
    assert firm["industry_import_share_value"].eq(1.0).all()
    assert "output_market_share" not in firm
    assert firm.loc[firm.year_quarter.eq("2024Q1"), "n_origin_links_raw"].iat[0] == 2


def test_transition_edges_are_censored_and_middle_exit_is_observed():
    out = add_transitions(
        add_activity(source_fixture()),
        definition="raw",
        start_quarter="2024Q1",
        end_quarter="2024Q2",
    )
    assert out.loc[out.year_quarter.eq("2024Q1"), "entry_raw"].isna().all()
    assert out.loc[out.year_quarter.eq("2024Q2"), "exit_next_raw"].isna().all()
    mx_q1 = out[
        out.origin_country.eq("MX") & out.year_quarter.eq("2024Q1")
    ].iloc[0]
    assert mx_q1["exit_next_raw"] == 1


@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_financial_asof_is_strict_and_limited_to_730_days():
    panel = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 2],
            "quarter_start": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        }
    )
    financials = pd.DataFrame(
        {
            "companyid": [1, 1, 2],
            "fin_period_end": pd.to_datetime(
                ["2023-12-31", "2024-01-01", "2021-12-31"]
            ),
            "revenue_usd": [10.0, 99.0, 20.0],
        }
    )
    out = attach_financials_asof(panel, financials)
    assert out.loc[out.ultimate_parent_companyid.eq(1), "revenue_usd"].iat[0] == 10.0
    assert out.loc[out.ultimate_parent_companyid.eq(1), "fin_age_days"].iat[0] == 1
    assert out.loc[out.ultimate_parent_companyid.eq(2), "has_financials"].iat[0] == 0


def test_firm_panel_omits_quarters_with_no_raw_import_link():
    one_link = source_fixture().iloc[[0]].copy()
    balanced = add_transitions(
        add_activity(one_link),
        definition="raw",
        start_quarter="2024Q1",
        end_quarter="2024Q2",
    )
    firm, _ = build_firm_panel(balanced)
    assert firm["year_quarter"].tolist() == ["2024Q1"]
