"""Pure panel transformations for the AB-entry pilot."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import END_QUARTER, MAX_FIN_AGE_DAYS, START_QUARTER


SOURCE_KEYS = [
    "ultimate_parent_companyid",
    "sector_id",
    "origin_country",
    "year_quarter",
]
FIRM_KEYS = ["ultimate_parent_companyid", "sector_id", "year_quarter"]
LINK_KEYS = ["ultimate_parent_companyid", "sector_id", "origin_country"]
ACTIVITY_DEFINITIONS = ("raw", "100k", "core")


def add_activity(df: pd.DataFrame) -> pd.DataFrame:
    """Add three non-destructive link-activity definitions."""

    out = df.copy()
    out["link_active_raw"] = out["shipment_count"].gt(0).astype("int8")
    out["link_active_100k"] = out["value_usd"].ge(100_000).astype("int8")
    out["link_active_core"] = (
        out["value_usd"].ge(1_000_000) & out["shipment_count"].ge(3)
    ).astype("int8")
    return out


def _safe_share(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.where(denominator.gt(0)))


def build_firm_panel(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add source shares and aggregate a separate firm-sector-quarter panel."""

    source = df.copy()
    group = source.groupby(FIRM_KEYS, dropna=False, sort=False)
    value_total = group["value_usd"].transform("sum")
    weight_total = group["weight_kg"].transform("sum")
    source["sourcing_share_value"] = _safe_share(source["value_usd"], value_total)
    source["sourcing_share_weight"] = _safe_share(source["weight_kg"], weight_total)

    aggregate = {
        "value_usd": "sum",
        "weight_kg": "sum",
        "teu": "sum",
        "shipment_count": "sum",
    }
    if "shipment_equivalent" in source:
        aggregate["shipment_equivalent"] = "sum"
    firm = group.agg(aggregate).reset_index()

    portfolio = group.agg(
        origin_hhi_value=(
            "sourcing_share_value",
            lambda values: values.dropna().pow(2).sum()
            if values.notna().any()
            else np.nan,
        ),
        largest_origin_share=("sourcing_share_value", "max"),
    ).reset_index()
    firm = firm.merge(portfolio, on=FIRM_KEYS, how="left", validate="one_to_one")

    counts = group[
        [f"link_active_{definition}" for definition in ACTIVITY_DEFINITIONS]
    ].sum().reset_index()
    counts = counts.rename(
        columns={
            f"link_active_{definition}": f"n_origin_links_{definition}"
            for definition in ACTIVITY_DEFINITIONS
        }
    )
    firm = firm.merge(counts, on=FIRM_KEYS, how="left", validate="one_to_one")
    for definition in ACTIVITY_DEFINITIONS:
        firm[f"multi_origin_{definition}"] = (
            firm[f"n_origin_links_{definition}"].ge(2).astype("int8")
        )

    industry_total = firm.groupby(
        ["sector_id", "year_quarter"], dropna=False
    )["value_usd"].transform("sum")
    firm["industry_import_share_value"] = _safe_share(
        firm["value_usd"], industry_total
    )
    firm["quarter_start"] = pd.PeriodIndex(
        firm["year_quarter"].astype(str), freq="Q"
    ).start_time
    source["quarter_start"] = pd.PeriodIndex(
        source["year_quarter"].astype(str), freq="Q"
    ).start_time
    return firm, source


def add_transitions(
    df: pd.DataFrame,
    definition: str,
    start_quarter: str = START_QUARTER,
    end_quarter: str = END_QUARTER,
) -> pd.DataFrame:
    """Complete the link-quarter grid and add censored entry/exit indicators."""

    if definition not in ACTIVITY_DEFINITIONS:
        raise ValueError(f"unknown activity definition: {definition}")
    if df.duplicated(SOURCE_KEYS).any():
        raise ValueError("duplicate source-panel keys")

    periods = [
        str(period)
        for period in pd.period_range(start_quarter, end_quarter, freq="Q")
    ]
    links = df[LINK_KEYS].drop_duplicates()
    quarter_frame = pd.DataFrame({"year_quarter": periods})
    grid = links.merge(quarter_frame, how="cross")
    out = grid.merge(df, on=SOURCE_KEYS, how="left", validate="one_to_one")

    activity_columns = [
        f"link_active_{name}" for name in ACTIVITY_DEFINITIONS if f"link_active_{name}" in out
    ]
    metric_columns = [
        column
        for column in (
            "shipment_count",
            "shipment_equivalent",
            "value_usd",
            "weight_kg",
            "teu",
        )
        if column in out
    ]
    out[activity_columns + metric_columns] = out[
        activity_columns + metric_columns
    ].fillna(0)
    for column in activity_columns:
        out[column] = out[column].astype("int8")

    out["_period"] = pd.PeriodIndex(out["year_quarter"], freq="Q")
    out = out.sort_values(LINK_KEYS + ["_period"]).reset_index(drop=True)
    active_column = f"link_active_{definition}"
    grouped = out.groupby(LINK_KEYS, dropna=False, sort=False)[active_column]
    previous = grouped.shift(1).fillna(0)
    next_one = grouped.shift(-1).fillna(0)
    next_two = grouped.shift(-2).fillna(0)

    out[f"entry_{definition}"] = (
        out[active_column].eq(1) & previous.eq(0)
    ).astype("Int8")
    out[f"exit_next_{definition}"] = (
        out[active_column].eq(1) & next_one.eq(0)
    ).astype("Int8")
    out[f"exit_2q_{definition}"] = (
        out[active_column].eq(1) & next_one.eq(0) & next_two.eq(0)
    ).astype("Int8")

    first = pd.Period(start_quarter, freq="Q")
    last = pd.Period(end_quarter, freq="Q")
    out.loc[out["_period"].eq(first), f"entry_{definition}"] = pd.NA
    out.loc[out["_period"].eq(last), f"exit_next_{definition}"] = pd.NA
    out.loc[out["_period"].ge(last - 1), f"exit_2q_{definition}"] = pd.NA
    return out.drop(columns="_period")


def attach_financials_asof(
    panel: pd.DataFrame,
    financials: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the latest strictly prior annual financial within 730 days."""

    left = panel.copy().reset_index(drop=False).rename(columns={"index": "_row_order"})
    left["_company_key"] = pd.to_numeric(
        left["ultimate_parent_companyid"], errors="raise"
    ).astype("int64")
    left["quarter_start"] = pd.to_datetime(left["quarter_start"])

    right = financials.copy()
    right["_company_key"] = pd.to_numeric(right["companyid"], errors="raise").astype(
        "int64"
    )
    right["fin_period_end"] = pd.to_datetime(right["fin_period_end"])
    right = (
        right.sort_values(["fin_period_end", "_company_key"])
        .drop_duplicates(["_company_key", "fin_period_end"], keep="last")
        .drop(columns="companyid")
    )

    merged = pd.merge_asof(
        left.sort_values(["quarter_start", "_company_key"]),
        right.sort_values(["fin_period_end", "_company_key"]),
        left_on="quarter_start",
        right_on="fin_period_end",
        by="_company_key",
        direction="backward",
        tolerance=pd.to_timedelta(MAX_FIN_AGE_DAYS, unit="D"),
        allow_exact_matches=False,
    )
    merged["fin_age_days"] = (
        merged["quarter_start"] - merged["fin_period_end"]
    ).dt.days.astype("Int64")
    merged["has_financials"] = merged["fin_period_end"].notna().astype("int8")
    return (
        merged.sort_values("_row_order")
        .drop(columns=["_row_order", "_company_key"])
        .reset_index(drop=True)
    )
