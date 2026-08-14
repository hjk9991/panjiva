"""Hard validation gates and reports for the AB-entry pilot."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import END_QUARTER, OUT, SECTORS, START_QUARTER
from .extract import ensure_output_path
from .transforms import FIRM_KEYS, SOURCE_KEYS


class GateFailure(RuntimeError):
    """A hard data gate failure that prevents downstream extraction or release."""

    def __init__(self, message: str, *, review_required: bool = False):
        super().__init__(message)
        self.review_required = review_required


def check_keys(frame: pd.DataFrame) -> dict:
    keys = SOURCE_KEYS if "origin_country" in frame else FIRM_KEYS
    missing_columns = [column for column in keys if column not in frame]
    if missing_columns:
        raise GateFailure(f"G1 missing key columns: {missing_columns}")
    null_rows = int(frame[keys].isna().any(axis=1).sum())
    duplicate_rows = int(frame.duplicated(keys).sum())
    if null_rows:
        raise GateFailure(f"G1 null primary-key rows: {null_rows}")
    if duplicate_rows:
        raise GateFailure(f"G1 duplicate primary-key rows: {duplicate_rows}")
    valid_quarters = set(
        str(period)
        for period in pd.period_range(START_QUARTER, END_QUARTER, freq="Q")
    )
    invalid_quarters = sorted(set(frame["year_quarter"].astype(str)) - valid_quarters)
    if invalid_quarters:
        raise GateFailure(f"G1 quarter outside approved window: {invalid_quarters[:5]}")
    return {"gate": "G1", "rows": int(len(frame)), "duplicates": 0, "null_keys": 0}


def _tolerance(total: float) -> float:
    return max(1.0, abs(float(total)) * 1e-10)


def check_conservation(source: pd.DataFrame, firm: pd.DataFrame) -> dict:
    source_totals = source.groupby(FIRM_KEYS, dropna=False)[
        ["value_usd", "weight_kg", "teu"]
    ].sum()
    firm_totals = firm.set_index(FIRM_KEYS)[["value_usd", "weight_kg", "teu"]]
    joined = source_totals.join(firm_totals, how="outer", lsuffix="_source", rsuffix="_firm")
    failures: list[str] = []
    for measure in ("value_usd", "weight_kg", "teu"):
        left = joined[f"{measure}_source"].fillna(0)
        right = joined[f"{measure}_firm"].fillna(0)
        difference = (left - right).abs()
        tolerance = np.maximum(1.0, left.abs() * 1e-10)
        if difference.gt(tolerance).any():
            failures.append(measure)
    if failures:
        raise GateFailure(f"G2 conservation failure: {failures}")
    return {"gate": "G2", "firm_keys": int(len(joined)), "failed_measures": []}


def check_shares(source: pd.DataFrame, firm: pd.DataFrame | None = None) -> dict:
    positive = source.groupby(FIRM_KEYS, dropna=False)["value_usd"].transform("sum").gt(0)
    source_sums = source.loc[positive].groupby(FIRM_KEYS, dropna=False)[
        "sourcing_share_value"
    ].sum()
    source_failures = int((source_sums - 1.0).abs().gt(1e-10).sum())
    if source_failures:
        raise GateFailure(f"G3 sourcing share failure groups: {source_failures}")

    industry_failures = 0
    if firm is not None:
        positive_industry = firm.groupby(["sector_id", "year_quarter"], dropna=False)[
            "value_usd"
        ].transform("sum").gt(0)
        industry_sums = firm.loc[positive_industry].groupby(
            ["sector_id", "year_quarter"], dropna=False
        )["industry_import_share_value"].sum()
        industry_failures = int((industry_sums - 1.0).abs().gt(1e-10).sum())
        if industry_failures:
            raise GateFailure(f"G3 industry import share failure groups: {industry_failures}")
        if "output_market_share" in firm:
            raise GateFailure("G3 output_market_share must not be generated")
    return {
        "gate": "G3",
        "sourcing_groups": int(len(source_sums)),
        "industry_groups": int(0 if firm is None else len(industry_sums)),
    }


def check_ownership(source: pd.DataFrame) -> dict:
    overlaps = int(source.get("pit_overlap_shipment_count", pd.Series(dtype=float)).fillna(0).sum())
    if overlaps:
        raise GateFailure(f"G4 PIT ownership overlap shipments: {overlaps}")
    value_columns = [
        "value_up_pit_usd",
        "value_up_current_fallback_usd",
        "value_up_self_fallback_usd",
    ]
    if all(column in source for column in value_columns):
        attributed = source[value_columns].sum(axis=1)
        difference = (attributed - source["value_usd"].fillna(0)).abs()
        tolerance = np.maximum(1.0, source["value_usd"].fillna(0).abs() * 1e-10)
        failures = int(difference.gt(tolerance).sum())
        if failures:
            raise GateFailure(f"G4 ownership value attribution failure rows: {failures}")
    return {"gate": "G4", "pit_overlap_shipments": overlaps}


def check_finance_asof(frame: pd.DataFrame) -> dict:
    matched = frame[frame["has_financials"].eq(1)].copy()
    if matched.empty:
        return {"gate": "G5", "matched_rows": 0}
    period_end = pd.to_datetime(matched["fin_period_end"])
    quarter_start = pd.to_datetime(matched["quarter_start"])
    future = int(period_end.ge(quarter_start).sum())
    if future:
        raise GateFailure(f"G5 future financial periods: {future}")
    age = pd.to_numeric(matched["fin_age_days"], errors="coerce")
    bad_age = int((age.lt(1) | age.gt(730) | age.isna()).sum())
    if bad_age:
        raise GateFailure(f"G5 financial age outside 1..730 days: {bad_age}")
    return {
        "gate": "G5",
        "matched_rows": int(len(matched)),
        "max_age_days": int(age.max()),
    }


def check_panel_sufficiency(firm: pd.DataFrame, source: pd.DataFrame) -> dict:
    results = {}
    for sector_id in SECTORS:
        firms = firm.loc[
            firm["sector_id"].eq(sector_id)
            & firm.get("strategic_importer_main", pd.Series(1, index=firm.index)).eq(1),
            "ultimate_parent_companyid",
        ].nunique()
        links = source.loc[source["sector_id"].eq(sector_id), SOURCE_KEYS[:-1]].drop_duplicates().shape[0]
        quarters = firm.loc[firm["sector_id"].eq(sector_id), "year_quarter"].nunique()
        if firms < 20 or links < 100 or quarters < 44:
            raise GateFailure(
                f"G7 insufficient panel for {sector_id}: firms={firms}, links={links}, quarters={quarters}"
            )
        results[sector_id] = {"firms": int(firms), "links": int(links), "quarters": int(quarters)}

    totals = firm.groupby(["sector_id", "year_quarter"], dropna=False)["value_usd"].sum()
    ratios = totals.groupby(level=0).pct_change()
    collapses = ratios[ratios.le(-0.9)]
    if len(collapses):
        raise GateFailure(
            f"G7 sector-quarter value collapse requires review: {list(collapses.index)}",
            review_required=True,
        )
    return {"gate": "G7", "sectors": results, "collapses": 0}


def check_entity_roles(firm: pd.DataFrame, review: pd.DataFrame) -> dict:
    allowed = {
        "producer_brand_owner",
        "distributor_retailer",
        "forwarder_logistics",
        "unclear",
    }
    keys = ["sector_id", "ultimate_parent_companyid"]
    if review.empty or review.duplicated(keys).any() or review[keys].isna().any(axis=None):
        raise GateFailure("G6 entity review has missing or duplicate keys")
    invalid = sorted(set(review["entity_role"].dropna()) - allowed)
    if invalid:
        raise GateFailure(f"G6 invalid entity roles: {invalid}")
    incomplete = review["review_date"].isna() | review["evidence_note"].fillna("").str.strip().eq("")
    if incomplete.any():
        raise GateFailure(f"G6 incomplete reviewed entities: {int(incomplete.sum())}")
    forwarders = firm["entity_role"].eq("forwarder_logistics")
    invalid_forwarders = int(
        (forwarders & firm["strategic_importer_main"].eq(1)).sum()
    )
    if invalid_forwarders:
        raise GateFailure(
            f"G6 forwarder included in strategic importer sample: {invalid_forwarders}"
        )
    return {
        "gate": "G6",
        "review_rows": int(len(review)),
        "roles": review["entity_role"].value_counts().to_dict(),
        "forwarders_in_strategic_sample": 0,
    }


def check_license_boundary(data_center_root: Path | str) -> dict:
    root = Path(data_center_root).resolve(strict=False)
    leaks = []
    exact_licensed_names = {
        "entity_review_top50.csv",
        "firm_master.parquet",
        "firm_financials_annual.parquet",
        "panel_source_quarter_main.parquet",
        "panel_source_quarter_allocated.parquet",
        "panel_firm_quarter_main.parquet",
        "panel_firm_quarter_allocated.parquet",
    }
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        lowered_parts = [part.lower() for part in path.parts]
        pilot_metadata_path = any("panjiva_ab_entry" in part for part in lowered_parts)
        if path.name.lower() in exact_licensed_names:
            leaks.append(str(path))
        elif pilot_metadata_path and path.suffix.lower() in {".parquet", ".dta"}:
            leaks.append(str(path))
    if leaks:
        raise GateFailure(f"G8 license boundary leak: {leaks[:10]}")
    return {"gate": "G8", "root": str(root), "licensed_row_files": 0}


def compare_week_totals(new: dict, reference: dict) -> dict:
    failures = {}
    for measure in ("shipment_count", "value_usd", "weight_kg", "teu"):
        difference = abs(float(new[measure]) - float(reference[measure]))
        allowed = 0.0 if measure == "shipment_count" else _tolerance(reference[measure])
        if difference > allowed:
            failures[measure] = {
                "new": float(new[measure]),
                "reference": float(reference[measure]),
                "difference": difference,
                "allowed": allowed,
            }
    if failures:
        raise GateFailure(f"G0 one-week reconciliation failure: {failures}")
    return {"gate": "G0", "status": "pass", "new": new, "reference": reference}


def write_full_report(results: dict, path: Path | str) -> None:
    target = ensure_output_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# AB-entry pilot full QA", ""]
    for name, result in results.items():
        lines.extend([f"## {name}", "", "```json", json.dumps(result, ensure_ascii=False, indent=2), "```", ""])
    target.write_text("\n".join(lines), encoding="utf-8")


def _sanitize_result(value):
    forbidden = ("company", "ultimate_parent", "companyid", "companyname", "top_")
    if isinstance(value, dict):
        return {
            key: _sanitize_result(item)
            for key, item in value.items()
            if not any(token in str(key).lower() for token in forbidden)
        }
    if isinstance(value, list):
        if all(isinstance(item, (str, int, float, bool, type(None))) for item in value):
            return value[:20]
        return []
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def write_sanitized_report(results: dict, path: Path | str) -> None:
    """Write aggregate QA only, dropping company identity-bearing fields."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    sanitized = _sanitize_result(results)
    text = "\n".join(
        [
            "# Panjiva–CapIQ AB-entry pilot: sanitized QA summary",
            "",
            "This report contains aggregate gate results only; no licensed rows or company identities.",
            "",
            "```json",
            json.dumps(sanitized, ensure_ascii=False, indent=2),
            "```",
            "",
        ]
    )
    target.write_text(text, encoding="utf-8")
