"""Command-line entry points for extraction, build, and QA."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from .config import END_QUARTER, OUT, SECTORS, START_QUARTER, WEEK_END, WEEK_START
from .extract import (
    atomic_parquet,
    connect,
    ensure_output_path,
    extract_parent_metadata,
    extract_trade_chunks,
    run_chunk,
    sha256_file,
    update_manifest,
)
from .qa import (
    check_conservation,
    check_finance_asof,
    check_keys,
    check_ownership,
    check_panel_sufficiency,
    check_shares,
    compare_week_totals,
    write_full_report,
)
from .sql import build_trade_sql
from .transforms import (
    add_activity,
    add_transitions,
    attach_financials_asof,
    build_firm_panel,
    make_entity_review_queue,
)


REFERENCE_L2 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q\L2")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-week")
    validate.add_argument("--start", default=WEEK_START)
    validate.add_argument("--end", default=WEEK_END)

    full = commands.add_parser("extract-full")
    full.add_argument("--start-quarter", default=START_QUARTER)
    full.add_argument("--end-quarter", default=END_QUARTER)

    commands.add_parser("build")
    commands.add_parser("qa")
    return parser


def _sector_mask(codes: pd.Series, sector_id: str) -> pd.Series:
    text = codes.astype("string")
    if sector_id == "auto_8703":
        return text.str.startswith("8703", na=False)
    if sector_id == "refrigerator_841810":
        return text.eq("841810").fillna(False)
    raise ValueError(f"unapproved sector_id: {sector_id}")


def reference_week_totals(
    l2_root: Path | str,
    date_start: str,
    date_end: str,
    sector_id: str,
    sample: str,
) -> dict:
    """Recalculate G0 totals from the existing validated 2024H1 facts."""

    root = Path(l2_root)
    columns = [
        "record_id",
        "arrival_date",
        "value_usd",
        "weight_kg",
        "teu",
        "n_hs6",
        "hs6_main",
        "consignee_up",
        "consignee_ciqid",
    ]
    shipments = pd.read_parquet(root / "fact_shipment.parquet", columns=columns)
    arrivals = pd.to_datetime(shipments["arrival_date"])
    shipments = shipments[
        arrivals.ge(pd.Timestamp(date_start)) & arrivals.lt(pd.Timestamp(date_end))
    ].copy()
    if sample == "main":
        selected = shipments[
            shipments["n_hs6"].eq(1) & _sector_mask(shipments["hs6_main"], sector_id)
        ]
        measures = selected.rename(
            columns={
                "value_usd": "_value",
                "weight_kg": "_weight",
                "teu": "_teu",
            }
        )
    elif sample == "allocated":
        hs = pd.read_parquet(
            root / "fact_shipment_hs.parquet",
            columns=["record_id", "hs6", "value_alloc", "weight_alloc", "teu_alloc"],
        )
        hs = hs[_sector_mask(hs["hs6"], sector_id)]
        measures = shipments[["record_id"]].merge(hs, on="record_id", how="inner")
        measures = measures.rename(
            columns={
                "value_alloc": "_value",
                "weight_alloc": "_weight",
                "teu_alloc": "_teu",
            }
        )
    else:
        raise ValueError(f"unapproved sample: {sample}")

    return {
        "shipment_count": int(measures["record_id"].nunique()),
        "value_usd": float(measures["_value"].sum()),
        "weight_kg": float(measures["_weight"].sum()),
        "teu": float(measures["_teu"].sum()),
    }


def _new_totals(frame: pd.DataFrame) -> dict:
    return {
        "shipment_count": int(frame["shipment_count"].sum()),
        "value_usd": float(frame["value_usd"].sum()),
        "weight_kg": float(frame["weight_kg"].sum()),
        "teu": float(frame["teu"].sum()),
    }


def _sql_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def validate_week(date_start: str = WEEK_START, date_end: str = WEEK_END) -> dict:
    """Extract and reconcile both sectors and samples for the approved week."""

    manifest_path = ensure_output_path(OUT / "extract_manifest.json")
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.exists()
        else {"chunks": {}}
    )
    results = {}
    connection = None
    try:
        for sample in ("main", "allocated"):
            for sector_id in SECTORS:
                sql = build_trade_sql(sector_id, date_start, date_end, sample)
                query_hash = _sql_hash(sql)
                key = f"validation/{sample}/{sector_id}/{date_start}_{date_end}"
                target = ensure_output_path(
                    OUT / "_validation" / sample / f"{sector_id}.parquet"
                )
                entry = manifest.get("chunks", {}).get(key)
                current = bool(
                    entry
                    and entry.get("g0_status") == "pass"
                    and entry.get("query_sha256") == query_hash
                    and target.exists()
                    and entry.get("file_sha256") == sha256_file(target)
                )
                if current:
                    frame = pd.read_parquet(target)
                else:
                    if connection is None:
                        connection = connect()
                    frame = run_chunk(connection.cursor(), sql, target)
                new = _new_totals(frame)
                reference = reference_week_totals(
                    REFERENCE_L2, date_start, date_end, sector_id, sample
                )
                result = compare_week_totals(new, reference)
                results[f"{sample}/{sector_id}"] = result
                manifest = update_manifest(
                    manifest_path,
                    key,
                    {
                        "status": "complete",
                        "g0_status": "pass",
                        "rows": int(len(frame)),
                        "query_sha256": query_hash,
                        "file_sha256": sha256_file(target),
                        "totals": new,
                    },
                )
    finally:
        if connection is not None:
            connection.close()
    write_full_report({"G0": results}, OUT / "qa_full.md")
    return results


def extract_full(start_quarter: str, end_quarter: str) -> dict:
    validate_week()
    quarters = [
        str(period)
        for period in pd.period_range(start_quarter, end_quarter, freq="Q")
    ]
    connection = connect()
    try:
        trade_manifest = extract_trade_chunks(connection.cursor(), quarters)
        metadata = extract_parent_metadata(connection)
        return {"trade_manifest": trade_manifest, "metadata": metadata}
    finally:
        connection.close()


def _read_chunks(sample: str) -> pd.DataFrame:
    files = sorted((OUT / "_chunks" / sample).glob("*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"no completed chunks for sample={sample}")
    return pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)


def build_panels() -> dict:
    company_master = pd.read_parquet(OUT / "firm_master.parquet")
    financials = pd.read_parquet(OUT / "firm_financials_annual.parquet")
    company_master["companyid"] = pd.to_numeric(
        company_master["companyid"], errors="raise"
    ).astype("int64")
    built = {}
    for sample in ("main", "allocated"):
        source = _read_chunks(sample)
        source = source[source["ultimate_parent_companyid"].notna()].copy()
        source["ultimate_parent_companyid"] = pd.to_numeric(
            source["ultimate_parent_companyid"], errors="raise"
        ).astype("int64")
        source = add_activity(source)
        for definition in ("raw", "100k", "core"):
            source = add_transitions(source, definition)
        firm, source = build_firm_panel(source)
        firm = attach_financials_asof(firm, financials)
        firm = firm.merge(
            company_master.rename(
                columns={"companyid": "ultimate_parent_companyid"}
            ),
            on="ultimate_parent_companyid",
            how="left",
            validate="many_to_one",
        )
        built[sample] = (firm, source)

    review_target = ensure_output_path(OUT / "entity_review_top50.csv")
    review = make_entity_review_queue(built["main"][0], company_master, top_n=50)
    if review_target.exists():
        existing = pd.read_csv(review_target)
        review_columns = [
            "sector_id",
            "ultimate_parent_companyid",
            "entity_role",
            "evidence_note",
            "review_date",
        ]
        existing = existing[[column for column in review_columns if column in existing]]
        review = review.drop(columns=["entity_role", "evidence_note", "review_date"]).merge(
            existing,
            on=["sector_id", "ultimate_parent_companyid"],
            how="left",
            validate="one_to_one",
        )
        review["entity_role"] = review["entity_role"].fillna("unclear")
        review["evidence_note"] = review["evidence_note"].fillna("")
    review.to_csv(review_target, index=False, encoding="utf-8-sig")

    roles = review[["sector_id", "ultimate_parent_companyid", "entity_role"]]
    outputs = {}
    for sample, (firm, source) in built.items():
        firm = firm.merge(
            roles,
            on=["sector_id", "ultimate_parent_companyid"],
            how="left",
            validate="many_to_one",
        )
        firm["entity_role"] = firm["entity_role"].fillna("unclear")
        firm["strategic_importer_main"] = firm["entity_role"].eq(
            "producer_brand_owner"
        ).astype("int8")
        source_target = ensure_output_path(OUT / f"panel_source_quarter_{sample}.parquet")
        firm_target = ensure_output_path(OUT / f"panel_firm_quarter_{sample}.parquet")
        atomic_parquet(source, source_target)
        atomic_parquet(firm, firm_target)
        outputs[sample] = {"source_rows": len(source), "firm_rows": len(firm)}
    return outputs


def run_qa() -> dict:
    results = {}
    for sample in ("main", "allocated"):
        source = pd.read_parquet(OUT / f"panel_source_quarter_{sample}.parquet")
        firm = pd.read_parquet(OUT / f"panel_firm_quarter_{sample}.parquet")
        sample_results = {
            "G1_source": check_keys(source),
            "G1_firm": check_keys(firm),
            "G2": check_conservation(source, firm),
            "G3": check_shares(source, firm),
            "G4": check_ownership(source),
            "G7": check_panel_sufficiency(firm, source),
        }
        if "has_financials" in firm:
            sample_results["G5"] = check_finance_asof(firm)
        results[sample] = sample_results
    write_full_report(results, OUT / "qa_full.md")
    return results


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "validate-week":
        validate_week(args.start, args.end)
    elif args.command == "extract-full":
        extract_full(args.start_quarter, args.end_quarter)
    elif args.command == "build":
        build_panels()
    elif args.command == "qa":
        run_qa()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
