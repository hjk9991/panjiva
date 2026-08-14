import pandas as pd

import scripts.ab_entry_pilot.cli as cli_module
import scripts.ab_entry_pilot.extract as extract_module
from scripts.ab_entry_pilot.cli import (
    build_panels,
    build_parser,
    extract_full,
    reference_week_totals,
)


def test_cli_exposes_five_execution_commands():
    parser = build_parser()
    for command in (
        "validate-week",
        "extract-full",
        "extract-segments",
        "build",
        "qa",
    ):
        args = parser.parse_args([command])
        assert args.command == command


def test_reference_week_totals_reproduce_main_and_allocated_rules(tmp_path):
    l2 = tmp_path / "L2"
    l2.mkdir()
    shipments = pd.DataFrame(
        {
            "record_id": [1, 2, 3, 4],
            "arrival_date": pd.to_datetime(
                ["2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05"]
            ),
            "value_usd": [100.0, 200.0, 300.0, 7.0],
            "weight_kg": [10.0, 20.0, 30.0, 0.7],
            "teu": [1.0, 2.0, 3.0, 0.07],
            "n_hs6": [1, 1, 2, 1],
            "hs6_main": ["870323", "841810", "870323", "870323"],
            "consignee_up": [10.0, 20.0, 30.0, float("nan")],
            "consignee_ciqid": [11.0, 21.0, 31.0, float("nan")],
        }
    )
    shipment_hs = pd.DataFrame(
        {
            "record_id": [1, 2, 3, 3, 4],
            "hs6": ["870323", "841810", "870323", "841810", "870323"],
            "value_alloc": [100.0, 200.0, 150.0, 150.0, 7.0],
            "weight_alloc": [10.0, 20.0, 15.0, 15.0, 0.7],
            "teu_alloc": [1.0, 2.0, 1.5, 1.5, 0.07],
        }
    )
    shipments.to_parquet(l2 / "fact_shipment.parquet", index=False)
    shipment_hs.to_parquet(l2 / "fact_shipment_hs.parquet", index=False)

    main = reference_week_totals(
        l2, "2024-03-01", "2024-03-08", "auto_8703", "main"
    )
    allocated = reference_week_totals(
        l2, "2024-03-01", "2024-03-08", "auto_8703", "allocated"
    )
    assert main == {
        "shipment_count": 2,
        "value_usd": 107.0,
        "weight_kg": 10.7,
        "teu": 1.07,
    }
    assert allocated == {
        "shipment_count": 3,
        "value_usd": 257.0,
        "weight_kg": 25.7,
        "teu": 2.57,
    }


def test_extract_full_runs_trade_then_parent_metadata(monkeypatch):
    calls = []

    class Connection:
        def cursor(self):
            return "cursor"

        def close(self):
            calls.append("close")

    monkeypatch.setattr(cli_module, "validate_week", lambda: calls.append("g0"))
    monkeypatch.setattr(cli_module, "connect", lambda: Connection())
    monkeypatch.setattr(
        cli_module,
        "extract_trade_chunks",
        lambda cursor, quarters: calls.append(("trade", len(quarters))) or {"ok": True},
    )
    monkeypatch.setattr(
        cli_module,
        "extract_parent_metadata",
        lambda connection: calls.append("metadata") or {"parents": 2},
        raising=False,
    )
    result = extract_full("2024Q1", "2024Q2")
    assert calls == ["g0", ("trade", 2), "metadata", "close"]
    assert result == {"trade_manifest": {"ok": True}, "metadata": {"parents": 2}}


def test_extract_segments_dispatches_and_closes_connection(monkeypatch):
    calls = []

    class Connection:
        def close(self):
            calls.append("close")

    monkeypatch.setattr(cli_module, "connect", lambda: Connection())
    monkeypatch.setattr(
        cli_module,
        "extract_segment_revenue",
        lambda connection: calls.append("segments") or {"rows": 2},
        raising=False,
    )

    assert cli_module.main(["extract-segments"]) == 0
    assert calls == ["segments", "close"]


def test_build_panels_attaches_finance_and_creates_entity_review_queue(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(cli_module, "OUT", tmp_path)
    monkeypatch.setattr(extract_module, "OUT", tmp_path)
    row = pd.DataFrame(
        {
            "ultimate_parent_companyid": [1.0],
            "sector_id": ["auto_8703"],
            "origin_country": ["JP"],
            "year_quarter": ["2024Q1"],
            "shipment_count": [4],
            "shipment_equivalent": [4.0],
            "value_usd": [2_000_000.0],
            "weight_kg": [20.0],
            "teu": [2.0],
        }
    )
    for sample in ("main", "allocated"):
        folder = tmp_path / "_chunks" / sample / "auto_8703"
        folder.mkdir(parents=True)
        row.to_parquet(folder / "2024Q1.parquet", index=False)
    pd.DataFrame(
        {"companyid": [1], "companyname": ["One Motors"], "industry": ["Cars"]}
    ).to_parquet(tmp_path / "firm_master.parquet", index=False)
    pd.DataFrame(
        {
            "companyid": [1],
            "fin_period_end": pd.to_datetime(["2023-12-31"]),
            "revenue_usd": [100.0],
        }
    ).to_parquet(tmp_path / "firm_financials_annual.parquet", index=False)

    build_panels()

    firm = pd.read_parquet(tmp_path / "panel_firm_quarter_main.parquet")
    queue = pd.read_csv(tmp_path / "entity_review_top50.csv")
    q1 = firm[firm["year_quarter"].eq("2024Q1")].iloc[0]
    assert q1["revenue_usd"] == 100.0
    assert q1["companyname"] == "One Motors"
    assert queue["entity_role"].tolist() == ["unclear"]
