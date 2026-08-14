import pandas as pd

from scripts.ab_entry_pilot.cli import build_parser, reference_week_totals


def test_cli_exposes_four_execution_commands():
    parser = build_parser()
    for command in ("validate-week", "extract-full", "build", "qa"):
        args = parser.parse_args([command])
        assert args.command == command


def test_reference_week_totals_reproduce_main_and_allocated_rules(tmp_path):
    l2 = tmp_path / "L2"
    l2.mkdir()
    shipments = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "arrival_date": pd.to_datetime(
                ["2024-03-02", "2024-03-03", "2024-03-04"]
            ),
            "value_usd": [100.0, 200.0, 300.0],
            "weight_kg": [10.0, 20.0, 30.0],
            "teu": [1.0, 2.0, 3.0],
            "n_hs6": [1, 1, 2],
            "hs6_main": ["870323", "841810", "870323"],
            "consignee_up": [10.0, 20.0, 30.0],
            "consignee_ciqid": [11.0, 21.0, 31.0],
        }
    )
    shipment_hs = pd.DataFrame(
        {
            "record_id": [1, 2, 3, 3],
            "hs6": ["870323", "841810", "870323", "841810"],
            "value_alloc": [100.0, 200.0, 150.0, 150.0],
            "weight_alloc": [10.0, 20.0, 15.0, 15.0],
            "teu_alloc": [1.0, 2.0, 1.5, 1.5],
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
        "shipment_count": 1,
        "value_usd": 100.0,
        "weight_kg": 10.0,
        "teu": 1.0,
    }
    assert allocated == {
        "shipment_count": 2,
        "value_usd": 250.0,
        "weight_kg": 25.0,
        "teu": 2.5,
    }
