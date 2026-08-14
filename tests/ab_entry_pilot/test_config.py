from scripts.ab_entry_pilot.config import (
    END_QUARTER,
    FIN_ITEMS,
    OUTPUT_ROOT,
    SECTORS,
    START_QUARTER,
    iter_quarters,
)


def test_scope_is_exactly_44_quarters_and_two_sectors():
    quarters = list(iter_quarters())
    assert quarters[0] == "2015Q1"
    assert quarters[-1] == "2025Q4"
    assert len(quarters) == 44
    assert set(SECTORS) == {"auto_8703", "refrigerator_841810"}


def test_financial_item_contract():
    assert FIN_ITEMS == {
        28: "revenue",
        34: "cogs",
        1007: "assets",
        1004: "ppent",
        2021: "capex",
        4371: "employees",
    }


def test_licensed_output_root_is_outside_onedrive():
    assert OUTPUT_ROOT == r"C:\panjiva\data\staging\ab_entry_pilot_v1"
    assert "OneDrive" not in OUTPUT_ROOT
    assert START_QUARTER == "2015Q1"
    assert END_QUARTER == "2025Q4"
