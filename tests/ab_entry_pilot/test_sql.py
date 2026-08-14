import pytest

from scripts.ab_entry_pilot.sql import build_financial_sql, build_trade_sql


def test_trade_sql_has_required_filters_and_no_mutation():
    sql = build_trade_sql(
        "auto_8703", "2024-03-01", "2024-03-08", "main"
    ).lower()
    assert "panjivausimport" in sql
    assert "panjivausimphscode" in sql
    assert "frob" in sql
    assert "concountry" in sql
    assert "activeflag = 1" in sql
    assert "primaryflag" not in sql
    assert "shpmtorigin" in sql
    assert "count(distinct" in sql
    assert all(
        word not in f" {sql} "
        for word in (" create ", " insert ", " update ", " delete ", " merge ")
    )


def test_main_and_allocated_sql_use_distinct_hs_rules():
    main = build_trade_sql(
        "refrigerator_841810", "2024-03-01", "2024-03-08", "main"
    )
    allocated = build_trade_sql(
        "refrigerator_841810", "2024-03-01", "2024-03-08", "allocated"
    )
    assert "n_hs6 = 1" in main
    assert "1.0 / n_hs6" in allocated
    assert "Classified:" in main
    assert "Parsed:" in main
    assert "Manual:" in main


def test_trade_sql_rejects_unapproved_identifiers():
    with pytest.raises(ValueError, match="sector_id"):
        build_trade_sql("8703'; drop table x; --", "2024-03-01", "2024-03-08", "main")
    with pytest.raises(ValueError, match="sample"):
        build_trade_sql("auto_8703", "2024-03-01", "2024-03-08", "other")


def test_financial_sql_uses_annual_items_and_fx():
    sql = build_financial_sql([101, 202], "2013-01-01", "2026-01-01").lower()
    assert "periodtypeid = 1" in sql
    assert "dataitemid in (28, 34, 1004, 1007, 2021, 4371)" in sql
    assert "ciqexchangerate" in sql
    assert "latestsnapflag = 1" in sql
    assert "companyid in (101, 202)" in sql


def test_financial_sql_rejects_empty_company_list():
    with pytest.raises(ValueError, match="company_ids"):
        build_financial_sql([], "2013-01-01", "2026-01-01")


def test_financial_output_selects_employees_once():
    sql = build_financial_sql([101], "2013-01-01", "2026-01-01").lower()
    final_select = sql.split("select f.companyid as companyid", 1)[1]
    assert final_select.count("f.employees") == 1
