from pathlib import Path

import pandas as pd
import pytest

import scripts.tire_ab_entry.schema_probe as probe_module
from scripts.tire_ab_entry.schema_probe import (
    CANDIDATE_COLUMNS,
    SCHEMA_OUTPUT_PATH,
    build_parent_candidate_query,
    build_schema_query,
    choose_description_column,
    discover_parent_candidates,
    probe_schema,
)


FORBIDDEN_SQL = (" create ", " insert ", " update ", " delete ", " merge ", " drop ")


class FakeCursor:
    def __init__(self, frame):
        self.frame = frame
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetch_pandas_all(self):
        return self.frame.copy()


class FakeConnection:
    account = "test-account"
    warehouse = "test-warehouse"
    database = "MI_XPRESSCLOUD"
    schema = "XPRESSFEED"

    def __init__(self, frame):
        self._cursor = FakeCursor(frame)

    def cursor(self):
        return self._cursor


def test_choose_description_column_uses_reviewed_preference():
    columns = [("PANJIVAUSIMPORT", "PANJIVARECORDID")]
    assert choose_description_column(columns) is None
    columns = [
        ("PANJIVAUSIMPORT", "PRODUCTDESCRIPTION"),
        ("PANJIVAUSIMPORT", "GOODSDESCRIPTION"),
    ]
    assert choose_description_column(columns) == (
        "PANJIVAUSIMPORT",
        "GOODSDESCRIPTION",
    )


def test_choose_description_column_normalizes_case_and_rejects_ambiguity():
    assert choose_description_column([("panjivausimport", "goodsdescription")]) == (
        "PANJIVAUSIMPORT",
        "GOODSDESCRIPTION",
    )
    with pytest.raises(ValueError, match="ambiguous"):
        choose_description_column(
            [
                ("PANJIVAUSIMPORT", "GOODSDESCRIPTION"),
                ("PANJIVAUSIMPORTARCHIVE", "goodsdescription"),
            ]
        )


def test_schema_query_is_information_schema_select_only():
    sql = build_schema_query()
    lowered = f" {sql.lower()} "
    assert lowered.lstrip().startswith("select")
    assert "information_schema.columns" in lowered
    assert "panjivausimp%" in lowered
    assert not any(word in lowered for word in FORBIDDEN_SQL)


def test_probe_schema_writes_complete_metadata_and_reports_available(tmp_path, monkeypatch):
    target = tmp_path / "panjiva_columns.parquet"
    frame = pd.DataFrame(
        {
            "TABLE_CATALOG": ["MI_XPRESSCLOUD", "MI_XPRESSCLOUD"],
            "TABLE_SCHEMA": ["XPRESSFEED", "XPRESSFEED"],
            "TABLE_NAME": ["PANJIVAUSIMPORT", "PANJIVAUSIMPORT"],
            "COLUMN_NAME": ["PANJIVARECORDID", "GOODSDESCRIPTION"],
            "ORDINAL_POSITION": [1, 2],
            "DATA_TYPE": ["NUMBER", "TEXT"],
        }
    )
    connection = FakeConnection(frame)
    monkeypatch.setattr(probe_module, "validate_output_path", lambda path: True)

    result = probe_schema(connection, output_path=target)

    saved = pd.read_parquet(target)
    assert result["description_mode"] == "available"
    assert result["description_table"] == "PANJIVAUSIMPORT"
    assert result["description_column"] == "GOODSDESCRIPTION"
    assert result["output_path"] == str(target)
    assert tuple(saved["column_name"]) == ("PANJIVARECORDID", "GOODSDESCRIPTION")
    assert {
        "query_timestamp_utc",
        "query_account",
        "query_warehouse",
        "query_database",
        "query_schema",
    }.issubset(saved.columns)
    sql, params = connection._cursor.calls[0]
    assert params is None
    assert sql == build_schema_query()


def test_probe_schema_reports_unavailable_without_attribution(tmp_path, monkeypatch):
    target = tmp_path / "panjiva_columns.parquet"
    frame = pd.DataFrame(
        {"TABLE_NAME": ["PANJIVAUSIMPORT"], "COLUMN_NAME": ["PANJIVARECORDID"]}
    )
    monkeypatch.setattr(probe_module, "validate_output_path", lambda path: True)

    result = probe_schema(FakeConnection(frame), output_path=target)

    assert result["description_mode"] == "unavailable"
    assert "description_table" not in result
    assert "description_column" not in result


def test_parent_query_is_read_only_parameterized_and_deterministic():
    sql, params = build_parent_candidate_query()
    lowered = f" {sql.lower()} "
    assert lowered.lstrip().startswith("select")
    assert not any(word in lowered for word in FORBIDDEN_SQL)
    assert sql.count("%s") == 6
    assert "Michelin" not in sql
    assert params == (
        "Michelin",
        "%michelin%",
        "Goodyear",
        "%goodyear%",
        "Hankook Tire & Technology",
        "%hankook tire & technology%",
    )
    assert "order by target_search_term, company_name, company_id" in lowered


def test_discover_parent_candidates_preserves_candidate_schema_without_selection(
    tmp_path, monkeypatch
):
    target = tmp_path / "manufacturer_parent_candidates.csv"
    frame = pd.DataFrame(
        {
            "TARGET_SEARCH_TERM": ["Michelin"],
            "COMPANY_ID": [101],
            "CURRENT_ULTIMATE_PARENT_ID": [100],
            "COMPANY_NAME": ["Michelin Example"],
            "COUNTRY": ["France"],
            "COMPANY_TYPE": ["Public Company"],
            "COMPANY_STATUS": ["Operating"],
            "INDUSTRY": ["Tires and Rubber"],
        }
    )
    connection = FakeConnection(frame)
    monkeypatch.setattr(probe_module, "validate_output_path", lambda path: True)

    result = discover_parent_candidates(connection, output_path=target)

    saved = pd.read_csv(target)
    assert tuple(saved.columns) == CANDIDATE_COLUMNS
    assert "manufacturer_parent_id" not in saved.columns
    assert result["candidate_counts"] == {
        "Michelin": 1,
        "Goodyear": 0,
        "Hankook Tire & Technology": 0,
    }
    sql, params = connection._cursor.calls[0]
    assert (sql, params) == build_parent_candidate_query()


def test_runtime_paths_are_exact_and_licensed():
    assert SCHEMA_OUTPUT_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\_schema\panjiva_columns.parquet"
    )
    assert probe_module.PARENT_CANDIDATE_OUTPUT_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\review\manufacturer_parent_candidates.csv"
    )
