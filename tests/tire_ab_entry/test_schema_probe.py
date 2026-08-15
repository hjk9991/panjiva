from pathlib import Path
import hashlib
import json

import pandas as pd
import pytest

import scripts.tire_ab_entry.schema_probe as probe_module
from scripts.tire_ab_entry.schema_probe import (
    APPROVED_DATABASE,
    APPROVED_SCHEMA,
    CANDIDATE_COLUMNS,
    PARENT_CANDIDATE_METADATA_PATH,
    SCHEMA_OUTPUT_PATH,
    PARENT_CANDIDATE_PARQUET_PATH,
    assert_read_only_query,
    build_parent_candidate_query,
    build_schema_query,
    choose_description_column,
    compute_query_contract_hash,
    discover_parent_candidates,
    probe_schema,
    validate_candidate_frame,
    validate_connection_context,
    validate_schema_frame,
)


FORBIDDEN_SQL = (" create ", " insert ", " update ", " delete ", " merge ", " drop ")


class FakeCursor:
    def __init__(self, frame, *, execute_error=None, fetch_error=None, close_error=None):
        self.frame = frame
        self.calls = []
        self.execute_error = execute_error
        self.fetch_error = fetch_error
        self.close_error = close_error
        self.closed = False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if self.execute_error is not None:
            raise self.execute_error
        return self

    def fetch_pandas_all(self):
        if self.fetch_error is not None:
            raise self.fetch_error
        return self.frame.copy()

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeConnection:
    account = "VLC67107"
    warehouse = "XF_READER_KOREADEVELOPMENT_WH"
    database = "MI_XPRESSCLOUD"
    schema = "XPRESSFEED"
    role = "XF_READER_KOREADEVELOPMENT"

    def __init__(self, frame, **cursor_options):
        self._cursor = FakeCursor(frame, **cursor_options)

    def cursor(self):
        return self._cursor


def schema_frame(*column_names):
    return pd.DataFrame(
        {
            "TABLE_CATALOG": ["MI_XPRESSCLOUD"] * len(column_names),
            "TABLE_SCHEMA": ["XPRESSFEED"] * len(column_names),
            "TABLE_NAME": ["PANJIVAUSIMPORT"] * len(column_names),
            "COLUMN_NAME": list(column_names),
            "ORDINAL_POSITION": range(1, len(column_names) + 1),
            "DATA_TYPE": ["TEXT"] * len(column_names),
            "IS_NULLABLE": ["YES"] * len(column_names),
            "CHARACTER_MAXIMUM_LENGTH": [None] * len(column_names),
            "NUMERIC_PRECISION": [None] * len(column_names),
            "NUMERIC_SCALE": [None] * len(column_names),
            "DATETIME_PRECISION": [None] * len(column_names),
        }
    )


def complete_candidate_frame():
    return pd.DataFrame(
        {
            "TARGET_SEARCH_TERM": [
                "Michelin",
                "Goodyear",
                "Hankook Tire & Technology",
            ],
            "COMPANY_ID": [101, 201, 301],
            "CURRENT_ULTIMATE_PARENT_ID": [100, 200, 300],
            "COMPANY_NAME": ["Michelin Example", "Goodyear Example", "Hankook Example"],
            "COUNTRY": ["France", "United States", "South Korea"],
            "COMPANY_TYPE": ["Public Company"] * 3,
            "COMPANY_STATUS": ["Operating"] * 3,
            "INDUSTRY": ["Tires and Rubber"] * 3,
        }
    )


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def allow_test_artifact_paths(monkeypatch):
    monkeypatch.setattr(
        probe_module.artifacts,
        "validate_output_path",
        lambda path: Path(path).resolve(),
    )


def test_choose_description_column_uses_reviewed_preference():
    columns = [
        ("MI_XPRESSCLOUD", "XPRESSFEED", "PANJIVAUSIMPORT", "PANJIVARECORDID")
    ]
    assert choose_description_column(columns) is None
    columns = [
        ("MI_XPRESSCLOUD", "XPRESSFEED", "PANJIVAUSIMPORT", "PRODUCTDESCRIPTION"),
        ("MI_XPRESSCLOUD", "XPRESSFEED", "PANJIVAUSIMPORT", "GOODSDESCRIPTION"),
    ]
    assert choose_description_column(columns) == (
        "MI_XPRESSCLOUD",
        "XPRESSFEED",
        "PANJIVAUSIMPORT",
        "GOODSDESCRIPTION",
    )


def test_choose_description_column_normalizes_case_and_rejects_ambiguity():
    assert choose_description_column(
        [("mi_xpresscloud", "xpressfeed", "panjivausimport", "goodsdescription")]
    ) == ("MI_XPRESSCLOUD", "XPRESSFEED", "PANJIVAUSIMPORT", "GOODSDESCRIPTION")
    with pytest.raises(ValueError, match="ambiguous"):
        choose_description_column(
            [
                (
                    "MI_XPRESSCLOUD",
                    "XPRESSFEED",
                    "PANJIVAUSIMPORT",
                    "GOODSDESCRIPTION",
                ),
                (
                    "MI_XPRESSCLOUD",
                    "OTHER_SCHEMA",
                    "PANJIVAUSIMPORT",
                    "goodsdescription",
                ),
            ]
        )


def test_schema_query_is_information_schema_select_only():
    sql = build_schema_query()
    lowered = f" {sql.lower()} "
    assert lowered.lstrip().startswith("select")
    assert f"{APPROVED_DATABASE}.information_schema.columns".lower() in lowered
    assert f"upper(table_schema) = '{APPROVED_SCHEMA}'".lower() in lowered
    assert "panjivausimp%" in lowered
    assert not any(word in lowered for word in FORBIDDEN_SQL)


@pytest.mark.parametrize(
    "sql",
    [
        "select 1; delete from x",
        "select 1 -- hidden mutation",
        "select 1 /* hidden */",
        "with x as (delete from y) select * from x",
        "with x as (select 1) execute immediate $payload$",
        "show tables",
    ],
)
def test_read_only_guard_rejects_multiple_statements_comments_and_mutations(sql):
    with pytest.raises(ValueError, match="read-only"):
        assert_read_only_query(sql)


def test_connection_context_rejects_wrong_namespace_before_query():
    connection = FakeConnection(schema_frame("PANJIVARECORDID"))
    connection.database = "OTHER_DATABASE"
    with pytest.raises(ValueError, match="database"):
        validate_connection_context(connection)
    assert connection._cursor.calls == []


@pytest.mark.parametrize("account", [None, "", "OTHER_ACCOUNT"])
def test_connection_context_rejects_blank_or_wrong_account_before_query(account):
    connection = FakeConnection(schema_frame("PANJIVARECORDID"))
    connection.account = account
    with pytest.raises(ValueError, match="account"):
        validate_connection_context(connection)
    assert connection._cursor.calls == []


@pytest.mark.parametrize(
    "mutator",
    [
        lambda frame: frame.iloc[0:0],
        lambda frame: frame.drop(columns="DATA_TYPE"),
        lambda frame: frame.assign(TABLE_CATALOG=None),
        lambda frame: frame.assign(TABLE_SCHEMA="OTHER_SCHEMA"),
        lambda frame: frame.assign(TABLE_NAME="OTHER_TABLE"),
        lambda frame: pd.concat([frame, frame], ignore_index=True),
    ],
)
def test_schema_contract_fails_closed_on_empty_partial_null_wrong_or_duplicate(mutator):
    with pytest.raises(ValueError, match="schema metadata"):
        validate_schema_frame(mutator(schema_frame("PANJIVARECORDID")))


def test_schema_contract_returns_deterministic_full_identity():
    frame = schema_frame("GOODSDESCRIPTION", "PANJIVARECORDID")
    frame["ORDINAL_POSITION"] = [2, 1]
    validated = validate_schema_frame(frame)
    assert tuple(validated["column_name"]) == (
        "PANJIVARECORDID",
        "GOODSDESCRIPTION",
    )
    assert not validated.duplicated(
        ["table_catalog", "table_schema", "table_name", "column_name"]
    ).any()


@pytest.mark.parametrize("ordinal", [0, -1, 1.5])
def test_schema_contract_requires_positive_integral_ordinals(ordinal):
    frame = schema_frame("PANJIVARECORDID")
    frame["ORDINAL_POSITION"] = ordinal
    with pytest.raises(ValueError, match="ordinal"):
        validate_schema_frame(frame)


def test_schema_contract_rejects_duplicate_ordinals_within_table():
    frame = schema_frame("PANJIVARECORDID", "GOODSDESCRIPTION")
    frame["ORDINAL_POSITION"] = [1, 1]
    with pytest.raises(ValueError, match="ordinal"):
        validate_schema_frame(frame)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda frame: frame.iloc[0:0],
        lambda frame: frame[frame["TARGET_SEARCH_TERM"].ne("Goodyear")],
        lambda frame: frame.assign(COMPANY_ID=[101.5, 201, 301]),
        lambda frame: frame.assign(CURRENT_ULTIMATE_PARENT_ID=[None, 200, 300]),
        lambda frame: frame.assign(COMPANY_NAME=["", "Goodyear", "Hankook"]),
        lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
    ],
)
def test_candidate_contract_fails_closed_on_invalid_core_rows(mutator):
    with pytest.raises(ValueError, match="candidate"):
        validate_candidate_frame(mutator(complete_candidate_frame()))


def test_candidate_contract_preserves_nullable_source_fields_with_missing_flags():
    frame = complete_candidate_frame()
    frame.loc[0, "COUNTRY"] = None
    frame.loc[1, "INDUSTRY"] = ""
    validated, warnings = validate_candidate_frame(frame)
    michelin = validated[validated["target_search_term"].eq("Michelin")].iloc[0]
    goodyear = validated[validated["target_search_term"].eq("Goodyear")].iloc[0]
    assert pd.isna(michelin["country"])
    assert bool(michelin["country_missing"])
    assert pd.isna(goodyear["industry"])
    assert bool(goodyear["industry_missing"])
    assert warnings == ()


def test_candidate_row_safeguard_warns_and_never_silently_truncates():
    frame = complete_candidate_frame()
    extra = frame.iloc[[0]].copy()
    extra["COMPANY_ID"] = 102
    extra["CURRENT_ULTIMATE_PARENT_ID"] = 102
    frame = pd.concat([frame, extra], ignore_index=True)
    validated, warnings = validate_candidate_frame(
        frame, warn_rows_per_target=1, max_rows_per_target=2
    )
    assert len(validated) == 4
    assert warnings == ("Michelin",)
    with pytest.raises(ValueError, match="safeguard"):
        validate_candidate_frame(frame, warn_rows_per_target=1, max_rows_per_target=1)


def test_probe_schema_writes_complete_metadata_and_reports_available(tmp_path, monkeypatch):
    target = tmp_path / "panjiva_columns.parquet"
    frame = schema_frame("PANJIVARECORDID", "GOODSDESCRIPTION")
    connection = FakeConnection(frame)
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )

    result = probe_schema(connection, output_path=target)

    saved = pd.read_parquet(target)
    assert result["description_mode"] == "available"
    assert result["description_catalog"] == "MI_XPRESSCLOUD"
    assert result["description_schema"] == "XPRESSFEED"
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
    assert connection._cursor.closed


def test_probe_schema_reports_unavailable_without_attribution(tmp_path, monkeypatch):
    target = tmp_path / "panjiva_columns.parquet"
    frame = schema_frame("PANJIVARECORDID")
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )

    result = probe_schema(FakeConnection(frame), output_path=target)

    assert result["description_mode"] == "unavailable"
    assert "description_table" not in result
    assert "description_column" not in result


def test_probe_schema_closes_cursor_when_fetch_fails(tmp_path, monkeypatch):
    connection = FakeConnection(
        schema_frame("PANJIVARECORDID"), fetch_error=RuntimeError("fetch failed")
    )
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    with pytest.raises(RuntimeError, match="fetch failed"):
        probe_schema(connection, output_path=tmp_path / "schema.parquet")
    assert connection._cursor.closed


def test_probe_schema_closes_cursor_when_execute_or_validation_fails(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    execute_failure = FakeConnection(
        schema_frame("PANJIVARECORDID"), execute_error=RuntimeError("execute failed")
    )
    with pytest.raises(RuntimeError, match="execute failed"):
        probe_schema(execute_failure, output_path=tmp_path / "execute.parquet")
    assert execute_failure._cursor.closed

    invalid_frame = FakeConnection(schema_frame("PANJIVARECORDID").drop(columns="DATA_TYPE"))
    with pytest.raises(ValueError, match="schema metadata"):
        probe_schema(invalid_frame, output_path=tmp_path / "invalid.parquet")
    assert invalid_frame._cursor.closed


def test_probe_schema_propagates_cursor_close_failure(tmp_path, monkeypatch):
    connection = FakeConnection(
        schema_frame("PANJIVARECORDID"), close_error=RuntimeError("cursor close failed")
    )
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    with pytest.raises(RuntimeError, match="cursor close failed"):
        probe_schema(connection, output_path=tmp_path / "schema.parquet")
    assert connection._cursor.closed


def test_parent_query_is_read_only_parameterized_and_deterministic():
    sql, params = build_parent_candidate_query()
    lowered = f" {sql.lower()} "
    assert lowered.lstrip().startswith("with")
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
    assert lowered.count(f"{APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqcompany c".lower()) == 1
    assert "values" in lowered
    assert "order by target_search_term, company_name, company_id" in lowered


def test_query_contract_hash_changes_with_ordered_parameters():
    sql, parameters = build_parent_candidate_query()
    baseline = compute_query_contract_hash(sql, parameters)
    changed_value = compute_query_contract_hash(
        sql, (*parameters[:-1], "%changed-target%")
    )
    changed_order = compute_query_contract_hash(
        sql, (parameters[2], parameters[3], parameters[0], parameters[1], *parameters[4:])
    )
    assert baseline != changed_value
    assert baseline != changed_order


def test_discover_parent_candidates_preserves_candidate_schema_without_selection(
    tmp_path, monkeypatch
):
    target = tmp_path / "manufacturer_parent_candidates.csv"
    frame = complete_candidate_frame()
    frame.loc[0, "COUNTRY"] = None
    frame.loc[1, "INDUSTRY"] = None
    frame.loc[2, "COMPANY_NAME"] = "=Hankook Formula"
    connection = FakeConnection(frame)
    publication_calls = []
    real_publish = probe_module.artifacts.publish_candidate_artifact_set

    def record_publish(*args, **kwargs):
        publication_calls.append((args, kwargs))
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(
        probe_module.artifacts, "publish_candidate_artifact_set", record_publish
    )
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    monkeypatch.setattr(
        probe_module.artifacts,
        "validate_output_path",
        lambda path: Path(path).resolve(),
    )

    result = discover_parent_candidates(connection, output_path=target)

    saved = pd.read_csv(target)
    canonical_path = tmp_path / "manufacturer_parent_candidates.parquet"
    metadata_path = tmp_path / "manufacturer_parent_candidates.metadata.json"
    canonical = pd.read_parquet(canonical_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert len(publication_calls) == 1
    assert tuple(saved.columns[:8]) == CANDIDATE_COLUMNS[:8]
    assert "manufacturer_parent_id" not in saved.columns
    assert pd.isna(
        canonical.loc[canonical["target_search_term"].eq("Michelin"), "country"].iloc[0]
    )
    assert saved.loc[saved["target_search_term"].eq("Michelin"), "country"].iloc[0] == (
        "[MISSING: see country_missing]"
    )
    assert saved.loc[
        saved["target_search_term"].eq("Hankook Tire & Technology"), "company_name"
    ].iloc[0] == "'=Hankook Formula"
    assert result["candidate_counts"] == {
        "Michelin": 1,
        "Goodyear": 1,
        "Hankook Tire & Technology": 1,
    }
    assert result["canonical_path"] == str(canonical_path)
    assert result["metadata_path"] == str(metadata_path)
    assert metadata["sql_contract_version"] == "tire-parent-candidate-v2"
    assert metadata["query_hash_contract_version"] == "sql-plus-ordered-parameters-v1"
    assert metadata["query_contract_sha256"] == compute_query_contract_hash(
        *build_parent_candidate_query()
    )
    assert metadata["namespace"]["database"] == "MI_XPRESSCLOUD"
    assert metadata["namespace"]["schema"] == "XPRESSFEED"
    assert metadata["namespace"]["role"] == "XF_READER_KOREADEVELOPMENT"
    assert metadata["candidate_counts"] == result["candidate_counts"]
    assert metadata["artifact_sha256"]["canonical_parquet"] == file_sha256(
        canonical_path
    )
    assert metadata["artifact_sha256"]["sanitized_csv"] == file_sha256(target)
    sql, params = connection._cursor.calls[0]
    assert (sql, params) == build_parent_candidate_query()
    assert connection._cursor.closed


def test_runtime_paths_are_exact_and_licensed():
    assert SCHEMA_OUTPUT_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\_schema\panjiva_columns.parquet"
    )
    assert probe_module.PARENT_CANDIDATE_OUTPUT_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\review\manufacturer_parent_candidates.csv"
    )
    assert PARENT_CANDIDATE_PARQUET_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\review\manufacturer_parent_candidates.parquet"
    )
    assert PARENT_CANDIDATE_METADATA_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\review\manufacturer_parent_candidates.metadata.json"
    )


def test_probe_and_discovery_reject_wrong_suffix_before_query(tmp_path, monkeypatch):
    connection = FakeConnection(schema_frame("PANJIVARECORDID"))
    monkeypatch.setattr(
        probe_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    with pytest.raises(ValueError, match=r"\.parquet"):
        probe_schema(connection, output_path=tmp_path / "schema.csv")
    with pytest.raises(ValueError, match=r"\.csv"):
        discover_parent_candidates(connection, output_path=tmp_path / "candidates.parquet")
    assert connection._cursor.calls == []


def test_candidate_artifact_paths_must_be_distinct_before_query(tmp_path, monkeypatch):
    connection = FakeConnection(complete_candidate_frame())
    collision = (tmp_path / "collision").resolve()
    monkeypatch.setattr(probe_module, "validate_output_path", lambda path: collision)
    with pytest.raises(ValueError, match="distinct"):
        discover_parent_candidates(connection, output_path=tmp_path / "candidates.csv")
    assert connection._cursor.calls == []
