import json

import pandas as pd
import pytest

import scripts.ab_entry_pilot.extract as extract_module
from scripts.ab_entry_pilot.extract import (
    atomic_parquet,
    collect_parent_ids,
    collect_reviewed_producer_ids,
    ensure_output_path,
    extract_parent_metadata,
    extract_segment_revenue,
    run_chunk,
    sha256_file,
    update_manifest,
)


class FakeCursor:
    def __init__(self, failures=0):
        self.calls = 0
        self.failures = failures

    def execute(self, sql):
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("temporary Snowflake failure")
        return self

    def fetch_pandas_all(self):
        return pd.DataFrame({"A": [1], "B": ["x"]})


class MetadataCursor:
    def __init__(self):
        self.sql = ""

    def execute(self, sql):
        self.sql = sql.lower()
        return self

    def fetch_pandas_all(self):
        if "from ciqcompany c" in self.sql:
            return pd.DataFrame(
                {"COMPANYID": [1, 2], "COMPANYNAME": ["One", "Two"]}
            )
        return pd.DataFrame(
            {
                "COMPANYID": [1, 2],
                "FIN_PERIOD_END": pd.to_datetime(["2023-12-31", "2023-12-31"]),
                "REVENUE_USD": [10.0, 20.0],
            }
        )


class MetadataConnection:
    def cursor(self):
        return MetadataCursor()


def test_chunk_lowercases_columns_and_writes_atomically(tmp_path):
    cursor = FakeCursor()
    result = run_chunk(
        cursor,
        "select 1",
        tmp_path / "chunk.parquet",
        sleep_fn=lambda _: None,
    )
    assert result.columns.tolist() == ["a", "b"]
    assert (tmp_path / "chunk.parquet").exists()
    assert not (tmp_path / "chunk.parquet.tmp").exists()


def test_chunk_retries_twice_then_succeeds(tmp_path):
    cursor = FakeCursor(failures=2)
    run_chunk(
        cursor,
        "select 1",
        tmp_path / "chunk.parquet",
        sleep_fn=lambda _: None,
    )
    assert cursor.calls == 3


def test_atomic_parquet_replaces_only_after_success(tmp_path):
    target = tmp_path / "x.parquet"
    atomic_parquet(pd.DataFrame({"x": [1]}), target)
    assert pd.read_parquet(target).x.tolist() == [1]
    assert len(sha256_file(target)) == 64


def test_output_boundary_rejects_onedrive_path(tmp_path):
    with pytest.raises(ValueError, match="licensed output root"):
        ensure_output_path(tmp_path / "leak.parquet")


def test_manifest_update_is_atomic_and_preserves_other_chunks(tmp_path):
    path = tmp_path / "manifest.json"
    update_manifest(path, "main/auto_8703/2024Q1", {"status": "complete"})
    update_manifest(path, "main/auto_8703/2024Q2", {"status": "complete"})
    manifest = json.loads(path.read_text(encoding="utf-8"))
    assert set(manifest["chunks"]) == {
        "main/auto_8703/2024Q1",
        "main/auto_8703/2024Q2",
    }
    assert not path.with_name("manifest.json.tmp").exists()


def test_collect_parent_ids_excludes_unmatched_and_deduplicates(tmp_path):
    root = tmp_path / "chunks"
    (root / "main" / "auto_8703").mkdir(parents=True)
    (root / "allocated" / "auto_8703").mkdir(parents=True)
    pd.DataFrame(
        {"ultimate_parent_companyid": [3.0, 1.0, float("nan")]}
    ).to_parquet(root / "main" / "auto_8703" / "2024Q1.parquet", index=False)
    pd.DataFrame({"ultimate_parent_companyid": [3.0, 2.0]}).to_parquet(
        root / "allocated" / "auto_8703" / "2024Q1.parquet", index=False
    )
    assert collect_parent_ids(root) == [1, 2, 3]


def test_extract_parent_metadata_writes_combined_company_and_finance_files(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(extract_module, "OUT", tmp_path)
    chunk = tmp_path / "_chunks" / "main" / "auto_8703"
    chunk.mkdir(parents=True)
    pd.DataFrame({"ultimate_parent_companyid": [1.0, 2.0]}).to_parquet(
        chunk / "2024Q1.parquet", index=False
    )
    result = extract_parent_metadata(MetadataConnection(), batch_size=1)
    companies = pd.read_parquet(tmp_path / "firm_master.parquet")
    financials = pd.read_parquet(tmp_path / "firm_financials_annual.parquet")
    assert result == {"parents": 2, "company_rows": 2, "financial_rows": 2}
    assert companies["companyid"].tolist() == [1, 2]
    assert financials["companyid"].tolist() == [1, 2]


def test_collect_reviewed_producer_ids_filters_roles_and_deduplicates(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(extract_module, "OUT", tmp_path)
    pd.DataFrame(
        {
            "ultimate_parent_companyid": [3, 1, 2, 3],
            "entity_role": [
                "producer_brand_owner",
                "unclear",
                "producer_brand_owner",
                "producer_brand_owner",
            ],
        }
    ).to_csv(tmp_path / "entity_review_top50.csv", index=False)

    assert collect_reviewed_producer_ids() == [2, 3]


def test_extract_segment_revenue_batches_and_records_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "OUT", tmp_path)
    pd.DataFrame(
        {
            "ultimate_parent_companyid": [1, 2, 3],
            "entity_role": ["producer_brand_owner"] * 3,
        }
    ).to_csv(tmp_path / "entity_review_top50.csv", index=False)
    calls = []

    def fake_run_chunk(cursor, sql, target):
        batch_number = len(calls)
        calls.append(sql)
        frame = pd.DataFrame(
            {
                "companyid": [batch_number + 1],
                "segmentid": [100 + batch_number],
                "segment_type_id": [1],
                "segment_name": ["Synthetic segment"],
                "fin_calendar_year": [2024],
                "dataitemid": [3508],
                "revenue_usd": [10.0 + batch_number],
            }
        )
        atomic_parquet(frame, target)
        return frame

    monkeypatch.setattr(extract_module, "run_chunk", fake_run_chunk)

    class Connection:
        def cursor(self):
            return object()

    result = extract_segment_revenue(Connection(), batch_size=2)

    target = tmp_path / "auto_output_share" / "firm_segment_revenue_annual.parquet"
    manifest = json.loads((tmp_path / "extract_manifest.json").read_text("utf-8"))
    assert result == {"parents": 3, "rows": 2, "batches": 2}
    assert len(calls) == 2
    assert target.exists()
    assert manifest["chunks"]["metadata/segment_revenue"] == {
        "status": "complete",
        "rows": 2,
        "company_count": 3,
        "file_sha256": sha256_file(target),
    }
