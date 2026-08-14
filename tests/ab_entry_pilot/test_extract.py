import json

import pandas as pd
import pytest

from scripts.ab_entry_pilot.extract import (
    atomic_parquet,
    ensure_output_path,
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
