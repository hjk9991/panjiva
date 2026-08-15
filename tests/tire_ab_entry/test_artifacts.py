from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading

import pandas as pd
import pytest

import scripts.tire_ab_entry.artifacts as artifacts_module
from scripts.tire_ab_entry.artifacts import (
    atomic_write_bytes,
    atomic_write_parquet,
    sanitize_candidate_csv,
)


@pytest.fixture(autouse=True)
def allow_test_output_paths(monkeypatch):
    monkeypatch.setattr(
        artifacts_module, "validate_output_path", lambda path: Path(path).resolve()
    )


def _temporary_residue(folder):
    return tuple(folder.glob(".*.atomic-*.tmp"))


def test_atomic_writer_uses_unique_temps_during_concurrent_writes(tmp_path):
    target = tmp_path / "artifact.bin"
    barrier = threading.Barrier(2)
    seen = []
    lock = threading.Lock()

    def write(payload):
        def validate(path):
            with lock:
                seen.append(path.name)
            barrier.wait(timeout=5)

        atomic_write_bytes(payload, target, validator=validate)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write, payload) for payload in (b"first", b"second")]
        for future in futures:
            future.result(timeout=10)

    assert len(set(seen)) == 2
    assert target.read_bytes() in {b"first", b"second"}
    assert _temporary_residue(tmp_path) == ()


def test_atomic_writer_preserves_old_target_on_validation_failure(tmp_path):
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"old")

    with pytest.raises(RuntimeError, match="validation failed"):
        atomic_write_bytes(
            b"new",
            target,
            validator=lambda path: (_ for _ in ()).throw(
                RuntimeError("validation failed")
            ),
        )

    assert target.read_bytes() == b"old"
    assert _temporary_residue(tmp_path) == ()


def test_atomic_writer_cleans_up_after_mid_write_failure(tmp_path, monkeypatch):
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"old")

    def fail_mid_write(fd, payload):
        artifacts_module.os.write(fd, payload[:1])
        raise OSError("mid-write failure")

    monkeypatch.setattr(artifacts_module, "_write_payload", fail_mid_write)
    with pytest.raises(OSError, match="mid-write"):
        atomic_write_bytes(b"new", target)

    assert target.read_bytes() == b"old"
    assert _temporary_residue(tmp_path) == ()


def test_atomic_writer_detects_content_change_before_replace(tmp_path):
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"old")

    def corrupt(path):
        path.write_bytes(b"corrupt")

    with pytest.raises(RuntimeError, match="checksum"):
        atomic_write_bytes(b"new", target, validator=corrupt)

    assert target.read_bytes() == b"old"
    assert _temporary_residue(tmp_path) == ()


def test_atomic_parquet_validates_exact_dataframe_content(tmp_path):
    target = tmp_path / "artifact.parquet"
    frame = pd.DataFrame({"id": pd.Series([1, 2], dtype="int64"), "name": ["a", "b"]})
    digest = atomic_write_parquet(frame, target)
    restored = pd.read_parquet(target)
    pd.testing.assert_frame_equal(restored, frame)
    assert len(digest) == 64


def test_candidate_csv_sanitizer_neutralizes_formulas_without_mutating_canonical():
    canonical = pd.DataFrame(
        {
            "target_search_term": ["Michelin", "Goodyear", "Hankook Tire & Technology"],
            "company_id": [1, 2, 3],
            "current_ultimate_parent_id": [10, 20, 30],
            "company_name": ["=cmd", "+plus", "@mention"],
            "country": [pd.NA, "-country", "South Korea"],
            "company_type": ["Public Company"] * 3,
            "company_status": ["Operating"] * 3,
            "industry": [pd.NA, "Tires", "Rubber"],
            "country_missing": [True, False, False],
            "industry_missing": [True, False, False],
        }
    )
    original = canonical.copy(deep=True)

    sanitized = sanitize_candidate_csv(canonical)

    pd.testing.assert_frame_equal(canonical, original)
    assert tuple(sanitized["company_id"]) == (1, 2, 3)
    assert tuple(sanitized["company_name"]) == ("'=cmd", "'+plus", "'@mention")
    assert sanitized.loc[0, "country"] == "[MISSING: see country_missing]"
    assert sanitized.loc[0, "industry"] == "[MISSING: see industry_missing]"
    assert sanitized.loc[1, "country"] == "'-country"
