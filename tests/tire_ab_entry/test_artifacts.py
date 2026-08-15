from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import multiprocessing
from pathlib import Path
import threading
import time

import pandas as pd
import pytest

import scripts.tire_ab_entry.artifacts as artifacts_module
from scripts.tire_ab_entry.artifacts import (
    atomic_write_bytes,
    atomic_write_parquet,
    candidate_publication_lock,
    publish_candidate_artifact_set,
    sanitize_candidate_csv,
)


@pytest.fixture(autouse=True)
def allow_test_output_paths(monkeypatch):
    monkeypatch.setattr(
        artifacts_module, "validate_output_path", lambda path: Path(path).resolve()
    )


def _temporary_residue(folder):
    return tuple(folder.glob(".*.atomic-*.tmp"))


def _publish_generation_worker(root_text, generation, start_event, queue):
    from pathlib import Path
    import pandas as pd
    import scripts.tire_ab_entry.artifacts as worker_artifacts

    worker_artifacts.validate_output_path = lambda path: Path(path).resolve()
    root = Path(root_text)
    frame = pd.DataFrame(
        {
            "generation": [generation] * 20_000,
            "payload": [generation * 128] * 20_000,
        }
    )
    start_event.wait(timeout=10)
    try:
        worker_artifacts.publish_candidate_artifact_set(
            frame,
            frame,
            {"generation": generation},
            canonical_path=root / "candidates.parquet",
            csv_path=root / "candidates.csv",
            metadata_path=root / "candidates.metadata.json",
            lock_timeout_seconds=10,
        )
        queue.put(None)
    except Exception as error:
        queue.put(type(error).__name__)
        raise


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


def test_publication_lock_has_bounded_wait_and_safe_release(tmp_path):
    lock_path = tmp_path / ".candidates.publication.lock"
    with candidate_publication_lock(lock_path, timeout_seconds=1):
        with pytest.raises(TimeoutError, match="publication lock"):
            with candidate_publication_lock(
                lock_path, timeout_seconds=0.05, poll_seconds=0.01
            ):
                pass
    assert not lock_path.exists()

    with pytest.raises(RuntimeError, match="inside failure"):
        with candidate_publication_lock(lock_path, timeout_seconds=1):
            raise RuntimeError("inside failure")
    assert not lock_path.exists()


def test_multiprocess_publication_sidecar_matches_one_complete_generation(tmp_path):
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(
            target=_publish_generation_worker,
            args=(str(tmp_path), generation, start_event, queue),
        )
        for generation in ("A", "B")
    ]
    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    assert [queue.get(timeout=2) for _ in processes] == [None, None]

    canonical_path = tmp_path / "candidates.parquet"
    csv_path = tmp_path / "candidates.csv"
    metadata_path = tmp_path / "candidates.metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    canonical = pd.read_parquet(canonical_path)
    review = pd.read_csv(csv_path)
    sha256 = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    assert set(canonical["generation"]) == {metadata["generation"]}
    assert set(review["generation"]) == {metadata["generation"]}
    assert metadata["artifact_sha256"] == {
        "canonical_parquet": sha256(canonical_path),
        "sanitized_csv": sha256(csv_path),
    }
    assert not (tmp_path / ".candidates.publication.lock").exists()
    assert _temporary_residue(tmp_path) == ()
