from pathlib import Path
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pytest

import scripts.tire_ab_entry.extract as extract_module
import scripts.tire_ab_entry.artifacts as artifacts_module
from scripts.tire_ab_entry.extract import atomic_parquet, pending_chunks


def test_atomic_parquet_replaces_target_with_exact_validated_content(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    target = tmp_path / "raw" / "2024Q1.parquet"
    old = pd.DataFrame({"year_quarter": ["OLD"], "value_usd": [1.0]})
    old.to_parquet(target.parent.mkdir(parents=True) or target, index=False)
    expected = pd.DataFrame({"year_quarter": ["2024Q1"], "value_usd": [2.5]})

    digest = atomic_parquet(expected, target)

    pd.testing.assert_frame_equal(pd.read_parquet(target), expected)
    assert digest == extract_module.sha256_file(target)
    assert tuple(target.parent.glob(f".{target.name}.extract-*")) == ()


def test_pending_chunks_skips_only_success_in_deterministic_input_order():
    chunks = ["raw/2024Q2", "raw/2024Q1", "finished/2024Q1"]
    manifest = {
        "chunks": {
            "raw/2024Q2": {"status": "success"},
            "raw/2024Q1": {"status": "failed"},
        }
    }

    assert pending_chunks(chunks, manifest) == ["raw/2024Q1", "finished/2024Q1"]


def output_frame(quarter="2024Q1"):
    frame = pd.DataFrame(
        {
            "manufacturer_parent_id": pd.Series([101], dtype="int64"),
            "year_quarter": pd.Series([quarter], dtype="string"),
            "hs6": pd.Series(["400100"], dtype="string"),
            "shipment_count_nonadditive": pd.Series([1], dtype="int64"),
            "unique_shipment_count_nonadditive": pd.Series([1], dtype="int64"),
            "shipment_equivalent": pd.Series([1.0], dtype="float64"),
            "value_usd": pd.Series([12.0], dtype="float64"),
            "weight_kg": pd.Series([2.0], dtype="float64"),
            "teu": pd.Series([0.5], dtype="float64"),
            "container_count": pd.Series([1.0], dtype="float64"),
        }
    )
    text_columns = {
        "origin_country", "year_quarter", "hs_full_code", "hs6", "input_group",
        "finished_market", "hs_review_status", "description_matched_alias",
        "importer_ownership_source", "shipper_ownership_source", "relationship",
        "import_route", "supplier_relationship",
    }
    for column in extract_module.REQUIRED_OUTPUT_COLUMNS:
        if column in frame:
            continue
        frame[column] = pd.Series(
            ["fixture" if column in text_columns else 0],
            dtype="string" if column in text_columns else "int64",
        )
    return frame.loc[:, list(extract_module.REQUIRED_OUTPUT_COLUMNS)]


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("2014Q1", ("2014-01-01", "2014-04-01")),
        ("2024Q4", ("2024-10-01", "2025-01-01")),
    ],
)
def test_quarter_bounds_are_strict_half_open(label, expected):
    assert extract_module.quarter_bounds(label) == expected


@pytest.mark.parametrize("label", ["2024q1", "2024-Q1", "2024Q0", " 2024Q1", "2024Q5"])
def test_quarter_bounds_reject_invalid_syntax(label):
    with pytest.raises(ValueError, match="quarter"):
        extract_module.quarter_bounds(label)


def test_strict_pending_verifies_file_and_all_contract_hashes(tmp_path):
    target = tmp_path / "2024Q1.parquet"
    frame = output_frame()
    frame.to_parquet(target, index=False)
    expected = {
        "raw/2024Q1": {
            "output_path": str(target),
            "sql_sha256": "sql",
            "parent_seed_sha256": "seed",
            "output_schema_fingerprint": extract_module.output_schema_fingerprint(frame),
            "contract_version": extract_module.CONTRACT_VERSION,
        }
    }
    entry = {
        "status": "success",
        "file_sha256": extract_module.sha256_file(target),
        **expected["raw/2024Q1"],
    }
    entry["code_version"] = extract_module.CODE_VERSION
    entry.update(
        {
                "quarter": "2024Q1",
                "game": "raw",
                "attempt_count": 1,
                "row_count": 1,
            "allocated_value_usd_sum": float(frame["value_usd"].sum()),
            "shipment_equivalent_sum": float(frame["shipment_equivalent"].sum()),
            "unique_shipment_count_nonadditive": 1,
                "nonadditive_count_sum": float(frame["shipment_count_nonadditive"].sum()),
                "started_at": "2026-08-15T00:00:00Z",
                "finished_at": "2026-08-15T00:00:01Z",
            }
    )
    manifest = {
        "manifest_version": extract_module.MANIFEST_VERSION,
        "chunks": {"raw/2024Q1": entry},
    }
    assert pending_chunks(["raw/2024Q1"], manifest, expected=expected) == []

    for field in ("sql_sha256", "parent_seed_sha256", "output_schema_fingerprint", "contract_version"):
        stale = json.loads(json.dumps(manifest))
        stale["chunks"]["raw/2024Q1"][field] = "stale"
        assert pending_chunks(["raw/2024Q1"], stale, expected=expected) == ["raw/2024Q1"]
    target.write_bytes(b"corrupt")
    assert pending_chunks(["raw/2024Q1"], manifest, expected=expected) == ["raw/2024Q1"]


@pytest.mark.parametrize(
    ("field", "stale_value"),
    [
        ("code_version", "old-code"),
        ("row_count", 999),
        ("allocated_value_usd_sum", 999.0),
        ("shipment_equivalent_sum", 999.0),
        ("unique_shipment_count_nonadditive", 999),
        ("nonadditive_count_sum", 999.0),
    ],
)
def test_strict_resume_recomputes_entry_version_counts_and_sums(
    tmp_path, monkeypatch, field, stale_value
):
    monkeypatch.setattr(
        extract_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    kwargs = dict(
        connection=object(),
        game="raw",
        quarter="2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seed",
        output_root=tmp_path,
    )
    extract_module.extract_chunk(
        **kwargs, query_fn=lambda connection, sql: output_frame()
    )
    path = tmp_path / "_manifests" / "raw.json"
    manifest = json.loads(path.read_text())
    manifest["chunks"]["raw/2024Q1"][field] = stale_value
    path.write_text(json.dumps(manifest), encoding="utf-8")
    calls = []
    result = extract_module.extract_chunk(
        **kwargs,
        query_fn=lambda connection, sql: calls.append(1) or output_frame(),
    )
    assert result["status"] == "success"
    assert calls == [1]


def test_strict_resume_rejects_stale_top_level_manifest_version(tmp_path):
    with pytest.raises(extract_module.ManifestContractError, match="version"):
        pending_chunks(
            ["raw/2024Q1"],
            {"manifest_version": "old-version", "chunks": {}},
            expected={"raw/2024Q1": {"output_path": str(tmp_path / "x.parquet")}},
        )


@pytest.mark.parametrize("payload", ["{corrupt", '{"manifest_version": "x"}'])
def test_existing_corrupt_or_malformed_manifest_fails_without_losing_audit_trail(
    tmp_path, monkeypatch, payload
):
    monkeypatch.setattr(
        extract_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    manifest_path = tmp_path / "_manifests" / "raw.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(payload, encoding="utf-8")
    original = manifest_path.read_bytes()
    calls = []
    with pytest.raises(extract_module.ManifestContractError):
        extract_module.extract_chunk(
            object(),
            "raw",
            "2024Q2",
            {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
            parent_seed_sha256="seed",
            output_root=tmp_path,
            query_fn=lambda connection, sql: calls.append(1) or output_frame("2024Q2"),
        )
    assert calls == []
    assert manifest_path.read_bytes() == original


@pytest.mark.parametrize("mutation", ["extra_column", "wrong_quarter", "duplicate_key"])
def test_strict_resume_reloads_and_revalidates_exact_parquet_contract(
    tmp_path, monkeypatch, mutation
):
    monkeypatch.setattr(
        extract_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    kwargs = dict(
        connection=object(),
        game="raw",
        quarter="2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seed",
        output_root=tmp_path,
    )
    extract_module.extract_chunk(
        **kwargs, query_fn=lambda connection, sql: output_frame()
    )
    target = tmp_path / "_chunks" / "raw" / "2024Q1.parquet"
    frame = pd.read_parquet(target)
    if mutation == "extra_column":
        frame["unexpected"] = 1
    elif mutation == "wrong_quarter":
        frame["year_quarter"] = "2024Q2"
    else:
        frame = pd.concat([frame, frame], ignore_index=True)
    frame.to_parquet(target, index=False)
    manifest_path = tmp_path / "_manifests" / "raw.json"
    manifest = json.loads(manifest_path.read_text())
    entry = manifest["chunks"]["raw/2024Q1"]
    entry["file_sha256"] = extract_module.sha256_file(target)
    entry["output_schema_fingerprint"] = extract_module.output_schema_fingerprint(frame)
    entry["row_count"] = len(frame)
    entry["allocated_value_usd_sum"] = float(frame["value_usd"].sum())
    entry["shipment_equivalent_sum"] = float(frame["shipment_equivalent"].sum())
    entry["unique_shipment_count_nonadditive"] = (
        0 if frame.empty else int(frame["unique_shipment_count_nonadditive"].iat[0])
    )
    entry["nonadditive_count_sum"] = float(frame["shipment_count_nonadditive"].sum())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    calls = []
    result = extract_module.extract_chunk(
        **kwargs,
        query_fn=lambda connection, sql: calls.append(1) or output_frame(),
    )
    assert result["status"] == "success"
    assert calls == [1]


def test_validate_output_accepts_empty_schema_and_rejects_bad_values():
    valid = output_frame()
    extract_module.validate_output_frame(valid, "2024Q1")
    empty = valid.iloc[0:0]
    extract_module.validate_output_frame(empty, "2024Q1")
    assert extract_module._manifest_metrics(empty)[
        "unique_shipment_count_nonadditive"
    ] == 0

    for column, value in (("value_usd", -1), ("shipment_equivalent", np.inf)):
        bad = valid.copy()
        bad.loc[0, column] = value
        with pytest.raises(ValueError, match="nonnegative|finite"):
            extract_module.validate_output_frame(bad, "2024Q1")
    with pytest.raises(ValueError, match="duplicate"):
        extract_module.validate_output_frame(pd.concat([valid, valid]), "2024Q1")
    with pytest.raises(ValueError, match="quarter"):
        extract_module.validate_output_frame(valid.assign(year_quarter="2024Q2"), "2024Q1")
    inconsistent_unique = pd.concat(
        [
            valid,
            valid.assign(
                hs_full_code="fixture-other",
                unique_shipment_count_nonadditive=2,
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(ValueError, match="chunk-global unique shipment"):
        extract_module.validate_output_frame(inconsistent_unique, "2024Q1")


def test_output_contract_requires_route_and_all_diagnostics():
    valid = output_frame().assign(import_route="fixture", main_ineligible_value_usd=0.0)
    with pytest.raises(ValueError, match="required columns"):
        extract_module.validate_output_frame(valid.drop(columns="import_route"), "2024Q1")
    with pytest.raises(ValueError, match="required columns"):
        extract_module.validate_output_frame(
            valid.drop(columns="main_ineligible_value_usd"), "2024Q1"
        )


class TransientNetworkError(ConnectionError):
    pass


class WrappedOperationalError(Exception):
    def __init__(self, message, *, sqlstate=None, errno=None):
        super().__init__(message)
        self.sqlstate = sqlstate
        self.errno = errno


def test_query_with_retry_uses_three_total_attempts_and_exponential_waits():
    calls = []
    waits = []

    def query():
        calls.append("query")
        if len(calls) < 3:
            raise TransientNetworkError("socket reset password=secret")
        return output_frame()

    result, attempts = extract_module.query_with_retry(query, sleep_fn=waits.append)
    assert len(result) == 1
    assert attempts == 3
    assert calls == ["query"] * 3
    assert waits == [1.0, 2.0]


def test_query_with_retry_never_retries_contract_or_auth_failures():
    for error in (ValueError("schema mismatch"), RuntimeError("authentication failed password=x")):
        calls = []
        with pytest.raises(type(error)):
            extract_module.query_with_retry(
                lambda: calls.append(1) or (_ for _ in ()).throw(error),
                sleep_fn=lambda seconds: pytest.fail("must not sleep"),
            )
        assert calls == [1]


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (WrappedOperationalError("SQL compilation error near secret_table"), "contract"),
        (WrappedOperationalError("invalid identifier 'SECRET_COLUMN'"), "contract"),
        (
            WrappedOperationalError(
                "incorrect username or password=do-not-print", sqlstate="08001"
            ),
            "authentication",
        ),
        (
            WrappedOperationalError(
                "insufficient privileges for role SECRET_ROLE", sqlstate="42501"
            ),
            "authorization",
        ),
        (WrappedOperationalError("masked auth failure", sqlstate="28000"), "authentication"),
        (WrappedOperationalError("masked login failure", errno="390100"), "authentication"),
        (WrappedOperationalError("masked compiler failure", errno="1003"), "contract"),
        (WrappedOperationalError("unclassified operational failure"), "operation"),
    ],
)
def test_wrapped_operational_contract_and_access_errors_never_retry(
    error, category, capsys
):
    calls = []
    waits = []
    with pytest.raises(WrappedOperationalError):
        extract_module.query_with_retry(
            lambda: calls.append(1) or (_ for _ in ()).throw(error),
            sleep_fn=waits.append,
        )
    assert extract_module.classify_error(error) == category
    assert calls == [1]
    assert waits == []
    assert "SECRET" not in capsys.readouterr().out
    assert "do-not-print" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "error",
    [
        WrappedOperationalError("network socket read timed out", sqlstate="08006"),
        WrappedOperationalError("connection reset by peer", errno=250003),
        WrappedOperationalError("masked network request failure", errno="250003"),
    ],
)
def test_explicit_wrapped_network_failures_retry(error):
    calls = []
    waits = []

    def query():
        calls.append(1)
        if len(calls) < 3:
            raise error
        return output_frame()

    _, attempts = extract_module.query_with_retry(query, sleep_fn=waits.append)
    assert attempts == 3
    assert waits == [1.0, 2.0]


def test_sql_hash_binds_keyed_mapping_dates_and_description_identity():
    base = extract_module.sql_contract_sha256(
        "select 1", {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
        "2024-01-01", "2024-04-01", None,
    )
    changed = extract_module.sql_contract_sha256(
        "select 1", {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 4},
        "2024-01-01", "2024-04-01", None,
    )
    assert base != changed


def test_atomic_parquet_failure_keeps_old_target_and_removes_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    target = tmp_path / "chunk.parquet"
    old = output_frame("2023Q4")
    old.to_parquet(target, index=False)
    old_bytes = target.read_bytes()
    monkeypatch.setattr(
        extract_module.pd,
        "read_parquet",
        lambda path: (_ for _ in ()).throw(RuntimeError("validation failed")),
    )
    with pytest.raises(RuntimeError, match="validation"):
        atomic_parquet(output_frame(), target)
    assert target.read_bytes() == old_bytes
    assert tuple(tmp_path.glob(".chunk.parquet.extract-*")) == ()


def reviewed_seed_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts_module, "validate_output_path", lambda path: Path(path).resolve())
    frame = pd.DataFrame(
        {
            "target_search_term": ["Michelin", "Goodyear", "Hankook Tire & Technology"],
            "current_ultimate_parent_id": [101, 202, 303],
        }
    )
    paths = {
        "canonical_path": tmp_path / "manufacturer_parent_candidates.parquet",
        "csv_path": tmp_path / "manufacturer_parent_candidates.csv",
        "metadata_path": tmp_path / "manufacturer_parent_candidates.metadata.json",
    }
    published = artifacts_module.publish_candidate_artifact_set(
        frame, frame, {"candidate_counts": {"Michelin": 1}}, **paths
    )
    pointer = Path(published["current_manifest_path"])
    rows = []
    for key, parent_id in zip(("MICHELIN", "GOODYEAR", "HANKOOK"), (101, 202, 303)):
        rows.append(
            {
                "manufacturer_key": key,
                "manufacturer_parent_id": parent_id,
                "review_status": "reviewed",
                "reviewer": "unit-reviewer",
                "reviewed_at_utc": "2026-08-15T01:02:03Z",
                "source_candidate_generation_id": published["generation_id"],
                "source_candidate_pointer_sha256": extract_module.sha256_file(pointer),
                "source_candidate_metadata_sha256": published["metadata_sha256"],
            }
        )
    seed_path = tmp_path / "manufacturer_parent_seed.csv"
    pd.DataFrame(rows).to_csv(seed_path, index=False)
    return seed_path, pointer


def test_parent_seed_gate_returns_exact_keyed_mapping_and_provenance(tmp_path, monkeypatch):
    seed_path, pointer = reviewed_seed_fixture(tmp_path, monkeypatch)
    snapshot = extract_module.load_parent_seed(seed_path, candidate_pointer_path=pointer)
    assert snapshot.parent_ids == {
        "MICHELIN": 101,
        "GOODYEAR": 202,
        "HANKOOK": 303,
    }
    assert snapshot.sha256 == hashlib.sha256(seed_path.read_bytes()).hexdigest()


@pytest.mark.parametrize("mode", ["missing", "extra", "duplicate", "null", "unreviewed", "bad_time"])
def test_parent_seed_gate_rejects_all_row_and_review_failures(tmp_path, monkeypatch, mode):
    seed_path, pointer = reviewed_seed_fixture(tmp_path, monkeypatch)
    frame = pd.read_csv(seed_path)
    if mode == "missing":
        frame = frame.iloc[:2]
    elif mode == "extra":
        frame.loc[len(frame)] = frame.iloc[0].assign if False else frame.iloc[0]
        frame.loc[len(frame) - 1, "manufacturer_key"] = "OTHER"
    elif mode == "duplicate":
        frame.loc[1, "manufacturer_key"] = "MICHELIN"
    elif mode == "null":
        frame.loc[0, "manufacturer_parent_id"] = None
    elif mode == "unreviewed":
        frame.loc[0, "review_status"] = "pending"
    else:
        frame.loc[0, "reviewed_at_utc"] = "2026-08-15 01:02:03"
    frame.to_csv(seed_path, index=False)
    with pytest.raises(ValueError, match="seed"):
        extract_module.load_parent_seed(seed_path, candidate_pointer_path=pointer)


def test_parent_seed_gate_rejects_stale_candidate_pointer_provenance(tmp_path, monkeypatch):
    seed_path, pointer = reviewed_seed_fixture(tmp_path, monkeypatch)
    frame = pd.read_csv(seed_path)
    frame["source_candidate_pointer_sha256"] = "0" * 64
    frame.to_csv(seed_path, index=False)
    with pytest.raises(ValueError, match="provenance"):
        extract_module.load_parent_seed(seed_path, candidate_pointer_path=pointer)


def test_parent_seed_gate_rejects_id_not_in_keyed_source_candidates(tmp_path, monkeypatch):
    seed_path, pointer = reviewed_seed_fixture(tmp_path, monkeypatch)
    frame = pd.read_csv(seed_path)
    frame.loc[frame["manufacturer_key"].eq("GOODYEAR"), "manufacturer_parent_id"] = 999
    frame.to_csv(seed_path, index=False)
    with pytest.raises(ValueError, match="candidate"):
        extract_module.load_parent_seed(seed_path, candidate_pointer_path=pointer)


def test_parent_seed_uses_one_stable_loaded_pointer_snapshot_during_publication_race(
    tmp_path, monkeypatch
):
    seed_path, pointer_path = reviewed_seed_fixture(tmp_path, monkeypatch)
    old_pointer_hash = extract_module.sha256_file(pointer_path)
    real_resolver = artifacts_module.resolve_candidate_generation_manifest
    real_read_bytes = Path.read_bytes
    pointer_reads = []
    switched = False

    def counted_read_bytes(path):
        if Path(path).resolve() == pointer_path.resolve():
            pointer_reads.append(1)
        return real_read_bytes(path)

    def race_after_loaded_resolution(path, loaded_pointer):
        nonlocal switched
        resolved = real_resolver(path, loaded_pointer)
        if not switched:
            switched = True
            replacement = pd.DataFrame(
                {
                    "target_search_term": [
                        "Michelin",
                        "Goodyear",
                        "Hankook Tire & Technology",
                    ],
                    "current_ultimate_parent_id": [111, 222, 333],
                }
            )
            artifacts_module.publish_candidate_artifact_set(
                replacement,
                replacement,
                {"candidate_counts": {"Michelin": 1}},
                canonical_path=tmp_path / "manufacturer_parent_candidates.parquet",
                csv_path=tmp_path / "manufacturer_parent_candidates.csv",
                metadata_path=tmp_path / "manufacturer_parent_candidates.metadata.json",
            )
        return resolved

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)
    monkeypatch.setattr(
        artifacts_module,
        "resolve_candidate_generation_manifest",
        race_after_loaded_resolution,
    )

    snapshot = extract_module.load_parent_seed(
        seed_path, candidate_pointer_path=pointer_path
    )
    assert snapshot.parent_ids == {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303}
    assert pointer_reads == [1]
    assert extract_module.sha256_file(pointer_path) != old_pointer_hash


def test_parent_seed_mapping_and_hash_come_from_one_csv_byte_snapshot(
    tmp_path, monkeypatch
):
    seed_path, pointer_path = reviewed_seed_fixture(tmp_path, monkeypatch)
    old_bytes = seed_path.read_bytes()
    old_hash = hashlib.sha256(old_bytes).hexdigest()
    replacement = pd.read_csv(seed_path)
    replacement["manufacturer_parent_id"] = [999, 888, 777]
    replacement_path = tmp_path / "replacement-seed.csv"
    replacement.to_csv(replacement_path, index=False)
    real_read_bytes = Path.read_bytes
    reads = []

    def swap_after_snapshot(path):
        payload = real_read_bytes(path)
        if Path(path).resolve() == seed_path.resolve():
            reads.append(1)
            os.replace(replacement_path, seed_path)
        return payload

    monkeypatch.setattr(Path, "read_bytes", swap_after_snapshot)
    snapshot = extract_module.load_parent_seed(
        seed_path, candidate_pointer_path=pointer_path
    )

    assert reads == [1]
    assert snapshot.parent_ids == {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303}
    assert snapshot.sha256 == old_hash
    assert extract_module.sha256_file(seed_path) != old_hash


def test_runtime_seed_and_chunk_paths_are_canonical():
    assert extract_module.PARENT_SEED_PATH == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\review\manufacturer_parent_seed.csv"
    )
    assert extract_module.chunk_path("raw", "2024Q1") == Path(
        r"C:\panjiva\data\staging\ab_entry_tire_v1\_chunks\raw\2024Q1.parquet"
    )


def test_extract_chunk_writes_verified_file_and_complete_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    frame = output_frame()
    calls = []
    result = extract_module.extract_chunk(
        object(),
        "raw",
        "2024Q1",
        {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seedhash",
        output_root=tmp_path,
        query_fn=lambda connection, sql: calls.append(sql) or frame,
        sleep_fn=lambda seconds: pytest.fail("unexpected retry"),
    )
    target = tmp_path / "_chunks" / "raw" / "2024Q1.parquet"
    manifest = json.loads((tmp_path / "_manifests" / "raw.json").read_text())
    entry = manifest["chunks"]["raw/2024Q1"]
    assert len(calls) == 1
    assert result["status"] == "success"
    assert entry["status"] == "success"
    assert entry["attempt_count"] == 1
    assert entry["row_count"] == 1
    assert entry["allocated_value_usd_sum"] == 12.0
    assert entry["shipment_equivalent_sum"] == 1.0
    assert entry["unique_shipment_count_nonadditive"] == 1
    assert entry["nonadditive_count_sum"] == 1.0
    assert entry["file_sha256"] == extract_module.sha256_file(target)
    assert entry["parent_seed_sha256"] == "seedhash"
    assert entry["sql_sha256"]
    assert entry["output_schema_fingerprint"] == extract_module.output_schema_fingerprint(frame)
    assert entry["contract_version"] == extract_module.CONTRACT_VERSION
    assert entry["started_at"].endswith("Z") and entry["finished_at"].endswith("Z")


def test_extract_chunk_resume_skips_only_strict_verified_success(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    kwargs = dict(
        connection=object(), game="finished", quarter="2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seedhash", output_root=tmp_path,
    )
    extract_module.extract_chunk(**kwargs, query_fn=lambda connection, sql: output_frame())
    result = extract_module.extract_chunk(
        **kwargs,
        query_fn=lambda connection, sql: pytest.fail("verified chunk must be skipped"),
    )
    assert result["status"] == "skipped"
    target = tmp_path / "_chunks" / "finished" / "2024Q1.parquet"
    target.write_bytes(b"corrupt")
    extract_module.extract_chunk(**kwargs, query_fn=lambda connection, sql: output_frame())
    assert pd.read_parquet(target).iloc[0]["year_quarter"] == "2024Q1"


def test_extract_chunk_records_redacted_nonretryable_failure_without_file(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    calls = []
    with pytest.raises(extract_module.ChunkExtractionError):
        extract_module.extract_chunk(
            object(), "raw", "2024Q1",
            {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
            parent_seed_sha256="seedhash", output_root=tmp_path,
            query_fn=lambda connection, sql: calls.append(1) or (_ for _ in ()).throw(
                RuntimeError("SQL compilation error password=do-not-record")
            ),
            sleep_fn=lambda seconds: pytest.fail("must not retry compile errors"),
        )
    manifest_text = (tmp_path / "_manifests" / "raw.json").read_text()
    assert "do-not-record" not in manifest_text
    entry = json.loads(manifest_text)["chunks"]["raw/2024Q1"]
    assert calls == [1]
    assert entry["status"] == "failed"
    assert entry["attempt_count"] == 1
    assert entry["error_category"] == "contract"
    assert not (tmp_path / "_chunks" / "raw" / "2024Q1.parquet").exists()


def test_failed_replacement_does_not_leave_corrupt_old_success_current(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    kwargs = dict(
        connection=object(), game="raw", quarter="2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seed", output_root=tmp_path,
    )
    extract_module.extract_chunk(**kwargs, query_fn=lambda connection, sql: output_frame())
    target = tmp_path / "_chunks" / "raw" / "2024Q1.parquet"
    target.write_bytes(b"corrupt")
    with pytest.raises(extract_module.ChunkExtractionError):
        extract_module.extract_chunk(
            **kwargs,
            query_fn=lambda connection, sql: (_ for _ in ()).throw(ValueError("bad schema")),
        )
    manifest = json.loads((tmp_path / "_manifests" / "raw.json").read_text())
    assert manifest["chunks"]["raw/2024Q1"]["status"] == "failed"


class CursorForClose:
    def __init__(self, frame, close_error=None):
        self.frame = frame
        self.close_error = close_error
        self.closed = False
    def execute(self, sql):
        return self
    def fetch_pandas_all(self):
        return self.frame.copy()
    def close(self):
        self.closed = True
        if self.close_error:
            raise self.close_error


class ConnectionForClose:
    def __init__(self, cursor):
        self.value = cursor
    def cursor(self):
        return self.value


def test_default_query_closes_cursor_on_success_and_failure():
    cursor = CursorForClose(output_frame())
    result = extract_module.execute_dataframe_query(ConnectionForClose(cursor), "select 1")
    assert len(result) == 1 and cursor.closed
    cursor = CursorForClose(output_frame(), RuntimeError("close secret password=x"))
    with pytest.raises(RuntimeError, match="cursor cleanup failed"):
        extract_module.execute_dataframe_query(ConnectionForClose(cursor), "select 1")
    assert cursor.closed


def test_description_identity_comes_from_persisted_full_schema_identity(tmp_path):
    path = tmp_path / "schema.parquet"
    pd.DataFrame(
        {
            "table_catalog": ["MI_XPRESSCLOUD"],
            "table_schema": ["XPRESSFEED"],
            "table_name": ["PANJIVAUSIMPORT"],
            "column_name": ["GOODSDESCRIPTION"],
        }
    ).to_parquet(path, index=False)
    identity = extract_module.load_description_identity(path)
    assert (identity.catalog, identity.schema, identity.table, identity.column) == (
        "MI_XPRESSCLOUD", "XPRESSFEED", "PANJIVAUSIMPORT", "GOODSDESCRIPTION"
    )


def test_validation_quarter_writes_only_below_validation_generation(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    results = extract_module.validate_quarter(
        object(), "2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seed", run_id="unit-run", output_root=tmp_path,
        query_fn=lambda connection, sql: output_frame(),
    )
    validation = tmp_path / "_validation" / "unit-run"
    assert results["validation_root"] == str(validation)
    assert (validation / "_chunks" / "raw" / "2024Q1.parquet").exists()
    assert (validation / "_chunks" / "finished" / "2024Q1.parquet").exists()
    assert not (tmp_path / "_chunks").exists()
    assert not (tmp_path / "_manifests").exists()


def test_concurrent_same_chunk_queries_once_and_cannot_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    calls = []
    kwargs = dict(
        connection=object(), game="raw", quarter="2024Q1",
        parent_ids={"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303},
        parent_seed_sha256="seed", output_root=tmp_path,
        query_fn=lambda connection, sql: calls.append(sql) or output_frame(),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda ignored: extract_module.extract_chunk(**kwargs), range(2)))
    assert len(calls) == 1
    assert sorted(item["status"] for item in results) == ["skipped", "success"]
    saved = pd.read_parquet(tmp_path / "_chunks" / "raw" / "2024Q1.parquet")
    assert len(saved) == 1


def test_atomic_manifest_replacement_failure_preserves_previous_manifest(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    original = {"manifest_version": extract_module.MANIFEST_VERSION, "chunks": {"old": {"status": "success"}}}
    extract_module._atomic_json(original, path)
    old_bytes = path.read_bytes()
    monkeypatch.setattr(extract_module.os, "replace", lambda source, target: (_ for _ in ()).throw(OSError("crash")))
    with pytest.raises(OSError, match="crash"):
        extract_module._atomic_json({"chunks": {"new": {"status": "success"}}}, path)
    assert path.read_bytes() == old_bytes
    assert tuple(tmp_path.glob(".manifest.json.extract-*")) == ()


def test_extract_full_orders_raw_then_finished_and_quarters_ascending(monkeypatch):
    calls = []
    monkeypatch.setattr(extract_module, "iter_quarters", lambda: iter(("2014Q1", "2014Q2")))
    monkeypatch.setattr(
        extract_module,
        "extract_chunk",
        lambda connection, game, quarter, parent_ids, **kwargs: calls.append((game, quarter))
        or {"status": "success"},
    )
    summary = extract_module.extract_full(
        object(), game="both",
        parent_ids={"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
        parent_seed_sha256="seed",
    )
    assert calls == [
        ("raw", "2014Q1"), ("raw", "2014Q2"),
        ("finished", "2014Q1"), ("finished", "2014Q2"),
    ]
    assert summary["completed"] == 4
