"""Atomic, resumable extraction for the licensed tire AB-entry package."""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from datetime import date
from datetime import datetime, timezone
from contextlib import contextmanager
import threading
from typing import Callable, Iterable, Mapping

import pandas as pd

from . import artifacts
from .config import (
    MANUFACTURER_KEYS,
    MANUFACTURER_PARENT_TARGETS,
    OUTPUT_ROOT,
    iter_quarters,
    validate_output_path,
)
from .sql import DescriptionIdentity, build_finished_sql, build_raw_sql
from .schema_probe import SCHEMA_OUTPUT_PATH, choose_description_column

try:
    import msvcrt
except ImportError:  # pragma: no cover - licensed writes are Windows-only
    msvcrt = None


CONTRACT_VERSION = "tire-extraction-contract-v1"
MANIFEST_VERSION = "tire-extraction-manifest-v1"
CODE_VERSION = "tire-extraction-code-v1"
PARENT_SEED_PATH = OUTPUT_ROOT / "review" / "manufacturer_parent_seed.csv"
PARENT_CANDIDATE_POINTER_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.current.json"
)
SEED_COLUMNS = (
    "manufacturer_key",
    "manufacturer_parent_id",
    "review_status",
    "reviewer",
    "reviewed_at_utc",
    "source_candidate_generation_id",
    "source_candidate_pointer_sha256",
    "source_candidate_metadata_sha256",
)
ADDITIVE_MEASURES = (
    "shipment_equivalent",
    "value_usd",
    "weight_kg",
    "teu",
    "container_count",
)
OUTPUT_KEY_COLUMNS = (
    "manufacturer_parent_id",
    "importer_companyid",
    "importer_up",
    "shipper_panjiva_id",
    "shipper_companyid",
    "shipper_up",
    "origin_country",
    "year_quarter",
    "hs_full_code",
    "hs6",
    "input_group",
    "finished_market",
    "hs_review_status",
    "hs_eligible",
    "distinct_full_code_count",
    "reviewed_full_code_count",
    "unreviewed_full_code_count",
    "mixed_review",
    "description_candidate_parent_id",
    "description_match_count",
    "description_ambiguous",
    "description_matched_alias",
    "description_alias_count",
    "importer_xref_distinct_candidate_count",
    "shipper_xref_distinct_candidate_count",
    "importer_xref_ambiguous",
    "shipper_xref_ambiguous",
    "importer_pit_distinct_parent_candidate_count",
    "shipper_pit_distinct_parent_candidate_count",
    "importer_pit_distinct_interval_match_count",
    "shipper_pit_distinct_interval_match_count",
    "importer_pit_ambiguous",
    "shipper_pit_ambiguous",
    "importer_pit_same_parent_overlap",
    "shipper_pit_same_parent_overlap",
    "importer_ownership_source",
    "shipper_ownership_source",
    "importer_historical_backcast",
    "shipper_historical_backcast",
    "relationship",
    "import_route",
    "supplier_relationship",
    "sensitivity_eligible",
    "estimation_eligible",
)
DIAGNOSTIC_NAMES = (
    "manufacturer_conflict",
    "importer_xref_unmatched",
    "shipper_xref_unmatched",
    "importer_xref_ambiguous",
    "shipper_xref_ambiguous",
    "importer_pit_ambiguous",
    "shipper_pit_ambiguous",
    "importer_pit_same_parent_overlap",
    "shipper_pit_same_parent_overlap",
    "importer_current_parent_fallback",
    "shipper_current_parent_fallback",
    "importer_self_fallback",
    "shipper_self_fallback",
    "importer_historical_backcast",
    "shipper_historical_backcast",
    "description_candidate",
    "description_ambiguous",
    "unattributed",
    "hs_review",
    "main_ineligible",
)
DIAGNOSTIC_MEASURES = tuple(
    f"{name}_{suffix}"
    for name in DIAGNOSTIC_NAMES
    for suffix in ("shipment_count_nonadditive", "shipment_equivalent", "value_usd")
)
REQUIRED_OUTPUT_COLUMNS = (
    *OUTPUT_KEY_COLUMNS,
    "shipment_count_nonadditive",
    "shipment_count_compatibility_nonadditive",
    *ADDITIVE_MEASURES,
    "manufacturer_conflict",
    "description_candidate",
    *DIAGNOSTIC_MEASURES,
)
TEXT_OUTPUT_COLUMNS = {
    "origin_country",
    "year_quarter",
    "hs_full_code",
    "hs6",
    "input_group",
    "finished_market",
    "hs_review_status",
    "description_matched_alias",
    "importer_ownership_source",
    "shipper_ownership_source",
    "relationship",
    "import_route",
    "supplier_relationship",
}


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_parquet(frame: pd.DataFrame, target: Path | str) -> str:
    """Write, fsync, exactly validate, and atomically replace one Parquet file."""

    destination = validate_output_path(target)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = validate_output_path(destination)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.extract-",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    os.close(fd)
    try:
        frame.to_parquet(temporary, index=False)
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        restored = pd.read_parquet(temporary)
        try:
            pd.testing.assert_frame_equal(
                restored.reset_index(drop=True),
                frame.reset_index(drop=True),
                check_dtype=True,
                check_like=False,
            )
        except AssertionError as error:
            raise RuntimeError("Parquet exact-content validation failed") from error
        digest = sha256_file(temporary)
        os.replace(validate_output_path(temporary), validate_output_path(destination))
        if sha256_file(destination) != digest:
            raise RuntimeError("Parquet replacement checksum validation failed")
        return digest
    finally:
        temporary.unlink(missing_ok=True)


def output_schema_fingerprint(frame: pd.DataFrame) -> str:
    contract = [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    return hashlib.sha256(
        json.dumps(contract, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()


def quarter_bounds(year_quarter: str) -> tuple[str, str]:
    match = re.fullmatch(r"([0-9]{4})Q([1-4])", year_quarter or "")
    if match is None:
        raise ValueError("quarter must use strict YYYYQ[1-4] syntax")
    year = int(match.group(1))
    quarter = int(match.group(2))
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    end = date(year + (quarter == 4), 1 if quarter == 4 else start_month + 3, 1)
    return start.isoformat(), end.isoformat()


def validate_game(game: str) -> str:
    if game not in {"raw", "finished"}:
        raise ValueError("game must be raw or finished")
    return game


def chunk_path(
    game: str,
    quarter: str,
    *,
    output_root: Path | str = OUTPUT_ROOT,
) -> Path:
    validate_game(game)
    quarter_bounds(quarter)
    target = Path(output_root) / "_chunks" / game / f"{quarter}.parquet"
    return validate_output_path(target) if Path(output_root) == OUTPUT_ROOT else target.resolve()


def manifest_path(game: str, *, output_root: Path | str = OUTPUT_ROOT) -> Path:
    validate_game(game)
    target = Path(output_root) / "_manifests" / f"{game}.json"
    return validate_output_path(target) if Path(output_root) == OUTPUT_ROOT else target.resolve()


def load_parent_seed(
    seed_path: Path | str = PARENT_SEED_PATH,
    *,
    candidate_pointer_path: Path | str = PARENT_CANDIDATE_POINTER_PATH,
) -> dict[str, int]:
    """Validate the human-reviewed seed and its current candidate provenance."""

    seed = Path(seed_path)
    pointer_path = Path(candidate_pointer_path)
    try:
        frame = pd.read_csv(seed, dtype={"manufacturer_key": "string"})
    except Exception as error:
        raise ValueError("reviewed parent seed is missing or unreadable") from error
    missing_columns = set(SEED_COLUMNS).difference(frame.columns)
    if missing_columns:
        raise ValueError(f"parent seed is missing columns: {sorted(missing_columns)}")
    if len(frame) != len(MANUFACTURER_KEYS):
        raise ValueError("parent seed must have exactly three rows")
    keys = frame["manufacturer_key"].astype("string")
    if keys.isna().any() or set(keys) != set(MANUFACTURER_KEYS) or keys.duplicated().any():
        raise ValueError("parent seed must contain each approved manufacturer key exactly once")
    numbers = pd.to_numeric(frame["manufacturer_parent_id"], errors="coerce")
    if (
        numbers.isna().any()
        or not numbers.map(lambda value: float(value).is_integer()).all()
        or (numbers <= 0).any()
        or numbers.duplicated().any()
    ):
        raise ValueError("parent seed IDs must be unique positive integral values")
    if not frame["review_status"].astype("string").eq("reviewed").all():
        raise ValueError("parent seed rows must have reviewed status")
    reviewers = frame["reviewer"].astype("string").str.strip()
    if reviewers.isna().any() or reviewers.eq("").any():
        raise ValueError("parent seed reviewer is required")
    timestamps = frame["reviewed_at_utc"].astype("string")
    strict_utc = timestamps.str.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z"
    )
    parsed = pd.to_datetime(timestamps, utc=True, errors="coerce")
    if not strict_utc.all() or parsed.isna().any():
        raise ValueError("parent seed reviewed_at_utc must be a valid UTC timestamp")
    try:
        generation = artifacts.resolve_current_candidate_generation(pointer_path)
        pointer_hash = sha256_file(pointer_path)
    except Exception as error:
        raise ValueError("parent seed candidate provenance is unreadable") from error
    provenance = {
        "source_candidate_generation_id": generation["generation_id"],
        "source_candidate_pointer_sha256": pointer_hash,
        "source_candidate_metadata_sha256": generation["metadata_sha256"],
    }
    for column, expected_value in provenance.items():
        values = frame[column].astype("string")
        if values.isna().any() or not values.eq(str(expected_value)).all():
            raise ValueError("parent seed candidate provenance is stale or inconsistent")
    keyed = dict(zip(keys, numbers.astype("int64"), strict=True))
    try:
        candidates = pd.read_parquet(generation["canonical_path"])
        required_candidates = {"target_search_term", "current_ultimate_parent_id"}
        if not required_candidates.issubset(candidates.columns):
            raise ValueError
        candidate_ids = pd.to_numeric(
            candidates["current_ultimate_parent_id"], errors="coerce"
        )
    except Exception as error:
        raise ValueError("parent seed source candidate contract is invalid") from error
    for key, target_name in zip(
        MANUFACTURER_KEYS, MANUFACTURER_PARENT_TARGETS, strict=True
    ):
        available = set(
            candidate_ids.loc[candidates["target_search_term"].eq(target_name)]
            .dropna()
            .astype("int64")
        )
        if int(keyed[key]) not in available:
            raise ValueError("parent seed selection is absent from its keyed candidates")
    return {key: int(keyed[key]) for key in MANUFACTURER_KEYS}


def validate_output_frame(frame: pd.DataFrame, quarter: str) -> pd.DataFrame:
    """Fail closed on the additive result contract while allowing typed empties."""

    quarter_bounds(quarter)
    if frame.columns.duplicated().any():
        raise ValueError("output has duplicate columns")
    missing = set(REQUIRED_OUTPUT_COLUMNS).difference(frame.columns)
    if missing:
        raise ValueError(f"output schema is missing required columns: {sorted(missing)}")
    if not frame.empty and not frame["year_quarter"].astype("string").eq(quarter).all():
        raise ValueError("output quarter does not match requested quarter")
    numeric_columns = [
        column
        for column in frame.columns
        if pd.api.types.is_numeric_dtype(frame[column].dtype)
    ]
    required_numeric = set(REQUIRED_OUTPUT_COLUMNS).difference(TEXT_OUTPUT_COLUMNS)
    if not required_numeric.issubset(numeric_columns):
        raise ValueError("output additive and count columns must be numeric")
    for column in numeric_columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        non_null = values.dropna().astype("float64")
        if not np_isfinite(non_null):
            raise ValueError(f"output numeric column {column} must be finite")
        if (non_null < 0).any():
            raise ValueError(f"output numeric column {column} must be nonnegative")
    if frame.duplicated(list(OUTPUT_KEY_COLUMNS)).any():
        raise ValueError("output has duplicate final keys")
    return frame


def np_isfinite(values: pd.Series) -> bool:
    return all(math.isfinite(float(value)) for value in values)


def sql_contract_sha256(
    sql: str,
    parent_ids: Mapping[str, int],
    date_start: str,
    date_end: str,
    description_identity,
) -> str:
    description = None
    if description_identity is not None:
        description = {
            field: getattr(description_identity, field)
            for field in ("catalog", "schema", "table", "column")
        }
    payload = {
        "sql": sql,
        "parent_ids": sorted((str(key), int(value)) for key, value in parent_ids.items()),
        "date_start": date_start,
        "date_end": date_end,
        "description_identity": description,
        "contract_version": CONTRACT_VERSION,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def classify_error(error: Exception) -> str:
    message = str(error).casefold()
    name = type(error).__name__.casefold()
    if isinstance(error, (ValueError, TypeError)) or "programmingerror" in name:
        return "contract"
    if any(
        token in message
        for token in (
            "authentication failed",
            "incorrect password",
            "invalid credential",
            "not authorized",
        )
    ):
        return "authentication"
    sqlstate = str(getattr(error, "sqlstate", "") or "")
    if isinstance(error, (ConnectionError, TimeoutError)) or sqlstate.startswith("08"):
        return "transient"
    if any(token in name for token in ("operationalerror", "network", "timeout")):
        return "transient"
    if any(
        token in message
        for token in ("connection reset", "connection aborted", "timed out", "service unavailable")
    ):
        return "transient"
    if any(token in message for token in ("syntax error", "sql compilation", "schema mismatch")):
        return "contract"
    return "operation"


def query_with_retry(
    query: Callable[[], pd.DataFrame],
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> tuple[pd.DataFrame, int]:
    for attempt in range(1, 4):
        try:
            return query(), attempt
        except Exception as error:
            if classify_error(error) != "transient" or attempt == 3:
                raise
            sleep_fn(min(30.0, float(2 ** (attempt - 1))))
    raise AssertionError("unreachable")


def pending_chunks(
    chunks: Iterable[str],
    manifest: dict,
    *,
    expected: Mapping[str, Mapping[str, object]] | None = None,
) -> list[str]:
    """Return input-ordered chunks that do not have a recorded success."""

    entries = manifest.get("chunks", {}) if isinstance(manifest, dict) else {}
    pending = []
    for chunk in chunks:
        entry = entries.get(chunk)
        verified = isinstance(entry, dict) and entry.get("status") == "success"
        if verified and expected is not None:
            contract = expected.get(chunk)
            verified = isinstance(contract, Mapping)
            if verified:
                for field in (
                    "sql_sha256",
                    "parent_seed_sha256",
                    "output_schema_fingerprint",
                    "contract_version",
                    "output_path",
                ):
                    if field in contract and contract.get(field) is not None:
                        verified = verified and entry.get(field) == contract.get(field)
                path = Path(str(contract.get("output_path", "")))
                try:
                    verified = bool(
                        verified
                        and path.is_file()
                        and entry.get("file_sha256") == sha256_file(path)
                    )
                    if verified:
                        restored = pd.read_parquet(path)
                        verified = (
                            output_schema_fingerprint(restored)
                            == entry.get("output_schema_fingerprint")
                        )
                except OSError:
                    verified = False
                except Exception:
                    verified = False
        if not verified:
            pending.append(chunk)
    return pending


class ChunkExtractionError(RuntimeError):
    """A redacted chunk failure safe for manifests and CLI summaries."""

    def __init__(self, category: str):
        self.category = category
        super().__init__(f"chunk extraction failed ({category})")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute_dataframe_query(connection, sql: str) -> pd.DataFrame:
    """Execute one query and close its cursor on every path."""

    cursor = connection.cursor()
    primary_error = None
    try:
        return cursor.execute(sql).fetch_pandas_all()
    except Exception as error:
        primary_error = error
        raise
    finally:
        try:
            cursor.close()
        except Exception as close_error:
            if primary_error is None:
                raise RuntimeError("cursor cleanup failed") from close_error


_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


@contextmanager
def extraction_lock(path: Path | str, *, timeout_seconds: float = 30.0):
    """Use a process-local mutex plus a Windows kernel byte-range lock."""

    if msvcrt is None:
        raise RuntimeError("licensed extraction locking requires Windows")
    lock_path = Path(path).resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    identity = os.path.normcase(str(lock_path))
    with _LOCKS_GUARD:
        thread_lock = _THREAD_LOCKS.setdefault(identity, threading.Lock())
    if not thread_lock.acquire(timeout=timeout_seconds):
        raise TimeoutError("extraction lock timed out")
    fd = None
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"\0")
            os.fsync(fd)
        deadline = time.monotonic() + timeout_seconds
        while True:
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("extraction lock timed out")
                time.sleep(0.05)
        try:
            yield lock_path
        finally:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    finally:
        if fd is not None:
            os.close(fd)
        thread_lock.release()


def _read_manifest(path: Path) -> dict:
    if not path.exists():
        return {"manifest_version": MANIFEST_VERSION, "chunks": {}}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"manifest_version": MANIFEST_VERSION, "chunks": {}}
    if not isinstance(manifest, dict) or not isinstance(manifest.get("chunks"), dict):
        return {"manifest_version": MANIFEST_VERSION, "chunks": {}}
    return manifest


def _entry_file_verified(entry: object) -> bool:
    if not isinstance(entry, dict) or entry.get("status") != "success":
        return False
    try:
        path = Path(str(entry["output_path"]))
        restored = pd.read_parquet(path)
        return bool(
            entry.get("file_sha256") == sha256_file(path)
            and entry.get("output_schema_fingerprint")
            == output_schema_fingerprint(restored)
        )
    except (KeyError, OSError, ValueError):
        return False
    except Exception:
        return False


def _atomic_json(payload: dict, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=f".{target.name}.extract-", suffix=".tmp", dir=target.parent
    )
    temporary = Path(name)
    try:
        encoded = json.dumps(
            payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        with os.fdopen(fd, "wb", closefd=True) as handle:
            fd = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if json.loads(temporary.read_text(encoding="utf-8")) != payload:
            raise RuntimeError("manifest validation failed")
        os.replace(temporary, target)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def update_manifest_entry(path: Path, chunk_key: str, entry: dict) -> dict:
    """Atomically merge an entry while serializing all writers to one manifest."""

    with extraction_lock(path.with_name(f".{path.name}.lock")):
        manifest = _read_manifest(path)
        manifest["manifest_version"] = MANIFEST_VERSION
        manifest.setdefault("chunks", {})[chunk_key] = entry
        _atomic_json(manifest, path)
        return manifest


def _build_sql(game, parent_ids, start, end, description_identity):
    builder = build_raw_sql if game == "raw" else build_finished_sql
    return builder(parent_ids, start, end, description_identity)


def extract_chunk(
    connection,
    game: str,
    quarter: str,
    parent_ids: Mapping[str, int],
    *,
    parent_seed_sha256: str,
    description_identity=None,
    output_root: Path | str = OUTPUT_ROOT,
    query_fn: Callable[[object, str], pd.DataFrame] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict:
    """Extract one game-quarter under a kernel lock with strict resume checks."""

    validate_game(game)
    start, end = quarter_bounds(quarter)
    target = chunk_path(game, quarter, output_root=output_root)
    manifest_target = manifest_path(game, output_root=output_root)
    sql = _build_sql(game, parent_ids, start, end, description_identity)
    sql_hash = sql_contract_sha256(
        sql, parent_ids, start, end, description_identity
    )
    chunk_key = f"{game}/{quarter}"
    expectation = {
        chunk_key: {
            "output_path": str(target),
            "sql_sha256": sql_hash,
            "parent_seed_sha256": parent_seed_sha256,
            "contract_version": CONTRACT_VERSION,
        }
    }
    lock_target = target.with_name(f".{target.name}.lock")
    with extraction_lock(lock_target):
        manifest = _read_manifest(manifest_target)
        if not pending_chunks([chunk_key], manifest, expected=expectation):
            return {"status": "skipped", "game": game, "quarter": quarter}
        previous = manifest.get("chunks", {}).get(chunk_key)
        started = _utc_now()
        attempts = 0
        query = query_fn or execute_dataframe_query
        try:
            while True:
                attempts += 1
                try:
                    frame = query(connection, sql)
                    break
                except Exception as error:
                    category = classify_error(error)
                    if category != "transient" or attempts >= 3:
                        raise
                    sleep_fn(min(30.0, float(2 ** (attempts - 1))))
            if not isinstance(frame, pd.DataFrame):
                raise ValueError("query result must be a dataframe")
            frame = frame.copy()
            frame.columns = [str(column).strip().lower() for column in frame.columns]
            validate_output_frame(frame, quarter)
            file_hash = atomic_parquet(frame, target)
            entry = {
                "quarter": quarter,
                "game": game,
                "status": "success",
                "attempt_count": attempts,
                "row_count": int(len(frame)),
                "allocated_value_usd_sum": float(frame["value_usd"].sum()),
                "shipment_equivalent_sum": float(frame["shipment_equivalent"].sum()),
                "nonadditive_count_sum": float(frame["shipment_count_nonadditive"].sum()),
                "sql_sha256": sql_hash,
                "parent_seed_sha256": parent_seed_sha256,
                "output_schema_fingerprint": output_schema_fingerprint(frame),
                "file_sha256": file_hash,
                "output_path": str(target),
                "started_at": started,
                "finished_at": _utc_now(),
                "code_version": CODE_VERSION,
                "contract_version": CONTRACT_VERSION,
            }
            update_manifest_entry(manifest_target, chunk_key, entry)
            return entry
        except Exception as error:
            category = classify_error(error)
            failed = {
                "quarter": quarter,
                "game": game,
                "status": "failed",
                "attempt_count": attempts,
                "row_count": 0,
                "allocated_value_usd_sum": 0.0,
                "shipment_equivalent_sum": 0.0,
                "nonadditive_count_sum": 0.0,
                "sql_sha256": sql_hash,
                "parent_seed_sha256": parent_seed_sha256,
                "output_schema_fingerprint": None,
                "file_sha256": None,
                "output_path": str(target),
                "started_at": started,
                "finished_at": _utc_now(),
                "error_category": category,
                "redacted_error": "chunk operation failed",
                "code_version": CODE_VERSION,
                "contract_version": CONTRACT_VERSION,
            }
            if not _entry_file_verified(previous):
                update_manifest_entry(manifest_target, chunk_key, failed)
            raise ChunkExtractionError(category) from error


def load_description_identity(
    schema_path: Path | str = SCHEMA_OUTPUT_PATH,
) -> DescriptionIdentity | None:
    """Resolve only an identity present in the current persisted Task4 probe."""

    try:
        frame = pd.read_parquet(schema_path)
    except Exception as error:
        raise ValueError("schema probe artifact is missing or unreadable") from error
    identity_columns = (
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
    )
    if not set(identity_columns).issubset(frame.columns) or frame.empty:
        raise ValueError("schema probe artifact does not satisfy its identity contract")
    identities = list(
        frame.loc[:, list(identity_columns)].itertuples(index=False, name=None)
    )
    selected = choose_description_column(identities)
    return DescriptionIdentity(*selected) if selected is not None else None


def extract_full(
    connection,
    *,
    game: str,
    parent_ids: Mapping[str, int],
    parent_seed_sha256: str,
    description_identity=None,
    output_root: Path | str = OUTPUT_ROOT,
    query_fn: Callable[[object, str], pd.DataFrame] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict:
    """Resume all requested chunks in raw-then-finished, chronological order."""

    if game not in {"raw", "finished", "both"}:
        raise ValueError("game must be raw, finished, or both")
    games = ("raw", "finished") if game == "both" else (game,)
    summaries = []
    for selected_game in games:
        for quarter in iter_quarters():
            summaries.append(
                extract_chunk(
                    connection,
                    selected_game,
                    quarter,
                    parent_ids,
                    parent_seed_sha256=parent_seed_sha256,
                    description_identity=description_identity,
                    output_root=output_root,
                    query_fn=query_fn,
                    sleep_fn=sleep_fn,
                )
            )
    return {
        "games": list(games),
        "completed": sum(item["status"] == "success" for item in summaries),
        "skipped": sum(item["status"] == "skipped" for item in summaries),
        "chunk_count": len(summaries),
    }


def validate_quarter(
    connection,
    quarter: str,
    *,
    parent_ids: Mapping[str, int],
    parent_seed_sha256: str,
    description_identity=None,
    run_id: str | None = None,
    output_root: Path | str = OUTPUT_ROOT,
    query_fn: Callable[[object, str], pd.DataFrame] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict:
    """Run both games in an isolated validation generation, never production chunks."""

    quarter_bounds(quarter)
    identifier = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    if re.fullmatch(r"[A-Za-z0-9_-]+", identifier) is None:
        raise ValueError("validation run ID is invalid")
    licensed_root = Path(output_root)
    validation_root = licensed_root / "_validation" / identifier
    if licensed_root == OUTPUT_ROOT:
        validation_root = validate_output_path(validation_root)
    results = []
    for selected_game in ("raw", "finished"):
        results.append(
            extract_chunk(
                connection,
                selected_game,
                quarter,
                parent_ids,
                parent_seed_sha256=parent_seed_sha256,
                description_identity=description_identity,
                output_root=validation_root,
                query_fn=query_fn,
                sleep_fn=sleep_fn,
            )
        )
    return {
        "run_id": identifier,
        "quarter": quarter,
        "validation_root": str(validation_root),
        "games": [
            {
                "game": item["game"],
                "status": item["status"],
                "row_count": item.get("row_count", 0),
                "file_sha256": item.get("file_sha256"),
                "sql_sha256": item.get("sql_sha256"),
            }
            for item in results
        ],
    }
