"""Fail-closed atomic writers for licensed tire artifacts."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from typing import Callable
import uuid

import pandas as pd

try:
    import msvcrt
except ImportError:  # pragma: no cover - licensed runtime is Windows-only
    msvcrt = None

from .config import validate_output_path


_REPLACE_LOCK = threading.Lock()


def _replace_with_retry(source: Path, target: Path) -> None:
    deadline = time.monotonic() + 1.0
    while True:
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_payload(fd: int, payload: bytes) -> None:
    with os.fdopen(fd, "wb", closefd=True) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def atomic_write_bytes(
    payload: bytes,
    target: Path | str,
    *,
    validator: Callable[[Path], None] | None = None,
) -> str:
    """Write bytes through an exclusive same-directory temp and exact checksum."""

    canonical = validate_output_path(target)
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical = validate_output_path(canonical)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{canonical.name}.atomic-",
        suffix=".tmp",
        dir=canonical.parent,
    )
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        _write_payload(fd, payload)
        descriptor_open = False
        if validator is not None:
            validator(temporary)
        expected_hash = _sha256_bytes(payload)
        if _sha256_file(temporary) != expected_hash:
            raise RuntimeError(f"atomic artifact checksum validation failed for {canonical}")

        final_target = validate_output_path(canonical)
        final_temporary = validate_output_path(temporary)
        if final_target != canonical or final_temporary.parent != final_target.parent:
            raise RuntimeError("licensed output parent changed during atomic write")
        with _REPLACE_LOCK:
            _replace_with_retry(final_temporary, final_target)
        return expected_hash
    finally:
        if descriptor_open:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def atomic_write_parquet(frame: pd.DataFrame, target: Path | str) -> str:
    """Atomically write Parquet and validate exact dataframe content and dtypes."""

    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    payload = buffer.getvalue()

    def validate(path: Path) -> None:
        restored = pd.read_parquet(path)
        try:
            pd.testing.assert_frame_equal(
                restored.reset_index(drop=True),
                frame.reset_index(drop=True),
                check_dtype=True,
                check_like=False,
            )
        except AssertionError as error:
            raise RuntimeError("Parquet exact-content validation failed") from error

    return atomic_write_bytes(payload, target, validator=validate)


def atomic_write_csv(frame: pd.DataFrame, target: Path | str) -> str:
    """Atomically write deterministic UTF-8-BOM CSV bytes."""

    payload = frame.to_csv(index=False, lineterminator="\n").encode("utf-8-sig")
    return atomic_write_bytes(payload, target)


def atomic_write_json(payload: dict, target: Path | str) -> str:
    """Atomically write deterministic JSON and validate exact parsed content."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")

    def validate(path: Path) -> None:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        if parsed != payload:
            raise RuntimeError("JSON exact-content validation failed")

    return atomic_write_bytes(encoded, target, validator=validate)


def sanitize_candidate_csv(frame: pd.DataFrame) -> pd.DataFrame:
    """Create a formula-safe review presentation while preserving audit flags."""

    sanitized = frame.copy(deep=True)
    for column in ("country", "industry"):
        flag_column = f"{column}_missing"
        missing = sanitized[column].isna()
        if not missing.eq(sanitized[flag_column].astype(bool)).all():
            raise ValueError(f"{column} missing values do not match {flag_column}")
        sanitized.loc[missing, column] = f"[MISSING: see {flag_column}]"

    text_columns = sanitized.select_dtypes(include=["object", "string"]).columns
    for column in text_columns:
        sanitized[column] = sanitized[column].map(
            lambda value: (
                "'" + value
                if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@"))
                else value
            )
        )
    return sanitized


@contextmanager
def candidate_publication_lock(
    lock_path: Path | str,
    *,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.05,
):
    """Acquire a bounded Windows kernel file lock released when the process dies."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("publication lock timing must be positive")
    if msvcrt is None:
        raise RuntimeError("candidate publication locking requires Windows")
    canonical = validate_output_path(lock_path)
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical = validate_output_path(canonical)
    fd = os.open(canonical, os.O_CREAT | os.O_RDWR)
    if os.fstat(fd).st_size == 0:
        os.write(fd, b"\0")
        os.fsync(fd)
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("candidate publication lock timed out")
                time.sleep(poll_seconds)
        try:
            yield canonical
        finally:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    finally:
        os.close(fd)
        try:
            canonical.unlink(missing_ok=True)
        except OSError:
            pass


GENERATION_CANONICAL_NAME = "manufacturer_parent_candidates.parquet"
GENERATION_CSV_NAME = "manufacturer_parent_candidates.csv"
GENERATION_METADATA_NAME = "manufacturer_parent_candidates.metadata.json"
CURRENT_MANIFEST_VERSION = "candidate-generation-pointer-v1"


def _new_generation_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}-{os.getpid()}-{uuid.uuid4().hex}"


def stage_candidate_generation(
    canonical_frame: pd.DataFrame,
    sanitized_frame: pd.DataFrame,
    metadata: dict,
    *,
    generations_root: Path | str,
) -> dict:
    """Write and validate a unique generation without making it current."""

    root = validate_output_path(generations_root)
    root.mkdir(parents=True, exist_ok=True)
    root = validate_output_path(root)
    generation_id = _new_generation_id()
    generation_path = validate_output_path(root / generation_id)
    if generation_path.parent != root:
        raise RuntimeError("generation path escaped its approved root")
    generation_path.mkdir()
    canonical_path = generation_path / GENERATION_CANONICAL_NAME
    csv_path = generation_path / GENERATION_CSV_NAME
    metadata_path = generation_path / GENERATION_METADATA_NAME
    try:
        canonical_hash = atomic_write_parquet(canonical_frame, canonical_path)
        csv_hash = atomic_write_csv(sanitized_frame, csv_path)
        published_metadata = deepcopy(metadata)
        published_metadata.update(
            {
                "generation_id": generation_id,
                "artifact_sha256": {
                    "canonical_parquet": canonical_hash,
                    "sanitized_csv": csv_hash,
                },
            }
        )
        metadata_hash = atomic_write_json(published_metadata, metadata_path)
        staged = {
            "generation_id": generation_id,
            "generation_path": generation_path,
            "canonical_path": canonical_path,
            "csv_path": csv_path,
            "metadata_path": metadata_path,
            "metadata_sha256": metadata_hash,
            "metadata": published_metadata,
        }
        _verify_generation(staged)
        return staged
    except Exception:
        shutil.rmtree(generation_path, ignore_errors=True)
        raise


def _verify_generation(generation: dict) -> None:
    metadata_path = Path(generation["metadata_path"])
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata != generation["metadata"]:
        raise RuntimeError("generation metadata content changed")
    if _sha256_file(metadata_path) != generation["metadata_sha256"]:
        raise RuntimeError("generation metadata hash changed")
    expected = metadata["artifact_sha256"]
    if _sha256_file(Path(generation["canonical_path"])) != expected["canonical_parquet"]:
        raise RuntimeError("generation canonical Parquet hash changed")
    if _sha256_file(Path(generation["csv_path"])) != expected["sanitized_csv"]:
        raise RuntimeError("generation sanitized CSV hash changed")


def _current_manifest_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(".current.json")


def _generations_root(csv_path: Path) -> Path:
    return csv_path.parent / "_generations"


def _commit_generation(current_path: Path, staged: dict) -> str:
    payload = {
        "manifest_version": CURRENT_MANIFEST_VERSION,
        "generation_id": staged["generation_id"],
        "metadata_sha256": staged["metadata_sha256"],
    }
    return atomic_write_json(payload, current_path)


def resolve_current_candidate_generation(current_path: Path | str) -> dict:
    """Resolve and validate the one generation trusted by the atomic pointer."""

    pointer_path = validate_output_path(current_path)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    required = {"manifest_version", "generation_id", "metadata_sha256"}
    if set(pointer) != required or pointer["manifest_version"] != CURRENT_MANIFEST_VERSION:
        raise RuntimeError("current candidate manifest has an invalid contract")
    generation_id = str(pointer["generation_id"])
    if not generation_id or Path(generation_id).name != generation_id:
        raise RuntimeError("current candidate manifest has an invalid generation ID")
    root = validate_output_path(_generations_root(pointer_path.with_suffix(".csv")))
    generation_path = validate_output_path(root / generation_id)
    if generation_path.parent != root:
        raise RuntimeError("current candidate generation escaped its approved root")
    metadata_path = generation_path / GENERATION_METADATA_NAME
    canonical_path = generation_path / GENERATION_CANONICAL_NAME
    csv_path = generation_path / GENERATION_CSV_NAME
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    generation = {
        "generation_id": generation_id,
        "generation_path": generation_path,
        "canonical_path": canonical_path,
        "csv_path": csv_path,
        "metadata_path": metadata_path,
        "metadata_sha256": pointer["metadata_sha256"],
        "metadata": metadata,
        "current_manifest_path": pointer_path,
    }
    if metadata.get("generation_id") != generation_id:
        raise RuntimeError("current candidate metadata generation does not match pointer")
    _verify_generation(generation)
    return generation


def _repair_projections(generation: dict, targets: tuple[Path, Path, Path]) -> None:
    for source_key, target in zip(
        ("canonical_path", "csv_path", "metadata_path"), targets, strict=True
    ):
        source = Path(generation[source_key])
        atomic_write_bytes(source.read_bytes(), target)


def _cleanup_orphan_generations(root: Path, keep_generation_id: str | None) -> None:
    if not root.exists():
        return
    canonical_root = validate_output_path(root)
    for candidate in canonical_root.iterdir():
        if not candidate.is_dir() or candidate.name == keep_generation_id:
            continue
        canonical_candidate = validate_output_path(candidate)
        if canonical_candidate.parent != canonical_root:
            raise RuntimeError("orphan generation path escaped its approved root")
        shutil.rmtree(canonical_candidate)


def publish_candidate_artifact_set(
    canonical_frame: pd.DataFrame,
    sanitized_frame: pd.DataFrame,
    metadata: dict,
    *,
    canonical_path: Path | str,
    csv_path: Path | str,
    metadata_path: Path | str,
    lock_timeout_seconds: float = 30.0,
) -> dict:
    """Publish one crash-consistent generation and repair convenience projections."""

    targets = tuple(
        validate_output_path(path)
        for path in (canonical_path, csv_path, metadata_path)
    )
    if len({os.path.normcase(str(path)) for path in targets}) != 3:
        raise ValueError("candidate publication paths must be distinct")
    canonical_target, csv_target, metadata_target = targets
    lock_path = csv_target.parent / f".{csv_target.stem}.publication.lock"
    current_path = _current_manifest_path(csv_target)
    generations_root = _generations_root(csv_target)
    with candidate_publication_lock(
        lock_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        previous = None
        if current_path.exists():
            previous = resolve_current_candidate_generation(current_path)
            _repair_projections(previous, targets)
        _cleanup_orphan_generations(
            generations_root,
            previous["generation_id"] if previous is not None else None,
        )
        staged = stage_candidate_generation(
            canonical_frame,
            sanitized_frame,
            metadata,
            generations_root=generations_root,
        )
        pointer_hash = _commit_generation(current_path, staged)
        committed = resolve_current_candidate_generation(current_path)
        _repair_projections(committed, targets)
        _cleanup_orphan_generations(generations_root, committed["generation_id"])
        return {
            **committed,
            "current_manifest_sha256": pointer_hash,
            "projection_paths": {
                "canonical_parquet": str(canonical_target),
                "sanitized_csv": str(csv_target),
                "metadata": str(metadata_target),
            },
        }
