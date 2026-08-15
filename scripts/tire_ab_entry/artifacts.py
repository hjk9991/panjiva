"""Fail-closed atomic writers for licensed tire artifacts."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from typing import Callable

import pandas as pd

from .config import validate_output_path


_REPLACE_LOCK = threading.Lock()


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
            os.replace(final_temporary, final_target)
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
    """Acquire a bounded cross-process directory mutex for one publication set."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("publication lock timing must be positive")
    canonical = validate_output_path(lock_path)
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical = validate_output_path(canonical)
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            canonical.mkdir()
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError("candidate publication lock timed out")
            time.sleep(poll_seconds)
    try:
        yield canonical
    finally:
        canonical.rmdir()


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
    """Publish and verify one locked generation at fixed consumer paths."""

    targets = tuple(
        validate_output_path(path)
        for path in (canonical_path, csv_path, metadata_path)
    )
    if len({os.path.normcase(str(path)) for path in targets}) != 3:
        raise ValueError("candidate publication paths must be distinct")
    canonical_target, csv_target, metadata_target = targets
    lock_path = csv_target.parent / f".{csv_target.stem}.publication.lock"
    with candidate_publication_lock(
        lock_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        canonical_hash = atomic_write_parquet(canonical_frame, canonical_target)
        csv_hash = atomic_write_csv(sanitized_frame, csv_target)
        published_metadata = deepcopy(metadata)
        published_metadata["artifact_sha256"] = {
            "canonical_parquet": canonical_hash,
            "sanitized_csv": csv_hash,
        }
        metadata_hash = atomic_write_json(published_metadata, metadata_target)

        if _sha256_file(canonical_target) != canonical_hash:
            raise RuntimeError("published canonical Parquet hash changed")
        if _sha256_file(csv_target) != csv_hash:
            raise RuntimeError("published sanitized CSV hash changed")
        parsed_metadata = json.loads(metadata_target.read_text(encoding="utf-8"))
        if parsed_metadata != published_metadata:
            raise RuntimeError("published metadata changed during verification")
        return {
            "canonical_sha256": canonical_hash,
            "csv_sha256": csv_hash,
            "metadata_sha256": metadata_hash,
            "metadata": published_metadata,
        }
