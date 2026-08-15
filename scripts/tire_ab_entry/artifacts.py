"""Fail-closed atomic writers for licensed tire artifacts."""

from __future__ import annotations

from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
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
