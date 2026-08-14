"""Resumable, atomic Snowflake extraction for the AB-entry pilot."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd

from .config import OUT, SECTORS, VERSION
from .sql import build_trade_sql


ENV_CANDIDATES = (
    Path.home() / ".snowflake.env",
    Path.home() / "OneDrive" / "Research" / "Panjiva" / ".env",
)


def ensure_output_path(path: Path | str) -> Path:
    """Reject any licensed output target outside the approved root."""

    target = Path(path).resolve(strict=False)
    root = OUT.resolve(strict=False)
    if not target.is_relative_to(root):
        raise ValueError(f"path is outside licensed output root: {target}")
    return target


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_parquet(frame: pd.DataFrame, target: Path | str) -> None:
    """Write and read-validate a Parquet file before atomic replacement."""

    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    frame.to_parquet(temporary, index=False)
    check = pd.read_parquet(temporary)
    if len(check) != len(frame) or list(check.columns) != list(frame.columns):
        raise RuntimeError(f"Parquet validation failed for {path}")
    temporary.replace(path)


def _atomic_json(payload: dict, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    json.loads(temporary.read_text(encoding="utf-8"))
    temporary.replace(target)


def update_manifest(path: Path | str, chunk_key: str, entry: dict) -> dict:
    """Atomically merge one chunk record into the extraction manifest."""

    target = Path(path)
    if target.exists():
        manifest = json.loads(target.read_text(encoding="utf-8"))
    else:
        manifest = {"version": VERSION, "chunks": {}}
    manifest.setdefault("chunks", {})[chunk_key] = entry
    _atomic_json(manifest, target)
    return manifest


def run_chunk(
    cursor,
    sql: str,
    target: Path | str,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> pd.DataFrame:
    """Fetch a query with bounded retries and atomically store lowercase columns."""

    waits = (1, 4, 16)
    last_error: Exception | None = None
    for attempt, wait_seconds in enumerate(waits, start=1):
        try:
            frame = cursor.execute(sql).fetch_pandas_all()
            frame.columns = [str(column).lower() for column in frame.columns]
            atomic_parquet(frame, target)
            return frame
        except Exception as error:  # connector exceptions vary by failure layer
            last_error = error
            if attempt == len(waits):
                break
            sleep_fn(wait_seconds)
    raise RuntimeError("Snowflake chunk failed after 3 attempts") from last_error


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect_kwargs() -> dict:
    """Load the existing credential file without exposing secret values."""

    for candidate in ENV_CANDIDATES:
        if candidate.exists():
            _load_env_file(candidate)
            break
    else:
        raise FileNotFoundError(f"Snowflake environment file not found: {ENV_CANDIDATES}")
    return {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
        "warehouse": os.environ.get(
            "SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"
        ),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
    }


def connect():
    import snowflake.connector

    return snowflake.connector.connect(**connect_kwargs())


def quarter_bounds(year_quarter: str) -> tuple[str, str]:
    period = pd.Period(year_quarter, freq="Q")
    return period.start_time.date().isoformat(), (period + 1).start_time.date().isoformat()


def _query_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _verified_existing_chunk(target: Path, entry: dict | None, query_hash: str) -> bool:
    return bool(
        entry
        and entry.get("status") == "complete"
        and entry.get("query_sha256") == query_hash
        and target.exists()
        and entry.get("file_sha256") == sha256_file(target)
    )


def extract_trade_chunks(
    cursor,
    quarters: Iterable[str],
    *,
    sectors: Iterable[str] = tuple(SECTORS),
    samples: Iterable[str] = ("main", "allocated"),
) -> dict:
    """Extract approved sector-quarter chunks, skipping verified completions."""

    manifest_path = ensure_output_path(OUT / "extract_manifest.json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {"version": VERSION, "chunks": {}}

    for sample in samples:
        for sector_id in sectors:
            if sector_id not in SECTORS:
                raise ValueError(f"unapproved sector_id: {sector_id}")
            for year_quarter in quarters:
                start, end = quarter_bounds(year_quarter)
                sql = build_trade_sql(sector_id, start, end, sample)
                query_hash = _query_hash(sql)
                chunk_key = f"{sample}/{sector_id}/{year_quarter}"
                target = ensure_output_path(
                    OUT / "_chunks" / sample / sector_id / f"{year_quarter}.parquet"
                )
                existing = manifest.get("chunks", {}).get(chunk_key)
                if _verified_existing_chunk(target, existing, query_hash):
                    continue
                frame = run_chunk(cursor, sql, target)
                entry = {
                    "status": "complete",
                    "rows": int(len(frame)),
                    "columns": list(frame.columns),
                    "query_sha256": query_hash,
                    "file_sha256": sha256_file(target),
                }
                manifest = update_manifest(manifest_path, chunk_key, entry)
    return manifest
