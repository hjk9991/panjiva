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
from .sql import (
    build_company_sql,
    build_financial_sql,
    build_segment_revenue_sql,
    build_trade_sql,
)


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


def collect_parent_ids(chunk_root: Path | str) -> list[int]:
    """Collect sorted unique matched importer-parent IDs from trade chunks."""

    identifiers: set[int] = set()
    for path in Path(chunk_root).glob("*/*/*.parquet"):
        frame = pd.read_parquet(path, columns=["ultimate_parent_companyid"])
        values = pd.to_numeric(
            frame["ultimate_parent_companyid"], errors="coerce"
        ).dropna()
        identifiers.update(values.astype("int64").tolist())
    return sorted(identifiers)


def collect_reviewed_producer_ids(review_path: Path | str | None = None) -> list[int]:
    """Return reviewed producer/brand-owner IDs from the licensed review file."""

    path = Path(review_path) if review_path is not None else OUT / "entity_review_top50.csv"
    review = pd.read_csv(path)
    required = {"ultimate_parent_companyid", "entity_role"}
    missing = required.difference(review.columns)
    if missing:
        raise ValueError(f"entity review is missing columns: {sorted(missing)}")
    selected = review.loc[
        review["entity_role"].eq("producer_brand_owner"),
        "ultimate_parent_companyid",
    ]
    identifiers = pd.to_numeric(selected, errors="coerce").dropna().astype("int64")
    return sorted(set(identifiers.tolist()))


def extract_parent_metadata(connection, *, batch_size: int = 2_000) -> dict:
    """Extract current parent attributes and annual financial candidates in batches."""

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    parent_ids = collect_parent_ids(OUT / "_chunks")
    if not parent_ids:
        raise RuntimeError("no matched importer parents found in completed chunks")

    company_batches = []
    financial_batches = []
    for batch_number, offset in enumerate(range(0, len(parent_ids), batch_size)):
        batch = parent_ids[offset : offset + batch_size]
        company_target = ensure_output_path(
            OUT / "_metadata" / f"company_{batch_number:04d}.parquet"
        )
        financial_target = ensure_output_path(
            OUT / "_metadata" / f"financial_{batch_number:04d}.parquet"
        )
        company_batches.append(
            run_chunk(connection.cursor(), build_company_sql(batch), company_target)
        )
        financial_batches.append(
            run_chunk(
                connection.cursor(),
                build_financial_sql(batch, "2013-01-01", "2026-01-01"),
                financial_target,
            )
        )

    companies = (
        pd.concat(company_batches, ignore_index=True)
        .drop_duplicates("companyid")
        .sort_values("companyid")
        .reset_index(drop=True)
    )
    financials = (
        pd.concat(financial_batches, ignore_index=True)
        .drop_duplicates(["companyid", "fin_period_end"])
        .sort_values(["companyid", "fin_period_end"])
        .reset_index(drop=True)
    )
    company_target = ensure_output_path(OUT / "firm_master.parquet")
    financial_target = ensure_output_path(OUT / "firm_financials_annual.parquet")
    atomic_parquet(companies, company_target)
    atomic_parquet(financials, financial_target)

    manifest_path = ensure_output_path(OUT / "extract_manifest.json")
    update_manifest(
        manifest_path,
        "metadata/company",
        {
            "status": "complete",
            "rows": int(len(companies)),
            "file_sha256": sha256_file(company_target),
        },
    )
    update_manifest(
        manifest_path,
        "metadata/financials",
        {
            "status": "complete",
            "rows": int(len(financials)),
            "file_sha256": sha256_file(financial_target),
        },
    )
    return {
        "parents": len(parent_ids),
        "company_rows": len(companies),
        "financial_rows": len(financials),
    }


def extract_segment_revenue(connection, *, batch_size: int = 2_000) -> dict:
    """Extract annual CapIQ segments for reviewed producers into licensed storage."""

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    parent_ids = collect_reviewed_producer_ids()
    if not parent_ids:
        raise RuntimeError("no reviewed producer/brand-owner parents found")

    batches = []
    output_root = OUT / "auto_output_share"
    for batch_number, offset in enumerate(range(0, len(parent_ids), batch_size)):
        batch = parent_ids[offset : offset + batch_size]
        target = ensure_output_path(
            output_root / "_metadata" / f"segment_{batch_number:04d}.parquet"
        )
        batches.append(
            run_chunk(
                connection.cursor(),
                build_segment_revenue_sql(batch, "2015-01-01", "2026-01-01"),
                target,
            )
        )

    segments = pd.concat(batches, ignore_index=True)
    key_columns = ["companyid", "fin_calendar_year", "segmentid", "dataitemid"]
    segments = (
        segments.drop_duplicates(key_columns)
        .sort_values(key_columns)
        .reset_index(drop=True)
    )
    target = ensure_output_path(output_root / "firm_segment_revenue_annual.parquet")
    atomic_parquet(segments, target)

    update_manifest(
        ensure_output_path(OUT / "extract_manifest.json"),
        "metadata/segment_revenue",
        {
            "status": "complete",
            "rows": int(len(segments)),
            "company_count": len(parent_ids),
            "file_sha256": sha256_file(target),
        },
    )
    return {
        "parents": len(parent_ids),
        "rows": len(segments),
        "batches": len(batches),
    }
