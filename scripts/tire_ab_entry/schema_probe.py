"""Read-only Snowflake schema and manufacturer-parent discovery probes."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Iterable

import pandas as pd
import numpy as np

from . import artifacts
from .config import (
    APPROVED_ACCOUNT,
    APPROVED_DATABASE,
    APPROVED_ROLE,
    APPROVED_SCHEMA,
    APPROVED_WAREHOUSE,
    MANUFACTURER_PARENT_TARGETS,
    OUTPUT_ROOT,
    PARENT_CANDIDATE_MAX_ROWS_PER_TARGET,
    PARENT_CANDIDATE_WARN_ROWS_PER_TARGET,
    validate_output_path,
)


SCHEMA_OUTPUT_PATH = OUTPUT_ROOT / "_schema" / "panjiva_columns.parquet"
PARENT_CANDIDATE_OUTPUT_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.csv"
)
PARENT_CANDIDATE_PARQUET_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.parquet"
)
PARENT_CANDIDATE_METADATA_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.metadata.json"
)
PARENT_CANDIDATE_CURRENT_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.current.json"
)
PARENT_QUERY_CONTRACT_VERSION = "tire-parent-candidate-v2"
QUERY_HASH_CONTRACT_VERSION = "sql-plus-ordered-parameters-v1"
DESCRIPTION_COLUMN_PREFERENCE = (
    "GOODSDESCRIPTION",
    "PRODUCTDESCRIPTION",
    "CARGODESCRIPTION",
    "DESCRIPTION",
)
SCHEMA_METADATA_COLUMNS = (
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "ordinal_position",
    "data_type",
    "is_nullable",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "datetime_precision",
)
CANDIDATE_SOURCE_COLUMNS = (
    "target_search_term",
    "company_id",
    "current_ultimate_parent_id",
    "company_name",
    "country",
    "company_type",
    "company_status",
    "industry",
)
CANDIDATE_AUDIT_COLUMNS = ("country_missing", "industry_missing")
CANDIDATE_COLUMNS = CANDIDATE_SOURCE_COLUMNS + CANDIDATE_AUDIT_COLUMNS


def choose_description_column(
    columns: Iterable[tuple[str, str, str, str]],
) -> tuple[str, str, str, str] | None:
    """Choose one reviewed description field, rejecting table ambiguity."""

    normalized = [
        tuple(str(part).strip().upper() for part in identity)
        for identity in columns
    ]
    for preferred in DESCRIPTION_COLUMN_PREFERENCE:
        matches = [identity for identity in normalized if identity[3] == preferred]
        if len(matches) > 1:
            raise ValueError(f"ambiguous description column: {preferred}")
        if matches:
            return matches[0]
    return None


def build_schema_query() -> str:
    """Return the complete read-only Panjiva US-import column probe."""

    return """
select table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       data_type,
       is_nullable,
       character_maximum_length,
       numeric_precision,
       numeric_scale,
       datetime_precision
from {APPROVED_DATABASE}.information_schema.columns
where upper(table_name) like 'PANJIVAUSIMP%'
  and upper(table_schema) = '{APPROVED_SCHEMA}'
order by table_catalog, table_schema, table_name, ordinal_position
""".strip().format(
        APPROVED_DATABASE=APPROVED_DATABASE,
        APPROVED_SCHEMA=APPROVED_SCHEMA,
    )


def build_parent_candidate_query() -> tuple[str, tuple[str, ...]]:
    """Build a parameterized, read-only CapIQ manufacturer candidate query."""

    rows = []
    parameters = []
    for target_order, target in enumerate(MANUFACTURER_PARENT_TARGETS):
        rows.append(f"({target_order}, %s, %s)")
        parameters.extend((target, f"%{target.lower()}%"))
    values = ",\n        ".join(rows)
    sql = f"""
with targets(target_order, target_search_term, search_pattern) as (
    select column1, column2, column3
    from values {values}
)
select t.target_search_term,
       c.companyId as company_id,
       up.ultimateParentCompanyId as current_ultimate_parent_id,
       c.companyName as company_name,
       g.country as country,
       ct.companyTypeName as company_type,
       st.companyStatusTypeName as company_status,
       si.simpleIndustryDescription as industry
from targets t
join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqCompany c
  on lower(c.companyName) like t.search_pattern
left join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqCompanyUltimateParent up
  on up.companyId = c.companyId
left join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqCountryGeo g
  on g.countryId = c.countryId
left join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqCompanyType ct
  on ct.companyTypeId = c.companyTypeId
left join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqCompanyStatusType st
  on st.companyStatusTypeId = c.companyStatusTypeId
left join {APPROVED_DATABASE}.{APPROVED_SCHEMA}.ciqSimpleIndustry si
  on si.simpleIndustryId = c.simpleIndustryId
order by target_search_term, company_name, company_id
""".strip()
    return sql, tuple(parameters)


def compute_query_contract_hash(sql: str, parameters: tuple[str, ...]) -> str:
    """Hash SQL plus ordered bound values under a versioned contract."""

    payload = json.dumps(
        {
            "contract_version": QUERY_HASH_CONTRACT_VERSION,
            "sql": sql,
            "ordered_parameters": list(parameters),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def assert_read_only_query(sql: str) -> None:
    """Reject SQL that is not one comment-free SELECT/CTE statement."""

    normalized = sql.strip()
    lowered = normalized.lower()
    invalid = (
        not normalized
        or ";" in normalized
        or "--" in normalized
        or "/*" in normalized
        or "*/" in normalized
        or re.match(r"^(select|with)\b", lowered) is None
        or re.search(
            r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|"
            r"revoke|call|copy|put|remove|undrop|execute)\b",
            lowered,
        )
        is not None
        or re.search(r"\bselect\b", lowered) is None
    )
    if invalid:
        raise ValueError("SQL violates the read-only query contract")


def _lowercase_columns(frame: pd.DataFrame, contract: str) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).strip().lower() for column in frame.columns]
    if normalized.columns.duplicated().any():
        raise ValueError(f"{contract} contains duplicate columns")
    return normalized


def validate_schema_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate the complete schema probe result and normalize full identities."""

    normalized = _lowercase_columns(frame, "schema metadata")
    if set(normalized.columns) != set(SCHEMA_METADATA_COLUMNS):
        raise ValueError("schema metadata does not match the selected column contract")
    if normalized.empty:
        raise ValueError("schema metadata must not be empty")
    for column in ("table_catalog", "table_schema", "table_name", "column_name"):
        values = normalized[column].astype("string").str.strip().str.upper()
        if values.isna().any() or values.eq("").any():
            raise ValueError(f"schema metadata has null or blank {column}")
        normalized[column] = values
    if not normalized["table_catalog"].eq(APPROVED_DATABASE).all():
        raise ValueError("schema metadata is outside the approved catalog")
    if not normalized["table_schema"].eq(APPROVED_SCHEMA).all():
        raise ValueError("schema metadata is outside the approved schema")
    if not normalized["table_name"].str.startswith("PANJIVAUSIMP").all():
        raise ValueError("schema metadata includes an unapproved table")
    ordinal = pd.to_numeric(normalized["ordinal_position"], errors="coerce")
    if (
        ordinal.isna().any()
        or not np.isfinite(ordinal).all()
        or (ordinal % 1).ne(0).any()
        or ordinal.le(0).any()
    ):
        raise ValueError("schema metadata has invalid ordinal positions")
    normalized["ordinal_position"] = ordinal.astype("int64")
    for column in ("data_type", "is_nullable"):
        values = normalized[column].astype("string").str.strip().str.upper()
        if values.isna().any() or values.eq("").any():
            raise ValueError(f"schema metadata has null or blank {column}")
        normalized[column] = values
    key = ["table_catalog", "table_schema", "table_name", "column_name"]
    if normalized.duplicated(key).any():
        raise ValueError("schema metadata has duplicate full column identities")
    table_key = ["table_catalog", "table_schema", "table_name", "ordinal_position"]
    if normalized.duplicated(table_key).any():
        raise ValueError("schema metadata has duplicate ordinal positions within a table")
    return normalized.loc[:, SCHEMA_METADATA_COLUMNS].sort_values(
        ["table_catalog", "table_schema", "table_name", "ordinal_position"],
        kind="stable",
    ).reset_index(drop=True)


def validate_candidate_frame(
    frame: pd.DataFrame,
    *,
    warn_rows_per_target: int = PARENT_CANDIDATE_WARN_ROWS_PER_TARGET,
    max_rows_per_target: int = PARENT_CANDIDATE_MAX_ROWS_PER_TARGET,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """Validate and order candidate rows while preserving documented source nulls."""

    if warn_rows_per_target < 1 or max_rows_per_target < warn_rows_per_target:
        raise ValueError("candidate row safeguards are invalid")
    normalized = _lowercase_columns(frame, "candidate result")
    if set(normalized.columns) != set(CANDIDATE_SOURCE_COLUMNS):
        raise ValueError("candidate result does not match the eight source fields")
    if normalized.empty:
        raise ValueError("candidate result must not be empty")

    required_text = (
        "target_search_term",
        "company_name",
        "company_type",
        "company_status",
    )
    for column in required_text:
        values = normalized[column].astype("string").str.strip()
        if values.isna().any() or values.eq("").any():
            raise ValueError(f"candidate result has null or blank {column}")
        normalized[column] = values
    if set(normalized["target_search_term"]) != set(MANUFACTURER_PARENT_TARGETS):
        raise ValueError("candidate result does not contain the exact approved target set")

    for column in ("company_id", "current_ultimate_parent_id"):
        numbers = pd.to_numeric(normalized[column], errors="coerce")
        if (
            numbers.isna().any()
            or not np.isfinite(numbers).all()
            or (numbers % 1).ne(0).any()
        ):
            raise ValueError(f"candidate result has invalid integral {column}")
        normalized[column] = numbers.astype("int64")

    # Country and industry are nullable in the CapIQ source. Preserve those
    # nulls and add explicit audit flags rather than fabricating factual values.
    for column in ("country", "industry"):
        values = normalized[column].astype("string").str.strip().replace("", pd.NA)
        normalized[column] = values
        normalized[f"{column}_missing"] = values.isna()

    if normalized.duplicated(["target_search_term", "company_id"]).any():
        raise ValueError("candidate result has duplicate target/company IDs")
    target_order = {
        target: index for index, target in enumerate(MANUFACTURER_PARENT_TARGETS)
    }
    normalized["_target_order"] = normalized["target_search_term"].map(target_order)
    normalized = normalized.sort_values(
        ["_target_order", "company_name", "company_id"],
        kind="stable",
    ).drop(columns="_target_order").reset_index(drop=True)
    counts = normalized["target_search_term"].value_counts()
    if any(int(counts[target]) > max_rows_per_target for target in MANUFACTURER_PARENT_TARGETS):
        raise ValueError("candidate row-count safeguard exceeded; no rows were truncated")
    warnings = tuple(
        target
        for target in MANUFACTURER_PARENT_TARGETS
        if int(counts[target]) > warn_rows_per_target
    )
    return normalized.loc[:, CANDIDATE_COLUMNS], warnings


def validate_connection_context(connection) -> dict[str, str]:
    """Validate the active Snowflake namespace without exposing credentials."""

    values = {
        "query_account": getattr(connection, "account", None),
        "query_warehouse": getattr(connection, "warehouse", None),
        "query_database": getattr(connection, "database", None),
        "query_schema": getattr(connection, "schema", None),
        "query_role": getattr(connection, "role", None),
    }
    expected = {
        "query_account": APPROVED_ACCOUNT,
        "query_warehouse": APPROVED_WAREHOUSE,
        "query_database": APPROVED_DATABASE,
        "query_schema": APPROVED_SCHEMA,
        "query_role": APPROVED_ROLE,
    }
    for field, approved in expected.items():
        actual = str(values[field] or "").strip().upper()
        if actual != approved:
            raise ValueError(f"Snowflake {field.removeprefix('query_')} is not approved")
        values[field] = actual
    return values


def _fetch_frame(connection, sql: str, parameters=None) -> pd.DataFrame:
    assert_read_only_query(sql)
    cursor = connection.cursor()
    primary_error = None
    frame = None
    try:
        executed = cursor.execute(sql, parameters) if parameters is not None else cursor.execute(sql)
        frame = executed.fetch_pandas_all()
    except Exception as error:
        primary_error = error
    try:
        cursor.close()
    except Exception:
        if primary_error is None:
            raise
    if primary_error is not None:
        raise primary_error
    return frame


def _connection_metadata(connection) -> dict[str, object]:
    return validate_connection_context(connection)


def probe_schema(connection, *, output_path: Path | str = SCHEMA_OUTPUT_PATH) -> dict:
    """Query and atomically persist Panjiva import-column metadata."""

    requested = Path(output_path)
    if requested.suffix.lower() != ".parquet":
        raise ValueError("schema output target must use the .parquet suffix")
    target = validate_output_path(requested)
    context = validate_connection_context(connection)
    frame = validate_schema_frame(_fetch_frame(connection, build_schema_query()))
    frame["query_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    for column, value in context.items():
        frame[column] = value
    artifact_hash = artifacts.atomic_write_parquet(frame, target)

    selected = choose_description_column(
        zip(
            frame["table_catalog"],
            frame["table_schema"],
            frame["table_name"],
            frame["column_name"],
            strict=True,
        )
    )
    result = {
        "description_mode": "unavailable",
        "column_count": int(len(frame)),
        "output_path": str(target),
        "artifact_sha256": artifact_hash,
    }
    if selected is not None:
        result.update(
            {
                "description_mode": "available",
                "description_catalog": selected[0],
                "description_schema": selected[1],
                "description_table": selected[2],
                "description_column": selected[3],
            }
        )
    return result


def discover_parent_candidates(
    connection,
    *,
    output_path: Path | str = PARENT_CANDIDATE_OUTPUT_PATH,
) -> dict:
    """Persist reviewed-name CapIQ candidates without selecting a parent."""

    requested = Path(output_path)
    if requested.suffix.lower() != ".csv":
        raise ValueError("parent review output target must use the .csv suffix")
    target = validate_output_path(requested)
    canonical_target = validate_output_path(target.with_suffix(".parquet"))
    metadata_target = validate_output_path(target.with_suffix(".metadata.json"))
    distinct_paths = {os.path.normcase(str(path)) for path in (
        target,
        canonical_target,
        metadata_target,
    )}
    if len(distinct_paths) != 3:
        raise ValueError("candidate artifact paths must resolve to distinct targets")
    context = validate_connection_context(connection)
    sql, parameters = build_parent_candidate_query()
    frame, warnings = validate_candidate_frame(_fetch_frame(connection, sql, parameters))
    counts = {
        search_term: int(frame["target_search_term"].eq(search_term).sum())
        for search_term in MANUFACTURER_PARENT_TARGETS
    }
    sanitized = artifacts.sanitize_candidate_csv(frame)
    timestamp = datetime.now(timezone.utc).isoformat()
    metadata = {
        "query_timestamp_utc": timestamp,
        "sql_contract_version": PARENT_QUERY_CONTRACT_VERSION,
        "query_contract_sha256": compute_query_contract_hash(sql, parameters),
        "query_hash_contract_version": QUERY_HASH_CONTRACT_VERSION,
        "namespace": {
            key.removeprefix("query_"): value for key, value in context.items()
        },
        "candidate_count": int(len(frame)),
        "candidate_counts": counts,
        "row_count_warnings": list(warnings),
        "nullable_source_fields": ["country", "industry"],
    }
    publication = artifacts.publish_candidate_artifact_set(
        frame,
        sanitized,
        metadata,
        canonical_path=canonical_target,
        csv_path=target,
        metadata_path=metadata_target,
    )
    return {
        "candidate_counts": counts,
        "candidate_count": int(len(frame)),
        "output_path": str(target),
        "canonical_path": str(publication["canonical_path"]),
        "metadata_path": str(publication["metadata_path"]),
        "current_manifest_path": str(publication["current_manifest_path"]),
        "generation_id": publication["generation_id"],
        "projection_paths": publication["projection_paths"],
        "metadata_sha256": publication["metadata_sha256"],
        "review_status": "human_selection_required",
        "row_count_warnings": warnings,
    }
