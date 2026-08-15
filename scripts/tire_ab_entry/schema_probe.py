"""Read-only Snowflake schema and manufacturer-parent discovery probes."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import MANUFACTURER_PARENT_TARGETS, OUTPUT_ROOT, validate_output_path


SCHEMA_OUTPUT_PATH = OUTPUT_ROOT / "_schema" / "panjiva_columns.parquet"
PARENT_CANDIDATE_OUTPUT_PATH = (
    OUTPUT_ROOT / "review" / "manufacturer_parent_candidates.csv"
)
DESCRIPTION_COLUMN_PREFERENCE = (
    "GOODSDESCRIPTION",
    "PRODUCTDESCRIPTION",
    "CARGODESCRIPTION",
    "DESCRIPTION",
)
CANDIDATE_COLUMNS = (
    "target_search_term",
    "company_id",
    "current_ultimate_parent_id",
    "company_name",
    "country",
    "company_type",
    "company_status",
    "industry",
)


def choose_description_column(
    columns: Iterable[tuple[str, str]],
) -> tuple[str, str] | None:
    """Choose one reviewed description field, rejecting table ambiguity."""

    normalized = [
        (str(table).strip().upper(), str(column).strip().upper())
        for table, column in columns
    ]
    for preferred in DESCRIPTION_COLUMN_PREFERENCE:
        matches = [pair for pair in normalized if pair[1] == preferred]
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
from information_schema.columns
where upper(table_name) like 'PANJIVAUSIMP%'
order by table_catalog, table_schema, table_name, ordinal_position
""".strip()


def build_parent_candidate_query() -> tuple[str, tuple[str, ...]]:
    """Build a parameterized, read-only CapIQ manufacturer candidate query."""

    selects = []
    parameters = []
    for target in MANUFACTURER_PARENT_TARGETS:
        selects.append(
            """
select %s as target_search_term,
       c.companyId as company_id,
       up.ultimateParentCompanyId as current_ultimate_parent_id,
       c.companyName as company_name,
       g.country as country,
       ct.companyTypeName as company_type,
       st.companyStatusTypeName as company_status,
       si.simpleIndustryDescription as industry
from ciqCompany c
left join ciqCompanyUltimateParent up on up.companyId = c.companyId
left join ciqCountryGeo g on g.countryId = c.countryId
left join ciqCompanyType ct on ct.companyTypeId = c.companyTypeId
left join ciqCompanyStatusType st on st.companyStatusTypeId = c.companyStatusTypeId
left join ciqSimpleIndustry si on si.simpleIndustryId = c.simpleIndustryId
where lower(c.companyName) like %s
""".strip()
        )
        parameters.extend((target, f"%{target.lower()}%"))
    sql = "\nunion all\n".join(selects)
    sql += "\norder by target_search_term, company_name, company_id"
    return sql, tuple(parameters)


def _atomic_parquet(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    frame.to_parquet(temporary, index=False)
    verified = pd.read_parquet(temporary)
    if list(verified.columns) != list(frame.columns) or len(verified) != len(frame):
        raise RuntimeError(f"Parquet validation failed for {target}")
    temporary.replace(target)


def _atomic_csv(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    frame.to_csv(temporary, index=False, encoding="utf-8-sig")
    verified = pd.read_csv(temporary)
    if list(verified.columns) != list(frame.columns) or len(verified) != len(frame):
        raise RuntimeError(f"CSV validation failed for {target}")
    temporary.replace(target)


def _connection_metadata(connection) -> dict[str, object]:
    return {
        "query_account": getattr(connection, "account", None),
        "query_warehouse": getattr(connection, "warehouse", None),
        "query_database": getattr(connection, "database", None),
        "query_schema": getattr(connection, "schema", None),
    }


def probe_schema(connection, *, output_path: Path | str = SCHEMA_OUTPUT_PATH) -> dict:
    """Query and atomically persist Panjiva import-column metadata."""

    target = Path(output_path)
    validate_output_path(target)
    frame = connection.cursor().execute(build_schema_query()).fetch_pandas_all()
    frame.columns = [str(column).lower() for column in frame.columns]
    required = {"table_name", "column_name"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"schema query missing columns: {sorted(missing)}")
    frame["query_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    for column, value in _connection_metadata(connection).items():
        frame[column] = value
    _atomic_parquet(frame, target)

    selected = choose_description_column(
        zip(frame["table_name"], frame["column_name"], strict=True)
    )
    result = {
        "description_mode": "unavailable",
        "column_count": int(len(frame)),
        "output_path": str(target),
    }
    if selected is not None:
        result.update(
            {
                "description_mode": "available",
                "description_table": selected[0],
                "description_column": selected[1],
            }
        )
    return result


def discover_parent_candidates(
    connection,
    *,
    output_path: Path | str = PARENT_CANDIDATE_OUTPUT_PATH,
) -> dict:
    """Persist reviewed-name CapIQ candidates without selecting a parent."""

    target = Path(output_path)
    validate_output_path(target)
    sql, parameters = build_parent_candidate_query()
    frame = connection.cursor().execute(sql, parameters).fetch_pandas_all()
    frame.columns = [str(column).lower() for column in frame.columns]
    missing = set(CANDIDATE_COLUMNS).difference(frame.columns)
    if missing:
        raise ValueError(f"parent candidate query missing columns: {sorted(missing)}")
    frame = frame.loc[:, CANDIDATE_COLUMNS].copy()
    target_order = {target: index for index, target in enumerate(MANUFACTURER_PARENT_TARGETS)}
    unknown_targets = set(frame["target_search_term"].dropna()).difference(target_order)
    if unknown_targets:
        raise ValueError("parent candidate query returned an unapproved target")
    frame["_target_order"] = frame["target_search_term"].map(target_order)
    frame = (
        frame.sort_values(
            ["_target_order", "company_name", "company_id"],
            kind="stable",
            na_position="last",
        )
        .drop(columns="_target_order")
        .reset_index(drop=True)
    )
    _atomic_csv(frame, target)
    counts = {
        search_term: int(frame["target_search_term"].eq(search_term).sum())
        for search_term in MANUFACTURER_PARENT_TARGETS
    }
    return {
        "candidate_counts": counts,
        "candidate_count": int(len(frame)),
        "output_path": str(target),
        "review_status": "human_selection_required",
    }
