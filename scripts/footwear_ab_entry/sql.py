"""Supplier-preserving, read-only Snowflake SQL for the tire AB-entry pilot."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from numbers import Integral
import re

from .config import (
    APPROVED_DATABASE,
    APPROVED_SCHEMA,
    ELIGIBLE_FAMILIES,
    ESCALATED_FAMILIES,
    MANUFACTURER_DESCRIPTION_ALIASES,
    MANUFACTURER_KEYS,
    PROBE_FAMILIES,
    SPORTS_FAMILIES,
)
from .schema_probe import DESCRIPTION_COLUMN_PREFERENCE, assert_read_only_query


_IMPORT_TABLE = "PANJIVAUSIMPORT"


@dataclass(frozen=True)
class DescriptionIdentity:
    """A schema-probe-approved, fully qualified shipment-description column."""

    catalog: str
    schema: str
    table: str
    column: str


def _validate_parent_ids(parent_ids: Mapping[str, int]) -> tuple[int, ...]:
    expected = len(MANUFACTURER_KEYS)
    if not isinstance(parent_ids, Mapping) or set(parent_ids) != set(MANUFACTURER_KEYS):
        raise ValueError(
            "parent_ids must be an exact keyed parent mapping for "
            + ", ".join(MANUFACTURER_KEYS)
        )
    values = tuple(parent_ids[key] for key in MANUFACTURER_KEYS)
    valid = (
        len(values) == expected
        and all(
            isinstance(value, Integral) and not isinstance(value, bool)
            for value in values
        )
        and all(int(value) > 0 for value in values)
        and len({int(value) for value in values}) == expected
    )
    if not valid:
        raise ValueError(
            "keyed parent mapping values must be unique positive integral IDs, "
            f"one per approved manufacturer ({expected})"
        )
    return tuple(int(value) for value in values)


def _validate_dates(date_start: str, date_end: str) -> tuple[str, str]:
    strict_iso = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
    if not isinstance(date_start, str) or strict_iso.fullmatch(date_start) is None:
        raise ValueError("date_start must be a strict ISO date")
    if not isinstance(date_end, str) or strict_iso.fullmatch(date_end) is None:
        raise ValueError("date_end must be a strict ISO date")
    try:
        start = date.fromisoformat(date_start)
        end = date.fromisoformat(date_end)
    except ValueError as exc:
        raise ValueError("dates must be valid strict ISO dates") from exc
    if start >= end:
        raise ValueError("date_start must be before date_end")
    return start.isoformat(), end.isoformat()


def _validate_description_identity(
    identity: DescriptionIdentity | None,
) -> DescriptionIdentity | None:
    if identity is None:
        return None
    if not isinstance(identity, DescriptionIdentity):
        raise ValueError("description column must be a structured DescriptionIdentity")
    expected = (
        APPROVED_DATABASE,
        APPROVED_SCHEMA,
        _IMPORT_TABLE,
    )
    actual = (identity.catalog, identity.schema, identity.table)
    if actual != expected or identity.column not in DESCRIPTION_COLUMN_PREFERENCE:
        raise ValueError("description identity is outside the approved probe contract")
    return identity


def _qualified(table: str) -> str:
    return f"{APPROVED_DATABASE}.{APPROVED_SCHEMA}.{table}"


def _id_list(parent_ids: tuple[int, int, int]) -> str:
    return ", ".join(str(value) for value in parent_ids)


def _reviewed_manufacturer_cte(parent_ids: tuple[int, int, int]) -> str:
    rows = []
    for order, (key, parent_id) in enumerate(
        zip(MANUFACTURER_KEYS, parent_ids, strict=True)
    ):
        for alias in MANUFACTURER_DESCRIPTION_ALIASES[key]:
            escaped = alias.replace("'", "''")
            rows.append(f"({order}, {parent_id}, '{escaped}')")
    return ",\n        ".join(rows)


def resolve_distinct_candidate(candidates: Iterable[int | None]) -> dict[str, int | None]:
    """Reference contract for fail-closed xref/PIT candidate resolution."""

    distinct = sorted({int(value) for value in candidates if value is not None})
    return {
        "candidate": distinct[0] if len(distinct) == 1 else None,
        "distinct_candidate_count": len(distinct),
        "ambiguous": int(len(distinct) > 1),
    }


def resolve_ownership_sides(
    *,
    importer_parent_candidates: Iterable[int | None],
    shipper_parent_candidates: Iterable[int | None],
) -> dict[str, dict[str, int | None]]:
    """Resolve side-specific PIT candidates without cross-product coupling."""

    return {
        "importer": resolve_distinct_candidate(importer_parent_candidates),
        "shipper": resolve_distinct_candidate(shipper_parent_candidates),
    }


def resolve_pit_intervals(
    intervals: Iterable[tuple[int, str, str]],
) -> dict[str, int | None]:
    """Resolve distinct PIT intervals while retaining same-parent overlaps."""

    distinct_intervals = set(intervals)
    parents = {parent_id for parent_id, _, _ in distinct_intervals}
    parent_count = len(parents)
    interval_count = len(distinct_intervals)
    return {
        "candidate": min(parents) if parent_count == 1 else None,
        "distinct_interval_match_count": interval_count,
        "distinct_parent_candidate_count": parent_count,
        "ambiguous": int(parent_count > 1),
        "same_parent_overlap": int(interval_count > 1 and parent_count == 1),
    }


def reference_hs6_allocation(
    full_codes: Iterable[str], *, value_usd: float
) -> dict[str, dict[str, float | int]]:
    """Reference the distinct-HS6 allocation and finished-code review contract."""

    by_hs6: dict[str, set[str]] = {}
    for code in full_codes:
        normalized = str(code).replace(".", "")
        if len(normalized) >= 6:
            by_hs6.setdefault(normalized[:6], set()).add(normalized)
    if not by_hs6:
        return {}
    allocation = 1.0 / len(by_hs6)
    output: dict[str, dict[str, float | int]] = {}
    for hs6 in sorted(by_hs6):
        codes = by_hs6[hs6]
        # Athletic status is decidable at six digits for footwear; the
        # escalated general families are estimation-eligible after the
        # 2026-08-17 boundary review.
        reviewed_count = len(
            {code for code in codes if code[:6] in ELIGIBLE_FAMILIES}
        )
        unreviewed_count = len(codes) - reviewed_count
        output[hs6] = {
            "distinct_full_code_count": len(codes),
            "reviewed_full_code_count": reviewed_count,
            "unreviewed_full_code_count": unreviewed_count,
            "mixed_review": int(reviewed_count > 0 and unreviewed_count > 0),
            "allocation_factor": allocation,
            "allocated_value_usd": value_usd * allocation,
            "hs_eligible": int(reviewed_count == len(codes) and reviewed_count > 0),
        }
    return output


def reference_raw_route(
    *,
    importer_is_reviewed: bool,
    importer_up: int | None,
    shipper_up: int | None,
    relationship: str,
    identity_ambiguous: bool,
    historical_backcast: bool,
    hs_eligible: bool,
) -> dict[str, str | int]:
    """Reference raw-route and main-versus-sensitivity eligibility semantics."""

    route = "manufacturer_direct" if importer_is_reviewed else "unattributed"
    if shipper_up is None:
        supplier = "unknown_supplier"
    elif importer_up == shipper_up and relationship in {"self", "parent_sub", "sibling"}:
        supplier = "intragroup_supplier"
    elif relationship == "arms_length" and importer_up != shipper_up:
        supplier = "external_supplier"
    else:
        supplier = "unknown_supplier"
    sensitivity = int(
        importer_is_reviewed and hs_eligible and not identity_ambiguous
    )
    return {
        "import_route": route,
        "supplier_relationship": supplier,
        "estimation_eligible": int(sensitivity == 1 and not historical_backcast),
        "sensitivity_eligible": sensitivity,
    }


def reference_review_pending_technical_eligibility(
    *,
    hs_eligible: int,
    manufacturer_conflict: int,
    description_ambiguous: int,
    importer_xref_ambiguous: int,
    shipper_xref_ambiguous: int,
    importer_pit_ambiguous: int,
    shipper_pit_ambiguous: int,
    importer_historical_backcast: int,
    shipper_historical_backcast: int,
    import_route: str,
) -> int:
    """Audited pending-review predicate excluding every hard technical failure."""

    return int(
        hs_eligible == 1
        and manufacturer_conflict == 0
        and description_ambiguous == 0
        and importer_xref_ambiguous == 0
        and shipper_xref_ambiguous == 0
        and importer_pit_ambiguous == 0
        and shipper_pit_ambiguous == 0
        and importer_historical_backcast == 0
        and shipper_historical_backcast == 0
        and import_route in {"manufacturer_direct", "distributor_intermediated"}
    )


def reference_validation_identity(
    rows: Iterable[tuple[str, float]],
) -> dict[str, int | float]:
    """Return independent record-count and allocated-equivalent diagnostics."""

    materialized = [(str(record_id), float(weight)) for record_id, weight in rows]
    unique = len({record_id for record_id, _ in materialized})
    equivalent = sum(weight for _, weight in materialized)
    return {
        "unique_shipment_count": unique,
        "shipment_equivalent": equivalent,
    }


def _base_ctes(
    parent_ids: tuple[int, int, int],
    start: str,
    end: str,
    description: DescriptionIdentity | None,
) -> str:
    if description is None:
        description_expression = "null as shipment_description"
    else:
        description_expression = f"i.{description.column} as shipment_description"
    # Athletic status is decidable at HS6, so reviewed matching is by family
    # prefix rather than the tire pilot's enumerated statistical codes.
    reviewed_codes = ", ".join(repr(code) for code in SPORTS_FAMILIES)
    return f"""
reviewed_manufacturers(
    manufacturer_order, manufacturer_parent_id, description_alias
) as (
    select column1, column2, column3
    from values {_reviewed_manufacturer_cte(parent_ids)}
),
base as (
    select i.panjivaRecordId,
           i.arrivalDate,
           i.conPanjivaId,
           i.shpPanjivaId,
           i.conCountry,
           i.shpCountry as shipper_country,
           i.shpmtOrigin,
           {description_expression},
           iff(i.valueOfGoodsUSD < 0, null, i.valueOfGoodsUSD) as value_usd,
           iff(i.weightKg < 0, null, i.weightKg) as weight_kg,
           iff(i.volumeTEU < 0, null, i.volumeTEU) as teu,
           iff(i.numberOfContainers < 0, null, i.numberOfContainers)
               as container_count
    from {_qualified('panjivaUSImport')} i
    where i.arrivalDate >= '{start}'
      and i.arrivalDate < '{end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
hs_agg as (
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from {_qualified('panjivaUSImpHSCode')} h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
hs_tokens as (
    select a.panjivaRecordId,
           replace(
               coalesce(
                   regexp_substr(
                       t.value, 'Classified: ?([0-9][0-9.]*)', 1, 1, 'e', 1
                   ),
                   regexp_substr(
                       t.value, 'Parsed: ?([0-9][0-9.]*)', 1, 1, 'e', 1
                   ),
                   regexp_substr(
                       t.value, 'Manual: ?([0-9][0-9.]*)', 1, 1, 'e', 1
                   )
               ),
               '.',
               ''
           ) as hs_full_code
    from hs_agg a, lateral split_to_table(a.allhs, chr(59)) t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
),
hs_codes as (
    select distinct panjivaRecordId,
           hs_full_code,
           left(hs_full_code, 6) as hs6
    from hs_tokens
    where hs_full_code is not null and length(hs_full_code) >= 6
),
hs6_codes as (
    select panjivaRecordId,
           hs6,
           listagg(distinct hs_full_code, '|')
               within group (order by hs_full_code) as hs_full_code,
           count(*) as distinct_full_code_count,
           sum(iff(left(hs_full_code, 6) in ({reviewed_codes}), 1, 0))
               as reviewed_full_code_count,
           sum(iff(left(hs_full_code, 6) not in ({reviewed_codes}), 1, 0))
               as unreviewed_full_code_count
    from hs_codes
    group by panjivaRecordId, hs6
),
hs_with_counts as (
    select panjivaRecordId,
           hs_full_code,
           hs6,
           distinct_full_code_count,
           reviewed_full_code_count,
           unreviewed_full_code_count,
           iff(
               reviewed_full_code_count > 0
               and unreviewed_full_code_count > 0,
               1,
               0
           ) as mixed_review,
           count(*) over (partition by panjivaRecordId) as n_hs6
    from hs6_codes
),
xref_distinct as (
    select distinct identifierValue, companyId
    from {_qualified('panjivaCompanyCrossRef')}
    where activeFlag = 1
      and identifierValue is not null
      and companyId is not null
),
xref_resolved as (
    select identifierValue,
           iff(count(*) = 1, min(companyId), null) as companyId,
           count(*) as distinct_company_candidate_count,
           iff(count(*) > 1, 1, 0) as xref_ambiguous
    from xref_distinct
    group by identifierValue
),
identified as (
    select b.*,
           xc.companyId as importer_companyid,
           xs.companyId as shipper_companyid,
           coalesce(xc.distinct_company_candidate_count, 0)
               as importer_xref_distinct_candidate_count,
           coalesce(xs.distinct_company_candidate_count, 0)
               as shipper_xref_distinct_candidate_count,
           coalesce(xc.xref_ambiguous, 0) as importer_xref_ambiguous,
           coalesce(xs.xref_ambiguous, 0) as shipper_xref_ambiguous
    from base b
    left join xref_resolved xc on xc.identifierValue = b.conPanjivaId
    left join xref_resolved xs on xs.identifierValue = b.shpPanjivaId
),
importer_pit_interval_matches as (
    select distinct i.panjivaRecordId,
           p.ultimateParentCompanyId,
           p.startDate,
           p.endDate
    from identified i
    join {_qualified('ciqCompanyUltimateParentPIT')} p
      on p.companyId = i.importer_companyid
     and i.arrivalDate >= p.startDate
     and i.arrivalDate <= p.endDate
    where p.ultimateParentCompanyId is not null
),
importer_pit_resolved as (
    select panjivaRecordId,
           iff(
               count(distinct ultimateParentCompanyId) = 1,
               min(ultimateParentCompanyId),
               null
           )
               as importer_up_pit,
           count(*) as importer_pit_distinct_interval_match_count,
           count(distinct ultimateParentCompanyId)
               as importer_pit_distinct_parent_candidate_count,
           iff(count(distinct ultimateParentCompanyId) > 1, 1, 0)
               as importer_pit_ambiguous,
           iff(
               count(*) > 1
               and count(distinct ultimateParentCompanyId) = 1,
               1,
               0
           ) as importer_pit_same_parent_overlap
    from importer_pit_interval_matches
    group by panjivaRecordId
),
shipper_pit_interval_matches as (
    select distinct i.panjivaRecordId,
           p.ultimateParentCompanyId,
           p.startDate,
           p.endDate
    from identified i
    join {_qualified('ciqCompanyUltimateParentPIT')} p
      on p.companyId = i.shipper_companyid
     and i.arrivalDate >= p.startDate
     and i.arrivalDate <= p.endDate
    where p.ultimateParentCompanyId is not null
),
shipper_pit_resolved as (
    select panjivaRecordId,
           iff(
               count(distinct ultimateParentCompanyId) = 1,
               min(ultimateParentCompanyId),
               null
           )
               as shipper_up_pit,
           count(*) as shipper_pit_distinct_interval_match_count,
           count(distinct ultimateParentCompanyId)
               as shipper_pit_distinct_parent_candidate_count,
           iff(count(distinct ultimateParentCompanyId) > 1, 1, 0)
               as shipper_pit_ambiguous,
           iff(
               count(*) > 1
               and count(distinct ultimateParentCompanyId) = 1,
               1,
               0
           ) as shipper_pit_same_parent_overlap
    from shipper_pit_interval_matches
    group by panjivaRecordId
),
ownership_inputs as (
    select i.*,
           ip.importer_up_pit,
           sp.shipper_up_pit,
           coalesce(ip.importer_pit_distinct_interval_match_count, 0)
               as importer_pit_distinct_interval_match_count,
           coalesce(sp.shipper_pit_distinct_interval_match_count, 0)
               as shipper_pit_distinct_interval_match_count,
           coalesce(ip.importer_pit_distinct_parent_candidate_count, 0)
               as importer_pit_distinct_parent_candidate_count,
           coalesce(sp.shipper_pit_distinct_parent_candidate_count, 0)
               as shipper_pit_distinct_parent_candidate_count,
           coalesce(ip.importer_pit_ambiguous, 0) as importer_pit_ambiguous,
           coalesce(sp.shipper_pit_ambiguous, 0) as shipper_pit_ambiguous,
           coalesce(ip.importer_pit_same_parent_overlap, 0)
               as importer_pit_same_parent_overlap,
           coalesce(sp.shipper_pit_same_parent_overlap, 0)
               as shipper_pit_same_parent_overlap,
           cc.ultimateParentCompanyId as importer_up_current,
           cs.ultimateParentCompanyId as shipper_up_current
    from identified i
    left join importer_pit_resolved ip
      on ip.panjivaRecordId = i.panjivaRecordId
    left join shipper_pit_resolved sp
      on sp.panjivaRecordId = i.panjivaRecordId
    left join {_qualified('ciqCompanyUltimateParent')} cc
      on cc.companyId = i.importer_companyid
    left join {_qualified('ciqCompanyUltimateParent')} cs
      on cs.companyId = i.shipper_companyid
),
ownership_mapped as (
    select o.*,
           iff(
               importer_xref_ambiguous = 1 or importer_pit_ambiguous = 1,
               null,
               coalesce(importer_up_pit, importer_up_current, importer_companyid)
           ) as importer_up,
           iff(
               shipper_xref_ambiguous = 1 or shipper_pit_ambiguous = 1,
               null,
               coalesce(shipper_up_pit, shipper_up_current, shipper_companyid)
           ) as shipper_up,
           case
               when importer_xref_ambiguous = 1 or importer_pit_ambiguous = 1
                   then 'unresolved'
               when importer_up_pit is not null then 'pit'
               when importer_up_current is not null then 'current_parent_fallback'
               when importer_companyid is not null then 'self_fallback'
               else 'unresolved'
           end as importer_ownership_source,
           case
               when shipper_xref_ambiguous = 1 or shipper_pit_ambiguous = 1
                   then 'unresolved'
               when shipper_up_pit is not null then 'pit'
               when shipper_up_current is not null then 'current_parent_fallback'
               when shipper_companyid is not null then 'self_fallback'
               else 'unresolved'
           end as shipper_ownership_source,
           iff(importer_up_pit is null and importer_up_current is not null, 1, 0)
               as importer_current_parent_fallback_flag,
           iff(shipper_up_pit is null and shipper_up_current is not null, 1, 0)
               as shipper_current_parent_fallback_flag,
           iff(importer_up_pit is null and importer_up_current is null
               and importer_companyid is not null, 1, 0)
               as importer_self_fallback_flag,
           iff(shipper_up_pit is null and shipper_up_current is null
               and shipper_companyid is not null, 1, 0)
               as shipper_self_fallback_flag,
           iff(
               importer_up_pit is null
               and (importer_up_current is not null or importer_companyid is not null),
               1,
               0
           ) as importer_historical_backcast,
           iff(
               shipper_up_pit is null
               and (shipper_up_current is not null or shipper_companyid is not null),
               1,
               0
           ) as shipper_historical_backcast
    from ownership_inputs o
),
ownership as (
    select o.*,
           case
               when shipper_companyid is null and importer_companyid is null
                   then 'unmatched_both'
               when shipper_companyid is null then 'unmatched_shipper'
               when importer_companyid is null then 'unmatched_importer'
               when shipper_companyid = importer_companyid then 'self'
               when coalesce(shipper_up_pit, shipper_up_current, shipper_companyid)
                    = importer_companyid
                 or coalesce(importer_up_pit, importer_up_current, importer_companyid)
                    = shipper_companyid then 'parent_sub'
               when coalesce(shipper_up_pit, shipper_up_current, shipper_companyid)
                    = coalesce(
                        importer_up_pit, importer_up_current, importer_companyid
                    ) then 'sibling'
               else 'arms_length'
           end as relationship
    from ownership_mapped o
)""".strip()


def _finished_scope_ctes(
    description: DescriptionIdentity | None,
) -> str:
    sports_families = ", ".join(repr(family) for family in SPORTS_FAMILIES)
    escalated_families = ", ".join(repr(family) for family in ESCALATED_FAMILIES)
    eligible_families = ", ".join(repr(family) for family in ELIGIBLE_FAMILIES)
    probe_families = ", ".join(repr(family) for family in PROBE_FAMILIES)
    if description is None:
        description_ctes = """
description_enriched as (
    select a.*,
           0 as description_candidate,
           cast(null as number(38,0)) as description_candidate_parent_id,
           0 as description_match_count,
           0 as description_ambiguous,
           null as description_matched_alias,
           0 as description_alias_count
    from allocated a
)""".strip()
    else:
        description_ctes = """
description_match_rows as (
    select distinct o.panjivaRecordId,
           rm.manufacturer_parent_id,
           rm.description_alias
    from allocated o
    join reviewed_manufacturers rm
      on coalesce(o.importer_up, -1) not in ({id_list})
     and coalesce(o.shipper_up, -1) not in ({id_list})
     and lower(o.shipment_description) like
         '%' || rm.description_alias || '%'
),
description_matches as (
    select o.panjivaRecordId,
           iff(
               count(distinct o.manufacturer_parent_id) = 1,
               min(o.manufacturer_parent_id),
               null
           ) as description_candidate_parent_id,
           count(distinct o.manufacturer_parent_id) as description_match_count,
           iff(
               count(distinct o.description_alias) = 1,
               min(o.description_alias),
               null
           ) as description_matched_alias,
           count(distinct o.description_alias) as description_alias_count
    from description_match_rows o
    group by o.panjivaRecordId
),
description_enriched as (
    select a.*,
           iff(dm.description_match_count > 0, 1, 0) as description_candidate,
           dm.description_candidate_parent_id,
           coalesce(dm.description_match_count, 0) as description_match_count,
           iff(
               dm.description_match_count > 1 or dm.description_alias_count > 1,
               1,
               0
           ) as description_ambiguous,
           dm.description_matched_alias,
           coalesce(dm.description_alias_count, 0) as description_alias_count
    from allocated a
    left join description_matches dm
      on dm.panjivaRecordId = a.panjivaRecordId
)""".strip()
    return f"""
hs_scope as (
    select h.*,
           null as input_group,
           case
               when h.hs6 in ({sports_families})
                   then 'athletic_sports'
               when h.hs6 in ({escalated_families})
                   then 'athletic_escalated_general'
               else 'general_footwear_unreviewed'
           end as finished_market,
           case
               when h.hs6 in ({sports_families}) then 'reviewed_estimation'
               when h.hs6 in ({escalated_families})
                   then 'reviewed_escalated_estimation'
               else 'general_footwear_unreviewed'
           end as hs_review_status,
           iff(h.hs6 in ({eligible_families}), 1, 0) as hs_eligible
    from hs_with_counts h
    where h.hs6 in ({probe_families})
),
allocated as (
    select o.*,
           h.hs_full_code,
           h.hs6,
           h.n_hs6,
           h.input_group,
           h.finished_market,
           h.hs_review_status,
           h.hs_eligible,
           h.distinct_full_code_count,
           h.reviewed_full_code_count,
           h.unreviewed_full_code_count,
           h.mixed_review,
           1.0::double / n_hs6 as allocation_factor
    from ownership o
    join hs_scope h on h.panjivaRecordId = o.panjivaRecordId
),
{description_ctes},
attributed as (
    select d.*,
           case
               when importer_up in ({{id_list}})
                and shipper_up in ({{id_list}})
                and importer_up <> shipper_up then null
               when importer_up in ({{id_list}}) then importer_up
               when shipper_up in ({{id_list}}) then shipper_up
               else null
           end as manufacturer_parent_id,
           iff(
               importer_up in ({{id_list}})
               and shipper_up in ({{id_list}})
               and importer_up <> shipper_up,
               1,
               0
           ) as manufacturer_conflict,
           case
               when importer_up in ({{id_list}})
                and shipper_up in ({{id_list}})
                and importer_up <> shipper_up then 'conflict'
               when importer_up in ({{id_list}}) then 'importer_parent'
               when shipper_up in ({{id_list}}) then 'shipper_parent'
               when description_candidate = 1 then 'description_review'
               else 'unattributed'
           end as attribution_source
    from description_enriched d
),
routed as (
    select a.*,
           case
               when manufacturer_conflict = 1 then 'conflict'
               when attribution_source = 'importer_parent'
                and importer_up = shipper_up
                and relationship in ('self', 'parent_sub', 'sibling')
                   then 'manufacturer_direct'
               when attribution_source = 'shipper_parent'
                and relationship = 'arms_length'
                and nullif(trim(shipper_country), '') is not null
                and upper(trim(shipper_country)) not in (
                    'UNITED STATES', 'US', 'USA'
                )
                   then 'distributor_intermediated'
               else 'unattributed'
           end as import_route
           ,case
               when shipper_up is null then 'unknown_supplier'
               when importer_up = shipper_up
                and relationship in ('self', 'parent_sub', 'sibling')
                   then 'intragroup_supplier'
               when importer_up <> shipper_up and relationship = 'arms_length'
                   then 'external_supplier'
               else 'unknown_supplier'
           end as supplier_relationship
    from attributed a
),
finalized as (
    select r.*,
           iff(
               hs_eligible = 1
               and manufacturer_conflict = 0
               and description_ambiguous = 0
               and importer_xref_ambiguous = 0
               and shipper_xref_ambiguous = 0
               and importer_pit_ambiguous = 0
               and shipper_pit_ambiguous = 0
               and importer_historical_backcast = 0
               and shipper_historical_backcast = 0
               and (
                   import_route in (
                       'manufacturer_direct', 'distributor_intermediated'
                   )
                   or (
                       description_candidate = 1
                       and description_candidate_parent_id is not null
                   )
               ),
               1,
               0
           ) as review_pending_technically_eligible,
           iff(
               hs_eligible = 1
               and manufacturer_conflict = 0
               and description_candidate = 0
               and description_ambiguous = 0
               and importer_xref_ambiguous = 0
               and shipper_xref_ambiguous = 0
               and importer_pit_ambiguous = 0
               and shipper_pit_ambiguous = 0
               and import_route in (
                   'manufacturer_direct', 'distributor_intermediated'
               ),
               1,
               0
           ) as sensitivity_eligible,
           iff(
               hs_eligible = 1
               and manufacturer_conflict = 0
               and description_candidate = 0
               and description_ambiguous = 0
               and importer_xref_ambiguous = 0
               and shipper_xref_ambiguous = 0
               and importer_pit_ambiguous = 0
               and shipper_pit_ambiguous = 0
               and importer_historical_backcast = 0
               and shipper_historical_backcast = 0
               and import_route in (
                   'manufacturer_direct', 'distributor_intermediated'
               ),
               1,
               0
           ) as estimation_eligible
    from routed r
)""".strip()


def _diagnostic_aggregates() -> str:
    conditions = {
        "manufacturer_conflict": "manufacturer_conflict = 1",
        "importer_xref_unmatched": "importer_xref_distinct_candidate_count = 0",
        "shipper_xref_unmatched": "shipper_xref_distinct_candidate_count = 0",
        "importer_xref_ambiguous": "importer_xref_ambiguous = 1",
        "shipper_xref_ambiguous": "shipper_xref_ambiguous = 1",
        "importer_pit_ambiguous": "importer_pit_ambiguous = 1",
        "shipper_pit_ambiguous": "shipper_pit_ambiguous = 1",
        "importer_pit_same_parent_overlap": (
            "importer_pit_same_parent_overlap = 1"
        ),
        "shipper_pit_same_parent_overlap": (
            "shipper_pit_same_parent_overlap = 1"
        ),
        "importer_current_parent_fallback": (
            "importer_current_parent_fallback_flag = 1"
        ),
        "shipper_current_parent_fallback": (
            "shipper_current_parent_fallback_flag = 1"
        ),
        "importer_self_fallback": "importer_self_fallback_flag = 1",
        "shipper_self_fallback": "shipper_self_fallback_flag = 1",
        "importer_historical_backcast": "importer_historical_backcast = 1",
        "shipper_historical_backcast": "shipper_historical_backcast = 1",
        "description_candidate": "description_candidate = 1",
        "description_ambiguous": "description_ambiguous = 1",
        "unattributed": "import_route = 'unattributed'",
        "hs_review": "hs_eligible = 0",
        "main_ineligible": "estimation_eligible = 0",
    }
    expressions = []
    for name, condition in conditions.items():
        expressions.extend(
            (
                f"count(distinct iff({condition}, panjivaRecordId, null))\n"
                f"           as {name}_shipment_count_nonadditive",
                f"sum(iff({condition}, allocation_factor, 0))\n"
                f"           as {name}_shipment_equivalent",
                f"sum(iff({condition}, value_usd * allocation_factor, 0))\n"
                f"           as {name}_value_usd",
            )
        )
    return ",\n       ".join(expressions)


def _aggregate_select() -> str:
    group_columns = (
        "manufacturer_parent_id",
        "importer_companyid",
        "importer_up",
        "shpPanjivaId",
        "shipper_companyid",
        "shipper_up",
        "coalesce(nullif(trim(shpmtOrigin), ''), 'UNKNOWN')",
        "to_char(arrivalDate, 'YYYY') || 'Q' || quarter(arrivalDate)",
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
        "review_pending_technically_eligible",
        "sensitivity_eligible",
        "estimation_eligible",
    )
    group_by = ",\n         ".join(group_columns)
    return f"""
select manufacturer_parent_id,
       importer_companyid,
       importer_up,
       shpPanjivaId as shipper_panjiva_id,
       shipper_companyid,
       shipper_up,
       coalesce(nullif(trim(shpmtOrigin), ''), 'UNKNOWN') as origin_country,
       to_char(arrivalDate, 'YYYY') || 'Q' || quarter(arrivalDate) as year_quarter,
       hs_full_code,
       hs6,
       input_group,
       finished_market,
       hs_review_status,
       hs_eligible,
       distinct_full_code_count,
       reviewed_full_code_count,
       unreviewed_full_code_count,
       mixed_review,
       description_candidate_parent_id,
       description_match_count,
       description_ambiguous,
       description_matched_alias,
       description_alias_count,
       importer_xref_distinct_candidate_count,
       shipper_xref_distinct_candidate_count,
       importer_xref_ambiguous,
       shipper_xref_ambiguous,
       importer_pit_distinct_parent_candidate_count,
       shipper_pit_distinct_parent_candidate_count,
       importer_pit_distinct_interval_match_count,
       shipper_pit_distinct_interval_match_count,
       importer_pit_ambiguous,
       shipper_pit_ambiguous,
       importer_pit_same_parent_overlap,
       shipper_pit_same_parent_overlap,
       importer_ownership_source,
       shipper_ownership_source,
       importer_historical_backcast,
       shipper_historical_backcast,
       relationship,
       import_route,
       supplier_relationship,
       review_pending_technically_eligible,
       sensitivity_eligible,
       estimation_eligible,
       (select count(distinct panjivaRecordId) from finalized)
           as unique_shipment_count_nonadditive,
       count(distinct panjivaRecordId) as shipment_count_nonadditive,
       count(distinct panjivaRecordId)
           as shipment_count_compatibility_nonadditive,
       sum(allocation_factor) as shipment_equivalent,
       sum(value_usd * allocation_factor) as value_usd,
       sum(weight_kg * allocation_factor) as weight_kg,
       sum(teu * allocation_factor) as teu,
       sum(container_count * allocation_factor) as container_count,
       max(manufacturer_conflict) as manufacturer_conflict,
       max(description_candidate) as description_candidate,
       {_diagnostic_aggregates()}
from finalized
group by {group_by}
""".strip()


def _build_game_ctes(
    game: str,
    ids: tuple[int, int, int],
    start: str,
    end: str,
    description: DescriptionIdentity | None,
) -> str:
    if game != "finished":
        raise ValueError("the footwear pilot has a single finished game")
    scope = _finished_scope_ctes(description)
    return (
        "with "
        + _base_ctes(ids, start, end, description)
        + ",\n"
        + scope.format(id_list=_id_list(ids))
    )


def build_finished_sql(
    parent_ids: Mapping[str, int],
    date_start: str,
    date_end: str,
    description_column: DescriptionIdentity | None = None,
) -> str:
    """Build broad athletic-footwear probes with attribution diagnostics."""

    ids = _validate_parent_ids(parent_ids)
    start, end = _validate_dates(date_start, date_end)
    description = _validate_description_identity(description_column)
    sql = _build_game_ctes("finished", ids, start, end, description) + "\n" + _aggregate_select()
    assert_read_only_query(sql)
    return sql


def build_validation_sql(
    game: str,
    parent_ids: Mapping[str, int],
    date_start: str,
    date_end: str,
    description_column: DescriptionIdentity | None = None,
) -> str:
    """Build record-level G0 totals plus the separately labeled output row count."""

    ids = _validate_parent_ids(parent_ids)
    start, end = _validate_dates(date_start, date_end)
    description = _validate_description_identity(description_column)
    output_query = _aggregate_select()
    sql = (
        _build_game_ctes(game, ids, start, end, description)
        + "\nselect (\n"
        + "           select count(*) from (\n"
        + output_query
        + "\n           ) output_rows\n"
        + "       ) as output_row_count,\n"
        + "       count(distinct panjivaRecordId) as unique_shipment_count,\n"
        + "       coalesce(sum(value_usd * allocation_factor), 0) as value_usd,\n"
        + "       coalesce(sum(allocation_factor), 0) as shipment_equivalent\n"
        + "from finalized"
    )
    assert_read_only_query(sql)
    return sql
