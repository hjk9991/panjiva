"""Supplier-preserving, read-only Snowflake SQL for the tire AB-entry pilot."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from numbers import Integral
import re
from typing import Iterable

from .config import (
    APPROVED_DATABASE,
    APPROVED_SCHEMA,
    MANUFACTURER_PARENT_TARGETS,
    RAW_GROUPS,
    REVIEWED_ESTIMATION_CODES,
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


def _validate_parent_ids(parent_ids: Iterable[int]) -> tuple[int, int, int]:
    if not isinstance(parent_ids, Sequence) or isinstance(
        parent_ids, (str, bytes, bytearray)
    ):
        raise ValueError(
            "parent_ids must be an ordered sequence of exactly three unique "
            "positive integral values"
        )
    values = tuple(parent_ids)
    valid = (
        len(values) == 3
        and all(
            isinstance(value, Integral) and not isinstance(value, bool)
            for value in values
        )
        and all(int(value) > 0 for value in values)
        and len({int(value) for value in values}) == 3
    )
    if not valid:
        raise ValueError(
            "parent_ids must contain exactly three unique positive integral values"
        )
    return tuple(int(value) for value in values)  # type: ignore[return-value]


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
    for order, (parent_id, target) in enumerate(
        zip(parent_ids, MANUFACTURER_PARENT_TARGETS, strict=True)
    ):
        pattern = target.lower().replace("'", "''")
        rows.append(f"({order}, {parent_id}, '%{pattern}%')")
    return ",\n        ".join(rows)


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
    reviewed_codes = ", ".join(repr(code) for code in REVIEWED_ESTIMATION_CODES)
    return f"""
reviewed_manufacturers(
    manufacturer_order, manufacturer_parent_id, brand_pattern
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
           max(iff(hs_full_code in ({reviewed_codes}), 1, 0))
               as reviewed_code_match
    from hs_codes
    group by panjivaRecordId, hs6
),
hs_with_counts as (
    select panjivaRecordId,
           hs_full_code,
           hs6,
           reviewed_code_match,
           count(*) over (partition by panjivaRecordId) as n_hs6
    from hs6_codes
),
xref_ranked as (
    select identifierValue,
           companyId,
           count(*) over (partition by identifierValue) as xref_match_count,
           row_number() over (
               partition by identifierValue order by companyId
           ) as xref_rank
    from {_qualified('panjivaCompanyCrossRef')}
    where activeFlag = 1 and identifierValue is not null
),
xref as (
    select identifierValue, companyId, xref_match_count
    from xref_ranked
    where xref_rank = 1
),
pit_window as (
    select companyId, ultimateParentCompanyId, startDate, endDate
    from {_qualified('ciqCompanyUltimateParentPIT')}
    where startDate < '{end}' and endDate >= '{start}'
),
ownership_candidates as (
    select b.*,
           xc.companyId as importer_companyid,
           xs.companyId as shipper_companyid,
           xc.xref_match_count as importer_xref_match_count,
           xs.xref_match_count as shipper_xref_match_count,
           pc.ultimateParentCompanyId as importer_up_pit,
           ps.ultimateParentCompanyId as shipper_up_pit,
           cc.ultimateParentCompanyId as importer_up_current,
           cs.ultimateParentCompanyId as shipper_up_current
    from base b
    left join xref xc on xc.identifierValue = b.conPanjivaId
    left join xref xs on xs.identifierValue = b.shpPanjivaId
    left join pit_window pc
      on pc.companyId = xc.companyId
     and b.arrivalDate >= pc.startDate
     and b.arrivalDate <= pc.endDate
    left join pit_window ps
      on ps.companyId = xs.companyId
     and b.arrivalDate >= ps.startDate
     and b.arrivalDate <= ps.endDate
    left join {_qualified('ciqCompanyUltimateParent')} cc
      on cc.companyId = xc.companyId
    left join {_qualified('ciqCompanyUltimateParent')} cs
      on cs.companyId = xs.companyId
),
ownership_ranked as (
    select o.*,
           count(*) over (partition by panjivaRecordId) as ownership_join_rows,
           row_number() over (
               partition by panjivaRecordId
               order by importer_up_pit,
                        shipper_up_pit,
                        importer_up_current,
                        shipper_up_current
           ) as ownership_rank
    from ownership_candidates o
),
ownership as (
    select o.*,
           coalesce(importer_up_pit, importer_up_current, importer_companyid)
               as importer_up,
           coalesce(shipper_up_pit, shipper_up_current, shipper_companyid)
               as shipper_up,
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
    from ownership_ranked o
    where ownership_rank = 1
)""".strip()


def _raw_scope_cte() -> str:
    group_cases = []
    review_cases = []
    eligible_cases = []
    predicates = []
    for group_name, group in RAW_GROUPS.items():
        predicate = f"left(h.hs6, {len(group.hs_prefix)}) = '{group.hs_prefix}'"
        predicates.append(predicate)
        group_cases.append(f"when {predicate} then '{group_name}'")
        review_cases.append(f"when {predicate} then '{group.status}'")
        eligible_cases.append(
            f"when {predicate} then {1 if group.status == 'included' else 0}"
        )
    return f"""
hs_scope as (
    select h.*,
           case {' '.join(group_cases)} end as input_group,
           null as finished_market,
           case {' '.join(review_cases)} end as hs_review_status,
           case {' '.join(eligible_cases)} else 0 end as hs_eligible
    from hs_with_counts h
    where {' or '.join(predicates)}
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
           1.0::double / n_hs6 as allocation_factor
    from ownership o
    join hs_scope h on h.panjivaRecordId = o.panjivaRecordId
),
attributed as (
    select a.*,
           case
               when importer_up in ({{id_list}}) then importer_up
               else null
           end as manufacturer_parent_id,
           0 as manufacturer_conflict,
           0 as description_candidate,
           null as description_candidate_parent_id,
           0 as description_match_count,
           'importer_parent' as attribution_source
    from allocated a
    where importer_up in ({{id_list}})
),
routed as (
    select a.*,
           case
               when importer_up = shipper_up
                and relationship in ('self', 'parent_sub', 'sibling')
                   then 'manufacturer_direct'
               else 'unattributed'
           end as import_route
    from attributed a
),
finalized as (
    select r.*,
           iff(hs_eligible = 1 and import_route = 'manufacturer_direct', 1, 0)
               as estimation_eligible
    from routed r
)""".strip()


def _finished_scope_ctes(
    description: DescriptionIdentity | None,
) -> str:
    if description is None:
        description_ctes = """
description_enriched as (
    select a.*,
           0 as description_candidate,
           null as description_candidate_parent_id,
           0 as description_match_count
    from allocated a
)""".strip()
    else:
        description_ctes = """
description_matches as (
    select o.panjivaRecordId,
           min(rm.manufacturer_parent_id) as description_candidate_parent_id,
           count(distinct rm.manufacturer_parent_id) as description_match_count
    from allocated o
    join reviewed_manufacturers rm
      on coalesce(o.importer_up, -1) not in ({id_list})
     and coalesce(o.shipper_up, -1) not in ({id_list})
     and lower(o.shipment_description) like rm.brand_pattern
    group by o.panjivaRecordId
),
description_enriched as (
    select a.*,
           iff(dm.description_match_count > 0, 1, 0) as description_candidate,
           dm.description_candidate_parent_id,
           coalesce(dm.description_match_count, 0) as description_match_count
    from allocated a
    left join description_matches dm
      on dm.panjivaRecordId = a.panjivaRecordId
)""".strip()
    return f"""
hs_scope as (
    select h.*,
           null as input_group,
           case
               when reviewed_code_match = 1 and h.hs6 = '401110'
                   then 'passenger_vehicle'
               when reviewed_code_match = 1 and h.hs6 = '401120'
                   then 'light_truck_on_highway'
               else 'broad_unreviewed'
           end as finished_market,
           iff(
               reviewed_code_match = 1,
               'reviewed_estimation',
               'broad_unreviewed'
           ) as hs_review_status,
           reviewed_code_match as hs_eligible
    from hs_with_counts h
    where h.hs6 in ('401110', '401120')
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
                and shipper_country is not null
                and shipper_country <> 'United States'
                   then 'distributor_intermediated'
               else 'unattributed'
           end as import_route
    from attributed a
),
finalized as (
    select r.*,
           iff(
               hs_eligible = 1
               and manufacturer_conflict = 0
               and description_candidate = 0
               and import_route in (
                   'manufacturer_direct', 'distributor_intermediated'
               ),
               1,
               0
           ) as estimation_eligible
    from routed r
)""".strip()


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
        "relationship",
        "import_route",
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
       relationship,
       import_route,
       estimation_eligible,
       count(distinct panjivaRecordId) as shipment_count,
       sum(allocation_factor) as shipment_equivalent,
       sum(value_usd * allocation_factor) as value_usd,
       sum(weight_kg * allocation_factor) as weight_kg,
       sum(teu * allocation_factor) as teu,
       sum(container_count * allocation_factor) as container_count,
       max(manufacturer_conflict) as manufacturer_conflict,
       count(distinct iff(manufacturer_conflict = 1, panjivaRecordId, null))
           as manufacturer_conflict_shipment_count,
       sum(iff(manufacturer_conflict = 1, value_usd * allocation_factor, 0))
           as manufacturer_conflict_value_usd,
       count(distinct iff(importer_xref_match_count > 1, panjivaRecordId, null))
           as importer_xref_conflict_shipment_count,
       sum(iff(importer_xref_match_count > 1, value_usd * allocation_factor, 0))
           as importer_xref_conflict_value_usd,
       count(distinct iff(shipper_xref_match_count > 1, panjivaRecordId, null))
           as shipper_xref_conflict_shipment_count,
       sum(iff(shipper_xref_match_count > 1, value_usd * allocation_factor, 0))
           as shipper_xref_conflict_value_usd,
       count(distinct iff(importer_companyid is null, panjivaRecordId, null))
           as importer_xref_unmatched_shipment_count,
       sum(iff(importer_companyid is null, value_usd * allocation_factor, 0))
           as importer_xref_unmatched_value_usd,
       count(distinct iff(shipper_companyid is null, panjivaRecordId, null))
           as shipper_xref_unmatched_shipment_count,
       sum(iff(shipper_companyid is null, value_usd * allocation_factor, 0))
           as shipper_xref_unmatched_value_usd,
       count(distinct iff(ownership_join_rows > 1, panjivaRecordId, null))
           as pit_overlap_shipment_count,
       sum(iff(ownership_join_rows > 1, value_usd * allocation_factor, 0))
           as pit_overlap_value_usd,
       count(distinct iff(
           importer_current_parent_fallback_flag = 1, panjivaRecordId, null
       )) as importer_current_parent_fallback_shipment_count,
       sum(iff(
           importer_current_parent_fallback_flag = 1,
           value_usd * allocation_factor,
           0
       )) as importer_current_parent_fallback_value_usd,
       count(distinct iff(
           shipper_current_parent_fallback_flag = 1, panjivaRecordId, null
       )) as shipper_current_parent_fallback_shipment_count,
       sum(iff(
           shipper_current_parent_fallback_flag = 1,
           value_usd * allocation_factor,
           0
       )) as shipper_current_parent_fallback_value_usd,
       count(distinct iff(
           importer_self_fallback_flag = 1, panjivaRecordId, null
       )) as importer_self_fallback_shipment_count,
       sum(iff(
           importer_self_fallback_flag = 1, value_usd * allocation_factor, 0
       )) as importer_self_fallback_value_usd,
       count(distinct iff(
           shipper_self_fallback_flag = 1, panjivaRecordId, null
       )) as shipper_self_fallback_shipment_count,
       sum(iff(
           shipper_self_fallback_flag = 1, value_usd * allocation_factor, 0
       )) as shipper_self_fallback_value_usd,
       max(description_candidate) as description_candidate,
       count(distinct iff(description_candidate = 1, panjivaRecordId, null))
           as description_review_shipment_count,
       sum(iff(description_candidate = 1, value_usd * allocation_factor, 0))
           as description_review_value_usd,
       count(distinct iff(import_route = 'unattributed', panjivaRecordId, null))
           as unattributed_shipment_count,
       sum(iff(import_route = 'unattributed', value_usd * allocation_factor, 0))
           as unattributed_value_usd
from finalized
group by {group_by}
""".strip()


def build_raw_sql(
    parent_ids: Iterable[int],
    date_start: str,
    date_end: str,
    description_column: DescriptionIdentity | None = None,
) -> str:
    """Build broad raw-material imports for reviewed manufacturer importers."""

    ids = _validate_parent_ids(parent_ids)
    start, end = _validate_dates(date_start, date_end)
    description = _validate_description_identity(description_column)
    sql = (
        "with "
        + _base_ctes(ids, start, end, description)
        + ",\n"
        + _raw_scope_cte().format(id_list=_id_list(ids))
        + "\n"
        + _aggregate_select()
    )
    assert_read_only_query(sql)
    return sql


def build_finished_sql(
    parent_ids: Iterable[int],
    date_start: str,
    date_end: str,
    description_column: DescriptionIdentity | None = None,
) -> str:
    """Build broad finished-tire probes with reviewed attribution diagnostics."""

    ids = _validate_parent_ids(parent_ids)
    start, end = _validate_dates(date_start, date_end)
    description = _validate_description_identity(description_column)
    sql = (
        "with "
        + _base_ctes(ids, start, end, description)
        + ",\n"
        + _finished_scope_ctes(description).format(id_list=_id_list(ids))
        + "\n"
        + _aggregate_select()
    )
    assert_read_only_query(sql)
    return sql
