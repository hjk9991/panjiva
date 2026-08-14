"""Read-only Snowflake SQL builders for the AB-entry pilot."""

from datetime import date
from typing import Iterable

from .config import FIN_ITEMS, SECTORS


def _iso_date(value: str) -> str:
    """Validate and return an ISO date literal without accepting SQL fragments."""

    return date.fromisoformat(value).isoformat()


def _sector_predicate(sector_id: str) -> str:
    if sector_id not in SECTORS:
        raise ValueError(f"unapproved sector_id: {sector_id}")
    sector = SECTORS[sector_id]
    if sector["kind"] == "prefix":
        return f"left(hs6, {len(sector['hs6'])}) = '{sector['hs6']}'"
    return f"hs6 = '{sector['hs6']}'"


def build_trade_sql(
    sector_id: str,
    date_start: str,
    date_end: str,
    sample: str,
) -> str:
    """Build a server-side aggregate for one sector-period-sample chunk."""

    if sample not in {"main", "allocated"}:
        raise ValueError(f"unapproved sample: {sample}")
    start = _iso_date(date_start)
    end = _iso_date(date_end)
    if start >= end:
        raise ValueError("date_start must be before date_end")
    sector_predicate = _sector_predicate(sector_id)
    if sample == "main":
        assignment_filter = f"n_hs6 = 1 and {sector_predicate}"
        allocation = "1.0"
    else:
        assignment_filter = sector_predicate
        allocation = "1.0::double / n_hs6"

    return f"""
with base as (
    select i.panjivaRecordId, i.arrivalDate,
           i.conPanjivaId, i.shpPanjivaId, i.shpmtOrigin,
           iff(i.valueOfGoodsUSD < 0, null, i.valueOfGoodsUSD) as value_usd,
           iff(i.weightKg < 0, null, i.weightKg) as weight_kg,
           iff(i.volumeTEU < 0, null, i.volumeTEU) as teu
    from panjivaUSImport i
    where i.arrivalDate >= '{start}' and i.arrivalDate < '{end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
hs_agg as (
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from panjivaUSImpHSCode h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
hs_tokens as (
    select a.panjivaRecordId,
           replace(coalesce(
               regexp_substr(t.value, 'Classified: ?([0-9][0-9.]*)', 1, 1, 'e', 1),
               regexp_substr(t.value, 'Parsed: ?([0-9][0-9.]*)', 1, 1, 'e', 1),
               regexp_substr(t.value, 'Manual: ?([0-9][0-9.]*)', 1, 1, 'e', 1)
           ), '.', '') as hs_raw
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
),
hs_codes as (
    select distinct panjivaRecordId, left(hs_raw, 6) as hs6
    from hs_tokens
    where hs_raw is not null and length(hs_raw) >= 6
),
hs_with_counts as (
    select panjivaRecordId, hs6,
           count(*) over (partition by panjivaRecordId) as n_hs6
    from hs_codes
),
sector_assignment as (
    select panjivaRecordId, max(n_hs6) as n_hs6,
           sum({allocation}) as allocation_factor
    from hs_with_counts
    where {assignment_filter}
    group by panjivaRecordId
),
xr_ranked as (
    select identifierValue, companyId,
           count(*) over (partition by identifierValue) as xr_match_count,
           row_number() over (partition by identifierValue order by companyId) as xr_rank
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
),
xr as (
    select identifierValue, companyId, xr_match_count
    from xr_ranked
    where xr_rank = 1
),
pit_window as (
    select companyId, ultimateParentCompanyId, startDate, endDate
    from ciqCompanyUltimateParentPIT
    where startDate < '{end}' and endDate >= '{start}'
),
joined as (
    select b.*, s.n_hs6, s.allocation_factor,
           xc.companyId as con_companyid,
           xs.companyId as shp_companyid,
           xc.xr_match_count as con_xr_match_count,
           pc.ultimateParentCompanyId as con_up_pit,
           ps.ultimateParentCompanyId as shp_up_pit,
           cc.ultimateParentCompanyId as con_up_current,
           cs.ultimateParentCompanyId as shp_up_current
    from base b
    join sector_assignment s on s.panjivaRecordId = b.panjivaRecordId
    left join xr xc on xc.identifierValue = b.conPanjivaId
    left join xr xs on xs.identifierValue = b.shpPanjivaId
    left join pit_window pc on pc.companyId = xc.companyId
                            and b.arrivalDate >= pc.startDate
                            and b.arrivalDate <= pc.endDate
    left join pit_window ps on ps.companyId = xs.companyId
                            and b.arrivalDate >= ps.startDate
                            and b.arrivalDate <= ps.endDate
    left join ciqCompanyUltimateParent cc on cc.companyId = xc.companyId
    left join ciqCompanyUltimateParent cs on cs.companyId = xs.companyId
),
ranked as (
    select j.*,
           count(*) over (partition by panjivaRecordId) as ownership_join_rows,
           row_number() over (
               partition by panjivaRecordId
               order by con_up_pit, shp_up_pit, con_up_current, shp_up_current
           ) as ownership_rank
    from joined j
),
mapped as (
    select *,
           coalesce(con_up_pit, con_up_current, con_companyid) as importer_up,
           coalesce(shp_up_pit, shp_up_current, shp_companyid) as shipper_up,
           case when con_up_pit is not null then 'pit'
                when con_up_current is not null then 'current_fallback'
                when con_companyid is not null then 'self_fallback'
                else 'unmatched' end as up_source,
           case when shp_companyid is null and con_companyid is null then 'unmatched_both'
                when shp_companyid is null or con_companyid is null then 'unmatched_one'
                when shp_companyid = con_companyid then 'self'
                when coalesce(shp_up_pit, shp_up_current, shp_companyid) = con_companyid
                  or coalesce(con_up_pit, con_up_current, con_companyid) = shp_companyid
                  then 'parent_sub'
                when coalesce(shp_up_pit, shp_up_current, shp_companyid)
                   = coalesce(con_up_pit, con_up_current, con_companyid)
                  then 'sibling'
                else 'arms_length' end as relationship
    from ranked
    where ownership_rank = 1
)
select importer_up as ultimate_parent_companyid,
       '{sector_id}' as sector_id,
       coalesce(nullif(trim(shpmtOrigin), ''), 'UNKNOWN') as origin_country,
       to_char(arrivalDate, 'YYYY') || 'Q' || quarter(arrivalDate) as year_quarter,
       count(distinct panjivaRecordId) as shipment_count,
       sum(allocation_factor) as shipment_equivalent,
       sum(value_usd * allocation_factor) as value_usd,
       sum(weight_kg * allocation_factor) as weight_kg,
       sum(teu * allocation_factor) as teu,
       count(distinct con_companyid) as n_importer_legal_entities,
       count(distinct shpPanjivaId) as n_shipper_panjiva_entities,
       count(distinct shp_companyid) as n_shipper_ciq_entities,
       count(distinct shipper_up) as n_shipper_ultimate_parents,
       sum(iff(relationship in ('parent_sub', 'sibling'), value_usd * allocation_factor, 0))
           as value_internal_usd,
       sum(iff(relationship = 'arms_length', value_usd * allocation_factor, 0))
           as value_arms_length_usd,
       sum(iff(relationship = 'self', value_usd * allocation_factor, 0))
           as value_self_usd,
       sum(iff(relationship like 'unmatched%', value_usd * allocation_factor, 0))
           as value_unmatched_usd,
       sum(iff(up_source = 'pit', value_usd * allocation_factor, 0)) as value_up_pit_usd,
       sum(iff(up_source = 'current_fallback', value_usd * allocation_factor, 0))
           as value_up_current_fallback_usd,
       sum(iff(up_source = 'self_fallback', value_usd * allocation_factor, 0))
           as value_up_self_fallback_usd,
       sum(iff(con_xr_match_count > 1, value_usd * allocation_factor, 0))
           as crossref_conflict_value_usd,
       count(distinct iff(ownership_join_rows > 1, panjivaRecordId, null))
           as pit_overlap_shipment_count
from mapped
group by 1, 2, 3, 4
""".strip()


def build_financial_sql(
    company_ids: Iterable[int],
    date_start: str,
    date_end: str,
) -> str:
    """Build annual financial candidates and period-end USD conversions."""

    ids = sorted({int(company_id) for company_id in company_ids})
    if not ids:
        raise ValueError("company_ids must not be empty")
    start = _iso_date(date_start)
    end = _iso_date(date_end)
    if start >= end:
        raise ValueError("date_start must be before date_end")
    id_list = ", ".join(str(company_id) for company_id in ids)
    item_ids = ", ".join(str(item_id) for item_id in sorted(FIN_ITEMS))
    pivot = ",\n           ".join(
        f"max(iff(d.dataItemId = {item_id}, d.dataItemValue, null)) as {name}"
        for item_id, name in FIN_ITEMS.items()
    )
    native_columns = ", ".join(f"f.{name}" for name in FIN_ITEMS.values())
    usd_columns = ",\n       ".join(
        f"f.{name} / nullif(f.fx_per_usd, 0) as {name}_usd"
        for name in FIN_ITEMS.values()
        if name != "employees"
    )

    return f"""
with periods as (
    select fp.financialPeriodId, fp.companyId, fp.calendarYear,
           fp.periodEndDate, fp.filingDate, fp.currencyId,
           fp.restatementTypeId, fi.isRestatementTypeId
    from ciqLatestInstanceFinPeriod fp
    left join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId
                               and fi.latestForFinancialPeriodFlag = 1
    where fp.companyId in ({id_list})
      and fp.periodTypeId = 1
      and fp.periodEndDate >= '{start}' and fp.periodEndDate < '{end}'
    qualify row_number() over (
        partition by fp.companyId, fp.calendarYear
        order by fp.periodEndDate desc, fp.filingDate desc
    ) = 1
),
wide as (
    select p.financialPeriodId, p.companyId, p.calendarYear,
           p.periodEndDate, p.filingDate, p.currencyId,
           p.restatementTypeId, p.isRestatementTypeId,
           {pivot}
    from periods p
    left join ciqFinancialData d on d.financialPeriodId = p.financialPeriodId
                                and d.dataItemId in ({item_ids})
    group by 1, 2, 3, 4, 5, 6, 7, 8
),
fx_candidates as (
    select w.*, cu.ISOCode as fin_currency,
           iff(cu.ISOCode = 'USD', 1.0, er.priceClose) as fx_per_usd,
           er.priceDate as fx_date,
           row_number() over (
               partition by w.financialPeriodId
               order by iff(cu.ISOCode = 'USD', w.periodEndDate, er.priceDate) desc
           ) as fx_rank
    from wide w
    left join ciqCurrency cu on cu.currencyId = w.currencyId
    left join ciqExchangeRate er on er.currencyId = w.currencyId
                                and er.latestSnapFlag = 1
                                and er.priceDate <= w.periodEndDate
                                and er.priceDate >= dateadd(day, -14, w.periodEndDate)
),
final as (
    select *
    from fx_candidates
    where fx_rank = 1
)
select f.companyId as companyid,
       f.calendarYear as fin_calendar_year,
       f.periodEndDate as fin_period_end,
       f.filingDate as fin_filing_date,
       f.fin_currency, f.fx_per_usd, f.fx_date,
       f.restatementTypeId as fin_restatement_type_id,
       f.isRestatementTypeId as fin_instance_restatement_type_id,
       {native_columns},
       {usd_columns}
from final f
""".strip()
