r"""
ex_20260806_wf2q_pull.py — within-firm 파일럿(2024 H1) L0 추출

목적
----
Within-Firm / Arm's Length 무역 DB 의 2분기(2024-01-01 ~ 2024-06-30) 파일럿 원천층(L0).
설계문서: lit_review_panjiva_ciq_within_firm_trade_20260805.md (star schema L0→L3).
`trade_ownership_pilot_1week` 과는 별개 산출물이며, 그 파일럿에서 검증된 SQL 패턴
(HS 재결합·PIT range join·표준 필터·crosswalk 규칙)을 그대로 가져와 확장했다.

추출 사유
--------
fact/panel 층(L1~L3) 구축의 원천. 수입 6개월 + 수출 6개월 + 기업·소유구조·분기재무.
실측 규모(2026-08-06): 수입 H1 7,237,772건(표준 필터 후), 수출 1,522,080건.

산출 (C:\panjiva\data\staging\within_firm_pilot_2q\L0\)
----
    imp_ship_YYYYMM.parquet   수입 선적 1건=1행 + crosswalk·PIT UP·관계분류 (월별 6개)
    imp_hs_YYYYMM.parquet     수입 선적×HS6 (자식 정규화, 월별 6개)
    exp_ship_YYYYMM.parquet   수출 선적 1건=1행 + crosswalk·PIT UP (월별 6개)
    exp_hs_YYYYMM.parquet     수출 선적×HS6 (월별 6개)
    company.parquet           등장 기업 전체의 CIQ 기준정보 (1회)
    fin_quarterly.parquet     등장 기업의 분기 재무 2023Q1~2024Q2 (1회)
    pit_intervals.parquet     등장 기업의 PIT 소유구조 전 구간 (1회)
    diag_pull.md              추출 진단 (행수·crosswalk 중복·소요시간)

사용법
------
    python scripts\extraction\ex_20260806_wf2q_pull.py --all           # 전체 (월별 순차)
    python scripts\extraction\ex_20260806_wf2q_pull.py --month 2024-03 # 한 달만
    python scripts\extraction\ex_20260806_wf2q_pull.py --once-only     # company/fin/pit만
    python scripts\extraction\ex_20260806_wf2q_pull.py --print-sql
이미 존재하는 청크 파일은 건너뛴다(재개 가능).

설계 메모
--------
* 수입 선적 SQL 은 1주치 파일럿에서 검증된 쿼리를 월 단위로 돌린다. 2024-03-01~07
  슬라이스가 254,873건으로 재현되는지가 L2 검증 게이트다.
* HS 재결합: 자식 값은 4,000자 경계에서 토큰 중간이 잘린다 → hsCodeId 순서로
  '구분자 없이' 이어붙여 복원. zfill 금지(4자리 HS 가 챕터를 바꾼다).
* crosswalk: activeFlag=1 만, primaryFlag 금지. identifierValue 중복(실측 극소수)은
  최소 companyId 로 결정적 해소(qualify) — 중복 규모는 diag 에 기록.
* PIT: 월과 겹치는 구간만 미리 좁혀 range join(43.5M 행 전체 join 회피).
  구간이 없으면 현재 스냅샷으로 대체 + fallback 플래그(1주치와 동일 규칙).
* 수출 BoL 에는 consignee 식별자가 아예 없다(36컬럼 전수 확인) — pair/within 불가.
  수출측은 미국 수출자(shipper) 정보만 확보하고, 목적지는
  shpmtDestination(채움 49.5%) + portOfUnladingCountry 를 함께 내려 L2 에서 coalesce.
* 분기재무: periodTypeId=2, periodEndDate 2023-01~2024-06. 계정은 ciq_dataitems.md 로
  확정한 7종(§3.5 의 sales/cogs/at/inventory/capx/ppent/dltt 대응). as-of 결합(미래정보
  차단)은 L3 에서 periodEndDate 기준으로 한다. 값은 백만 단위·원표시 통화.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import snowflake.connector

OUT_DIR = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q\L0")
MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
WINDOW_START, WINDOW_END = "2024-01-01", "2024-07-01"

# 재무 계정 (ciq_dataitems.md 확정) — 설계문서 §3.5: sales, cogs, at, inventory, capx, ppent, dltt
FIN_ITEMS = {
    28: "revenue",       # Total Revenues (팀 표준)
    34: "cogs",          # Cost of Goods Sold, Total
    1007: "assets",      # Total Assets
    1043: "inventory",   # Inventory
    2021: "capex",       # Capital Expenditure
    1004: "ppent",       # Net Property Plant And Equipment
    1049: "lt_debt",     # Long-Term Debt (Compustat dltt 대응)
}

# ---------------------------------------------------------------------------
# 0. 접속 — 비밀번호는 홈 폴더 .env 에서만. 저장소 안에 두지 않는다.
# ---------------------------------------------------------------------------
ENV_CANDIDATES = [
    Path.home() / ".snowflake.env",
    Path.home() / "OneDrive" / "Research" / "Panjiva" / ".env",
]


def load_env_file(path: Path) -> None:
    """python-dotenv 가 없는 인터프리터에서도 돌도록 KEY=VALUE 를 직접 읽는다."""
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def connect_kwargs() -> dict:
    for path in ENV_CANDIDATES:
        if path.exists():
            load_env_file(path)
            print(f"[env] {path}")
            break
    else:
        sys.exit(f"자격증명 파일을 찾을 수 없습니다: {ENV_CANDIDATES}")
    return dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
    )


# ---------------------------------------------------------------------------
# 공용 CTE 조각 — 수입 표준 필터 / crosswalk / PIT
# ---------------------------------------------------------------------------
CTE_XR = """
xr as (
    -- Panjiva 기업 ID -> CIQ companyId. activeFlag=1 만, primaryFlag 로 거르지 않는다.
    select identifierValue, companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
)"""

# ---------------------------------------------------------------------------
# 1. 수입 선적 층 (1주치 검증 쿼리, 월 단위) — 한 행 = 선적 1건
# ---------------------------------------------------------------------------
SQL_IMP_SHIP = """
with base as (
    -- 금액·TEU·중량은 이 층에서만 다룬다(자식 조인 전). 표준 필터 2종 적용.
    select i.panjivaRecordId, i.arrivalDate, i.billOfLadingType,
           i.conPanjivaId, i.conName, i.conCountry,
           i.shpPanjivaId, i.shpName, i.shpCountry,
           i.shpmtOrigin, i.portOfLadingCountry, i.portOfLading, i.portOfUnlading,
           i.weightKg, i.valueOfGoodsUSD, i.volumeTEU, i.numberOfContainers
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)  -- 미국 실착 화물만
      and (i.frob is null or i.frob <> 1)                           -- 타국행 통과화물 제외
),
hs_agg as (
    -- 자식 값은 한 칸에 ';' 로 이어지고, 4,000자를 넘으면 토큰 중간에서 잘려 여러 행이 된다.
    -- 반드시 hsCodeId(저장 순서)로 정렬해 '구분자 없이' 이어붙여야 잘린 코드가 복원된다.
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from panjivaUSImpHSCode h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
hs_rep as (
    -- 선적당 대표 HS 1개: Classified → Parsed → Manual 순 (1:1 유지)
    select panjivaRecordId,
           replace(coalesce(
                regexp_substr(allhs, 'Classified: ?([0-9][0-9.]*)', 1, 1, 'e', 1),
                regexp_substr(allhs, 'Parsed: ?([0-9][0-9.]*)',     1, 1, 'e', 1),
                regexp_substr(allhs, 'Manual: ?([0-9][0-9.]*)',     1, 1, 'e', 1)
           ), '.', '') as hs_raw
    from hs_agg
),
hs_cnt as (
    -- 재결합한 문자열을 ';' 로 나눠 서로 다른 HS6 이 몇 개인지 센다
    select a.panjivaRecordId,
           count(distinct left(replace(regexp_substr(t.value, '[0-9][0-9.]*'), '.', ''), 6)) as n_hs6
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
    group by a.panjivaRecordId
),
{cte_xr},
pit_wk as (
    -- 이 달과 겹치는 소유구조 구간만 미리 좁힌다(43.5M 행 전체 range join 회피).
    select companyId, ultimateParentCompanyId, startDate, endDate
    from ciqCompanyUltimateParentPIT
    where startDate <= '{date_end}' and endDate >= '{date_start}'
),
j as (
    select b.*,
           xc.companyId as con_companyid,
           xs.companyId as shp_companyid,
           pc.ultimateParentCompanyId as con_up_pit,
           ps.ultimateParentCompanyId as shp_up_pit,
           sc.ultimateParentCompanyId as con_up_snapshot,
           ss.ultimateParentCompanyId as shp_up_snapshot,
           r.hs_raw,
           coalesce(c.n_hs6, 0) as n_hs6
    from base b
    left join hs_rep r on r.panjivaRecordId = b.panjivaRecordId
    left join hs_cnt c on c.panjivaRecordId = b.panjivaRecordId
    left join xr xc on xc.identifierValue = b.conPanjivaId
    left join xr xs on xs.identifierValue = b.shpPanjivaId
    -- PIT: 도착일이 구간 안에 들어가는 행. 구간 중복 0건(실측)이라 최대 1행만 붙는다.
    left join pit_wk pc on pc.companyId = xc.companyId
                       and b.arrivalDate >= pc.startDate and b.arrivalDate <= pc.endDate
    left join pit_wk ps on ps.companyId = xs.companyId
                       and b.arrivalDate >= ps.startDate and b.arrivalDate <= ps.endDate
    -- 스냅샷: PIT 가 비었을 때의 대체값(fallback 플래그로 구분)
    left join ciqCompanyUltimateParent sc on sc.companyId = xc.companyId
    left join ciqCompanyUltimateParent ss on ss.companyId = xs.companyId
)
select panjivaRecordId, arrivalDate, billOfLadingType,
       conPanjivaId, conName, conCountry,
       shpPanjivaId, shpName, shpCountry,
       shpmtOrigin, portOfLadingCountry, portOfLading, portOfUnlading,
       weightKg, valueOfGoodsUSD, volumeTEU, numberOfContainers,
       left(hs_raw, 6) as hs6,
       left(hs_raw, 2) as hs2,
       length(hs_raw)  as hs_ndigits,
       n_hs6,
       iff(n_hs6 > 1, 1, 0) as hs_is_multi,
       con_companyid, shp_companyid,
       coalesce(con_up_pit, con_up_snapshot) as con_up,
       coalesce(shp_up_pit, shp_up_snapshot) as shp_up,
       iff(con_companyid is not null and con_up_pit is null, 1, 0) as con_ownership_is_fallback,
       iff(shp_companyid is not null and shp_up_pit is null, 1, 0) as shp_ownership_is_fallback,
       case
         when shp_companyid is null and con_companyid is null then 'unmatched_both'
         when shp_companyid is null or  con_companyid is null then 'unmatched_one'
         when shp_companyid = con_companyid                   then 'self'
         -- parent_sub 를 sibling 보다 먼저 본다. 그룹 최상위는 자기 자신이 UP 이라
         -- 순서를 뒤집으면 모-자 거래가 전부 sibling 으로 흡수된다.
         when coalesce(shp_up_pit, shp_up_snapshot) = con_companyid
           or coalesce(con_up_pit, con_up_snapshot) = shp_companyid then 'parent_sub'
         when coalesce(shp_up_pit, shp_up_snapshot)
            = coalesce(con_up_pit, con_up_snapshot)               then 'sibling'
         else 'arms_length'
       end as relationship
from j
"""

# ---------------------------------------------------------------------------
# 2. 선적×HS6 (fact_shipment_hs 원천) — 한 행 = 선적 × 고유 HS6
#    수입/수출 공용 본문 + 방향별 base CTE 로 조립한다.
# ---------------------------------------------------------------------------
SQL_HS_BASE_IMP = """
with base as (
    select i.panjivaRecordId
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
"""

SQL_HS_BASE_EXP = """
with base as (
    select e.panjivaRecordId
    from panjivaUSExport e
    where e.shpmtDate >= '{date_start}' and e.shpmtDate < '{date_end}'
),
"""

SQL_HS_BODY = """
hs_agg as (
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from {hs_table} h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
tok as (
    -- 재결합 문자열을 ';' 로 나눈 토큰. index 는 원 기재 순서(대표 HS 판단용).
    select a.panjivaRecordId,
           t.index as tok_idx,
           replace(regexp_substr(t.value, '[0-9][0-9.]*'), '.', '') as hs_raw
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
)
select panjivaRecordId,
       left(hs_raw, 6)  as hs6,
       count(*)         as n_tokens,       -- 이 HS6 이 몇 번 기재됐나
       min(tok_idx)     as first_tok_idx,  -- 첫 등장 순서 (대표 HS 결정용)
       min(length(hs_raw)) as hs_ndigits_min
from tok
where hs_raw is not null and length(hs_raw) >= 2
group by 1, 2
"""

# ---------------------------------------------------------------------------
# 3. 수출 선적 층 — 한 행 = 선적 1건 (consignee 없음: 미국 수출자만 식별)
# ---------------------------------------------------------------------------
SQL_EXP_SHIP = """
with base as (
    select e.panjivaRecordId, e.shpmtDate,
           e.shpPanjivaId, e.shpName, e.shpCountry, e.shpCity, e.shpStateRegion,
           e.shpmtDestination,
           e.portOfLading, e.portOfLadingCountry,
           e.portOfUnlading, e.portOfUnladingCountry,
           e.weightKg, e.weightT, e.volumeTEU, e.valueOfGoodsUSD,
           e.isContainerized
    from panjivaUSExport e
    where e.shpmtDate >= '{date_start}' and e.shpmtDate < '{date_end}'
),
hs_agg as (
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from panjivaUSExpHSCode h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
hs_rep as (
    select panjivaRecordId,
           replace(coalesce(
                regexp_substr(allhs, 'Classified: ?([0-9][0-9.]*)', 1, 1, 'e', 1),
                regexp_substr(allhs, 'Parsed: ?([0-9][0-9.]*)',     1, 1, 'e', 1),
                regexp_substr(allhs, 'Manual: ?([0-9][0-9.]*)',     1, 1, 'e', 1)
           ), '.', '') as hs_raw
    from hs_agg
),
hs_cnt as (
    select a.panjivaRecordId,
           count(distinct left(replace(regexp_substr(t.value, '[0-9][0-9.]*'), '.', ''), 6)) as n_hs6
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
    group by a.panjivaRecordId
),
{cte_xr},
pit_wk as (
    select companyId, ultimateParentCompanyId, startDate, endDate
    from ciqCompanyUltimateParentPIT
    where startDate <= '{date_end}' and endDate >= '{date_start}'
),
j as (
    select b.*,
           xs.companyId as shp_companyid,
           ps.ultimateParentCompanyId as shp_up_pit,
           ss.ultimateParentCompanyId as shp_up_snapshot,
           r.hs_raw,
           coalesce(c.n_hs6, 0) as n_hs6
    from base b
    left join hs_rep r on r.panjivaRecordId = b.panjivaRecordId
    left join hs_cnt c on c.panjivaRecordId = b.panjivaRecordId
    left join xr xs on xs.identifierValue = b.shpPanjivaId
    left join pit_wk ps on ps.companyId = xs.companyId
                       and b.shpmtDate >= ps.startDate and b.shpmtDate <= ps.endDate
    left join ciqCompanyUltimateParent ss on ss.companyId = xs.companyId
)
select panjivaRecordId, shpmtDate,
       shpPanjivaId, shpName, shpCountry, shpCity, shpStateRegion,
       shpmtDestination, portOfLading, portOfLadingCountry,
       portOfUnlading, portOfUnladingCountry,
       weightKg, weightT, volumeTEU, valueOfGoodsUSD, isContainerized,
       left(hs_raw, 6) as hs6,
       left(hs_raw, 2) as hs2,
       length(hs_raw)  as hs_ndigits,
       n_hs6,
       iff(n_hs6 > 1, 1, 0) as hs_is_multi,
       shp_companyid,
       coalesce(shp_up_pit, shp_up_snapshot) as shp_up,
       iff(shp_companyid is not null and shp_up_pit is null, 1, 0) as shp_ownership_is_fallback
from j
"""

def build_hs_sql(direction: str, ds: str, de: str) -> str:
    base = SQL_HS_BASE_IMP if direction == "imp" else SQL_HS_BASE_EXP
    table = "panjivaUSImpHSCode" if direction == "imp" else "panjivaUSExpHSCode"
    return base.format(date_start=ds, date_end=de) + SQL_HS_BODY.format(hs_table=table)


# ---------------------------------------------------------------------------
# 4. 기업 기준정보 (6개월 전체 등장 기업, 1회) — 수입 양측 + 수출 shipper
# ---------------------------------------------------------------------------
CTE_IDS_WINDOW = """
imp_ids as (
    select i.conPanjivaId, i.shpPanjivaId
    from panjivaUSImport i
    where i.arrivalDate >= '{window_start}' and i.arrivalDate < '{window_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
exp_ids as (
    select e.shpPanjivaId
    from panjivaUSExport e
    where e.shpmtDate >= '{window_start}' and e.shpmtDate < '{window_end}'
),
{cte_xr},
ids as (
    select distinct companyId from (
        select x.companyId from imp_ids b join xr x on x.identifierValue = b.conPanjivaId
        union all
        select x.companyId from imp_ids b join xr x on x.identifierValue = b.shpPanjivaId
        union all
        select x.companyId from exp_ids b join xr x on x.identifierValue = b.shpPanjivaId
    ) where companyId is not null
)"""

SQL_COMPANY = """
with {cte_ids},
fam as (
    -- 등장 기업집단의 규모(소속 법인 수). 펀드·트러스트 이상치 확인용.
    select up.ultimateParentCompanyId, count(*) as family_size
    from ciqCompanyUltimateParent up
    where up.ultimateParentCompanyId in (
        select u.ultimateParentCompanyId
        from ciqCompanyUltimateParent u join ids on ids.companyId = u.companyId)
    group by 1
)
select c.companyId                     as companyid,
       c.companyName                   as companyname,
       g.isoCountry2                   as country_iso2,
       g.country                       as country,
       c.simpleIndustryId              as simple_industry_id,
       si.simpleIndustryDescription    as industry,
       ct.companyTypeName              as company_type,
       cst.companyStatusTypeName       as company_status,
       up.ultimateParentCompanyId      as ultimate_parent_companyid,
       iff(c.companyId <> up.ultimateParentCompanyId, 1, 0) as is_subsidiary,
       f.family_size,
       upc.companyName                 as ultimate_parent_name,
       upt.companyTypeName             as ultimate_parent_type,
       upg.isoCountry2                 as ultimate_parent_country_iso2
from ids
join      ciqCompany            c   on c.companyId = ids.companyId
left join ciqCountryGeo         g   on g.countryId = c.countryId
left join ciqSimpleIndustry     si  on si.simpleIndustryId = c.simpleIndustryId
left join ciqCompanyType        ct  on ct.companyTypeId = c.companyTypeId
left join ciqCompanyStatusType  cst on cst.companyStatusTypeId = c.companyStatusTypeId
left join ciqCompanyUltimateParent up on up.companyId = c.companyId
left join fam f   on f.ultimateParentCompanyId = up.ultimateParentCompanyId
left join ciqCompany     upc on upc.companyId = up.ultimateParentCompanyId
left join ciqCompanyType upt on upt.companyTypeId = upc.companyTypeId
left join ciqCountryGeo  upg on upg.countryId = upc.countryId
"""

# ---------------------------------------------------------------------------
# 5. 분기 재무 (등장 기업, 2023Q1~2024Q2 전 분기, 1회)
#    as-of 선택은 L3 에서 periodEndDate 기준으로 한다 — 여기서는 전 분기 보존.
# ---------------------------------------------------------------------------
SQL_FIN_Q = """
with {cte_ids},
per as (
    -- 회사×분기 1행. 같은 기간에 정정본이 여러 개면 최신 신고분.
    select fp.financialPeriodId, fp.companyId, fp.calendarYear, fp.calendarQuarter,
           fp.periodEndDate, fp.currencyId, fp.filingDate
    from ciqLatestInstanceFinPeriod fp
    join ids on ids.companyId = fp.companyId
    where fp.periodTypeId = 2                                   -- 2 = 분기
      and fp.periodEndDate >= '2023-01-01'
      and fp.periodEndDate <  '{window_end}'
    qualify row_number() over (partition by fp.companyId, fp.periodEndDate
                               order by fp.filingDate desc) = 1
),
wide as (
    select p.companyId, p.calendarYear, p.calendarQuarter,
           p.periodEndDate, p.filingDate, p.currencyId,
           {pivot}
    from per p
    left join ciqFinancialData d on d.financialPeriodId = p.financialPeriodId
                                and d.dataItemId in ({item_ids})
    group by 1, 2, 3, 4, 5, 6
)
select w.companyId        as companyid,
       w.calendarYear     as cal_year,
       w.calendarQuarter  as cal_quarter,
       w.periodEndDate    as period_end,
       w.filingDate       as filing_date,
       cu.ISOCode         as fin_currency,
       {cols}
from wide w
left join ciqCurrency cu on cu.currencyId = w.currencyId
"""

# ---------------------------------------------------------------------------
# 6. PIT 소유구조 전 구간 (등장 기업, 1회) — dim_group_link 원천 + fallback 진단
# ---------------------------------------------------------------------------
SQL_PIT = """
with {cte_ids}
select p.companyId              as companyid,
       p.ultimateParentCompanyId as ultimate_parent_companyid,
       p.startDate              as start_date,
       p.endDate                as end_date
from ciqCompanyUltimateParentPIT p
join ids on ids.companyId = p.companyId
"""

# ---------------------------------------------------------------------------
# 7. 진단 — crosswalk identifierValue 중복 규모 (결정적 해소 규칙의 영향 범위)
# ---------------------------------------------------------------------------
SQL_DIAG_XR_DUP = """
select count(*) as n_dup_ids, sum(k) as n_rows_involved
from (
    select identifierValue, count(distinct companyId) as k
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    group by 1 having count(distinct companyId) > 1
)
"""


# ---------------------------------------------------------------------------
# 실행 헬퍼
# ---------------------------------------------------------------------------
def month_bounds(ym: str) -> tuple[str, str]:
    y, m = int(ym[:4]), int(ym[5:7])
    nxt = f"{y + 1}-01-01" if m == 12 else f"{y}-{m + 1:02d}-01"
    return f"{y}-{m:02d}-01", nxt


def fetch(cur, sql: str, label: str) -> pd.DataFrame:
    t0 = time.time()
    print(f"  · {label} ...", end="", flush=True)
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    print(f" {len(df):,}행 ({time.time() - t0:,.0f}s)")
    return df


def save(df: pd.DataFrame, path: Path, diag: list) -> None:
    # 코드성 컬럼은 문자열로 굳힌다 (zfill 금지)
    for c in ("hs6", "hs2"):
        if c in df.columns:
            s = df[c].astype("string").str.strip()
            df[c] = s.mask(s.eq("") | s.isna(), pd.NA)
    df.to_parquet(path, index=False)
    diag.append((path.name, len(df)))
    print(f"    -> {path.name} 저장")


def build_ids_cte() -> str:
    return CTE_IDS_WINDOW.format(window_start=WINDOW_START, window_end=WINDOW_END,
                                 cte_xr=CTE_XR.strip())


def build_fin_sql() -> str:
    pivot = ",\n           ".join(
        f"max(case when d.dataItemId = {k} then d.dataItemValue end) as {v}"
        for k, v in FIN_ITEMS.items())
    return SQL_FIN_Q.format(
        cte_ids=build_ids_cte(), window_end=WINDOW_END,
        item_ids=", ".join(str(k) for k in FIN_ITEMS),
        pivot=pivot,
        cols=", ".join(f"w.{v}" for v in FIN_ITEMS.values()),
    )


def pull_month(cur, ym: str, out: Path, diag: list, skip_existing: bool = True) -> None:
    ds, de = month_bounds(ym)
    tag = ym.replace("-", "")
    jobs = [
        (f"imp_ship_{tag}.parquet", SQL_IMP_SHIP.format(
            date_start=ds, date_end=de, cte_xr=CTE_XR.strip()), f"수입 선적 {ym}"),
        (f"imp_hs_{tag}.parquet", build_hs_sql("imp", ds, de), f"수입 HS {ym}"),
        (f"exp_ship_{tag}.parquet", SQL_EXP_SHIP.format(
            date_start=ds, date_end=de, cte_xr=CTE_XR.strip()), f"수출 선적 {ym}"),
        (f"exp_hs_{tag}.parquet", build_hs_sql("exp", ds, de), f"수출 HS {ym}"),
    ]
    for fname, sql, label in jobs:
        path = out / fname
        if skip_existing and path.exists():
            print(f"  · {label} — 이미 있음, 건너뜀")
            continue
        df = fetch(cur, sql, label)
        save(df, path, diag)


def pull_once(cur, out: Path, diag: list, skip_existing: bool = True) -> None:
    cte_ids = build_ids_cte()
    jobs = [
        ("company.parquet", SQL_COMPANY.format(cte_ids=cte_ids), "기업 기준정보"),
        ("fin_quarterly.parquet", build_fin_sql(), "분기 재무"),
        ("pit_intervals.parquet", SQL_PIT.format(cte_ids=cte_ids), "PIT 전 구간"),
    ]
    for fname, sql, label in jobs:
        path = out / fname
        if skip_existing and path.exists():
            print(f"  · {label} — 이미 있음, 건너뜀")
            continue
        df = fetch(cur, sql, label)
        save(df, path, diag)

    df = fetch(cur, SQL_DIAG_XR_DUP, "crosswalk 중복 진단")
    diag.append(("diag_xr_dup", f"중복 identifierValue {int(df.iloc[0, 0] or 0):,}개"))


def main() -> None:
    ap = argparse.ArgumentParser(description="within-firm 2Q 파일럿 L0 추출")
    ap.add_argument("--month", help="한 달만 (예: 2024-03)")
    ap.add_argument("--all", action="store_true", help="6개월 + 1회성 전부")
    ap.add_argument("--once-only", action="store_true", help="company/fin/pit 만")
    ap.add_argument("--force", action="store_true", help="기존 파일도 다시 추출")
    ap.add_argument("--print-sql", action="store_true")
    args = ap.parse_args()

    if args.print_sql:
        ds, de = month_bounds("2024-03")
        print(SQL_IMP_SHIP.format(date_start=ds, date_end=de, cte_xr=CTE_XR.strip()))
        print("\n" + "=" * 70 + "\n")
        print(SQL_COMPANY.format(cte_ids=build_ids_cte()))
        print("\n" + "=" * 70 + "\n")
        print(build_fin_sql())
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    diag: list = []
    skip = not args.force
    t0 = time.time()

    with snowflake.connector.connect(**connect_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute("alter session set statement_timeout_in_seconds = 3600")
            if args.month:
                pull_month(cur, args.month, OUT_DIR, diag, skip)
            elif args.once_only:
                pull_once(cur, OUT_DIR, diag, skip)
            elif args.all:
                for ym in MONTHS:
                    print(f"\n[{ym}]")
                    pull_month(cur, ym, OUT_DIR, diag, skip)
                print("\n[1회성]")
                pull_once(cur, OUT_DIR, diag, skip)
            else:
                ap.error("--month / --all / --once-only 중 하나를 지정하세요")

    lines = ["# L0 추출 진단 (ex_20260806_wf2q_pull.py)", "",
             f"- 실행: {pd.Timestamp.now():%Y-%m-%d %H:%M}, 총 {time.time() - t0:,.0f}초",
             f"- 기간: {WINDOW_START} ~ {WINDOW_END} (exclusive)", "",
             "| 산출물 | 행수/내용 |", "|---|---|"]
    lines += [f"| {n} | {v:,} |" if isinstance(v, int) else f"| {n} | {v} |"
              for n, v in diag]
    (OUT_DIR / "diag_pull.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n완료. 진단: {OUT_DIR / 'diag_pull.md'}")


if __name__ == "__main__":
    main()
