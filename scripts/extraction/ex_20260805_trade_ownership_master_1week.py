r"""
ex_20260805_trade_ownership_master_1week.py
과제 03 `trade_ownership_master` 의 1주치 파일럿 추출.

목적
----
Panjiva 선적 + panjivaCompanyCrossRef + 소유구조(PIT) + CIQ 재무를 하나로 합친 결과가
실제로 어떤 모양인지 눈으로 확인하고, 그동안 미뤄둔 실측 항목 8가지를 한 번에 정리한다.

추출 사유
--------
`03_통합스키마_v0.1_초안.md` 의 테이블 모양을 확정하기 전 관통 테스트.
1주치(2024-03-01~03-07)는 팀 문서가 이 주간으로 여러 수치를 실측해둬서 대사가 가능하다.

산출
----
C:\panjiva\data\staging\trade_ownership_pilot_1week\
    01_shipment.parquet   선적 1건 = 1행 (약 25만)
    02_pair.parquet       (shipper, consignee) 쌍 = 1행, 양측 재무 부착
    03_firm.parquet       기업 1개 = 1행, 자기 재무 + 무역 집계
    90_checks.md          진단 8종 + 검증 결과

설계 메모
--------
* 선적 파일에는 재무를 붙이지 않는다. 재무는 기업×기간 단위라 25만 선적 행에 복제하면
  용량만 커지고 회귀에서 관측치 가중이 왜곡된다. 재무는 pair/firm 층에서 붙인다.
* pair/firm 집계는 SQL 재실행 없이 선적 결과에서 pandas 로 만든다(크레딧 절약 +
  세 파일의 합계가 정의상 일치).
* 팀 표준 `pull_panjiva_fixed.py` 의 SQL 을 가져오되 아래 3건을 고쳐 썼다.
    1) listagg 구분자 '|' 제거 → 잘린 토큰이 복원되지 않던 문제
       (실측 rid=287952173: id1 끝 '...8609.0' + id2 앞 '0; ...' → 8609.00)
    2) listagg 정렬을 hsCode 문자열 → hsCodeId 로. 문자열 정렬은 조각 순서를 뒤섞는다
    3) hs6 에 zfill(6) 금지. 4자리 HS 가 '008528' 이 되고 빈 값이 '000000' 이 된다
       → 원본 길이를 보존하고 hs_ndigits 로 드러낸다
* 소유구조는 PIT(시점별) 를 기본으로 쓰고, 스냅샷도 함께 받아 두 값을 대조한다.
  실측(2026-08-05): PIT 는 endDate 가 null 인 행이 0(전부 9999-12-31 sentinel),
  구간 중복 0건, dateTypeId 는 값이 1 하나뿐.

사용법
------
    python scripts\extraction\ex_20260805_trade_ownership_master_1week.py
    python ... --print-sql        # SQL 만 출력
    python ... --week-start 2024-03-01 --week-end 2024-03-08
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import snowflake.connector

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


OUT_DIR = Path(r"C:\panjiva\data\staging\trade_ownership_pilot_1week")

# 재무 계정 — ciq_dataitems.md / DATA_GUIDE B2. 값은 백만 단위, 원표시 통화.
FIN_ITEMS = {28: "revenue", 1007: "assets", 34: "cogs", 4371: "employees", 4051: "ebitda"}

# ---------------------------------------------------------------------------
# 1. 선적 층 — 한 행 = 선적 1건
# ---------------------------------------------------------------------------
SQL_SHIPMENT = """
with base as (
    -- 금액·TEU·중량은 이 층에서만 다룬다(자식 조인 전). 표준 필터 2종 적용.
    select i.panjivaRecordId, i.arrivalDate, i.fileDate, i.billOfLadingType,
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
xr as (
    -- Panjiva 기업 ID -> CIQ companyId. activeFlag=1 만, primaryFlag 로 거르지 않는다.
    -- identifierValue 는 사실상 고유(실측 중복 1건)라 이 조인은 행을 늘리지 않는다.
    select identifierValue, companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
),
pit_wk as (
    -- 이 주간과 겹치는 소유구조 구간만 미리 좁힌다(43.5백만 행 전체를 range join 하지 않기 위해).
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
    -- PIT: 도착일이 구간 안에 들어가는 행. 구간 중복이 0건이라 최대 1행만 붙는다(실측).
    left join pit_wk pc on pc.companyId = xc.companyId
                       and b.arrivalDate >= pc.startDate and b.arrivalDate <= pc.endDate
    left join pit_wk ps on ps.companyId = xs.companyId
                       and b.arrivalDate >= ps.startDate and b.arrivalDate <= ps.endDate
    -- 스냅샷: PIT 가 비었을 때의 대체값이자 두 소스 대조용
    left join ciqCompanyUltimateParent sc on sc.companyId = xc.companyId
    left join ciqCompanyUltimateParent ss on ss.companyId = xs.companyId
)
select panjivaRecordId, arrivalDate, fileDate, billOfLadingType,
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
       con_up_pit, shp_up_pit, con_up_snapshot, shp_up_snapshot,
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
# 2. 이 주간에 등장한 회사들의 기준정보
# ---------------------------------------------------------------------------
SQL_COMPANY = """
with base as (
    select i.conPanjivaId, i.shpPanjivaId
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
xr as (
    select identifierValue, companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
),
ids0 as (
    select distinct companyId from (
        select xc.companyId from base b join xr xc on xc.identifierValue = b.conPanjivaId
        union all
        select xs.companyId from base b join xr xs on xs.identifierValue = b.shpPanjivaId
    ) where companyId is not null
),
ids as (
    -- 무역 당사자 + 그들의 최종 모회사. 해외 모회사는 미국에 직접 수입하지 않으므로
    -- 당사자만 모으면 목록에서 빠지고, 그러면 그 그룹 전체가 재무 없는 것처럼 보인다.
    select distinct companyId from (
        select companyId from ids0
        union all
        select up.ultimateParentCompanyId
        from ciqCompanyUltimateParent up join ids0 on ids0.companyId = up.companyId
    ) where companyId is not null
),
fam as (
    -- 우리 표본에 등장한 기업집단의 규모(소속 법인 수). 펀드·트러스트 이상치 확인용.
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
       upt.companyTypeName             as ultimate_parent_type
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
"""

# ---------------------------------------------------------------------------
# 3. 재무 — 연간. **후보 기간을 전부 내려받고, 최종 선택(as-of)은 pandas 에서 한다.**
#
# ⚠️ SQL 단계에서 회사당 1행으로 잘라내면 표본 전체가 하나의 컷오프를 쓰게 된다.
#    1주치에서는 티가 안 나지만 기간을 2020~2025로 넓히면 2025년 선적에 2020년 기준
#    재무가 붙는다(조용히 어긋남). 그래서 여기서는 창(window) 안의 후보를 다 받고,
#    행별 기준시점으로 attach_financials() 가 고른다. 최대 소급은 4년.
# ---------------------------------------------------------------------------
SQL_FIN = """
with base as (
    select i.conPanjivaId, i.shpPanjivaId
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
xr as (
    select identifierValue, companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
),
ids0 as (
    select distinct companyId from (
        select xc.companyId from base b join xr xc on xc.identifierValue = b.conPanjivaId
        union all
        select xs.companyId from base b join xr xs on xs.identifierValue = b.shpPanjivaId
    ) where companyId is not null
),
ids as (
    -- ⚠️ 무역 당사자 + 그들의 최종 모회사를 함께 조회한다.
    -- 미국 판매법인은 재무를 공시하지 않고 본사가 공시하는데, 본사는 미국에 직접
    -- 수입하지 않아 당사자 목록에 안 잡힌다. 이 한 줄이 빠지면 삼성·Mazda 같은
    -- 그룹이 통째로 재무 없는 것으로 나온다(실측: 수입액 커버리지 6.0% → 43.3%).
    select distinct companyId from (
        select companyId from ids0
        union all
        select up.ultimateParentCompanyId
        from ciqCompanyUltimateParent up join ids0 on ids0.companyId = up.companyId
    ) where companyId is not null
),
per as (
    -- 회사×연도 1행으로 정리. 같은 기간에 정정본이 여러 개면 최신 신고분을 쓴다.
    -- 창(window)은 [표본시작 - 4년, 표본종료). 이 안의 후보를 **전부** 내려보낸다.
    select fp.financialPeriodId, fp.companyId, fp.calendarYear, fp.periodEndDate,
           fp.currencyId, fp.filingDate, fp.restatementTypeId,
           -- 손익계산서 정정 유형(사업범위 변경 식별용). latestForFinancialPeriodFlag=1 은
           -- 실측상 financialPeriodId 당 최대 1행이라 LEFT JOIN 이 행을 늘리지 않는다.
           fi.isRestatementTypeId
    from ciqLatestInstanceFinPeriod fp
    join ids on ids.companyId = fp.companyId
    left join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId
                               and fi.latestForFinancialPeriodFlag = 1
    where fp.periodTypeId = 1                                   -- 1 = 연간
      and fp.periodEndDate <  '{date_end}'
      and fp.periodEndDate >= dateadd(year, -4, to_date('{date_start}'))
    qualify row_number() over (partition by fp.companyId, fp.calendarYear
                               order by fp.periodEndDate desc, fp.filingDate desc) = 1
),
wide as (
    select p.companyId, p.calendarYear, p.periodEndDate, p.filingDate, p.currencyId,
           p.restatementTypeId, p.isRestatementTypeId,
           {pivot}
    from per p
    left join ciqFinancialData d on d.financialPeriodId = p.financialPeriodId
                                and d.dataItemId in ({item_ids})
    group by 1, 2, 3, 4, 5, 6, 7
)
-- ⚠️ fin_filing_date 는 '최초 공시일'이 아니다. ciqLatestInstanceFinPeriod 의 filingDate 는
--    그 기간의 **최신 인스턴스**가 실린 서류의 제출일이다. 회계기간은 이후 연차보고서에
--    비교 열로 계속 다시 실리므로 날짜가 몇 년 뒤로 갱신된다(실측: 결산일 대비 중위 453일,
--    연간 기간의 73.8%가 400~800일). 이 컬럼으로 point-in-time 필터를 걸면 실제로는
--    이미 공시돼 있던 재무가 대량으로 사라진다(파일럿 기준 80.9% 소실).
--    시점 통제가 필요하면 ciqFinInstance 를 조인해 financialPeriodId 별 min(filingDate) 를 쓸 것.
select w.companyId     as companyid,
       -- ⚠️ CIQ calendarYear 는 Compustat fyear 가 아니다(1월 결산만 전년, 2~12월은 종료 연도.
       --    Compustat 은 1~5월 결산을 전년으로 배정 → 3·4·5월 결산에서 100% 불일치).
       --    Compustat 과 대조할 때는 periodEndDate 에서 직접 계산할 것.
       w.calendarYear  as fin_calendar_year,
       w.periodEndDate as fin_period_end,
       w.filingDate    as fin_filing_date,   -- 최신 인스턴스 제출일 (위 주석 참조)
       cu.ISOCode      as fin_currency,
       rt.restatementTypeName as fin_restatement_type,
       -- 이 기간의 '최신' 인스턴스가 보도자료뿐이라는 뜻(감사 전 잠정치). 제외하지 말고 표시만 한다
       iff(w.restatementTypeId = 1, 1, 0) as fin_is_press_release,
       w.isRestatementTypeId as fin_is_restate_type_id,
       -- 5 Reclassified for Disposal · 6 동 in Amendment · 12 Discontinued Operations
       -- = 사업 범위가 바뀌어 손익 총액 자체가 달라진 기간. 관계분류(소유구조)와 재무의
       --   기업 경계가 어긋날 수 있으므로 표시해 둔다.
       iff(w.isRestatementTypeId in (5, 6, 12), 1, 0) as fin_perimeter_change,
       {cols}
from wide w
left join ciqCurrency cu on cu.currencyId = w.currencyId
left join ciqRestatementType rt on rt.restatementTypeId = w.restatementTypeId
-- ⚠️ 여기서 회사당 1행으로 자르지 않는다. 최종 선택은 attach_financials() 가 행별로 한다.
"""


def build_fin_sql(date_start: str, date_end: str) -> str:
    pivot = ",\n           ".join(
        f"max(case when d.dataItemId = {k} then d.dataItemValue end) as {v}"
        for k, v in FIN_ITEMS.items())
    return SQL_FIN.format(
        date_start=date_start, date_end=date_end,
        item_ids=", ".join(str(k) for k in FIN_ITEMS),
        pivot=pivot,
        cols=", ".join(f"w.{v}" for v in FIN_ITEMS.values()),
    )


# ---------------------------------------------------------------------------
# 4. 진단 쿼리 — 90_checks.md 재료
# ---------------------------------------------------------------------------
SQL_CHECK_PIT = """
select count(*) as rows_all,
       count(distinct companyId) as companies,
       sum(iff(endDate is null, 1, 0)) as enddate_null,
       count(distinct dateTypeId) as datetypeid_values,
       sum(iff(endDate = '9999-12-31', 1, 0)) as enddate_sentinel
from ciqCompanyUltimateParentPIT
"""

SQL_CHECK_PIT_INTERVALS = """
select iff(n = 1, '1개(스냅샷과 동일)', '2개 이상(이력 있음)') as bucket,
       count(*) as companies
from (select companyId, count(*) n from ciqCompanyUltimateParentPIT group by 1)
group by 1
"""

SQL_CHECK_CHILDJOIN = """
-- 자식 테이블을 그대로 LEFT JOIN 하면 합계가 얼마나 부풀는지. 행수로는 잘 안 드러난다.
with base as (
    select i.panjivaRecordId, i.volumeTEU, i.valueOfGoodsUSD
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
)
select 'A. 조인 전(정답)' as step, count(*) as rows_,
       count(distinct panjivaRecordId) as shipments,
       sum(volumeTEU) as teu, sum(valueOfGoodsUSD) as value_usd
from base
union all
select 'B. HS 자식 LEFT JOIN 후', count(*), count(distinct b.panjivaRecordId),
       sum(b.volumeTEU), sum(b.valueOfGoodsUSD)
from base b left join panjivaUSImpHSCode h on h.panjivaRecordId = b.panjivaRecordId
union all
select 'C. HS+컨테이너 2개 조인 후', count(*), count(distinct b.panjivaRecordId),
       sum(b.volumeTEU), sum(b.valueOfGoodsUSD)
from base b
left join panjivaUSImpHSCode h on h.panjivaRecordId = b.panjivaRecordId
left join panjivaUSImpContNum n on n.panjivaRecordId = b.panjivaRecordId
"""

SQL_CHECK_FIN_COVERAGE = """
with base as (
    select i.conPanjivaId, i.shpPanjivaId
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
xr as (
    select identifierValue, companyId from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
),
ids0 as (
    select distinct companyId from (
        select xc.companyId from base b join xr xc on xc.identifierValue = b.conPanjivaId
        union all
        select xs.companyId from base b join xr xs on xs.identifierValue = b.shpPanjivaId
    ) where companyId is not null
),
ids as (
    -- 실제 부착 대상과 같은 집합(무역 당사자 + 최종 모회사)
    select distinct companyId from (
        select companyId from ids0
        union all
        select up.ultimateParentCompanyId
        from ciqCompanyUltimateParent up join ids0 on ids0.companyId = up.companyId
    ) where companyId is not null
)
select (select count(*) from ids) as matched_companies,
       count(distinct iff(fp.periodTypeId = 1, fp.companyId, null)) as has_annual,
       count(distinct iff(fp.periodTypeId = 2, fp.companyId, null)) as has_quarterly
from ids
left join ciqLatestInstanceFinPeriod fp
       on fp.companyId = ids.companyId
      and fp.periodTypeId in (1, 2)
      -- ⚠️ 실제 부착 규칙(attach_financials)과 **같은 창**을 써야 §5 숫자가 어긋나지 않는다
      and fp.periodEndDate <  '{date_end}'
      and fp.periodEndDate >= dateadd(year, -4, to_date('{date_start}'))
"""

SQL_CHECK_HS_REASSEMBLY = """
-- 수정 1·2 검증: 문서 예시(rid=287952173)에서 잘린 '8609.00' 이 복원되는지
select
  (select count(*) from panjivaUSImpHSCode where panjivaRecordId = 287952173) as chunks,
  length(bad.s)  as len_wrong, length(good.s) as len_right,
  regexp_count(bad.s,  'Classified: ?8609[.]00') as codes_wrong_order,
  regexp_count(good.s, 'Classified: ?8609[.]00') as codes_right_order
from
  (select listagg(hsCode, '|') within group (order by hsCode) as s
   from panjivaUSImpHSCode where panjivaRecordId = 287952173) bad,
  (select listagg(hsCode, '')  within group (order by hsCodeId) as s
   from panjivaUSImpHSCode where panjivaRecordId = 287952173) good
"""


# ---------------------------------------------------------------------------
# 5. 실행 헬퍼
# ---------------------------------------------------------------------------
def md_table(df: pd.DataFrame, index: bool = False) -> str:
    """마크다운 표. tabulate 가 공용 환경에 없어 직접 만든다(패키지 추가 없이)."""
    d = df.reset_index() if index else df

    def fmt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
            return ""
        if isinstance(v, float):
            return f"{v:,.2f}" if abs(v) < 1e6 else f"{v:,.0f}"
        if isinstance(v, (int,)) or str(type(v)).find("int") > -1:
            try:
                return f"{int(v):,}"
            except (TypeError, ValueError):
                return str(v)
        return str(v)

    head = "| " + " | ".join(str(c) for c in d.columns) + " |"
    rule = "|" + "|".join("---" for _ in d.columns) + "|"
    body = ["| " + " | ".join(fmt(v) for v in row) + " |"
            for row in d.itertuples(index=False, name=None)]
    return "\n".join([head, rule] + body)


def fetch(cur, sql: str, label: str) -> pd.DataFrame:
    print(f"  · {label} ...", end="", flush=True)
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]   # Snowflake 는 대문자로 돌려준다
    print(f" {len(df):,}행")
    return df


def tidy_ship(df: pd.DataFrame) -> pd.DataFrame:
    """저장 직전 타입 고정. parquet 은 쓸 때의 타입을 굳히므로 여기가 마지막 방어선."""
    # 코드성 컬럼은 문자열. ⚠️ zfill 금지 — 4자리 HS 가 '008528' 이 되어 챕터가 뒤바뀐다.
    for c in ("hs6", "hs2"):
        s = df[c].astype("string").str.strip()
        df[c] = s.mask(s.eq("") | s.isna(), pd.NA)
    int_cols = [
        "panjivarecordid", "conpanjivaid", "shppanjivaid", "numberofcontainers",
        "con_companyid", "shp_companyid", "con_up", "shp_up",
        "con_up_pit", "shp_up_pit", "con_up_snapshot", "shp_up_snapshot",
        "n_hs6", "hs_ndigits", "hs_is_multi",
        "con_ownership_is_fallback", "shp_ownership_is_fallback",
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("weightkg", "valueofgoodsusd", "volumeteu"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("arrivaldate", "filedate"):
        df[c] = pd.to_datetime(df[c], errors="coerce")

    df["intra_group"] = (
        df["relationship"].map({"parent_sub": 1, "sibling": 1, "arms_length": 0})
        .astype("Int64"))          # 미매칭은 0 이 아니라 결측 — 시장거래로 세면 안 된다
    df["unit_value_usd_per_kg"] = (
        df["valueofgoodsusd"] / df["weightkg"].where(df["weightkg"] > 0))
    return df


# 재무 컬럼 목록 — attach_financials 가 접두어를 붙여 옮긴다
FIN_META = ["fin_calendar_year", "fin_period_end", "fin_filing_date", "fin_currency",
            "fin_restatement_type", "fin_is_press_release",
            "fin_is_restate_type_id", "fin_perimeter_change"]
FIN_COLS = FIN_META + list(FIN_ITEMS.values())

MAX_LOOKBACK = pd.Timedelta(days=365 * 4)   # SQL 창(dateadd(year,-4))과 같은 값


def attach_financials(panel: pd.DataFrame, fin: pd.DataFrame, key: str,
                      ref_col: str, prefix: str) -> pd.DataFrame:
    """기준시점(`ref_col`) **이전에 끝난** 회계연도 중 가장 최근 1개를 행별로 붙인다.

    ⚠️ 이 선택을 SQL 에서 한 번에 하면 표본 전체가 같은 컷오프를 쓰게 된다.
       1주치는 티가 안 나지만 기간을 넓히면 최근 선적에 몇 년 전 재무가 붙는다.
       `merge_asof` 는 규칙(뒤로만, 4년 이내, 결산일==기준일은 제외)이 코드에 그대로 드러난다.
    """
    right = (fin.dropna(subset=["companyid", "fin_period_end"])
                .assign(_k=lambda d: d["companyid"].astype("int64"))
                .sort_values("fin_period_end")[["_k", *FIN_COLS]])
    left = (panel.assign(_k=lambda d: d[key].astype("int64"))
                 .sort_values(ref_col))
    out = pd.merge_asof(
        left, right,
        left_on=ref_col, right_on="fin_period_end", by="_k",
        direction="backward",              # 기준시점보다 **과거**만
        tolerance=MAX_LOOKBACK,            # 4년 넘게 묵은 재무는 안 붙임
        allow_exact_matches=False,         # 결산일 == 기준일이면 아직 공시 전
    ).drop(columns="_k")
    out[f"{prefix}fin_age_days"] = (out[ref_col] - out["fin_period_end"]).dt.days
    out[f"{prefix}has_financials"] = out["fin_calendar_year"].notna().astype("int8")
    return out.rename(columns={c: f"{prefix}{c}" for c in FIN_COLS})


def _value_split(sub: pd.DataFrame, key, prefix: str) -> pd.DataFrame:
    """관계 유형별 금액 분해 — 03_firm 과 04_group 이 **같은 정의**를 쓰도록 한 곳에 모은다.

    `_intra_share` 의 분모는 양쪽 다 **분류된 거래**(intra + arms_length)다.
    미매칭을 분모에 넣으면 매칭률이 낮은 기업일수록 비중이 기계적으로 낮아진다.
    `self`(양측 동일 법인)는 어느 쪽에도 넣지 않고 따로 뺀다 — v0.2 미결 #9.
    """
    INTRA = ("parent_sub", "sibling")
    rel, val = sub["relationship"], sub["valueofgoodsusd"]
    parts = sub.assign(
        _internal=val.where(rel.isin(INTRA), 0),
        _arms=val.where(rel.eq("arms_length"), 0),
        _self=val.where(rel.eq("self"), 0),
        _unmatched=val.where(rel.str.startswith("unmatched"), 0),
    ).groupby(key)[["_internal", "_arms", "_self", "_unmatched"]].sum()
    parts.columns = [f"{prefix}_value_internal", f"{prefix}_value_arms",
                     f"{prefix}_value_self", f"{prefix}_value_unmatched"]
    classified = parts[f"{prefix}_value_internal"] + parts[f"{prefix}_value_arms"]
    parts[f"{prefix}_value_classified"] = classified
    parts[f"{prefix}_intra_share"] = (
        parts[f"{prefix}_value_internal"] / classified).where(classified > 0)
    return parts


def build_pair(ship: pd.DataFrame, co: pd.DataFrame, fin: pd.DataFrame) -> pd.DataFrame:
    """한 행 = (shipper, consignee) 쌍. 양측이 모두 매칭된 선적만 대상."""
    both = ship[ship["con_companyid"].notna() & ship["shp_companyid"].notna()].copy()
    g = both.groupby(["shp_companyid", "con_companyid"], dropna=False)
    pair = g.agg(
        relationship=("relationship", "first"),
        # relationship 은 '선적별' 속성이다(PIT 소유구조가 바뀌면 같은 쌍이라도 값이 달라짐).
        # first 로 뭉개면 인수·분할 시점이 조용히 사라지므로 불일치 건수를 같이 센다.
        n_relationship=("relationship", "nunique"),
        intra_group=("intra_group", "first"),
        n_ship=("panjivarecordid", "nunique"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
        n_hs6=("hs6", "nunique"),
        first_arrival=("arrivaldate", "min"),
        last_arrival=("arrivaldate", "max"),
    ).reset_index()
    pair["unit_value_usd_per_kg"] = pair["value_usd"] / pair["weight_kg"].where(pair["weight_kg"] > 0)
    keys = ["shp_companyid", "con_companyid"]
    for col, out in (("hs2", "top_hs2"), ("hs6", "top_hs6")):
        pair = pair.merge(_top_by_value(both, keys, col, out), on=keys, how="left")

    pair["period_start"] = pd.Timestamp(ship["arrivaldate"].min())
    pair["period_end"] = pd.Timestamp(ship["arrivaldate"].max())
    for side, key in (("shp", "shp_companyid"), ("con", "con_companyid")):
        c = co.add_prefix(f"{side}_").rename(columns={f"{side}_companyid": key})
        pair = pair.merge(c, on=key, how="left")
        pair = attach_financials(pair, fin, key, "period_start", f"{side}_")
    return pair


def build_firm(ship: pd.DataFrame, co: pd.DataFrame, fin: pd.DataFrame) -> pd.DataFrame:
    """한 행 = 기업 1개. 수입자로서의 실적(imp_*)과 수출자로서의 실적(exp_*)을 나란히."""
    blocks = []
    for prefix, key in (("imp", "con_companyid"), ("exp", "shp_companyid")):
        sub = ship[ship[key].notna()].copy()
        g = sub.groupby(key)
        other = "shp_companyid" if key == "con_companyid" else "con_companyid"
        b = g.agg(**{
            f"{prefix}_n_ship": ("panjivarecordid", "nunique"),
            f"{prefix}_value_usd": ("valueofgoodsusd", "sum"),
            f"{prefix}_weight_kg": ("weightkg", "sum"),
            f"{prefix}_teu": ("volumeteu", "sum"),
            f"{prefix}_n_partners": (other, "nunique"),
        })
        b = b.join(_value_split(sub, key, prefix), how="left")
        for col, out in (("hs2", f"{prefix}_top_hs2"),
                         ("shpmtorigin", f"{prefix}_top_partner_country")):
            b = b.join(_top_by_value(sub, [key], col, out).set_index(key), how="left")
        b.index.name = "companyid"
        blocks.append(b)

    firm = blocks[0].join(blocks[1], how="outer").reset_index()
    firm = firm.merge(co, on="companyid", how="left")
    firm["period_start"] = pd.Timestamp(ship["arrivaldate"].min())
    firm["period_end"] = pd.Timestamp(ship["arrivaldate"].max())
    return attach_financials(firm, fin, "companyid", "period_start", "")


def build_group(ship: pd.DataFrame, co: pd.DataFrame, fin: pd.DataFrame) -> pd.DataFrame:
    """한 행 = 기업집단(최종 모회사). 무역은 소속 법인 합산, 재무는 **모회사 것 하나**.

    ⚠️ 재무를 소속 법인끼리 더하면 안 된다 — 모회사의 연결 재무에 자회사 실적이 이미
       들어 있어 이중계상이 된다. entity 패널(03_firm)과 이 파일을 UNION 하는 것도 금지.
    """
    blocks = []
    for prefix, key, other in (("imp", "con_up", "shp_up"), ("exp", "shp_up", "con_up")):
        sub = ship[ship[key].notna()].copy()
        member = "con_companyid" if key == "con_up" else "shp_companyid"
        g = sub.groupby(key)
        b = g.agg(**{
            f"{prefix}_n_ship": ("panjivarecordid", "nunique"),
            f"{prefix}_value_usd": ("valueofgoodsusd", "sum"),
            f"{prefix}_weight_kg": ("weightkg", "sum"),
            f"{prefix}_teu": ("volumeteu", "sum"),
            f"{prefix}_n_members": (member, "nunique"),       # 이 표본에 등장한 소속 법인 수
            f"{prefix}_n_partner_groups": (other, "nunique"),
        })
        # 03_firm 과 **같은 정의**로 분해한다(분모 = 분류된 거래, self 는 별도).
        # 집단 단위에서 '내부거래'는 상대의 최종 모회사가 나와 같은 거래인데,
        # 그것이 곧 parent_sub + sibling 이므로 _value_split 과 결과가 일치한다.
        b = b.join(_value_split(sub, key, prefix), how="left")
        for col, out in (("hs2", f"{prefix}_top_hs2"),
                         ("shpmtorigin", f"{prefix}_top_partner_country")):
            b = b.join(_top_by_value(sub, [key], col, out).set_index(key), how="left")
        b.index.name = "ultimate_parent_companyid"
        blocks.append(b)

    grp = blocks[0].join(blocks[1], how="outer").reset_index()

    # 모회사 자신의 기준정보·재무를 붙인다(합산 아님).
    # ⚠️ co 에도 ultimate_parent_companyid 컬럼이 있으므로 rename 전에 필요한 것만 골라낸다.
    keep = ["companyid", "companyname", "country_iso2", "country",
            "industry", "company_type", "company_status", "family_size"]
    co_up = (co[[c for c in keep if c in co.columns]]
             .rename(columns={"companyid": "ultimate_parent_companyid"}))
    grp = grp.merge(co_up, on="ultimate_parent_companyid", how="left")
    grp["period_start"] = pd.Timestamp(ship["arrivaldate"].min())
    grp["period_end"] = pd.Timestamp(ship["arrivaldate"].max())
    return attach_financials(grp, fin, "ultimate_parent_companyid", "period_start", "")


def _top_by_value(df: pd.DataFrame, keys: list, col: str, out: str) -> pd.DataFrame:
    """그룹별로 금액이 가장 큰 값 1개(대표값). 결측은 제외하고 고른다.
    반환은 DataFrame[keys + [out]] — 인덱스 정렬 사고를 피하려고 merge/join 용으로 준다."""
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=keys + [out])
    return (sub.groupby(keys + [col], dropna=False)["valueofgoodsusd"].sum()
               .reset_index()
               # 금액 동률일 때 실행마다 값이 달라지지 않도록 안정 정렬 + 결정적 타이브레이크
               .sort_values(["valueofgoodsusd", col], ascending=[False, True],
                            kind="mergesort")
               .drop_duplicates(keys)
               .rename(columns={col: out})[keys + [out]])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--week-start", default="2024-03-01")
    ap.add_argument("--week-end", default="2024-03-08", help="이 날짜 미만(exclusive)")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    ap.add_argument("--print-sql", action="store_true")
    args = ap.parse_args()

    ds, de = args.week_start, args.week_end
    run_date = date.today().isoformat()          # 카탈로그·리포트에 하드코딩하지 않는다
    sql_ship = SQL_SHIPMENT.format(date_start=ds, date_end=de)

    if args.print_sql:
        print(sql_ship)
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    conn_kw = connect_kwargs()

    print(f"\n[1/4] Snowflake 조회 ({ds} ~ {de})")
    with snowflake.connector.connect(**conn_kw) as conn:
        with conn.cursor() as cur:
            ship = fetch(cur, sql_ship, "선적 층")
            co = fetch(cur, SQL_COMPANY.format(date_start=ds, date_end=de), "기업 기준정보")
            fin = fetch(cur, build_fin_sql(ds, de), "재무(연간, 후보 전체)")
            chk = {
                "pit": fetch(cur, SQL_CHECK_PIT, "진단 PIT 프로파일"),
                "pit_iv": fetch(cur, SQL_CHECK_PIT_INTERVALS, "진단 PIT 구간수"),
                "childjoin": fetch(cur, SQL_CHECK_CHILDJOIN.format(date_start=ds, date_end=de),
                                   "진단 자식조인 부풀림"),
                "fincov": fetch(cur, SQL_CHECK_FIN_COVERAGE.format(date_start=ds, date_end=de),
                                "진단 재무 커버리지"),
                "hsfix": fetch(cur, SQL_CHECK_HS_REASSEMBLY, "진단 HS 재결합"),
            }

    print("\n[2/4] 타입 정리 · 집계")
    ship = tidy_ship(ship)
    for c in ("companyid", "ultimate_parent_companyid", "family_size", "is_subsidiary"):
        if c in co.columns:
            co[c] = pd.to_numeric(co[c], errors="coerce").astype("Int64")
    fin["companyid"] = pd.to_numeric(fin["companyid"], errors="coerce").astype("Int64")
    for c in ("fin_calendar_year", "fin_is_press_release",
              "fin_is_restate_type_id", "fin_perimeter_change"):
        fin[c] = pd.to_numeric(fin[c], errors="coerce").astype("Int64")
    for c in ("fin_period_end", "fin_filing_date"):
        fin[c] = pd.to_datetime(fin[c], errors="coerce")

    # 선적 파일에도 회사 이름·산업 정도는 붙여 둔다(눈으로 볼 때 필요). 재무는 붙이지 않는다.
    co_lite = co[["companyid", "companyname", "country_iso2", "industry",
                  "company_type", "family_size"]]
    for side, key in (("con", "con_companyid"), ("shp", "shp_companyid")):
        c = co_lite.add_prefix(f"{side}_ciq_").rename(columns={f"{side}_ciq_companyid": key})
        ship = ship.merge(c, on=key, how="left")

    pair = build_pair(ship, co, fin)
    firm = build_firm(ship, co, fin)
    group = build_group(ship, co, fin)

    print("\n[3/4] 저장")
    paths = {}
    for name, df in (("01_shipment", ship), ("02_pair", pair),
                     ("03_firm", firm), ("04_group", group)):
        p = out_dir / f"{name}.parquet"
        df.columns = [str(c).lower() for c in df.columns]
        df.to_parquet(p, index=False, compression="snappy")
        paths[name] = p
        print(f"  {p.name:<20} {len(df):>9,}행  {p.stat().st_size/1e6:>7.1f} MB")

    print("\n[4/4] 진단 리포트")
    write_checks(out_dir / "90_checks.md", ship, pair, firm, group, co, fin, chk,
                 ds, de, run_date)
    print(f"  {out_dir / '90_checks.md'}")

    print("\n" + "=" * 78)
    print("_catalog.md 에 붙일 줄 (C:\\panjiva\\data\\staging\\_catalog.md)")
    print("=" * 78)
    folder = out_dir.name
    desc = {
        "01_shipment": "미국 수입 선적 + 양측 CIQ 매칭·모기업(PIT)·관계분류·대표HS",
        "02_pair": "수출자–수입자 쌍 집계 + 양측 재무 (양측 매칭 건만)",
        "03_firm": "법인 단위 집계(수입/수출 분리) + 자기 재무",
        "04_group": "기업집단(최종 모회사) 단위 집계 + 모회사 재무",
    }
    days = (pd.Timestamp(de) - pd.Timestamp(ds)).days
    for name, df in (("01_shipment", ship), ("02_pair", pair),
                     ("03_firm", firm), ("04_group", group)):
        print(f"| `{folder}/{name}.parquet` | {desc[name]} | {ds}~{de} ({days}일) | "
              f"{len(df):,} | `{Path(__file__).name}` | {run_date} | 김영수 |")


def write_checks(path, ship, pair, firm, group, co, fin, chk, ds, de, run_date) -> None:
    """진단 8종 + 검증. 숫자만 적지 않고 '그래서 뭘 뜻하는지'를 같이 적는다."""
    n = len(ship)
    both = ship["con_companyid"].notna() & ship["shp_companyid"].notna()
    val = ship["valueofgoodsusd"]
    rel = ship["relationship"].value_counts()
    rel_val = ship.groupby("relationship")["valueofgoodsusd"].sum()

    def pct(x, d=None):
        d = n if d is None else d
        return f"{100 * x / d:.1f}%" if d else "—"

    days = (pd.Timestamp(de) - pd.Timestamp(ds)).days
    L = []
    A = L.append
    A(f"# 파일럿 추출 — 확인 결과\n")
    A(f"**표본**: {ds} ~ {de} ({days}일, 도착일 기준) · "
      f"**생성**: `{Path(__file__).name}` · {run_date}\n")
    A("숫자마다 '그래서 뭘 뜻하는지'를 아래에 붙여 뒀습니다.\n")

    A("\n## 0. 먼저 — 표본이 문서와 맞는가\n")
    A(f"- 표준 필터(미국 실착 + 통과화물 제외) 적용 후 선적 **{n:,}건**")
    if (ds, de) == ("2024-03-01", "2024-03-08"):
        A("- 팀 문서(`data_dictionary.md` §1)가 **같은 주간**으로 적은 값은 **254,873건**")
        A(f"- 차이 **{n - 254873:+,}건** ({abs(n - 254873) / 254873 * 100:.2f}%) — "
          "0에 가까우면 필터가 문서와 같다는 뜻입니다. 피드가 갱신되면 조금씩 늘어납니다.\n")
    else:
        A(f"- ⚠️ 문서 대조값(2024-03-01~03-08, 254,873건)과 **기간이 달라 직접 비교는 못 합니다.**\n")

    A("\n## 1. 소유구조 이력(PIT)이 실제로 어떻게 생겼나\n")
    p = chk["pit"].iloc[0]
    rows_all, sentinel = int(p["rows_all"]), int(p["enddate_sentinel"])
    A(f"- 전체 {rows_all:,}행 / {int(p['companies']):,}개 회사")
    A(f"- `endDate` 가 비어 있는(null) 행: **{int(p['enddate_null']):,}개**")
    A(f"- `endDate` 가 `9999-12-31`(= 아직 유효한 구간)인 행: {sentinel:,}개 "
      f"({100 * sentinel / rows_all:.1f}%). 나머지는 실제로 종료된 과거 구간입니다.")
    A(f"- `dateTypeId` 값의 종류: **{int(p['datetypeid_values'])}가지**")
    A("\n**뜻**: 진행 중인 구간을 `endDate is null` 로 찾으면 **한 건도 못 찾습니다.** "
      "`9999-12-31` 로 표시돼 있으니 구간 조건(`거래일이 시작~종료 사이`)만 쓰면 됩니다. "
      "`dateTypeId` 는 값이 하나뿐이라 신경 쓸 필요가 없습니다.\n")
    iv = chk["pit_iv"]
    A("\n소유구조가 바뀐 적이 있는 회사는 얼마나 되나:\n")
    A(md_table(iv))
    tot_co = iv["companies"].sum()
    multi = iv.loc[iv["bucket"].str.startswith("2"), "companies"].sum()
    A(f"\n**뜻**: 이력이 2개 이상인 회사는 {100 * multi / tot_co:.1f}% 뿐이라, "
      "나머지는 시점별 데이터를 써도 스냅샷과 값이 같습니다. "
      "그래도 PIT 를 쓰는 이유는 그 소수가 하필 인수·분할이 있었던 회사들이고, "
      "관계 분류가 갈리는 것도 정확히 그 거래들이기 때문입니다.\n")

    A("\n## 2. 시점별(PIT) vs 현재 스냅샷 — 실제로 다른가\n")
    diffs = {}
    for side, ko in (("con", "수입자"), ("shp", "수출자")):
        m = ship[f"{side}_companyid"].notna()
        pit, snap = ship.loc[m, f"{side}_up_pit"], ship.loc[m, f"{side}_up_snapshot"]
        both_have = pit.notna() & snap.notna()
        # 둘 다 값이 있는 건에서만 '다르다'를 센다. PIT 가 비어 있는 건은 별도(대체) 항목.
        diff = int((pit[both_have] != snap[both_have]).sum())
        fb = int(ship.loc[m, f"{side}_ownership_is_fallback"].sum())
        diffs[side] = diff
        A(f"- {ko}: 매칭 {int(m.sum()):,}건 중")
        A(f"    - 두 소스에 값이 다 있는데 **서로 다른** 건: **{diff:,}건** "
          f"({pct(diff, int(both_have.sum()))})")
        A(f"    - PIT 에 해당 구간이 없어 스냅샷으로 대체한 건: {fb:,}건 ({pct(fb, int(m.sum()))})")
    if max(diffs.values()) == 0:
        A("\n**뜻**: 이 표본에서는 두 소스의 값이 **완전히 같습니다.** "
          "PIT 를 쓰는 이득은 기간을 과거로 넓힐 때 나타납니다.\n")
    else:
        A("\n**뜻**: 값이 다른 건이 실제로 있습니다 — 그 거래들은 소스를 무엇으로 잡느냐에 따라 "
          "관계 분류가 바뀝니다. PIT(거래 시점 기준)가 맞는 값입니다.\n")
    A("\n`ownership_is_fallback=1` 인 행은 소유구조를 현재 스냅샷으로 채운 것이라, "
      "분석에서 제외하거나 민감도를 따로 볼 수 있게 컬럼으로 남겨 뒀습니다.\n")

    A("\n## 3. 관계 분류가 실제로 어떻게 나오나\n")
    tbl = pd.DataFrame({
        "관계": rel.index, "건수": rel.values,
        "건수 비중(%)": (rel / n * 100).round(2).values,
        "금액(백만$)": (rel_val.reindex(rel.index) / 1e6).round(1).values,
        "금액 비중(%)": (rel_val.reindex(rel.index) / rel_val.sum() * 100).round(2).values,
    })
    A(md_table(tbl))
    A(f"\n- `self`(양쪽이 같은 법인) **{int(rel.get('self', 0)):,}건** — "
      "0에 가까워야 정상입니다. 크면 회사 매핑에 문제가 있다는 신호입니다.")
    A("\n**뜻**: `unmatched_*` 는 회사를 못 찾은 선적입니다. "
      "**이걸 '남남 거래(arms_length)'로 세면 안 됩니다** — 그래서 `intra_group` 컬럼에서 "
      "미매칭은 0이 아니라 빈칸으로 뒀습니다.")
    A("\n그리고 `parent_sub` 는 **상대가 그룹의 최상위 회사일 때만** 잡힙니다. "
      "중간 지주회사를 낀 모–자 거래는 `sibling` 으로 들어갑니다. "
      "즉 `parent_sub` 는 하한, `sibling` 은 상한이고, **둘을 합친 `intra_group` 은 이 문제와 무관**합니다.\n")

    A("\n## 4. 기업집단 크기 이상치 — 펀드·트러스트가 섞이나\n")
    big = (co.dropna(subset=["family_size"])
             .sort_values("family_size", ascending=False)
             .drop_duplicates("ultimate_parent_companyid")
             .head(10)[["ultimate_parent_name", "ultimate_parent_type", "family_size"]])
    A("이 표본에 실제로 등장한 기업집단 중 소속 법인이 많은 순:\n")
    A(md_table(big))
    max_fam = int(big["family_size"].max()) if len(big) else 0
    A(f"\n**배경**: CIQ 전체에서 최상위 기준으로 묶으면 최대 집단이 379만 법인(Freddie Mac 유동화 "
      "트러스트)까지 나옵니다. 그런 최상위 아래에서는 아무 관계 없는 두 회사가 `sibling` 으로 붙습니다.")
    if max_fam < 100000:
        A(f"\n**이 표본에서의 결과**: 상위 집단이 전부 실제 기업집단(최대 {max_fam:,} 법인)이고 "
          "펀드·트러스트는 올라오지 않았습니다. **무역 데이터에서는 이 문제가 크게 나타나지 않습니다** "
          "— 유동화 트러스트는 물건을 수입하지 않기 때문입니다. 그래도 본 구축에서는 "
          "비사업 엔티티 제외 필터를 걸어 두는 편이 안전합니다.\n")
    else:
        A(f"\n**이 표본에서의 결과**: 최대 집단이 {max_fam:,} 법인으로 비정상적으로 큽니다. "
          "비사업 엔티티(펀드·트러스트) 제외 필터를 반드시 걸어야 합니다.\n")

    A("\n## 5. 재무는 몇 %에 붙나\n")
    f = chk["fincov"].iloc[0]
    mc = int(f["matched_companies"])
    A(f"- 이 주간에 등장한 매칭 기업(+ 최종 모회사) **{mc:,}개**")
    A(f"- 연간 재무 보유 **{int(f['has_annual']):,}개** ({pct(int(f['has_annual']), mc)})")
    A(f"- 분기 재무 보유 **{int(f['has_quarterly']):,}개** ({pct(int(f['has_quarterly']), mc)})")
    n_fin_co = int(fin["companyid"].nunique())
    A(f"- ↳ 대사: 실제로 내려받은 재무의 고유 회사 수 **{n_fin_co:,}개** — "
      + ("위 '연간 재무 보유'와 일치합니다(진단 쿼리와 부착 규칙이 같은 창을 씁니다)."
         if n_fin_co == int(f["has_annual"])
         else f"⚠️ 위 값과 **{n_fin_co - int(f['has_annual']):+,}개 차이** — 진단 쿼리와 "
              "부착 규칙의 조건이 어긋났다는 뜻이니 확인이 필요합니다."))
    att = firm[firm["fin_calendar_year"].notna()]
    if len(att):
        A(f"- 실제로 붙은 재무의 회계연도 분포: "
          + ", ".join(f"{int(k)}년 {v:,}개"
                      for k, v in att["fin_calendar_year"].value_counts().sort_index().items()))
        age = pd.to_numeric(att["fin_age_days"], errors="coerce")
        A(f"- 결산일 → 기준시점 간격: 중위 {age.median():.0f}일 "
          f"(최소 **{age.min():.0f}** · 최대 **{age.max():.0f}**) — "
          "음수가 있으면 미래 재무가 샌 것이고, 1,461일(4년)을 넘으면 소급 한도가 깨진 것입니다")
        cur_mix = att["fin_currency"].value_counts()
        usd = 100 * cur_mix.get("USD", 0) / len(att)
        A(f"- 표시 통화 상위: " + ", ".join(f"{k} {v:,}" for k, v in cur_mix.head(5).items())
          + f"  → **USD 는 {usd:.0f}% 뿐**")
        pc = int(pd.to_numeric(att["fin_perimeter_change"], errors="coerce").fillna(0).sum())
        A(f"- **사업 범위가 바뀐 회계기간**(자산매각·중단사업 재분류): {pc:,}개 "
          f"({100*pc/len(att):.1f}%) — `fin_perimeter_change=1`. "
          "이 기간은 재무의 기업 경계와 소유구조 기준 경계가 어긋날 수 있습니다")
    A("\n**뜻 (1) 커버리지**: 재무가 없는 회사가 많은 건 데이터 오류가 아니라 "
      "**비상장사는 재무를 공시하지 않기 때문**입니다. 재무를 요구하는 분석은 표본이 "
      "상장사 쪽으로 크게 치우칩니다 — 이건 데이터를 더 받아서 해결되는 문제가 아닙니다.")

    # 법인 단위 vs 집단 단위 — 재무가 덮는 무역액이 얼마나 달라지는가
    imp = firm[firm["imp_value_usd"].notna()]
    gimp = group[group["imp_value_usd"].notna()]
    if len(imp) and len(gimp):
        e_tot, g_tot = imp["imp_value_usd"].sum(), gimp["imp_value_usd"].sum()
        e_cov = imp.loc[imp["has_financials"] == 1, "imp_value_usd"].sum() / e_tot
        g_cov = gimp.loc[gimp["has_financials"] == 1, "imp_value_usd"].sum() / g_tot
        A("\n**법인 단위(03) vs 집단 단위(04) — 재무가 덮는 수입액**\n")
        A(md_table(pd.DataFrame([
            {"패널": "03_firm (법인, 자기 재무)", "행 수": len(imp),
             "재무 보유 비율(%)": round(100 * (imp["has_financials"] == 1).mean(), 1),
             "덮는 수입액(%)": round(100 * e_cov, 1)},
            {"패널": "04_group (집단, 모회사 재무)", "행 수": len(gimp),
             "재무 보유 비율(%)": round(100 * (gimp["has_financials"] == 1).mean(), 1),
             "덮는 수입액(%)": round(100 * g_cov, 1)},
        ])))
        A("\n미국 판매법인은 재무를 따로 내지 않고 본사가 냅니다. 그래서 **재무를 쓰는 분석은 "
          "집단 단위(04)가 사실상 유일한 선택**입니다. "
          "⚠️ 두 파일을 UNION 하면 같은 무역이 두 번 세어집니다.\n")

    A("\n**뜻 (2) 통화**: 표시 통화가 제각각이라 `revenue` 를 그대로 비교하면 안 됩니다. "
      "USD 환산이 필요하고, 환율은 `ciqExchangeRate` 에 있습니다. (초안 6절 미결 #3)")
    A("\n**뜻 (3) 시점**: 거래일보다 **먼저 끝난** 회계연도만 붙였습니다(`periodEndDate < 거래일`). "
      "`calendarYear <= 거래연도` 로 거르면 3월 선적에 그해 12월 결산 재무가 붙어 "
      "미래 정보가 새어 듭니다.")
    A("\n> **`fin_filing_date` 로 시점 필터를 걸지 마세요.** 이 컬럼은 최초 공시일이 아니라 "
      "그 기간의 **최신 인스턴스**가 실린 서류의 제출일입니다. 회계기간은 이후 연차보고서에 "
      "비교 열로 계속 다시 실려 날짜가 갱신됩니다(결산일 대비 중위 **453일**, 연간의 73.8%가 400~800일). "
      "이 표본에 걸면 재무 보유 기업의 **80.9%가 사라집니다** — 실제로는 이미 공시돼 있던 수치인데도요.\n"
      ">\n"
      "> **애초에 걸 필요가 없습니다.** 재무는 여기서 *기업 규모의 척도*로 쓰이지 "
      "*그 시점의 정보집합*이 아닙니다. 거래 당사자는 자기 회사 규모를 알고 있었고, "
      "우리가 사후에 관측하는 것이 편의를 만들지 않습니다. 무역·산업조직 문헌의 표준 관행도 "
      "회계연도 기준 결합입니다.\n"
      ">\n"
      "> 최초 공시일이 정말 필요해지면(주가 반응·공시 전후 비교 등) `ciqFinInstance`(5,761만 행)에서 "
      "`restatementTypeId = 2`(Original) 또는 `= 1`(Press Release) 인스턴스를 **유형으로 골라** 씁니다. "
      "`min(filingDate)` 로 뭉뚱그리면 이 둘이 섞이고, 후행 재게재만 남은 기간에서 조용히 틀립니다. "
      "실측: 가장 이른 인스턴스가 Original 65.7% · Press Release 33.5%, 결산일 대비 중위 90일.\n")
    A("\n**뜻 (4) 정정본**: `ciqLatestInstanceFinPeriod` 는 각 기간의 **최신 정정본**을 줍니다. "
      "기술통계에는 이게 더 정확합니다. `fin_is_press_release=1` 인 행은 그 기간에 대해 "
      "보도자료(감사 전 잠정치)밖에 없다는 뜻이니 제외하지 말고 플래그로만 다루세요.")
    A("\n정정 유형 중 `Reclassified` 는 **대부분 항목 재배치라 총액이 그대로**지만, "
      "**약 9%는 자산매각·중단사업 재분류라 손익 총액 자체가 바뀝니다**"
      "(Reclassified for Disposal 5.2% + Restated 3.6% + Discontinued Operations 0.1%). "
      "이 과제에서 특히 중요한 이유는, 우리가 거래를 **소유구조로 분류**하기 때문입니다 — "
      "2025년에 자회사를 매각하면 2023년 매출이 그 자회사를 빼고 다시 그려지는데 "
      "그 자회사의 2023년 거래는 데이터에 그대로 남아, **재무와 관계분류의 기업 경계가 어긋납니다.** "
      "`fin_perimeter_change=1` 로 해당 기간을 식별할 수 있습니다.\n")

    A("\n## 6. 자식 테이블을 조인하면 합계가 얼마나 틀어지나\n")
    cj = chk["childjoin"].copy()
    base = cj.iloc[0]
    cj["행수 배수"] = (cj["rows_"] / base["rows_"]).round(4)
    cj["TEU 배수"] = (cj["teu"] / base["teu"]).round(4)
    cj["금액 배수"] = (cj["value_usd"] / base["value_usd"]).round(4)
    A(md_table(cj[["step", "rows_", "shipments", "행수 배수", "TEU 배수", "금액 배수"]]))
    A("\n**뜻**: 행 수는 거의 안 늘었는데 TEU·금액 합계는 눈에 띄게 부풀 수 있습니다. "
      "**행 수로 검증하면 통과하고 합계는 틀리는** 상황이라, 검증은 반드시 합계로 해야 합니다. "
      "이 파일들은 자식 조인 전 값을 쓰므로 안전합니다.\n")

    A("\n## 7. 회사 매칭률 — 이 표본이 미국 수입의 얼마를 덮나\n")
    rows = []
    for side, ko in (("con", "수입자"), ("shp", "수출자")):
        m = ship[f"{side}_companyid"].notna()
        rows.append({"당사자": ko, "건수 기준": pct(int(m.sum())),
                     "금액 기준": pct(val[m].sum(), val.sum())})
    rows.append({"당사자": "양쪽 모두", "건수 기준": pct(int(both.sum())),
                 "금액 기준": pct(val[both].sum(), val.sum())})
    A(md_table(pd.DataFrame(rows)))
    A("\n**뜻**: 관계 분류(내부거래/시장거래)는 **양쪽이 다 매칭된 거래에서만** 가능합니다. "
      "그래서 '미국 수입의 X%가 그룹 내부거래'라고 말하면 안 되고, "
      "**'회사가 확인된 거래 중 X%'** 라고 해야 정직합니다.\n")

    A("\n## 8. 단가(금액÷중량)로 비교할 수 있나\n")
    uv = ship[(ship["hs6"].notna()) & ship["unit_value_usd_per_kg"].notna()
              & (ship["unit_value_usd_per_kg"] > 0)]
    g = uv.groupby("hs6")["unit_value_usd_per_kg"]
    cv = (g.std() / g.mean()).dropna()
    cv = cv[g.size() >= 30]
    A("`valueOfGoodsUSD` 는 Panjiva 추정치입니다. 만약 그 추정이 'HS 코드별 표준 단가 × 중량' "
      "으로 계산된 값이라면, 같은 품목 안에서 단가가 거의 흩어지지 않고 관계별 비교가 무의미해집니다. "
      "그래서 품목(HS6) 안에서 단가가 얼마나 흩어지는지를 먼저 봤습니다.\n")
    if len(cv):
        A(f"- HS6 30건 이상인 품목 {len(cv):,}개의 셀 내 변동계수(CV)")
        A(f"  - 중위 **{cv.median():.2f}** · 4분위 {cv.quantile(.25):.2f} ~ {cv.quantile(.75):.2f} "
          f"· CV<0.1(사실상 상수)인 품목 **{100*(cv < .1).mean():.1f}%**")
        if cv.median() >= 0.2:
            A(f"\n**결론: 단가 분석을 해도 됩니다.** 같은 품목 안에서도 단가가 충분히 흩어집니다"
              f"(중위 CV {cv.median():.2f}). 금액이 품목별 단가표로 기계적으로 계산된 값은 "
              "아니라는 뜻입니다. 다만 추정치라는 각주는 계속 필요합니다.\n")
        else:
            A(f"\n**결론: 단가 분석은 피하세요.** 같은 품목 안에서 단가가 거의 흩어지지 않습니다"
              f"(중위 CV {cv.median():.2f}). 금액이 사실상 품목·중량으로 결정된 값이라 "
              "관계별 단가 차이를 볼 수 없습니다. 건수·중량·TEU 기반 통계로 가세요.\n")
    else:
        A("\n- 표본이 작아 판정 불가. 기간을 넓혀 다시 확인하세요.\n")

    A("\n## 9. 매칭 품질 — 눈으로 확인할 대상\n")
    A("`panjivaCompanyCrossRef` 가 무역 업체를 엉뚱한 CIQ 회사에 연결하는 경우가 있습니다. "
      "실제 사례: Panjiva 의 `Chevron Products Co.`(원유 수입)가 CIQ 의 `Chevron Inc`에 붙었는데, "
      "그 회사는 **Miller Industries 산하 견인차 업체**였습니다(1주일치에서만 2억 달러 오귀속).\n")
    A("자동으로 다 걸러낼 수는 없지만, **업종과 품목이 안 맞는 건**은 신호가 됩니다. "
      "아래는 광물·석유(HS 26·27)를 대량 수입했는데 CIQ 업종이 에너지 계열이 아닌 건들입니다:\n")
    ENERGY = "Oil|Gas|Energy|Chemical|Utilit|Metal|Mining|Petro"
    susp = ship[(ship["hs2"].isin(["26", "27"])) & ship["con_companyid"].notna()
                & ship["con_ciq_industry"].notna()
                & ~ship["con_ciq_industry"].str.contains(ENERGY, case=False, na=False)]
    if len(susp):
        t = (susp.groupby(["conname", "con_ciq_companyname", "con_ciq_industry"], dropna=False)
                 .agg(건수=("panjivarecordid", "size"),
                      금액백만=("valueofgoodsusd", lambda x: round(x.sum() / 1e6, 1)))
                 .reset_index().sort_values("금액백만", ascending=False).head(12))
        A(md_table(t))
        A(f"\n총 {len(susp):,}건 · {susp['valueofgoodsusd'].sum()/1e6:,.0f} 백만$ 가 이 규칙에 걸립니다.")
    else:
        A("(해당 없음)")
    A("\n**이 표는 '틀렸다'는 판정이 아니라 spot-check 대상 목록입니다.** "
      "정유사가 소매업으로 분류돼 있을 수도 있고, 실제로 상사(商社)가 원유를 수입할 수도 있습니다. "
      "지시서 02 의 보고항목인 *'알려진 기업집단 표본으로 분류 정확성 spot-check'* 가 이 작업입니다.\n")

    A("\n## 10. 파일 검증\n")
    h = chk["hsfix"].iloc[0]
    A(f"- **HS 재결합 수정 확인** (`rid=287952173`, 조각 {int(h['chunks'])}개): "
      f"잘못된 방식(구분자 `|` + 문자열 정렬)으로는 코드 {int(h['codes_wrong_order']):,}개, "
      f"고친 방식(구분자 없음 + hsCodeId 정렬)으로는 **{int(h['codes_right_order']):,}개** 인식")
    A(f"- 선적 중복: **{n - ship['panjivarecordid'].nunique():,}건** (0이어야 정상 — "
      "0이 아니면 소유구조 구간 조인이 행을 늘린 것)")
    for label, df, key in (("03_firm", firm, "companyid"),
                           ("04_group", group, "ultimate_parent_companyid")):
        A(f"- {label} 키 중복: **{len(df) - df[key].nunique():,}건** (0이어야 정상)")
    orphan = firm["companyname"].isna().sum()
    A(f"- `panjivaCompanyCrossRef` 가 가리키는데 `ciqCompany` 에 행이 없는 companyId: "
      f"**{int(orphan):,}개** — 이름·산업이 빈칸으로 남습니다(크로스레퍼런스 품질 지표)")

    # 재무 as-of 결합이 규칙대로 됐는지 (미래 재무 유입 / 소급 한도 초과)
    ages = pd.concat([pd.to_numeric(d[c], errors="coerce")
                      for d, c in ((firm, "fin_age_days"), (group, "fin_age_days"),
                                   (pair, "shp_fin_age_days"), (pair, "con_fin_age_days"))
                      if c in d.columns]).dropna()
    if len(ages):
        neg, over = int((ages < 0).sum()), int((ages > 1461).sum())
        A(f"- 재무 as-of 결합: `fin_age_days` 음수 **{neg:,}건**(0이어야 정상 — "
          f"미래 재무가 새면 여기서 잡힙니다), 4년 초과 **{over:,}건**(0이어야 정상)")
    mixed = int((pd.to_numeric(pair.get("n_relationship"), errors="coerce") > 1).sum()) \
        if "n_relationship" in pair.columns else 0
    A(f"- 같은 거래쌍인데 기간 안에서 관계 분류가 갈린 쌍: **{mixed:,}건** — "
      "0이 아니면 그 기간에 인수·분할이 있었다는 뜻이고, `relationship` 대표값 하나로 "
      "요약하면 안 됩니다(집계 단위를 월·분기로 내려야 함)")

    tot = val.sum()
    exp_pair, exp_imp = val[both].sum(), val[ship["con_companyid"].notna()].sum()
    exp_grp = val[ship["con_up"].notna()].sum()
    A("\n네 파일의 금액 합계가 서로 맞물리는지 (단위: 백만$):\n")
    A(md_table(pd.DataFrame([
        {"파일": "01_shipment", "금액": tot / 1e6, "기대값": tot / 1e6, "설명": "전체"},
        {"파일": "02_pair", "금액": pair["value_usd"].sum() / 1e6, "기대값": exp_pair / 1e6,
         "설명": "양측 매칭 건의 합과 같아야 함"},
        {"파일": "03_firm(수입)", "금액": firm["imp_value_usd"].sum() / 1e6,
         "기대값": exp_imp / 1e6, "설명": "수입자 매칭 건의 합과 같아야 함"},
        {"파일": "04_group(수입)", "금액": group["imp_value_usd"].sum() / 1e6,
         "기대값": exp_grp / 1e6, "설명": "03과 같아야 함 (묶는 단위만 다름)"},
    ])))
    A("\n금액과 기대값이 같으면 집계 과정에서 행이 새거나 중복되지 않았다는 뜻입니다. "
      "**03과 04의 합계가 같은 것이 정상입니다** — 같은 무역을 법인별로 보느냐 집단별로 보느냐의 차이뿐입니다.\n")

    A(f"\n- HS6 결측 {ship['hs6'].isna().mean()*100:.1f}% · "
      f"HS 코드가 여러 개인 선적 {ship['hs_is_multi'].mean()*100:.1f}%")
    raw_len = ship["hs_ndigits"].dropna()
    A(f"- 원본 HS 코드의 자릿수: " + ", ".join(
        f"{int(k)}자리 {v:,}건" for k, v in raw_len.value_counts().sort_index().items()))
    A("  - 6자리보다 긴 것은 앞 6자리만 잘라 `hs6` 로 씁니다(정상).")
    A("  - 6자리보다 **짧은** 것이 있으므로 `zfill(6)` 로 앞을 채우면 안 됩니다 — "
      "`8528`(4자리)이 `008528`이 되어 챕터가 00으로 뒤바뀝니다. 원본 길이를 그대로 뒀습니다.\n")

    path.write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    main()
