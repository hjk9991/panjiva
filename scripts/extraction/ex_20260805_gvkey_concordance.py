r"""
ex_20260805_gvkey_concordance.py — companyId ↔ GVKey 콘코던스 실측

질문
----
우리 구독에 CIQ `companyId` ↔ Compustat `gvkey` 매핑이 포함되어 있는가?
(있다면 별도 보유한 Compustat North America / Global 을 붙일 수 있는가?)

접근
----
`ciqCompanyCrossRef` 의 `identifierTypeId = 69` (S&P GVKey) 를 잰다.
GVKey 변형 8종(236~248: ProForma·PreFASB·PreDvst·PostDvst·PreAmend·
Public/Private/Public·MultiIssue·Canadian Exchange·ETF)도 함께 센다.

측정 항목
--------
 1. GVKey 계열 타입별 행수·기업수·식별자수·플래그·값 길이
 2. typeId=69 의 양방향 유일성 (companyId→gvkey, gvkey→companyId)
 3. 국가별 커버리지 (Compustat NA vs Global 어느 쪽에 붙일지 판단용)
 4. CIQ 재무 보유 기업 중 gvkey 보유율
 5. Panjiva 무역에 연결된 기업 중 gvkey 보유율   ← 이 과제에서 제일 중요
 6. 1주치 파일럿 기업(49,272개) 기준 건수·금액 가중 커버리지 (로컬 조인)

산출
----
C:\panjiva\data\staging\gvkey_concordance\
    ciq_gvkey_crosswalk.parquet   companyid ↔ gvkey 전량 (재사용용)
    gvkey_type_summary.csv
    gvkey_country_coverage.csv
    95_gvkey_checks.md            요약 리포트

사용법
------
    python scripts\extraction\ex_20260805_gvkey_concordance.py
    python ... --print-sql
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector

OUT_DIR = Path(r"C:\panjiva\data\staging\gvkey_concordance")
PILOT_FIRM = Path(r"C:\panjiva\data\staging\trade_ownership_pilot_1week\03_firm.parquet")

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
            break
    else:
        sys.exit(f"자격증명 파일이 없습니다: {ENV_CANDIDATES}")
    return dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
    )


# ---------------------------------------------------------------------------
# 1. 쿼리 — 전부 자기완결 CTE 단일쿼리. 서버에서 집계하고 컴팩트 결과만 받는다.
# ---------------------------------------------------------------------------
Q_TYPE_SUMMARY = """
select x.identifierTypeId                                   as identifiertypeid,
       t.identifierTypeName                                 as identifiertypename,
       count(*)                                             as n_rows,
       count(distinct x.companyId)                          as n_companies,
       count(distinct x.identifierValue)                    as n_identifiers,
       sum(iff(x.activeFlag = 1, 1, 0))                     as n_active,
       sum(iff(x.primaryFlag = 1, 1, 0))                    as n_primary,
       sum(iff(x.startDate is not null, 1, 0))              as n_with_startdate,
       sum(iff(x.endDate is not null, 1, 0))                as n_with_enddate,
       min(length(x.identifierValue))                       as min_len,
       max(length(x.identifierValue))                       as max_len
from ciqCompanyCrossRef x
join ciqCrossRefIdentifierType t on t.identifierTypeId = x.identifierTypeId
where t.identifierTypeName ilike '%gvkey%'
group by 1, 2
order by 1
"""

# 양방향 유일성. activeFlag=1 만 본다(비활성 매핑은 과거 이력).
Q_UNIQUENESS = """
with g as (
    select companyId, identifierValue
    from ciqCompanyCrossRef
    where identifierTypeId = 69 and activeFlag = 1
),
c as (select companyId, count(distinct identifierValue) as k from g group by 1),
v as (select identifierValue, count(distinct companyId) as k from g group by 1)
select (select count(*) from g)            as n_rows,
       (select count(*) from c)            as n_companies,
       (select count(*) from v)            as n_gvkeys,
       (select count(*) from c where k > 1) as companies_with_multi_gvkey,
       (select count(*) from v where k > 1) as gvkeys_with_multi_company,
       (select max(k) from c)              as max_gvkey_per_company,
       (select max(k) from v)              as max_company_per_gvkey
"""

# 국가별 — gvkey 또는 연간재무를 가진 기업으로 한정해 가볍게 (전체 4,048만 스캔 회피).
Q_COUNTRY = """
with g as (
    select distinct companyId
    from ciqCompanyCrossRef
    where identifierTypeId = 69 and activeFlag = 1
),
f as (
    select distinct companyId
    from ciqLatestInstanceFinPeriod
    where periodTypeId = 1
),
u as (select companyId from g union select companyId from f)
select coalesce(cg.isoCountry2, '??')                              as iso2,
       count(*)                                                    as n_companies,
       count_if(g.companyId is not null)                           as n_gvkey,
       count_if(f.companyId is not null)                           as n_annual_fin,
       count_if(g.companyId is not null and f.companyId is not null) as n_both
from u
join ciqCompany c        on c.companyId = u.companyId
left join ciqCountryGeo cg on cg.countryId = c.countryId
left join g on g.companyId = u.companyId
left join f on f.companyId = u.companyId
group by 1
order by n_gvkey desc
"""

# 재무 보유 기업 / Panjiva 연결 기업 각각의 gvkey 보유율
Q_OVERLAP = """
with g as (
    select distinct companyId
    from ciqCompanyCrossRef
    where identifierTypeId = 69 and activeFlag = 1
),
f as (
    select distinct companyId
    from ciqLatestInstanceFinPeriod
    where periodTypeId = 1
),
pj as (
    select distinct companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1
)
select 'CIQ 연간재무 보유'  as universe,
       (select count(*) from f)                                            as n_companies,
       (select count(*) from f join g on g.companyId = f.companyId)        as n_with_gvkey
union all
select 'Panjiva 연결(무역)',
       (select count(*) from pj),
       (select count(*) from pj join g on g.companyId = pj.companyId)
union all
select 'Panjiva 연결 ∩ 재무보유',
       (select count(*) from pj join f on f.companyId = pj.companyId),
       (select count(*) from pj join f on f.companyId = pj.companyId
                           join g on g.companyId = pj.companyId)
"""

# 전량 다운로드 — 15만 행 수준이라 부담 없음. 팀 재사용용 산출물.
Q_CROSSWALK = """
select companyId          as companyid,
       identifierValue    as gvkey,
       activeFlag         as activeflag,
       primaryFlag        as primaryflag
from ciqCompanyCrossRef
where identifierTypeId = 69
"""

QUERIES = [
    ("타입별 요약", Q_TYPE_SUMMARY),
    ("유일성", Q_UNIQUENESS),
    ("국가별 커버리지", Q_COUNTRY),
    ("표본별 중첩", Q_OVERLAP),
    ("크로스워크 전량", Q_CROSSWALK),
]


def to_md(df: pd.DataFrame) -> str:
    """마크다운 표. tabulate 의존을 피한다(팀 venv에 미설치)."""
    def cell(v):
        if pd.isna(v):
            return ""
        return f"{v:,}" if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)

    head = "| " + " | ".join(df.columns) + " |"
    rule = "|" + "|".join("---" for _ in df.columns) + "|"
    rows = ["| " + " | ".join(cell(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, rule, *rows])


def fetch(cur, sql: str) -> pd.DataFrame:
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print-sql", action="store_true", help="SQL만 출력하고 종료")
    args = ap.parse_args()

    if args.print_sql:
        for name, sql in QUERIES:
            print(f"\n{'=' * 70}\n-- {name}\n{'=' * 70}{sql}")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with snowflake.connector.connect(**connect_kwargs()) as conn, conn.cursor() as cur:
        print("[1/5] GVKey 계열 타입별 요약 ...")
        types = fetch(cur, Q_TYPE_SUMMARY)

        print("[2/5] typeId=69 양방향 유일성 ...")
        uniq = fetch(cur, Q_UNIQUENESS)

        print("[3/5] 국가별 커버리지 ...")
        country = fetch(cur, Q_COUNTRY)

        print("[4/5] 표본별 중첩 ...")
        overlap = fetch(cur, Q_OVERLAP)

        print("[5/5] 크로스워크 전량 내려받기 ...")
        xwalk = fetch(cur, Q_CROSSWALK)

    # gvkey 는 앞자리 0 이 살아야 Compustat 과 붙는다. 문자열로 굳힌다.
    xwalk["gvkey"] = xwalk["gvkey"].astype("string")
    xwalk["companyid"] = xwalk["companyid"].astype("int64")

    xwalk.to_parquet(OUT_DIR / "ciq_gvkey_crosswalk.parquet", index=False)
    types.to_csv(OUT_DIR / "gvkey_type_summary.csv", index=False, encoding="utf-8-sig")
    country.to_csv(OUT_DIR / "gvkey_country_coverage.csv", index=False, encoding="utf-8-sig")

    # ------------------------------------------------------------------
    # 6. 1주치 파일럿 기업과 로컬 조인 — 건수·금액 가중 커버리지
    # ------------------------------------------------------------------
    pilot_lines = []
    if PILOT_FIRM.exists():
        firm = pd.read_parquet(PILOT_FIRM)
        active = xwalk.loc[xwalk["activeflag"] == 1, ["companyid", "gvkey"]].drop_duplicates()
        firm = firm.merge(active.assign(has_gvkey=1), on="companyid", how="left")
        firm["has_gvkey"] = firm["has_gvkey"].fillna(0).astype(int)

        n = len(firm)
        n_gv = int(firm["has_gvkey"].sum())
        val = firm[["imp_value_usd", "exp_value_usd"]].fillna(0).sum(axis=1)
        val_gv = val[firm["has_gvkey"] == 1].sum()
        fin = firm["has_financials"] == 1
        pilot_lines = [
            f"- 파일럿 매칭 기업 {n:,}개 중 gvkey 보유 **{n_gv:,}개 ({n_gv / n:.1%})**",
            f"- 무역금액 가중 커버리지 **{val_gv / val.sum():.1%}** "
            f"(gvkey 보유 기업 거래액 ${val_gv:,.0f} / 전체 ${val.sum():,.0f})",
            f"- CIQ 재무 보유 기업 {int(fin.sum()):,}개 중 gvkey 보유 "
            f"{int(firm.loc[fin, 'has_gvkey'].sum()):,}개 "
            f"({firm.loc[fin, 'has_gvkey'].mean():.1%})",
            f"- 재무 **없는** 기업 {int((~fin).sum()):,}개 중 gvkey 보유 "
            f"{int(firm.loc[~fin, 'has_gvkey'].sum()):,}개 "
            f"({firm.loc[~fin, 'has_gvkey'].mean():.1%}) "
            f"← Compustat 을 붙여 재무를 보강할 수 있는 여지",
        ]
        firm[["companyid", "companyname", "country_iso2", "has_financials", "has_gvkey"]] \
            .to_parquet(OUT_DIR / "pilot_firm_gvkey_flag.parquet", index=False)

    # ------------------------------------------------------------------
    # 7. 리포트
    # ------------------------------------------------------------------
    md = ["# companyId ↔ GVKey 콘코던스 실측", "",
          "생성: `scripts\\extraction\\ex_20260805_gvkey_concordance.py` (2026-08-05)", "",
          "## 1. GVKey 계열 식별자 타입", "",
          to_md(types), "",
          "## 2. typeId=69 (S&P GVKey) 유일성 — activeFlag=1", "",
          to_md(uniq), "",
          "## 3. 표본별 gvkey 보유율", "",
          to_md(overlap.assign(
              share=lambda d: (d["n_with_gvkey"] / d["n_companies"]).map("{:.1%}".format))), "",
          "## 4. 국가별 커버리지 (상위 25)", "",
          to_md(country.head(25)), ""]
    if pilot_lines:
        md += ["## 5. 1주치 파일럿 기업 기준", ""] + pilot_lines + [""]
    (OUT_DIR / "95_gvkey_checks.md").write_text("\n".join(md), encoding="utf-8")

    print()
    print("\n".join(md[:40]))
    print(f"\n산출: {OUT_DIR}")


if __name__ == "__main__":
    main()
