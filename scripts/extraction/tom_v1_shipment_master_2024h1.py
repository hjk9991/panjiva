# -*- coding: utf-8 -*-
r"""
tom_v1_shipment_master_2024h1.py — 업무지시서 03 산출물 v1 (문언 그대로)

한 행 = 미국 수입 선적 1건(2024 H1). 그 행에 양 당사자(수출자·수입자)의
소유관계 분류와 재무 특성(법인·모회사 × 연간·분기 = 8블록)을 전부 부착한다.

모든 분석 결정은 사용자(김영수 연구원)가 단계별로 확정했다 —
근거·대안·실측치는 산출 폴더의 DECISIONS.md 참조. 요약:

  1단계  원천 = wf2q L0 재사용(표준필터 기적용) · 수입만 · B/L 전부
  2단계  UP = PIT + 스냅샷 fallback(L0 계산값) · self 는 intra=1 ·
         relationship 7분류(unmatched 를 con/shp/both 로 세분) + intra_group 병기
  3단계  재무 = 법인+모회사 × 연간+분기 · 계정 9종 · 도착일별 as-of ·
         결산일(periodEndDate) 기준 선택 + calendarYear/Quarter 라벨 병기 · 소급 2년
  4단계  대표 HS = Classified→Parsed→Manual(L0 값) · 품질 플래그 2종 포함
  5단계  전 계정 USD 환산(결산일 환율, latestSnapFlag=1)만 싣고
         fin_currency·fx_per_usd 보존(원표시 복원 가능) · 월별 6파일

산출:
  C:\panjiva\data\staging\tom_v1_2024h1\
      shipment_master_YYYYMM.parquet ×6   (약 178열)
      fin_annual.parquet · fin_quarterly.parquet   (재무 중간 산출 — 재현·검증용)
      90_checks.md
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import snowflake.connector

L0 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q\L0")
OUT = Path(r"C:\panjiva\data\staging\tom_v1_2024h1")
MONTHS = ["202401", "202402", "202403", "202404", "202405", "202406"]
H1_START, H1_END = "2024-01-01", "2024-07-01"
LOOKBACK = pd.Timedelta(days=730)          # 소급 한도 2년 (결정 3-6)
FIN_WINDOW_START = "2022-01-01"            # H1 최소 도착일 - 2년

# 재무 계정 9종 (결정 3-3). 고용은 사람 수라 환산 없음.
FIN_ITEMS = {28: "revenue", 34: "cogs", 1007: "assets", 4371: "employees",
             4051: "ebitda", 1043: "inventory", 2021: "capex",
             1004: "ppent", 1049: "lt_debt"}
MONEY_ITEMS = [v for v in FIN_ITEMS.values() if v != "employees"]

# 재무 블록 8세트 (결정 3-1·3-2): (접두어, 선적의 키 컬럼, 연간/분기)
BLOCKS = [("con_a_", "con_companyid", "a"), ("con_q_", "con_companyid", "q"),
          ("con_up_a_", "con_up", "a"),     ("con_up_q_", "con_up", "q"),
          ("shp_a_", "shp_companyid", "a"), ("shp_q_", "shp_companyid", "q"),
          ("shp_up_a_", "shp_up", "a"),     ("shp_up_q_", "shp_up", "q")]


# ---------------------------------------------------------------------------
# 0. 접속 — 자격증명은 홈 폴더 .snowflake.env 에서만
# ---------------------------------------------------------------------------
def connect_kwargs() -> dict:
    envp = Path.home() / ".snowflake.env"
    for line in envp.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return dict(
        user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"))


# ---------------------------------------------------------------------------
# 1. 재무 후보 추출 (연간·분기 각 1회) — 회사×기간 1행, 후보 전체를 내려받고
#    최종 선택(도착일별 as-of)은 pandas 에서 한다 (결정 3-4·3-5)
# ---------------------------------------------------------------------------
SQL_FIN = """
with base as (
    select i.conPanjivaId, i.shpPanjivaId
    from panjivaUSImport i
    where i.arrivalDate >= '{h1_start}' and i.arrivalDate < '{h1_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)
      and (i.frob is null or i.frob <> 1)
),
xr as (
    -- activeFlag=1 만, identifierValue 당 1행(낮은 companyId 승자 — 결정 2-4)
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
    -- 무역 당사자 + 최종 모회사 (결정 3-1: 모회사 재무도 붙이므로 조회 대상에 포함)
    select distinct companyId from (
        select companyId from ids0
        union all
        select up.ultimateParentCompanyId
        from ciqCompanyUltimateParent up join ids0 on ids0.companyId = up.companyId
    ) where companyId is not null
),
per as (
    -- 회사×기간 1행. 같은 기간에 정정본 여러 개면 최신 신고분(구독이 Latest 만 제공).
    -- 손익 정정 유형(사업범위 변경 식별)은 ciqFinInstance 에서 —
    -- latestForFinancialPeriodFlag=1 은 기간당 최대 1행(실측)이라 행을 늘리지 않는다.
    select fp.financialPeriodId, fp.companyId, fp.calendarYear, fp.calendarQuarter,
           fp.periodEndDate, fp.currencyId, fp.restatementTypeId,
           fi.isRestatementTypeId
    from ciqLatestInstanceFinPeriod fp
    join ids on ids.companyId = fp.companyId
    left join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId
                               and fi.latestForFinancialPeriodFlag = 1
    where fp.periodTypeId = {period_type}
      and fp.periodEndDate >= '{fin_start}'
      and fp.periodEndDate <  '{h1_end}'
    qualify row_number() over (partition by fp.companyId, fp.periodEndDate
                               order by fp.filingDate desc) = 1
),
wide as (
    select p.companyId, p.calendarYear, p.calendarQuarter, p.periodEndDate,
           p.currencyId, p.restatementTypeId, p.isRestatementTypeId,
           {pivot}
    from per p
    left join ciqFinancialData d on d.financialPeriodId = p.financialPeriodId
                                and d.dataItemId in ({item_ids})
    group by 1, 2, 3, 4, 5, 6, 7
)
select w.companyId       as companyid,
       w.calendarYear    as cal_year,
       w.calendarQuarter as cal_quarter,
       w.periodEndDate   as period_end,
       cu.ISOCode        as fin_currency,
       w.currencyId      as currencyid,
       iff(w.restatementTypeId = 1, 1, 0) as is_press_release,
       case when w.isRestatementTypeId is null then null
            when w.isRestatementTypeId in (5, 6, 12) then 1
            else 0 end as perimeter_change,
       {cols}
from wide w
left join ciqCurrency cu on cu.currencyId = w.currencyId
"""

# 환율: 필요 범위(2021-06~)만 통째로 받아 pandas 에서 결산일 asof (일 204통화 → 약 25만 행)
SQL_FX = """
select er.currencyId as currencyid, er.priceDate as price_date,
       er.priceClose as fx_per_usd            -- 1 USD 당 현지통화 (실측 확인)
from ciqExchangeRate er
where er.latestSnapFlag = 1                   -- 하루 스냅 여러 개 → 1개만 (행 증식 방지)
  and er.priceDate >= '2021-06-01'
"""


def build_fin_sql(period_type: int) -> str:
    pivot = ",\n           ".join(
        f"max(case when d.dataItemId = {k} then d.dataItemValue end) as {v}"
        for k, v in FIN_ITEMS.items())
    return SQL_FIN.format(h1_start=H1_START, h1_end=H1_END, fin_start=FIN_WINDOW_START,
                          period_type=period_type, pivot=pivot,
                          item_ids=", ".join(map(str, FIN_ITEMS)),
                          cols=", ".join(f"w.{v}" for v in FIN_ITEMS.values()))


def fetch(cur, sql: str, label: str) -> pd.DataFrame:
    print(f"  · {label} ...", end="", flush=True)
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    print(f" {len(df):,}행")
    return df


def prepare_fin(fin: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    """타입 정리 + 결산일 환율 asof + 전 계정 USD 환산 (결정 5-1·5-3)."""
    fin = fin.copy()
    fin["companyid"] = pd.to_numeric(fin["companyid"], errors="coerce").astype("int64")
    for c in ["cal_year", "cal_quarter", "is_press_release", "perimeter_change"]:
        fin[c] = pd.to_numeric(fin[c], errors="coerce").astype("Int64")
    fin["period_end"] = pd.to_datetime(fin["period_end"]).astype("datetime64[ns]")
    for c in FIN_ITEMS.values():
        fin[c] = pd.to_numeric(fin[c], errors="coerce")     # Decimal → float

    # 결산일 직전 최근 환율 (통화별 asof)
    fx = fx.copy()
    fx["currencyid"] = pd.to_numeric(fx["currencyid"], errors="coerce").astype("int64")
    fx["price_date"] = pd.to_datetime(fx["price_date"]).astype("datetime64[ns]")
    fx["fx_per_usd"] = pd.to_numeric(fx["fx_per_usd"], errors="coerce")
    fx = fx.sort_values("price_date")
    fin["currencyid"] = pd.to_numeric(fin["currencyid"], errors="coerce").astype("int64")
    fin = pd.merge_asof(fin.sort_values("period_end"), fx,
                        left_on="period_end", right_on="price_date",
                        by="currencyid", direction="backward",
                        tolerance=pd.Timedelta(days=14))     # 결산일 부근 2주 내 환율
    fin = fin.drop(columns=["price_date", "currencyid"])

    # 전 계정 USD 환산. USD 표시 재무는 환율 1 로 통일(환산 불변).
    fin.loc[fin["fin_currency"] == "USD", "fx_per_usd"] = 1.0
    for c in MONEY_ITEMS:
        fin[f"{c}_usd"] = fin[c] / fin["fx_per_usd"]
    fin = fin.drop(columns=MONEY_ITEMS)                      # 원표시는 환율로 복원 가능
    return fin.sort_values("period_end")


# ---------------------------------------------------------------------------
# 2. 선적 층 — L0 재사용 (결정 1-1) + 관계 세분 (결정 2-3)
# ---------------------------------------------------------------------------
def load_month(ym: str) -> pd.DataFrame:
    df = pd.read_parquet(L0 / f"imp_ship_{ym}.parquet")
    for c in ["conpanjivaid", "shppanjivaid", "con_companyid", "shp_companyid",
              "con_up", "shp_up"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["arrivaldate"] = pd.to_datetime(df["arrivaldate"]).astype("datetime64[ns]")

    # unmatched 를 어느 쪽 실패인지로 세분 (사용자 결정 — one 은 방향 정보를 잃는다)
    con_na, shp_na = df["con_companyid"].isna(), df["shp_companyid"].isna()
    df["relationship"] = np.select(
        [con_na & shp_na, con_na & ~shp_na, ~con_na & shp_na],
        ["unmatched_both", "unmatched_con", "unmatched_shp"],
        default=df["relationship"])

    # intra_group: 1={parent_sub, sibling, self}, 0=arms_length, 결측=unmatched_*
    # self 포함은 사용자 결정 2-2 (같은 법인 = 같은 그룹; 오매칭 의심 건은 relationship 으로 식별)
    df["intra_group"] = (df["relationship"]
                         .map({"parent_sub": 1, "sibling": 1, "self": 1, "arms_length": 0})
                         .astype("Int64"))
    return df


def attach_company_info(df: pd.DataFrame, co: pd.DataFrame) -> pd.DataFrame:
    """양측에 CIQ 이름·국가·산업·유형 + 모회사 이름·국가 부착 (L0 company.parquet)."""
    keep = co[["companyid", "companyname", "country_iso2", "industry", "company_type",
               "ultimate_parent_name", "ultimate_parent_country_iso2"]].copy()
    keep["companyid"] = keep["companyid"].astype("Int64")
    for side in ("con", "shp"):
        c = keep.add_prefix(f"{side}_ciq_").rename(
            columns={f"{side}_ciq_companyid": f"{side}_companyid",
                     f"{side}_ciq_ultimate_parent_name": f"{side}_up_name",
                     f"{side}_ciq_ultimate_parent_country_iso2": f"{side}_up_country"})
        df = df.merge(c, on=f"{side}_companyid", how="left")
    return df


def attach_block(df: pd.DataFrame, fin: pd.DataFrame, key: str, prefix: str) -> pd.DataFrame:
    """재무 1블록을 도착일별 as-of 로 부착 (결정 3-4: 행별 도착일, 소급 2년, 당일 제외)."""
    fin_cols = ["cal_year", "cal_quarter", "period_end", "fin_currency", "fx_per_usd",
                "is_press_release", "perimeter_change", "employees"] \
               + [f"{c}_usd" for c in MONEY_ITEMS]
    right = (fin[["companyid"] + fin_cols]
             .rename(columns={"companyid": "_k"}).sort_values("period_end"))
    left = df.copy()
    left["_k"] = left[key].astype("float").astype("Int64")
    # merge_asof 는 by 키에 결측을 허용하지 않으므로 매칭 행만 asof 하고 나머지는 그대로
    has = left["_k"].notna()
    sub = left.loc[has].copy()
    sub["_k"] = sub["_k"].astype("int64")
    sub = pd.merge_asof(sub.sort_values("arrivaldate"), right,
                        left_on="arrivaldate", right_on="period_end", by="_k",
                        direction="backward", tolerance=LOOKBACK,
                        allow_exact_matches=False)   # 결산 당일 도착분은 공시 전 → 제외
    out = pd.concat([sub, left.loc[~has]], ignore_index=False).sort_index()
    out["age_days"] = (out["arrivaldate"] - out["period_end"]).dt.days
    out = out.drop(columns=["_k"])
    return out.rename(columns={c: f"{prefix}{c}" for c in fin_cols + ["age_days"]})


# ---------------------------------------------------------------------------
# 3. 실행
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*", default=MONTHS)
    ap.add_argument("--skip-fin-pull", action="store_true",
                    help="fin_annual/quarterly.parquet 이 이미 있으면 추출 생략")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    run_date = date.today().isoformat()

    # --- 재무·환율 추출 (1회) ---
    fa, fq = OUT / "fin_annual.parquet", OUT / "fin_quarterly.parquet"
    if args.skip_fin_pull and fa.exists() and fq.exists():
        print("[1/3] 재무 추출 생략 (기존 파일 재사용)")
        fin_a, fin_q = pd.read_parquet(fa), pd.read_parquet(fq)
    else:
        print("[1/3] Snowflake: 재무 후보(연간·분기) + 환율 추출")
        with snowflake.connector.connect(**connect_kwargs()) as conn:
            with conn.cursor() as cur:
                raw_a = fetch(cur, build_fin_sql(1), "연간 재무 후보")
                raw_q = fetch(cur, build_fin_sql(2), "분기 재무 후보")
                fx = fetch(cur, SQL_FX, "환율 (2021-06~)")
        fin_a, fin_q = prepare_fin(raw_a, fx), prepare_fin(raw_q, fx)
        fin_a.to_parquet(fa, index=False)
        fin_q.to_parquet(fq, index=False)
        print(f"  저장: {fa.name} {len(fin_a):,}행 · {fq.name} {len(fin_q):,}행")

    co = pd.read_parquet(L0 / "company.parquet")

    # --- 월별 빌드 ---
    print("\n[2/3] 월별 빌드 (L0 → 관계 세분 → 기업정보 → 재무 8블록 as-of)")
    stats = []
    for ym in args.months:
        df = load_month(ym)
        n0, v0 = len(df), df["valueofgoodsusd"].sum()
        df = attach_company_info(df, co)
        for prefix, key, freq in BLOCKS:
            df = attach_block(df, fin_a if freq == "a" else fin_q, key, prefix)
        assert len(df) == n0, f"{ym}: 행 수 변동 {n0:,} → {len(df):,}"
        assert abs(df["valueofgoodsusd"].sum() - v0) < 1, f"{ym}: 금액 합계 변동"
        p = OUT / f"shipment_master_{ym}.parquet"
        df.to_parquet(p, index=False, compression="snappy")
        stats.append((ym, len(df), df.shape[1], v0))
        print(f"  {p.name:<28} {len(df):>9,}행 × {df.shape[1]}열")

    # --- 검증 리포트 ---
    print("\n[3/3] 검증 리포트")
    write_checks(stats, fin_a, fin_q, run_date)
    print(f"  {OUT / '90_checks.md'}")

    print("\n_catalog.md 줄 (복사용):")
    total = sum(s[1] for s in stats)
    print(f"| `tom_v1_2024h1/shipment_master_*.parquet` ×{len(stats)} | "
          f"v1: 선적 1행에 양측 관계분류+재무 8블록(USD) | 2024 H1 | {total:,} | "
          f"`tom_v1_shipment_master_2024h1.py` | {run_date} | 김영수 |")


def write_checks(stats, fin_a, fin_q, run_date) -> None:
    L = []
    A = L.append
    A("# v1 shipment_master — 검증 결과\n")
    A(f"**생성**: `tom_v1_shipment_master_2024h1.py` · {run_date} · "
      "결정 근거는 `DECISIONS.md`\n")

    total = sum(s[1] for s in stats)
    A("\n## 1. 행 수 대사 (원천 L0 와 동일해야 함)\n")
    A("| 월 | 행 수 | 열 수 |")
    A("|---|---|---|")
    for ym, n, ncol, _ in stats:
        A(f"| {ym} | {n:,} | {ncol} |")
    A(f"| **합계** | **{total:,}** | |")
    A(f"\n- L0 실측 7,237,772건과의 차이: **{total - 7237772:+,}건** (0이어야 정상)")

    # 표본 하나로 심층 검증 (1월)
    df = pd.read_parquet(OUT / "shipment_master_202401.parquet")
    A("\n## 2. 심층 검증 (202401 표본)\n")
    A(f"- `panjivarecordid` 중복: **{int(df['panjivarecordid'].duplicated().sum()):,}건** (0이어야 정상)")
    rel = df["relationship"].value_counts()
    A("- 관계분류 분포: " + " · ".join(f"{k} {v:,}" for k, v in rel.items()))
    ig = df["intra_group"]
    A(f"- intra_group: 1={int((ig == 1).sum()):,} / 0={int((ig == 0).sum()):,} / "
      f"결측={int(ig.isna().sum()):,} (결측 = unmatched)")

    A("\n### 재무 as-of 규칙 (전 블록)")
    for prefix, key, freq in BLOCKS:
        age = pd.to_numeric(df[f"{prefix}age_days"], errors="coerce").dropna()
        if len(age):
            bad = int((age < 1).sum()) + int((age > 730).sum())
            A(f"- `{prefix}*`: 부착 {len(age):,}건, age 1~730일 위반 **{bad}건**, "
              f"중위 {age.median():.0f}일")

    A("\n### 재무 커버리지 (금액 기준, 202401)")
    v = df["valueofgoodsusd"]
    for prefix, _, _ in BLOCKS:
        has = df[f"{prefix}period_end"].notna()
        A(f"- `{prefix}*`: 선적 {100*has.mean():.1f}% · 수입액 {100*v[has].sum()/v.sum():.1f}%")

    A("\n### USD 환산")
    for label, fin in (("연간", fin_a), ("분기", fin_q)):
        need = fin["fin_currency"].notna() & (fin["fin_currency"] != "USD")
        fail = need & fin["fx_per_usd"].isna()
        A(f"- {label}: 비USD {int(need.sum()):,}기간 중 환율 미부착 **{int(fail.sum()):,}건** "
          f"({100*fail.sum()/max(need.sum(),1):.2f}%) — 실패분은 *_usd 결측")

    (OUT / "90_checks.md").write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    main()
