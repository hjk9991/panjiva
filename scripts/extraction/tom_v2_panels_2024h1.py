# -*- coding: utf-8 -*-
r"""
tom_v2_panels_2024h1.py — 업무지시서 03 산출물 v2 (변형: 기업×분기 패널) 2024 H1

v2 = 선적 base + 파생 패널 3종. 1주 파일럿(`ex_20260805_trade_ownership_master_1week.py`)의
집계 로직을 L0 입력으로 이식하고, 2026-08-12 리뷰에서 확정된 통일 사항을 반영했다:

  - 원천: wf2q L0 재사용 (v1·v3와 동일 — 교차 대사 가능)
  - 재무: v1 중간산출 fin_annual.parquet 재사용 (연간 9계정 USD 환산본, 소급 2년)
  - self = intra 1 (v1 결정 2-2와 통일; relationship 표시는 유지)
  - relationship 은 unmatched 를 con/shp/both 로 세분 (v1 결정 2-3과 통일)
  - intra_share 분모 = 분류된 거래(internal + self + arms), 미매칭 제외
  - 선적 층(01)은 저장하지 않음 — tom_v1_2024h1\shipment_master_* 가 동일 행집합(+재무)

산출: C:\panjiva\data\staging\tom_v2_2024h1\
    02_pair.parquet    (수출자, 수입자, 분기) 1행 + 양측 법인 재무
    03_firm.parquet    (법인, 분기) 1행 + 자기 재무
    04_group.parquet   (기업집단, 분기) 1행 + 모회사 재무
    90_checks.md
"""

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

L0 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q\L0")
V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024h1")
OUT = Path(r"C:\panjiva\data\staging\tom_v2_2024h1")
MONTHS = ["202401", "202402", "202403", "202404", "202405", "202406"]
LOOKBACK = pd.Timedelta(days=730)          # 소급 2년 (v1 결정 3-6과 통일)

FIN_MONEY = ["revenue", "cogs", "assets", "ebitda", "inventory", "capex", "ppent", "lt_debt"]
FIN_COLS = ["cal_year", "cal_quarter", "period_end", "fin_currency", "fx_per_usd",
            "is_press_release", "perimeter_change", "employees"] \
           + [f"{c}_usd" for c in FIN_MONEY]

SHIP_COLS = ["panjivarecordid", "arrivaldate", "conpanjivaid", "shppanjivaid",
             "con_companyid", "shp_companyid", "con_up", "shp_up",
             "relationship", "valueofgoodsusd", "weightkg", "volumeteu",
             "hs6", "hs2", "shpmtorigin"]


def load_ship() -> pd.DataFrame:
    """L0 6개월 로드 + v1과 동일한 관계 세분·intra 파생."""
    parts = []
    for ym in MONTHS:
        df = pd.read_parquet(L0 / f"imp_ship_{ym}.parquet", columns=SHIP_COLS)
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    for c in ["con_companyid", "shp_companyid", "con_up", "shp_up"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["arrivaldate"] = pd.to_datetime(df["arrivaldate"]).astype("datetime64[ns]")

    con_na, shp_na = df["con_companyid"].isna(), df["shp_companyid"].isna()
    df["relationship"] = np.select(
        [con_na & shp_na, con_na & ~shp_na, ~con_na & shp_na],
        ["unmatched_both", "unmatched_con", "unmatched_shp"],
        default=df["relationship"])
    df["intra_group"] = (df["relationship"]
                         .map({"parent_sub": 1, "sibling": 1, "self": 1, "arms_length": 0})
                         .astype("Int64"))
    df["trade_quarter"] = df["arrivaldate"].dt.to_period("Q").astype(str)
    return df


def quarter_start(labels: pd.Series) -> pd.Series:
    return pd.Series(pd.PeriodIndex(labels.astype(str), freq="Q").start_time,
                     index=labels.index)


def value_split(sub: pd.DataFrame, keys, prefix: str) -> pd.DataFrame:
    """관계별 금액 분해 — 03·04 공통 정의. self 는 intra 에 포함(확정)하되 원자료는 분리."""
    rel, val = sub["relationship"], sub["valueofgoodsusd"]
    parts = sub.assign(
        _internal=val.where(rel.isin(["parent_sub", "sibling"]), 0),
        _self=val.where(rel.eq("self"), 0),
        _arms=val.where(rel.eq("arms_length"), 0),
        _unmatched=val.where(rel.str.startswith("unmatched"), 0),
    ).groupby(keys)[["_internal", "_self", "_arms", "_unmatched"]].sum()
    parts.columns = [f"{prefix}_value_internal", f"{prefix}_value_self",
                     f"{prefix}_value_arms", f"{prefix}_value_unmatched"]
    classified = (parts[f"{prefix}_value_internal"] + parts[f"{prefix}_value_self"]
                  + parts[f"{prefix}_value_arms"])
    parts[f"{prefix}_value_classified"] = classified
    parts[f"{prefix}_intra_share"] = (
        (parts[f"{prefix}_value_internal"] + parts[f"{prefix}_value_self"])
        / classified).where(classified > 0)
    return parts.reset_index()


def top_by_value(df: pd.DataFrame, keys, col: str, out: str) -> pd.DataFrame:
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=list(keys) + [out])
    return (sub.groupby(list(keys) + [col], dropna=False)["valueofgoodsusd"].sum()
               .reset_index()
               .sort_values(["valueofgoodsusd", col], ascending=[False, True],
                            kind="mergesort")
               .drop_duplicates(keys)
               .rename(columns={col: out})[list(keys) + [out]])


def attach_financials(panel: pd.DataFrame, fin: pd.DataFrame, key: str,
                      prefix: str) -> pd.DataFrame:
    """분기 시작일 기준 as-of (backward 2년, 당일 제외) — v2 규약."""
    right = (fin[["companyid"] + FIN_COLS]
             .rename(columns={"companyid": "_k"}).sort_values("period_end"))
    left = panel.copy()
    left["_k"] = pd.to_numeric(left[key], errors="coerce").astype("Int64")
    has = left["_k"].notna()
    sub = left.loc[has].copy()
    sub["_k"] = sub["_k"].astype("int64")
    sub["period_start"] = sub["period_start"].astype("datetime64[ns]")
    sub = pd.merge_asof(sub.sort_values("period_start"), right,
                        left_on="period_start", right_on="period_end", by="_k",
                        direction="backward", tolerance=LOOKBACK,
                        allow_exact_matches=False)
    out = pd.concat([sub, left.loc[~has]], ignore_index=False).sort_index()
    out[f"{prefix}fin_age_days"] = (out["period_start"] - out["period_end"]).dt.days
    out[f"{prefix}has_financials"] = out["cal_year"].notna().astype("int8")
    out = out.drop(columns=["_k"])
    return out.rename(columns={c: f"{prefix}{c}" for c in FIN_COLS})


def build_pair(ship, co, fin):
    both = ship[ship["con_companyid"].notna() & ship["shp_companyid"].notna()].copy()
    gk = ["shp_companyid", "con_companyid", "trade_quarter"]
    pair = both.groupby(gk, dropna=False).agg(
        relationship=("relationship", "first"),
        n_relationship=("relationship", "nunique"),
        intra_group=("intra_group", "first"),
        n_ship=("panjivarecordid", "nunique"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
        n_hs6=("hs6", "nunique"),
    ).reset_index()
    for col, out in (("hs2", "top_hs2"), ("hs6", "top_hs6")):
        pair = pair.merge(top_by_value(both, gk, col, out), on=gk, how="left")
    pair["period_start"] = quarter_start(pair["trade_quarter"])
    for side, key in (("shp", "shp_companyid"), ("con", "con_companyid")):
        c = co.add_prefix(f"{side}_").rename(columns={f"{side}_companyid": key})
        pair = pair.merge(c, on=key, how="left")
        pair = attach_financials(pair, fin, key, f"{side}_")
    return pair


def build_firm(ship, co, fin):
    blocks = []
    for prefix, key in (("imp", "con_companyid"), ("exp", "shp_companyid")):
        sub = ship[ship[key].notna()].copy()
        other = "shp_companyid" if key == "con_companyid" else "con_companyid"
        gk = [key, "trade_quarter"]
        b = sub.groupby(gk).agg(**{
            f"{prefix}_n_ship": ("panjivarecordid", "nunique"),
            f"{prefix}_value_usd": ("valueofgoodsusd", "sum"),
            f"{prefix}_weight_kg": ("weightkg", "sum"),
            f"{prefix}_teu": ("volumeteu", "sum"),
            f"{prefix}_n_partners": (other, "nunique"),
        }).reset_index()
        b = b.merge(value_split(sub, gk, prefix), on=gk, how="left")
        for col, out in (("hs2", f"{prefix}_top_hs2"),
                         ("shpmtorigin", f"{prefix}_top_partner_country")):
            b = b.merge(top_by_value(sub, gk, col, out), on=gk, how="left")
        blocks.append(b.rename(columns={key: "companyid"}))
    firm = blocks[0].merge(blocks[1], on=["companyid", "trade_quarter"], how="outer")
    firm = firm.merge(co, on="companyid", how="left")
    firm["period_start"] = quarter_start(firm["trade_quarter"])
    return attach_financials(firm, fin, "companyid", "")


def build_group(ship, co, fin):
    blocks = []
    for prefix, key, other in (("imp", "con_up", "shp_up"), ("exp", "shp_up", "con_up")):
        sub = ship[ship[key].notna()].copy()
        member = "con_companyid" if key == "con_up" else "shp_companyid"
        gk = [key, "trade_quarter"]
        b = sub.groupby(gk).agg(**{
            f"{prefix}_n_ship": ("panjivarecordid", "nunique"),
            f"{prefix}_value_usd": ("valueofgoodsusd", "sum"),
            f"{prefix}_weight_kg": ("weightkg", "sum"),
            f"{prefix}_teu": ("volumeteu", "sum"),
            f"{prefix}_n_members": (member, "nunique"),
            f"{prefix}_n_partner_groups": (other, "nunique"),
        }).reset_index()
        b = b.merge(value_split(sub, gk, prefix), on=gk, how="left")
        for col, out in (("hs2", f"{prefix}_top_hs2"),
                         ("shpmtorigin", f"{prefix}_top_partner_country")):
            b = b.merge(top_by_value(sub, gk, col, out), on=gk, how="left")
        blocks.append(b.rename(columns={key: "ultimate_parent_companyid"}))
    grp = blocks[0].merge(blocks[1], on=["ultimate_parent_companyid", "trade_quarter"],
                          how="outer")
    # 모회사 자신의 기준정보·재무 (합산 금지 — 연결재무에 자회사 이미 포함)
    # ⚠️ co 에 ultimate_parent_companyid 컬럼이 이미 있으므로 빼고 rename (중복 방지)
    co_up = (co.drop(columns=["ultimate_parent_companyid"], errors="ignore")
               .rename(columns={"companyid": "ultimate_parent_companyid"}))
    grp = grp.merge(co_up, on="ultimate_parent_companyid", how="left")
    grp["period_start"] = quarter_start(grp["trade_quarter"])
    return attach_financials(grp, fin, "ultimate_parent_companyid", "")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    run_date = date.today().isoformat()

    print("[1/3] 입력 로드")
    ship = load_ship()
    print(f"  선적 {len(ship):,}건 (L0 6개월)")
    fin = pd.read_parquet(V1 / "fin_annual.parquet")   # v1 중간산출 재사용 (연간 9계정 USD)
    fin["companyid"] = pd.to_numeric(fin["companyid"], errors="coerce").astype("int64")
    fin["period_end"] = pd.to_datetime(fin["period_end"]).astype("datetime64[ns]")
    fin = fin.sort_values("period_end")
    co_full = pd.read_parquet(L0 / "company.parquet")
    co = co_full[["companyid", "companyname", "country_iso2", "industry",
                  "company_type", "ultimate_parent_companyid", "family_size"]].copy()
    co["companyid"] = pd.to_numeric(co["companyid"], errors="coerce").astype("Int64")

    print("[2/3] 패널 3종 집계")
    pair = build_pair(ship, co, fin)
    firm = build_firm(ship, co, fin)
    grp = build_group(ship, co, fin)
    for name, df in (("02_pair", pair), ("03_firm", firm), ("04_group", grp)):
        p = OUT / f"{name}.parquet"
        df.columns = [str(c).lower() for c in df.columns]
        df.to_parquet(p, index=False, compression="snappy")
        print(f"  {p.name:<18} {len(df):>9,}행 × {df.shape[1]}열")

    print("[3/3] 검증")
    checks(ship, pair, firm, grp, run_date)
    print(f"  {OUT / '90_checks.md'}")
    total = len(ship)
    print(f"\n_catalog.md 줄:")
    for name, df, desc in (("02_pair", pair, "쌍×분기 + 양측 법인 재무(연간 USD)"),
                           ("03_firm", firm, "법인×분기 + 자기 재무"),
                           ("04_group", grp, "기업집단×분기 + 모회사 재무")):
        print(f"| `tom_v2_2024h1/{name}.parquet` | v2: {desc} | 2024 H1 | "
              f"{len(df):,} | `tom_v2_panels_2024h1.py` | {run_date} | 김영수 |")


def checks(ship, pair, firm, grp, run_date) -> None:
    L = []
    A = L.append
    ok = []

    def chk(label, cond, detail=""):
        ok.append(bool(cond))
        A(f"- [{'OK' if cond else '실패'}] {label}" + (f" — {detail}" if detail else ""))

    A(f"# v2 패널 (2024 H1) — 검증 결과\n")
    A(f"**생성**: `tom_v2_panels_2024h1.py` · {run_date} · 선적 층은 v1 파일 참조\n")

    v = ship["valueofgoodsusd"]
    both = ship["con_companyid"].notna() & ship["shp_companyid"].notna()
    chk("선적 행 수 == L0 (7,237,772)", len(ship) == 7_237_772, f"{len(ship):,}")
    chk("02_pair 키 유일", pair.duplicated(["shp_companyid", "con_companyid",
                                            "trade_quarter"]).sum() == 0)
    chk("03_firm 키 유일", firm.duplicated(["companyid", "trade_quarter"]).sum() == 0)
    chk("04_group 키 유일", grp.duplicated(["ultimate_parent_companyid",
                                            "trade_quarter"]).sum() == 0)
    chk("02 금액 == 양측 매칭 합",
        abs(pair["value_usd"].sum() - v[both].sum()) < 1,
        f"{pair['value_usd'].sum()/1e6:,.1f} vs {v[both].sum()/1e6:,.1f} 백만$")
    chk("03(수입) 금액 == 수입자 매칭 합",
        abs(firm["imp_value_usd"].sum() - v[ship['con_companyid'].notna()].sum()) < 1)
    chk("04(수입) 금액 == 03(수입) 금액",
        abs(grp["imp_value_usd"].sum() - firm["imp_value_usd"].sum()) < 1)
    for name, df in (("03_firm", firm), ("04_group", grp)):
        s = df[df["imp_value_usd"].notna()]
        ident = (s["imp_value_classified"].fillna(0) + s["imp_value_unmatched"].fillna(0))
        chk(f"{name}: classified+unmatched == value_usd",
            bool((ident - s["imp_value_usd"]).abs().max() < 1))
    ages = pd.concat([pd.to_numeric(d[c], errors="coerce").dropna()
                      for d, c in ((firm, "fin_age_days"), (grp, "fin_age_days"),
                                   (pair, "shp_fin_age_days"), (pair, "con_fin_age_days"))])
    chk("fin_age_days ∈ [1, 730]", bool(ages.min() >= 1 and ages.max() <= 730),
        f"min {ages.min():.0f} · max {ages.max():.0f}")

    A(f"\n**결과: {sum(ok)}/{len(ok)} 통과**")
    A("\n## 참고 수치")
    ig = ship["intra_group"]
    A(f"- 관계 분포(건수): " + " · ".join(
        f"{k} {v_:,}" for k, v_ in ship['relationship'].value_counts().items()))
    A(f"- intra_group=1 금액(self 포함): {v[ig == 1].sum()/1e9:,.1f} 십억$ "
      f"(분류된 거래의 {100*v[ig == 1].sum()/v[ig.notna()].sum():.1f}%)")
    for name, df in (("03_firm", firm), ("04_group", grp)):
        s = df[df["imp_value_usd"].notna()]
        cov = s.loc[s["has_financials"] == 1, "imp_value_usd"].sum() / s["imp_value_usd"].sum()
        A(f"- {name} 재무가 덮는 수입액: {100*cov:.1f}%")
    (OUT / "90_checks.md").write_text("\n".join(L), encoding="utf-8")
    if not all(ok):
        sys.exit(1)


if __name__ == "__main__":
    main()
