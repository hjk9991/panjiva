r"""
wf2q_30_build_panels.py — within-firm 파일럿(2024 H1) L3 분석패널 층

입력:  L2\fact_shipment*.parquet, L0\company.parquet, fin_quarterly.parquet
산출:  L3\panel_pair_month.parquet    1행 = consignee×shipper×월 (활동 월만)
       L3\dim_relationship.parquet    1행 = pair (관계 이력 요약, 절단 플래그)
       L3\panel_firm_quarter.parquet  1행 = 기업×방향×분기 (+CIQ 분기재무 as-of 결합)
       L3\panel_firm_origin_hs.parquet 1행 = 수입기업×원산국×HS6 (2024H1)
       L3\dim_group_trade_potential.parquet  스키마 스텁 (§3.5 자리 예약)
       L3\*.dta                       Stata 핸드오프 (pair_month·firm_quarter·relationship)
       L3\diag_panels.md              키 유일성·합계 대사 게이트

정의 (사용자 확정 + 실측 제약):
  * pair = 양측 CIQ 매칭 선적의 (consignee_ciqid, shipper_ciqid) — §3.1 v1 방침
  * within_firm(선적) = PIT 동일 ultimate parent (self·parent_sub·sibling 통합).
    지분율 부재로 related_minority 는 산출 불가 — 스키마에 자리만 둔다.
  * within_firm(pair-월) = 금액가중 within 비중 > 0.5 (within_share 도 저장)
  * 재무 as-of = 분기 시작일 이전 마지막 periodEndDate (미래정보 차단)

사용법:  python scripts\processing\wf2q_30_build_panels.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

STAGE = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q")
L0, L2, L3 = STAGE / "L0", STAGE / "L2", STAGE / "L3"
QUARTERS = {"2024Q1": ("2024-01-01", ["2024-01", "2024-02", "2024-03"]),
            "2024Q2": ("2024-04-01", ["2024-04", "2024-05", "2024-06"])}
FIN_COLS = ["revenue", "cogs", "assets", "inventory", "capex", "ppent", "lt_debt"]


def top_by(df: pd.DataFrame, keys: list, col: str, weight: str, out: str) -> pd.DataFrame:
    """그룹별 가중치 최대 값 1개 (결측 제외). merge 용 DataFrame 반환."""
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=keys + [out])
    return (sub.groupby(keys + [col], dropna=False, observed=True)[weight].sum()
               .reset_index().sort_values(weight, ascending=False)
               .drop_duplicates(keys).rename(columns={col: out})[keys + [out]])


def fin_asof(fin: pd.DataFrame, q_start: str) -> pd.DataFrame:
    """분기 시작일 '이전에 끝난' 마지막 분기 재무 1행/기업 + lag 일수."""
    sub = fin[fin["period_end"] < pd.Timestamp(q_start)].sort_values("period_end")
    out = sub.drop_duplicates("companyid", keep="last").copy()
    out["fin_lag_days"] = (pd.Timestamp(q_start) - out["period_end"]).dt.days
    keep = ["companyid", "period_end", "filing_date", "fin_currency",
            "fin_lag_days"] + FIN_COLS
    return out[keep].rename(columns={"period_end": "fin_period_end",
                                     "filing_date": "fin_filing_date"})


def to_stata(df: pd.DataFrame, path: Path) -> None:
    """Stata 118 핸드오프. Int64/NA 를 Stata 가 받는 타입으로 강제."""
    out = df.copy()
    for c in out.columns:
        if str(out[c].dtype) == "Int64":
            out[c] = out[c].astype("float64")
        elif str(out[c].dtype).startswith("string"):
            out[c] = out[c].fillna("").astype(str)
        elif out[c].dtype == object:
            out[c] = out[c].fillna("").astype(str)
        elif str(out[c].dtype).startswith("datetime"):
            out[c] = out[c].dt.strftime("%Y-%m-%d").fillna("")
    out.columns = [c[:32] for c in out.columns]
    out.to_stata(path, write_index=False, version=118)


def main() -> None:
    L3.mkdir(parents=True, exist_ok=True)
    diag = ["# L3 panels 빌드 진단 (wf2q_30_build_panels.py)", ""]

    print("[1/6] fact 로드")
    cols = ["record_id", "arrival_date", "ym", "consignee_ciqid", "shipper_ciqid",
            "consignee_pcid", "shipper_pcid", "origin_country", "hs6_main", "hs2_main",
            "n_hs6", "weight_kg", "value_usd", "teu", "within_firm", "relationship",
            "con_own_fallback", "shp_own_fallback"]
    imp = pd.read_parquet(L2 / "fact_shipment.parquet", columns=cols)
    co = pd.read_parquet(L0 / "company.parquet")
    fin = pd.read_parquet(L0 / "fin_quarterly.parquet")
    fin["period_end"] = pd.to_datetime(fin["period_end"])

    up_country = co.set_index("companyid")["ultimate_parent_country_iso2"]

    both = imp[imp["consignee_ciqid"].notna() & imp["shipper_ciqid"].notna()].copy()
    diag += [f"- 수입 선적 {len(imp):,} / 양측 매칭 {len(both):,} "
             f"({len(both) / len(imp):.1%}; 금액 기준 "
             f"{both['value_usd'].sum() / imp['value_usd'].sum():.1%})"]

    # ---- 2. panel_pair_month ---------------------------------------------
    print("[2/6] panel_pair_month")
    keys = ["consignee_ciqid", "shipper_ciqid", "ym"]
    g = both.groupby(keys, dropna=False)
    pm = g.agg(
        n_shipments=("record_id", "nunique"),
        teu=("teu", "sum"), weight_kg=("weight_kg", "sum"),
        value_usd=("value_usd", "sum"),
        n_hs6=("hs6_main", "nunique"),
        n_within=("within_firm", "sum"),
        n_rel=("within_firm", "count"),
    ).reset_index()

    vw = (both.assign(vw=both["value_usd"].fillna(0) * both["within_firm"].astype(float))
              .groupby(keys, dropna=False)
              .agg(v_within=("vw", "sum"), v_all=("value_usd", "sum")).reset_index())
    pm = pm.merge(vw, on=keys, how="left")
    pm["within_share"] = (pm["v_within"] / pm["v_all"].where(pm["v_all"] > 0))
    # 금액이 전부 결측인 pair-월은 건수 기준으로 폴백
    cnt_share = pm["n_within"] / pm["n_rel"].where(pm["n_rel"] > 0)
    pm["within_share"] = pm["within_share"].fillna(cnt_share)
    pm["within_firm"] = (pm["within_share"] > 0.5).astype("int8")
    pm = pm.drop(columns=["v_within", "v_all", "n_within", "n_rel"])

    pm = pm.merge(top_by(both, keys, "hs6_main", "value_usd", "hs6_main"),
                  on=keys, how="left")
    pm = pm.merge(top_by(both, keys, "origin_country", "value_usd", "origin_main"),
                  on=keys, how="left")

    pm["shp_up_country"] = pm["shipper_ciqid"].map(up_country)
    pm["kor_mnc_link"] = ((pm["shp_up_country"] == "KR")
                          & (pm["origin_main"] == "South Korea")
                          & (pm["within_firm"] == 1)).astype("int8")
    pm["pair_id"] = (pm["consignee_ciqid"].astype("int64").astype(str) + "_"
                     + pm["shipper_ciqid"].astype("int64").astype(str))

    dup = pm.duplicated(["pair_id", "ym"]).sum()
    diag.append(f"- G4 pair_month 키 중복: **{dup}** (0 이어야 함)")
    if dup:
        sys.exit(f"[게이트 실패] pair_month 키 중복 {dup}")
    # 합계 대사: pair 층 금액 합 = 양측 매칭 선적 금액 합
    ok = np.isclose(pm["value_usd"].sum(), both["value_usd"].sum(), rtol=1e-9)
    diag.append(f"- G5 pair 층 금액 대사: {pm['value_usd'].sum():,.0f} vs "
                f"{both['value_usd'].sum():,.0f} {'✅' if ok else '❌'}")
    if not ok:
        sys.exit("[게이트 실패] pair 층 금액 합계 불일치")
    pm.to_parquet(L3 / "panel_pair_month.parquet", index=False)
    print(f"  -> panel_pair_month.parquet {len(pm):,}행 (pair {pm['pair_id'].nunique():,})")

    # ---- 3. dim_relationship ---------------------------------------------
    print("[3/6] dim_relationship")
    pm_s = pm.sort_values(["pair_id", "ym"])
    midx = {m: i for i, m in enumerate(sorted(pm["ym"].unique()))}
    pm_s["m_i"] = pm_s["ym"].map(midx)
    gaps = pm_s.groupby("pair_id")["m_i"].apply(lambda s: int((s.diff() > 1).sum()))
    rel = (pm_s.groupby("pair_id")
               .agg(consignee_ciqid=("consignee_ciqid", "first"),
                    shipper_ciqid=("shipper_ciqid", "first"),
                    first_ym=("ym", "min"), last_ym=("ym", "max"),
                    n_active_months=("ym", "nunique"),
                    n_shipments=("n_shipments", "sum"),
                    value_usd=("value_usd", "sum"),
                    ever_within=("within_firm", "max"),
                    within_share=("within_share", "mean"),
                    kor_mnc_link=("kor_mnc_link", "max"))
               .reset_index())
    rel["n_spells"] = rel["pair_id"].map(gaps).fillna(0).astype(int) + 1
    rel["left_censored"] = (rel["first_ym"] == "2024-01").astype("int8")
    rel["right_censored"] = (rel["last_ym"] == "2024-06").astype("int8")
    rel = rel.merge(top_by(pm, ["pair_id"], "hs6_main", "value_usd", "hs6_main"),
                    on="pair_id", how="left")
    rel = rel.merge(top_by(pm, ["pair_id"], "origin_main", "value_usd", "origin_main"),
                    on="pair_id", how="left")
    rel.to_parquet(L3 / "dim_relationship.parquet", index=False)
    diag.append(f"- dim_relationship: {len(rel):,} pair / 절단(좌·우) "
                f"{rel['left_censored'].mean():.0%}·{rel['right_censored'].mean():.0%}")
    print(f"  -> dim_relationship.parquet {len(rel):,}행")

    # ---- 4. panel_firm_quarter -------------------------------------------
    print("[4/6] panel_firm_quarter")
    imp["yq"] = imp["ym"].map(
        {m: q for q, (_, ms) in QUARTERS.items() for m in ms})
    both["yq"] = both["ym"].map(
        {m: q for q, (_, ms) in QUARTERS.items() for m in ms})

    blocks = []
    # 수입측: 기업 = 매칭된 consignee. 총계는 자기 매칭 선적 전체,
    # 파트너·within 지표는 상대측도 매칭된 부분집합에서 계산 (분모 차이를 문서화).
    sub = imp[imp["consignee_ciqid"].notna()]
    g = sub.groupby(["consignee_ciqid", "yq"], dropna=False)
    b = g.agg(n_shipments=("record_id", "nunique"), teu=("teu", "sum"),
              weight_kg=("weight_kg", "sum"), value_usd=("value_usd", "sum"),
              n_origin_countries=("origin_country", "nunique"),
              n_hs6=("hs6_main", "nunique")).reset_index()
    gb = both.groupby(["consignee_ciqid", "yq"], dropna=False)
    b2 = gb.agg(n_partners=("shipper_ciqid", "nunique"),
                value_classified=("value_usd", "sum")).reset_index()
    w = both[both["within_firm"] == 1].groupby(
        ["consignee_ciqid", "yq"], dropna=False)
    b3 = w.agg(n_partners_within=("shipper_ciqid", "nunique"),
               value_within=("value_usd", "sum")).reset_index()
    hhi = (both.groupby(["consignee_ciqid", "yq", "shipper_ciqid"], dropna=False)
               ["value_usd"].sum().reset_index())
    hhi["tot"] = hhi.groupby(["consignee_ciqid", "yq"])["value_usd"].transform("sum")
    hhi["sh2"] = (hhi["value_usd"] / hhi["tot"].where(hhi["tot"] > 0)) ** 2
    b4 = (hhi.groupby(["consignee_ciqid", "yq"])["sh2"].sum()
             .rename("hhi_partners").reset_index())
    # 관계 회전율: pair 최초/최후 활동 분기 기준 (창 절단 — 진단에 명시)
    pq = pm.copy()
    pq["yq"] = pq["ym"].map({m: q for q, (_, ms) in QUARTERS.items() for m in ms})
    fl = pq.groupby("pair_id").agg(fq=("yq", "min"), lq=("yq", "max"),
                                   con=("consignee_ciqid", "first")).reset_index()
    ent = (fl.groupby(["con", "fq"]).size().rename("n_entry").reset_index()
             .rename(columns={"con": "consignee_ciqid", "fq": "yq"}))
    ext = (fl.groupby(["con", "lq"]).size().rename("n_exit").reset_index()
             .rename(columns={"con": "consignee_ciqid", "lq": "yq"}))
    for extra in (b2, b3, b4, ent, ext):
        b = b.merge(extra, on=["consignee_ciqid", "yq"], how="left")
    b["share_within"] = b["value_within"] / b["value_classified"].where(
        b["value_classified"] > 0)
    b = b.rename(columns={"consignee_ciqid": "ciq_companyid"})
    b["direction"] = "import"
    blocks.append(b)

    # 수출측: 기업 = 매칭된 미국 수출자. consignee 부재로 파트너 지표 없음.
    exp = pd.read_parquet(
        L2 / "fact_shipment_export.parquet",
        columns=["record_id", "ym", "exporter_ciqid", "dest_country",
                 "hs6_main", "weight_kg", "value_usd", "teu"])
    exp["yq"] = exp["ym"].map({m: q for q, (_, ms) in QUARTERS.items() for m in ms})
    sube = exp[exp["exporter_ciqid"].notna()]
    ge = sube.groupby(["exporter_ciqid", "yq"], dropna=False)
    be = ge.agg(n_shipments=("record_id", "nunique"), teu=("teu", "sum"),
                weight_kg=("weight_kg", "sum"), value_usd=("value_usd", "sum"),
                n_dest_countries=("dest_country", "nunique"),
                n_hs6=("hs6_main", "nunique")).reset_index()
    be = be.rename(columns={"exporter_ciqid": "ciq_companyid"})
    be["direction"] = "export"
    blocks.append(be)

    fq = pd.concat(blocks, ignore_index=True)

    # 재무 as-of 결합 (분기 시작 전 마지막 분기)
    parts = []
    for q, (q_start, _) in QUARTERS.items():
        chunk = fq[fq["yq"] == q].merge(fin_asof(fin, q_start),
                                        left_on="ciq_companyid",
                                        right_on="companyid", how="left")
        parts.append(chunk.drop(columns=["companyid"]))
    fq = pd.concat(parts, ignore_index=True)
    fq["has_financials"] = fq["revenue"].notna().astype("int8")

    dup = fq.duplicated(["ciq_companyid", "direction", "yq"]).sum()
    diag.append(f"- G4 firm_quarter 키 중복: **{dup}**")
    if dup:
        sys.exit(f"[게이트 실패] firm_quarter 키 중복 {dup}")
    diag.append(f"- firm_quarter: {len(fq):,}행 / 재무 보유 "
                f"{fq['has_financials'].mean():.1%}")
    fq.to_parquet(L3 / "panel_firm_quarter.parquet", index=False)
    print(f"  -> panel_firm_quarter.parquet {len(fq):,}행")

    # ---- 5. panel_firm_origin_hs (2024H1) + 스텁 --------------------------
    print("[5/6] panel_firm_origin_hs")
    sub = imp[imp["consignee_ciqid"].notna()]
    fo = (sub.groupby(["consignee_ciqid", "origin_country", "hs6_main"], dropna=False)
             .agg(n_shipments=("record_id", "nunique"), weight_kg=("weight_kg", "sum"),
                  value_usd=("value_usd", "sum"), teu=("teu", "sum"))
             .reset_index())
    wsub = both[both["within_firm"] == 1]
    fo_w = (wsub.groupby(["consignee_ciqid", "origin_country", "hs6_main"], dropna=False)
                ["value_usd"].sum().rename("value_within").reset_index())
    fo = fo.merge(fo_w, how="left",
                  on=["consignee_ciqid", "origin_country", "hs6_main"])
    fo["period"] = "2024H1"
    fo.to_parquet(L3 / "panel_firm_origin_hs.parquet", index=False)
    print(f"  -> panel_firm_origin_hs.parquet {len(fo):,}행")

    # §3.5 dim_group_trade_potential — 자리 예약 (본구축에서 적재)
    pd.DataFrame(columns=["us_ciq_companyid", "group_up_id", "member_ciq_companyid",
                          "member_country_iso2", "has_trade_link"]) \
      .to_parquet(L3 / "dim_group_trade_potential.parquet", index=False)

    # ---- 6. Stata 핸드오프 -------------------------------------------------
    print("[6/6] Stata .dta")
    to_stata(pm, L3 / "panel_pair_month.dta")
    to_stata(fq, L3 / "panel_firm_quarter.dta")
    to_stata(rel, L3 / "dim_relationship.dta")

    (L3 / "diag_panels.md").write_text("\n".join(diag) + "\n", encoding="utf-8")
    print(f"완료. 진단: {L3 / 'diag_panels.md'}")


if __name__ == "__main__":
    main()
