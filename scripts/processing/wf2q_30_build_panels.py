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

2026-08-12 리뷰 반영 (버전맵·DECISIONS 참조 — 사용자 확정 3건):
  * 재무 층을 v1 표준으로 교체 — tom_v1_2024h1\fin_annual·fin_quarterly (9계정 USD 환산,
    소급 2년 한도) + 자기/모회사 × 연간/분기 4블록. 모회사 = 거래시점(PIT+대체) UP.
    (기존: L0 분기재무만·법인만·소급 무제한 → 커버리지 1.3~2.5%)
  * panel_firm_origin_hs 를 L2 균등배분값(value_alloc)으로 재구축 — L2 와 합계 일치
    (기존: 대표HS 100% 귀속이라 두 층의 품목별 합계가 상충)
  * kor_mnc_link 의 수출자 모회사 국적을 거래시점 UP(shipper_up, PIT+대체) 기준으로
    (기존: 현재 스냅샷 UP 국적 — within_firm 의 PIT 기준과 시점 혼용)

사용법:  python scripts\processing\wf2q_30_build_panels.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

STAGE = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q")
L0, L2, L3 = STAGE / "L0", STAGE / "L2", STAGE / "L3"
V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024h1")     # v1 재무 중간산출 (리뷰 반영)
QUARTERS = {"2024Q1": ("2024-01-01", ["2024-01", "2024-02", "2024-03"]),
            "2024Q2": ("2024-04-01", ["2024-04", "2024-05", "2024-06"])}
FIN_LOOKBACK_DAYS = 730                                  # 소급 2년 (v1 결정 3-6 통일)
FIN_MONEY = ["revenue", "cogs", "assets", "ebitda", "inventory", "capex", "ppent", "lt_debt"]
FIN_VALUE_COLS = [f"{c}_usd" for c in FIN_MONEY] + ["employees"]
FIN_META_COLS = ["fin_currency", "fx_per_usd", "is_press_release", "perimeter_change"]


def top_by(df: pd.DataFrame, keys: list, col: str, weight: str, out: str) -> pd.DataFrame:
    """그룹별 가중치 최대 값 1개 (결측 제외). merge 용 DataFrame 반환."""
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=keys + [out])
    return (sub.groupby(keys + [col], dropna=False, observed=True)[weight].sum()
               .reset_index()
               .sort_values([weight, col], ascending=[False, True], kind="mergesort")
               .drop_duplicates(keys).rename(columns={col: out})[keys + [out]])


def fin_asof(fin: pd.DataFrame, q_start: str, prefix: str) -> pd.DataFrame:
    """분기 시작일 '이전에 끝난' 마지막 재무 1행/기업 + lag. 소급 2년 초과는 결측 처리."""
    qs = pd.Timestamp(q_start)
    sub = fin[(fin["period_end"] < qs)
              & (fin["period_end"] >= qs - pd.Timedelta(days=FIN_LOOKBACK_DAYS))]
    out = sub.sort_values("period_end").drop_duplicates("companyid", keep="last").copy()
    out["fin_lag_days"] = (qs - out["period_end"]).dt.days
    keep = ["companyid", "period_end", "fin_lag_days"] + FIN_META_COLS + FIN_VALUE_COLS
    out = out[keep].rename(columns={"period_end": "fin_period_end"})
    return out.rename(columns={c: f"{prefix}{c}" for c in out.columns if c != "companyid"})


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
            "consignee_pcid", "shipper_pcid", "consignee_up", "shipper_up",
            "origin_country", "hs6_main", "hs2_main",
            "n_hs6", "weight_kg", "value_usd", "teu", "within_firm", "relationship",
            "con_own_fallback", "shp_own_fallback"]
    imp = pd.read_parquet(L2 / "fact_shipment.parquet", columns=cols)
    co = pd.read_parquet(L0 / "company.parquet")

    # 재무: v1 표준 (연간+분기, 9계정 USD, 환율·플래그 포함) — 2026-08-12 리뷰 확정
    def _load_fin(name: str) -> pd.DataFrame:
        f = pd.read_parquet(V1 / name)
        f["companyid"] = pd.to_numeric(f["companyid"], errors="coerce").astype("int64")
        f["period_end"] = pd.to_datetime(f["period_end"]).astype("datetime64[ns]")
        return f.sort_values("period_end")
    fin_a, fin_q = _load_fin("fin_annual.parquet"), _load_fin("fin_quarterly.parquet")

    # UP 국가: 거래시점 UP(PIT+대체) id → 소재국. H1 등장 UP 전체를 덮는 신규 참조 테이블.
    upc = pd.read_parquet(V1 / "up_countries.parquet")
    up_country = upc.set_index("companyid")["country_iso2"]

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

    # kor_mnc_link — 수출자 '거래시점' UP(PIT+대체)의 소재국 기준 (2026-08-12 리뷰 확정).
    # 기존(스냅샷 UP 국적)은 within_firm 의 PIT 기준과 시점이 어긋났다.
    pm = pm.merge(top_by(both, keys, "shipper_up", "value_usd", "shp_up_main"),
                  on=keys, how="left")
    pm["shp_up_country"] = pm["shp_up_main"].map(up_country)
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
        columns=["record_id", "ym", "exporter_ciqid", "exporter_up", "dest_country",
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

    # 기업×분기의 모회사 id — 거래시점 UP(금액가중 대표). 수입=consignee_up, 수출=shipper_up.
    up_imp = top_by(imp[imp["consignee_ciqid"].notna()], ["consignee_ciqid", "yq"],
                    "consignee_up", "value_usd", "up_id") \
        .rename(columns={"consignee_ciqid": "ciq_companyid"})
    up_imp["direction"] = "import"
    up_exp = top_by(sube.assign(yq=sube["ym"].map(
                        {m: q for q, (_, ms) in QUARTERS.items() for m in ms})),
                    ["exporter_ciqid", "yq"], "exporter_up", "value_usd", "up_id") \
        .rename(columns={"exporter_ciqid": "ciq_companyid"})
    up_exp["direction"] = "export"
    fq = fq.merge(pd.concat([up_imp, up_exp], ignore_index=True),
                  on=["ciq_companyid", "direction", "yq"], how="left")

    # 재무 as-of 결합 — v1 표준 4블록: 자기(연간 접두어 없음 / 분기 q_) + 모회사(up_a_ / up_q_)
    parts = []
    for q, (q_start, _) in QUARTERS.items():
        chunk = fq[fq["yq"] == q]
        for prefix, key, fin in (("", "ciq_companyid", fin_a),
                                 ("q_", "ciq_companyid", fin_q),
                                 ("up_a_", "up_id", fin_a),
                                 ("up_q_", "up_id", fin_q)):
            chunk = chunk.merge(fin_asof(fin, q_start, prefix),
                                left_on=key, right_on="companyid", how="left") \
                         .drop(columns=["companyid"])
        parts.append(chunk)
    fq = pd.concat(parts, ignore_index=True)
    fq["has_financials"] = fq["revenue_usd"].notna().astype("int8")       # 자기·연간 기준
    fq["up_has_financials"] = fq["up_a_revenue_usd"].notna().astype("int8")

    dup = fq.duplicated(["ciq_companyid", "direction", "yq"]).sum()
    diag.append(f"- G4 firm_quarter 키 중복: **{dup}**")
    if dup:
        sys.exit(f"[게이트 실패] firm_quarter 키 중복 {dup}")
    diag.append(f"- firm_quarter: {len(fq):,}행 / 재무 보유(자기·연간) "
                f"{fq['has_financials'].mean():.1%} / 모회사·연간 "
                f"{fq['up_has_financials'].mean():.1%} "
                f"(리뷰 전 분기·법인만 기준 1.4%)")
    ages = pd.concat([pd.to_numeric(fq[c], errors="coerce").dropna()
                      for c in ("fin_lag_days", "q_fin_lag_days",
                                "up_a_fin_lag_days", "up_q_fin_lag_days")])
    diag.append(f"- 재무 lag: min {ages.min():.0f} · max {ages.max():.0f} "
                f"(1~730 이어야 함) {'✅' if ages.min() >= 1 and ages.max() <= 730 else '❌'}")
    diag.append(f"- kor_mnc_link=1: {int(pm['kor_mnc_link'].sum()):,} pair-월 "
                f"(리뷰 전 스냅샷 UP 기준 477 — 거래시점 UP 로 교체 후 값)")
    fq.to_parquet(L3 / "panel_firm_quarter.parquet", index=False)
    print(f"  -> panel_firm_quarter.parquet {len(fq):,}행")

    # ---- 5. panel_firm_origin_hs (2024H1) + 스텁 --------------------------
    # 2026-08-12 리뷰 확정: 대표HS 100% 귀속 → L2 균등배분값(value_alloc)으로 재구축.
    # 이제 이 패널의 품목별 합계가 L2 fact_shipment_hs 와 정의상 일치한다.
    print("[5/6] panel_firm_origin_hs (균등배분 기준)")
    hs = pd.read_parquet(L2 / "fact_shipment_hs.parquet",
                         columns=["record_id", "hs6", "weight_alloc",
                                  "value_alloc", "teu_alloc"])
    hs = hs.merge(imp[["record_id", "consignee_ciqid", "origin_country", "within_firm"]],
                  on="record_id", how="inner")
    sub = hs[hs["consignee_ciqid"].notna()]
    fo = (sub.groupby(["consignee_ciqid", "origin_country", "hs6"], dropna=False)
             .agg(n_shipments=("record_id", "nunique"),
                  weight_kg=("weight_alloc", "sum"),
                  value_usd=("value_alloc", "sum"),
                  teu=("teu_alloc", "sum"))
             .reset_index().rename(columns={"hs6": "hs6_main"}))
    fo_w = (sub[sub["within_firm"] == 1]
            .groupby(["consignee_ciqid", "origin_country", "hs6"], dropna=False)
            ["value_alloc"].sum().rename("value_within").reset_index()
            .rename(columns={"hs6": "hs6_main"}))
    fo = fo.merge(fo_w, how="left",
                  on=["consignee_ciqid", "origin_country", "hs6_main"])
    fo["period"] = "2024H1"
    fo["alloc_rule"] = "v1_equal"
    fo.to_parquet(L3 / "panel_firm_origin_hs.parquet", index=False)
    # 대사: 배분값 합 == HS 보유 선적의 원값 합 (consignee 매칭분)
    lhs = fo["value_usd"].sum()
    rhs = imp.loc[imp["consignee_ciqid"].notna()
                  & imp["record_id"].isin(hs["record_id"]), "value_usd"].sum()
    diag.append(f"- firm_origin_hs 배분 대사: {lhs:,.0f} vs {rhs:,.0f} "
                f"{'✅' if np.isclose(lhs, rhs, rtol=1e-9) else '❌'}")
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
