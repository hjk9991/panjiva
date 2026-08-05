r"""
wf2q_40_stats_report.py — within-firm 파일럿(2024 H1) §3.6 기초통계 + 진단 리포트

입력:  L1\dim_company.parquet, L2\fact_shipment*.parquet, L3\panel_*.parquet
산출:  projects\20251201\output\tables\wf2q\t1~t5, d1~d7 (CSV)
       staging\within_firm_pilot_2q\95_report.md  (요약 리포트)

§3.6 기초통계 5종:
  t1  월별 pair·기업 수·within 비중 (건수/TEU/금액 3중 기준)
  t2  관계 활동개월 분포 (within vs arms, 절단 플래그 병기)
  t3  수입기업당 파트너 수 분포 (왜도 확인 — Alviarez et al. 2022 Table 5–6 대비)
  t4  월간 마진 분해 (Δlog 총액 = Δlog pair 수 + Δlog pair당 HS + Δlog HS당 금액) + DHS 진입·이탈
  t5  한국 origin 서브셋: t1~t4 요약 + kor_mnc_link 비중

진단 7종: 매칭률 / redaction / 포워더 민감도 / n_hs6 / PIT fallback / 재무 커버리지 / 수출 목적지

사용법:  python scripts\processing\wf2q_40_stats_report.py
"""

from pathlib import Path

import numpy as np
import pandas as pd

STAGE = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q")
L0, L1, L2, L3 = STAGE / "L0", STAGE / "L1", STAGE / "L2", STAGE / "L3"
TABLES = Path(r"C:\panjiva\projects\20251201\output\tables\wf2q")


def md_table(df: pd.DataFrame) -> str:
    def fmt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
            return ""
        if isinstance(v, float):
            return f"{v:,.3f}" if abs(v) < 100 else f"{v:,.0f}"
        try:
            return f"{int(v):,}"
        except (TypeError, ValueError):
            return str(v)
    head = "| " + " | ".join(str(c) for c in df.columns) + " |"
    rule = "|" + "|".join("---" for _ in df.columns) + "|"
    body = ["| " + " | ".join(fmt(v) for v in r) + " |"
            for r in df.itertuples(index=False, name=None)]
    return "\n".join([head, rule] + body)


def save(df: pd.DataFrame, name: str) -> pd.DataFrame:
    df.to_csv(TABLES / f"{name}.csv", index=False, encoding="utf-8-sig")
    print(f"  -> {name}.csv ({len(df):,}행)")
    return df


def overview(imp: pd.DataFrame, pm: pd.DataFrame, label: str) -> pd.DataFrame:
    """월별 규모·within 비중 (건수/TEU/금액). imp = fact 슬라이스, pm = pair_month 슬라이스."""
    rows = []
    for ym, g in imp.groupby("ym"):
        both = g[g["within_firm"].notna()]
        p = pm[pm["ym"] == ym]
        rows.append({
            "ym": ym, "subset": label,
            "n_shipments": g["record_id"].nunique(),
            "n_shipments_classified": both["record_id"].nunique(),
            "n_pairs": p["pair_id"].nunique(),
            "n_importers": p["consignee_ciqid"].nunique(),
            "n_exporters": p["shipper_ciqid"].nunique(),
            "within_share_count": both["within_firm"].mean(),
            "within_share_teu": (both.loc[both["within_firm"] == 1, "teu"].sum()
                                 / both["teu"].sum() if both["teu"].sum() else np.nan),
            "within_share_value": (both.loc[both["within_firm"] == 1, "value_usd"].sum()
                                   / both["value_usd"].sum()
                                   if both["value_usd"].sum() else np.nan),
            "value_usd_total": g["value_usd"].sum(),
        })
    return pd.DataFrame(rows)


def margin_decomp(pm: pd.DataFrame, label: str) -> pd.DataFrame:
    """월간 분해: log V = log N_pair + log (pair당 HS) + log (HS당 금액) + DHS 진입·이탈."""
    m = (pm.groupby("ym")
           .agg(n_pairs=("pair_id", "nunique"), value=("value_usd", "sum"),
                pair_hs=("n_hs6", "sum")).reset_index().sort_values("ym"))
    m["hs_per_pair"] = m["pair_hs"] / m["n_pairs"]
    m["val_per_hs"] = m["value"] / m["pair_hs"]
    for c, o in (("value", "dlog_value"), ("n_pairs", "dlog_pairs"),
                 ("hs_per_pair", "dlog_hs_per_pair"), ("val_per_hs", "dlog_val_per_hs")):
        m[o] = np.log(m[c]).diff()
    m["decomp_check"] = (m["dlog_pairs"] + m["dlog_hs_per_pair"]
                         + m["dlog_val_per_hs"] - m["dlog_value"])
    # DHS: 전월 대비 pair 진입·이탈 (활동 여부 기준)
    act = pm.groupby(["pair_id", "ym"])["value_usd"].sum().unstack(fill_value=np.nan)
    months = sorted(act.columns)
    dhs = []
    for prev, cur in zip(months, months[1:]):
        a, b = act[prev], act[cur]
        entry = (a.isna() & b.notna())
        exit_ = (a.notna() & b.isna())
        dhs.append({"ym": cur, "n_entry": int(entry.sum()), "n_exit": int(exit_.sum()),
                    "entry_value": b[entry].sum(), "exit_value_prev": a[exit_].sum()})
    m = m.merge(pd.DataFrame(dhs), on="ym", how="left")
    m["subset"] = label
    return m


def main() -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    rep = ["# Within-firm 파일럿(2024 H1) — 기초통계·진단 리포트", "",
           f"생성: wf2q_40_stats_report.py, {pd.Timestamp.now():%Y-%m-%d %H:%M}", "",
           "표 원본: `projects\\20251201\\output\\tables\\wf2q\\*.csv`", ""]

    print("[로드]")
    imp = pd.read_parquet(
        L2 / "fact_shipment.parquet",
        columns=["record_id", "ym", "consignee_ciqid", "shipper_ciqid",
                 "consignee_pcid", "shipper_pcid", "origin_country", "hs2_main",
                 "n_hs6", "weight_kg", "value_usd", "teu", "within_firm",
                 "relationship", "con_own_fallback", "shp_own_fallback"])
    pm = pd.read_parquet(L3 / "panel_pair_month.parquet")
    rel = pd.read_parquet(L3 / "dim_relationship.parquet")
    fq = pd.read_parquet(L3 / "panel_firm_quarter.parquet")
    dim = pd.read_parquet(L1 / "dim_company.parquet",
                          columns=["pcid", "is_redacted", "is_forwarder",
                                   "is_forwarder_heur"])
    exp = pd.read_parquet(L2 / "fact_shipment_export.parquet",
                          columns=["record_id", "ym", "dest_country", "dest_source",
                                   "exporter_ciqid", "value_usd"])

    # ---- t1 ---------------------------------------------------------------
    print("[t1] 월별 개요")
    t1 = save(overview(imp, pm, "all"), "t1_monthly_overview")
    rep += ["## t1. 월별 개요 (전체 수입)", "", md_table(t1.round(4)), ""]

    # ---- t2 ---------------------------------------------------------------
    print("[t2] 관계 지속")
    t2 = (rel.groupby(["ever_within", "n_active_months"])
             .agg(n_pairs=("pair_id", "size"), value_usd=("value_usd", "sum"),
                  left_censored=("left_censored", "mean"),
                  right_censored=("right_censored", "mean")).reset_index())
    t2["share_pairs"] = t2["n_pairs"] / t2.groupby("ever_within")["n_pairs"].transform("sum")
    save(t2, "t2_relationship_duration")
    rep += ["## t2. 관계 활동개월 분포 (6개월 창 — 대부분 절단, 파이프라인 검증용)", "",
            md_table(t2.round(3)), ""]

    # ---- t3 ---------------------------------------------------------------
    print("[t3] 파트너 분포")
    rows = []
    for (yq,), g in fq[fq["direction"] == "import"].groupby(["yq"]):
        s = g["n_partners"].dropna()
        rows.append({"yq": yq, "n_importers": len(s), "mean": s.mean(),
                     "p25": s.quantile(.25), "p50": s.quantile(.5),
                     "p75": s.quantile(.75), "p90": s.quantile(.9),
                     "p99": s.quantile(.99), "max": s.max()})
    t3 = save(pd.DataFrame(rows), "t3_partners_per_importer")
    rep += ["## t3. 수입기업당 파트너 수 (분기)", "", md_table(t3.round(2)), "",
            "중위 1~2·극단 왜도가 문헌(Alviarez et al. 2022 Tab.5–6) 예상과 부합하는지 확인.", ""]

    # ---- t4 ---------------------------------------------------------------
    print("[t4] 마진 분해")
    t4 = save(margin_decomp(pm, "all"), "t4_margin_decomposition")
    rep += ["## t4. 월간 마진 분해 (양측 매칭 pair 기준)", "",
            md_table(t4[["ym", "n_pairs", "value", "dlog_value", "dlog_pairs",
                         "dlog_hs_per_pair", "dlog_val_per_hs", "decomp_check",
                         "n_entry", "n_exit"]].round(4)), "",
            "`decomp_check` ≈ 0 이어야 함 (로그 항등식).", ""]

    # ---- t5 ---------------------------------------------------------------
    print("[t5] 한국 서브셋")
    kr = imp[imp["origin_country"] == "South Korea"]
    kr_pm = pm[pm["origin_main"] == "South Korea"]
    t5a = overview(kr, kr_pm, "korea_origin")
    link = (kr_pm.groupby("ym")
                 .agg(n_pairs_kor_mnc=("kor_mnc_link", "sum"),
                      value_kor_mnc=("value_usd",
                                     lambda s: s[kr_pm.loc[s.index, "kor_mnc_link"] == 1].sum()))
                 .reset_index())
    t5 = t5a.merge(link, on="ym", how="left")
    t5["kor_mnc_pair_share"] = t5["n_pairs_kor_mnc"] / t5["n_pairs"]
    save(t5, "t5_korea_subset")
    t5b = save(margin_decomp(kr_pm, "korea_origin"), "t5b_korea_margin_decomp")
    rep += ["## t5. 한국 origin 서브셋", "", md_table(t5.round(4)), "",
            "`kor_mnc_link` = 한국계 UP 그룹 내부(within) & origin=KR — "
            "'한국 본사→미국 현지법인' 마이크로 관측치.", ""]

    # ---- 진단 -------------------------------------------------------------
    print("[진단]")
    # d1 매칭률 (건수 vs 금액 가중)
    rows = []
    for ym, g in imp.groupby("ym"):
        v = g["value_usd"].sum()
        rows.append({
            "ym": ym,
            "match_con_cnt": g["consignee_ciqid"].notna().mean(),
            "match_shp_cnt": g["shipper_ciqid"].notna().mean(),
            "match_both_cnt": (g["consignee_ciqid"].notna()
                               & g["shipper_ciqid"].notna()).mean(),
            "match_con_val": g.loc[g["consignee_ciqid"].notna(), "value_usd"].sum() / v,
            "match_shp_val": g.loc[g["shipper_ciqid"].notna(), "value_usd"].sum() / v,
            "match_both_val": g.loc[g["consignee_ciqid"].notna()
                                    & g["shipper_ciqid"].notna(), "value_usd"].sum() / v,
        })
    d1 = save(pd.DataFrame(rows), "d1_match_rates")
    rep += ["## d1. crosswalk 매칭률", "", md_table(d1.round(3)), ""]

    # d2 redaction (당사자 기준 → 선적 기준)
    red = dim.set_index("pcid")["is_redacted"]
    imp["red_any"] = (imp["consignee_pcid"].map(red).fillna(0).astype(int)
                      | imp["shipper_pcid"].map(red).fillna(0).astype(int))
    d2a = (imp.groupby("ym")["red_any"].mean().rename("redacted_share").reset_index())
    d2b = (imp.groupby("hs2_main")["red_any"].mean().rename("redacted_share")
              .reset_index().sort_values("redacted_share", ascending=False).head(15))
    save(d2a, "d2_redaction_by_month")
    save(d2b, "d2_redaction_by_hs2_top15")
    rep += ["## d2. redaction (당사자 이름 은폐·결측)", "", md_table(d2a.round(4)), ""]

    # d3 포워더 민감도 — within 비중이 포워더 포함/제외에 얼마나 흔들리나
    fwd = dim.set_index("pcid")["is_forwarder"]
    imp["fwd_any"] = (imp["consignee_pcid"].map(fwd).fillna(0).astype(int)
                      | imp["shipper_pcid"].map(fwd).fillna(0).astype(int))
    cls = imp[imp["within_firm"].notna()]
    d3 = pd.DataFrame([{
        "subset": "all_classified",
        "n": len(cls), "within_cnt": cls["within_firm"].mean(),
        "within_val": cls.loc[cls["within_firm"] == 1, "value_usd"].sum()
                      / cls["value_usd"].sum()},
        {"subset": "excl_forwarder",
         "n": (cls["fwd_any"] == 0).sum(),
         "within_cnt": cls.loc[cls["fwd_any"] == 0, "within_firm"].mean(),
         "within_val": cls.loc[(cls["fwd_any"] == 0) & (cls["within_firm"] == 1),
                               "value_usd"].sum()
                       / cls.loc[cls["fwd_any"] == 0, "value_usd"].sum()}])
    save(d3, "d3_forwarder_sensitivity")
    rep += ["## d3. 포워더 포함/제외 민감도 (within 비중)", "", md_table(d3.round(4)), ""]

    # d4 n_hs6 분포
    d4 = (imp["n_hs6"].clip(upper=10).value_counts(normalize=True).sort_index()
          .rename("share").reset_index().rename(columns={"index": "n_hs6"}))
    save(d4, "d4_nhs6_distribution")

    # d5 PIT fallback
    mt = imp[imp["consignee_ciqid"].notna() | imp["shipper_ciqid"].notna()]
    d5 = pd.DataFrame([{
        "con_fallback_share": imp.loc[imp["consignee_ciqid"].notna(),
                                      "con_own_fallback"].mean(),
        "shp_fallback_share": imp.loc[imp["shipper_ciqid"].notna(),
                                      "shp_own_fallback"].mean()}])
    save(d5, "d5_pit_fallback")
    rep += ["## d5. PIT 구간 부재 → 스냅샷 fallback 비중 (매칭 선적 기준)", "",
            md_table(d5.round(4)), ""]

    # d6 재무 커버리지
    d6 = (fq.groupby(["direction", "yq"])
            .agg(n_firms=("ciq_companyid", "nunique"),
                 has_fin=("has_financials", "mean")).reset_index())
    save(d6, "d6_fin_coverage")
    rep += ["## d6. 분기재무 as-of 커버리지 (firm_quarter)", "", md_table(d6.round(3)), ""]

    # d7 수출 목적지
    d7 = (exp.groupby(["ym", "dest_source"]).size().rename("n").reset_index())
    d7["share"] = d7["n"] / d7.groupby("ym")["n"].transform("sum")
    save(d7, "d7_export_dest_source")
    kr_exp = exp[exp["dest_country"] == "South Korea"]
    rep += ["## d7. 수출 목적지 출처 (consignee 부재 — 기업×목적지까지만)", "",
            md_table(d7.round(3)), "",
            f"미국→한국 수출 선적: {len(kr_exp):,}건, "
            f"금액 ${kr_exp['value_usd'].sum():,.0f} "
            f"(매칭 수출기업 {kr_exp['exporter_ciqid'].nunique():,}개)", ""]

    # 한계 요약 (설계문서 §5 대응)
    rep += ["## 한계·이월 (본구축)", "",
            "- related_minority(지분율 10–50%) — **산출 불가**(구독에 지분율·1단계 모자관계 없음)",
            "- 수출측 pair/within — **불가**(수출 BoL 에 consignee 식별자 없음, 36컬럼 전수 확인)",
            "- 금액은 Panjiva 추정치(`value_flag='panjiva_est'`) — Census 단위가치 대치는 이월",
            "- ISIC 연결·Census 대비 HS2 커버리지 비율표·id 분열 조화·group_trade_potential 적재 — 이월",
            "- 6개월 창이라 duration 통계는 절단 지배적 — 파이프라인 검증 목적", ""]

    (STAGE / "95_report.md").write_text("\n".join(rep), encoding="utf-8")
    print(f"완료. 리포트: {STAGE / '95_report.md'}")


if __name__ == "__main__":
    main()
