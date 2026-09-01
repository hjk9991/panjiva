# -*- coding: utf-8 -*-
r"""
wf_v3_stats.py — v3 기초통계 t1~t10 + 진단 d1~d7 (+ d8·d9 삽입) (명세 §8.3)

산출: `--tables-dir`(기본 `output\tables\wf{start 연도}[_asof]`) 의 CSV + v3 폴더의 `95_report.md`

## 기초통계

  t1  월별 pair·기업 수·within 비중 (건수 / TEU / 금액 3중 기준)
  t2  관계 지속 분포 — 활동개월 수, within vs arms, 절단(censoring) 병기
  t3  수입기업당 파트너 수 분포 (왜도 확인)
  t4  월간 마진 분해 — Δlog 총액 = Δlog pair수 + Δlog pair당 HS + Δlog HS당 금액
  t5  한국 origin 서브셋 요약 + kor_mnc_link 비중

## 수출입 패턴 (사용자 요청 ①~⑤) — 수출 = `panel_firm_export_quarter` (미국 **수출** B/L 의 shipper)

  t6  수입·수출 **동시** firm 수·비중 — 법인(ciqid) 기준 / 모회사(up) 기준, 분기·연간
  t7  방향 전환 — 첫 수입 분기 vs 첫 수출 분기 (좌측절단은 판정불가로 분리)
  t8  수입 source 수 분포 — 원산국 수(firm-분기 · firm-연) / 파트너 수
  t9  수출 목적지 — firm-분기 목적지 수 분포, 상위 목적지(coalesce), 결측 비중
  t10 신규 원산국 진입(t) 전후 수출 변화 — 처리 vs 대조, 2×2 + Δlog·Δ목적지

  ⚠️ `panel_firm_quarter` 의 `exp_*` 는 **수입 B/L 의 shipper** 라 여기서 쓰지 않는다.

## 진단

  d1  매칭률 (건수·금액)
  d2  **당사자 결측** — 두 층(은폐 / CIQ 미등재) + 이름 관용어 (근거: DECISIONS X-6)
  d4  선적당 HS6 개수 분포
  d5  PIT 대체(fallback) 규모
  d6  재무 커버리지 (행 기준 vs 금액 기준)
  d7  수출 목적지 (보조자료) — `coalesce(shpmtdestination, portofunladingcountry)`, 결측 비중 병기
  d8·d9  `95_d8d9_report.md`(wf_v3_d8d9.py 산출) 본문을 리포트 끝에 삽입 (명세 §6.5)

## 이번에 고친 것

  X-6 redaction  기존 `is_redacted` 는 이름이 `TO ORDER`·`N/A` 같은 관용어인지만 봐서
                 **0.0001%(14건)** 로 나왔다. 실제 은폐는 **당사자 블록 전체가 사라지는 것**
                 이고 실측 38.9% 다. 두 가지를 **따로** 센다:
                   `party_missing`  = panjivaid 자체가 없음 (세관 기밀 승인)
                   `name_placeholder` = 이름 칸이 관용어 (B/L 기재 관행)
                 이름이 "redaction" 하나였던 것이 오해의 원인이라 이름부터 나눈다.

  X-7 게이트     "1주 슬라이스 = 254,873건" 같은 **하드코딩 대사값을 없앤다**.
                 필터나 기간이 바뀌면 반드시 깨지는 검증이었다. 원천에서 실측해 대사한다.
"""

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wf_v3_d8d9 import detect_join_v1, default_tables_dir  # noqa: E402
from wf_v3_panels import load_export_ships, quarter_list   # noqa: E402

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
SRC = Path(r"C:\panjiva\data\staging\source\trade")

# panel_firm_quarter 는 2,641열 — 필요한 열만 읽는다
FQ_COLS = ["companyid", "trade_quarter", "imp_n_ship", "imp_value_usd", "imp_n_partners",
           "imp_n_origin_countries", "up"]
# 구간 표 공통 bin
BIN_EDGES = [-1, 0, 1, 2, 5, 10, 10**9]

# B/L 관행상 실당사자를 가리는 기재 (이름 칸 관용어 — 은폐와 다른 것)
PLACEHOLDER = re.compile(
    r"^TO (THE )?ORDER( OF)?\b|^ORDER( OF)?$|^N/?A$|^NOT AVAILABLE|^UNKNOWN"
    r"|^UNAVAILABLE|^CONFIDENTIAL|^WITHHELD|^SAME AS (CONSIGNEE|SHIPPER)|^-+$")

SHIP_COLS = ["panjivarecordid", "arrivaldate", "valueofgoodsusd", "volumeteu",
             "conpanjivaid", "shppanjivaid", "conname", "shpname",
             "con_ciqid", "shp_ciqid", "con_up", "shp_up",
             "con_ownership_is_fallback", "shp_ownership_is_fallback",
             "relationship", "self_shipment", "n_hs6", "hs6", "shpmtorigin",
             "con_up_a_financial_period_id", "con_up_q_financial_period_id",
             "con_a_financial_period_id"]
L = []


def say(s=""):
    print(s)
    L.append(s)


def md(df, fmt="{:,.2f}"):
    def f(v):
        if isinstance(v, float):
            return "" if pd.isna(v) else fmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return "" if v is None else str(v)
    return "\n".join(["| " + " | ".join(map(str, df.columns)) + " |",
                      "|" + "|".join(["---"] * len(df.columns)) + "|"]
                     + ["| " + " | ".join(f(v) for v in r) + " |"
                        for r in df.itertuples(index=False)])


def save(df, name, tables: Path):
    df.to_csv(tables / f"{name}.csv", index=False, encoding="utf-8-sig")
    return df


# ---------------------------------------------------------------------------
def t1_monthly(pm: pd.DataFrame, ship: pd.DataFrame, tables) -> pd.DataFrame:
    """월별 규모와 within 비중 — **건수·TEU·금액 3중 기준**을 나란히 본다."""
    g = pm.groupby("ym")
    t = g.agg(n_pairs=("pair_id", "nunique"),
              n_importers=("con_ciqid", "nunique"),
              n_exporters=("shp_ciqid", "nunique"),
              n_shipments=("n_shipments", "sum"),
              value_usd=("value_usd", "sum"),
              teu=("teu", "sum"),
              v_within=("value_within_firm", "sum"),
              v_arms=("value_arms", "sum"),
              n_within=("n_within_firm", "sum"),
              n_arms=("n_arms", "sum")).reset_index()
    s = ship.assign(ym=ship.arrivaldate.dt.strftime("%Y%m"))
    tw = s[s.relationship.eq("within_firm")].groupby("ym")["volumeteu"].sum()
    ta = s[s.relationship.eq("arms_length")].groupby("ym")["volumeteu"].sum()
    t["teu_within"] = t.ym.map(tw).fillna(0)
    t["teu_arms"] = t.ym.map(ta).fillna(0)
    t["within_share_value"] = (t.v_within / (t.v_within + t.v_arms) * 100).round(2)
    t["within_share_count"] = (t.n_within / (t.n_within + t.n_arms) * 100).round(2)
    t["within_share_teu"] = (t.teu_within / (t.teu_within + t.teu_arms) * 100).round(2)
    return save(t, "t1_monthly_overview", tables)


def t2_duration(rel: pd.DataFrame, tables) -> pd.DataFrame:
    """관계 지속 — 활동개월 분포. **절단된 관계를 섞어 평균 내면 안 된다.**"""
    r = rel.copy()
    # ⚠️ `ever_within` 은 nullable 정수라 NA 가 섞인다 — 그대로 비교하면
    #    "boolean value of NA is ambiguous" 로 죽는다. NA = 분류 가능한 달이 없던 pair 다.
    ever = r.ever_within.eq(1).fillna(False)
    r["kind"] = np.where(ever, "within_firm",
                         np.where(r.ever_within.notna(), "arms_length", "분류불가"))
    r["censored"] = np.where(r.left_censored.eq(1).fillna(False)
                             | r.right_censored.eq(1).fillna(False),
                             "절단(표본 경계에 닿음)", "완결(표본 안에서 시작·종료)")
    t = (r.groupby(["kind", "censored"])
           .agg(n_pairs=("pair_id", "size"),
                median_months=("n_active_months", "median"),
                mean_months=("n_active_months", "mean"),
                p90_months=("n_active_months", lambda x: x.quantile(.9)),
                mean_spells=("n_spells", "mean"),
                value_usd=("value_usd", "sum")).reset_index())
    return save(t, "t2_relationship_duration", tables)


def t3_partners(pm: pd.DataFrame, tables) -> pd.DataFrame:
    """수입기업당 파트너(수출자) 수 분포 — 왜도가 큰 것이 이 자료의 특징이다."""
    n = pm.groupby("con_ciqid")["shp_ciqid"].nunique()
    v = pm.groupby("con_ciqid")["value_usd"].sum()
    q = [.5, .75, .9, .95, .99, 1.0]
    t = pd.DataFrame([{"지표": "파트너 수", **{f"p{int(x*100)}": n.quantile(x) for x in q},
                       "평균": n.mean(), "기업 수": len(n)}])
    # 파트너 수 구간별 금액 집중도
    b = pd.cut(n, [0, 1, 2, 5, 10, 50, 10**9],
               labels=["1", "2", "3-5", "6-10", "11-50", "50+"])
    t2 = (pd.DataFrame({"bin": b, "value": v.reindex(n.index)})
          .groupby("bin", observed=True)
          .agg(n_firms=("value", "size"), value_usd=("value", "sum")).reset_index())
    t2["firm_share(%)"] = (t2.n_firms / t2.n_firms.sum() * 100).round(2)
    t2["value_share(%)"] = (t2.value_usd / t2.value_usd.sum() * 100).round(2)
    save(t, "t3_partners_per_importer", tables)
    save(t2, "t3b_partners_bins", tables)
    return t2


def t4_margins(pm: pd.DataFrame, tables) -> pd.DataFrame:
    """월간 마진 분해 — 총액 변화가 **어느 축에서** 나왔는지 나눈다.

        Δlog(총액) = Δlog(pair 수) + Δlog(pair당 HS 수) + Δlog(HS당 금액)

    세 항의 합은 항등식이라 **반올림 오차 말고는 정확히 맞아야 한다**(아래에서 검증).
    """
    g = pm.groupby("ym").agg(value=("value_usd", "sum"),
                             n_pairs=("pair_id", "nunique"),
                             n_hs=("n_hs6", "sum")).reset_index().sort_values("ym")
    g["hs_per_pair"] = g.n_hs / g.n_pairs
    g["value_per_hs"] = g.value / g.n_hs
    for c in ("value", "n_pairs", "hs_per_pair", "value_per_hs"):
        g[f"dlog_{c}"] = np.log(g[c]).diff()
    g["residual"] = (g.dlog_value - g.dlog_n_pairs - g.dlog_hs_per_pair
                     - g.dlog_value_per_hs)
    return save(g, "t4_margin_decomposition", tables)


def t5_korea(pm: pd.DataFrame, tables) -> pd.DataFrame:
    """한국 origin 서브셋 + kor_mnc_link.

    ⚠️ `origin_main` 은 Panjiva 의 **자유 텍스트 국가명**(`South Korea`)이고,
       `shp_up_country` 는 CIQ 의 **ISO2 코드**(`KR`)다. 축이 다르니 섞지 않는다.
    """
    kr = pm[pm.origin_main.astype("string").str.contains("Korea", case=False, na=False)]
    t = pd.DataFrame([{
        "구분": "한국 원산지 pair-월", "행": len(kr),
        "전체 대비(%)": len(kr) / len(pm) * 100,
        "금액($B)": kr.value_usd.sum() / 1e9,
        "전체 금액 대비(%)": kr.value_usd.sum() / pm.value_usd.sum() * 100,
        "within 비중(금액,%)":
            kr.value_within_firm.sum() / max(kr.value_classified.sum(), 1) * 100,
        "kor_mnc_link=1 행": int(kr.kor_mnc_link.sum()),
        "kor_mnc_link 금액($B)": kr.loc[kr.kor_mnc_link == 1, "value_usd"].sum() / 1e9,
    }, {
        "구분": "수출자 모회사가 한국(KR) pair-월",
        "행": int((pm.shp_up_country == "KR").sum()),
        "전체 대비(%)": (pm.shp_up_country == "KR").mean() * 100,
        "금액($B)": pm.loc[pm.shp_up_country == "KR", "value_usd"].sum() / 1e9,
        "전체 금액 대비(%)":
            pm.loc[pm.shp_up_country == "KR", "value_usd"].sum() / pm.value_usd.sum() * 100,
        "within 비중(금액,%)": np.nan, "kor_mnc_link=1 행": int(pm.kor_mnc_link.sum()),
        "kor_mnc_link 금액($B)": pm.loc[pm.kor_mnc_link == 1, "value_usd"].sum() / 1e9,
    }])
    return save(t, "t5_korea_subset", tables)


# ---------------------------------------------------------------------------
def d1_match(ship: pd.DataFrame, tables) -> pd.DataFrame:
    s = ship.assign(ym=ship.arrivaldate.dt.strftime("%Y%m"))
    v = s.valueofgoodsusd.fillna(0)
    rows = []
    for ym, g in s.groupby("ym"):
        gv = g.valueofgoodsusd.fillna(0)
        tot = gv.sum()
        rows.append({
            "ym": ym, "n_shipments": len(g),
            "수입자 매칭(%)": g.con_ciqid.notna().mean() * 100,
            "수입자 매칭 금액(%)": gv[g.con_ciqid.notna()].sum() / tot * 100,
            "수출자 매칭(%)": g.shp_ciqid.notna().mean() * 100,
            "수출자 매칭 금액(%)": gv[g.shp_ciqid.notna()].sum() / tot * 100,
            "양측 매칭(%)": (g.con_ciqid.notna() & g.shp_ciqid.notna()).mean() * 100,
            "양측 매칭 금액(%)":
                gv[g.con_ciqid.notna() & g.shp_ciqid.notna()].sum() / tot * 100,
        })
    return save(pd.DataFrame(rows), "d1_match_rates", tables)


def d2_party_missing(ship: pd.DataFrame, tables) -> pd.DataFrame:
    """X-6 — 당사자 결측을 **두 층으로 나눠** 센다.

    기존 지표는 이름 관용어만 봐서 0.0001% 였다. 실제 은폐는 블록 전체가 사라진다.
    """
    v = ship.valueofgoodsusd.fillna(0)
    rows = []
    for side, lab in (("con", "수입자"), ("shp", "수출자")):
        miss = ship[f"{side}panjivaid"].isna()
        nm = ship[f"{side}name"].astype("string").str.upper().str.strip()
        ph = nm.str.match(PLACEHOLDER, na=False)
        nociq = (~miss) & ship[f"{side}_ciqid"].isna()
        rows += [
            {"당사자": lab, "층": "① 은폐 — panjivaid 자체가 없음 (세관 기밀 승인)",
             "선적": int(miss.sum()), "행(%)": miss.mean() * 100,
             "금액($B)": v[miss].sum() / 1e9, "금액(%)": v[miss].sum() / v.sum() * 100},
            {"당사자": lab, "층": "② CIQ 미등재 — panjivaid 는 있는데 crosswalk 실패",
             "선적": int(nociq.sum()), "행(%)": nociq.mean() * 100,
             "금액($B)": v[nociq].sum() / 1e9, "금액(%)": v[nociq].sum() / v.sum() * 100},
            {"당사자": lab, "층": "③ 이름 관용어 (TO ORDER 등) — ①·② 와 별개로 센 것",
             "선적": int(ph.sum()), "행(%)": ph.mean() * 100,
             "금액($B)": v[ph].sum() / 1e9, "금액(%)": v[ph].sum() / v.sum() * 100},
        ]
    return save(pd.DataFrame(rows), "d2_party_missing", tables)


def d4_nhs6(ship: pd.DataFrame, tables) -> pd.DataFrame:
    b = pd.cut(ship.n_hs6.fillna(0), [-1, 0, 1, 2, 5, 10, 10**9],
               labels=["0 (HS 없음)", "1", "2", "3-5", "6-10", "10+"])
    v = ship.valueofgoodsusd.fillna(0)
    t = (pd.DataFrame({"bin": b, "v": v}).groupby("bin", observed=True)
         .agg(n_shipments=("v", "size"), value_usd=("v", "sum")).reset_index())
    t["행(%)"] = (t.n_shipments / t.n_shipments.sum() * 100).round(2)
    t["금액(%)"] = (t.value_usd / t.value_usd.sum() * 100).round(2)
    return save(t, "d4_nhs6_distribution", tables)


def d5_fallback(ship: pd.DataFrame, tables) -> pd.DataFrame:
    v = ship.valueofgoodsusd.fillna(0)
    rows = []
    for side, lab in (("con", "수입자"), ("shp", "수출자")):
        m = ship[f"{side}_ciqid"].notna()
        fb = ship[f"{side}_ownership_is_fallback"] == 1
        rows.append({"당사자": lab, "CIQ 매칭 선적": int(m.sum()),
                     "그중 PIT 대체(fallback)": int((m & fb).sum()),
                     "대체 비율(%)": (m & fb).sum() / max(int(m.sum()), 1) * 100,
                     "대체 금액($B)": v[m & fb].sum() / 1e9})
    return save(pd.DataFrame(rows), "d5_pit_fallback", tables)


def d6_fin_coverage(ship: pd.DataFrame, tables) -> pd.DataFrame:
    v = ship.valueofgoodsusd.fillna(0)
    rows = []
    for c, lab in (("con_up_a_financial_period_id", "수입자 모회사 · 연간"),
                   ("con_up_q_financial_period_id", "수입자 모회사 · 분기"),
                   ("con_a_financial_period_id", "수입자 법인 · 연간")):
        h = ship[c].notna()
        rows.append({"블록": lab, "부착 선적": int(h.sum()),
                     "행 커버(%)": h.mean() * 100,
                     "금액 커버(%)": v[h].sum() / v.sum() * 100})
    return save(pd.DataFrame(rows), "d6_fin_coverage", tables)


def d7_export(ex: pd.DataFrame, tables) -> tuple:
    """수출 — 명세 §2.1 대로 **기업×목적지 보조자료로만** 쓴다.

    목적지 = `coalesce(shpmtdestination, portofunladingcountry)` (`load_export_ships`).
    결측은 "(결측)" 행으로 남기고, coalesce 전/후 결측 비중(행·금액)을 함께 돌려준다.
    수출 선적 **전체**(수출자 매칭 여부 무관)가 분모다 — `n_exporters` 만 매칭된 법인 수.
    """
    if not len(ex):
        return pd.DataFrame(), {}
    v = ex.valueofgoodsusd.fillna(0)
    tot = float(v.sum())
    pre, post = ex.shpmtdestination.isna(), ex.dest_country.isna()
    port = ex.dest_source.eq("port").fillna(False)
    miss = {"pre_rows": pre.mean() * 100, "pre_value": float(v[pre].sum()) / tot * 100,
            "post_rows": post.mean() * 100, "post_value": float(v[post].sum()) / tot * 100,
            "port_rows": port.mean() * 100, "port_value": float(v[port].sum()) / tot * 100}
    t = (ex.assign(dest_country=ex.dest_country.fillna("(결측)"))
         .groupby("dest_country")
         .agg(n_shipments=("valueofgoodsusd", "size"),
              value_usd=("valueofgoodsusd", "sum"),
              n_exporters=("shp_ciqid_original", "nunique")).reset_index()
         .sort_values("value_usd", ascending=False).head(20))
    t["금액(%)"] = (t.value_usd / tot * 100).round(2)
    return save(t, "d7_export_destinations", tables), miss


# ---------------------------------------------------------------------------
# 수출입 패턴 t6~t10 — 수출은 전부 `panel_firm_export_quarter`(미국 수출 B/L 의 shipper)
# ---------------------------------------------------------------------------
def bin_table(n, v, zero_label: str) -> pd.DataFrame:
    """개수 `n` 을 0 / 1 / 2 / 3-5 / 6-10 / 11+ 로 나눠 건수·금액 비중을 센다."""
    labels = [zero_label, "1", "2", "3-5", "6-10", "11+"]
    b = pd.cut(pd.Series(n).astype("float64").to_numpy(), BIN_EDGES, labels=labels)
    # 범주형을 그대로 둬야 표가 0 → 1 → 2 → … 순서로 나온다(numpy 로 바꾸면 사전순이 된다)
    t = (pd.DataFrame({"bin": pd.Categorical(b, categories=labels, ordered=True),
                       "v": pd.Series(v).fillna(0).to_numpy()})
         .groupby("bin", observed=True)
         .agg(n=("v", "size"), value_usd=("v", "sum")).reset_index())
    t["bin"] = t.bin.astype(str)
    t["share(%)"] = (t.n / max(int(t.n.sum()), 1) * 100).round(2)
    t["value_share(%)"] = (t.value_usd / max(float(t.value_usd.sum()), 1) * 100).round(2)
    return t


def t6_both_direction(fq: pd.DataFrame, fx: pd.DataFrame, quarters: list, tables) -> dict:
    """수입·수출 **동시** firm — 법인(ciqid) 기준과 모회사(up) 기준, 분기별 + 연간.

    수입 firm = `panel_firm_quarter.imp_n_ship > 0` 인 `companyid`(수입 B/L consignee),
    수출 firm = `panel_firm_export_quarter.companyid`(수출 B/L shipper, `shp_ciqid_original`).
    모회사 기준은 각각 `panel_firm_quarter.up` · 수출 패널 `up`(그 분기 금액 최대 UP).
    비중 열은 전부 %. `imp_value_share_of_both` = 양방향 firm 의 수입액 / 그 기간 전체 수입액.
    """
    imp = fq.loc[fq.imp_n_ship > 0, ["companyid", "trade_quarter", "imp_value_usd", "up"]]
    exp = fx.loc[fx.n_ship > 0, ["companyid", "trade_quarter", "value_usd", "up"]]
    years = sorted({q[:4] for q in quarters})
    periods = [(q, [q]) for q in quarters] \
        + [(y, [q for q in quarters if q.startswith(y)]) for y in years]
    if len(years) > 1:
        periods.append(("전체", quarters))
    out = {}
    for level, name in (("companyid", "t6_both_direction_firms"),
                        ("up", "t6b_both_direction_parents")):
        rows = []
        for label, qs in periods:
            i = imp[imp.trade_quarter.isin(qs)]
            x = exp[exp.trade_quarter.isin(qs)]
            si = set(i[level].dropna().astype("int64"))
            sx = set(x[level].dropna().astype("int64"))
            both = si & sx
            iv, xv = float(i.imp_value_usd.sum()), float(x.value_usd.sum())
            rows.append({
                "period": label, "n_importers": len(si), "n_exporters": len(sx),
                "n_both": len(both),
                "both_share_of_importers": len(both) / len(si) * 100 if si else np.nan,
                "both_share_of_exporters": len(both) / len(sx) * 100 if sx else np.nan,
                "imp_value_share_of_both":
                    float(i.loc[i[level].isin(both), "imp_value_usd"].sum()) / iv * 100
                    if iv > 0 else np.nan,
                "exp_value_share_of_both":
                    float(x.loc[x[level].isin(both), "value_usd"].sum()) / xv * 100
                    if xv > 0 else np.nan,
            })
        out[name] = save(pd.DataFrame(rows), name, tables)
    return out


T7_ORDER = ["imp_only", "exp_only", "both_from_first_quarter", "imp_then_exp",
            "exp_then_imp", "same_quarter_start"]


def t7_direction_transition(fq: pd.DataFrame, fx: pd.DataFrame, quarters: list,
                            tables) -> tuple:
    """firm(ciqid) 단위 — 표본 안의 **첫 수입 분기 vs 첫 수출 분기** 로 방향 전환을 분류한다.

      imp_only / exp_only            표본 안에서 한 방향만
      both_from_first_quarter        둘 다 표본 첫 분기에 이미 있음 — **좌측절단, 판정 불가**
      imp_then_exp / exp_then_imp    한쪽을 먼저 하다가 나중 분기에 다른 쪽 시작
      same_quarter_start             같은 분기에 둘 다 시작 (표본 첫 분기 아님)
    금액은 그 firm 의 표본 전체 수입액·수출액 합.
    """
    qi = {q: i for i, q in enumerate(quarters)}
    fi = (fq[fq.imp_n_ship > 0].groupby("companyid")
          .agg(first_imp_q=("trade_quarter", "min"), imp_value_usd=("imp_value_usd", "sum")))
    fe = (fx[fx.n_ship > 0].groupby("companyid")
          .agg(first_exp_q=("trade_quarter", "min"), exp_value_usd=("value_usd", "sum")))
    fe.index = fe.index.astype("int64")
    f = fi.join(fe, how="outer")
    a = f.first_imp_q.map(qi).to_numpy(dtype="float64", na_value=np.nan)
    b = f.first_exp_q.map(qi).to_numpy(dtype="float64", na_value=np.nan)
    f["category"] = np.select(
        [np.isnan(b) & ~np.isnan(a), np.isnan(a) & ~np.isnan(b),
         (a == 0) & (b == 0), a < b, b < a],
        ["imp_only", "exp_only", "both_from_first_quarter", "imp_then_exp", "exp_then_imp"],
        default="same_quarter_start")
    t = (f.groupby("category").agg(n_firms=("category", "size"),
                                   imp_value_usd=("imp_value_usd", "sum"),
                                   exp_value_usd=("exp_value_usd", "sum"))
         .reindex(T7_ORDER).fillna(0).reset_index())
    t["n_firms"] = t.n_firms.astype("int64")
    t["share(%)"] = (t.n_firms / max(int(t.n_firms.sum()), 1) * 100).round(2)
    t = t[["category", "n_firms", "share(%)", "imp_value_usd", "exp_value_usd"]]
    cnt = dict(zip(t.category, t.n_firms))
    n_all = int(t.n_firms.sum())
    n_ok = n_all - cnt["both_from_first_quarter"]
    n_tr = cnt["imp_then_exp"] + cnt["exp_then_imp"]
    n_both_ok = n_tr + cnt["same_quarter_start"]
    notes = {"n_all": n_all, "n_assessable": n_ok, "n_transition": n_tr,
             "transition_share_of_assessable": n_tr / max(n_ok, 1) * 100,
             "n_both_assessable": n_both_ok,
             "transition_share_of_both_assessable": n_tr / max(n_both_ok, 1) * 100}
    return save(t, "t7_direction_transition", tables), notes


def t8_import_sources(fq: pd.DataFrame, fo: pd.DataFrame, tables) -> pd.DataFrame:
    """수입 source 수 분포 — 세 기준을 한 csv(`basis` 열)에 쌓는다.

      원산국 수 · firm-분기  = `imp_n_origin_countries`(con 매칭 선적 전체, F4 정의)
      원산국 수 · firm-연    = `panel_firm_origin_quarter` 에서 (con_ciqid, 연도) 별 nunique
                              (결측 원산국은 세지 않되 금액에는 포함)
      파트너 수 · firm-분기  = `imp_n_partners`(양측 매칭 분모 — 0 은 수출자 미매칭)
    """
    imp = fq[fq.imp_n_ship > 0]
    a = bin_table(imp.imp_n_origin_countries, imp.imp_value_usd, "0 (원산국 결측)")
    a.insert(0, "basis", "원산국 수 · firm-분기 (con 매칭 선적 전체)")
    y = (fo.assign(year=fo.trade_quarter.astype(str).str[:4])
         .groupby(["con_ciqid", "year"])
         .agg(n=("shpmtorigin", "nunique"), v=("value_usd", "sum")).reset_index())
    b = bin_table(y.n, y.v, "0 (원산국 결측)")
    b.insert(0, "basis", "원산국 수 · firm-연 (panel_firm_origin_quarter)")
    c = bin_table(imp.imp_n_partners, imp.imp_value_usd, "0 (수출자 미매칭)")
    c.insert(0, "basis", "파트너(수출자 법인) 수 · firm-분기 (양측 매칭)")
    return save(pd.concat([a, b, c], ignore_index=True), "t8_import_sources", tables)


def t9_export_destinations(fx: pd.DataFrame, ex: pd.DataFrame, tables) -> tuple:
    """수출 목적지 — firm-분기 목적지 수 분포 + 상위 15 목적지(coalesce) + 목적지 출처 비중.

    상위 목적지 표의 분모는 **수출자 매칭(`shp_ciqid_original` 비결측) 선적** — 수출 패널과
    같은 모집단이다(d7 은 전체 수출 선적이 분모).
    """
    a = bin_table(fx.n_dest_countries, fx.value_usd, "0 (목적지 결측만)")
    a.insert(0, "basis", "목적지 국가 수 · 수출 firm-분기")
    save(a, "t9_export_destinations", tables)
    m = ex[ex.shp_ciqid_original.notna()]
    tot = float(m.valueofgoodsusd.fillna(0).sum())
    n_exp_all = int(m.shp_ciqid_original.nunique())
    d = (m.assign(dest=m.dest_country.fillna("(결측)"))
         .groupby("dest")
         .agg(n_shipments=("panjivarecordid", "size"), value_usd=("valueofgoodsusd", "sum"),
              n_exporters=("shp_ciqid_original", "nunique")).reset_index()
         .sort_values("value_usd", ascending=False))
    d["value_share(%)"] = (d.value_usd / max(tot, 1) * 100).round(2)
    d["exporter_share(%)"] = (d.n_exporters / max(n_exp_all, 1) * 100).round(2)
    top = save(d.head(15), "t9b_export_top_destinations", tables)
    src = (m.assign(src=m.dest_source.fillna("(결측)"))
           .groupby("src").agg(n_shipments=("panjivarecordid", "size"),
                               value_usd=("valueofgoodsusd", "sum")).reset_index())
    src["rows(%)"] = (src.n_shipments / len(m) * 100).round(2)
    src["value(%)"] = (src.value_usd / max(tot, 1) * 100).round(2)
    return a, top, src


def t10_new_origin_vs_export(fq: pd.DataFrame, fx: pd.DataFrame, fo: pd.DataFrame,
                             quarters: list, tables) -> tuple:
    """신규 원산국 진입(t) 전후의 수출 변화 — 처리군 vs 대조군 기술통계.

    이벤트  = `panel_firm_origin_quarter` 에서 `is_new_origin==1 & new_origin_assessable==1`
              (원산국 결측 행 제외) 인 (con_ciqid, t). t−1·t+1 이 표본 안에 있는 t 만.
    기반    = t−1·t·t+1 모두 수입(`imp_n_ship>0`)이 있는 firm — 처리·대조 **둘 다** 이 조건.
              (t 에 처음 수입을 시작한 firm 은 "신규 원산국" 이 아니라 "신규 수입자" 라 뺀다;
               제외된 이벤트 firm 수를 함께 적는다)
    처리군  = 기반 ∩ t 에 이벤트 있는 firm, 대조군 = 기반 − t 에 신규 원산국 있는 firm.
    결과 (a) 수출 상태 2×2: t−1 수출 여부 × t+1 수출 여부(수출 패널 `n_ship>0`)
         (b) t−1·t+1 모두 수출한 firm: Δlog(value_usd)(양쪽 금액>0), Δn_dest_countries
    표본이 3분기 미만이면 "해당 없음" 으로 정상 종료한다.
    """
    n = len(quarters)
    note = pd.DataFrame([{"note": "해당 없음 — t−1·t+1 이 표본 안에 있는 이벤트 분기가 없다",
                          "n_quarters": n}])
    if n < 3:
        save(note, "t10_new_origin_vs_export", tables)
        save(note, "t10b_new_origin_vs_export_delta", tables)
        return None, None, {"n_event_quarters": 0, "n_events": 0, "n_dropped": 0}
    qi = {q: i for i, q in enumerate(quarters)}
    imp = fq[fq.imp_n_ship > 0]
    pres = (imp.assign(_1=1).pivot_table(index="companyid", columns="trade_quarter",
                                         values="_1", aggfunc="max")
            .reindex(columns=quarters))
    xp = fx.pivot_table(index="companyid", columns="trade_quarter", values="n_ship",
                        aggfunc="sum").reindex(columns=quarters)
    xv = fx.pivot_table(index="companyid", columns="trade_quarter", values="value_usd",
                        aggfunc="sum").reindex(columns=quarters)
    xn = fx.pivot_table(index="companyid", columns="trade_quarter",
                        values="n_dest_countries", aggfunc="max").reindex(columns=quarters)
    for x in (xp, xv, xn):
        x.index = x.index.astype("int64")
    ev = fo[(fo.is_new_origin == 1) & (fo.new_origin_assessable == 1) & fo.shpmtorigin.notna()]
    ev = ev[["con_ciqid", "trade_quarter"]].drop_duplicates()
    ev = ev.assign(con_ciqid=ev.con_ciqid.astype("int64"))

    rows22, rowsd, pool = [], [], {}
    n_events, n_dropped = 0, 0
    for t in quarters[1:-1]:
        tm, tp = quarters[qi[t] - 1], quarters[qi[t] + 1]
        base = pd.Index(pres.index[pres[[tm, t, tp]].notna().all(axis=1)].astype("int64"))
        ev_t = set(ev.loc[ev.trade_quarter == t, "con_ciqid"])
        n_events += len(ev_t)
        treated = base[base.isin(ev_t)]
        n_dropped += len(ev_t) - len(treated)
        control = base[~base.isin(ev_t)]
        for grp, firms in (("처리(신규 원산국 진입)", treated), ("대조(진입 없음)", control)):
            p0 = xp.reindex(firms)[tm].fillna(0).to_numpy() > 0
            p1 = xp.reindex(firms)[tp].fillna(0).to_numpy() > 0
            c = {"exp_neither(0→0)": int((~p0 & ~p1).sum()), "exp_start(0→1)": int((~p0 & p1).sum()),
                 "exp_stop(1→0)": int((p0 & ~p1).sum()), "exp_both(1→1)": int((p0 & p1).sum())}
            rows22.append({"group": grp, "t": t, "n_firms": len(firms), **c})
            both = firms[p0 & p1]
            v0, v1 = xv.reindex(both)[tm].to_numpy(), xv.reindex(both)[tp].to_numpy()
            ok = (v0 > 0) & (v1 > 0)
            dlog = np.log(v1[ok]) - np.log(v0[ok])
            dn = (xn.reindex(both)[tp] - xn.reindex(both)[tm]).dropna().to_numpy()
            rowsd.append({"group": grp, "t": t, "n_exp_both": len(both), "n_dlog": int(ok.sum()),
                          "dlog_value_mean": dlog.mean() if len(dlog) else np.nan,
                          "dlog_value_median": np.median(dlog) if len(dlog) else np.nan,
                          "dn_dest_mean": dn.mean() if len(dn) else np.nan,
                          "dn_dest_median": np.median(dn) if len(dn) else np.nan})
            p = pool.setdefault(grp, {"n": 0, "c": dict.fromkeys(c, 0), "nb": 0,
                                      "dlog": [], "dn": []})
            p["n"] += len(firms)
            for k in c:
                p["c"][k] += c[k]
            p["nb"] += len(both)
            p["dlog"].append(dlog)
            p["dn"].append(dn)
    for grp, p in pool.items():          # 이벤트 분기 합산 행
        rows22.append({"group": grp, "t": "전체", "n_firms": p["n"], **p["c"]})
        dlog, dn = np.concatenate(p["dlog"]), np.concatenate(p["dn"])
        rowsd.append({"group": grp, "t": "전체", "n_exp_both": p["nb"], "n_dlog": len(dlog),
                      "dlog_value_mean": dlog.mean() if len(dlog) else np.nan,
                      "dlog_value_median": np.median(dlog) if len(dlog) else np.nan,
                      "dn_dest_mean": dn.mean() if len(dn) else np.nan,
                      "dn_dest_median": np.median(dn) if len(dn) else np.nan})
    t22 = pd.DataFrame(rows22)
    t22["exp_at_t+1(%)"] = ((t22["exp_start(0→1)"] + t22["exp_both(1→1)"])
                            / t22.n_firms.replace(0, np.nan) * 100).round(2)
    t22["start_among_nonexp_t-1(%)"] = (t22["exp_start(0→1)"]
                                        / (t22["exp_neither(0→0)"] + t22["exp_start(0→1)"])
                                        .replace(0, np.nan) * 100).round(2)
    td = pd.DataFrame(rowsd)
    save(t22, "t10_new_origin_vs_export", tables)
    save(td, "t10b_new_origin_vs_export_delta", tables)
    return t22, td, {"n_event_quarters": n - 2, "n_events": n_events, "n_dropped": n_dropped}


def include_d8d9(v3: Path) -> list:
    """`95_d8d9_report.md`(wf_v3_d8d9.py) 본문을 H1 만 빼고 그대로 가져온다 — 명세 §6.5.

    제목 수준만 한 단계 내려(`##`→`###`) 이 리포트의 절 아래에 들어가게 한다.
    """
    head = "\n---\n\n## d8·d9 — 소유구조 변화와 관계전환 (명세 §6, 상세: 95_d8d9_report.md)\n"
    p = v3 / "95_d8d9_report.md"
    if not p.exists():
        return [head, "> ⚠️ **d8d9 미실행** — `95_d8d9_report.md` 가 없다. `wf_v3_d8d9.py` 를 "
                      "먼저 돌린 뒤 이 스크립트를 다시 돌리면 여기에 자동 삽입된다."]
    body = [("#" + l if l.startswith("##") else l)
            for l in p.read_text(encoding="utf-8").splitlines() if not l.startswith("# ")]
    return [head] + body


# ---------------------------------------------------------------------------
def main() -> None:
    global V1, V3, SRC
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01")
    ap.add_argument("--v1-dir", default=str(V1))
    ap.add_argument("--v3-dir", default=str(V3))
    ap.add_argument("--src-dir", default=str(SRC))
    ap.add_argument("--tables-dir", default=None,
                    help="기본: output\\tables\\wf{start 연도}[_asof] (v1 스키마로 판별)")
    a = ap.parse_args()
    V1, V3, SRC = Path(a.v1_dir), Path(a.v3_dir), Path(a.src_dir)
    join = detect_join_v1(V1, a.start)
    TABLES = Path(a.tables_dir) if a.tables_dir else default_tables_dir(a.start, join)
    TABLES.mkdir(parents=True, exist_ok=True)
    months = list(pd.date_range(a.start, a.end, freq="MS").strftime("%Y%m")[:-1])
    quarters = quarter_list(a.start, a.end)
    t0 = datetime.now()

    print("[1] 입력 로드")
    ship = pd.concat([pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                                      columns=SHIP_COLS) for m in months],
                     ignore_index=True)
    pm = pd.read_parquet(V3 / "panel_pair_month.parquet")
    rel = pd.read_parquet(V3 / "dim_relationship.parquet")
    fq = pd.read_parquet(V3 / "panel_firm_quarter.parquet", columns=FQ_COLS)
    # v2 03_firm 을 base 로 쓴 패널이라 v2 창이 더 넓으면 창 밖 분기 행이 있을 수 있다 —
    # t6~t10 은 `--start/--end` 의 달력 분기만 본다(창이 같으면 0행이 걸러진다).
    n_fq0 = len(fq)
    fq = fq[fq.trade_quarter.isin(quarters)]
    if len(fq) != n_fq0:
        print(f"  ⚠️ panel_firm_quarter 에서 창 밖 분기 {n_fq0 - len(fq):,}행 제외 "
              f"(달력 분기 {quarters[0]}~{quarters[-1]})")
    fx_p, fo_p = V3 / "panel_firm_export_quarter.parquet", V3 / "panel_firm_origin_quarter.parquet"
    fx = pd.read_parquet(fx_p) if fx_p.exists() else None
    fo = pd.read_parquet(fo_p) if fo_p.exists() else None
    ex = load_export_ships(SRC, months)          # 수출 원천 (d7 · t9 공용)
    print(f"  선적 {len(ship):,} · pair-월 {len(pm):,} · pair {len(rel):,} · "
          f"firm-분기 {len(fq):,} · 수출 firm-분기 {0 if fx is None else len(fx):,} · "
          f"원산국-분기 {0 if fo is None else len(fo):,} · 수출 선적 {len(ex):,}")

    say("# v3 기초통계·진단 리포트\n")
    say(f"**생성일** {date.today()} · **기간** {a.start} ~ {a.end}(미포함) · "
        f"**대상** `{V3}` · **결합** {join} · **표** `{TABLES}` · "
        f"**스크립트** `wf_v3_stats.py`\n")

    # --- 원천 실측 대사 (근거: DECISIONS X-7) ---
    say("## 대사 — 원천 실측\n")
    src_n = sum(pd.read_parquet(SRC / f"imp_ship_{m}.parquet",
                                columns=["panjivarecordid"]).shape[0] for m in months)
    both = ship.con_ciqid.notna() & ship.shp_ciqid.notna()
    say(md(pd.DataFrame([{
        "항목": "공용 원천 선적", "값": src_n},
        {"항목": "v1 선적 (이 리포트의 base)", "값": len(ship)},
        {"항목": "차이", "값": len(ship) - src_n},
        {"항목": "양측 CIQ 매칭 선적", "값": int(both.sum())},
        {"항목": "pair-월이 담은 선적", "값": int(pm.n_shipments.sum())},
        {"항목": "차이", "값": int(pm.n_shipments.sum() - both.sum())}])))
    say("\n> 대사값을 코드에 박지 않고 원천에서 실측한다(근거는 `DECISIONS.md` X-7).\n")

    print("[2] 기초통계 t1~t5")
    say("\n## t1. 월별 개요 — 건수·TEU·금액 3중 기준\n")
    t1 = t1_monthly(pm, ship, TABLES)
    say(md(t1[["ym", "n_pairs", "n_importers", "n_shipments", "value_usd",
               "within_share_count", "within_share_teu", "within_share_value"]]
          .rename(columns={"within_share_count": "within%(건수)",
                           "within_share_teu": "within%(TEU)",
                           "within_share_value": "within%(금액)"})))
    say("\n> **세 기준이 크게 다르다.** 금액 기준이 가장 높다 — 그룹내 거래가 대형 "
        "기업에 몰려 있기 때문이다. 어느 기준인지 밝히지 않은 within 비중은 의미가 없다.")

    say("\n## t2. 관계 지속 — 절단 여부를 반드시 갈라 본다\n")
    say(md(t2_duration(rel, TABLES)))
    say("\n> ⚠️ **절단된 관계와 완결된 관계를 섞어 평균을 내면 안 된다.** 표본 첫 달에 "
        "이미 있었거나 마지막 달까지 이어진 관계는 실제 지속기간이 더 길다.")

    say("\n## t3. 수입기업당 파트너 수\n")
    say(md(t3_partners(pm, TABLES)))

    say("\n## t4. 월간 마진 분해\n")
    t4 = t4_margins(pm, TABLES)
    say(md(t4[["ym", "value", "n_pairs", "hs_per_pair", "value_per_hs",
               "dlog_value", "dlog_n_pairs", "dlog_hs_per_pair",
               "dlog_value_per_hs", "residual"]], "{:,.4f}"))
    resid = t4.residual.abs().max()
    say(f"\n- 항등식 잔차 최대 **{resid:.2e}** (0 이어야 정상 — 부동소수 오차 범위)")

    say("\n## t5. 한국 서브셋\n")
    say(md(t5_korea(pm, TABLES)))
    say("\n> ⚠️ `origin_main` 은 Panjiva 의 **자유 텍스트**(`South Korea`), "
        "`shp_up_country` 는 CIQ 의 **ISO2**(`KR`)다. 축이 다르니 두 줄을 더하면 안 된다. "
        "`kor_mnc_link` 는 **후자(모회사 국적) + within_firm** 으로 정의했다.")

    print("[3] 진단 d1~d7")
    say("\n---\n\n## d1. 매칭률\n")
    say(md(d1_match(ship, TABLES)))

    say("\n## d2. 당사자 결측 — 두 층 + 이름 관용어\n")
    say(md(d2_party_missing(ship, TABLES)))
    say("\n> ①은 **복구 불가**(세관이 지움), ②는 crosswalk 보강으로 일부 복구 가능하다. "
        "성격이 다르니 합쳐서 보고하면 안 된다. ③은 B/L 기재 관행(이름 칸 관용어)이라 "
        "①·② 와 별개로 센 것이다 — 층을 나눈 근거는 `DECISIONS.md` X-6.")

    say("\n## d4. 선적당 HS6 개수\n")
    say(md(d4_nhs6(ship, TABLES)))
    say("\n> 다중 HS 선적은 `panel_firm_origin_hs` 에서 **균등배분**된다(결정 X-3). "
        "`single_hs==1` 로 거르면 배분 없는 값만 쓸 수 있다.")

    say("\n## d5. PIT 대체(fallback)\n")
    say(md(d5_fallback(ship, TABLES)))
    say("\n> ⚠️ 명세 §3.2 — 대체로 채운 행은 **연중 소유구조 변화 판정에 쓰면 안 된다**. "
        "d8 이 이미 그렇게 처리한다.")

    say("\n## d6. 재무 커버리지\n")
    say(md(d6_fin_coverage(ship, TABLES)))
    say("\n> **행 기준과 금액 기준이 2배 이상 다르다.** 영세업체가 행 수를 지배하기 때문이다. "
        "두 기준을 병기하지 않은 커버리지 숫자는 오독을 부른다.")

    say("\n## d7. 수출 목적지 (보조자료)\n")
    d7, miss = d7_export(ex, TABLES)
    if len(d7):
        say(f"> ⚠️ **목적지 결측** — `shpmtdestination` 만 쓰면 행 {miss['pre_rows']:.1f}% · "
            f"금액 {miss['pre_value']:.1f}% 가 결측이다. `portofunladingcountry`(양륙항 국가)로 "
            f"대체(coalesce)하면 행 {miss['post_rows']:.1f}% · 금액 {miss['post_value']:.1f}% 로 "
            f"준다(대체된 선적: 행 {miss['port_rows']:.1f}% · 금액 {miss['port_value']:.1f}%). "
            "아래 표의 목적지는 **coalesce 기준**이고 남은 결측은 `(결측)` 행이다.\n")
        say(md(d7.head(15)))
    else:
        say("> 이 기간에 수출 원천(`exp_ship_*`)이 없다 — 표 없음.")
    say("\n> 명세 §2.1 — 수출은 **기업×목적지 보조자료로만** 쓴다. 수출 B/L 에는 상대방 "
        "식별자가 없어 pair·within-firm 판정이 구조적으로 불가능하다.")

    # ------------------------------------------------------------------ t6~t10
    print("[4] 수출입 패턴 t6~t10")
    say("\n---\n\n## 수출입 패턴 t6~t10\n")
    say("> **'수출' 의 정의** — 이 절의 수출은 전부 `panel_firm_export_quarter`(미국 **수출** B/L 의 "
        "shipper = `shp_ciqid_original`, 원천 `exp_ship_*`)다. `panel_firm_quarter` 의 `exp_*` 는 "
        "**'이 법인이 shipper 인 미국 수입 B/L'** 이라 여기서는 쓰지 않는다. "
        "수입 firm = `panel_firm_quarter.imp_n_ship > 0`(수입 B/L 의 consignee, CIQ 매칭).")
    has_fx = fx is not None and len(fx) > 0
    if not has_fx:
        say("\n> ⚠️ `panel_firm_export_quarter` 가 없거나 비어 있다(이 기간에 수출 원천 없음) — "
            "t6 · t7 · t9 · t10 은 건너뛴다.")
    if has_fx:
        say("\n### t6. 수입·수출 동시 firm — 법인(ciqid) 기준\n")
        t6 = t6_both_direction(fq, fx, quarters, TABLES)
        say(md(t6["t6_both_direction_firms"]))
        say("\n### t6b. 수입·수출 동시 — 모회사(up) 기준\n")
        say(md(t6["t6b_both_direction_parents"]))
        say("\n> 비중 열은 전부 %. `n_both` = 그 기간에 수입도 하고 수출도 한 단위 수. "
            "`imp_value_share_of_both` = 양방향 단위의 수입액 / 그 기간 전체 수입액(수출도 같은 식). "
            "모회사 기준은 `panel_firm_quarter.up` 과 수출 패널 `up`(분기 금액 최대 UP)을 쓴다 — "
            "계열사끼리 방향을 나눠 맡는 그룹이 있으면 법인 기준보다 커진다.")

        say("\n### t7. 방향 전환 — 첫 수입 분기 vs 첫 수출 분기 (firm 단위, 표본 전체)\n")
        t7, n7 = t7_direction_transition(fq, fx, quarters, TABLES)
        say(md(t7))
        say(f"\n- 판정가능(좌측절단 `both_from_first_quarter` 제외) firm {n7['n_assessable']:,} 중 "
            f"한쪽→둘 다 전환(`imp_then_exp`+`exp_then_imp`) **{n7['n_transition']:,} "
            f"({n7['transition_share_of_assessable']:.2f}%)**")
        say(f"- 양방향을 다 한 firm 중 판정가능 {n7['n_both_assessable']:,} 에서 전환 비율 "
            f"**{n7['transition_share_of_both_assessable']:.2f}%** (나머지는 같은 분기 동시 시작)")
        say("\n> ⚠️ `both_from_first_quarter` 는 표본 첫 분기에 이미 둘 다 하고 있던 firm — 어느 쪽이 "
            "먼저였는지 **표본 안에서는 알 수 없다**(좌측절단). 한 방향만 하는 firm 도 표본 밖에서 "
            "다른 방향을 했을 수 있으니 이 표는 표본 창 안의 분류다.")

    say("\n### t8. 수입 source 수 분포 — 원산국 수 · 파트너 수\n")
    if fo is not None:
        t8 = t8_import_sources(fq, fo, TABLES)
        for basis, g in t8.groupby("basis", sort=False):
            say(f"\n**{basis}**\n")
            say(md(g.drop(columns="basis")))
        say("\n> 원산국 수(`imp_n_origin_countries`)는 **수입자 CIQ 매칭 선적 전체**에서 센다(상대 "
            "매칭 불필요). 파트너 수(`imp_n_partners`)는 **양측 매칭** 선적에서 센다 — 0 은 그 분기 "
            "수출자가 한 곳도 CIQ 매칭되지 않은 firm-분기다. 두 표의 분모가 다르다.")
    else:
        say("> ⚠️ `panel_firm_origin_quarter` 없음 — 건너뜀.")

    if has_fx:
        say("\n### t9. 수출 목적지 — firm-분기 목적지 수 · 상위 목적지 · 결측\n")
        t9a, t9b, t9s = t9_export_destinations(fx, ex, TABLES)
        say("\n**목적지 국가 수 · 수출 firm-분기** (`n_dest_countries`, coalesce 기준)\n")
        say(md(t9a.drop(columns="basis")))
        say("\n**상위 15 목적지** (수출자 매칭 선적 기준, coalesce)\n")
        say(md(t9b))
        say("\n**목적지 출처** (`dest_source`: declared = 신고 목적지 · port = 양륙항 국가로 대체 · "
            "(결측) = 둘 다 없음)\n")
        say(md(t9s))
        say("\n> `exporter_share(%)` 의 분모는 매칭 수출기업 수 — 한 기업이 여러 목적지에 수출하므로 "
            "열 합이 100 을 넘는다.")

        say("\n### t10. 신규 원산국 진입(t) 전후 수출 변화 — 처리 vs 대조\n")
        t10, t10d, n10 = t10_new_origin_vs_export(fq, fx, fo, quarters, TABLES) \
            if fo is not None else (None, None, {"n_event_quarters": 0, "n_events": 0, "n_dropped": 0})
        if t10 is None or n10["n_event_quarters"] == 0:
            say("> **해당 없음** — t−1·t+1 이 표본 안에 있는 이벤트 분기가 없다"
                f"(달력 분기 {len(quarters)}개, 이벤트 판정에는 3개 이상 필요).")
        else:
            say(f"이벤트 분기 {n10['n_event_quarters']}개({', '.join(quarters[1:-1])}) · "
                f"이벤트 (firm, t) {n10['n_events']:,}건 · 그중 t−1 또는 t+1 에 수입이 없어 제외 "
                f"{n10['n_dropped']:,}건\n")
            say("**(a) 수출 상태 2×2 — t−1 수출 여부 × t+1 수출 여부**\n")
            say(md(t10))
            say("\n**(b) t−1·t+1 모두 수출한 firm 의 Δlog(수출액) · Δ목적지 수**\n")
            say(md(t10d, "{:,.3f}"))
            say("\n> `start_among_nonexp_t-1(%)` = t−1 에 수출이 없던 firm 중 t+1 에 수출을 시작한 비율 — "
                "처리군이 대조군보다 높으면 '새 원산국 진입 뒤 수출 시작' 과 부합한다(인과가 아니라 "
                "기술통계다).")
        say("\n> ⚠️ **2024 는 이벤트 분기가 2개(Q2·Q3)뿐인 기술통계다.** 표본 첫 분기 진입은 판정 "
            "불가(좌측절단)이고 마지막 분기는 t+1 이 없다. 전 기간(2007-07~)에서 돌려야 의미가 있다.")

    # ------------------------------------------------------------------ d8·d9 (명세 §6.5)
    print("[5] d8·d9 삽입")
    for line in include_d8d9(V3):
        say(line)

    (V3 / "95_report.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {TABLES}")


if __name__ == "__main__":
    main()
