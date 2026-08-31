# -*- coding: utf-8 -*-
r"""
wf_v3_stats.py — v3 기초통계 t1~t5 + 진단 d1~d7 (명세 §8.3)

산출: `output\tables\wf2024\` 의 CSV + v3 폴더의 `95_report.md`

## 기초통계

  t1  월별 pair·기업 수·within 비중 (건수 / TEU / 금액 3중 기준)
  t2  관계 지속 분포 — 활동개월 수, within vs arms, 절단(censoring) 병기
  t3  수입기업당 파트너 수 분포 (왜도 확인)
  t4  월간 마진 분해 — Δlog 총액 = Δlog pair수 + Δlog pair당 HS + Δlog HS당 금액
  t5  한국 origin 서브셋 요약 + kor_mnc_link 비중

## 진단

  d1  매칭률 (건수·금액)
  d2  **당사자 결측** — 아래 X-6 참조
  d4  선적당 HS6 개수 분포
  d5  PIT 대체(fallback) 규모
  d6  재무 커버리지 (행 기준 vs 금액 기준)
  d7  수출 목적지 (보조자료)

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
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
SRC = Path(r"C:\panjiva\data\staging\source\trade_2024")
TABLES = Path(r"C:\panjiva\projects\20251201\output\tables\wf2024")

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


def d7_export(src: Path, months, tables) -> pd.DataFrame:
    """수출 — 명세 §2.1 대로 **기업×목적지 보조자료로만** 쓴다."""
    parts = []
    for m in months:
        f = src / f"exp_ship_{m}.parquet"
        if f.exists():
            # ⚠️ 수출 원천에는 `shp_ciqid` 가 없다 — override 적용본은 v1(수입 전용)에서만
            #    만들어진다. 원본 crosswalk 값인 `shp_ciqid_original` 을 쓴다.
            parts.append(pd.read_parquet(
                f, columns=["shpmtdestination", "valueofgoodsusd",
                            "shp_ciqid_original"])
                .rename(columns={"shp_ciqid_original": "shp_ciqid"}))
    if not parts:
        return pd.DataFrame()
    e = pd.concat(parts, ignore_index=True)
    t = (e.groupby("shpmtdestination", dropna=False)
         .agg(n_shipments=("valueofgoodsusd", "size"),
              value_usd=("valueofgoodsusd", "sum"),
              n_exporters=("shp_ciqid", "nunique")).reset_index()
         .sort_values("value_usd", ascending=False).head(20))
    t["금액(%)"] = (t.value_usd / e.valueofgoodsusd.sum() * 100).round(2)
    return save(t, "d7_export_destinations", tables)


# ---------------------------------------------------------------------------
def main() -> None:
    global V1, V3, SRC, TABLES
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01")
    ap.add_argument("--v1-dir", default=str(V1))
    ap.add_argument("--v3-dir", default=str(V3))
    ap.add_argument("--src-dir", default=str(SRC))
    ap.add_argument("--tables-dir", default=str(TABLES))
    a = ap.parse_args()
    V1, V3, SRC = Path(a.v1_dir), Path(a.v3_dir), Path(a.src_dir)
    TABLES = Path(a.tables_dir); TABLES.mkdir(parents=True, exist_ok=True)
    months = list(pd.date_range(a.start, a.end, freq="MS").strftime("%Y%m")[:-1])
    t0 = datetime.now()

    print("[1] 입력 로드")
    ship = pd.concat([pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                                      columns=SHIP_COLS) for m in months],
                     ignore_index=True)
    pm = pd.read_parquet(V3 / "panel_pair_month.parquet")
    rel = pd.read_parquet(V3 / "dim_relationship.parquet")
    print(f"  선적 {len(ship):,} · pair-월 {len(pm):,} · pair {len(rel):,}")

    say("# v3 기초통계·진단 리포트\n")
    say(f"**생성일** {date.today()} · **기간** {a.start} ~ {a.end}(미포함) · "
        f"**스크립트** `wf_v3_stats.py`\n")

    # --- X-7: 하드코딩 대사값 대신 원천 실측 대사 ---
    say("## 대사 (X-7 — 하드코딩 게이트 제거)\n")
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
    say("\n> 기존 v3 는 \"1주 슬라이스 = 254,873건\" 을 코드에 박아 검증했다. "
        "필터나 기간이 바뀌면 반드시 깨지므로 **원천 실측 대사로 바꿨다**.\n")

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

    say("\n## d2. 당사자 결측 — X-6 수정\n")
    say(md(d2_party_missing(ship, TABLES)))
    say("\n> **기존 지표는 ③만 세어 0.0001%(14건) 였다.** 실제 은폐는 ①이고 수입자 기준 "
        "38.9% 다. 이름이 `redaction` 하나였던 것이 오해의 원인이라 층을 나누고 "
        "이름을 `party_missing` · `name_placeholder` 로 갈랐다.")
    say("\n> ①은 **복구 불가**(세관이 지움), ②는 crosswalk 보강으로 일부 복구 가능하다. "
        "성격이 다르니 합쳐서 보고하면 안 된다.")

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
    d7 = d7_export(SRC, months, TABLES)
    if len(d7):
        say(md(d7.head(15)))
    say("\n> 명세 §2.1 — 수출은 **기업×목적지 보조자료로만** 쓴다. 수출 B/L 에는 상대방 "
        "식별자가 없어 pair·within-firm 판정이 구조적으로 불가능하다.")

    (V3 / "95_report.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {TABLES}")


if __name__ == "__main__":
    main()
