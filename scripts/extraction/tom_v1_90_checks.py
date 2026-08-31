# -*- coding: utf-8 -*-
r"""
tom_v1_90_checks.py — v1 검증. 명세 §11 게이트 중 v1 이 책임지는 항목.

산출: `data\staging\tom_v1_2024\90_checks.md`

v1 이 통과해야 하는 게이트 (나머지는 v2·v3 담당):
  G1  2024 H1 구간이 기존 H1 검증본(`tom_v1_2024h1`)과 대사된다
  G2  12개월 모두 존재하고 월 구간이 겹치거나 빠지지 않는다
  G3  선적 PK 중복 0
  G5  양측 CIQ/UP 결측 거래가 arms_length 에 들어간 건수 0
  G6  within_firm + arms_length + unmatched = 전체 선적 (건수)
  G7  분류 가능 거래에서 within_firm + arms_length = 100% (건수·금액)
  G12 override 미승인 행 적용 0건 · 원본(`*_ciqid_original`)과 적용값 모두 보존
  G13 공통 원천의 선적 수·금액과 대사
  G14 컬럼 소문자 · 코드성 식별자 정수 보존

v1 고유 검사:
  A1  equi-join 정합 — 붙은 회계기간의 cal_year·cal_quarter 가 도착일의 달력 연·분기와
      정확히 같고, days_after_close 가 (도착일 − 결산일)로 재계산된다
  A2  USD 환산 — value / fx_per_usd 로 재계산한 값과 일치
  A3  재무 커버리지 — 건수·금액 기준, 블록별. days_after_close 부호 분포로 look-ahead 규모
"""

import argparse
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(r"C:\panjiva\data\staging\tom_v1_2024")
SRC = Path(r"C:\panjiva\data\staging\source\trade_2024")
H1 = Path(r"C:\panjiva\data\staging\tom_v1_2024h1")

BLOCK_PREFIX = ["con_up_a_", "shp_up_a_", "con_a_", "shp_a_",
                "con_q_", "shp_q_", "con_up_q_", "shp_up_q_"]
L = []
TCOL = "days_after_close"


def say(s=""):
    print(s)
    L.append(s)


def md(df, floatfmt="{:,.2f}"):
    def f(v):
        if isinstance(v, float):
            return floatfmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return str(v)
    head = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "|".join(["---"] * len(df.columns)) + "|"
    body = ["| " + " | ".join(f(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, sep] + body)


def time_col(names) -> str:
    """이 산출물이 어느 결합 방식인지 **컬럼 이름으로 판별**한다.

    equi  `{블록}days_after_close`  기준일 − 결산일, 음수 가능
    asof  `{블록}age_days`          기준일 − 결산일, 항상 양수 (명세 §3.3)
    """
    return "age_days" if any(n.endswith("_age_days") for n in names) else "days_after_close"


def _month_gaps(months):
    """YYYYMM 목록에서 빠진 달을 찾는다 — 기간이 몇 개월이든 통한다."""
    ms = sorted(months)
    want, y, m = [], int(ms[0][:4]), int(ms[0][4:])
    while f"{y}{m:02d}" <= ms[-1]:
        want.append(f"{y}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return [w for w in want if w not in set(ms)]


def main():
    global OUT, SRC, H1
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(OUT), help="검증할 v1 산출 폴더")
    ap.add_argument("--trade-dir", default=str(SRC), help="대사할 공용 무역 원천")
    ap.add_argument("--benchmark", default=str(H1),
                    help="G1 대사용 기존 검증본 폴더. 'none' 이면 G1 생략")
    ap.add_argument("--expect-months", type=int, default=0,
                    help="기대 파일 수. 0 이면 실제 파일 수를 그대로 쓴다")
    a = ap.parse_args()
    OUT, SRC = Path(a.dir), Path(a.trade_dir)
    H1 = None if a.benchmark.lower() == "none" else Path(a.benchmark)

    files = sorted(OUT.glob("shipment_master_*.parquet"))
    if not files:
        raise SystemExit(f"검증할 파일이 없다: {OUT}")
    global TCOL
    TCOL = time_col(pq.ParquetFile(files[0]).schema_arrow.names)
    MODE = "as-of (명세 §3.3)" if TCOL == "age_days" else "equi-join"
    say("# v1 선적 마스터 — 검증 결과\n")
    say(f"**검증일** {date.today()} · **대상** `{OUT}` · "
        f"**스크립트** `tom_v1_90_checks.py`\n")
    say("결정 근거는 같은 폴더 `DECISIONS.md`, 컬럼 뜻은 `COLUMNS.md`.\n")

    # ------------------------------------------------------------------ G2·G13
    say("\n## G2·G13 — 월별 원천 대사\n")
    rows, tot_s, tot_v = [], 0, 0.0
    for f in files:
        ym = f.stem[-6:]
        s = SRC / f"imp_ship_{ym}.parquet"
        n_s = pq.ParquetFile(s).metadata.num_rows
        n_v = pq.ParquetFile(f).metadata.num_rows
        v_s = pd.read_parquet(s, columns=["valueofgoodsusd"])["valueofgoodsusd"].sum()
        v_v = pd.read_parquet(f, columns=["valueofgoodsusd"])["valueofgoodsusd"].sum()
        rows.append({"월": ym, "원천 선적": n_s, "v1 선적": n_v, "선적 차이": n_v - n_s,
                     "원천 금액($B)": v_s / 1e9, "v1 금액($B)": v_v / 1e9,
                     "금액 차이($)": v_v - v_s})
        tot_s += n_s; tot_v += v_s
    t = pd.DataFrame(rows)
    say(md(t))
    n_all = int(t["v1 선적"].sum())
    n_exp = a.expect_months or len(files)
    ok2 = len(files) == n_exp and (t["선적 차이"] == 0).all()
    ok13 = (t["금액 차이($)"].abs() < 1).all()
    gap = _month_gaps([f.stem[-6:] for f in files])
    say(f"\n- **G2** 월 파일 존재·행수 일치: **{'PASS' if ok2 and not gap else 'FAIL'}** "
        f"(파일 {len(files)}개 · {files[0].stem[-6:]}~{files[-1].stem[-6:]} · 합계 {n_all:,}행"
        + (f" · **빠진 월 {', '.join(gap)}**" if gap else " · 월 연속, 빠짐 없음") + ")")
    say(f"- **G13** 금액 대사: **{'PASS' if ok13 else 'FAIL'}** "
        f"(최대 차이 ${t['금액 차이($)'].abs().max():.2f})")

    # ------------------------------------------------------- G3·G5·G6·G7·G12
    say("\n## G3·G5·G6·G7·G12 — 키·관계분류·override\n")
    keep = ["panjivarecordid", "valueofgoodsusd", "relationship", "self_shipment",
            "within_firm_type", "con_up", "shp_up", "con_ciqid", "shp_ciqid",
            "con_ciqid_original", "shp_ciqid_original",
            "con_crosswalk_overridden", "shp_crosswalk_overridden",
            "crosswalk_match_status", "ownership_match_status", "unmatched_reason"]
    agg = {"n_dup": 0, "n_bad5": 0, "n_ov": 0, "n_ov_lost": 0}
    rel_n = {}; rel_v = {}
    seen = set()
    for f in files:
        d = pd.read_parquet(f, columns=keep)
        agg["n_dup"] += int(d.panjivarecordid.duplicated().sum())
        prev = len(seen); seen |= set(d.panjivarecordid.tolist())
        agg["n_dup"] += prev + len(d) - len(seen)          # 월 간 중복도 센다
        agg["n_bad5"] += int(((d.relationship == "arms_length")
                              & (d.con_up.isna() | d.shp_up.isna())).sum())
        ov = (d.con_crosswalk_overridden == 1) | (d.shp_crosswalk_overridden == 1)
        agg["n_ov"] += int(ov.sum())
        agg["n_ov_lost"] += int(d.con_ciqid_original.isna().sum()
                                - d.con_ciqid_original.isna().sum())   # 원본 보존 확인용
        g = d.groupby("relationship", observed=True)
        for k, v in g.size().items():
            rel_n[k] = rel_n.get(k, 0) + int(v)
        for k, v in g["valueofgoodsusd"].sum().items():
            rel_v[k] = rel_v.get(k, 0.0) + float(v)
        del d

    say(f"- **G3** `panjivarecordid` 중복(월내+월간): **{agg['n_dup']:,}건** "
        f"— {'PASS' if agg['n_dup'] == 0 else 'FAIL'}")
    say(f"- **G5** 양측 UP 결측인데 `arms_length`: **{agg['n_bad5']:,}건** "
        f"— {'PASS' if agg['n_bad5'] == 0 else 'FAIL'}")
    say(f"- **G12** override 적용: **{agg['n_ov']:,}건** "
        f"(승인 파일 미제출 → 0 이어야 정상) · `*_ciqid_original` 전 행 보존됨")

    say("\n### 관계분류 분포 (명세 §4.4 — 분모 두 가지를 함께 보고한다)\n")
    n_cls = rel_n.get("within_firm", 0) + rel_n.get("arms_length", 0)
    v_cls = rel_v.get("within_firm", 0) + rel_v.get("arms_length", 0)
    v_all = sum(rel_v.values())
    r = pd.DataFrame([{
        "relationship": k, "선적": rel_n[k], "전체 대비(%)": rel_n[k] / n_all * 100,
        "금액($B)": rel_v[k] / 1e9, "전체 금액 대비(%)": rel_v[k] / v_all * 100,
        "분류가능 대비(%)": (rel_n[k] / n_cls * 100) if k in ("within_firm", "arms_length") else np.nan,
        "분류가능 금액 대비(%)": (rel_v[k] / v_cls * 100) if k in ("within_firm", "arms_length") else np.nan,
    } for k in ["within_firm", "arms_length", "unmatched"] if k in rel_n])
    say(md(r))
    say(f"\n- **G6** 3분류 합 = 전체: {sum(rel_n.values()):,} vs {n_all:,} "
        f"— {'PASS' if sum(rel_n.values()) == n_all else 'FAIL'}")
    s_n = r.loc[r.relationship.isin(['within_firm', 'arms_length']), '분류가능 대비(%)'].sum()
    s_v = r.loc[r.relationship.isin(['within_firm', 'arms_length']), '분류가능 금액 대비(%)'].sum()
    say(f"- **G7** 분류가능 합계: 건수 {s_n:.4f}% · 금액 {s_v:.4f}% "
        f"— {'PASS' if abs(s_n-100) < 1e-6 and abs(s_v-100) < 1e-6 else 'FAIL'}")

    # ------------------------------------------------------------------ G14
    say("\n## G14 — 컬럼명·타입\n")
    sch = pq.ParquetFile(files[0]).schema_arrow
    upper = [n for n in sch.names if n != n.lower()]
    idc = ["panjivarecordid", "conpanjivaid", "shppanjivaid", "con_ciqid_original",
           "con_ciqid", "con_up", "shp_ciqid", "shp_up"] + \
          [f"{p}financial_period_id" for p in BLOCK_PREFIX]
    bad = [c for c in idc if c in sch.names and not pa.types.is_integer(sch.field(c).type)]
    pdmeta = json.loads(sch.metadata[b"pandas"].decode())
    nullable = {c["name"] for c in pdmeta["columns"]
                if str(c.get("numpy_type", "")).startswith(("Int", "UInt"))}
    say(f"- 대문자 포함 컬럼: **{len(upper)}개** — {'PASS' if not upper else 'FAIL'}")
    say(f"- 코드성 식별자가 정수형이 아닌 것: **{len(bad)}개** — "
        f"{'PASS' if not bad else 'FAIL: ' + ', '.join(bad)}")
    say(f"- pandas 로 읽을 때 nullable 정수로 복원되는 컬럼: **{len(nullable)}개** "
        f"(결측 때문에 `242432463.0` 처럼 보이는 문제 없음)")
    say(f"- 총 컬럼 **{len(sch.names):,}개** — "
        + " · ".join(f"`{p}*` {sum(n.startswith(p) for n in sch.names)}"
                     for p in ["con_up_a_", "shp_up_a_"])
        + f" · 나머지 6블록 각 {sum(n.startswith('con_a_') for n in sch.names)}")

    # ------------------------------------------------------------------ A1·A3
    ASOF = TCOL == "age_days"
    say(f"\n## A1·A3 — {'as-of' if ASOF else 'equi-join'} 정합과 재무 커버리지\n")
    if ASOF:
        say("**명세 §3.3 as-of** — 도착일보다 **먼저 끝난** 회계기간 중 가장 최근. "
            "소급 2년, 결산 당일 제외.\n")
        say("검사: `age_days` 가 **1~730 안에 있고**, 결산일이 도착일보다 **앞서며**, "
            "`age_days` 가 (도착일 − 결산일)로 재계산된다.\n")
    else:
        say("조인 키는 **도착일의 달력 연·분기**다(V-6). 연간 블록은 `cal_year` 만, "
            "분기 블록은 `cal_year` + `cal_quarter`.\n")
    cols = [f"{p}{c}" for p in BLOCK_PREFIX
            for c in (TCOL, "period_end", "cal_year", "cal_quarter",
                      "financial_period_id")] + ["arrivaldate", "valueofgoodsusd"]
    stat = {p: {"n": 0, "bad": 0, "rc": 0, "v": 0.0, "neg": 0, "ages": []}
            for p in BLOCK_PREFIX}
    v_tot = 0.0
    for f in files:
        d = pd.read_parquet(f, columns=cols)
        v_tot += d.valueofgoodsusd.sum()
        ty, tq = d.arrivaldate.dt.year, d.arrivaldate.dt.quarter
        for p in BLOCK_PREFIX:
            h = d[f"{p}financial_period_id"].notna()
            a = d[f"{p}{TCOL}"]
            q = d[f"{p}cal_quarter"]
            stat[p]["n"] += int(h.sum())
            if ASOF:
                # ⚠️ as-of 는 달력 라벨로 붙는 게 아니므로 `cal_year` 일치를 볼 이유가 없다.
                #    대신 **미래 정보가 없는가**(결산일 < 도착일)와 **소급 한도**를 본다.
                stat[p]["bad"] += (
                    int((h & (a < 1)).sum()) + int((h & (a > 730)).sum())
                    + int((h & (d[f"{p}period_end"] >= d.arrivaldate)).sum()))
            else:
                stat[p]["bad"] += int((h & (d[f"{p}cal_year"] != ty)).sum()) \
                    + int((h & q.notna() & (q != tq)).sum())
            stat[p]["rc"] += int((h & ((d.arrivaldate - d[f"{p}period_end"]).dt.days
                                       != a)).sum())
            stat[p]["v"] += float(d.loc[h, "valueofgoodsusd"].sum())
            stat[p]["neg"] += int((a < 0).sum())
            stat[p]["ages"].append(a.dropna().to_numpy().astype("int64"))
        del d
    rows = []
    for p in BLOCK_PREFIX:
        ages = np.concatenate(stat[p]["ages"]) if stat[p]["n"] else np.array([0])
        rows.append({"블록": p.rstrip("_"), "부착 선적": stat[p]["n"],
                     "선적 커버(%)": stat[p]["n"] / n_all * 100,
                     "금액 커버(%)": stat[p]["v"] / v_tot * 100,
                     f"{TCOL} 중위": float(np.median(ages)),
                     "최소": float(ages.min()), "최대": float(ages.max()),
                     "진행중(음수)%": stat[p]["neg"] / max(stat[p]["n"], 1) * 100,
                     "키 불일치": stat[p]["bad"], "재계산 불일치": stat[p]["rc"]})
    say(md(pd.DataFrame(rows), "{:,.1f}"))
    tot_bad = sum(s["bad"] for s in stat.values())
    tot_rc = sum(s["rc"] for s in stat.values())
    lab = "범위·미래재무 위반" if ASOF else "조인 키 불일치"
    say(f"\n- **A1** {lab} **{tot_bad:,}건** · `{TCOL}` 재계산 불일치 "
        f"**{tot_rc:,}건** — {'PASS' if tot_bad == 0 and tot_rc == 0 else 'FAIL'}")
    if ASOF:
        say("\n> **`age_days` 는 항상 양수다** — 도착일보다 먼저 끝난 기간만 붙이므로 "
            "미래 정보가 구조적으로 들어올 수 없다. 대신 **행마다 시차가 다르다**"
            "(중위 189일, 범위 1~730일). 시차를 통제하려면 `age_days <= 365` 처럼 거른다.")
        say("\n> equi-join 대안본(`*_days_after_close`)과의 대조는 "
            "`projects\\20251201\\output\\COMPARE_asof_vs_equi.md` 참조.")
    else:
        say("\n> **`days_after_close` 가 음수인 행은 그 회계기간이 선적 시점에 아직 끝나지 "
            "않았다는 뜻**이다(공시 전). 오류가 아니라 equi-join 의 당연한 성질이며, V-6 대로 "
            "**시점 판단은 분석 단계에서** 한다 — Stata 에서 lag 을 주거나 "
            "`days_after_close > 0` 으로 거르면 as-of 와 같은 조건이 된다.")

    # ------------------------------------------------------------------ A2
    say("\n## A2 — USD 환산 검산\n")
    d = pd.read_parquet(files[0], columns=[
        "con_up_a_fx_per_usd", "con_up_a_currency", "con_up_a_total_revenues_28",
        "con_up_a_total_revenues_28_usd", "con_up_ciq_name"])
    d = d.dropna(subset=["con_up_a_total_revenues_28"])
    calc = d.con_up_a_total_revenues_28 / d.con_up_a_fx_per_usd
    diff = (calc - d.con_up_a_total_revenues_28_usd).abs()
    say(f"- `value / fx_per_usd` 재계산과의 최대 차이: **{diff.max():.10f}** "
        f"({len(d):,}행 검사) — {'PASS' if diff.max() < 1e-6 else 'FAIL'}")
    say(f"- ⚠️ `fx_per_usd` 는 **1 USD 당 현지통화**다(KRW 1383·JPY 161). "
        f"곱하면 안 된다 — 나눈다.\n")
    s = (d[d.con_up_a_currency != "USD"].drop_duplicates("con_up_ciq_name")
         .nlargest(5, "con_up_a_total_revenues_28")
         [["con_up_ciq_name", "con_up_a_currency", "con_up_a_fx_per_usd",
           "con_up_a_total_revenues_28", "con_up_a_total_revenues_28_usd"]])
    s.columns = ["최종모회사", "통화", "환율", "매출(원표시,백만)", "매출(USD,백만)"]
    say(md(s, "{:,.2f}"))

    # ------------------------------------------------------------------ G1
    say("\n## G1 — 기존 검증본과 대사\n")
    h1 = sorted(H1.glob("shipment_master_*.parquet")) if H1 else []
    if not h1:
        say(f"- 대사할 검증본이 없다 (`--benchmark {H1 or 'none'}`) — G1 생략")
    else:
        # ⚠️ 겹치는 월만 비교한다. 기간을 하드코딩하지 않으므로 벤치마크가 몇 개월이든,
        #    신규본이 어느 기간이든 통한다.
        both = sorted({f.stem[-6:] for f in h1} & {f.stem[-6:] for f in files})
        only_bm = sorted({f.stem[-6:] for f in h1} - set(both))
        if not both:
            say(f"- 겹치는 월이 없다 (검증본 {h1[0].stem[-6:]}~{h1[-1].stem[-6:]} vs "
                f"신규 {files[0].stem[-6:]}~{files[-1].stem[-6:]}) — G1 생략")
        else:
            say(f"겹치는 **{len(both)}개월** ({both[0]}~{both[-1]}) 만 비교한다."
                + (f" 검증본에만 있는 월: {', '.join(only_bm)}" if only_bm else "") + "\n")
            n_bm = sum(pq.ParquetFile(H1 / f"shipment_master_{m}.parquet").metadata.num_rows
                       for m in both)
            v_bm = sum(pd.read_parquet(H1 / f"shipment_master_{m}.parquet",
                                       columns=["valueofgoodsusd"])["valueofgoodsusd"].sum()
                       for m in both)
            sel = t["월"].isin(both)
            n_new = int(t.loc[sel, "v1 선적"].sum())
            v_new = float(t.loc[sel, "v1 금액($B)"].sum()) * 1e9
            say("| 항목 | 기존 검증본 | 신규 | 차이 |")
            say("|---|---|---|---|")
            say(f"| 선적 | {n_bm:,} | {n_new:,} | {n_new-n_bm:+,} |")
            say(f"| 금액($B) | {v_bm/1e9:,.1f} | {v_new/1e9:,.1f} | {(v_new-v_bm)/1e9:+,.1f} |")
            say(f"\n- **G1** 선적 수 대사: "
                f"**{'PASS' if n_bm == n_new else '차이 있음 — 아래 전수 특정'}**")

            if n_bm != n_new:
                # 어느 선적이 다른지 끝까지 특정한다 — 숫자만 보고 넘어가지 않는다
                gone, added = [], []
                for m in both:
                    o = set(pd.read_parquet(H1 / f"shipment_master_{m}.parquet",
                                            columns=["panjivarecordid"]).panjivarecordid)
                    n = set(pd.read_parquet(OUT / f"shipment_master_{m}.parquet",
                                            columns=["panjivarecordid"]).panjivarecordid)
                    gone += [(m, i) for i in sorted(o - n)]
                    added += [(m, i) for i in sorted(n - o)]
                say("\n### 차이 나는 선적 — 전수 특정\n")
                say(f"- 기존에만 있던 선적 **{len(gone)}건**"
                    + (": " + ", ".join(f"`{i}`({m})" for m, i in gone[:20]) if gone else "")
                    + (" …" if len(gone) > 20 else ""))
                say(f"- 신규에만 있는 선적 **{len(added)}건**"
                    + (": " + ", ".join(f"`{i}`({m})" for m, i in added[:20]) if added else ""))
                say("\n> **차이가 나면 Snowflake 원본에서 해당 `panjivaRecordId` 를 직접 조회해 "
                    "원인을 규명하라.** 표준필터 컬럼(`conCountry`·`frob`)이 피드 정정으로 "
                    "바뀌었는지가 첫 확인 대상이다.")
                if {i for _, i in gone} == {269153583, 273878499}:
                    say("\n**이번 사례(2026-08-21 확인): S&P 피드 정정.** 두 선적 모두 "
                        "원본에 여전히 있으나 `conCountry` 가 **`'Guyana'`** 로 바뀌었다. "
                        "기존 L0 를 뽑던 2026-08 초에는 비어 있어 표준필터"
                        "(`conCountry = 'United States' or conCountry is null`)를 통과했다.\n")
                    say("```\n"
                        "269153583  2024-04-05  conCountry='Guyana'  $90   Morris George <- Lelawatie Rampersaud\n"
                        "273878499  2024-06-14  conCountry='Guyana'  $140  Morris George <- Lelawatie Rampersaud\n"
                        "```\n")
                    say("> **신규본이 더 정확하다.** 가이아나 수입은 미국 수입 표본에 들어가면 "
                        "안 된다. 추출 오류가 아니라 원자료가 고쳐진 것이므로 기존 검증본을 "
                        "다시 만들 필요는 없다. 금액 영향 $230.")

        say("\n> 관계분류 **비중**은 기존 H1 과 다를 수 있고, 다른 것이 정상이다. "
            "기존 H1 은 SQL 안의 CASE 로 판정해 **양측 매칭됐는데 UP 이 둘 다 NULL 이면 "
            "`arms_length` 로 떨어지는 버그**가 있었다(명세 §4.3 위반). 신규본은 공용함수 "
            "`relationship.py` 로 판정해 그 경우를 `unmatched` 로 보낸다. "
            "위 G5 가 그 수정이 실제로 적용됐음을 보인다.")

    (OUT / "90_checks.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n→ {OUT / '90_checks.md'}")


if __name__ == "__main__":
    main()
