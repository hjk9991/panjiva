# -*- coding: utf-8 -*-
r"""
v4_00_period_probe.py — v4 를 2007~ 전 기간으로 넓히기 전 **연도별 사용가능성 프로브**.

묻는 것: "몇 년부터가 쓸 만한가?" v4 의 2024 실측치가 과거에도 성립하는지 확인한다.
전 기간(2.33억 행, 3~4시간) 추출 **전에** 돌린다 — 팀 규약 `CLAUDE.md` §9.

입력 : source\trade_probe\imp_ship_YYYYMM.parquet  (프로브 월)
       source\trade_2024\imp_ship_202401.parquet   (기준점)
출력 : data\staging\v4_pairhs_full\00_period_probe.md  + .csv

재는 것
  A 쌍 성립률   — 양쪽 panjivaid 가 다 있는가 (v4 가 남기는 행)
  B redaction   — 당사자 블록 통째 결측 (CBP 기밀요청). 복구 불가
  C crosswalk   — panjivaid 는 있는데 CIQ 에 없음. 연도가 오래될수록 나빠지는지가 핵심
  D PIT fallback— 시점 소유구조가 없어 스냅샷으로 대체한 비중
  E HS          — 자릿수·다중품목·고유코드 수. **HS 개정(2007/2012/2017/2022) 흔적**
  F v4 행수     — 그 달을 v4 grain 으로 접으면 몇 행인가
"""

from pathlib import Path

import numpy as np
import pandas as pd

PROBE = Path(r"C:\panjiva\data\staging\source\trade_probe")
BASE = Path(r"C:\panjiva\data\staging\source\trade_2024")
OUT = Path(r"C:\panjiva\data\staging\v4_pairhs_full")

COLS = ["panjivarecordid", "conpanjivaid", "con_ciqid_original", "con_up",
        "con_ownership_is_fallback", "shppanjivaid", "shp_ciqid_original", "shp_up",
        "shp_ownership_is_fallback", "hs6", "hs_ndigits", "n_hs6", "valueofgoodsusd"]


def month_path(ym):
    p = PROBE / f"imp_ship_{ym}.parquet"
    return p if p.exists() else BASE / f"imp_ship_{ym}.parquet"


def measure(ym):
    d = pd.read_parquet(month_path(ym), columns=COLS)
    d["valueofgoodsusd"] = d["valueofgoodsusd"].fillna(0.0)
    n, v = len(d), d["valueofgoodsusd"].sum()
    r = {"ym": ym, "shipments": n, "value_usd_bn": round(v / 1e9, 1)}

    for s in ["con", "shp"]:
        pid, cid = f"{s}panjivaid", f"{s}_ciqid_original"
        blank = d[pid].isna()
        noxw = d[pid].notna() & d[cid].isna()
        ok = d[cid].notna()
        r[f"{s}_redacted_pct"] = round(blank.mean() * 100, 1)
        r[f"{s}_redacted_val_pct"] = round(d.loc[blank, "valueofgoodsusd"].sum() / v * 100, 1)
        r[f"{s}_no_crosswalk_pct"] = round(noxw.mean() * 100, 1)
        r[f"{s}_matched_pct"] = round(ok.mean() * 100, 1)
        r[f"{s}_matched_val_pct"] = round(d.loc[ok, "valueofgoodsusd"].sum() / v * 100, 1)
        r[f"{s}_pit_fallback_pct"] = round(d[f"{s}_ownership_is_fallback"].mean() * 100, 1)

    pair = d.conpanjivaid.notna() & d.shppanjivaid.notna()
    both = d.con_ciqid_original.notna() & d.shp_ciqid_original.notna()
    r["pair_ok_pct"] = round(pair.mean() * 100, 1)
    r["pair_ok_val_pct"] = round(d.loc[pair, "valueofgoodsusd"].sum() / v * 100, 1)
    r["both_matched_pct"] = round(both.mean() * 100, 1)
    r["both_matched_val_pct"] = round(d.loc[both, "valueofgoodsusd"].sum() / v * 100, 1)

    nh = d["n_hs6"].fillna(0)
    r["hs_missing_pct"] = round((nh == 0).mean() * 100, 1)
    r["hs_multi_pct"] = round((nh > 1).mean() * 100, 1)
    r["hs_multi_val_pct"] = round(d.loc[nh > 1, "valueofgoodsusd"].sum() / v * 100, 1)
    r["hs_ndigits6_pct"] = round((d["hs_ndigits"] == 6).mean() * 100, 1)
    r["hs6_distinct"] = int(d.loc[nh == 1, "hs6"].nunique())

    # v4 grain 으로 접으면 몇 행인가 (D1~D3 규칙 그대로)
    k = d[pair].copy()
    k["hs_status"] = np.where(k.n_hs6 == 1, "single", np.where(k.n_hs6 > 1, "multi", "missing"))
    k.loc[k.hs_status != "single", "hs6"] = np.nan
    r["v4_rows"] = int(k.groupby(["shppanjivaid", "conpanjivaid", "hs6", "hs_status"],
                                 dropna=False).ngroups)
    codes = set(d.loc[nh == 1, "hs6"].dropna().unique())
    return r, codes


def main():
    months = ["200707", "201001", "201301", "201601", "201901", "202201", "202401"]
    months = [m for m in months if month_path(m).exists()]
    rows, codesets = [], {}
    for ym in months:
        print(f"  {ym} ...", flush=True)
        r, c = measure(ym)
        rows.append(r)
        codesets[ym] = c
    t = pd.DataFrame(rows)
    t.to_csv(OUT / "00_period_probe.csv", index=False, encoding="utf-8-sig")

    def tbl(cols, hdr):
        out = ["", f"### {hdr}", "", "| 항목 | " + " | ".join(t.ym) + " |",
               "|---|" + "---|" * len(t)]
        for c, lab in cols:
            out.append(f"| {lab} | " + " | ".join(
                f"{t[c].iloc[i]:,}" if isinstance(t[c].iloc[i], (int, np.integer))
                else f"{t[c].iloc[i]}" for i in range(len(t))) + " |")
        return out

    md = ["# v4 전 기간 확장 프로브 — 몇 년부터 쓸 만한가", "",
          "**측정일**: 2026-08-24 · **스크립트**: `scripts\\processing\\v4_00_period_probe.py`",
          "**전 기간 집계**: `scripts\\extraction\\ex_20260824_import_coverage_by_year.py`", "",
          "전 기간(2.24억 행) 추출 **전에** 연도별 사용가능성을 확인한다 — `CLAUDE.md` §9.", "",
          "## 결론 (먼저)", "",
          "1. **실사용 시작은 2007-07 이다.** `DATA_GUIDE` 의 \"2007-01~\" 는 형식적으로만 맞다 —",
          "   2007-01~05 는 월 5천건 수준(정상의 0.6%)이고 2007-06 에 55,032 로 램프업,",
          "   **2007-07 에 886,912 건으로 정상 볼륨**이 된다. 상반기를 넣으면 그 6개월만 사실상 빈다.",
          "2. **과거 데이터가 오히려 매칭이 더 좋다.** 걱정했던 \"과거로 갈수록 나빠진다\" 는",
          "   **성립하지 않는다.** 양측 CIQ 매칭은 2013·2016 이 35% 로 최고이고 **2024 가 23.3% 로 최저**다.",
          "   쌍 성립률도 2007-07 이 78.6% 로 가장 높고 2022 가 55.4% 로 가장 낮다.",
          "   → **시작 연도를 늦출 이유가 없다. 2007-07 부터 전부 쓰는 것이 맞다.**",
          "3. **경계할 것은 과거가 아니라 최근이다.** 당사자 ID 커버리지가 2020년 이후 떨어지고",
          "   **2025 년은 수출자 51.4% 로 급락**한다(백필 진행 중일 가능성). 2025 는 잠정 취급이 필요하다.",
          "4. **HS 개정은 데이터만으로 판별되지 않는다.** 개정 연도(2012·2017·2022)를 걸치는 구간의",
          "   코드집합 겹침(0.847·0.863)이 걸치지 않는 구간(0.856·0.871)과 **차이가 없다** —",
          "   일반적인 품목구성 변화에 묻힌다. 처리 방식은 **PI 판단이 필요하다**",
          "   (질문서: `shared memory\\BECRS_Matching_Project\\92_질문_김영수_HS개정_시계열처리.md`).", ""]
    cov = OUT / "00_import_coverage_by_year.csv"
    if cov.exists():
        c = pd.read_csv(cov)
        md += ["## 연도별 전수 커버리지 (Snowflake 집계, 표본 아님)", "",
               "`filter_keeps` = 표준필터(미국 실착 + 통과화물 제외) 통과율 · "
               "`con_id`/`shp_id` = 당사자 ID 가 있는 비중(= 1 − redaction)", "",
               "| 연도 | 원본 행 | 필터후 | filter_keeps | con_id | shp_id |", "|---|---|---|---|---|---|"]
        for r in c.itertuples():
            md.append(f"| {r.yr} | {r.raw_rows:,} | {r.pass_both:,} | {r.filter_keeps_pct}% | "
                      f"{r.con_id_pct}% | {r.shp_id_pct}% |")
        md += ["", f"합계 원본 **{c.raw_rows.sum():,}** · 필터후 **{c.pass_both.sum():,}**", "",
               "> 표준필터 통과율이 86~93% 로 **전 연도에서 안정적**이다 — 필터가 과거를 죽이는 것이 아니다.", ""]
    md += tbl([("shipments", "선적 수"), ("value_usd_bn", "금액($B)"),
               ("v4_rows", "**v4 행수**")], "규모")
    md += tbl([("pair_ok_pct", "**쌍 성립률(행%)**"), ("pair_ok_val_pct", "쌍 성립률(금액%)"),
               ("both_matched_pct", "**양측 CIQ 매칭(행%)**"),
               ("both_matched_val_pct", "양측 CIQ 매칭(금액%)")], "A. v4 가 남기는 행")
    md += tbl([("con_redacted_pct", "수입자 은폐(행%)"), ("con_redacted_val_pct", "수입자 은폐(금액%)"),
               ("shp_redacted_pct", "수출자 은폐(행%)"),
               ("shp_redacted_val_pct", "수출자 은폐(금액%)")], "B. redaction — 복구 불가")
    md += tbl([("con_no_crosswalk_pct", "수입자 CIQ 미등재(행%)"),
               ("shp_no_crosswalk_pct", "수출자 CIQ 미등재(행%)"),
               ("con_matched_pct", "수입자 매칭(행%)"), ("con_matched_val_pct", "수입자 매칭(금액%)"),
               ("shp_matched_pct", "수출자 매칭(행%)"),
               ("shp_matched_val_pct", "수출자 매칭(금액%)")],
              "C. crosswalk — 과거로 갈수록 나빠지는가")
    md += tbl([("con_pit_fallback_pct", "수입자 PIT fallback(%)"),
               ("shp_pit_fallback_pct", "수출자 PIT fallback(%)")], "D. 소유구조 시점 정확도")
    md += tbl([("hs_missing_pct", "HS 없음(%)"), ("hs_multi_pct", "다중 HS(행%)"),
               ("hs_multi_val_pct", "다중 HS(금액%)"), ("hs_ndigits6_pct", "6자리 코드(%)"),
               ("hs6_distinct", "고유 HS6 수")], "E. HS")

    md += ["", "### HS 개정 흔적 — 연도 간 코드 집합 겹침", "",
           "WCO 는 **2007·2012·2017·2022** 에 HS 를 개정한다. 아래는 각 연도의 단일HS 선적에서",
           "관찰된 HS6 집합을 첫 연도와 비교한 것이다. 품목 구성 변화도 섞여 있으므로 **개정의",
           "직접 증거는 아니고 규모 감**이다.", "",
           "| 비교 | 공통 | 앞해에만 | 뒷해에만 | Jaccard |", "|---|---|---|---|---|"]
    ms = list(codesets)
    for a, b in zip(ms, ms[1:]):
        A, B = codesets[a], codesets[b]
        md.append(f"| {a} → {b} | {len(A & B):,} | {len(A - B):,} | {len(B - A):,} | "
                  f"{len(A & B)/max(len(A | B),1):.3f} |")
    if len(ms) > 1:
        A, B = codesets[ms[0]], codesets[ms[-1]]
        md.append(f"| **{ms[0]} → {ms[-1]}** | {len(A & B):,} | {len(A - B):,} | {len(B - A):,} | "
                  f"{len(A & B)/max(len(A | B),1):.3f} |")

    (OUT / "00_period_probe.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"\n-> {OUT / '00_period_probe.md'}")
    print(t.to_string(index=False))


if __name__ == "__main__":
    main()
