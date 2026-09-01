# -*- coding: utf-8 -*-
r"""
v4_00_period_probe.py — v4 **완성 산출물**로 연도별 사용가능성을 전수 재계산한다.

묻는 것: "몇 년부터가 쓸 만한가, 최근은 왜 나빠 보이는가?" 2026-08-24 판은 연 1개월 표본
(별도 빈티지 `source\trade_probe`) 이었고 결론이 리터럴로 박혀 있었다. 이 판은
빌드가 끝난 `trade_pair_hs_quarter_YYYY.parquet` + `00_drop_accounting.csv` (전수) 만 읽고,
결론 문장을 **계산값에서 조립**한다 (측정일 = 실행일). build_all 의 6단계.

입력 : --in  (기본 v4_pairhs_full)  trade_pair_hs_quarter_*.parquet (필요 열만) · 00_drop_accounting.csv
       (있으면) 00_import_coverage_by_year.csv — Snowflake 연도별 전수 집계 (ex_20260824_import_coverage_by_year.py)
출력 : --out (기본 = --in)  00_period_probe.md · 00_period_probe.csv

연도별로 재는 것
  규모      원천 선적수·금액 (버림 회계 4버킷 합) · 쌍성립(kept_both) · v4 행수 · 분기 수
  쌍성립률  kept / 원천  (행=선적 기준, 금액)
  양측 매칭 match_status=both 의 선적·금액을 **전체 분모(원천)** 와 **쌍성립 분모(kept)** 로 각각
  단측 매칭 수입자(con_ciqid)·수출자(shp_ciqid) — 쌍성립 분모 (전체 분모는 csv 에)
  소급 UP   *_up_backcast 평균 (열이 있으면) — 1 에 가까우면 그 해 UP 은 시점값이 아님
  HS        단일HS 코드 집합의 인접 연도 Jaccard (WCO 개정 2012·2017·2022 표시)
분해: 전체 분모 양측 매칭률 = 쌍성립률 x 쌍성립분 대비 매칭률 -> 연도 간 변동을 log 분산으로 나눠
      "추세의 원인이 redaction(쌍 미성립) 인지 crosswalk 매칭 인지" 를 수치로 말한다.

사용:
    python v4_00_period_probe.py                                    # v4_pairhs_full -> 같은 폴더
    python v4_00_period_probe.py --in <산출폴더> --out <md 폴더>
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from v4_common import OUT_FULL, file_years, write_manifest

HS_REVISIONS = [2012, 2017, 2022]        # WCO HS 개정 발효 연도 (2007 은 표본 시작 전)
BASE_COLS = ["trade_quarter", "cal_year", "shp_ciqid", "con_ciqid", "hs6", "hs_status",
             "n_shipments", "value_usd", "match_status"]
OPT_COLS = ["shp_up_backcast", "con_up_backcast"]


def measure_year(p, acct):
    """연도 파일 하나 + 그 해의 버림 회계 -> 지표 dict, 단일HS 코드 집합."""
    have = pq.ParquetFile(p).schema_arrow.names
    cols = BASE_COLS + [c for c in OPT_COLS if c in have]
    f = pd.read_parquet(p, columns=cols)
    y = int(p.stem.split("_")[-1])
    src_n = int(acct["n_shipments"].sum())
    src_v = float(acct["value_usd"].sum())
    kept = acct[acct.bucket == "kept_both"]
    kn, kv = int(kept["n_shipments"].sum()), float(kept["value_usd"].sum())
    fn, fv = int(f["n_shipments"].sum()), float(f["value_usd"].sum())
    r = {"year": y, "quarters": int(f["trade_quarter"].nunique()), "v4_rows": len(f),
         "src_shipments": src_n, "src_value_bn": src_v / 1e9,
         "kept_shipments": kn, "kept_value_bn": kv / 1e9,
         "fact_shipments": fn, "fact_value_bn": fv / 1e9,
         "pair_pct": kn / src_n * 100 if src_n else np.nan,
         "pair_val_pct": kv / src_v * 100 if src_v else np.nan}
    masks = {"both": f["match_status"].eq("both"),
             "con": f["con_ciqid"].notna(), "shp": f["shp_ciqid"].notna()}
    for k, m in masks.items():
        n_, v_ = int(f.loc[m, "n_shipments"].sum()), float(f.loc[m, "value_usd"].sum())
        r[f"{k}_full_pct"] = n_ / src_n * 100 if src_n else np.nan
        r[f"{k}_full_val_pct"] = v_ / src_v * 100 if src_v else np.nan
        r[f"{k}_pair_pct"] = n_ / kn * 100 if kn else np.nan
        r[f"{k}_pair_val_pct"] = v_ / kv * 100 if kv else np.nan
    for s in ["shp", "con"]:
        c = f"{s}_up_backcast"
        r[f"{s}_backcast_mean"] = float(f[c].mean()) if c in f.columns and f[c].notna().any() else np.nan
    single = f["hs_status"].eq("single")
    r["hs_single_pct"] = float(f.loc[single, "n_shipments"].sum() / fn * 100) if fn else np.nan
    r["hs_multi_pct"] = float(f.loc[f["hs_status"].eq("multi"), "n_shipments"].sum() / fn * 100) if fn else np.nan
    codes = set(f.loc[single, "hs6"].dropna().unique())
    r["hs6_distinct"] = len(codes)
    return r, codes


def fmt(v, d=1):
    return "-" if pd.isna(v) else (f"{v:,.0f}" if d == 0 else f"{v:,.{d}f}")


def main():
    ap = argparse.ArgumentParser(description="v4 연도별 사용가능성 프로브 (완성 산출물 전수)")
    ap.add_argument("--in", dest="inp", default=str(OUT_FULL), help="v4 산출 폴더 (연도 parquet + 버림 회계)")
    ap.add_argument("--out", default=None, help="md/csv 출력 폴더 (기본 = --in)")
    a = ap.parse_args()
    inp = Path(a.inp)
    out = Path(a.out) if a.out else inp
    out.mkdir(parents=True, exist_ok=True)

    files = sorted(inp.glob("trade_pair_hs_quarter_*.parquet"))
    if not files:
        raise SystemExit(f"연도 파일 없음: {inp}")
    acct = pd.read_csv(inp / "00_drop_accounting.csv")
    acct["year"] = acct["trade_quarter"].str[:4].astype(int)

    rows, codesets = [], {}
    for p in files:
        y = int(p.stem.split("_")[-1])
        print(f"  {y} ...", flush=True)
        r, c = measure_year(p, acct[acct.year == y])
        rows.append(r)
        codesets[y] = c
    t = pd.DataFrame(rows).set_index("year").sort_index()
    ys = list(t.index)

    # HS Jaccard (인접 연도)
    jac = {}
    for a_, b_ in zip(ys, ys[1:]):
        A, B = codesets[a_], codesets[b_]
        jac[b_] = {"common": len(A & B), "only_prev": len(A - B), "only_next": len(B - A),
                   "jaccard": len(A & B) / max(len(A | B), 1), "revision": b_ in HS_REVISIONS}
    t["hs_jaccard_prev"] = pd.Series({y: v["jaccard"] for y, v in jac.items()})
    t["hs_revision_year"] = t.index.isin(HS_REVISIONS).astype(int)

    # 전년 대비 원천 선적수 변화 (피드 레짐 변화 의심용 — 완전 연도만)
    full = t[t["quarters"] == 4]
    t["src_yoy_pct"] = full["src_shipments"].pct_change() * 100

    csv_path = out / "00_period_probe.csv"
    t.reset_index().to_csv(csv_path, index=False, encoding="utf-8-sig")

    # ---- 분해: log(both_full) = log(pair) + log(both|pair) — 연도 간 분산의 몫 ----
    dec = {}
    for suf, lab in [("", "행"), ("_val", "금액")]:
        bf = np.log(t[f"both_full{suf}_pct"] / 100)
        pr = np.log(t[f"pair{suf}_pct"] / 100)
        bp = np.log(t[f"both_pair{suf}_pct"] / 100)
        var_bf = float(bf.var())
        cov_pr = float(((pr - pr.mean()) * (bf - bf.mean())).mean() * len(bf) / (len(bf) - 1))
        cov_bp = float(((bp - bp.mean()) * (bf - bf.mean())).mean() * len(bf) / (len(bf) - 1))
        dec[lab] = {"share_pair": cov_pr / var_bf if var_bf else np.nan,
                    "share_cond": cov_bp / var_bf if var_bf else np.nan,
                    "range_full": float(t[f"both_full{suf}_pct"].max() - t[f"both_full{suf}_pct"].min()),
                    "range_pair": float(t[f"pair{suf}_pct"].max() - t[f"pair{suf}_pct"].min()),
                    "range_cond": float(t[f"both_pair{suf}_pct"].max() - t[f"both_pair{suf}_pct"].min())}

    # ---- 결론 문장 (전부 계산값) ----
    q_first = pd.read_parquet(files[0], columns=["trade_quarter"])["trade_quarter"].min()
    q_last = pd.read_parquet(files[-1], columns=["trade_quarter"])["trade_quarter"].max()
    hi_pair, lo_pair = t["pair_pct"].idxmax(), t["pair_pct"].idxmin()
    hi_bf, lo_bf = t["both_full_pct"].idxmax(), t["both_full_pct"].idxmin()
    hi_bp, lo_bp = t["both_pair_pct"].idxmax(), t["both_pair_pct"].idxmin()
    hi_bpv, lo_bpv = t["both_pair_val_pct"].idxmax(), t["both_pair_val_pct"].idxmin()
    last = ys[-1]
    prev3 = t.loc[[y for y in ys[-4:-1]]]
    shp_last, shp_prev = t.loc[last, "shp_pair_pct"], prev3["shp_pair_pct"].mean()
    shp_drop = shp_prev - shp_last
    d_row, d_val = dec["행"], dec["금액"]

    def cause(d):
        gap = d["share_pair"] - d["share_cond"]
        if abs(gap) < 0.1:
            return "둘이 비슷하게 기여"
        return "**쌍 미성립(redaction)**" if gap > 0 else "**crosswalk 매칭**"

    bc = t["con_backcast_mean"]
    bc_years = [y for y in ys if pd.notna(bc[y]) and bc[y] >= 0.9]
    jump_years = [y for y in ys if pd.notna(t.loc[y, "src_yoy_pct"]) and abs(t.loc[y, "src_yoy_pct"]) >= 20]
    cov = inp / "00_import_coverage_by_year.csv"
    cov_jumps = ""
    if cov.exists():
        c = pd.read_csv(cov)
        dd_ = c["con_id_pct"].diff()
        cj = [f"{int(r.yr)}({d:+.1f}p)" for r, d in zip(c.itertuples(), dd_) if pd.notna(d) and abs(d) >= 10]
        cov_jumps = f" 수입자 ID 보유율(Snowflake 전수) 전년비 ±10%p 이상 변화 연도: {', '.join(cj) if cj else '없음'}."
    # HS Jaccard: 비개정·완전연도 전이의 중앙값±3MAD 밴드 밖이면 "뚜렷"
    non_j = {y: jac[y]["jaccard"] for y in jac if not jac[y]["revision"] and t.loc[y, "quarters"] == 4
             and t.loc[y - 1, "quarters"] == 4}
    rev_j = {y: jac[y]["jaccard"] for y in jac if jac[y]["revision"]}
    med = float(np.median(list(non_j.values()))) if non_j else np.nan
    mad = float(np.median([abs(v - med) for v in non_j.values()])) if non_j else np.nan
    band = med - 3 * max(mad, 0.005) if non_j else np.nan
    rev_txt = ", ".join(f"{y} {v:.3f}({'뚜렷' if v < band else '구별 안 됨'})" for y, v in rev_j.items())
    odd_non = [f"{y} {v:.3f}" for y, v in non_j.items() if v < band]
    n_clear = sum(1 for v in rev_j.values() if v < band)

    few = len(ys) < 3            # 연도 3개 미만이면 추세·분해·최근연도 판단을 하지 않는다
    bc_txt = ("연도별 평균: " + ", ".join(f"{y} {bc[y]:.2f}" for y in ys if pd.notna(bc[y]))
              if bc.notna().any() and len(ys) <= 6 else
              (f"연도별 평균 {bc.min():.2f}~{bc.max():.2f}" if bc.notna().any() else ""))
    concl = [
        f"1. **기간·규모**: 산출물은 {q_first}~{q_last} ({len(ys)}개 연도, 분기 {int(t['quarters'].sum())}개, "
        f"v4 {int(t['v4_rows'].sum()):,}행; 분기 4개 미만인 연도: "
        f"{', '.join(f'{y}({int(t.loc[y, "quarters"])}개)' for y in ys if t.loc[y, 'quarters'] < 4) or '없음'} — "
        f"원천 추출 시작·잠정 연도는 DECISIONS P-1·P-2). 원천 선적 {int(t['src_shipments'].sum()):,}건 중 쌍성립(양측 panjivaid) "
        f"{int(t['kept_shipments'].sum()):,}건 ({t['kept_shipments'].sum() / t['src_shipments'].sum() * 100:.1f}%, "
        f"금액 {t['kept_value_bn'].sum() / t['src_value_bn'].sum() * 100:.1f}%).",
        (f"2. **쌍성립률(행)** 은 {hi_pair} 이 {t.loc[hi_pair, 'pair_pct']:.1f}% 로 최고, {lo_pair} 가 "
         f"{t.loc[lo_pair, 'pair_pct']:.1f}% 로 최저. **전체 분모 양측 CIQ 매칭(행)** 은 {hi_bf} {t.loc[hi_bf, 'both_full_pct']:.1f}% "
         f"최고 / {lo_bf} {t.loc[lo_bf, 'both_full_pct']:.1f}% 최저 — 과거로 갈수록 나빠지지 "
         f"{'않는다(첫 해가 마지막 해보다 높다: ' if t.loc[ys[0], 'both_full_pct'] >= t.loc[ys[-1], 'both_full_pct'] else '는 않는다고 말할 수 없다('}"
         f"{ys[0]} {t.loc[ys[0], 'both_full_pct']:.1f}% vs {ys[-1]} {t.loc[ys[-1], 'both_full_pct']:.1f}%). "
         f"시작 연도를 늦출 근거는 {'없다' if hi_bf < ys[-1] else '있다'}."
         if not few else
         f"2. **쌍성립률·매칭률**: 연도 {len(ys)}개뿐이라 추세 판단 없음 — {ys[-1]} 쌍성립률 {t.loc[ys[-1], 'pair_pct']:.1f}%(행)/"
         f"{t.loc[ys[-1], 'pair_val_pct']:.1f}%(금액), 전체 분모 양측 매칭 {t.loc[ys[-1], 'both_full_pct']:.1f}%/"
         f"{t.loc[ys[-1], 'both_full_val_pct']:.1f}%, 쌍성립 분모 {t.loc[ys[-1], 'both_pair_pct']:.1f}%/{t.loc[ys[-1], 'both_pair_val_pct']:.1f}%."),
        (f"3. **추세의 원인 분해**: 전체 분모 매칭률 = 쌍성립률 x 쌍성립분 대비 매칭률. 연도 간 log 분산의 몫이 "
         f"쌍성립률 {d_row['share_pair'] * 100:.0f}% · 쌍성립분 대비 매칭률 {d_row['share_cond'] * 100:.0f}% (행) / "
         f"{d_val['share_pair'] * 100:.0f}% · {d_val['share_cond'] * 100:.0f}% (금액). 연도별 폭(행)은 전체 분모 "
         f"{d_row['range_full']:.1f}%p · 쌍성립률 {d_row['range_pair']:.1f}%p · 쌍성립분 대비 {d_row['range_cond']:.1f}%p. "
         f"→ 행 기준 주 원인은 {cause(d_row)}, 금액 기준은 {cause(d_val)}. "
         f"쌍성립분 대비 매칭률(금액)은 {lo_bpv} {t.loc[lo_bpv, 'both_pair_val_pct']:.1f}% ~ {hi_bpv} "
         f"{t.loc[hi_bpv, 'both_pair_val_pct']:.1f}% (폭 {d_val['range_cond']:.1f}%p) 로 "
         f"{'쌍성립률(폭 ' + f'{d_val["range_pair"]:.1f}' + '%p) 보다 평탄하다' if d_val['range_cond'] < d_val['range_pair'] else '쌍성립률보다 더 움직인다'}; "
         f"행 기준은 {lo_bp} {t.loc[lo_bp, 'both_pair_pct']:.1f}% ~ {hi_bp} {t.loc[hi_bp, 'both_pair_pct']:.1f}%."
         if not few else "3. **추세의 원인 분해**: 연도 3개 이상일 때만 계산한다."),
        f"4. **피드 레짐 변화 의심 연도** (원천 선적수 전년 대비 ±20% 이상, 완전 연도만): "
        f"{', '.join(f'{y}({t.loc[y, "src_yoy_pct"]:+.0f}%)' for y in jump_years) if jump_years else ('없음' if not few else '판단 불가(연도 부족)')}.{cov_jumps} "
        f"crosswalk 는 activeFlag=1 현재 스냅샷의 소급 적용이므로 과거 연도 매칭률에는 생존편향 방향의 힘이 있다 "
        f"(측정으로 분리 불가 — 해석 주의).",
        (f"5. **최근 연도({last})**: 수출자 매칭(쌍성립 분모, 행) {shp_last:.1f}% vs 직전 3개년 평균 {shp_prev:.1f}% "
         f"({abs(shp_drop):.1f}%p {'하락' if shp_drop > 0 else '상승'}) → "
         + (f"**{last} 는 잠정 취급**(피드 백필 가능성; 확정 시 재계산)." if shp_drop >= 3
            else f"{last} 를 특별 취급할 근거는 없다.")
         if not few else
         f"5. **최근 연도({last})**: 수출자 매칭(쌍성립 분모, 행) {shp_last:.1f}% — 비교할 직전 연도가 없어 판단 없음."),
        f"6. **소급 UP**: `con_up_backcast` 평균이 0.9 이상인 연도 = "
        + (f"{bc_years[0]}~{bc_years[-1]} ({len(bc_years)}개)" if bc_years else "없음")
        + (f" ({bc_txt})" if bc_txt else "")
        + (" — 이 연도들의 `*_up` 은 거래 시점값이 아니라 현재 기준 소급값이다(CIQ PIT start_date 가 1900 아니면 2018-04 이후). "
           if bc_years else "")
        + (" 2018-04 이후 연도의 1 은 '추적 시작 이래 모회사 변경 기록 없음' 이라는 뜻(값은 동시점 관측; 1900 구간의 93% 가 9999-12-31 까지 열려 있다)."
           if bc.notna().any() else " (열 없음 — 2026-09-01 이전 빌드)"),
        (f"7. **HS 개정 흔적**: 인접 연도 Jaccard — 비개정·완전연도 전이 {len(non_j)}개의 중앙값 {med:.3f}(MAD {mad:.3f}, "
         f"밴드 하한 {band:.3f}); 개정 연도 {rev_txt}. → 개정 {len(rev_j)}개 중 {n_clear}개만 뚜렷"
         + (f"; 비개정 연도에도 밴드 밖 급락 {', '.join(odd_non)} (피드 레짐 변화·품목 구성 변화)" if odd_non else "")
         + " — 개정만의 흔적이라 단정할 수 없고 처리 방식은 PI 판단(질문 92호)."
         if non_j and rev_j else
         f"7. **HS 개정 흔적**: 인접 연도 전이가 부족해(개정 {len(rev_j)}개 · 비개정 {len(non_j)}개) 판단 없음."),
    ]

    # ---- md ----
    md = ["# v4 전 기간 프로브 — 연도별 사용가능성 (완성 산출물 전수 재계산)", "",
          f"**측정일**: {pd.Timestamp.now():%Y-%m-%d} · **입력**: `{inp}` (연도 parquet {len(files)}개 + "
          f"`00_drop_accounting.csv`) · **스크립트**: `scripts\\processing\\v4_00_period_probe.py`", "",
          "2026-08-24 판(연 1개월 표본, 별도 빈티지 `source\\trade_probe`)을 대체한다. 아래 수치와 결론 문장은 "
          "전부 이 실행에서 계산된 값이다.", "",
          "## 결론 (계산값)", ""] + concl + ["", "## 연도별 표", "",
          "분모: **전체** = 원천 선적(버림 회계 4버킷 합), **쌍성립** = 양측 panjivaid 가 있어 v4 에 남은 선적(kept_both). "
          "행 = 선적 건수 기준, 금액 = valueofgoodsusd 기준.", "",
          "### A. 규모 · 쌍성립", "",
          "| 연도 | 분기 | 원천 선적 | 원천 $B | 쌍성립 선적 | 쌍성립률 행% | 쌍성립률 금액% | v4 행 | 원천 전년비% |",
          "|---|---|---|---|---|---|---|---|---|"]
    for y, r in t.iterrows():
        md.append(f"| {y} | {r.quarters:.0f} | {r.src_shipments:,.0f} | {r.src_value_bn:,.1f} | {r.kept_shipments:,.0f} | "
                  f"{r.pair_pct:.1f} | {r.pair_val_pct:.1f} | {r.v4_rows:,.0f} | {fmt(r.src_yoy_pct)} |")
    md += ["", "### B. 양측 CIQ 매칭 — 전체 분모 vs 쌍성립 분모", "",
           "| 연도 | 전체 행% | 전체 금액% | 쌍성립 행% | 쌍성립 금액% |", "|---|---|---|---|---|"]
    for y, r in t.iterrows():
        md.append(f"| {y} | {r.both_full_pct:.1f} | {r.both_full_val_pct:.1f} | {r.both_pair_pct:.1f} | {r.both_pair_val_pct:.1f} |")
    md += ["", "### C. 단측 매칭 (쌍성립 분모) · 소급 UP", "",
           "| 연도 | 수입자 행% | 수입자 금액% | 수출자 행% | 수출자 금액% | con_up_backcast 평균 | shp_up_backcast 평균 |",
           "|---|---|---|---|---|---|---|"]
    for y, r in t.iterrows():
        md.append(f"| {y} | {r.con_pair_pct:.1f} | {r.con_pair_val_pct:.1f} | {r.shp_pair_pct:.1f} | {r.shp_pair_val_pct:.1f} | "
                  f"{fmt(r.con_backcast_mean, 3)} | {fmt(r.shp_backcast_mean, 3)} |")
    md += ["", "### D. HS — 단일HS 비중 · 고유 HS6 · 인접 연도 Jaccard", "",
           "WCO HS 개정 발효 연도(2012·2017·2022)는 ★ 표시. Jaccard 는 (앞해 ∩ 뒷해)/(앞해 ∪ 뒷해).", "",
           "| 연도 | 단일HS 선적% | 다중HS 선적% | 고유 HS6 | 공통 | 앞해에만 | 뒷해에만 | Jaccard(전년→) |",
           "|---|---|---|---|---|---|---|---|"]
    for y, r in t.iterrows():
        j = jac.get(y)
        star = " ★" if y in HS_REVISIONS else ""
        md.append(f"| {y}{star} | {r.hs_single_pct:.1f} | {r.hs_multi_pct:.1f} | {r.hs6_distinct:,.0f} | "
                  + (f"{j['common']:,} | {j['only_prev']:,} | {j['only_next']:,} | {j['jaccard']:.3f} |" if j else "- | - | - | - |"))

    if cov.exists():
        c = pd.read_csv(cov)
        md += ["", "## 참고 — 연도별 전수 커버리지 (Snowflake 집계, 원천 추출과 별개)", "",
               f"`{cov.name}` (`ex_20260824_import_coverage_by_year.py`). `filter_keeps` = 표준필터(미국 실착 + 통과화물 제외) "
               "통과율 · `con_id`/`shp_id` = 당사자 ID 가 있는 비중(= 1 − redaction). 표준필터 통과율 범위 "
               f"{c.filter_keeps_pct.min():.1f}~{c.filter_keeps_pct.max():.1f}% · 수출자 ID 최저 연도 "
               f"{int(c.loc[c.shp_id_pct.idxmin(), 'yr'])} ({c.shp_id_pct.min():.1f}%) · 수입자 ID 전년비 ±10%p 이상 변화 연도: "
               + (", ".join(f"{int(r.yr)}({d:+.1f}p)" for r, d in zip(c.itertuples(), c.con_id_pct.diff()) if pd.notna(d) and abs(d) >= 10) or "없음"),
               "", "| 연도 | 원본 행 | 필터후 | filter_keeps | con_id | shp_id |", "|---|---|---|---|---|---|"]
        for r in c.itertuples():
            md.append(f"| {r.yr} | {r.raw_rows:,} | {r.pass_both:,} | {r.filter_keeps_pct}% | "
                      f"{r.con_id_pct}% | {r.shp_id_pct}% |")
        md.append(f"\n합계 원본 **{c.raw_rows.sum():,}** · 필터후 **{c.pass_both.sum():,}**")

    md += ["", "## 읽는 법", "",
           "- 쌍성립률이 떨어지는 해는 CBP 기밀요청(redaction)으로 당사자 블록이 지워진 B/L 이 늘어난 해다 — 복구 불가, "
           "비율 지표의 분모는 원천에서 따로 구할 것 (README §2-①).",
           "- 쌍성립분 대비 매칭률은 crosswalk(CIQ 등재) 품질을 재는 지표다. 이 값이 평탄하면 '과거 매칭이 나쁘다' 는 걱정은 근거가 없다.",
           "- `*_up_backcast` 가 1 인 연도의 소유구조(UP) 는 현재 기준 소급값이다 — M&A·소유구조 변화 연구에는 쓰지 말 것 (COLUMNS·DECISIONS D4 보충)."]
    md_path = out / "00_period_probe.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
    write_manifest(out, "period_probe", inputs=files + [inp / "00_drop_accounting.csv"],
                   outputs=[md_path, csv_path],
                   extra={"years": file_years(files), "decomposition": dec})
    print(f"\n-> {md_path}")
    print("\n".join(concl))


if __name__ == "__main__":
    main()
