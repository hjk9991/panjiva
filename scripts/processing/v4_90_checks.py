# -*- coding: utf-8 -*-
r"""
v4_90_checks.py — v4 무역 팩트 무결성 검증. 결과를 90_checks.md 로 쓰고,
하나라도 FAIL 이면 exit 1 (v4_build_all 이 여기서 멈추도록). WARN 은 exit 에 영향 없음.

원칙: "통과"를 자칭하지 않고 원천과 직접 대사한다. 실패는 실패로 적는다.
연도 파일 단위로 검사해 메모리를 묶는다 (분기가 연도를 걸치지 않으므로 등가).
원천 월은 검사 대상 폴더에 있는 연도 파일의 연도로 자동 제한한다 (부분 빌드도 대사 가능).

검사 목록
  1   버림회계 4버킷 합 = 원천 총계 (선적·금액)          1b 중량·TEU·컨테이너도 (2026-09-01 추가 열)
  2   팩트 선적·금액 = kept_both                          2b 중량·TEU·컨테이너도
  3   grain 유일   4 panjivaid->ciqid 1:1   5 UP (panjivaid,분기) 단일   8b ciqid 의 UP 단일(양측 통합)
  6   hs6 는 single 일 때만                                6b [WARN] single 인데 6자리 아님 (원천 left(hs_raw,6))
                                                          6c hs6_ndigits = len(hs6) (열 있을 때)
  7   ciqid·UP roll-up 금액 보존
  8   is_intra_group 규칙 · self->intra · self 인데 intra NA 없음
  9   측정값 음수·결측 없음                                9b n_bl_house+n_bl_simple <= n_shipments
  9c  cal_quarter∈1..4 · cal_year=파일명 연도 · trade_quarter 라벨 정합
  9d  분기 연속성 (min~max 사이 빠진 분기 없음)
  10  [WARN] 분기 규모 — 행·금액이 분기 중위값의 10% 미만이면 경고 (매직 임계값 대신 상대 기준)
  10b 연도별 *_up_changed 합계 · *_up_backcast 평균 표 (H1 노출: 2018 이전 UP 은 소급값)
  11  2024 조각 = 동결 벤치마크 — **공통 열만** 값 완전 동일, 열 집합 차이는 목록으로 출력
  F1~F5 재무 (폴더에 ciq_fin_period.parquet 이 있을 때만): period_id 유일 · fx 결측률 ·
        cal_year 범위 · wide 키 유일 · wide period_type 단일

사용:
    python v4_90_checks.py                                   # v4_pairhs_full 검사
    python v4_90_checks.py --out <dir> [--src <dir>...] [--benchmark <parquet>|""] [--fin-dir <dir>]
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from v4_common import (DEFAULT_TRADE_SRC, OUT_2024, OUT_FULL, discover_months, file_years,
                       write_manifest)

R = []


def chk(no, name, ok, detail, warn=False):
    """warn=True 면 실패해도 FAIL 이 아니라 WARN (exit 코드에 영향 없음)."""
    res = "PASS" if ok else ("WARN" if warn else "**FAIL**")
    R.append({"no": no, "check": name, "result": res, "detail": detail})
    print(f"  [{res.strip('*')}] {no}. {name} — {detail}")
    return ok


def close(a, b, scale):
    """합계 대사용 허용오차: 부동소수 합산 순서 차이만 허용 (상대 1e-9 + 절대 1.0)."""
    return abs(a - b) <= 1e-9 * abs(scale) + 1.0


def main():
    ap = argparse.ArgumentParser(description="v4 무역 팩트 무결성 검증")
    ap.add_argument("--out", default=str(OUT_FULL), help="검사할 v4 산출 폴더 (90_checks.md 도 여기에)")
    ap.add_argument("--src", nargs="*", default=None, help="원천 폴더(들). 기본 v4_common.DEFAULT_TRADE_SRC")
    ap.add_argument("--benchmark", default=str(OUT_2024 / "trade_pair_hs_quarter_2024.parquet"),
                    help="2024 동결본 경로 ('' 이면 11번 생략)")
    ap.add_argument("--fin-dir", default=None,
                    help="재무 산출 폴더 (F1~F5). 기본 = --out. ciq_fin_period.parquet 없으면 생략")
    a = ap.parse_args()
    out = Path(a.out)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC
    fin_dir = Path(a.fin_dir) if a.fin_dir else out

    files = sorted(out.glob("trade_pair_hs_quarter_*.parquet"))
    if not files:
        raise SystemExit(f"연도 파일 없음: {out}")
    acct_path = out / "00_drop_accounting.csv"
    acct = pd.read_csv(acct_path)
    yrs = file_years(files)
    months = discover_months(src_dirs, years=yrs)
    print(f"검사 대상: 연도 파일 {len(files)}개 ({yrs[0]}~{yrs[-1]}) · 원천 {len(months)}개월")

    # ---- 원천 총계 (선적·금액·중량·TEU·컨테이너) ----
    src = {"n": 0, "value_usd": 0.0, "weight_kg": 0.0, "teu": 0.0, "n_containers": 0}
    for ym, p in months.items():
        d = pd.read_parquet(p, columns=["valueofgoodsusd", "weightkg", "volumeteu", "numberofcontainers"])
        src["n"] += len(d)
        src["value_usd"] += float(d["valueofgoodsusd"].fillna(0.0).sum())
        src["weight_kg"] += float(d["weightkg"].sum())
        src["teu"] += float(d["volumeteu"].sum())
        src["n_containers"] += int(d["numberofcontainers"].astype("int64").sum())
        del d
    an, av = int(acct["n_shipments"].sum()), float(acct["value_usd"].sum())
    chk(1, "버림회계 4버킷 합 = 원천 총계 (선적·금액)", an == src["n"] and abs(av - src["value_usd"]) < 1.0,
        f"선적 {an:,} vs 원천 {src['n']:,} (차 {an - src['n']}) · 금액 차 ${av - src['value_usd']:,.2f}")

    has_acct_ext = all(c in acct.columns for c in ["weight_kg", "teu", "n_containers"])
    if has_acct_ext:
        aw, at, ac = float(acct["weight_kg"].sum()), float(acct["teu"].sum()), int(acct["n_containers"].sum())
        chk("1b", "버림회계 4버킷 합 = 원천 총계 (중량·TEU·컨테이너)",
            close(aw, src["weight_kg"], src["weight_kg"]) and close(at, src["teu"], src["teu"])
            and ac == src["n_containers"],
            f"중량 차 {aw - src['weight_kg']:,.3f}kg · TEU 차 {at - src['teu']:,.4f} · "
            f"컨테이너 {ac:,} vs {src['n_containers']:,} (차 {ac - src['n_containers']})")
    else:
        chk("1b", "버림회계 4버킷 합 = 원천 총계 (중량·TEU·컨테이너)", False,
            "00_drop_accounting.csv 에 weight_kg·teu·n_containers 열 없음 (2026-09-01 이전 빌드) — 재빌드 필요",
            warn=True)

    kept = acct[acct.bucket == "kept_both"]
    kn, kv = int(kept["n_shipments"].sum()), float(kept["value_usd"].sum())

    # ---- 연도 파일 순회 검사 ----
    K = ["trade_quarter", "shp_panjivaid", "con_panjivaid", "hs6", "hs_status"]
    tot = {"rows": 0, "ships": 0, "value_usd": 0.0, "weight_kg": 0.0, "teu": 0.0, "n_containers": 0}
    dup = bad4 = bad5 = bad8b = v6a = v6b = v81 = v82 = v83 = v84 = neg = nul = 0
    hs_short = nd_bad = bl_bad = q_bad = y_bad = lab_bad = 0
    rollup_bad = 0.0
    qstats, ytab, cols_seen = [], [], None
    for p in files:
        f = pd.read_parquet(p)
        fy = int(p.stem.split("_")[-1])
        cols_seen = list(f.columns) if cols_seen is None else cols_seen
        tot["rows"] += len(f)
        tot["ships"] += int(f["n_shipments"].sum())
        tot["value_usd"] += float(f["value_usd"].sum())
        tot["weight_kg"] += float(f["weight_kg"].sum())
        tot["teu"] += float(f["teu"].sum())
        tot["n_containers"] += int(f["n_containers"].sum())
        dup += int(f.duplicated(K).sum())
        for s in ["shp", "con"]:
            g = (f.dropna(subset=[f"{s}_ciqid"])
                 .groupby(["trade_quarter", f"{s}_panjivaid"])[f"{s}_ciqid"].nunique())
            bad4 += int((g > 1).sum())
            g = (f.dropna(subset=[f"{s}_up"])
                 .groupby(["trade_quarter", f"{s}_panjivaid"])[f"{s}_up"].nunique())
            bad5 += int((g > 1).sum())
        aa = f.dropna(subset=["shp_ciqid", "shp_up"])[
            ["trade_quarter", "shp_ciqid", "shp_up"]].rename(
            columns={"shp_ciqid": "ciqid", "shp_up": "up"})
        cc = f.dropna(subset=["con_ciqid", "con_up"])[
            ["trade_quarter", "con_ciqid", "con_up"]].rename(
            columns={"con_ciqid": "ciqid", "con_up": "up"})
        g = pd.concat([aa, cc]).drop_duplicates().groupby(["trade_quarter", "ciqid"])["up"].nunique()
        bad8b += int((g > 1).sum())
        del aa, cc, g
        single = f["hs_status"].eq("single")
        v6a += int((~single & f["hs6"].notna()).sum())
        v6b += int((single & f["hs6"].isna()).sum())
        hlen = f["hs6"].str.len()
        hs_short += int((single & f["hs6"].notna() & hlen.ne(6).fillna(False)).sum())
        if "hs6_ndigits" in f.columns:
            nd = f["hs6_ndigits"]
            nd_bad += int(((nd.isna() != f["hs6"].isna()) | (nd.astype("Float64") != hlen.astype("Float64")).fillna(False)).sum())
        m = f[f.match_status == "both"]
        av_ = m["value_usd"].sum()
        bv = m.groupby(["trade_quarter", "shp_ciqid", "con_ciqid", "hs6", "hs_status"],
                       dropna=False)["value_usd"].sum().sum()
        cv = m.groupby(["trade_quarter", "shp_up", "con_up", "hs6", "hs_status"],
                       dropna=False)["value_usd"].sum().sum()
        rollup_bad = max(rollup_bad, abs(av_ - bv), abs(av_ - cv))
        del m
        both_up = f.shp_up.notna() & f.con_up.notna()
        v81 += int((~both_up & f.is_intra_group.notna()).sum())
        v82 += int((both_up & f.is_intra_group.isna()).sum())
        v83 += int(((f.is_self == 1) & f.is_intra_group.notna() & (f.is_intra_group != 1)).sum())
        v84 += int(((f.is_self == 1) & f.is_intra_group.isna()).sum())
        neg += int((f[["n_shipments", "value_usd"]] < 0).any(axis=1).sum())
        nul += int(f["n_shipments"].isna().sum())
        bl_bad += int(((f["n_bl_house"] + f["n_bl_simple"]) > f["n_shipments"]).sum())
        q_bad += int((~f["cal_quarter"].isin([1, 2, 3, 4])).sum())
        y_bad += int((f["cal_year"] != fy).sum())
        lab = f["cal_year"].astype(str) + "Q" + f["cal_quarter"].astype(str)
        lab_bad += int((f["trade_quarter"] != lab).sum())
        qstats.append(f.groupby("trade_quarter").agg(rows=("value_usd", "size"),
                                                     value=("value_usd", "sum"),
                                                     ships=("n_shipments", "sum")))
        row = {"year": fy, "rows": len(f),
               "shp_up_changed": int(f["shp_up_changed"].sum()),
               "con_up_changed": int(f["con_up_changed"].sum())}
        for s in ["shp", "con"]:
            c = f"{s}_up_backcast"
            if c in f.columns:
                v = f[c]
                row[f"{s}_backcast_mean"] = float(v.mean()) if v.notna().any() else np.nan
                row[f"{s}_backcast_na_pct"] = float(v.isna().mean() * 100)
            else:
                row[f"{s}_backcast_mean"] = np.nan
                row[f"{s}_backcast_na_pct"] = np.nan
        ytab.append(row)
        del f

    chk(2, "팩트 선적수·금액 = kept_both", tot["ships"] == kn and abs(tot["value_usd"] - kv) < 1.0,
        f"선적 {tot['ships']:,} vs {kn:,} · 금액 차 ${tot['value_usd'] - kv:,.2f}")
    if has_acct_ext:
        kw, kt, kc = float(kept["weight_kg"].sum()), float(kept["teu"].sum()), int(kept["n_containers"].sum())
        chk("2b", "팩트 중량·TEU·컨테이너 = kept_both",
            close(tot["weight_kg"], kw, kw) and close(tot["teu"], kt, kt) and tot["n_containers"] == kc,
            f"중량 차 {tot['weight_kg'] - kw:,.3f}kg · TEU 차 {tot['teu'] - kt:,.4f} · "
            f"컨테이너 {tot['n_containers']:,} vs {kc:,}")
    chk(3, "grain 유일 (분기 x shp x con x hs6 x hs_status)", dup == 0, f"중복 {dup}행")
    chk(4, "panjivaid -> ciqid 가 분기 내 1:1", bad4 == 0, f"위반 {bad4}건")
    chk(5, "UP 이 (panjivaid, 분기) 단위로 단일", bad5 == 0, f"위반 {bad5}건")
    chk(6, "hs6 는 hs_status=single 일 때만 존재", v6a == 0 and v6b == 0,
        f"위반 {v6a}/{v6b}")
    chk("6b", "hs_status=single 인데 hs6 가 6자리가 아님 (원천 left(hs_raw,6) — 2·4자리)", hs_short == 0,
        f"{hs_short:,}행 (판정 아님 — 원천 사실. 자릿수는 hs6_ndigits 열)", warn=True)
    if "hs6_ndigits" in cols_seen:
        chk("6c", "hs6_ndigits = len(hs6) (결측 일치 포함)", nd_bad == 0, f"불일치 {nd_bad}행")
    else:
        chk("6c", "hs6_ndigits = len(hs6)", False, "hs6_ndigits 열 없음 (2026-09-01 이전 빌드)", warn=True)
    chk(7, "ciqid·UP 단위 roll-up 시 금액 보존", rollup_bad < 1.0,
        f"최대 오차 ${rollup_bad:.4f}")
    chk(8, "is_intra_group 은 양측 UP 있을 때만 · self 는 항상 intra · self 인데 intra NA 없음",
        v81 == 0 and v82 == 0 and v83 == 0 and v84 == 0, f"위반 {v81}/{v82}/{v83}/{v84}")
    chk("8b", "같은 ciqid 는 분기 내 UP 이 하나 (양측 통합)", bad8b == 0, f"위반 {bad8b}건")
    chk(9, "측정값에 음수·결측 없음", neg == 0 and nul == 0, f"음수 {neg} · 결측 {nul}")
    chk("9b", "n_bl_house + n_bl_simple <= n_shipments", bl_bad == 0, f"위반 {bl_bad}행")
    chk("9c", "cal_quarter∈{1..4} · cal_year = 파일명 연도 · trade_quarter = f'{cal_year}Q{cal_quarter}'",
        q_bad == 0 and y_bad == 0 and lab_bad == 0, f"위반 {q_bad}/{y_bad}/{lab_bad}행")

    q = pd.concat(qstats).groupby(level=0).sum().sort_index()
    have = list(q.index)
    p0, p1 = pd.Period(have[0], freq="Q"), pd.Period(have[-1], freq="Q")
    expect = [str(x) for x in pd.period_range(p0, p1, freq="Q")]
    missing_q = [x for x in expect if x not in set(have)]
    chk("9d", "분기 연속성 (첫~끝 분기 사이 빠짐 없음)", not missing_q,
        f"{have[0]}~{have[-1]} {len(have)}개 분기 · 빠짐 {missing_q if missing_q else '없음'}")

    med_rows, med_val = float(q["rows"].median()), float(q["value"].median())
    low = q[(q["rows"] < 0.1 * med_rows) | (q["value"] < 0.1 * med_val)]
    chk(10, "분기별 규모 — 행·금액이 분기 중위값의 10% 이상", len(low) == 0,
        f"{len(q)}개 분기 · 중위 {med_rows:,.0f}행/${med_val/1e9:.0f}B · 최소 {q['rows'].min():,}행/"
        f"${q['value'].min()/1e9:.0f}B" + (f" · 미달 {list(low.index)}" if len(low) else ""), warn=True)

    yt = pd.DataFrame(ytab).set_index("year").sort_index()
    bc_years = [int(y) for y, r in yt.iterrows()
                if pd.notna(r["con_backcast_mean"]) and r["con_backcast_mean"] >= 0.9]
    zero_chg = [int(y) for y, r in yt.iterrows() if r["con_up_changed"] == 0 and r["shp_up_changed"] == 0]
    chk("10b", "연도별 *_up_changed 합계 · *_up_backcast 평균 (아래 표 — 판정 아님)", True,
        (f"con_up_backcast 평균 0.9 이상(PIT 추적 시작 2018-04-16 이전 — UP 이 시점값이 아님) 연도: {bc_years[0]}~{bc_years[-1]} "
         f"({len(bc_years)}개)" if bc_years
         else ("평균 0.9 이상 연도 없음" if yt["con_backcast_mean"].notna().any()
               else "backcast 열 없음 (2026-09-01 이전 빌드)"))
        + (f" · up_changed 합이 양측 모두 0 인 연도: {zero_chg[0]}~{zero_chg[-1]} ({len(zero_chg)}개)" if zero_chg
           else " · up_changed 합이 0 인 연도 없음"))

    # ---- 11. 2024 벤치마크 대사 (공통 열만 값 비교, 열 집합 차이는 목록) ----
    bench = Path(a.benchmark) if a.benchmark else None
    p2024 = out / "trade_pair_hs_quarter_2024.parquet"
    bench_used = False
    if bench and bench.exists() and p2024.exists() and bench.resolve() != p2024.resolve():
        bench_used = True
        aa = pd.read_parquet(bench)
        bb = pd.read_parquet(p2024)
        common = [c for c in aa.columns if c in bb.columns]
        only_bench = [c for c in aa.columns if c not in bb.columns]
        only_new = [c for c in bb.columns if c not in aa.columns]
        aa = aa.sort_values(K, kind="mergesort").reset_index(drop=True)
        bb = bb.sort_values(K, kind="mergesort").reset_index(drop=True)
        same_n = len(aa) == len(bb)
        diff_cols = [c for c in common if not aa[c].equals(bb[c])] if same_n else common
        ok11 = same_n and not diff_cols
        chk(11, "2024 조각 = 동결 벤치마크 (공통 열 값 완전 동일 · 결측 포함)", ok11,
            f"{len(aa):,} vs {len(bb):,}행 · 공통 {len(common)}열 "
            + ("전부 동일" if ok11 else f"차이 {diff_cols}")
            + (f" · 벤치마크에만 {only_bench}" if only_bench else "")
            + (f" · 새 파일에만 {only_new} (존재만 확인)" if only_new else ""))
        del aa, bb

    # ---- F. 재무 (있을 때만) ----
    fin_rows = []
    if (fin_dir / "ciq_fin_period.parquet").exists():
        per = pd.read_parquet(fin_dir / "ciq_fin_period.parquet",
                              columns=["financial_period_id", "companyid", "period_type_id",
                                       "cal_year", "cal_quarter", "currency", "fx_per_usd",
                                       "is_preferred", "is_preferred_year"])
        dupf = int(per["financial_period_id"].duplicated().sum())
        chk("F1", "재무 financial_period_id 유일", dupf == 0, f"중복 {dupf} / {len(per):,}행")
        fxna = per["fx_per_usd"].isna()
        chk("F2", "재무 fx_per_usd 결측률 (통화 있는 행 기준 0.5% 이하)",
            float(fxna[per["currency"].notna()].mean()) <= 0.005,
            f"결측 {int(fxna.sum()):,}행 ({fxna.mean()*100:.3f}%) · 통화 결측 {int(per['currency'].isna().sum()):,}행",
            warn=True)
        y0, y1 = int(per["cal_year"].min()), int(per["cal_year"].max())
        mf = fin_dir / "_manifest.json"
        want = None
        if mf.exists():
            for h in json.loads(mf.read_text(encoding="utf-8")):
                if h.get("stage") == "fin_build":
                    want = h.get("extra", {}).get("years")
        chk("F3", "재무 cal_year 범위 = 빌드 요청 범위 (manifest fin_build.years)",
            want is None or (y0 >= want[0] and y1 <= want[1]),
            f"cal_year {y0}~{y1}" + (f" vs 요청 {want[0]}~{want[1]}" if want else " (manifest 없음 — 범위만 보고)"))
        for name, keys, ptype, flag in [("quarter", ["companyid", "cal_year", "cal_quarter"], 2, "is_preferred"),
                                        ("annual", ["companyid", "cal_year"], 1, "is_preferred_year")]:
            wp = fin_dir / f"ciq_fin_wide_{name}.parquet"
            if not wp.exists():
                continue
            w = pd.read_parquet(wp, columns=keys + ["period_type_id"])
            dw = int(w.duplicated(keys).sum())
            pts = sorted(w["period_type_id"].dropna().unique().tolist())
            chk(f"F4-{name}", f"wide {name} 키 {tuple(keys)} 유일", dw == 0, f"중복 {dw} / {len(w):,}행")
            chk(f"F5-{name}", f"wide {name} period_type_id 단일 (= {ptype})", pts == [ptype], f"{pts}")
            pref = per[(per.period_type_id == ptype) & (per[flag] == 1)]
            chk(f"F6-{name}", f"wide {name} 행 수 = period 의 {flag}=1 행 수", len(w) == len(pref),
                f"{len(w):,} vs {len(pref):,}")
            del w
        del per
        fin_rows = [r for r in R if str(r["no"]).startswith("F")]

    # ---- 문서 ----
    fails = sum(1 for r in R if "FAIL" in r["result"])
    warns = sum(1 for r in R if r["result"] == "WARN")
    md = [f"# v4 무역 팩트 검증 ({out.name})", "",
          f"**검증일**: {pd.Timestamp.now():%Y-%m-%d} · 연도 파일 {len(files)}개 ({yrs[0]}~{yrs[-1]}) · "
          f"총 {tot['rows']:,}행 · **스크립트**: `scripts\\processing\\v4_90_checks.py`", "",
          f"**요약**: 검사 {len(R)}항목 중 PASS {len(R) - fails - warns} · WARN {warns} · FAIL {fails}"
          + (" — WARN 은 판정 보류(사실 기록)이며 exit 코드에 영향 없음" if warns else ""), "",
          "## 검증 결과", "", "| # | 검사 | 결과 | 상세 |", "|---|---|---|---|"]
    for r in R:
        md.append(f"| {r['no']} | {r['check']} | {r['result']} | {r['detail']} |")
    md += ["", "## 참고 — 연도별 UP 변동·소급 (10b, 판정 아님)", "",
           "`*_up_changed` 합 = 분기 안에서 UP 이 흔들려 금액 최대값을 고른 행 수. "
           "`*_up_backcast` 평균 = 분기 시작일이 PIT 추적 시작(1900 이 아닌 최소 start_date, 실측 2018-04-16) **이전**인 행 비중 "
           "— 그 분기에는 시점별 소유구조 기록이 없었으므로 UP 은 **시점값이 아니라 소급값**(1900 구간 값 또는 스냅샷 fallback)이다. "
           "추적 시작 이후 분기는 0(93% 의 1900 구간이 9999-12-31 까지 열려 있어 '변경 기록 없음' 일 뿐 관측값). "
           "NA% = UP 이 없는 행.", "",
           "| 연도 | 행 | shp_up_changed 합 | con_up_changed 합 | shp_backcast 평균 | (NA%) | con_backcast 평균 | (NA%) |",
           "|---|---|---|---|---|---|---|---|"]
    for y, r in yt.iterrows():
        f_ = lambda v, d=3: "-" if pd.isna(v) else f"{v:.{d}f}"
        md.append(f"| {y} | {r.rows:,.0f} | {r.shp_up_changed:,.0f} | {r.con_up_changed:,.0f} | "
                  f"{f_(r.shp_backcast_mean)} | {f_(r.shp_backcast_na_pct, 1)} | "
                  f"{f_(r.con_backcast_mean)} | {f_(r.con_backcast_na_pct, 1)} |")
    md += ["", "## 참고 — 분기별 규모 (판정 아님)", "",
           "| 분기 | 행 | 선적 | 금액($B) |", "|---|---|---|---|"]
    for i, r in q.iterrows():
        md.append(f"| {i} | {r.rows:,.0f} | {r.ships:,.0f} | {r.value/1e9:,.1f} |")
    md_path = out / "90_checks.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")

    write_manifest(out, "trade_checks",
                   inputs=files + [acct_path] + ([bench] if bench_used else []),
                   outputs=[md_path],
                   extra={"checks": len(R), "fails": fails, "warns": warns,
                          "years": yrs, "source_months": len(months),
                          "fin_checks": len(fin_rows)})
    print(f"\n-> {md_path}   FAIL {fails}개 · WARN {warns}개")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
