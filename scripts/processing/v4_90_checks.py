# -*- coding: utf-8 -*-
r"""
v4_90_checks.py — v4 무역 팩트 무결성 검증. 결과를 90_checks.md 로 쓰고,
하나라도 FAIL 이면 exit 1 (v4_build_all 이 여기서 멈추도록).

원칙: "통과"를 자칭하지 않고 원천과 직접 대사한다. 실패는 실패로 적는다.
연도 파일 단위로 검사해 메모리를 묶는다 (분기가 연도를 걸치지 않으므로 등가).

11번: full 빌드의 2024 조각은 동결된 v4_pairhs_2024 벤치마크와 **완전 동일**해야 한다
      (같은 원천을 읽으므로). 이 검사가 통과하면 재작성된 빌드 로직 전체가 검증된다.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from v4_common import DEFAULT_TRADE_SRC, OUT_2024, OUT_FULL, discover_months

R = []


def chk(no, name, ok, detail):
    R.append({"no": no, "check": name, "result": "PASS" if ok else "**FAIL**", "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {no}. {name} — {detail}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUT_FULL), help="검사할 v4 산출 폴더")
    ap.add_argument("--src", nargs="*", default=None)
    ap.add_argument("--benchmark", default=str(OUT_2024 / "trade_pair_hs_quarter_2024.parquet"),
                    help="2024 동결본 경로 ('' 이면 11번 생략)")
    a = ap.parse_args()
    out = Path(a.out)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC

    files = sorted(out.glob("trade_pair_hs_quarter_*.parquet"))
    acct = pd.read_csv(out / "00_drop_accounting.csv")
    months = discover_months(src_dirs)
    print(f"검사 대상: 연도 파일 {len(files)}개 · 원천 {len(months)}개월")

    # ---- 원천 총계 (선적수·금액) ----
    src_n, src_v = 0, 0.0
    for ym, p in months.items():
        d = pd.read_parquet(p, columns=["valueofgoodsusd"])
        src_n += len(d)
        src_v += float(d["valueofgoodsusd"].fillna(0.0).sum())
    an, av = int(acct["n_shipments"].sum()), float(acct["value_usd"].sum())
    chk(1, "버림회계 4버킷 합 = 원천 총계", an == src_n and abs(av - src_v) < 1.0,
        f"선적 {an:,} vs 원천 {src_n:,} (차 {an - src_n}) · 금액 차 ${av - src_v:,.2f}")

    kept = acct[acct.bucket == "kept_both"]
    kn, kv = int(kept["n_shipments"].sum()), float(kept["value_usd"].sum())

    # ---- 연도 파일 순회 검사 ----
    K = ["trade_quarter", "shp_panjivaid", "con_panjivaid", "hs6", "hs_status"]
    tot_n = tot_ship = 0
    tot_v = 0.0
    dup = bad4 = bad5 = bad8b = v6a = v6b = v81 = v82 = v83 = neg = nul = 0
    rollup_bad = 0.0
    qstats = []
    for p in files:
        f = pd.read_parquet(p)
        tot_n += len(f)
        tot_ship += int(f["n_shipments"].sum())
        tot_v += float(f["value_usd"].sum())
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
        v6a += int((f["hs_status"].ne("single") & f["hs6"].notna()).sum())
        v6b += int((f["hs_status"].eq("single") & f["hs6"].isna()).sum())
        m = f[f.match_status == "both"]
        av_ = m["value_usd"].sum()
        bv = m.groupby(["trade_quarter", "shp_ciqid", "con_ciqid", "hs6", "hs_status"],
                       dropna=False)["value_usd"].sum().sum()
        cv = m.groupby(["trade_quarter", "shp_up", "con_up", "hs6", "hs_status"],
                       dropna=False)["value_usd"].sum().sum()
        rollup_bad = max(rollup_bad, abs(av_ - bv), abs(av_ - cv))
        both_up = f.shp_up.notna() & f.con_up.notna()
        v81 += int((~both_up & f.is_intra_group.notna()).sum())
        v82 += int((both_up & f.is_intra_group.isna()).sum())
        v83 += int(((f.is_self == 1) & f.is_intra_group.notna() & (f.is_intra_group != 1)).sum())
        neg += int((f[["n_shipments", "value_usd"]] < 0).any(axis=1).sum())
        nul += int(f["n_shipments"].isna().sum())
        qstats.append(f.groupby("trade_quarter").agg(rows=("value_usd", "size"),
                                                     value=("value_usd", "sum"),
                                                     ships=("n_shipments", "sum")))
        del f

    chk(2, "팩트 선적수·금액 = kept_both", tot_ship == kn and abs(tot_v - kv) < 1.0,
        f"선적 {tot_ship:,} vs {kn:,} · 금액 차 ${tot_v - kv:,.2f}")
    chk(3, "grain 유일 (분기 x shp x con x hs6 x hs_status)", dup == 0, f"중복 {dup}행")
    chk(4, "panjivaid -> ciqid 가 분기 내 1:1", bad4 == 0, f"위반 {bad4}건")
    chk(5, "UP 이 (panjivaid, 분기) 단위로 단일", bad5 == 0, f"위반 {bad5}건")
    chk(6, "hs6 는 hs_status=single 일 때만 존재", v6a == 0 and v6b == 0,
        f"위반 {v6a}/{v6b}")
    chk(7, "ciqid·UP 단위 roll-up 시 금액 보존", rollup_bad < 1.0,
        f"최대 오차 ${rollup_bad:.4f}")
    chk(8, "is_intra_group 은 양측 UP 있을 때만 · self 는 항상 intra",
        v81 == 0 and v82 == 0 and v83 == 0, f"위반 {v81}/{v82}/{v83}")
    chk("8b", "같은 ciqid 는 분기 내 UP 이 하나 (양측 통합)", bad8b == 0, f"위반 {bad8b}건")
    chk(9, "측정값에 음수·결측 없음", neg == 0 and nul == 0, f"음수 {neg} · 결측 {nul}")

    q = pd.concat(qstats).groupby(level=0).sum().sort_index()
    ok10 = bool((q["rows"] > 50_000).all() and (q["value"] > 2e10).all())
    chk(10, "분기별 규모가 정상 범위 (행>5만 · 금액>$20B)", ok10,
        f"{len(q)}개 분기 · 최소 {q['rows'].min():,}행 / ${q['value'].min()/1e9:.0f}B")

    # ---- 11. 2024 벤치마크 대사 ----
    bench = Path(a.benchmark) if a.benchmark else None
    p2024 = out / "trade_pair_hs_quarter_2024.parquet"
    if bench and bench.exists() and p2024.exists() and bench.resolve() != p2024.resolve():
        aa = pd.read_parquet(bench).sort_values(K).reset_index(drop=True)
        bb = pd.read_parquet(p2024).sort_values(K).reset_index(drop=True)
        chk(11, "2024 조각 = 동결 벤치마크와 완전 동일", aa.equals(bb),
            f"{len(aa):,} vs {len(bb):,}행 · frame equals={aa.equals(bb)}")
        del aa, bb

    # ---- 문서 ----
    md = [f"# v4 무역 팩트 검증 ({out.name})", "",
          f"**검증일**: {pd.Timestamp.now():%Y-%m-%d} · 연도 파일 {len(files)}개 · "
          f"총 {tot_n:,}행 · **스크립트**: `scripts\\processing\\v4_90_checks.py`", "",
          "## 검증 결과", "", "| # | 검사 | 결과 | 상세 |", "|---|---|---|---|"]
    for r in R:
        md.append(f"| {r['no']} | {r['check']} | {r['result']} | {r['detail']} |")
    md += ["", "## 참고 — 분기별 규모 (판정 아님)", "",
           "| 분기 | 행 | 선적 | 금액($B) |", "|---|---|---|---|"]
    for i, r in q.iterrows():
        md.append(f"| {i} | {r.rows:,.0f} | {r.ships:,.0f} | {r.value/1e9:,.1f} |")
    (out / "90_checks.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    fails = sum(1 for r in R if "FAIL" in r["result"])
    print(f"\n-> {out / '90_checks.md'}   FAIL {fails}개")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
