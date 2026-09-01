# -*- coding: utf-8 -*-
r"""
v4_91_guide_stats.py — 가이드 메모(README §2-②③④, §1)용 통계를 원천 전수로 계산한다.
결과: <out>\91_guide_stats.json (메모 작성용 원자료) — README 의 수치는 여기서 옮겨 적는다.

파이프라인 필수 단계가 아니다 — 문서 갱신이 필요할 때만 돌린다 (전 기간이면 20~30분,
`--years 2024 2024` 면 1~2분). build_all 의 7단계(--with-guide).

재는 것
  layers      당사자 결측 두 층 (은폐 / CIQ 미등재 / 매칭) — 행·금액 비중, 수입자·수출자
  by_bl       B/L 유형별 은폐 비중 (House 는 수입자, Simple 은 수출자가 주로 지워짐)
  by_origin   원산지 상위 15개국의 수출자 미매칭률
  con_decile  수입자 금액 십분위별 매칭률 (대기업 편향)
  up_history  무역에 등장한 ciqid 의 PIT 이력 — 다중 구간 · flip-flop (같은 UP 재등장) 비중
              flip_flop = 이력에 같은 UP 이 두 번 이상 (인접 중복 포함, 구판 정의)
              flip_flop_strict = A→B→A 처럼 다른 UP 을 거쳐 되돌아온 경우만
  fin_period_gap  (--trade-dir 에 무역 팩트, --fin-dir 에 ciq_fin_wide_quarter 가 있을 때)
              수입자 모회사 분기 재무를 붙였을 때 결산일 − 분기시작 간격(일) 의 중앙값·5/95% ·
              회계분기 라벨 ≠ 달력분기 비중 (README §1 의 "중앙값 90일 · 28%")

사용:
    python v4_91_guide_stats.py                                   # 전 기간 -> v4_pairhs_full
    python v4_91_guide_stats.py --years 2024 2024 --out <dir> --trade-dir <dir> --fin-dir <dir>
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from v4_common import CIQ_REF as CIQ
from v4_common import DEFAULT_TRADE_SRC, OUT_FULL, discover_months, write_manifest

COLS = ["conpanjivaid", "con_ciqid_original", "shppanjivaid", "shp_ciqid_original",
        "shpmtorigin", "valueofgoodsusd", "billofladingtype"]


def strict_flip(seq):
    """A→B→A: 어떤 UP 이 다른 UP 을 사이에 두고 다시 나타나는가."""
    seen, cur = set(), None
    for u in seq:
        if u == cur:
            continue
        if u in seen:
            return True
        seen.add(u)
        cur = u
    return False


def fin_period_gap(trade_dir, fin_dir, years):
    """수입자 모회사 분기 재무 부착 시 결산일 간격·회계분기 어긋남 (README §1 근거)."""
    tfiles = sorted(Path(trade_dir).glob("trade_pair_hs_quarter_*.parquet"))
    if years:
        tfiles = [p for p in tfiles if years[0] <= int(p.stem.split("_")[-1]) <= years[1]]
    wq = Path(fin_dir) / "ciq_fin_wide_quarter.parquet"
    if not tfiles or not wq.exists():
        return None
    w = pd.read_parquet(wq, columns=["companyid", "cal_year", "cal_quarter", "period_end", "fiscal_quarter"])
    w = w.rename(columns={"companyid": "con_up"})
    gaps, mism, n = [], 0, 0
    for p in tfiles:
        t = pd.read_parquet(p, columns=["con_up", "cal_year", "cal_quarter", "quarter_start_date"])
        t = t.dropna(subset=["con_up"])
        t["con_up"] = t["con_up"].astype("int64")
        j = t.merge(w, on=["con_up", "cal_year", "cal_quarter"], how="inner")
        if not len(j):
            continue
        g = (j["period_end"].astype("datetime64[ns]") - j["quarter_start_date"].astype("datetime64[ns]")).dt.days
        gaps.append(g.to_numpy(dtype="int32"))
        mism += int((j["fiscal_quarter"] != j["cal_quarter"]).sum())
        n += len(j)
        del t, j
    if not gaps:
        return None
    g = np.concatenate(gaps)
    return {"rows_joined": int(n), "gap_days_median": float(np.median(g)),
            "gap_days_p05": float(np.percentile(g, 5)), "gap_days_p95": float(np.percentile(g, 95)),
            "gap_days_min": int(g.min()), "gap_days_max": int(g.max()),
            "fiscal_ne_cal_quarter_pct": mism / n * 100}


def main():
    ap = argparse.ArgumentParser(description="v4 가이드 메모용 통계 (원천 전수)")
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--src", nargs="*", default=None)
    ap.add_argument("--years", nargs=2, type=int, default=None, help="이 범위 연도의 원천 월만 (기본 전 기간)")
    ap.add_argument("--trade-dir", default=str(OUT_FULL), help="fin_period_gap 용 무역 팩트 폴더")
    ap.add_argument("--fin-dir", default=str(OUT_FULL), help="fin_period_gap 용 재무 wide 폴더")
    a = ap.parse_args()
    OUT = Path(a.out)
    OUT.mkdir(parents=True, exist_ok=True)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC
    years = tuple(a.years) if a.years else None
    paths = discover_months(src_dirs, years=years)
    if not paths:
        raise SystemExit(f"원천 월 없음: {src_dirs} years={years}")
    months = list(paths)
    print(f"원천 {len(months)}개월 ({months[0]}~{months[-1]})")
    tot_n = 0
    tot_v = 0.0
    lay = {s: {k: [0, 0.0] for k in ["blank", "no_ciq", "matched"]} for s in ["con", "shp"]}
    bl = []
    org = []
    con_ent = []
    seen = set()

    for ym in months:
        print(f"  {ym}", flush=True)
        d = pd.read_parquet(paths[ym], columns=COLS)
        d["valueofgoodsusd"] = d["valueofgoodsusd"].fillna(0.0)
        tot_n += len(d)
        tot_v += d["valueofgoodsusd"].sum()

        for s in ["con", "shp"]:
            pid, cid = f"{s}panjivaid", f"{s}_ciqid_original"
            masks = {"blank": d[pid].isna(),
                     "no_ciq": d[pid].notna() & d[cid].isna(),
                     "matched": d[cid].notna()}
            for k, m in masks.items():
                lay[s][k][0] += int(m.sum())
                lay[s][k][1] += float(d.loc[m, "valueofgoodsusd"].sum())
            seen |= set(d[cid].dropna().astype("int64").unique())

        bl.append(d.assign(cb=d.conpanjivaid.isna(), sb=d.shppanjivaid.isna())
                   .groupby("billofladingtype", dropna=False)[["cb", "sb"]]
                   .agg(["sum", "size"]))
        org.append(d.assign(miss=d.shp_ciqid_original.isna())
                    .groupby("shpmtorigin", dropna=False)
                    .agg(n=("miss", "size"), miss=("miss", "sum"),
                         val=("valueofgoodsusd", "sum")))
        con_ent.append(d.dropna(subset=["conpanjivaid"])
                        .groupby("conpanjivaid")
                        .agg(val=("valueofgoodsusd", "sum"),
                             mat=("con_ciqid_original", "max")))
        del d

    res = {"measured": pd.Timestamp.now().strftime("%Y-%m-%d"),
           "months": [months[0], months[-1]], "n_months": len(months),
           "total_shipments": tot_n, "total_value": tot_v, "layers": {}}
    for s in ["con", "shp"]:
        res["layers"][s] = {k: {"n": v[0], "n_pct": v[0] / tot_n * 100,
                                "value": v[1], "value_pct": v[1] / tot_v * 100}
                            for k, v in lay[s].items()}

    b = pd.concat(bl).groupby(level=0).sum()
    res["by_bl"] = {str(i): {"con_blank_pct": r[("cb", "sum")] / r[("cb", "size")] * 100,
                             "shp_blank_pct": r[("sb", "sum")] / r[("sb", "size")] * 100,
                             "n": int(r[("cb", "size")])} for i, r in b.iterrows()}

    o = pd.concat(org).groupby(level=0).sum()
    o["miss_pct"] = o["miss"] / o["n"] * 100
    o["n_pct"] = o["n"] / tot_n * 100
    res["by_origin"] = {str(i): {"n": int(r.n), "n_pct": r.n_pct, "miss_pct": r.miss_pct, "value": r.val}
                        for i, r in o.nlargest(15, "n").iterrows()}

    ce = pd.concat(con_ent).groupby(level=0).agg(val=("val", "sum"), mat=("mat", "max"))
    ce["dec"] = pd.qcut(ce["val"].rank(method="first"), 10, labels=range(1, 11))
    dd = ce.groupby("dec", observed=True).agg(
        firms=("mat", "size"), match=("mat", lambda x: x.notna().mean() * 100),
        value=("val", "sum"))
    dd["value_pct"] = dd["value"] / dd["value"].sum() * 100
    res["con_decile"] = {int(i): {"firms": int(r.firms), "match_pct": r.match,
                                  "value": r.value, "value_pct": r.value_pct} for i, r in dd.iterrows()}
    res["con_match_rows_pct"] = res["layers"]["con"]["matched"]["n_pct"]
    res["con_match_value_pct"] = res["layers"]["con"]["matched"]["value_pct"]

    # UP flip-flop — 무역에 등장한 ciqid 의 PIT 이력을 분류 (pyarrow 로 걸러 읽어 메모리 절약)
    print("  PIT 이력 분류 ...", flush=True)
    t = pq.read_table(CIQ / "ownership_pit.parquet",
                      columns=["companyid", "ultimate_parent_companyid", "start_date"])
    m = pc.is_in(t["companyid"], value_set=pa.array(sorted(seen), type=t["companyid"].type))
    p = t.filter(m).to_pandas().sort_values(["companyid", "start_date"])
    del t, m
    g = p.groupby("companyid")["ultimate_parent_companyid"].agg(list)
    multi = g[g.map(len) > 1]
    ff = multi.map(lambda s: len(s) != len(set(s)))
    ffs = multi.map(strict_flip)
    res["up_history"] = {"companies_in_trade": len(seen),
                         "with_pit": int(len(g)),
                         "with_pit_pct": len(g) / len(seen) * 100 if seen else np.nan,
                         "multi_record": int(len(multi)),
                         "flip_flop": int(ff.sum()),
                         "flip_flop_pct_of_multi": float(ff.mean() * 100) if len(multi) else np.nan,
                         "flip_flop_strict": int(ffs.sum()),
                         "flip_flop_strict_pct_of_multi": float(ffs.mean() * 100) if len(multi) else np.nan}
    del p, g, multi

    gap = fin_period_gap(a.trade_dir, a.fin_dir, years)
    res["fin_period_gap"] = gap

    jp = OUT / "91_guide_stats.json"
    jp.write_text(json.dumps(res, indent=2, ensure_ascii=False, default=float), encoding="utf-8")
    write_manifest(OUT, "guide_stats", inputs=list(paths.values()) + [CIQ / "ownership_pit.parquet"],
                   outputs=[jp], extra={"months": len(months), "years": years})
    print(json.dumps(res["layers"], indent=2, ensure_ascii=False))
    print(json.dumps(res["up_history"], indent=2, ensure_ascii=False))
    print(json.dumps(res["fin_period_gap"], indent=2, ensure_ascii=False))
    print(f"-> {jp}")


if __name__ == "__main__":
    main()
