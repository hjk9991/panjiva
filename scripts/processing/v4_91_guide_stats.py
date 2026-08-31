# -*- coding: utf-8 -*-
r"""
v4_91_guide_stats.py — 가이드 메모(README §3)용 통계를 원천 전수로 계산한다.
결과: <out>\91_guide_stats.json (메모 작성용 원자료)

파이프라인 필수 단계가 아니다 — 문서 갱신이 필요할 때만 돌린다 (전 기간이면 20~30분).
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from v4_common import CIQ_REF as CIQ
from v4_common import DEFAULT_TRADE_SRC, OUT_FULL, discover_months

COLS = ["conpanjivaid", "con_ciqid_original", "shppanjivaid", "shp_ciqid_original",
        "shpmtorigin", "valueofgoodsusd", "billofladingtype"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--src", nargs="*", default=None)
    a = ap.parse_args()
    global OUT
    OUT = Path(a.out)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC
    paths = discover_months(src_dirs)
    months = list(paths)
    tot_n = 0
    tot_v = 0.0
    lay = {s: {k: [0, 0.0] for k in ["blank", "no_ciq", "matched"]} for s in ["con", "shp"]}
    bl = []
    org = []
    con_ent = []

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

    res = {"total_shipments": tot_n, "total_value": tot_v, "layers": {}}
    for s in ["con", "shp"]:
        res["layers"][s] = {k: {"n": v[0], "n_pct": v[0] / tot_n * 100,
                                "value": v[1], "value_pct": v[1] / tot_v * 100}
                            for k, v in lay[s].items()}

    b = pd.concat(bl).groupby(level=0).sum()
    res["by_bl"] = {i: {"con_blank_pct": r[("cb", "sum")] / r[("cb", "size")] * 100,
                        "shp_blank_pct": r[("sb", "sum")] / r[("sb", "size")] * 100,
                        "n": int(r[("cb", "size")])} for i, r in b.iterrows()}

    o = pd.concat(org).groupby(level=0).sum()
    o["miss_pct"] = o["miss"] / o["n"] * 100
    res["by_origin"] = {str(i): {"n": int(r.n), "miss_pct": r.miss_pct, "value": r.val}
                        for i, r in o.nlargest(15, "n").iterrows()}

    ce = pd.concat(con_ent).groupby(level=0).agg(val=("val", "sum"), mat=("mat", "max"))
    ce["dec"] = pd.qcut(ce["val"].rank(method="first"), 10, labels=range(1, 11))
    dd = ce.groupby("dec", observed=True).agg(
        firms=("mat", "size"), match=("mat", lambda x: x.notna().mean() * 100),
        value=("val", "sum"))
    res["con_decile"] = {int(i): {"firms": int(r.firms), "match_pct": r.match,
                                  "value": r.value} for i, r in dd.iterrows()}

    # UP flip-flop — 2024 무역에 등장한 ciqid 의 PIT 이력을 분류
    seen = set()
    for ym in months:
        d = pd.read_parquet(paths[ym],
                            columns=["con_ciqid_original", "shp_ciqid_original"])
        for c in d.columns:
            seen |= set(d[c].dropna().astype("int64").unique())
    pit = pd.read_parquet(CIQ / "ownership_pit.parquet")
    p = pit[pit.companyid.isin(seen)].sort_values(["companyid", "start_date"])
    g = p.groupby("companyid")["ultimate_parent_companyid"].agg(list)
    multi = g[g.map(len) > 1]
    ff = multi.map(lambda s: len(s) != len(set(s)))
    res["up_history"] = {"companies_in_trade": len(seen),
                         "with_pit": int(len(g)),
                         "multi_record": int(len(multi)),
                         "flip_flop": int(ff.sum()),
                         "flip_flop_pct_of_multi": float(ff.mean() * 100)}

    (OUT / "91_guide_stats.json").write_text(json.dumps(res, indent=2, ensure_ascii=False),
                                             encoding="utf-8")
    print(json.dumps(res["layers"], indent=2, ensure_ascii=False))
    print(json.dumps(res["up_history"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
