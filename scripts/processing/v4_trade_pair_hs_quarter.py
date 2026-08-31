# -*- coding: utf-8 -*-
r"""
v4_trade_pair_hs_quarter.py — v4 무역 팩트 (분기 x 수출자 x 수입자 x HS6)

목적: CIQ 재무를 **붙이지 않은 채** 키만 들고 있는 무역 테이블. 나중에 모회사 companyId 로
      양쪽에 연별·분기별 재무를 equi-join 할 수 있게 한다. (v1·v2·v3 와 별개 — 기존은 손대지 않음)

원천 : data\staging\source\trade_2024\imp_ship_YYYYMM.parquet (12개월)
산출 : --out 인자 (기본 data\staging\v4_pairhs_full\)
         trade_pair_hs_quarter_2024.parquet   분기 x shp x con x hs6 = 1행
         00_drop_accounting.csv               버린 행 회계 (원천 총계 대사용)

확정 결정 (2026-08-21 싱크):
  D1 grain    = panjivaid 쌍 x hs6. panjivaid -> ciqid 가 1:1(Q1 위반 0건)이라
                ciqid·UP 단위로 손실 없이 roll-up 가능. ciqid·up 은 값으로 동승.
  D2 HS       = imp_hs 자식 안 씀. n_hs6==1 일 때만 실제 코드를 넣고,
                다중(multi)·부재(missing)는 hs6 를 비우고 hs_status 로 이유를 구분.
                -> HS별 금액이 오염되지 않으면서 쌍 총액은 보존된다.
  D3 redaction= 양쪽 panjivaid 가 다 있는 행만 남긴다(쌍 미성립분은 버림).
                버린 비중은 00_drop_accounting.csv 와 가이드 메모에 기록.
  D4 UP       = 분기 내 UP 이 흔들리면 **금액가중 최빈값**. up_changed_in_quarter 플래그 유지.
                ★ UP 은 (panjivaid, 분기) 단위로 한 번 정해 모든 행에 뿌린다.
                  행마다(=HS별로) 따로 구하면 같은 회사가 HS 별로 다른 UP 을 갖게 된다.

메모리: 월 단위로 부분집계를 만들어 누적한다(전부 합계형이라 결합 가능). 분기 통째 로드 안 함.
      **연도 단위로 저장하고 비운다** — 전 기간(38M행)을 쌓으면 concat 피크 19GB 로 공용 머신에
      민폐다(실측). 연도별로 쓰면 peak 1.3GB.

기간 하드코딩 없음: 원천 폴더(들)에 실제로 있는 imp_ship_YYYYMM.parquet 을 discover 한다.
같은 월이 두 폴더에 있으면 앞 폴더(trade_2024, 검증 완료본)가 이긴다.

사용:
    python v4_trade_pair_hs_quarter.py                          # 원천 전체 -> v4_pairhs_full
    python v4_trade_pair_hs_quarter.py --years 2024 2024 --out <dir>   # 부분 재실행
"""

import argparse
import gc
from pathlib import Path

import numpy as np
import pandas as pd

from v4_common import (DEFAULT_TRADE_SRC, OUT_FULL, discover_months, to_quarters,
                       write_manifest)

PATHS = {}          # {YYYYMM: 원천 파일 경로} — main 에서 채움

READ_COLS = [
    "panjivarecordid", "billofladingtype", "shpmtorigin",
    "conname", "conpanjivaid", "concountry", "con_ciqid_original", "con_up",
    "con_ownership_is_fallback",
    "shpname", "shppanjivaid", "shpcountry", "shp_ciqid_original", "shp_up",
    "shp_ownership_is_fallback",
    "hs6", "hs2", "n_hs6",
    "valueofgoodsusd", "weightkg", "volumeteu", "numberofcontainers",
]

RENAME = {
    "conname": "con_name", "conpanjivaid": "con_panjivaid", "concountry": "con_country",
    "con_ciqid_original": "con_ciqid", "con_ownership_is_fallback": "con_fb",
    "shpname": "shp_name", "shppanjivaid": "shp_panjivaid", "shpcountry": "shp_country",
    "shp_ciqid_original": "shp_ciqid", "shp_ownership_is_fallback": "shp_fb",
}

FACT_KEYS = ["shp_panjivaid", "con_panjivaid", "hs6", "hs_status"]

SUMS = ["n_shipments", "value_usd", "weight_kg", "teu", "n_containers",
        "n_bl_house", "n_bl_simple"]


def pick_mode(part, keys, valcol):
    """금액가중 최빈값. 동률은 선적수 -> 값 오름차순으로 결정적으로 깬다."""
    g = part.dropna(subset=[valcol])
    if not len(g):
        return pd.DataFrame(columns=keys + [valcol])
    g = g.groupby(keys + [valcol], dropna=False, as_index=False)[["w", "n"]].sum()
    g = g.sort_values(["w", "n", valcol], ascending=[False, False, True], kind="mergesort")
    return g.drop_duplicates(keys)[keys + [valcol]]


def month_partials(ym):
    """월 하나를 읽어 부분집계 + 버림 회계를 낸다."""
    df = pd.read_parquet(PATHS[ym], columns=READ_COLS).rename(columns=RENAME)
    df["valueofgoodsusd"] = df["valueofgoodsusd"].fillna(0.0)

    has_con, has_shp = df["con_panjivaid"].notna(), df["shp_panjivaid"].notna()
    acct = pd.DataFrame([
        {"bucket": label, "n_shipments": int(m.sum()),
         "value_usd": float(df.loc[m, "valueofgoodsusd"].sum())}
        for label, m in [("kept_both", has_con & has_shp),
                         ("drop_con_only", has_con & ~has_shp),
                         ("drop_shp_only", ~has_con & has_shp),
                         ("drop_neither", ~has_con & ~has_shp)]])

    d = df[has_con & has_shp].copy()
    del df
    for c in ["con_panjivaid", "shp_panjivaid"]:
        d[c] = d[c].astype("int64")

    # D2 — 단일 HS 일 때만 코드를 남긴다
    nh = d["n_hs6"].fillna(0)
    d["hs_status"] = np.where(nh == 1, "single", np.where(nh > 1, "multi", "missing"))
    d.loc[d["hs_status"] != "single", ["hs6", "hs2"]] = np.nan

    d["w"] = d["valueofgoodsusd"]
    d["n"] = 1
    # int16 합계가 무음 오버플로 나지 않게 승격 (분기 그룹합 실측 최대 ~3.3천, 한계 32,767)
    d["numberofcontainers"] = d["numberofcontainers"].astype("int64")
    d["_house"] = (d["billofladingtype"] == "House").astype("int64")
    d["_simple"] = (d["billofladingtype"] == "Simple").astype("int64")

    fact = d.groupby(FACT_KEYS, dropna=False, as_index=False).agg(
        n_shipments=("panjivarecordid", "size"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
        n_containers=("numberofcontainers", "sum"),
        n_bl_house=("_house", "sum"),
        n_bl_simple=("_simple", "sum"),
    )
    # hs2 는 hs6 에 함수적으로 종속 — 별도 매핑으로 보관
    hs2map = d.loc[d["hs_status"] == "single", ["hs6", "hs2"]].drop_duplicates()

    origin = d.groupby(FACT_KEYS + ["shpmtorigin"], dropna=False, as_index=False)[["w", "n"]].sum()

    ents = {}
    for s in ["shp", "con"]:
        ents[f"{s}_id"] = d.groupby([f"{s}_panjivaid", f"{s}_ciqid", f"{s}_up"],
                                    dropna=False, as_index=False).agg(
            w=("w", "sum"), n=("n", "sum"), fb=(f"{s}_fb", "sum"))
        ents[f"{s}_nm"] = d.groupby([f"{s}_panjivaid", f"{s}_name"],
                                    dropna=False, as_index=False)[["w", "n"]].sum()
        ents[f"{s}_ct"] = d.groupby([f"{s}_panjivaid", f"{s}_country"],
                                    dropna=False, as_index=False)[["w", "n"]].sum()
    return acct, fact, hs2map, origin, ents


def build_quarter(q, months):
    acc = {k: [] for k in ["acct", "fact", "hs2", "origin",
                           "shp_id", "shp_nm", "shp_ct", "con_id", "con_nm", "con_ct"]}
    for ym in months:
        print(f"    {ym} ...", end="", flush=True)
        a, f, h, o, e = month_partials(ym)
        acc["acct"].append(a); acc["fact"].append(f)
        acc["hs2"].append(h); acc["origin"].append(o)
        for k, v in e.items():
            acc[k].append(v)
        print(f" fact {len(f):,}")

    acct = pd.concat(acc["acct"]).groupby("bucket", as_index=False).sum()
    acct.insert(0, "trade_quarter", q)

    fact = pd.concat(acc["fact"]).groupby(FACT_KEYS, dropna=False, as_index=False)[SUMS].sum()

    # 원산지 대표값 + 개수
    origin = pd.concat(acc["origin"]).groupby(FACT_KEYS + ["shpmtorigin"], dropna=False,
                                              as_index=False)[["w", "n"]].sum()
    top_origin = pick_mode(origin, FACT_KEYS, "shpmtorigin").rename(
        columns={"shpmtorigin": "top_origin"})
    n_origin = (origin.dropna(subset=["shpmtorigin"])
                .groupby(FACT_KEYS, dropna=False, as_index=False)["shpmtorigin"].nunique()
                .rename(columns={"shpmtorigin": "n_origin"}))
    fact = fact.merge(top_origin, on=FACT_KEYS, how="left").merge(n_origin, on=FACT_KEYS, how="left")
    fact["n_origin"] = fact["n_origin"].fillna(0).astype("int64")

    hs2 = pd.concat(acc["hs2"]).dropna().drop_duplicates("hs6")
    fact = fact.merge(hs2, on="hs6", how="left")

    # 엔티티 해소 1 — ciqid·이름·국가는 panjivaid 의 속성이므로 panjivaid 단위로 정한다
    ids_by_side = {}
    for s in ["shp", "con"]:
        pid = f"{s}_panjivaid"
        ids = pd.concat(acc[f"{s}_id"]).groupby([pid, f"{s}_ciqid", f"{s}_up"],
                                                dropna=False, as_index=False)[["w", "n", "fb"]].sum()
        ids_by_side[s] = ids
        ciq = pick_mode(ids, [pid], f"{s}_ciqid")
        nm = pick_mode(pd.concat(acc[f"{s}_nm"]).groupby([pid, f"{s}_name"], dropna=False,
                                                         as_index=False)[["w", "n"]].sum(),
                       [pid], f"{s}_name")
        ct = pick_mode(pd.concat(acc[f"{s}_ct"]).groupby([pid, f"{s}_country"], dropna=False,
                                                         as_index=False)[["w", "n"]].sum(),
                       [pid], f"{s}_country")
        ent = (ciq.merge(nm, on=pid, how="outer").merge(ct, on=pid, how="outer"))
        fact = fact.merge(ent, on=pid, how="left")

    # 엔티티 해소 2 (D4) — UP 은 **ciqid 단위**로, 수출자·수입자 관측을 합쳐 한 번만 정한다.
    #   UP 은 CIQ 회사의 속성이지 panjivaid 나 거래방향의 속성이 아니다. 방향별·panjivaid 별로
    #   따로 정하면 같은 회사가 수출자일 때와 수입자일 때 다른 모회사를 갖는다(초기판 실측 48건).
    #   UP 은 ciqid 가 있어야만 존재하므로(원천 SQL 이 crosswalk 를 거쳐 조인) 별도 fallback 불필요.
    pool = pd.concat([ids_by_side[s].rename(columns={f"{s}_ciqid": "ciqid", f"{s}_up": "up"})
                      [["ciqid", "up", "w", "n", "fb"]] for s in ["shp", "con"]])
    pool = (pool.dropna(subset=["ciqid"])
                .groupby(["ciqid", "up"], dropna=False, as_index=False)[["w", "n", "fb"]].sum())
    updim = pick_mode(pool, ["ciqid"], "up")
    chg = (pool.dropna(subset=["up"]).groupby("ciqid", as_index=False)["up"].nunique()
           .rename(columns={"up": "up_changed"}))
    chg["up_changed"] = (chg["up_changed"] > 1).astype("int8")
    tot = pool.groupby("ciqid", as_index=False)[["n", "fb"]].sum()
    tot["up_fallback_share"] = tot["fb"] / tot["n"]
    updim = (updim.merge(chg, on="ciqid", how="outer")
                  .merge(tot[["ciqid", "up_fallback_share"]], on="ciqid", how="outer"))
    for s in ["shp", "con"]:
        fact = fact.merge(updim.rename(columns={
            "ciqid": f"{s}_ciqid", "up": f"{s}_up", "up_changed": f"{s}_up_changed",
            "up_fallback_share": f"{s}_up_fallback_share"}), on=f"{s}_ciqid", how="left")
        fact[f"{s}_up_changed"] = fact[f"{s}_up_changed"].fillna(0).astype("int8")

    # 시점 키 + 판정 컬럼
    fact = fact.reset_index(drop=True)
    per = pd.Period(q, freq="Q")
    fact.insert(0, "trade_quarter", q)
    fact.insert(1, "cal_year", per.year)
    fact.insert(2, "cal_quarter", per.quarter)
    fact.insert(3, "quarter_start_date", per.start_time)

    cn, sn = fact["con_ciqid"].notna(), fact["shp_ciqid"].notna()
    fact["match_status"] = np.select([cn & sn, cn & ~sn, ~cn & sn],
                                     ["both", "con_only", "shp_only"], default="neither")
    # is_self 는 ciqid 로, is_intra_group 은 UP 으로 판정한다.
    # ciqid 는 붙었는데 UP 이 원천에 없는 행이 소수 있어(2024 전체 6행) 그 경우 intra 는 판정 불가.
    both_id = (cn & sn).to_numpy()
    both_up = (fact["shp_up"].notna() & fact["con_up"].notna()).to_numpy()
    eq_up = (fact["shp_up"] == fact["con_up"]).fillna(False).astype(bool).to_numpy()
    eq_id = (fact["shp_ciqid"] == fact["con_ciqid"]).fillna(False).astype(bool).to_numpy()
    fact["is_intra_group"] = pd.Series(np.where(both_up, eq_up, np.nan)).astype("Float64").astype("Int8")
    fact["is_self"] = pd.Series(np.where(both_id, eq_id, np.nan)).astype("Float64").astype("Int8")

    for c in ["shp_ciqid", "shp_up", "con_ciqid", "con_up"]:
        fact[c] = fact[c].astype("Int64")
    return acct, fact


ORDER = ["trade_quarter", "cal_year", "cal_quarter", "quarter_start_date",
         "shp_panjivaid", "shp_ciqid", "shp_up", "shp_name", "shp_country",
         "shp_up_changed", "shp_up_fallback_share",
         "con_panjivaid", "con_ciqid", "con_up", "con_name", "con_country",
         "con_up_changed", "con_up_fallback_share",
         "hs6", "hs2", "hs_status",
         "n_shipments", "value_usd", "weight_kg", "teu", "n_containers",
         "n_bl_house", "n_bl_simple", "top_origin", "n_origin",
         "match_status", "is_intra_group", "is_self"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", nargs="*", default=None,
                    help="원천 폴더(들). 기본: trade_2024 + trade_hist (앞이 우선)")
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--years", nargs=2, type=int, default=None,
                    help="이 범위 연도만 (부분 재실행)")
    args = ap.parse_args()

    src_dirs = [Path(s) for s in args.src] if args.src else DEFAULT_TRADE_SRC
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    global PATHS
    PATHS = discover_months(src_dirs)
    if args.years:
        PATHS = {ym: p for ym, p in PATHS.items()
                 if args.years[0] <= int(ym[:4]) <= args.years[1]}
    if not PATHS:
        raise SystemExit(f"원천에 월이 없음: {src_dirs}")
    quarters = to_quarters(PATHS)
    years = sorted({q[:4] for q in quarters})
    print(f"원천 {len(PATHS)}개월 ({min(PATHS)}~{max(PATHS)}) -> 연도 {len(years)}개 · 출력 {out}")

    accts, total, outputs = [], 0, []
    for yr in years:                              # 연도 단위로 빌드-저장-해제 (메모리 상한 고정)
        facts = []
        for q in [q for q in quarters if q.startswith(yr)]:
            print(f"[{q}]")
            a, f = build_quarter(q, quarters[q])
            print(f"  -> {q} 출력 {len(f):,}행")
            accts.append(a)
            facts.append(f)
        ydf = pd.concat(facts, ignore_index=True)[ORDER]
        p = out / f"trade_pair_hs_quarter_{yr}.parquet"
        ydf.to_parquet(p, index=False, compression="zstd")
        outputs.append(p)
        total += len(ydf)
        print(f"  == {yr}: {len(ydf):,}행 -> {p.name}")
        del facts, ydf
        gc.collect()

    pd.concat(accts, ignore_index=True).to_csv(out / "00_drop_accounting.csv", index=False)
    write_manifest(out, "trade_build", inputs=PATHS.values(),
                   outputs=outputs + [out / "00_drop_accounting.csv"],
                   extra={"months": len(PATHS), "rows_total": total})
    print(f"\n완료: 총 {total:,}행 · 연도 파일 {len(outputs)}개 -> {out}")


if __name__ == "__main__":
    main()
