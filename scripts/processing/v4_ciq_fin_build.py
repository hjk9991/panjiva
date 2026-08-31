# -*- coding: utf-8 -*-
r"""
v4_ciq_fin_build.py — v4 의 CIQ 재무 쪽. **무역과 섞지 않고 따로** 만든다.

결정 근거: v4 폴더 DECISIONS.md §2 (F-1~F-7). 요약:
  F-1 전 계정 long 보관 (계정을 미리 고르지 않는다)
  F-2 원표시통화 저장 + fx_per_usd 동봉 (환산은 **나눗셈** value/fx — v4_join.to_usd 사용)
  F-3 기간 = 무역 연도 ±2 (인자)
  F-4 연간·분기·반기 전부
  F-5 중복은 지우지 않고 is_preferred / is_preferred_year 플래그
      (tie-break: period_end > 정정유형 > 계정수 > id)
  F-6 wide 는 카탈로그 계정만 (전량은 long)
  F-7 조회 대상 = 무역의 ciqid ∪ up 합집합 (모회사 포함 — CLAUDE.md 함정 §7)

메모리: long 을 **원천 연도별로 나눠 즉시 write** 한다 (ciq_fin_long_YYYY.parquet).
  전 기간을 concat 하면 피크 ~29GB (실측 배수 0.97x) 로 공용 머신에 민폐 — 분할 시 ~2GB.
  wide 피벗도 연도별로 하고 작은 결과만 concat 한다.

사용:
    python v4_ciq_fin_build.py                      # trade-dir 의 무역 팩트 기준, 2005~2026
    python v4_ciq_fin_build.py --years 2022 2024    # 기간 제한
"""

import argparse
import gc
import re
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from v4_common import CIQ_REF, OUT_FULL, write_manifest

DOC = Path(r"C:\panjiva\shared memory\ciq_dataitems.md")

# F-5 정정유형 우선순위 (클수록 우선)
REST_RANK = {3: 5, 5: 4, 4: 3, 2: 2, 1: 1}   # Restated > Reclassified > NoChange > Original > PR

FP_COLS = ["financial_period_id", "companyid", "period_type_id", "period_type",
           "period_end", "cal_year", "cal_quarter", "fiscal_year", "fiscal_quarter",
           "currency", "restatement_type_id", "restatement_type", "latest_period_flag",
           "is_restatement_type_id"]


# ---------- 카탈로그 ----------

def parse_catalog():
    """계정 카탈로그. 분류(statement/section)는 md 문서에서, 이름은 ciqDataItem 참조표에서.

    md 는 사람이 쓴 문서라 서식 변화에 약하므로 이름의 정본은 ref_dataitem.parquet 이다
    (없으면 md 이름으로 fallback).
    """
    sec = {"3": "Income Statement", "4": "Balance Sheet", "5": "Cash Flow",
           "6": "Ratios", "7": "Key Stats"}
    cur_sec = cur_sub = None
    rows = []
    for ln in DOC.read_text(encoding="utf-8").split("\n"):
        m = re.match(r"^## (\d+)\.", ln)
        if m:
            cur_sec, cur_sub = sec.get(m.group(1)), None
            continue
        m = re.match(r"^\*\*(.+?)\*\*\s*\(\d+개\)", ln)
        if m and cur_sec:
            cur_sub = m.group(1)
            continue
        m = re.match(r"^\|\s*(\d+)\s*\|\s*([^|]*?)\s*\|\s*([^|]*?)\s*\|", ln)
        if m and cur_sec:
            rows.append({"data_item_id": int(m.group(1)), "item_name": m.group(2).strip(),
                         "item_name_ko": m.group(3).strip(), "statement": cur_sec,
                         "section": cur_sub})
    cat = pd.DataFrame(rows).drop_duplicates("data_item_id").reset_index(drop=True)
    ref = CIQ_REF / "ref_dataitem.parquet"
    if ref.exists():
        names = pd.read_parquet(ref).rename(columns={"dataitemid": "data_item_id",
                                                     "dataitemname": "ref_name"})
        cat = cat.merge(names, on="data_item_id", how="left")
        cat["item_name"] = cat["ref_name"].fillna(cat["item_name"])
        cat = cat.drop(columns="ref_name")
    return cat


def slug(name, iid):
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return f"{s or 'item'}_{iid}"


# ---------- 회계기간 dim ----------

def sample_companies(trade_dir):
    """F-7 — 무역 연도 파일들에서 ciqid ∪ up 합집합."""
    ids = set()
    files = sorted(Path(trade_dir).glob("trade_pair_hs_quarter_*.parquet"))
    if not files:
        raise SystemExit(f"무역 팩트 없음: {trade_dir} — 무역 빌드를 먼저 실행할 것")
    for p in files:
        t = pd.read_parquet(p, columns=["shp_ciqid", "shp_up", "con_ciqid", "con_up"])
        for c in t.columns:
            ids |= set(t[c].dropna().astype("int64").unique())
    return ids, files


def build_period(ids, years):
    fp = pd.read_parquet(CIQ_REF / "fin_period.parquet", columns=FP_COLS)
    fp = fp[fp.companyid.isin(ids) & fp.cal_year.between(*years)].copy()
    print(f"  회계기간 {len(fp):,}행 (기업 {fp.companyid.nunique():,}개)")

    fx = pd.read_parquet(CIQ_REF / "fx_rate.parquet",
                         columns=["currency", "price_date", "fx_per_usd"])
    fx = fx.dropna(subset=["currency"]).sort_values("price_date")
    fp["period_end"] = fp["period_end"].astype("datetime64[ns]")
    fp = pd.merge_asof(fp.sort_values("period_end"), fx,
                       left_on="period_end", right_on="price_date", by="currency",
                       direction="backward").drop(columns="price_date")
    fp.loc[fp.currency.eq("USD"), "fx_per_usd"] = 1.0
    return fp


def set_preferred(fp, n_items):
    """F-5 — 행을 지우지 않고 우선순위 플래그만. 근거는 DECISIONS §2 F-5 보충."""
    fp = fp.copy()
    fp["_r"] = fp.restatement_type_id.map(REST_RANK).fillna(0)
    fp["_n"] = fp.financial_period_id.map(n_items).fillna(0).astype("int64")
    fp = fp.sort_values(["period_end", "_r", "_n", "financial_period_id"],
                        ascending=[False, False, False, False], kind="mergesort")
    fp["is_preferred"] = (~fp.duplicated(
        ["companyid", "period_type_id", "cal_year", "cal_quarter"])).astype("int8")
    fp["is_preferred_year"] = (~fp.duplicated(
        ["companyid", "period_type_id", "cal_year"])).astype("int8")
    fp = fp.rename(columns={"_n": "n_data_items"}).drop(columns="_r")
    d1 = int((fp.is_preferred == 0).sum())
    d2 = int(((fp.is_preferred == 1) & (fp.is_preferred_year == 0)).sum())
    print(f"  is_preferred=0 {d1:,} ({d1/len(fp)*100:.3f}%) · "
          f"연도키에서 추가로 밀린 행(결산월 변경) {d2:,}")
    return fp.sort_values(["companyid", "period_end"]).reset_index(drop=True)


# ---------- long 스캔 (연도별 분할 write) ----------

def scan_fin_data(pids_sorted, years, out):
    """fin_data_YYYY 를 훑어 표본 기간의 전 계정을 **연도별 파일로 즉시 저장**한다.

    반환: (연도별 출력 경로, financial_period_id 별 계정수 Series,
           data_item_id 별 관측수 Series, (item, unit) Counter)
    """
    n_items = pd.Series(dtype="int64")
    item_counts = pd.Series(dtype="int64")
    unit_counts = Counter()
    outputs = []
    for y in range(years[0] - 1, years[1] + 2):
        f = CIQ_REF / f"fin_data_{y}.parquet"
        if not f.exists():
            continue
        pf = pq.ParquetFile(f)
        parts = []
        for i in range(pf.metadata.num_row_groups):
            b = pf.read_row_group(i, columns=["financial_period_id", "data_item_id",
                                              "value", "unit_type_id"]).to_pandas()
            v = b.financial_period_id.values
            idx = np.searchsorted(pids_sorted, v)
            idx[idx >= len(pids_sorted)] = 0
            hit = pids_sorted[idx] == v
            if hit.any():
                parts.append(b.loc[hit])
        if not parts:
            print(f"    fin_data_{y}: 0행")
            continue
        yl = pd.concat(parts, ignore_index=True)
        del parts
        p = out / f"ciq_fin_long_{y}.parquet"
        tmp = out / (p.name + ".tmp")
        yl.to_parquet(tmp, index=False, compression="zstd")
        import os
        os.replace(tmp, p)
        outputs.append(p)
        print(f"    fin_data_{y}: {len(yl):,}행 -> {p.name}")

        n_items = n_items.add(yl.financial_period_id.value_counts(), fill_value=0)
        item_counts = item_counts.add(yl.data_item_id.value_counts(), fill_value=0)
        for (it, un), c in yl.groupby(["data_item_id", "unit_type_id"]).size().items():
            unit_counts[(it, un)] += int(c)
        del yl
        gc.collect()
    return outputs, n_items.astype("int64"), item_counts.astype("int64"), unit_counts


# ---------- wide (카탈로그 계정, 연도별 피벗) ----------

def build_wide(long_paths, fp, cat, ptype, name, out):
    """카탈로그 계정만 wide. **환산하지 않는다**(F-2). 연간은 is_preferred_year 로 거른다
    (결산월 변경 기업 때문 — F-5)."""
    flag = "is_preferred_year" if ptype == 1 else "is_preferred"
    per = fp[(fp.period_type_id == ptype) & (fp[flag] == 1)]
    keep_pids = set(per.financial_period_id)
    keep_items = set(cat.data_item_id)

    wides = []
    for p in long_paths:
        lg = pd.read_parquet(p, columns=["financial_period_id", "data_item_id", "value"])
        sub = lg[lg.financial_period_id.isin(keep_pids)
                 & lg.data_item_id.isin(keep_items)]
        del lg
        if not len(sub):
            continue
        sub = sub.drop_duplicates(["financial_period_id", "data_item_id"], keep="first")
        wides.append(sub.pivot(index="financial_period_id",
                               columns="data_item_id", values="value"))
        del sub
        gc.collect()
    w = pd.concat(wides).groupby(level=0).first() if wides else pd.DataFrame()
    del wides

    ren = {r.data_item_id: slug(r.item_name, r.data_item_id)
           for r in cat.itertuples() if r.data_item_id in w.columns}
    w = w.rename(columns=ren)
    # period_end·fiscal_* 동봉 — join 후 결산일 기반 lag 판단용 (README §1)
    keep = ["financial_period_id", "companyid", "period_type_id",
            "cal_year", "cal_quarter", "fiscal_year", "fiscal_quarter", "period_end",
            "currency", "fx_per_usd", "restatement_type_id", "restatement_type",
            "latest_period_flag", "n_data_items"]
    res = per[keep].merge(w, left_on="financial_period_id", right_index=True, how="left")
    p = out / f"ciq_fin_wide_{name}.parquet"
    tmp = out / (p.name + ".tmp")
    res.to_parquet(tmp, index=False, compression="zstd")
    import os
    os.replace(tmp, p)
    print(f"  ciq_fin_wide_{name}.parquet  {len(res):,}행 x {res.shape[1]}열")
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-dir", default=str(OUT_FULL),
                    help="무역 팩트 폴더 (조회 대상 기업을 여기서 뽑는다)")
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--years", nargs=2, type=int, default=[2005, 2026],
                    help="fin_period cal_year 범위 (기본 = 무역 2007~2025 의 소급 2년 + 현재)")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    years = (a.years[0], a.years[1])

    print(f"[1] 조회 대상 기업 (무역: {a.trade_dir})")
    ids, trade_files = sample_companies(a.trade_dir)
    print(f"  ciqid ∪ up 합집합 {len(ids):,}개")

    print(f"[2] 회계기간 dim  (cal_year {years[0]}~{years[1]})")
    fp = build_period(ids, years)

    print("[3] 전 계정 long 스캔 (연도별 분할 저장)")
    pids = np.sort(fp.financial_period_id.unique().astype("int64"))
    long_paths, n_items, item_counts, unit_counts = scan_fin_data(pids, years, out)
    total_long = int(item_counts.sum())
    print(f"  -> long 합계 {total_long:,}행 · 고유계정 {len(item_counts):,}개 · 파일 {len(long_paths)}개")

    print("[3b] 중복 우선순위 플래그 (계정 수 반영)")
    fp = set_preferred(fp, n_items)
    pp = out / "ciq_fin_period.parquet"
    fp.to_parquet(pp, index=False, compression="zstd")
    print(f"  -> ciq_fin_period.parquet {len(fp):,}행")

    print("[4] 계정 카탈로그 + 실측 커버리지")
    cat = parse_catalog()
    unit_mode = {}
    for (it, un), c in unit_counts.items():
        if it not in unit_mode or c > unit_mode[it][1]:
            unit_mode[it] = (un, c)
    cat["n_obs"] = cat.data_item_id.map(item_counts).fillna(0).astype("int64")
    cat["cov_pct"] = (cat.n_obs / len(fp) * 100).round(1)
    cat["unit_type_id"] = cat.data_item_id.map(lambda i: unit_mode.get(i, (pd.NA,))[0])
    cat["column_name"] = [slug(r.item_name, r.data_item_id) for r in cat.itertuples()]
    cat = cat.sort_values("cov_pct", ascending=False)
    cp = out / "ciq_dataitem_catalog.csv"
    cat.to_csv(cp, index=False, encoding="utf-8-sig")
    print(f"  -> ciq_dataitem_catalog.csv {len(cat):,}개")

    print("[5] wide (카탈로그 계정, 원표시통화)")
    wa = build_wide(long_paths, fp, cat, 1, "annual", out)
    wq = build_wide(long_paths, fp, cat, 2, "quarter", out)

    write_manifest(out, "fin_build",
                   inputs=list(trade_files) + [CIQ_REF / "fin_period.parquet"],
                   outputs=long_paths + [pp, cp, wa, wq],
                   extra={"years": list(years), "companies": len(ids),
                          "long_rows": total_long})


if __name__ == "__main__":
    main()
