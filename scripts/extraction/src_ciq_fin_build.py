# -*- coding: utf-8 -*-
r"""
src_ciq_fin_build.py — v1·v2·v3·v4 **공용** CIQ 재무층

`v4_ciq_fin_build.py`(김영수, 2026-08-21)의 일반화판이다. 결정(F-1~F-6)은 그대로 두고
**대상 기업 범위와 출력 경로만 매개변수화**했다. v4 스크립트와 산출물은 건드리지 않는다
(벤치마크 보존).

v4 판과의 차이:
  1. 대상 기업 = **무역 원천 전체**(수입 양측 + 수출자, 법인·모회사). v4 는 쌍이 성립한
     행만 썼기에 12,045개였고, 한쪽만 식별된 선적의 회사 135개가 빠져 있었다.
  2. 기간 필터 = **`cal_year`**. 무역과 붙이는 키가 `(cal_year, cal_quarter)` 이므로
     필터도 같은 축이어야 한다. `period_end` 로 자르면 **1·2월 결산 기업**이 샌다 —
     CIQ 는 1월 결산의 달력연도를 한 해 당기므로 `cal_year=2024` 인데 결산일이
     2025-01-31 인 기간이 있고, `period_end < 2025-01-01` 은 그 행을 통째로 버린다.
  3. 출력 = `data\staging\source\ciq_fin\` — 원천 옆에 두어 네 버전이 같은 것을 본다.

원천 : source\ciq_ref\{fin_period, fin_data_YYYY, fx_rate}.parquet
       source\trade_2024\{imp,exp}_ship_YYYYMM.parquet   (대상 기업 범위)
       shared memory\ciq_dataitems.md                     (계정 이름·분류 카탈로그)

산출 : data\staging\source\ciq_fin\
         ciq_fin_period.parquet        회계기간 dim (회사×기간) + 환율 + 중복 플래그
         ciq_fin_long.parquet          기간×계정 = 1행 — **전 계정, 아무것도 안 버림**
         ciq_fin_wide_annual.parquet   기간×카탈로그계정 (연간)
         ciq_fin_wide_quarter.parquet  기간×카탈로그계정 (분기)
         ciq_dataitem_catalog.csv      계정 카탈로그 + 이 표본 실측 커버리지
         _build_log.md                 실행 기록

## 승계한 결정 (v4 DECISIONS.md §2)

  F-1 계정 범위   = **전 계정**. long 이 싸므로 아무것도 버리지 않는다.
  F-2 통화        = **환산하지 않고 원표시통화 저장** + `fx_per_usd` 동봉.
                    `unit_type_id` 는 통화/비율 구분이 아니라 "백만 단위" 표시라
                    (주식수 3217 도 unit_type=2) 일괄 환산하면 주식수·비율까지 환산된다.
  F-3 기간 범위   = **거래 기간과 같게 잡으면 된다.** 무역과 `(cal_year, cal_quarter)` 로
                    equi-join 하므로 거래 연도의 재무만 쓰인다 — 소급분이 필요 없다.
                    앞으로 더 받는 것은 **표본 시작 시점에서 시차변수(L1·L4…)를 만들
                    여유분**일 때만 의미가 있다.
  F-4 주기        = 1 연간 · 2 분기 · 10 반기 전부. 반기만 내는 기업이 있다.
  F-5 중복 해소   = 행을 지우지 않고 `is_preferred`(분기키) · `is_preferred_year`(연도키)
                    플래그만 세운다. 우선순위: period_end 최신 > 정정유형 > id.
  F-6 wide 범위   = 카탈로그에 이름이 확인된 계정만 눕힌다. 전량은 long 에 있다.

사용:
  python scripts\extraction\src_ciq_fin_build.py
  python ... --cal-years 2018 2024                     # 담을 cal_year 범위 넓히기
"""

import argparse
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

SRC = Path(r"C:\panjiva\data\staging\source")
CIQ = SRC / "ciq_ref"
TRADE = SRC / "trade_2024"
OUT = SRC / "ciq_fin"
DOC = Path(r"C:\panjiva\shared memory\ciq_dataitems.md")

# F-5 정정유형 우선순위 (클수록 우선)
REST_RANK = {3: 5, 5: 4, 4: 3, 2: 2, 1: 1}   # Restated > Reclassified > NoChange > Original > PR

FP_COLS = ["financial_period_id", "companyid", "period_type_id", "period_type",
           "period_end", "cal_year", "cal_quarter", "fiscal_year", "fiscal_quarter",
           "currency", "restatement_type_id", "restatement_type", "latest_period_flag",
           "is_restatement_type_id"]

# 무역 원천에서 회사 범위를 긁을 컬럼 (수출 B/L 에는 상대방 식별자가 없다)
UNIVERSE_COLS = {
    "imp_ship_*.parquet": ["con_ciqid_original", "con_up", "shp_ciqid_original", "shp_up"],
    "exp_ship_*.parquet": ["shp_ciqid_original", "shp_up"],
}


# ---------------------------------------------------------------------------
def trade_universe(trade_dir: Path) -> set:
    """무역 원천에 등장하는 모든 CIQ 회사 = 법인 + 최종모회사, 수입 양측 + 수출자."""
    ids = set()
    for pat, cols in UNIVERSE_COLS.items():
        files = sorted(trade_dir.glob(pat))
        got = set()
        for f in files:
            d = pd.read_parquet(f, columns=cols)
            for c in cols:
                got |= set(d[c].dropna().astype("int64").unique())
        print(f"  {pat:<22} 파일 {len(files):>2}개 → 회사 {len(got):,}개")
        ids |= got
    print(f"  합집합 {len(ids):,}개")
    return ids


def parse_catalog() -> pd.DataFrame:
    """ciq_dataitems.md 의 계정표를 파싱한다 (id · 이름 · 재무제표 구분 · 소절)."""
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
    return pd.DataFrame(rows).drop_duplicates("data_item_id").reset_index(drop=True)


def slug(name: str, iid: int) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return f"{s or 'item'}_{iid}"


def build_period(ids: set, y0: int, y1: int) -> pd.DataFrame:
    """대상 기업의 회계기간 dim. 환율 부착 + 중복 플래그 (F-5).

    ⚠️ **`cal_year` 로 자른다. `period_end` 로 자르면 안 된다.**
       무역과 붙이는 키가 `(cal_year, cal_quarter)` 이므로 필터도 같은 축이어야 한다.
       `period_end` 로 자르면 **1·2월 결산 기업**이 샌다 — CIQ 는 1월 결산의 달력연도를
       한 해 당겨 붙이므로 `cal_year=2024` 인데 결산일이 2025-01-31 인 기간이 존재하고,
       `period_end < 2025-01-01` 필터는 그 행을 통째로 버린다(연간 1,064·분기 2,393개).
    """
    fp = pd.read_parquet(CIQ / "fin_period.parquet", columns=FP_COLS)
    fp["period_end"] = fp["period_end"].astype("datetime64[ns]")
    fp = fp[fp.companyid.isin(ids) & fp.cal_year.between(y0, y1)].copy()
    print(f"  회계기간 {len(fp):,}행 (기업 {fp.companyid.nunique():,}개) · "
          f"결산일 범위 {fp.period_end.min().date()} ~ {fp.period_end.max().date()}")

    # 환율 — period_end 기준 backward as-of (통화별). F-2: 환산은 하지 않고 계수만 동봉.
    fx = pd.read_parquet(CIQ / "fx_rate.parquet",
                         columns=["currency", "price_date", "fx_per_usd"])
    fx = fx.dropna(subset=["currency"]).sort_values("price_date")
    fp = pd.merge_asof(fp.sort_values("period_end"), fx,
                       left_on="period_end", right_on="price_date", by="currency",
                       direction="backward").drop(columns="price_date")
    fp.loc[fp.currency.eq("USD"), "fx_per_usd"] = 1.0
    miss = int(fp.fx_per_usd.isna().sum())
    print(f"  환율 미부착 {miss:,}행 ({miss/len(fp)*100:.2f}%)")

    # F-5 — 행을 지우지 않고 우선순위 플래그만
    fp["_r"] = fp.restatement_type_id.map(REST_RANK).fillna(0)
    fp = fp.sort_values(["period_end", "_r", "financial_period_id"],
                        ascending=[False, False, False], kind="mergesort")
    fp["is_preferred"] = (~fp.duplicated(
        ["companyid", "period_type_id", "cal_year", "cal_quarter"])).astype("int8")
    # 결산월을 바꾼 기업은 같은 cal_year 에 연간 기간이 둘 있다(3월 결산 → 12월 결산 등).
    # 오류가 아니라 실제 데이터 특성이므로, 연도 단위 join 용 플래그를 따로 둔다.
    fp["is_preferred_year"] = (~fp.duplicated(
        ["companyid", "period_type_id", "cal_year"])).astype("int8")
    d1 = int((fp.is_preferred == 0).sum())
    d2 = int(((fp.is_preferred == 1) & (fp.is_preferred_year == 0)).sum())
    print(f"  is_preferred=0 {d1:,} ({d1/len(fp)*100:.2f}%) · "
          f"연도키에서 추가로 밀린 행(결산월 변경) {d2:,}")
    return fp.drop(columns="_r").sort_values(
        ["companyid", "period_end"]).reset_index(drop=True)


def scan_fin_data(pids, y0: int, y1: int) -> pd.DataFrame:
    """fin_data_YYYY 를 rowgroup 단위로 훑어 대상 기간의 **전 계정**을 모은다.

    fin_data 파일은 `period_end` 연도로 쪼개져 있는데(원천 추출 SQL) 우리가 고른 기간은
    `cal_year` 기준이라 축이 다르다. `cal_year=Y` 인 기간의 결산일은 Y 년 2월부터
    Y+1 년 2월까지 흩어질 수 있으므로 **앞뒤로 한 해씩 더 연다.**
    전체를 메모리에 올리지 않고 rowgroup 마다 걸러 담는다.
    """
    pids = np.sort(np.asarray(sorted(pids), dtype="int64"))
    parts, total = [], 0
    for y in range(y0 - 1, y1 + 2):
        f = CIQ / f"fin_data_{y}.parquet"
        if not f.exists():
            continue
        pf = pq.ParquetFile(f)
        got = 0
        for i in range(pf.metadata.num_row_groups):
            b = pf.read_row_group(i, columns=["financial_period_id", "data_item_id",
                                              "value", "unit_type_id"]).to_pandas()
            v = b.financial_period_id.values
            idx = np.searchsorted(pids, v)
            idx[idx >= len(pids)] = 0
            hit = pids[idx] == v
            if hit.any():
                parts.append(b.loc[hit])
                got += int(hit.sum())
        pf.close()
        total += got
        print(f"    fin_data_{y}: {got:,}행")
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_wide(long, fp, cat, ptype: int, name: str, out: Path) -> int:
    """카탈로그 계정만 wide 로. **환산하지 않는다**(F-2) — currency·fx 를 같이 싣는다.

    연간(ptype=1)은 join 키가 (companyid, cal_year) 이므로 `is_preferred_year` 로 걸러야
    한다. 분기 키로 거르면 결산월 변경 기업에서 같은 연도가 두 행 남아 join 시 증식한다.
    """
    flag = "is_preferred_year" if ptype == 1 else "is_preferred"
    per = fp[(fp.period_type_id == ptype) & (fp[flag] == 1)]
    sub = long[long.financial_period_id.isin(set(per.financial_period_id))
               & long.data_item_id.isin(set(cat.data_item_id))]
    sub = sub.drop_duplicates(["financial_period_id", "data_item_id"], keep="first")
    w = sub.pivot(index="financial_period_id", columns="data_item_id", values="value")
    ren = {r.data_item_id: slug(r.item_name, r.data_item_id)
           for r in cat.itertuples() if r.data_item_id in w.columns}
    w = w.rename(columns=ren)
    keep = ["financial_period_id", "companyid", "cal_year", "cal_quarter", "period_end",
            "currency", "fx_per_usd", "restatement_type_id", "latest_period_flag",
            "is_preferred", "is_preferred_year"]
    res = per[keep].merge(w, left_on="financial_period_id", right_index=True, how="left")
    res.to_parquet(out / f"ciq_fin_wide_{name}.parquet", index=False, compression="zstd")
    print(f"  ciq_fin_wide_{name}.parquet  {len(res):,}행 × {res.shape[1]}열")
    return len(res)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cal-years", nargs=2, type=int, default=[2022, 2024],
                    help="담을 cal_year 범위(양끝 포함). 무역과 붙이는 키와 같은 축이다. "
                         "거래 기간과 같게 잡으면 되고, 앞으로 더 받는 것은 시차변수용 여유분")
    ap.add_argument("--trade-dir", default=str(TRADE))
    ap.add_argument("--out-dir", default=str(OUT))
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    t0 = datetime.now()

    print(f"[1] 대상 기업 범위 (무역 원천 전체)")
    ids = trade_universe(Path(a.trade_dir))

    y0, y1 = a.cal_years
    print(f"[2] 회계기간 dim  (cal_year {y0} ~ {y1})")
    fp = build_period(ids, y0, y1)
    fp.to_parquet(out / "ciq_fin_period.parquet", index=False, compression="zstd")
    print(f"  → ciq_fin_period.parquet {len(fp):,}행")

    print("\n[3] 전 계정 long 스캔")
    long = scan_fin_data(fp.financial_period_id.unique(), y0, y1)
    long.to_parquet(out / "ciq_fin_long.parquet", index=False, compression="zstd")
    n_item = long.data_item_id.nunique()
    print(f"  → ciq_fin_long.parquet {len(long):,}행 · 고유계정 {n_item:,}개")

    print("\n[4] 계정 카탈로그 + 이 표본 실측 커버리지")
    cat = parse_catalog()
    cnt = long.data_item_id.value_counts()
    unit = long.groupby("data_item_id")["unit_type_id"].agg(lambda x: x.mode().iloc[0])
    cat["n_obs"] = cat.data_item_id.map(cnt).fillna(0).astype("int64")
    cat["cov_pct"] = (cat.n_obs / len(fp) * 100).round(1)
    cat["unit_type_id"] = cat.data_item_id.map(unit)
    cat["column_name"] = [slug(r.item_name, r.data_item_id) for r in cat.itertuples()]
    cat = cat.sort_values("cov_pct", ascending=False)
    cat.to_csv(out / "ciq_dataitem_catalog.csv", index=False, encoding="utf-8-sig")
    print(f"  → ciq_dataitem_catalog.csv {len(cat):,}개 (long 전체 {n_item:,}개 중 이름 확인분)")

    print("\n[5] wide (카탈로그 계정, 원표시통화)")
    na = build_wide(long, fp, cat, 1, "annual", out)
    nq = build_wide(long, fp, cat, 2, "quarter", out)

    (out / "_build_log.md").write_text(
        "# 공용 CIQ 재무층 빌드 로그 (`src_ciq_fin_build.py`)\n\n"
        f"- 실행: {t0:%Y-%m-%d %H:%M} ~ {datetime.now():%H:%M}\n"
        f"- 기간 창: **`cal_year` {y0} ~ {y1}** "
        "(period_end 가 아니라 cal_year 로 자른다 — 무역과 붙이는 키가 같은 축이므로)\n"
        f"- 대상 기업: 무역 원천 전체 **{len(ids):,}개** "
        f"(그중 재무 보유 {fp.companyid.nunique():,}개, "
        f"{fp.companyid.nunique()/len(ids)*100:.1f}%)\n"
        f"- `ciq_fin_period` {len(fp):,}행 · `ciq_fin_long` {len(long):,}행 "
        f"(고유계정 {n_item:,})\n"
        f"- `ciq_fin_wide_annual` {na:,}행 · `ciq_fin_wide_quarter` {nq:,}행\n\n"
        "결정은 v4 `DECISIONS.md` §2 (F-1~F-6) 을 그대로 승계한다. "
        "v4 폴더의 기존 산출물은 벤치마크로 보존한다.\n",
        encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {out}")


if __name__ == "__main__":
    main()
