# -*- coding: utf-8 -*-
r"""
finblocks.py — 공용 재무층을 테이블에 **블록**으로 붙이는 기계 (v2·v3 공용)

재무 블록 하나 = "누구의 · 어느 주기" 재무 한 벌. 이름은 `{접두어}{항목}` 꼴이다.

    con_up_a_total_revenues_28        수입자 최종모회사 · 연간 · 매출(원표시)
    con_up_a_total_revenues_28_usd    〃 USD 환산
    con_up_a_period_end               〃 그 회계기간의 결산일
    con_up_a_days_after_close         〃 기준일 − 결산일 (음수면 아직 진행 중)

## 결합 방식 — `(cal_year, cal_quarter)` equi-join

거래가 일어난 **달력 분기**(연간 블록은 달력 연도)의 재무를 붙인다. 판정이 아니라 사실이라
되돌릴 필요가 없다. 시점 판단(그 재무가 거래 시점에 공시됐는가)은 `*_days_after_close` 로
분석 단계에서 한다. 근거는 `data\staging\tom_v1_2024\DECISIONS.md` V-6.

⚠️ **오른쪽이 조인 키로 유일해야 한다.** 안 그러면 왼쪽 행이 늘어난다.
   - 연간: `is_preferred_year=1` (결산월을 바꾼 기업 대비)
   - 분기: `is_preferred=1` + 같은 달력분기를 분기(2)와 반기(10)가 함께 가리킬 때 **분기 우선**

⚠️ `fx_per_usd` 는 **1 USD 당 현지통화**다. USD 환산은 곱셈이 아니라 **나눗셈**이다.

⚠️ 환산 대상은 **금액성 계정만**이다. `unit_type_id` 는 통화 표시가 아니라 "백만 단위"
   표시라, 주식수·EPS·마진율까지 일괄 환산하면 안 된다.

`tom_v1_shipment_master.py` 에 같은 내용이 인라인으로 들어 있다(먼저 만들어져서). 규칙이
바뀌면 **두 곳을 함께** 고쳐야 한다.
"""

from __future__ import annotations

import gc
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

__all__ = ["load_fin_layer", "eq_frame", "attach_block_keys", "write_with_blocks",
           "KEY_META"]

# 블록마다 항상 싣는 메타. 결측 있는 정수는 nullable 정수로 — 안 그러면 pandas 가
# float64 로 만들어 `242432463.0` 처럼 보인다(명세 게이트 14).
KEY_META = {
    "financial_period_id": "Int32",     # CIQ 는 음수 ID 도 쓴다
    "period_end":          "datetime64[ns]",
    "period_type_id":      "Int8",      # 1 연간 · 2 분기 · 10 반기
    "cal_year":            "Int16",
    "cal_quarter":         "Int8",      # 연간 블록은 결측
    "currency":            "string",
    "fx_per_usd":          "float64",
    "restatement_type_id": "Int8",
    "days_after_close":    "Int16",     # 기준일 − 결산일. 음수 = 아직 진행 중
}

# as-of 결합에서는 마지막 열 이름이 다르다 — **섞이지 않게 하려고 일부러 갈랐다**.
#   equi  `days_after_close`  기준일 − 결산일, **음수 가능**(회계기간이 아직 안 끝남)
#   asof  `age_days`          기준일 − 결산일, **항상 양수**(끝난 것만 붙이므로)
# 명세 §3.3 은 as-of 를 요구하며 `fin_age_days` 라는 이름을 쓴다.
KEY_META_ASOF = {**{k: v for k, v in KEY_META.items() if k != "days_after_close"},
                 "age_days": "Int16"}

LOOKBACK = pd.Timedelta(days=730)      # 명세 §3.3 "최대 2년 소급"

_PER_COLS = ["financial_period_id", "companyid", "period_type_id", "period_end",
             "cal_year", "cal_quarter", "currency", "fx_per_usd",
             "restatement_type_id", "is_preferred", "is_preferred_year"]

_WIDE_META = {"financial_period_id", "companyid", "cal_year", "cal_quarter", "period_end",
              "currency", "fx_per_usd", "restatement_type_id", "latest_period_flag",
              "is_preferred", "is_preferred_year"}


def load_fin_layer(fin_dir: Path, freqs=("a", "q")) -> dict:
    """공용 재무층을 읽어 블록 부착에 필요한 것만 담은 dict 를 돌려준다.

    돌려주는 것:
      per                  회계기간 dim (전 주기)
      wide[f], cols[f]     주기별 값 표(financial_period_id 색인)와 계정 컬럼 목록
      conv[f]              그중 USD 환산 대상 컬럼
    """
    fin_dir = Path(fin_dir)
    out = {"per": pd.read_parquet(fin_dir / "ciq_fin_period.parquet", columns=_PER_COLS),
           "wide": {}, "cols": {}, "conv": {}}
    out["per"]["period_end"] = out["per"]["period_end"].astype("datetime64[ns]")

    cat = pd.read_csv(fin_dir / "ciq_dataitem_catalog.csv", encoding="utf-8-sig")
    share = cat.item_name.str.contains("Shares", case=False, na=False)
    conv_ids = set(cat.loc[(cat.unit_type_id == 2) & ~share, "data_item_id"])
    id_of = {r.column_name: r.data_item_id for r in cat.itertuples()}

    for f, name in (("a", "annual"), ("q", "quarter")):
        if f not in freqs:
            continue
        w = pd.read_parquet(fin_dir / f"ciq_fin_wide_{name}.parquet")
        cols = [c for c in w.columns if c not in _WIDE_META]
        out["wide"][f] = w.set_index("financial_period_id")[cols]
        out["cols"][f] = cols
        out["conv"][f] = [c for c in cols if id_of.get(c) in conv_ids]
        print(f"  재무층 {name}: {len(w):,}행 × {len(cols)}계정 "
              f"(USD 환산 {len(out['conv'][f])})")
    return out


def eq_frame(per: pd.DataFrame, ptypes) -> tuple:
    """equi-join 용 오른쪽 표 + 조인 키. 오른쪽을 키로 유일하게 정리한다."""
    annual = tuple(ptypes) == (1,)
    key = ["companyid", "cal_year"] if annual else ["companyid", "cal_year", "cal_quarter"]
    s = per[per.period_type_id.isin(ptypes)].copy()
    s = s[s.is_preferred_year == 1] if annual else s[s.is_preferred == 1]
    s["_pt"] = (s.period_type_id == 2).astype("int8")          # 분기 > 반기
    s = s.sort_values(["_pt", "financial_period_id"], ascending=[False, False],
                      kind="mergesort").drop_duplicates(key, keep="first")
    s["companyid"] = s["companyid"].astype("int64")
    s = s.drop(columns=["_pt", "is_preferred", "is_preferred_year"])
    if annual:
        s = s.drop(columns=["cal_quarter"])
    return s.reset_index(drop=True), key


def asof_frame(per: pd.DataFrame, ptypes) -> pd.DataFrame:
    """as-of 결합용 오른쪽 표 — `(회사, 결산일)` 이 유일해야 행이 늘지 않는다.

    ⚠️ equi 용 `eq_frame` 과 유일성 기준이 다르다. as-of 는 `period_end` 로 붙으므로
       같은 회사·같은 결산일이 둘이면(정정본, 또는 분기와 반기가 같은 날 끝남) 증식한다.
    """
    s = per[per.period_type_id.isin(ptypes)].copy()
    s["_pt"] = (s.period_type_id == 2).astype("int8")          # 분기 > 반기
    s = s.sort_values(["_pt", "is_preferred", "financial_period_id"],
                      ascending=[False, False, False], kind="mergesort")
    s = s.drop_duplicates(["companyid", "period_end"], keep="first")
    s["companyid"] = s["companyid"].astype("int64")
    s = s.drop(columns=["_pt", "is_preferred", "is_preferred_year"])
    return s.sort_values("period_end").reset_index(drop=True)


def attach_block_keys_asof(df: pd.DataFrame, blocks, per: pd.DataFrame,
                           ref_date: str) -> pd.DataFrame:
    """**명세 §3.3 as-of** — 기준시점보다 먼저 끝난 회계기간 중 가장 최근 것을 붙인다.

    최대 2년 소급하고, 결산 **당일**은 제외한다(그 시점엔 아직 공시 전).
    `age_days` 는 항상 양수다.
    """
    frames = {}
    for prefix, key, ptypes in blocks:
        if ptypes not in frames:
            frames[ptypes] = asof_frame(per, ptypes)
        right = frames[ptypes].rename(columns={"companyid": key})
        # ⚠️ merge_asof 는 두 시각 키의 **해상도까지** 같아야 한다 — us vs ns 면 MergeError.
        #    `PeriodIndex.start_time` 이 [us] 를 주는 경우가 있어 여기서 못 박는다.
        left = pd.DataFrame({key: df[key],
                             ref_date: df[ref_date].astype("datetime64[ns]"),
                             "_row": np.arange(len(df))})
        ok = left[key].notna() & left[ref_date].notna()
        sub = left.loc[ok].copy()
        sub[key] = sub[key].astype("int64")
        r = right.copy()
        r[key] = r[key].astype("int64")
        n0 = len(sub)
        sub = pd.merge_asof(sub.sort_values(ref_date), r,
                            left_on=ref_date, right_on="period_end", by=key,
                            direction="backward", tolerance=LOOKBACK,
                            allow_exact_matches=False)
        assert len(sub) == n0, f"{prefix}: as-of 가 행을 늘렸다 {n0:,} → {len(sub):,}"
        sub["age_days"] = (sub[ref_date]
                            - sub["period_end"].astype("datetime64[ns]")).dt.days
        miss = sub["financial_period_id"].isna()
        for c in ("cal_year", "cal_quarter"):
            if c in sub.columns:
                sub.loc[miss, c] = np.nan
        pos = sub["_row"].to_numpy()
        for c, dt in KEY_META_ASOF.items():
            col = f"{prefix}{c}"
            if c not in sub:
                df[col] = pd.array([pd.NA] * len(df), dtype=dt)
            elif dt.startswith("Int"):
                raw = np.full(len(df), np.nan)
                raw[pos] = pd.to_numeric(sub[c], errors="coerce").to_numpy(dtype="float64")
                df[col] = pd.array(raw, dtype="Float64").astype(dt)
            else:
                s = pd.Series(index=df.index, dtype=dt)
                s.iloc[pos] = sub[c].values
                df[col] = s
        del left, sub, r
    return df


def attach_block_keys(df: pd.DataFrame, blocks, per: pd.DataFrame,
                      ref_date: str, cy: str = None, cq: str = None,
                      mode: str = "equi") -> pd.DataFrame:
    """각 블록이 **어느 회계기간에 붙는지**를 계산해 메타 9열씩 붙인다 (값은 아직).

    blocks : [(접두어, 회사 id 컬럼, periodTypeId 튜플), ...]
    ref_date : 기준일 컬럼.
    cy, cq   : 달력 연/분기 컬럼(equi 전용). 없으면 `ref_date` 에서 만든다.
    mode     : `"equi"` = `(회사, cal_year[, cal_quarter])` 일치
               `"asof"` = 명세 §3.3, 기준시점 직전 완료 기간 (소급 2년)
    """
    if mode == "asof":
        return attach_block_keys_asof(df, blocks, per, ref_date)
    cyv = df[cy] if cy else df[ref_date].dt.year
    cqv = df[cq] if cq else df[ref_date].dt.quarter
    frames = {}
    for prefix, key, ptypes in blocks:
        if ptypes not in frames:
            frames[ptypes] = eq_frame(per, ptypes)
        right, jkey = frames[ptypes]
        left = pd.DataFrame({key: df[key], "_row": np.arange(len(df))})
        left["cal_year"] = np.asarray(cyv)
        if "cal_quarter" in jkey:
            left["cal_quarter"] = np.asarray(cqv)
        jc = [key if c == "companyid" else c for c in jkey]
        sub = left.loc[left[jc].notna().all(axis=1)].copy()
        for c in jc:
            sub[c] = sub[c].astype("int64")
        r = right.rename(columns={"companyid": key})
        for c in jc:
            r[c] = r[c].astype("int64")
        n0 = len(sub)
        sub = sub.merge(r, on=jc, how="left")
        assert len(sub) == n0, f"{prefix}: equi-join 이 행을 늘렸다 {n0:,} → {len(sub):,}"

        # ⚠️ `cal_year`·`cal_quarter` 는 **조인 키라 왼쪽에서 온 값**이다. 그대로 두면
        #    재무가 안 붙은 행에도 값이 남아, `*_cal_year.notna()` 로 커버리지를 재면
        #    100% 가 나온다. 매칭 실패 행은 블록 메타를 전부 비운다.
        miss = sub["financial_period_id"].isna()
        for c in ("cal_year", "cal_quarter"):
            if c in sub.columns:
                sub.loc[miss, c] = np.nan

        base = pd.Series(df[ref_date].to_numpy()[sub["_row"].to_numpy()], index=sub.index)
        sub["days_after_close"] = (base - sub["period_end"]).dt.days
        pos = sub["_row"].to_numpy()
        for c, dt in KEY_META.items():
            col = f"{prefix}{c}"
            if c not in sub:                       # 연간 블록은 cal_quarter 가 없다
                df[col] = pd.array([pd.NA] * len(df), dtype=dt)
            elif dt.startswith("Int"):
                raw = np.full(len(df), np.nan)
                raw[pos] = pd.to_numeric(sub[c], errors="coerce").to_numpy(dtype="float64")
                df[col] = pd.array(raw, dtype="Float64").astype(dt)
            else:
                s = pd.Series(index=df.index, dtype=dt)
                s.iloc[pos] = sub[c].values
                df[col] = s
        del left, sub, r, base
    return df


def _schema_with_meta(tbl: pa.Table, base: pd.DataFrame, n_base: int) -> pa.Schema:
    """`from_arrays` 가 버린 pandas 메타데이터를 복원 — nullable 정수를 되살린다."""
    meta = pa.Table.from_pandas(base.head(0), preserve_index=False).schema.metadata
    md = json.loads(meta[b"pandas"].decode())
    for nm in tbl.schema.names[n_base:]:
        md["columns"].append({"name": nm, "field_name": nm, "pandas_type": "float64",
                              "numpy_type": "float64", "metadata": None})
    return tbl.schema.with_metadata({b"pandas": json.dumps(md).encode()})


def write_with_blocks(df: pd.DataFrame, path: Path, blocks, layer: dict,
                      chunk: int = 100_000) -> tuple:
    """base + 각 블록의 계정 값(원표시 + USD)을 parquet 으로 쓴다.

    수천 열을 한꺼번에 메모리에 올리지 않도록 **행 청크**로 나눠 쓴다.
    blocks 의 4번째 원소가 True 인 블록만 값을 싣는다(False 면 메타 9열만 이미 붙어 있다).
    """
    path = Path(path)
    val = {}                                       # 블록별 (numpy 값표, 컬럼→열번호, 환산집합)
    for prefix, _, ptypes, with_values in blocks:
        if not with_values:
            continue
        f = "a" if tuple(ptypes) == (1,) else "q"
        w, cols = layer["wide"][f], layer["cols"][f]
        fid = df[f"{prefix}financial_period_id"]
        ok = fid.notna().to_numpy()
        probe = np.zeros(len(df), dtype="int64")
        probe[ok] = fid[ok].astype("int64").to_numpy()
        p = w.index.get_indexer(pd.Index(probe))
        p[~ok] = -1                                # ⚠️ CIQ 는 음수 ID 도 쓴다 — 마스크로 처리
        val[prefix] = (w[cols].to_numpy(dtype="float64"),
                       {c: i for i, c in enumerate(cols)}, cols,
                       set(layer["conv"][f]), p)

    writer, schema, ncol = None, None, 0
    tmp = path.with_suffix(".tmp")
    try:
        for lo in range(0, len(df), chunk):
            hi = min(lo + chunk, len(df))
            base = df.iloc[lo:hi]
            t = pa.Table.from_pandas(base, preserve_index=False)
            arrays, names = list(t.columns), list(t.schema.names)
            n_base = len(names)
            del t
            for prefix, _, _, with_values in blocks:
                if not with_values:
                    continue
                np_w, at, cols, conv, pos = val[prefix]
                p = pos[lo:hi]
                miss = p < 0
                safe = np.where(miss, 0, p)
                fx = base[f"{prefix}fx_per_usd"].to_numpy(dtype="float64")
                for c in cols:
                    x = np.where(miss, np.nan, np_w[safe, at[c]])
                    arrays.append(pa.array(x)); names.append(f"{prefix}{c}")
                    if c in conv:
                        arrays.append(pa.array(x / fx)); names.append(f"{prefix}{c}_usd")
            tbl = pa.Table.from_arrays(arrays, names=names)
            if writer is None:
                schema = _schema_with_meta(tbl, base, n_base)
                ncol = tbl.num_columns
                writer = pq.ParquetWriter(tmp, schema, compression="zstd")
            writer.write_table(tbl.cast(schema))
            del arrays, names, tbl, base
            gc.collect()
    finally:
        if writer is not None:
            writer.close()
    tmp.replace(path)
    return len(df), ncol
