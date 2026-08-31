# -*- coding: utf-8 -*-
r"""
tom_v1_shipment_master.py — 업무지시서 03 / 명세 04 산출물 **v1 (선적 감사층)**

한 행 = 미국 해상 수입 선적 1건. 그 행에 양 당사자(수출자·수입자)의 매칭상태·관계분류와
재무를 붙인다. 명세 `04_2024연간파일럿_통합명세.md` §8.1 대응.

기간·경로가 전부 인자다(명세 §10) — H1 하드코딩 없음.

## 김영수 연구원 확정 결정 (2026-08-21)

  V-1 원천      = `data\staging\source\trade_2024\imp_ship_YYYYMM.parquet` (공용 원천)
                  표준필터(미국 실착·통과화물 제외)는 원천에서 이미 적용됐다.
  V-2 관계판정  = `scripts\common\relationship.py` 공용함수. v1·v2·v3 가 같은 함수를 쓴다.
                  3분류(within_firm / arms_length / unmatched) + 2단계 매칭상태 +
                  `unmatched_reason` + `self_shipment`.
                  parent_sub·sibling 은 명세 §1.4 대로 **참고용 `within_firm_type`** 로 보존.
  V-3 재무소스  = `data\staging\source\ciq_fin\` 공용 재무층 (전 계정 4,518개).
  V-4 재무구성  = **모회사 × 연간** 2블록(수입자·수출자)에 **카탈로그 410계정 전부**를
                  원표시+USD 로 싣는다. 명세 §1.7 이 이 블록을 주 재무변수로 지정했다.
                  나머지 6블록(법인×연간·분기, 모회사×분기)은 `financial_period_id` 키만
                  싣는다 — 그 키로 공용 재무층에서 **4,518계정 어느 것이든** 꺼내 붙일 수
                  있고 as-of 판정을 다시 하지 않는다.
  V-5 USD 환산  = `unit_type_id==2`(백만 단위 금액성) 중 주식수 6종을 뺀 **263계정**만
                  `value / fx_per_usd`. 비율·성장률·주당지표는 원표시만 두고 `fx_per_usd` 동봉.
                  ⚠️ `fx_per_usd` 는 **1 USD 당 현지통화**다(KRW 1383, JPY 161). 곱하면 안 된다.
  V-6 결합방식  = **`(cal_year, cal_quarter)` equi-join.** 도착일이 속한 달력 분기(연간은
                  달력 연도)의 재무를 붙인다. as-of 를 쓰지 않는다 — as-of 는 "소급 2년",
                  "결산 당일 제외" 같은 **판정**을 값 안에 녹여 되돌릴 수 없게 만든다.
                  병합 단계는 사실만 담고, 시점 판단(lag)은 Stata 분석 단계에서 한다.
                  근거 자료로 `*_period_end` 와 `*_days_after_close`(도착일−결산일, 음수면
                  아직 진행 중)를 남긴다. ⚠️ 명세 §3.3 은 as-of 를 요구 — PI 보고 대상.
  V-7 분기블록  = periodTypeId **2(분기) + 10(반기)**. 반기만 내는 기업을 버리지 않기 위함이며
                  `*_q_period_type_id` 로 어느 쪽인지 항상 식별된다. 연간블록은 1만 쓴다.
  V-8 override  = 명세 §7. 승인 파일이 있으면 적용하고 `*_crosswalk_overridden=1`.
                  원본은 `*_ciqid_original` 로 항상 보존한다(§4.1). 파일이 없으면 적용 0건.

사용:
  python scripts\extraction\tom_v1_shipment_master.py
  python ... --start 2024-01-01 --end 2024-03-01        # 일부 월만
  python ... --override path\to\crosswalk_override.parquet
"""

import argparse
import gc
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from relationship import add_relationship          # noqa: E402
from finblocks import attach_block_keys_asof         # noqa: E402  (as-of 경로만 위임)

SRC = Path(r"C:\panjiva\data\staging\source")
TRADE = SRC / "trade_2024"
CIQ = SRC / "ciq_ref"
FIN = SRC / "ciq_fin"
OUT = Path(r"C:\panjiva\data\staging\tom_v1_2024")

CHUNK = 400_000                        # 행 청크 — 1,400열을 통째로 올리지 않는다

# V-4 재무 블록. (접두어, 선적의 키 컬럼, periodTypeId 목록, 값도 실을지)
BLOCKS = [
    ("con_up_a_", "con_up",    (1,),     True),    # 수입자 모회사 × 연간  — 주 재무변수
    ("shp_up_a_", "shp_up",    (1,),     True),    # 수출자 모회사 × 연간  — 주 재무변수
    ("con_a_",    "con_ciqid", (1,),     False),   # 법인 × 연간
    ("shp_a_",    "shp_ciqid", (1,),     False),
    ("con_q_",    "con_ciqid", (2, 10),  False),   # 법인 × 분기(+반기)
    ("shp_q_",    "shp_ciqid", (2, 10),  False),
    ("con_up_q_", "con_up",    (2, 10),  False),   # 모회사 × 분기(+반기)
    ("shp_up_q_", "shp_up",    (2, 10),  False),
]

# 블록마다 항상 싣는 메타 (값 블록·키 블록 공통) — 타입까지 못 박는다.
#
# ⚠️ 결측이 있는 정수 컬럼을 그냥 두면 pandas 가 float64 로 만들어 `242432463.0` 처럼 보인다.
#    parquet 에 **nullable 정수형(Int64 등)** 으로 적으면 읽을 때도 정수로 복원된다.
#    명세 게이트 14 "코드성 식별자는 정수 손실 없이 보존된다" 대응.
KEY_META = {
    "financial_period_id": "Int32",     # CIQ 는 음수 ID 도 쓴다 (범위 -2,147,483,647 ~ 2,101,261,862)
    "period_end":          "datetime64[ns]",
    "period_type_id":      "Int8",      # 1 연간 · 2 분기 · 10 반기
    "cal_year":            "Int16",
    "cal_quarter":         "Int8",
    "currency":            "string",
    "fx_per_usd":          "float64",
    "restatement_type_id": "Int8",
    # 도착일 − 결산일. **양수 = 이미 끝난 기간 · 음수 = 아직 진행 중(공시 전)**
    # 이 부호 하나로 "거래 시점에 알 수 있었나" 를 바로 판별한다. Stata 에서 lag 을
    # 줄 때의 근거 자료이며, `> 0` 으로 거르면 as-of 와 같은 조건이 된다.
    "days_after_close":    "Int16",
}

# 우리가 만드는 컬럼의 타입 (원천 컬럼은 parquet 스키마를 보고 자동 판정)
DERIVED_DTYPE = {
    "con_ciqid": "Int64", "shp_ciqid": "Int64",
    "con_crosswalk_overridden": "Int8", "shp_crosswalk_overridden": "Int8",
}


# ---------------------------------------------------------------------------
# 1. 참조 자료
# ---------------------------------------------------------------------------
def load_fin_layer():
    """공용 재무층 → (기간 dim, 연간 wide, 환산대상 컬럼)."""
    per = pd.read_parquet(FIN / "ciq_fin_period.parquet", columns=[
        "financial_period_id", "companyid", "period_type_id", "period_end",
        "cal_year", "cal_quarter", "currency", "fx_per_usd",
        "restatement_type_id", "is_preferred", "is_preferred_year"])
    per["period_end"] = per["period_end"].astype("datetime64[ns]")

    wide = pd.read_parquet(FIN / "ciq_fin_wide_annual.parquet")
    meta = {"financial_period_id", "companyid", "cal_year", "cal_quarter", "period_end",
            "currency", "fx_per_usd", "restatement_type_id", "latest_period_flag",
            "is_preferred", "is_preferred_year"}
    val_cols = [c for c in wide.columns if c not in meta]
    wide = wide.set_index("financial_period_id")[val_cols]

    # V-5 — 환산 대상 결정
    cat = pd.read_csv(FIN / "ciq_dataitem_catalog.csv", encoding="utf-8-sig")
    share = cat.item_name.str.contains("Shares", case=False, na=False)
    conv_ids = set(cat.loc[(cat.unit_type_id == 2) & ~share, "data_item_id"])
    id_of = {r.column_name: r.data_item_id for r in cat.itertuples()}
    conv_cols = [c for c in val_cols if id_of.get(c) in conv_ids]
    print(f"  재무층: 기간 {len(per):,} · 연간 wide {len(wide):,}행 × {len(val_cols)}계정 "
          f"(USD 환산 대상 {len(conv_cols)})")
    return per, wide, val_cols, conv_cols


def load_company():
    co = pd.read_parquet(CIQ / "company.parquet", columns=[
        "companyid", "companyname", "country_iso2", "industry", "company_type"])
    return co.drop_duplicates("companyid").set_index("companyid")


def eq_frame(per: pd.DataFrame, ptypes) -> tuple:
    """equi-join 용 오른쪽 표. 조인 키까지 함께 돌려준다.

    조인 키는 **거래 도착일의 달력 연·분기**다.
      - 연간 블록: `(companyid, cal_year)`
      - 분기 블록: `(companyid, cal_year, cal_quarter)`

    ⚠️ 오른쪽이 그 키로 **유일해야** 한다. 안 그러면 왼쪽 행이 늘어난다.
       - 연간은 `is_preferred_year=1` 이 보장한다(결산월을 바꾼 기업 대비).
       - 분기는 `is_preferred=1` 이 정정본 중복을 없애지만, **분기(2)와 반기(10)가 같은
         달력분기를 가리킬 수 있어** 주기를 섞으면 여전히 중복이다 → **분기 우선**으로
         한 번 더 정리한다. 어느 쪽이 붙었는지는 `*_period_type_id` 로 항상 보인다.
    """
    annual = ptypes == (1,)
    key = ["companyid", "cal_year"] if annual else ["companyid", "cal_year", "cal_quarter"]
    s = per[per.period_type_id.isin(ptypes)].copy()
    s = s[s.is_preferred_year == 1] if annual else s[s.is_preferred == 1]
    s["_pt"] = (s.period_type_id == 2).astype("int8")          # 분기 > 반기
    s = s.sort_values(["_pt", "financial_period_id"], ascending=[False, False],
                      kind="mergesort")
    n0 = len(s)
    s = s.drop_duplicates(key, keep="first")
    if n0 != len(s):
        print(f"    주기{ptypes}: 키 {tuple(key[1:])} 중복 {n0-len(s):,}행 정리 → {len(s):,}행")
    s["companyid"] = s["companyid"].astype("int64")
    s = s.drop(columns=["_pt", "is_preferred", "is_preferred_year"])
    if annual:
        s = s.drop(columns=["cal_quarter"])                    # 연간은 분기 라벨이 없다
    return s.reset_index(drop=True), key


# ---------------------------------------------------------------------------
# 2. 월별 선적층
# ---------------------------------------------------------------------------
_NULLABLE_INT = {"int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
                 "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32"}


def load_month(ym: str, override: pd.DataFrame | None, co: pd.DataFrame) -> pd.DataFrame:
    path = TRADE / f"imp_ship_{ym}.parquet"
    # 원천의 정수 컬럼을 nullable 정수로 — 결측 때문에 float64 로 뭉개지는 것을 막는다
    schema = pq.ParquetFile(path).schema_arrow
    int_cast = {f.name: _NULLABLE_INT[str(f.type)] for f in schema
                if pa.types.is_integer(f.type) and str(f.type) in _NULLABLE_INT}
    df = pd.read_parquet(path).astype(int_cast)
    df["arrivaldate"] = df["arrivaldate"].astype("datetime64[ns]")

    # 거래 쪽 달력 분기 — 재무를 붙이는 조인 키다. **블록 안이 아니라 최상위**에 둔다.
    # 블록 안의 `*_cal_year` 는 "붙은 회계기간의" 달력연도라 뜻이 다르고, 재무가 안 붙은
    # 행에서는 결측이다. 둘을 이름으로 확실히 갈라 놓는다(v2 패널과 같은 구조).
    df["trade_quarter"] = df["arrivaldate"].dt.to_period("Q").astype("string")
    df["cal_year"] = df["arrivaldate"].dt.year.astype("Int16")
    df["cal_quarter"] = df["arrivaldate"].dt.quarter.astype("Int8")

    # --- V-8 override (명세 §4.1·§7) — 원본은 항상 보존 ---
    for side in ("con", "shp"):
        df[f"{side}_ciqid"] = df[f"{side}_ciqid_original"].astype("Int64")
        df[f"{side}_crosswalk_overridden"] = pd.Series(0, index=df.index, dtype="Int8")
    if override is not None and len(override):
        ov = override.set_index("panjiva_id")["replacement_companyid"]
        for side in ("con", "shp"):
            new = df[f"{side}panjivaid"].map(ov)
            hit = new.notna()
            df.loc[hit, f"{side}_ciqid"] = new[hit].astype("Int64")
            df.loc[hit, f"{side}_crosswalk_overridden"] = pd.Series(
                1, index=df.index, dtype="Int8")[hit]

    # --- V-2 관계판정 (공용함수) ---
    df = add_relationship(df)

    # 명세 §1.4 — parent_sub·sibling 은 주 분석에 쓰지 않되 참고용으로 보존
    same_up = df["con_up"].notna() & df["shp_up"].notna() & (df["con_up"] == df["shp_up"])
    is_self = df["self_shipment"] == 1
    is_ps = (df["con_ciqid"].notna() & df["shp_up"].notna() & (df["con_ciqid"] == df["shp_up"])) \
        | (df["shp_ciqid"].notna() & df["con_up"].notna() & (df["shp_ciqid"] == df["con_up"]))
    df["within_firm_type"] = pd.Series(
        np.select([is_self, is_ps & same_up, same_up], ["self", "parent_sub", "sibling"],
                  default=None), index=df.index, dtype="string")

    # --- CIQ 기업정보 (법인 + 최종모회사) ---
    for side in ("con", "shp"):
        for key, tag in ((f"{side}_ciqid", side), (f"{side}_up", f"{side}_up")):
            k = df[key]
            for src, dst in (("companyname", "name"), ("country_iso2", "country"),
                             ("industry", "industry"), ("company_type", "type")):
                if tag.endswith("_up") and src in ("industry", "company_type"):
                    continue          # 모회사는 이름·국가만 (열 절약)
                df[f"{tag}_ciq_{dst}"] = k.map(co[src])
    return df


def attach_keys(df: pd.DataFrame, per_by_ptype: dict, per: pd.DataFrame,
                mode: str = "equi") -> pd.DataFrame:
    """8블록에 **거래 도착일의 달력 연·분기로 붙은** 회계기간을 붙인다 (값은 나중에).

    V-6: `(cal_year, cal_quarter)` equi-join. 도착일이 속한 달력 분기(연간은 달력 연도)의
    재무를 붙인다. **판정이 아니라 사실이다** — "이 선적은 2024Q2 에 일어났고 그 회사의
    2024Q2 재무는 이것" 이라는 진술뿐이다.

    시점 문제(그 재무가 거래 시점에 공시됐는가)는 **`*_days_after_close` 와
    `*_period_end` 를 남겨** 분석 단계에서 처리한다 — Stata 에서 lag 을 주거나
    `days_after_close > 0` 으로 거르면 된다. `DECISIONS.md` V-6 참조.
    """
    if mode == "asof":
        # 명세 §3.3 — 도착일보다 먼저 끝난 회계기간 중 가장 최근(소급 2년, 당일 제외).
        # 컬럼 이름이 `*_age_days` 로 달라진다(equi 는 `*_days_after_close`).
        return attach_block_keys_asof(
            df, [(b[0], b[1], b[2]) for b in BLOCKS], per, "arrivaldate")
    cy = df["arrivaldate"].dt.year
    cq = df["arrivaldate"].dt.quarter
    for prefix, key, ptypes, _ in BLOCKS:
        right, jkey = per_by_ptype[ptypes]
        # ⚠️ 조인 컬럼 이름을 오른쪽 기준으로 맞춘다. 오른쪽의 cal_year·cal_quarter 를
        #    다른 이름으로 바꿔 버리면 아래 KEY_META 루프가 그 컬럼을 못 찾아
        #    `*_cal_year` 가 통째로 결측이 된다.
        left = pd.DataFrame({key: df[key], "_row": np.arange(len(df))})
        left["cal_year"] = cy.to_numpy()
        if "cal_quarter" in jkey:
            left["cal_quarter"] = cq.to_numpy()
        jc = [key if c == "companyid" else c for c in jkey]
        ok = left[jc].notna().all(axis=1)
        sub = left.loc[ok].copy()
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
        #    (같은 수정이 `scripts\common\finblocks.py` 에도 있다 — 함께 고쳐야 한다)
        no_hit = sub["financial_period_id"].isna()
        for c in ("cal_year", "cal_quarter"):
            if c in sub.columns:
                sub.loc[no_hit, c] = np.nan

        # 도착일 − 결산일. 양수 = 이미 끝난 기간 · 음수 = 아직 진행 중(공시 전)
        arr = pd.Series(df["arrivaldate"].to_numpy()[sub["_row"].to_numpy()],
                        index=sub.index)
        sub["days_after_close"] = (arr - sub["period_end"]).dt.days
        sub = sub.set_index("_row")
        pos = sub.index.values
        for c, dt in KEY_META.items():
            if c not in sub:                      # 연간 블록은 cal_quarter 가 없다
                df[f"{prefix}{c}"] = pd.array([pd.NA] * len(df), dtype=dt)
                continue
            if dt.startswith("Int"):
                # 결측을 담아야 하니 float 로 받아서 nullable 정수로 — 값은 전부 정수다
                raw = np.full(len(df), np.nan)
                raw[pos] = pd.to_numeric(sub[c], errors="coerce").to_numpy(dtype="float64")
                df[f"{prefix}{c}"] = pd.array(raw, dtype="Float64").astype(dt)
            else:
                s = pd.Series(index=df.index, dtype=dt)
                s.iloc[pos] = sub[c].values
                df[f"{prefix}{c}"] = s
        del left, sub, r, arr
    return df


# ---------------------------------------------------------------------------
# 3. 청크 단위 기록 — 1,400여 열을 통째로 메모리에 올리지 않는다
# ---------------------------------------------------------------------------
def _schema_with_pandas_meta(tbl: pa.Table, base: pd.DataFrame, n_base: int) -> pa.Schema:
    """`from_arrays` 가 버린 pandas 메타데이터를 복원한다.

    앞 `n_base` 개 컬럼은 base DataFrame 에서 온 것이라 dtype 을 그대로 적고, 뒤에 붙는
    재무열은 전부 float64 다. 이 메타데이터가 있어야 `pd.read_parquet` 이 `Int64`·`Int8`
    같은 nullable 정수형을 되살린다.
    """
    import json
    meta = pa.Table.from_pandas(base.head(0), preserve_index=False).schema.metadata
    md = json.loads(meta[b"pandas"].decode())
    for nm in tbl.schema.names[n_base:]:
        md["columns"].append({"name": nm, "field_name": nm, "pandas_type": "float64",
                              "numpy_type": "float64", "metadata": None})
    return tbl.schema.with_metadata({b"pandas": json.dumps(md).encode()})


def write_month(df: pd.DataFrame, path: Path, wide, val_cols, conv_cols) -> tuple:
    conv = set(conv_cols)
    # wide 를 한 번만 numpy 로 — 청크마다 410번씩 변환하지 않는다 (35k×410 ≈ 115MB)
    wide_np = wide[val_cols].to_numpy(dtype="float64")
    col_at = {c: i for i, c in enumerate(val_cols)}

    pos_cache = {}
    for prefix, _, _, with_values in BLOCKS:
        if not with_values:
            continue
        # ⚠️ 결측을 특정 숫자로 채워 넣으면 안 된다 — CIQ 는 **음수 ID 도 쓰므로**
        #    -1 같은 표식이 실제 ID 와 충돌할 수 있다. 결측은 마스크로 따로 처리한다.
        fid = df[f"{prefix}financial_period_id"]
        ok = fid.notna().to_numpy()
        probe = np.zeros(len(df), dtype="int64")
        probe[ok] = fid[ok].astype("int64").to_numpy()
        p = wide.index.get_indexer(pd.Index(probe))
        p[~ok] = -1                       # 결측 행은 무조건 '못 찾음'
        pos_cache[prefix] = p

    writer, schema, ncol = None, None, 0
    tmp = path.with_suffix(".tmp")
    try:
        for lo in range(0, len(df), CHUNK):
            hi = min(lo + CHUNK, len(df))
            arrays, names = [], []
            base = df.iloc[lo:hi]
            t = pa.Table.from_pandas(base, preserve_index=False)
            arrays += list(t.columns); names += list(t.schema.names)
            del t

            for prefix, _, _, with_values in BLOCKS:
                if not with_values:
                    continue
                p = pos_cache[prefix][lo:hi]
                fx = base[f"{prefix}fx_per_usd"].to_numpy(dtype="float64")
                miss = p < 0
                safe = np.where(miss, 0, p)
                for c in val_cols:
                    x = np.where(miss, np.nan, wide_np[safe, col_at[c]])
                    arrays.append(pa.array(x)); names.append(f"{prefix}{c}")
                    if c in conv:
                        arrays.append(pa.array(x / fx)); names.append(f"{prefix}{c}_usd")
            tbl = pa.Table.from_arrays(arrays, names=names)
            if writer is None:
                # ⚠️ from_arrays 는 pandas 메타데이터를 버린다. 그대로 두면 nullable 정수
                #    (Int64 등)가 읽을 때 float64 로 돌아와 `242432463.0` 처럼 보인다.
                #    base 쪽 메타데이터를 살려 재무열 항목만 이어붙인다.
                schema = _schema_with_pandas_meta(tbl, base, n_base=len(base.columns))
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


# ---------------------------------------------------------------------------
def month_list(start: str, end: str) -> list:
    rng = pd.date_range(start, end, freq="MS")
    return [d.strftime("%Y%m") for d in rng[:-1]] if len(rng) > 1 else []


def main() -> None:
    global TRADE, FIN
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01", help="미포함")
    ap.add_argument("--trade-dir", default=str(TRADE))
    ap.add_argument("--fin-dir", default=str(FIN))
    ap.add_argument("--out-dir", default=str(OUT))
    ap.add_argument("--override", default=None, help="승인된 crosswalk override 파일 (명세 §7)")
    ap.add_argument("--join", choices=["equi", "asof"], default="equi",
                    help="재무 결합 방식. equi=(회사,cal_year[,cal_quarter]) 일치, "
                         "asof=명세 §3.3 기준시점 직전 완료 기간(소급 2년)")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    TRADE, FIN = Path(a.trade_dir), Path(a.fin_dir)
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    months = month_list(a.start, a.end)
    t0 = datetime.now()
    print(f"[1] 참조 자료  (대상 {len(months)}개월: {months[0]}~{months[-1]}) "
          f"· 재무 결합 **{a.join}**")

    per, wide, val_cols, conv_cols = load_fin_layer()
    per_by_ptype = {pt: eq_frame(per, pt) for pt in {b[2] for b in BLOCKS}}
    co = load_company()
    override = pd.read_parquet(a.override) if a.override else None
    print(f"  override: {0 if override is None else len(override):,}건")

    print("\n[2] 월별 빌드")
    stats = []
    for ym in months:
        p = out / f"shipment_master_{ym}.parquet"
        if p.exists() and not a.force:
            print(f"  {p.name:<34} 건너뜀(존재)")
            continue
        ts = datetime.now()
        df = load_month(ym, override, co)
        n0, v0 = len(df), df["valueofgoodsusd"].sum()
        df = attach_keys(df, per_by_ptype, per, a.join)
        assert len(df) == n0, f"{ym}: 행 수 변동 {n0:,} → {len(df):,}"
        n, ncol = write_month(df, p, wide, val_cols, conv_cols)
        stats.append((ym, n, ncol, v0, p.stat().st_size))
        print(f"  {p.name:<34} {n:>9,}행 × {ncol:,}열  "
              f"{p.stat().st_size/1e6:>7.0f}MB  ({(datetime.now()-ts).seconds}s)")
        del df; gc.collect()

    tot = sum(s[1] for s in stats)
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) · {len(stats)}개월 {tot:,}행 → {out}")


if __name__ == "__main__":
    main()
