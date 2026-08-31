# -*- coding: utf-8 -*-
r"""
tom_v2_panels.py — 명세 04 산출물 **v2 (표준 통합 데이터층)**

명세 §8.2 대응. 선적 base 는 **v1 을 공용으로 사용**하고, 여기서는 집계 패널 3종만 만든다.

    02_pair.parquet    (수출자 법인, 수입자 법인, 분기) 1행 — 양측 법인·모회사 × 연간·분기 재무
    03_firm.parquet    (법인, 분기) 1행 — 자기 재무 + 그 법인의 최종모회사 재무
    04_group.parquet   (기업집단 = 최종모회사, 분기) 1행 — 모회사 재무

기간·경로가 전부 인자다(명세 §10). 재현 절차는 `scripts\RUNBOOK.md`.

## 김영수 연구원 확정 결정

  W-1 원천      = **v1 산출물**(`tom_v1_2024\shipment_master_*.parquet`). 명세 §8.2 가
                  "선적 base 는 v1 을 공용으로 사용" 이라 규정했다. 관계분류·기업정보가
                  이미 붙어 있어 v1·v2 총계가 정의상 대사된다(게이트 13).
  W-2 관계판정  = v1 이 공용함수로 계산한 것을 그대로 쓴다. 다시 판정하지 않는다.
  W-3 재무      = 세 패널 모두 **전 계정**(카탈로그 392) × 원표시 + USD.
                  02_pair 는 **8블록**(양측 × 법인·모회사 × 연간·분기) ≈ 6,400열,
                  03_firm 은 4블록(자기·모회사 × 연간·분기), 04_group 은 2블록.
  W-4 결합      = `(cal_year, cal_quarter)` **equi-join**. v1 과 같다(V-6).
                  기준일은 **분기 시작일**이며 `*_days_after_close` 로 시점을 판별한다.
  W-5 §5 변수   = `within_share_value` · `within_share_count` · `within_share` ·
                  `within_share_is_count_based` · `within_firm` · `relationship_mixed`
                  을 02_pair 에 넣는다. 명세 §5 는 pair×월(v3)을 말하지만 **같은 정의를
                  pair×분기에도 써서 두 버전이 같은 지표를 갖게** 한다.
  W-6 분모      = 구성비 분모는 **분류 가능 거래**(within_firm + arms_length)다(명세 §4.4).
                  미매칭 금액은 `*_value_unmatched` 로 따로 보존한다.
  W-7 self      = `within_firm` 에 포함하되 `*_value_self` · `*_n_self` 로 분리해 둔다.
                  "재고 이동일 뿐" 이라 판단되면 빼고 재계산할 수 있다.

⚠️ **03 과 04 를 서로 UNION 하지 않는다**(명세 §8.2). 같은 무역을 다른 단위로 집계한
   것이라 합치면 이중계상이다.
"""

import argparse
import gc
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from finblocks import (attach_block_keys, load_fin_layer,       # noqa: E402
                       write_with_blocks)

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
FIN = Path(r"C:\panjiva\data\staging\source\ciq_fin")
OUT = Path(r"C:\panjiva\data\staging\tom_v2_2024")

# v1 에서 읽어올 컬럼만 (1,425열 중 필요한 것만 — parquet 은 컬럼 단위로 읽는다)
SHIP_COLS = [
    "panjivarecordid", "arrivaldate", "valueofgoodsusd", "weightkg", "volumeteu",
    "con_ciqid", "shp_ciqid", "con_up", "shp_up",
    "relationship", "intra_group", "self_shipment", "within_firm_type",
    "hs6", "hs2", "shpmtorigin",
]
CO_COLS = ["companyid", "companyname", "country_iso2", "industry", "company_type"]

# (접두어, 회사 id 컬럼, periodTypeId, 값도 실을지)
BLOCKS_PAIR = [
    ("shp_a_",    "shp_ciqid", (1,),    True), ("shp_q_",    "shp_ciqid", (2, 10), True),
    ("shp_up_a_", "shp_up",    (1,),    True), ("shp_up_q_", "shp_up",    (2, 10), True),
    ("con_a_",    "con_ciqid", (1,),    True), ("con_q_",    "con_ciqid", (2, 10), True),
    ("con_up_a_", "con_up",    (1,),    True), ("con_up_q_", "con_up",    (2, 10), True),
]
BLOCKS_FIRM = [
    ("fin_a_", "companyid", (1,),    True), ("fin_q_", "companyid", (2, 10), True),
    ("up_a_",  "up",        (1,),    True), ("up_q_",  "up",        (2, 10), True),
]
BLOCKS_GROUP = [
    ("fin_a_", "ultimate_parent_companyid", (1,),    True),
    ("fin_q_", "ultimate_parent_companyid", (2, 10), True),
]


# ---------------------------------------------------------------------------
def load_ship(v1: Path, months) -> pd.DataFrame:
    parts = []
    for ym in months:
        f = v1 / f"shipment_master_{ym}.parquet"
        if not f.exists():
            raise SystemExit(f"v1 파일이 없다: {f}")
        parts.append(pd.read_parquet(f, columns=SHIP_COLS))
    df = pd.concat(parts, ignore_index=True)
    del parts
    df["trade_quarter"] = df["arrivaldate"].dt.to_period("Q").astype(str)
    df["cal_year"] = df["arrivaldate"].dt.year.astype("Int16")
    df["cal_quarter"] = df["arrivaldate"].dt.quarter.astype("Int8")
    return df


def quarter_start(labels: pd.Series) -> pd.Series:
    """분기 라벨(`2024Q2`) → 분기 시작일. 재무 시점 판별의 기준일이다."""
    return pd.Series(pd.PeriodIndex(labels.astype(str), freq="Q").start_time,
                     index=labels.index)


def rel_split(sub: pd.DataFrame, keys, prefix: str = "") -> pd.DataFrame:
    """관계별 건수·금액 분해 + 명세 §4.4 분모. self 는 within_firm 에 포함하되 분리 보존."""
    rel, val = sub["relationship"], sub["valueofgoodsusd"].fillna(0)
    w, a, u = rel.eq("within_firm"), rel.eq("arms_length"), rel.eq("unmatched")
    s = sub["self_shipment"].eq(1)
    g = sub.assign(
        _vw=val.where(w, 0), _va=val.where(a, 0), _vu=val.where(u, 0), _vs=val.where(s, 0),
        _nw=w.astype("int64"), _na=a.astype("int64"), _nu=u.astype("int64"),
        _ns=s.astype("int64"),
    ).groupby(keys, dropna=False)[["_vw", "_va", "_vu", "_vs",
                                   "_nw", "_na", "_nu", "_ns"]].sum()
    g.columns = [f"{prefix}{c}" for c in
                 ["value_within_firm", "value_arms", "value_unmatched", "value_self",
                  "n_within_firm", "n_arms", "n_unmatched", "n_self"]]
    vc = g[f"{prefix}value_within_firm"] + g[f"{prefix}value_arms"]
    nc = g[f"{prefix}n_within_firm"] + g[f"{prefix}n_arms"]
    g[f"{prefix}value_classified"] = vc
    g[f"{prefix}n_classified"] = nc
    # 명세 §5 — 금액 기준을 기본으로, 금액이 전부 결측·0 이면 건수 기준으로 대체
    sv = (g[f"{prefix}value_within_firm"] / vc).where(vc > 0)
    sn = (g[f"{prefix}n_within_firm"] / nc).where(nc > 0)
    g[f"{prefix}within_share_value"] = sv
    g[f"{prefix}within_share_count"] = sn
    g[f"{prefix}within_share"] = sv.fillna(sn)
    g[f"{prefix}within_share_is_count_based"] = (sv.isna() & sn.notna()).astype("int8")
    g[f"{prefix}within_firm"] = (g[f"{prefix}within_share"] > 0.5).astype("Int8") \
        .where(g[f"{prefix}within_share"].notna())
    g[f"{prefix}relationship_mixed"] = (
        (g[f"{prefix}n_within_firm"] > 0) & (g[f"{prefix}n_arms"] > 0)).astype("int8")
    return g.reset_index()


def finalize_counts(p: pd.DataFrame) -> pd.DataFrame:
    """수입·수출 블록을 outer join 하면 한쪽만 있는 회사에 결측이 생긴다.

    - **건수·금액·중량은 0** 으로 채운다. "그 분기에 그쪽 거래가 없었다"는 뜻이고,
      결측으로 두면 `imp + exp` 합계가 통째로 결측이 된다.
    - **비율·플래그는 결측 그대로** 둔다. 거래가 없으면 정의되지 않는다.
    - 건수는 nullable 정수로 되돌린다(결측 때문에 float64 가 되어 `12.0` 처럼 보인다).
    """
    zero = [c for c in p.columns
            if c.startswith(("imp_", "exp_"))
            and any(k in c for k in ("n_ship", "n_partners", "n_members",
                                     "n_partner_groups", "value_", "weight_kg", "teu",
                                     "n_within_firm", "n_arms", "n_unmatched",
                                     "n_self", "n_classified"))]
    p[zero] = p[zero].fillna(0)
    for c in p.columns:
        if c.startswith(("imp_n_", "exp_n_", "n_")) and pd.api.types.is_float_dtype(p[c]):
            p[c] = p[c].round().astype("Int64")
    for c in p.columns:
        if c.endswith(("relationship_mixed", "within_share_is_count_based")):
            p[c] = pd.to_numeric(p[c], errors="coerce").astype("Int8")
    return p


def top_by_value(df: pd.DataFrame, keys, col: str, out: str) -> pd.DataFrame:
    """분기 안에서 **금액가중 최빈값**. 동점이면 사전순 앞선 값(재현 가능하게)."""
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=list(keys) + [out])
    return (sub.groupby(list(keys) + [col], dropna=False)["valueofgoodsusd"].sum()
               .reset_index()
               .sort_values(["valueofgoodsusd", col], ascending=[False, True],
                            kind="mergesort")
               .drop_duplicates(keys)
               .rename(columns={col: out})[list(keys) + [out]])


def add_company(panel: pd.DataFrame, co: pd.DataFrame, key: str, prefix: str):
    c = co.set_index("companyid")
    for src, dst in (("companyname", "name"), ("country_iso2", "country"),
                     ("industry", "industry"), ("company_type", "type")):
        panel[f"{prefix}{dst}"] = panel[key].map(c[src])
    return panel


# ---------------------------------------------------------------------------
def build_pair(ship: pd.DataFrame) -> pd.DataFrame:
    """(수출자 법인, 수입자 법인, 분기). 양측이 다 식별된 선적만."""
    s = ship[ship.con_ciqid.notna() & ship.shp_ciqid.notna()]
    gk = ["shp_ciqid", "con_ciqid", "trade_quarter"]
    p = s.groupby(gk, dropna=False).agg(
        n_ship=("panjivarecordid", "nunique"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
        n_hs6=("hs6", "nunique"),
        n_relationship=("relationship", "nunique"),
    ).reset_index()
    p = p.merge(rel_split(s, gk), on=gk, how="left")
    for col, out in (("hs2", "top_hs2"), ("hs6", "top_hs6"),
                     ("shpmtorigin", "top_origin"), ("within_firm_type", "top_within_type")):
        p = p.merge(top_by_value(s, gk, col, out), on=gk, how="left")
    # 분기 내 UP — 금액가중 최빈값. 흔들리면 up_changed 로 표시한다(명세 §6.3 예비)
    for side in ("shp", "con"):
        u = top_by_value(s[s[f"{side}_up"].notna()], gk, f"{side}_up", f"{side}_up")
        n = (s.groupby(gk)[f"{side}_up"].nunique().rename(f"{side}_up_changed")
             .reset_index())
        p = p.merge(u, on=gk, how="left").merge(n, on=gk, how="left")
        p[f"{side}_up_changed"] = (p[f"{side}_up_changed"] > 1).astype("int8")
    for c in p.columns:
        if c.startswith("n_") and pd.api.types.is_float_dtype(p[c]):
            p[c] = p[c].round().astype("Int64")
    return p


def build_firm(ship: pd.DataFrame) -> pd.DataFrame:
    """(법인, 분기). 수입 측·수출 측을 각각 집계해 outer join."""
    blocks = []
    for tag, key, other in (("imp", "con_ciqid", "shp_ciqid"),
                            ("exp", "shp_ciqid", "con_ciqid")):
        s = ship[ship[key].notna()]
        gk = [key, "trade_quarter"]
        b = s.groupby(gk).agg(**{
            f"{tag}_n_ship": ("panjivarecordid", "nunique"),
            f"{tag}_value_usd": ("valueofgoodsusd", "sum"),
            f"{tag}_weight_kg": ("weightkg", "sum"),
            f"{tag}_teu": ("volumeteu", "sum"),
            f"{tag}_n_partners": (other, "nunique"),
        }).reset_index()
        b = b.merge(rel_split(s, gk, f"{tag}_"), on=gk, how="left")
        for col, out in (("hs2", f"{tag}_top_hs2"),
                         ("shpmtorigin", f"{tag}_top_partner_country")):
            b = b.merge(top_by_value(s, gk, col, out), on=gk, how="left")
        up = "con_up" if key == "con_ciqid" else "shp_up"
        b = b.merge(top_by_value(s[s[up].notna()], gk, up, "up"), on=gk, how="left")
        blocks.append(b.rename(columns={key: "companyid"}))
    f = blocks[0].merge(blocks[1], on=["companyid", "trade_quarter"], how="outer",
                        suffixes=("", "_y"))
    # 수입·수출 양쪽에서 up 이 잡히면 하나로 (같은 회사이므로 같아야 한다)
    if "up_y" in f.columns:
        f["up"] = f["up"].fillna(f["up_y"])
        f = f.drop(columns=[c for c in f.columns if c.endswith("_y")])
    return finalize_counts(f)


def build_group(ship: pd.DataFrame) -> pd.DataFrame:
    """(기업집단 = 최종모회사, 분기). 모회사 자신의 재무만 붙인다 — 합산 금지."""
    blocks = []
    for tag, key, other, member in (("imp", "con_up", "shp_up", "con_ciqid"),
                                    ("exp", "shp_up", "con_up", "shp_ciqid")):
        s = ship[ship[key].notna()]
        gk = [key, "trade_quarter"]
        b = s.groupby(gk).agg(**{
            f"{tag}_n_ship": ("panjivarecordid", "nunique"),
            f"{tag}_value_usd": ("valueofgoodsusd", "sum"),
            f"{tag}_weight_kg": ("weightkg", "sum"),
            f"{tag}_teu": ("volumeteu", "sum"),
            f"{tag}_n_members": (member, "nunique"),
            f"{tag}_n_partner_groups": (other, "nunique"),
        }).reset_index()
        b = b.merge(rel_split(s, gk, f"{tag}_"), on=gk, how="left")
        for col, out in (("hs2", f"{tag}_top_hs2"),
                         ("shpmtorigin", f"{tag}_top_partner_country")):
            b = b.merge(top_by_value(s, gk, col, out), on=gk, how="left")
        blocks.append(b.rename(columns={key: "ultimate_parent_companyid"}))
    return finalize_counts(
        blocks[0].merge(blocks[1], on=["ultimate_parent_companyid", "trade_quarter"],
                        how="outer"))


# ---------------------------------------------------------------------------
def month_list(start: str, end: str) -> list:
    rng = pd.date_range(start, end, freq="MS")
    return [d.strftime("%Y%m") for d in rng[:-1]] if len(rng) > 1 else []


def main() -> None:
    global V1, FIN
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01", help="미포함")
    ap.add_argument("--v1-dir", default=str(V1), help="선적 base (v1 산출물)")
    ap.add_argument("--fin-dir", default=str(FIN))
    ap.add_argument("--out-dir", default=str(OUT))
    ap.add_argument("--only", nargs="*", choices=["02_pair", "03_firm", "04_group"])
    ap.add_argument("--join", choices=["equi", "asof"], default="equi",
                    help="재무 결합 방식. asof 는 명세 §3.3(분기 시작일 직전 완료 기간)")
    a = ap.parse_args()
    V1, FIN = Path(a.v1_dir), Path(a.fin_dir)
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    months = month_list(a.start, a.end)
    t0 = datetime.now()

    print(f"[1] 입력 — v1 {len(months)}개월 ({months[0]}~{months[-1]}) "
          f"· 재무 결합 **{a.join}**")
    ship = load_ship(V1, months)
    print(f"  선적 {len(ship):,}건 · 금액 ${ship.valueofgoodsusd.sum()/1e9:,.1f}B")
    layer = load_fin_layer(FIN)
    co = pd.read_parquet(Path(r"C:\panjiva\data\staging\source\ciq_ref\company.parquet"),
                         columns=CO_COLS).drop_duplicates("companyid")

    targets = a.only or ["02_pair", "03_firm", "04_group"]
    specs = {
        "02_pair":  (build_pair,  BLOCKS_PAIR,
                     [("shp_ciqid", "shp_"), ("con_ciqid", "con_"),
                      ("shp_up", "shp_up_"), ("con_up", "con_up_")]),
        "03_firm":  (build_firm,  BLOCKS_FIRM,
                     [("companyid", ""), ("up", "up_")]),
        "04_group": (build_group, BLOCKS_GROUP,
                     [("ultimate_parent_companyid", "")]),
    }
    print("\n[2] 패널 빌드")
    for name in targets:
        ts = datetime.now()
        fn, blocks, cinfo = specs[name]
        p = fn(ship)
        p["period_start"] = quarter_start(p["trade_quarter"])
        p["cal_year"] = p["period_start"].dt.year.astype("Int16")
        p["cal_quarter"] = p["period_start"].dt.quarter.astype("Int8")
        for key, prefix in cinfo:
            p = add_company(p, co, key, f"{prefix}ciq_")
        p = attach_block_keys(p, [(b[0], b[1], b[2]) for b in blocks],
                              layer["per"], "period_start", "cal_year", "cal_quarter",
                              mode=a.join)
        p.columns = [str(c).lower() for c in p.columns]
        n, ncol = write_with_blocks(p, out / f"{name}.parquet", blocks, layer)
        sz = (out / f"{name}.parquet").stat().st_size
        print(f"  {name}.parquet  {n:>9,}행 × {ncol:,}열  {sz/1e6:>6.0f}MB  "
              f"({(datetime.now()-ts).seconds}s)")
        del p; gc.collect()

    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {out}")


if __name__ == "__main__":
    main()
