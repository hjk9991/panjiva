# -*- coding: utf-8 -*-
r"""
wf_v3_panels.py — 명세 04 산출물 **v3 (공식 within-firm 분석패널)**

명세 §8.3 대응. 산출 폴더는 명세 §2.2 가 지정한 `within_firm_pilot_2024\`.

    panel_pair_month.parquet     1행 = 수입자 법인 × 수출자 법인 × **월**
    dim_relationship.parquet     1행 = pair (관계 이력 요약 + 관계전환 = 명세 §6.4)
    panel_firm_quarter.parquet   1행 = 법인 × 분기 (v2 03_firm + within 전용 지표)
    panel_firm_origin_hs.parquet 1행 = 수입기업 × 원산국 × HS6 (균등배분)

기간·경로가 전부 인자다(명세 §10). 재현 절차는 `scripts\RUNBOOK.md`.

## 김영수 연구원 확정 결정

  X-1 원천      = 수입 선적은 **v1 산출물**, HS 자식은 **공용 원천** `imp_hs_YYYYMM`.
                  관계분류는 v1 이 공용함수로 계산한 것을 그대로 쓴다(다시 판정하지 않음).
  X-2 firm 패널 = **v2 `03_firm` 을 base 로 읽어** within 전용 지표만 더한다.
                  기업×분기를 두 번 집계하지 않으므로 v2·v3 총계가 정의상 대사된다.
                  더하는 것: `hhi_partners` · `n_entry` · `n_exit` · `n_origin_countries`.
  X-3 HS 배분   = **균등배분 유지**(선적 금액 ÷ 그 선적의 고유 HS6 개수) + `single_hs`
                  플래그. 총액이 보존되고, 나중에 `single_hs==1` 만 써서 재계산할 수 있다.
  X-4 pair 단위 = **법인(ciqid) 쌍**. 양측이 다 CIQ 매칭된 선적만. 월 단위(명세 §5).
  X-5 §5 변수   = v2 와 **같은 정의**를 쓴다 — `rel_split()` 로직 공유.
  X-6 redaction = 기존 `is_redacted`(이름 패턴)는 **0.0001% 만 잡아 실측 38.9% 와 어긋난다**.
                  당사자 블록 전체 결측(`panjivaid` 없음)을 `party_missing` 으로 따로 센다.
  X-7 게이트    = 하드코딩된 대사값(1주 254,873건)을 **원천 실측 대사**로 바꾼다.

⚠️ 수출은 **기업×목적지 보조자료로만** 쓴다(명세 §2.1). 수출 B/L 에는 상대방 식별자가
   없어 pair·within-firm 판정이 구조적으로 불가능하다.
"""

import argparse
import gc
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from relationship import RELATIONSHIP_VALUES  # noqa: E402,F401  (값 목록 문서화용)

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V2 = Path(r"C:\panjiva\data\staging\tom_v2_2024")
SRC = Path(r"C:\panjiva\data\staging\source\trade_2024")
OUT = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")

SHIP_COLS = ["panjivarecordid", "arrivaldate", "trade_quarter", "cal_year", "cal_quarter",
             "valueofgoodsusd", "weightkg", "volumeteu",
             "conpanjivaid", "shppanjivaid", "con_ciqid", "shp_ciqid", "con_up", "shp_up",
             "con_up_ciq_country", "shp_up_ciq_country",
             "relationship", "self_shipment", "within_firm_type",
             "hs6", "n_hs6", "shpmtorigin"]


# ---------------------------------------------------------------------------
# 공통 — v2 와 같은 §5 지표 정의
# ---------------------------------------------------------------------------
def rel_split(sub: pd.DataFrame, keys, prefix: str = "") -> pd.DataFrame:
    """관계별 건수·금액 분해 + 명세 §5 지표. v2 `tom_v2_panels.rel_split` 과 같은 정의."""
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


def top_by_value(df: pd.DataFrame, keys, col: str, out: str) -> pd.DataFrame:
    """금액가중 최빈값. 동점이면 사전순 앞선 값(재현 가능하게)."""
    sub = df[df[col].notna()]
    if not len(sub):
        return pd.DataFrame(columns=list(keys) + [out])
    return (sub.groupby(list(keys) + [col], dropna=False)["valueofgoodsusd"].sum()
               .reset_index()
               .sort_values(["valueofgoodsusd", col], ascending=[False, True],
                            kind="mergesort")
               .drop_duplicates(keys)
               .rename(columns={col: out})[list(keys) + [out]])


def to_int(p: pd.DataFrame, pref=("n_",)) -> pd.DataFrame:
    for c in p.columns:
        if c.startswith(pref) and pd.api.types.is_float_dtype(p[c]):
            p[c] = p[c].round().astype("Int64")
    return p


# ---------------------------------------------------------------------------
# 1. panel_pair_month — 명세 §5
# ---------------------------------------------------------------------------
def build_pair_month(ship: pd.DataFrame) -> pd.DataFrame:
    s = ship[ship.con_ciqid.notna() & ship.shp_ciqid.notna()]
    gk = ["con_ciqid", "shp_ciqid", "ym"]
    p = s.groupby(gk, dropna=False).agg(
        n_shipments=("panjivarecordid", "nunique"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
        n_hs6=("hs6", "nunique"),
        n_origin=("shpmtorigin", "nunique"),
        n_relationship=("relationship", "nunique"),
    ).reset_index()
    p = p.merge(rel_split(s, gk), on=gk, how="left")
    for col, out in (("hs6", "hs6_main"), ("shpmtorigin", "origin_main"),
                     ("within_firm_type", "within_type_main")):
        p = p.merge(top_by_value(s, gk, col, out), on=gk, how="left")

    # 월 안에서 최종모회사가 흔들리면 금액가중 최빈값 + 변동 플래그 (명세 §6.3 예비)
    for side in ("con", "shp"):
        u = top_by_value(s[s[f"{side}_up"].notna()], gk, f"{side}_up", f"{side}_up")
        n = s.groupby(gk)[f"{side}_up"].nunique().rename(f"{side}_up_changed").reset_index()
        p = p.merge(u, on=gk, how="left").merge(n, on=gk, how="left")
        p[f"{side}_up_changed"] = (p[f"{side}_up_changed"] > 1).astype("int8")
    # 수출자 최종모회사 국적 — 거래시점 UP 기준(PIT+대체), ISO2 코드
    c = top_by_value(s[s.shp_up_ciq_country.notna()], gk,
                     "shp_up_ciq_country", "shp_up_country")
    p = p.merge(c, on=gk, how="left")
    # kor_mnc_link: 수출자 모회사가 한국 기업이면서 그룹내 거래인 pair-월
    p["kor_mnc_link"] = ((p.shp_up_country == "KR") & (p.within_firm == 1)).astype("int8")

    p["pair_id"] = (p.con_ciqid.astype("int64").astype(str) + "_"
                    + p.shp_ciqid.astype("int64").astype(str))
    return to_int(p)


# ---------------------------------------------------------------------------
# 2. dim_relationship — pair 이력 + 명세 §6.4 관계전환
# ---------------------------------------------------------------------------
def transitions(ship: pd.DataFrame) -> pd.DataFrame:
    """명세 §6.4 관계전환 — **선적 단위**로 센다.

    "동일 (consignee_ciqid, shipper_ciqid) pair 의 **분류 가능 선적**을 날짜순으로 정렬한다.
     직전 분류 가능 선적과 값이 달라질 때마다 전환 1건으로 센다."

    ⚠️ 월 단위로 세면 같은 달 안에서 일어난 전환을 놓친다. 명세는 선적 단위다.
    ⚠️ `unmatched` 선적은 **건너뛴다**(분류 불가라 비교 대상이 아니다). 명세가 "분류 가능
       선적을" 이라고 못박았다 — 중간에 미매칭이 끼어도 전환으로 세지 않는다.
    """
    s = ship[ship.con_ciqid.notna() & ship.shp_ciqid.notna()
             & ship.relationship.isin(["within_firm", "arms_length"])].copy()
    s["pair_id"] = (s.con_ciqid.astype("int64").astype(str) + "_"
                    + s.shp_ciqid.astype("int64").astype(str))
    s["wf"] = s.relationship.eq("within_firm").astype("int8")
    # 같은 날짜가 여럿이면 순서가 임의가 되므로 record id 로 동점을 깬다(재현 가능하게)
    s = s.sort_values(["pair_id", "arrivaldate", "panjivarecordid"], kind="mergesort")
    s["prev"] = s.groupby("pair_id")["wf"].shift()
    s["chg"] = s.prev.notna() & (s.wf != s.prev)

    g = s.groupby("pair_id")
    tr = g.agg(n_classified_shipments=("wf", "size"),
               _min=("wf", "min"), _max=("wf", "max"),
               n_transitions=("chg", "sum"),
               first_state=("wf", "first"), last_state=("wf", "last")).reset_index()
    w2a = s[s.chg & (s.wf == 0)].groupby("pair_id").size().rename("within_to_arms")
    a2w = s[s.chg & (s.wf == 1)].groupby("pair_id").size().rename("arms_to_within")
    tr = tr.merge(w2a, on="pair_id", how="left").merge(a2w, on="pair_id", how="left")
    tr[["within_to_arms", "arms_to_within"]] =         tr[["within_to_arms", "arms_to_within"]].fillna(0)
    # 연중 within_firm 과 arms_length 가 **모두** 관측되면 1 (명세 §6.4)
    tr["relationship_changed_2024"] = ((tr._min == 0) & (tr._max == 1)).astype("int8")
    tr["value_transitioned"] = s[s.chg].groupby("pair_id")["valueofgoodsusd"].sum()         .reindex(tr.pair_id).fillna(0).to_numpy()
    return tr.drop(columns=["_min", "_max"])


def build_relationship(pm: pd.DataFrame, ship: pd.DataFrame, months: list) -> pd.DataFrame:
    """pair 단위 요약 + 명세 §6.4 관계전환(선적 단위)."""
    gk = ["pair_id", "con_ciqid", "shp_ciqid"]
    d = pm.sort_values(gk + ["ym"], kind="mergesort")
    r = d.groupby(gk, dropna=False).agg(
        first_ym=("ym", "min"), last_ym=("ym", "max"),
        n_active_months=("ym", "nunique"),
        n_shipments=("n_shipments", "sum"),
        value_usd=("value_usd", "sum"),
        value_within_firm=("value_within_firm", "sum"),
        value_arms=("value_arms", "sum"),
        value_classified=("value_classified", "sum"),
        n_within_firm=("n_within_firm", "sum"),
        n_arms=("n_arms", "sum"),
        n_classified=("n_classified", "sum"),
        ever_within=("within_firm", "max"),
        kor_mnc_link=("kor_mnc_link", "max"),
        n_up_changed_months=("shp_up_changed", "sum"),
    ).reset_index()
    r["within_share_value"] = (r.value_within_firm / r.value_classified).where(
        r.value_classified > 0)
    r["within_share_count"] = (r.n_within_firm / r.n_classified).where(r.n_classified > 0)
    r["within_share"] = r.within_share_value.fillna(r.within_share_count)

    r = r.merge(transitions(ship), on="pair_id", how="left")

    # --- 활동 구간(spell) 과 절단 ---
    o = d[gk[:1] + ["ym"]].drop_duplicates().sort_values(["pair_id", "ym"])
    idx = pd.Index(months)
    o["_p"] = idx.get_indexer(o.ym)
    o["_gap"] = o.groupby("pair_id")["_p"].diff().fillna(0) > 1
    r = r.merge(o.groupby("pair_id")["_gap"].sum().add(1).rename("n_spells").reset_index(),
                on="pair_id", how="left")
    r["left_censored"] = (r.first_ym == months[0]).astype("int8")
    r["right_censored"] = (r.last_ym == months[-1]).astype("int8")

    for col, out in (("hs6_main", "hs6_main"), ("origin_main", "origin_main")):
        t = (pm.groupby(["pair_id", col], dropna=False)["value_usd"].sum().reset_index()
               .sort_values(["value_usd", col], ascending=[False, True], kind="mergesort")
               .drop_duplicates("pair_id").rename(columns={col: out})[["pair_id", out]])
        r = r.merge(t, on="pair_id", how="left")
    return to_int(r)


# ---------------------------------------------------------------------------
# 3. panel_firm_quarter — v2 03_firm + within 전용 지표 (X-2)
# ---------------------------------------------------------------------------
def firm_extra(ship: pd.DataFrame) -> pd.DataFrame:
    """within 전용 지표만 계산한다 — 나머지는 v2 03_firm 이 이미 갖고 있다."""
    extra = []
    for tag, key, other in (("imp", "con_ciqid", "shp_ciqid"),
                            ("exp", "shp_ciqid", "con_ciqid")):
        s = ship[ship[key].notna() & ship[other].notna()]
        gk = [key, "trade_quarter"]
        # 파트너 집중도 HHI — 파트너별 금액 점유율 제곱합 (1 = 한 파트너에 전량)
        pv = s.groupby(gk + [other])["valueofgoodsusd"].sum().reset_index()
        tot = pv.groupby(gk)["valueofgoodsusd"].transform("sum")
        pv["_sh2"] = (pv.valueofgoodsusd / tot.where(tot > 0)) ** 2
        b = pv.groupby(gk)["_sh2"].sum().rename(f"{tag}_hhi_partners").reset_index()
        # 원산국 수 (수입 방향만 의미 있음)
        if tag == "imp":
            b = b.merge(s.groupby(gk)["shpmtorigin"].nunique()
                        .rename("imp_n_origin_countries").reset_index(), on=gk, how="left")
        # 진입·이탈 — 직전/다음 분기와 파트너 집합을 비교한다.
        #   진입 = 이 분기에 있는데 **직전 분기에 없던** 파트너
        #   이탈 = 이 분기에 있는데 **다음 분기에 없는** 파트너
        # ⚠️ 표본 첫 분기의 진입, 마지막 분기의 이탈은 **판정 불가**다(비교 대상이 없다).
        #    0 으로 채우면 "아무도 안 들어왔다"로 오독되므로 결측으로 둔다.
        qs = sorted(ship.trade_quarter.unique())
        qi = pd.Index(qs)
        cur = pv[gk + [other]].copy()
        cur["_qi"] = qi.get_indexer(cur.trade_quarter)
        link = cur[[key, other, "_qi"]]

        # 직전 분기 존재 여부: t 시점 행을 t+1 로 옮겨 두면 t 에서 만나는 것이 t-1 의 기록
        prev = link.assign(_qi=link._qi + 1, _prev=1)
        m = cur.merge(prev, on=[key, other, "_qi"], how="left")
        ent = (m[m._prev.isna()].groupby(gk).size().rename(f"{tag}_n_entry").reset_index())

        nxt = link.assign(_qi=link._qi - 1, _next=1)
        m2 = cur.merge(nxt, on=[key, other, "_qi"], how="left")
        ext = (m2[m2._next.isna()].groupby(gk).size().rename(f"{tag}_n_exit").reset_index())

        b = b.merge(ent, on=gk, how="left").merge(ext, on=gk, how="left")
        b[f"{tag}_n_entry"] = b[f"{tag}_n_entry"].fillna(0)
        b[f"{tag}_n_exit"] = b[f"{tag}_n_exit"].fillna(0)
        extra.append(b.rename(columns={key: "companyid"}))

    e = extra[0].merge(extra[1], on=["companyid", "trade_quarter"], how="outer")
    # 그 방향 거래가 아예 없던 회사는 건수가 0 이다(결측 아님).
    # 반면 **표본 첫 분기의 진입 · 마지막 분기의 이탈은 판정 자체가 불가능**하다
    # (비교할 직전/다음 분기가 표본 밖). 값 0 과 구분되도록 플래그를 따로 둔다.
    for c in e.columns:
        if c.endswith(("_n_entry", "_n_exit", "_n_origin_countries")):
            e[c] = e[c].fillna(0).round().astype("Int64")
    qs = sorted(ship.trade_quarter.dropna().unique())
    e["entry_assessable"] = (e.trade_quarter != qs[0]).astype("int8")
    e["exit_assessable"] = (e.trade_quarter != qs[-1]).astype("int8")
    return e


def write_firm_quarter(extra: pd.DataFrame, v2_dir: Path, path: Path,
                       chunk: int = 100_000) -> tuple:
    """v2 `03_firm` 을 그대로 흘려보내면서 within 지표 열만 덧붙여 쓴다.

    03_firm 은 2,632열이라 통째로 메모리에 올리면 13GB 다. 행 청크로 읽고 쓴다.
    키가 1:1 이므로 **행 수는 변하지 않는다**(아래에서 검증).
    """
    ex = extra.copy()
    ex["companyid"] = ex["companyid"].astype("int64")
    ex["trade_quarter"] = ex["trade_quarter"].astype(str)
    ex = ex.set_index(["companyid", "trade_quarter"])
    cols = list(ex.columns)

    pf = pq.ParquetFile(v2_dir / "03_firm.parquet")
    writer, schema, n, hit = None, None, 0, 0
    tmp = path.with_suffix(".tmp")
    try:
        for b in pf.iter_batches(batch_size=chunk):
            d = b.to_pandas()
            key = pd.MultiIndex.from_arrays(
                [d["companyid"].astype("int64"), d["trade_quarter"].astype(str)])
            sub = ex.reindex(key)
            for c in cols:
                v = sub[c].to_numpy()
                d[c] = pd.array(v, dtype="Float64").astype(ex[c].dtype) \
                    if str(ex[c].dtype).startswith("Int") else v
            hit += int(sub[cols].notna().any(axis=1).sum())
            t = pa.Table.from_pandas(d, preserve_index=False)
            if writer is None:
                schema = t.schema
                writer = pq.ParquetWriter(tmp, schema, compression="zstd")
            writer.write_table(t.cast(schema))
            n += len(d)
            del d, sub, t
            gc.collect()
    finally:
        if writer is not None:
            writer.close()
        pf.close()
    tmp.replace(path)
    return n, hit


# ---------------------------------------------------------------------------
# 4. panel_firm_origin_hs — 균등배분 (X-3)
# ---------------------------------------------------------------------------
def build_origin_hs(ship: pd.DataFrame, src: Path, months: list) -> pd.DataFrame:
    """수입기업 × 원산국 × HS6. 다중 HS 선적은 **균등배분**하고 `single_hs` 를 남긴다."""
    base = ship.loc[ship.con_ciqid.notna(),
                    ["panjivarecordid", "con_ciqid", "shpmtorigin", "relationship",
                     "valueofgoodsusd", "weightkg", "volumeteu", "trade_quarter"]]
    parts = []
    for ym in months:
        h = pd.read_parquet(src / f"imp_hs_{ym}.parquet", columns=["panjivarecordid", "hs6"])
        parts.append(h.drop_duplicates())
    hs = pd.concat(parts, ignore_index=True).drop_duplicates()
    del parts
    n = hs.groupby("panjivarecordid")["hs6"].nunique().rename("n_hs6_rec")
    hs = hs.merge(n, on="panjivarecordid")
    j = base.merge(hs, on="panjivarecordid", how="inner")
    for c in ("valueofgoodsusd", "weightkg", "volumeteu"):
        j[c] = j[c] / j["n_hs6_rec"]                    # 균등배분
    j["single_hs"] = (j.n_hs6_rec == 1).astype("int8")

    gk = ["con_ciqid", "origin_country", "hs6"]
    j = j.rename(columns={"shpmtorigin": "origin_country"})
    g = j.groupby(gk, dropna=False).agg(
        n_shipments=("panjivarecordid", "nunique"),
        value_usd=("valueofgoodsusd", "sum"),
        weight_kg=("weightkg", "sum"),
        teu=("volumeteu", "sum"),
    ).reset_index()
    w = j[j.relationship.eq("within_firm")].groupby(gk, dropna=False).agg(
        value_within_firm=("valueofgoodsusd", "sum")).reset_index()
    a = j[j.relationship.eq("arms_length")].groupby(gk, dropna=False).agg(
        value_arms=("valueofgoodsusd", "sum")).reset_index()
    sg = j[j.single_hs == 1].groupby(gk, dropna=False).agg(
        value_usd_single_hs=("valueofgoodsusd", "sum"),
        n_shipments_single_hs=("panjivarecordid", "nunique")).reset_index()
    for x in (w, a, sg):
        g = g.merge(x, on=gk, how="left")
    for c in ["value_within_firm", "value_arms", "value_usd_single_hs"]:
        g[c] = g[c].fillna(0)
    g["n_shipments_single_hs"] = g["n_shipments_single_hs"].fillna(0)
    g["value_classified"] = g.value_within_firm + g.value_arms
    g["within_share_value"] = (g.value_within_firm / g.value_classified).where(
        g.value_classified > 0)
    g["alloc_rule"] = "equal_split"
    return to_int(g)


# ---------------------------------------------------------------------------
def month_list(start: str, end: str) -> list:
    rng = pd.date_range(start, end, freq="MS")
    return [d.strftime("%Y%m") for d in rng[:-1]] if len(rng) > 1 else []


def main() -> None:
    global V1, V2, SRC
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01", help="미포함")
    ap.add_argument("--v1-dir", default=str(V1))
    ap.add_argument("--v2-dir", default=str(V2))
    ap.add_argument("--src-dir", default=str(SRC))
    ap.add_argument("--out-dir", default=str(OUT))
    a = ap.parse_args()
    V1, V2, SRC = Path(a.v1_dir), Path(a.v2_dir), Path(a.src_dir)
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    months = month_list(a.start, a.end)
    t0 = datetime.now()

    print(f"[1] 선적 base — v1 {len(months)}개월 ({months[0]}~{months[-1]})")
    parts = [pd.read_parquet(V1 / f"shipment_master_{m}.parquet", columns=SHIP_COLS)
             for m in months]
    ship = pd.concat(parts, ignore_index=True); del parts
    ship["ym"] = ship.arrivaldate.dt.strftime("%Y%m")
    print(f"  선적 {len(ship):,}건 · ${ship.valueofgoodsusd.sum()/1e9:,.1f}B")

    print("\n[2] panel_pair_month")
    pm = build_pair_month(ship)
    pm.to_parquet(out / "panel_pair_month.parquet", index=False, compression="zstd")
    print(f"  {len(pm):,}행 × {pm.shape[1]}열")

    print("[3] dim_relationship")
    rel = build_relationship(pm, ship, months)
    rel.to_parquet(out / "dim_relationship.parquet", index=False, compression="zstd")
    print(f"  {len(rel):,}행 × {rel.shape[1]}열")

    print("[4] panel_firm_quarter — v2 03_firm + within 지표")
    ex = firm_extra(ship)
    print(f"  within 지표 {len(ex):,}행 × {ex.shape[1]-2}개")
    n_fq, hit = write_firm_quarter(ex, V2, out / "panel_firm_quarter.parquet")
    nc = len(pq.ParquetFile(out / "panel_firm_quarter.parquet").schema_arrow.names)
    v2n = pq.ParquetFile(V2 / "03_firm.parquet").metadata.num_rows
    assert n_fq == v2n, f"행 수가 변했다 {v2n:,} -> {n_fq:,}"
    print(f"  {n_fq:,}행 × {nc:,}열 (v2 03_firm {v2n:,}행과 일치) · 지표 부착 {hit:,}행")

    print("[5] panel_firm_origin_hs")
    fo = build_origin_hs(ship, SRC, months)
    fo.to_parquet(out / "panel_firm_origin_hs.parquet", index=False, compression="zstd")
    print(f"  {len(fo):,}행 × {fo.shape[1]}열")

    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {out}")


if __name__ == "__main__":
    main()
