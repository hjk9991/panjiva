r"""
wf2q_20_build_facts.py — within-firm 파일럿(2024 H1) L2 선적사실 층

입력:  L0\imp_ship_*.parquet, imp_hs_*.parquet, exp_ship_*.parquet, exp_hs_*.parquet
산출:  L2\fact_shipment.parquet            수입, 1행 = 선적 1건 (이름 제외 — dim_company 참조)
       L2\fact_shipment_hs.parquet         수입, 1행 = 선적 × 고유 HS6, v1 균등 배분
       L2\fact_shipment_export.parquet     수출, 1행 = 선적 1건 (consignee 없음)
       L2\fact_shipment_export_hs.parquet  수출, 동일 구조
       L2\diag_facts.md                    검증 게이트 결과 (합계 추적표 포함)

검증 게이트 (실패 시 종료 코드 1):
  G1. 1주 대사 — 2024-03-01~07 슬라이스 = 254,873건 (1주치 파일럿 실측치와 정확 일치)
  G2. PK 유일 — record_id 중복 0
  G3. HS 배분 복원 — fact_shipment_hs 배분값 합 = fact_shipment 합 (HS 보유 선적 한정)
  검증은 행 수가 아니라 **합계**로 한다 (자식 조인 부풀림 함정, 팀 CLAUDE.md §2).

사용법:  python scripts\processing\wf2q_20_build_facts.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

STAGE = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q")
L0, L2 = STAGE / "L0", STAGE / "L2"
MONTHS = ["202401", "202402", "202403", "202404", "202405", "202406"]

RENAME_IMP = {
    "panjivarecordid": "record_id", "arrivaldate": "arrival_date",
    "billofladingtype": "bol_type",
    "conpanjivaid": "consignee_pcid", "concountry": "consignee_country",
    "shppanjivaid": "shipper_pcid", "shpcountry": "shipper_country",
    "shpmtorigin": "origin_country",
    "portofladingcountry": "port_lading_country", "portoflading": "port_lading",
    "portofunlading": "port_unlading",
    "weightkg": "weight_kg", "valueofgoodsusd": "value_usd", "volumeteu": "teu",
    "numberofcontainers": "n_containers",
    "hs6": "hs6_main", "hs2": "hs2_main",
    "con_companyid": "consignee_ciqid", "shp_companyid": "shipper_ciqid",
    "con_up": "consignee_up", "shp_up": "shipper_up",
    "con_ownership_is_fallback": "con_own_fallback",
    "shp_ownership_is_fallback": "shp_own_fallback",
}
DROP_IMP = ["conname", "shpname"]   # 이름은 dim_company 로 — fact 는 키·측정값만

RENAME_EXP = {
    "panjivarecordid": "record_id", "shpmtdate": "shipment_date",
    "shppanjivaid": "exporter_pcid", "shpcountry": "exporter_country",
    "shpcity": "exporter_city", "shpstateregion": "exporter_state",
    "shpmtdestination": "dest_reported",
    "portoflading": "port_lading", "portofladingcountry": "port_lading_country",
    "portofunlading": "port_unlading", "portofunladingcountry": "port_unlading_country",
    "weightkg": "weight_kg", "weightt": "weight_t", "volumeteu": "teu",
    "valueofgoodsusd": "value_usd", "iscontainerized": "is_containerized",
    "hs6": "hs6_main", "hs2": "hs2_main",
    "shp_companyid": "exporter_ciqid", "shp_up": "exporter_up",
    "shp_ownership_is_fallback": "exp_own_fallback",
}
DROP_EXP = ["shpname"]


def month_totals(df: pd.DataFrame, wcol: str = "weight_kg",
                 vcol: str = "value_usd") -> dict:
    return dict(n=df["record_id"].nunique(), w=df[wcol].sum(), v=df[vcol].sum())


def load_facts(kind: str, rename: dict, drop: list, datecol: str) -> pd.DataFrame:
    frames = []
    for m in MONTHS:
        f = L0 / f"{kind}_ship_{m}.parquet"
        if not f.exists():
            sys.exit(f"누락: {f}")
        df = pd.read_parquet(f).drop(columns=drop, errors="ignore").rename(columns=rename)
        frames.append(df)
        print(f"  {f.name}: {len(df):,}행")
    out = pd.concat(frames, ignore_index=True)
    out["ym"] = out[datecol].dt.strftime("%Y-%m")
    return out


def load_hs(kind: str) -> pd.DataFrame:
    frames = []
    for m in MONTHS:
        f = L0 / f"{kind}_hs_{m}.parquet"
        df = pd.read_parquet(f).rename(columns={"panjivarecordid": "record_id"})
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def build_hs_fact(hs: pd.DataFrame, fact: pd.DataFrame, label: str,
                  diag: list) -> pd.DataFrame:
    """v1 균등 배분: 선적의 고유 HS6 개수로 물량·금액을 나눈다."""
    n_per = hs.groupby("record_id")["hs6"].nunique().rename("n_hs6_rec")
    hs = hs.merge(n_per, on="record_id", how="left")
    meas = fact.set_index("record_id")[["weight_kg", "value_usd", "teu"]]
    hs = hs.join(meas, on="record_id")
    for c in ("weight_kg", "value_usd", "teu"):
        hs[f"{c.split('_')[0]}_alloc" if c != "value_usd" else "value_alloc"] = \
            hs[c] / hs["n_hs6_rec"]
    hs = hs.rename(columns={"weight_alloc": "weight_alloc"})
    hs["alloc_rule"] = "v1_equal"
    hs["single_hs"] = (hs["n_hs6_rec"] == 1).astype("int8")

    # G3 — 배분 복원: HS 를 가진 선적의 총계와 배분값 합이 일치해야 한다
    has_hs = fact[fact["record_id"].isin(hs["record_id"].unique())]
    for col, alloc in (("weight_kg", "weight_alloc"), ("value_usd", "value_alloc"),
                       ("teu", "teu_alloc")):
        tot, allocd = has_hs[col].sum(), hs[alloc].sum()
        ok = np.isclose(tot, allocd, rtol=1e-9, equal_nan=True) or \
            (pd.isna(tot) and pd.isna(allocd))
        diag.append(f"| G3 {label} {col} | {tot:,.1f} | {allocd:,.1f} | "
                    f"{'✅' if ok else '❌'} |")
        if not ok:
            sys.exit(f"[게이트 실패] {label} {col} 배분 복원 불일치: {tot} vs {allocd}")
    return hs.drop(columns=["weight_kg", "value_usd", "teu"])


def main() -> None:
    L2.mkdir(parents=True, exist_ok=True)
    diag = ["# L2 facts 빌드 진단 (wf2q_20_build_facts.py)", ""]

    # ---- 수입 -------------------------------------------------------------
    print("[1/4] fact_shipment (수입)")
    imp = load_facts("imp", RENAME_IMP, DROP_IMP, "arrival_date")

    # G2: PK 유일
    dup = imp["record_id"].duplicated().sum()
    diag.append(f"- G2 수입 PK 중복: **{dup}** (0 이어야 함)")
    if dup:
        sys.exit(f"[게이트 실패] 수입 record_id 중복 {dup}건")

    # G1: 1주 대사 — 1주치 파일럿 실측치와 정확 일치해야 한다
    wk = imp[(imp["arrival_date"] >= "2024-03-01") & (imp["arrival_date"] < "2024-03-08")]
    diag.append(f"- G1 1주 대사(2024-03-01~07): **{len(wk):,}건** (기대 254,873)")
    if len(wk) != 254_873:
        sys.exit(f"[게이트 실패] 1주 대사 불일치: {len(wk):,} ≠ 254,873")
    rel = wk["relationship"].value_counts(normalize=True)
    diag += ["  - 관계분류(1주 슬라이스, 건수 기준): "
             + ", ".join(f"{k} {v:.1%}" for k, v in rel.items())]

    # 파생: value_flag, within (self·parent_sub·sibling = 그룹 내)
    imp["value_flag"] = np.where(imp["value_usd"].notna(), "panjiva_est", "missing")
    imp["within_firm"] = (imp["relationship"].map(
        {"self": 1, "parent_sub": 1, "sibling": 1, "arms_length": 0}).astype("Int64"))
    # 미매칭은 0 이 아니라 결측 — 시장거래로 세면 안 된다

    # 월별 합계 추적표 (이후 층과의 대사 기준)
    diag += ["", "## 수입 월별 합계 (L2 기준선)", "",
             "| ym | 선적 | weight_kg | value_usd |", "|---|---|---|---|"]
    for ym, g in imp.groupby("ym"):
        diag.append(f"| {ym} | {g['record_id'].nunique():,} | "
                    f"{g['weight_kg'].sum():,.0f} | {g['value_usd'].sum():,.0f} |")

    imp.to_parquet(L2 / "fact_shipment.parquet", index=False)
    print(f"  -> fact_shipment.parquet {len(imp):,}행")

    # ---- 수입 HS ----------------------------------------------------------
    print("[2/4] fact_shipment_hs")
    diag += ["", "## HS 배분 복원 (G3)", "",
             "| 게이트 | fact 총계 | 배분 합 | 판정 |", "|---|---|---|---|"]
    imp_hs = build_hs_fact(load_hs("imp"), imp, "수입", diag)
    no_hs = len(imp) - imp["record_id"].isin(imp_hs["record_id"].unique()).sum()
    diag.append(f"\n- HS 자식이 없는 수입 선적: {no_hs:,}건 "
                f"({no_hs / len(imp):.2%}) — hs fact 에서 제외, fact_shipment 에는 존재")
    imp_hs.to_parquet(L2 / "fact_shipment_hs.parquet", index=False)
    print(f"  -> fact_shipment_hs.parquet {len(imp_hs):,}행")
    del imp_hs

    # ---- 수출 -------------------------------------------------------------
    print("[3/4] fact_shipment_export")
    exp = load_facts("exp", RENAME_EXP, DROP_EXP, "shipment_date")
    dup = exp["record_id"].duplicated().sum()
    diag.append(f"- G2 수출 PK 중복: **{dup}**")
    if dup:
        sys.exit(f"[게이트 실패] 수출 record_id 중복 {dup}건")

    exp["value_flag"] = np.where(exp["value_usd"].notna(), "panjiva_est", "missing")
    # 목적지: 기재값 우선, 없으면 양륙항 국가 (출처 플래그 유지 — 설계 §3.3 준용)
    exp["dest_country"] = exp["dest_reported"].fillna(exp["port_unlading_country"])
    exp["dest_source"] = np.select(
        [exp["dest_reported"].notna(), exp["port_unlading_country"].notna()],
        ["reported", "port_unlading"], default="missing")
    d = exp["dest_source"].value_counts(normalize=True)
    diag += ["", "## 수출 목적지 커버리지 (consignee 부재 — pair/within 불가)", "",
             "  " + ", ".join(f"{k} {v:.1%}" for k, v in d.items())]
    exp.to_parquet(L2 / "fact_shipment_export.parquet", index=False)
    print(f"  -> fact_shipment_export.parquet {len(exp):,}행")

    print("[4/4] fact_shipment_export_hs")
    diag += ["", "| 게이트 | fact 총계 | 배분 합 | 판정 |", "|---|---|---|---|"]
    exp_hs = build_hs_fact(load_hs("exp"), exp, "수출", diag)
    exp_hs.to_parquet(L2 / "fact_shipment_export_hs.parquet", index=False)
    print(f"  -> fact_shipment_export_hs.parquet {len(exp_hs):,}행")

    (L2 / "diag_facts.md").write_text("\n".join(diag) + "\n", encoding="utf-8")
    print(f"완료. 진단: {L2 / 'diag_facts.md'}")


if __name__ == "__main__":
    main()
