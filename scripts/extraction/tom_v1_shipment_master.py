# -*- coding: utf-8 -*-
r"""
tom_v1_shipment_master.py — 업무지시서 03 / 명세 04 산출물 **v1 (선적 감사층)**

한 행 = 미국 해상 수입 선적 1건. 그 행에 양 당사자(수출자·수입자)의 매칭상태·관계분류와
재무를 붙인다. 명세 `04_2024연간파일럿_통합명세.md` §8.1 대응.

기간·경로가 전부 인자다(명세 §10) — H1 하드코딩 없음. `--start/--end` 를 주지 않으면
원천 폴더에 있는 **모든 월**(파일명 `imp_ship_YYYYMM.parquet` 로 발견)을 만든다.

## 김영수 연구원 확정 결정 (2026-08-21)

  V-1 원천      = `data\staging\source\trade\imp_ship_YYYYMM.parquet` (공용 원천, 2007-07~)
                  표준필터(미국 실착·통과화물 제외)는 원천에서 이미 적용됐다.
  V-2 관계판정  = `scripts\common\relationship.py` 공용함수. v1·v2·v3 가 같은 함수를 쓴다.
                  3분류(within_firm / arms_length / unmatched) + 2단계 매칭상태 +
                  `unmatched_reason` + `self_shipment`.
                  parent_sub·sibling 은 명세 §1.4 대로 **참고용 `within_firm_type`** 로 보존.
  V-3 재무소스  = `data\staging\source\ciq_fin\` 공용 재무층 (전 계정 4,517개).
  V-4 재무구성  = **모회사 × 연간** 2블록(수입자·수출자)에 **카탈로그 392계정 전부**를
                  원표시+USD 로 싣는다. 명세 §1.7 이 이 블록을 주 재무변수로 지정했다.
                  나머지 6블록(법인×연간·분기, 모회사×분기)은 `financial_period_id` 키만
                  싣는다 — 그 키로 공용 재무층에서 **4,517계정 어느 것이든** 꺼내 붙일 수
                  있고 as-of 판정을 다시 하지 않는다.
  V-5 USD 환산  = `unit_type_id==2`(백만 단위 금액성) 중 주식수 6종을 뺀 **245계정**만
                  `value / fx_per_usd`. 비율·성장률·주당지표는 원표시만 두고 `fx_per_usd` 동봉.
                  ⚠️ `fx_per_usd` 는 **1 USD 당 현지통화**다(KRW 1383, JPY 161). 곱하면 안 된다.
  V-6 결합방식  = **`(cal_year, cal_quarter)` equi-join.** 도착일이 속한 달력 분기(연간은
                  달력 연도)의 재무를 붙인다. as-of 를 쓰지 않는다 — as-of 는 "소급 2년",
                  "결산 당일 제외" 같은 **판정**을 값 안에 녹여 되돌릴 수 없게 만든다.
                  병합 단계는 사실만 담고, 시점 판단(lag)은 Stata 분석 단계에서 한다.
                  근거 자료로 `*_period_end` 와 `*_days_after_close`(도착일−결산일, 음수면
                  아직 진행 중)를 남긴다. ⚠️ 명세 §3.3 은 as-of 를 요구 — `--join asof` 로
                  같은 코드에서 as-of 판(`*_age_days`)을 만든다(2026-08-28, 정본).
  V-7 분기블록  = periodTypeId **2(분기) + 10(반기)**. 반기만 내는 기업을 버리지 않기 위함이며
                  `*_q_period_type_id` 로 어느 쪽인지 항상 식별된다. 연간블록은 1만 쓴다.
  V-8 override  = 명세 §7. `--override` 로 준 §7.2 csv 에서 **`status=approved` 이고
                  `pi_approved_by` 가 있는 행만** 적용한다(나머지는 건수만 로그).
                  `action=replace` 는 `*_ciqid` 를 `replacement_companyid` 로, `force_unmatched`
                  는 `*_ciqid` 를 결측으로. `effective_start/end` 가 있으면 도착일이
                  `[start, end)` 안일 때만. 원본은 `*_ciqid_original` 로 항상 보존하고(§4.1)
                  `*_crosswalk_overridden=1`. 교체된 행은 `*_up`·`*_ownership_is_fallback` 을
                  PIT(닫힌 구간) → 스냅샷 순으로 **다시 조회**하고 관계분류는 그 뒤에 판정한다.
                  영향표 `override_impact.csv`(+`_detail`) 를 출력 폴더에 쓴다.
                  파일이 없으면 적용 0건.

사용:
  python scripts\extraction\tom_v1_shipment_master.py                      # 원천의 전 기간
  python ... --start 2024-01-01 --end 2024-03-01                             # 일부 월만
  python ... --join asof --out-dir ...\tom_v1_2024_asof                      # 명세 §3.3 판
  python ... --override "shared memory\BECRS_Matching_Project\crosswalk_overrides.csv" --force
  python ... --start 2024-01-01 --end 2024-02-01 --nrows 300000 --out-dir <scratch>   # 스모크
"""

import argparse
import gc
import io
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "processing"))
from relationship import add_relationship          # noqa: E402
from finblocks import attach_block_keys_asof         # noqa: E402  (as-of 경로만 위임)
from v4_common import discover_months                # noqa: E402  (월 목록은 디스크가 안다)

SRC = Path(r"C:\panjiva\data\staging\source")
TRADE = SRC / "trade"
CIQ = SRC / "ciq_ref"
FIN = SRC / "ciq_fin"
OUT = Path(r"C:\panjiva\data\staging\tom_v1_2024")

CHUNK = 400_000                        # 행 청크 — 1,400열을 통째로 올리지 않는다 (`--chunk`)
NROWS = None                           # 디버그 전용 `--nrows`: 월 파일의 앞 N 행만

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

# 명세 §7.2 override 파일 스키마 — 전부 있어야 한다 (없으면 예외)
OVERRIDE_COLS = [
    "override_id", "panjiva_company_id", "original_companyid", "replacement_companyid",
    "action", "effective_start", "effective_end", "reason_code", "evidence",
    "proposed_by", "proposed_at", "reviewed_by_01", "reviewed_by_02",
    "pi_approved_by", "pi_approved_at", "status", "notes",
]
OVERRIDE_ACTIONS = ("replace", "force_unmatched")

# `override_impact.csv` 열 (한 행 요약. 0건이면 전부 0)
IMPACT_COLS = [
    "run_at", "months_built", "override_file",
    "n_override_rows_total",          # 파일의 전체 행
    "n_override_rows_approved",       # status=approved & pi_approved_by 있음 → 적용 대상
    "n_override_rows_not_applied",    # 나머지 (proposed·rejected·retired·PI 미승인)
    "n_override_rows_hit",            # 적용 대상 중 실제로 선적에 적중한 행
    "n_panjiva_companies",            # 수정된 Panjiva 기업 수 (적중한 panjiva_company_id)
    "n_shipments",                    # 어느 한쪽이라도 수정된 선적 수
    "n_shipments_con", "n_shipments_shp",
    "value_usd",                      # 그 선적들의 금액 합
    "n_shipments_up_changed",         # `*_up`(family) 이 교체 전과 달라진 선적
    "n_shipments_relationship_changed",   # `relationship` 이 교체 전 판정과 달라진 선적
    "n_shipments_force_unmatched",    # action=force_unmatched 로 결측 처리된 선적
    "n_original_mismatch",            # override 의 original_companyid 가 실제 원본과 다른 선적 (참고)
    "n_pit_multi_interval",           # 재조회 시 PIT 구간이 둘 이상 겹친 건수 (최근 start 채택)
]


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
    # 4,081만 행 × 문자열 4열이라 `drop_duplicates` 복사만으로 피크가 4GB 늘어난다.
    # companyid 가 이미 유일하면(실측 그렇다) 복사 없이 그대로 쓴다 — 결과는 같다.
    ids = co["companyid"].to_numpy()
    if len(np.unique(ids)) != len(ids):
        co = co.drop_duplicates("companyid")
    return co.set_index("companyid")


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
# 1b. 명세 §7 override — 로더 · UP 재조회 · 영향 집계
# ---------------------------------------------------------------------------
def load_override(path) -> tuple:
    """§7.2 스키마의 csv(정본; `.parquet` 도 허용)를 읽어 **적용 대상 행만** 돌려준다.

    적용 대상 = `status == 'approved'` 이고 `pi_approved_by` 가 비어 있지 않은 행(§7.1).
    나머지는 건수만 세어 `info` 로 돌려준다(적용 0건 — 게이트 12).
    스키마 컬럼이 하나라도 없으면 예외. `action` 이 replace/force_unmatched 밖이거나,
    replace 인데 `replacement_companyid` 가 없거나, 같은 Panjiva 기업의 적용기간이
    겹치면(어느 것을 적용할지 모호) 예외.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"override 파일이 없다: {p}")
    if p.suffix.lower() == ".parquet":
        ov = pd.read_parquet(p)
    else:
        ov = pd.read_csv(p, dtype="string", encoding="utf-8-sig")
    missing = [c for c in OVERRIDE_COLS if c not in ov.columns]
    if missing:
        raise ValueError(f"override 파일에 명세 §7.2 컬럼이 없다: {missing} ({p})")

    def s(c):
        return ov[c].astype("string").str.strip()

    status = s("status").str.lower()
    pi = s("pi_approved_by").fillna("")
    approved = (status == "approved").fillna(False) & pi.ne("").fillna(False)
    info = {
        "file": str(p), "n_total": int(len(ov)), "n_approved": int(approved.sum()),
        "n_approved_without_pi": int(((status == "approved").fillna(False) & pi.eq("")).sum()),
        "by_status": {str(k): int(v) for k, v in
                      status.fillna("(빈값)").value_counts().items()},
    }
    ap = ov.loc[approved].copy()
    for c in ("override_id", "action", "reason_code"):
        ap[c] = s(c)[approved]
    ap["action"] = ap["action"].str.lower()
    if not len(ap):
        for c in ("panjiva_company_id", "original_companyid", "replacement_companyid"):
            ap[c] = pd.array([], dtype="Int64")
        for c in ("effective_start", "effective_end"):
            ap[c] = pd.array([], dtype="datetime64[ns]")
        return ap.reset_index(drop=True), info

    if ap["override_id"].isna().any() or ap["override_id"].duplicated().any():
        raise ValueError("approved 행의 override_id 가 비었거나 중복이다")
    bad = ~ap["action"].isin(OVERRIDE_ACTIONS)
    if bad.any():
        raise ValueError(f"action 은 {OVERRIDE_ACTIONS} 중 하나여야 한다: "
                         f"{ap.loc[bad, ['override_id', 'action']].to_dict('records')}")
    ap["panjiva_company_id"] = pd.to_numeric(ap["panjiva_company_id"],
                                             errors="raise").astype("Int64")
    if ap["panjiva_company_id"].isna().any():
        raise ValueError("approved 행에 panjiva_company_id 결측")
    ap["original_companyid"] = pd.to_numeric(ap["original_companyid"],
                                             errors="coerce").astype("Int64")
    ap["replacement_companyid"] = pd.to_numeric(ap["replacement_companyid"],
                                                errors="coerce").astype("Int64")
    need = (ap["action"] == "replace") & ap["replacement_companyid"].isna()
    if need.any():
        raise ValueError(f"action=replace 인데 replacement_companyid 결측: "
                         f"{ap.loc[need, 'override_id'].tolist()}")
    ap.loc[ap["action"] == "force_unmatched", "replacement_companyid"] = pd.NA
    for c in ("effective_start", "effective_end"):
        ap[c] = pd.to_datetime(ap[c], errors="raise").astype("datetime64[ns]")
    both = ap["effective_start"].notna() & ap["effective_end"].notna()
    if (ap.loc[both, "effective_start"] >= ap.loc[both, "effective_end"]).any():
        raise ValueError("effective_start >= effective_end 인 override 가 있다")
    # 같은 Panjiva 기업에 approved 행이 여럿이면 적용기간이 겹치면 안 된다
    for pid, g in ap.groupby("panjiva_company_id"):
        if len(g) < 2:
            continue
        st = g["effective_start"].fillna(pd.Timestamp.min).to_numpy()
        en = g["effective_end"].fillna(pd.Timestamp.max).to_numpy()
        o = np.argsort(st, kind="mergesort")
        if (st[o][1:] < en[o][:-1]).any():
            raise ValueError(f"panjiva_company_id={pid} 의 approved override 적용기간이 겹친다: "
                             f"{g['override_id'].tolist()}")
    return ap.reset_index(drop=True), info


class UpLookup:
    r"""교체된 companyid 의 거래일 기준 최종모회사 재조회 (명세 §3.2, 원천 SQL 과 같은 규칙).

    1순위 `ownership_pit` — 도착일이 `[start_date, end_date]` **닫힌 구간** 안인 행
      (원천 `src_trade_pull.py` 와 동일: `>= startDate and <= endDate`. 근거는
      `source\DECISIONS.md` T-10 — 다음 구간이 정확히 1초 뒤 시작하므로 배타로 하면 공백).
    2순위 `ownership_snapshot` — PIT 가 없을 때만, `*_ownership_is_fallback=1`
      (원천과 같이 "crosswalk 있고 PIT 없음" 이면 스냅샷 유무와 무관하게 1).

    대상 companyid 로 먼저 걸러 읽으므로(PIT 4,366만 행 전체를 올리지 않는다) 메모리가 작다.
    """

    def __init__(self, ciq_dir: Path, companyids):
        ids = sorted({int(x) for x in companyids if pd.notna(x)})
        self.ids = ids
        cols_p = ["companyid", "ultimate_parent_companyid", "start_date", "end_date"]
        if not ids:
            self.pit = pd.DataFrame({c: pd.Series(dtype="int64") for c in cols_p})
            self.snap = {}
            return
        flt = [("companyid", "in", ids)]
        pit = pq.read_table(Path(ciq_dir) / "ownership_pit.parquet", columns=cols_p,
                            filters=flt).to_pandas()
        # `end_date` 에 9999-12-31(개방 구간) 이 있어 ns 로 바로 바꾸면 넘친다 → 2100-01-01 로 자른다
        ed = pit["end_date"].to_numpy().astype("datetime64[us]")
        pit["end_date"] = np.minimum(ed, np.datetime64("2100-01-01", "us")).astype("datetime64[ns]")
        pit["start_date"] = pit["start_date"].to_numpy().astype("datetime64[ns]")
        pit["companyid"] = pit["companyid"].astype("int64")
        pit["ultimate_parent_companyid"] = pit["ultimate_parent_companyid"].astype("int64")
        self.pit = pit.sort_values(["companyid", "start_date"]).reset_index(drop=True)
        snap = pq.read_table(Path(ciq_dir) / "ownership_snapshot.parquet",
                             columns=["companyid", "ultimate_parent_companyid"],
                             filters=flt).to_pandas().dropna().drop_duplicates("companyid")
        self.snap = dict(zip(snap["companyid"].astype("int64"),
                             snap["ultimate_parent_companyid"].astype("int64")))
        print(f"  override UP 재조회 준비: 대상 companyid {len(ids):,} · PIT 구간 {len(pit):,} · "
              f"스냅샷 {len(self.snap):,}")

    def lookup(self, companyid, date) -> tuple:
        """→ (`up` Int64, `is_fallback` Int8, PIT 다중 적중 건수). companyid 결측이면 둘 다 결측/0."""
        cid = pd.array(companyid, dtype="Int64")
        d = pd.Series(date).astype("datetime64[ns]").to_numpy()
        n = len(cid)
        up = np.full(n, np.nan)
        ok = ~pd.isna(cid)
        q = pd.DataFrame({"_i": np.flatnonzero(ok),
                          "companyid": np.asarray(cid[ok], dtype="int64"), "d": d[ok]})
        m = q.merge(self.pit, on="companyid", how="inner")
        m = m[(m["d"] >= m["start_date"]) & (m["d"] <= m["end_date"])]
        n_multi = int(m["_i"].duplicated().sum())
        m = m.sort_values(["_i", "start_date"], kind="mergesort").drop_duplicates("_i", keep="last")
        hit = np.zeros(n, dtype=bool)
        hit[m["_i"].to_numpy()] = True
        up[m["_i"].to_numpy()] = m["ultimate_parent_companyid"].to_numpy(dtype="float64")
        fb = ok & ~hit                              # crosswalk 있고 PIT 없음 → 스냅샷 대체
        if fb.any():
            s = pd.Series(np.asarray(cid[fb], dtype="int64")).map(self.snap)
            up[np.flatnonzero(fb)] = s.to_numpy(dtype="float64", na_value=np.nan)
        up_arr = pd.array(up, dtype="Float64").astype("Int64")
        return up_arr, pd.array(fb.astype("int8"), dtype="Int8"), n_multi


class OverrideContext:
    """한 실행 동안의 override 상태 — 적용 대상 표, UP 재조회기, 영향 누적."""

    def __init__(self, path, ciq_dir: Path):
        self.ov, self.info = load_override(path)
        self.uplook = UpLookup(ciq_dir, self.ov["replacement_companyid"].dropna())
        self.pids, self.oids = set(), set()
        self.n = dict.fromkeys(["n_shipments", "n_shipments_con", "n_shipments_shp",
                                "n_shipments_up_changed", "n_shipments_relationship_changed",
                                "n_shipments_force_unmatched", "n_original_mismatch",
                                "n_pit_multi_interval"], 0)
        self.value = 0.0
        self.detail = {}          # (override_id, side) → [n_shipments, value]

    def hits(self, df: pd.DataFrame, side: str) -> pd.DataFrame:
        """이 달 선적 중 `{side}panjivaid` 가 적용 대상이고 도착일이 적용기간 안인 행."""
        pid = df[f"{side}panjivaid"]
        ids = self.ov["panjiva_company_id"].astype("int64").tolist()
        cand = pid.notna().to_numpy() & pid.isin(ids).to_numpy(dtype=bool, na_value=False)
        empty = pd.DataFrame(columns=["_row", "override_id", "action", "replacement_companyid",
                                      "original_companyid"])
        if not cand.any():
            return empty
        left = pd.DataFrame({"_row": np.flatnonzero(cand),
                             "panjiva_company_id": pid[cand].astype("int64").to_numpy(),
                             "arr": df["arrivaldate"].to_numpy()[cand]})
        r = self.ov[["override_id", "panjiva_company_id", "action", "replacement_companyid",
                     "original_companyid", "effective_start", "effective_end"]].copy()
        r["panjiva_company_id"] = r["panjiva_company_id"].astype("int64")
        m = left.merge(r, on="panjiva_company_id", how="inner")
        inr = ((m["effective_start"].isna() | (m["arr"] >= m["effective_start"]))
               & (m["effective_end"].isna() | (m["arr"] < m["effective_end"])))
        m = m.loc[inr]
        if m["_row"].duplicated().any():           # 로더가 겹침을 막았으므로 오면 버그다
            raise RuntimeError(f"{side}: 한 선적에 override 가 둘 이상 적중")
        return m.reset_index(drop=True) if len(m) else empty


def _neq(a, b) -> np.ndarray:
    """결측을 값으로 보는 부등 비교 (NA vs NA 는 같음, NA vs 값은 다름)."""
    a, b = pd.array(a, dtype="Int64"), pd.array(b, dtype="Int64")
    na_a, na_b = pd.isna(a), pd.isna(b)
    both = ~na_a & ~na_b
    out = na_a != na_b
    out[both] = np.asarray(a[both], dtype="int64") != np.asarray(b[both], dtype="int64")
    return np.asarray(out, dtype=bool)


def apply_override(df: pd.DataFrame, ctx: OverrideContext) -> pd.DataFrame:
    """V-8 — 원본은 손대지 않고 `*_ciqid`·`*_up`·`*_ownership_is_fallback` 만 바꾼다.

    관계분류는 이 함수 **뒤에** `add_relationship` 이 계산하므로 자동 반영된다. 영향표를
    위해 교체 전 값으로 한 번 더 판정해 `relationship`·`*_up` 변경 선적을 센다.
    """
    hits = {side: ctx.hits(df, side) for side in ("con", "shp")}
    rows = [h["_row"].to_numpy(dtype="int64") for h in hits.values() if len(h)]
    if not rows:
        return df
    rows = np.unique(np.concatenate(rows))
    rel_cols = ["con_ciqid", "shp_ciqid", "con_up", "shp_up"]
    before = df.iloc[rows][rel_cols].reset_index(drop=True)
    before_rel = add_relationship(before)["relationship"]

    n_force = np.zeros(len(rows), dtype=bool)
    for side in ("con", "shp"):
        h = hits[side]
        if not len(h):
            continue
        r = h["_row"].to_numpy(dtype="int64")
        # 참고: override 가 적은 원본 ID 와 실제 원본이 다르면 센다 (crosswalk 가 그새 바뀐 경우)
        orig_doc = h["original_companyid"]
        orig_real = df[f"{side}_ciqid_original"].iloc[r]
        chk = orig_doc.notna().to_numpy()
        ctx.n["n_original_mismatch"] += int(_neq(orig_doc[chk], orig_real[chk]).sum())

        new_id = pd.array(h["replacement_companyid"], dtype="Int64")     # force → NA
        up, fb, n_multi = ctx.uplook.lookup(new_id, df["arrivaldate"].iloc[r])
        ctx.n["n_pit_multi_interval"] += n_multi
        df.loc[r, f"{side}_ciqid"] = new_id
        df.loc[r, f"{side}_up"] = up
        df.loc[r, f"{side}_ownership_is_fallback"] = fb
        df.loc[r, f"{side}_crosswalk_overridden"] = pd.array(np.ones(len(r), dtype="int8"),
                                                              dtype="Int8")
        ctx.n[f"n_shipments_{side}"] += len(r)
        forced = h["action"].eq("force_unmatched").to_numpy()
        n_force[np.searchsorted(rows, r)] |= forced
        g = h.assign(v=df["valueofgoodsusd"].to_numpy()[r]).groupby(
            ["override_id", "panjiva_company_id", "action"], dropna=False)["v"] \
            .agg(["size", "sum"])
        for (oid, pid, act), row in g.iterrows():
            k = (str(oid), int(pid), str(act), side)
            d = ctx.detail.setdefault(k, [0, 0.0])
            d[0] += int(row["size"]); d[1] += float(row["sum"])
            ctx.oids.add(str(oid)); ctx.pids.add(int(pid))

    after = df.iloc[rows][rel_cols].reset_index(drop=True)
    after_rel = add_relationship(after)["relationship"]
    up_chg = _neq(before["con_up"], after["con_up"]) | _neq(before["shp_up"], after["shp_up"])
    rel_chg = (before_rel.fillna("") != after_rel.fillna("")).to_numpy()
    ctx.n["n_shipments"] += len(rows)
    ctx.n["n_shipments_up_changed"] += int(up_chg.sum())
    ctx.n["n_shipments_relationship_changed"] += int(rel_chg.sum())
    ctx.n["n_shipments_force_unmatched"] += int(n_force.sum())
    ctx.value += float(np.nansum(df["valueofgoodsusd"].to_numpy(dtype="float64")[rows]))
    return df


def write_override_impact(out: Path, ctx, months_built) -> Path:
    """§7.3 영향표. override 가 없어도(0건) 0 으로 채운 1행을 쓴다 — 게이트 12 가 읽는다."""
    row = {c: 0 for c in IMPACT_COLS}
    row.update({"run_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "months_built": " ".join(months_built), "override_file": "", "value_usd": 0.0})
    if ctx is not None:
        row.update({"override_file": ctx.info["file"],
                    "n_override_rows_total": ctx.info["n_total"],
                    "n_override_rows_approved": ctx.info["n_approved"],
                    "n_override_rows_not_applied": ctx.info["n_total"] - ctx.info["n_approved"],
                    "n_override_rows_hit": len(ctx.oids),
                    "n_panjiva_companies": len(ctx.pids), "value_usd": ctx.value})
        row.update(ctx.n)
    p = out / "override_impact.csv"
    pd.DataFrame([row])[IMPACT_COLS].to_csv(p, index=False, encoding="utf-8")
    det = out / "override_impact_detail.csv"
    dcols = ["override_id", "panjiva_company_id", "action", "side", "n_shipments", "value_usd"]
    drows = [{"override_id": k[0], "panjiva_company_id": k[1], "action": k[2], "side": k[3],
              "n_shipments": v[0], "value_usd": v[1]}
             for k, v in sorted((ctx.detail if ctx else {}).items())]
    pd.DataFrame(drows, columns=dcols).to_csv(det, index=False, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# 2. 월별 선적층
# ---------------------------------------------------------------------------
_NULLABLE_INT = {"int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
                 "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32"}


def _read_head(path: Path, nrows: int) -> pd.DataFrame:
    """디버그 전용 — 월 파일의 앞 `nrows` 행만. 잘라낸 표를 같은 스키마(pandas 메타 포함)로
    메모리 buffer 에 다시 써서 `pd.read_parquet` 로 읽으므로 전체 읽기와 dtype 이 같다."""
    pf = pq.ParquetFile(path)
    batches, got = [], 0
    for b in pf.iter_batches(batch_size=min(nrows, 262_144)):
        batches.append(b)
        got += b.num_rows
        if got >= nrows:
            break
    tbl = pa.Table.from_batches(batches, schema=pf.schema_arrow).slice(0, nrows)
    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    buf.seek(0)
    return pd.read_parquet(buf)


def load_month(ym: str, co: pd.DataFrame, ovctx: OverrideContext | None = None) -> pd.DataFrame:
    path = TRADE / f"imp_ship_{ym}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"원천 월 파일이 없다: {path}")
    # 원천의 정수 컬럼을 nullable 정수로 — 결측 때문에 float64 로 뭉개지는 것을 막는다
    schema = pq.ParquetFile(path).schema_arrow
    int_cast = {f.name: _NULLABLE_INT[str(f.type)] for f in schema
                if pa.types.is_integer(f.type) and str(f.type) in _NULLABLE_INT}
    df = (_read_head(path, NROWS) if NROWS else pd.read_parquet(path)).astype(int_cast)
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
    if ovctx is not None and len(ovctx.ov):
        df = apply_override(df, ovctx)

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
    # wide 를 한 번만 numpy 로 — 청크마다 392번씩 변환하지 않는다 (35k×392 ≈ 110MB)
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


def resolve_months(trade_dir: Path, start, end) -> list:
    """`--start/--end` 가 있으면 그 월들(원천에 없는 월이 있으면 예외), 없으면 디스크에서 발견."""
    found = list(discover_months([trade_dir], "imp_ship"))
    if not found:
        raise FileNotFoundError(f"원천 폴더에 imp_ship_YYYYMM.parquet 이 없다: {trade_dir}")
    if start is None and end is None:
        return found
    if start is None:
        start = f"{found[0][:4]}-{found[0][4:]}-01"
    if end is None:
        last = pd.Timestamp(f"{found[-1][:4]}-{found[-1][4:]}-01") + pd.offsets.MonthBegin(1)
        end = last.strftime("%Y-%m-%d")
    months = month_list(start, end)
    if not months:
        raise SystemExit(f"기간이 비었다: --start {start} --end {end}")
    missing = [m for m in months if m not in set(found)]
    if missing:
        raise FileNotFoundError(
            f"요청한 {len(months)}개월 중 원천에 없는 월 {len(missing)}개: "
            f"{', '.join(missing[:12])}{' …' if len(missing) > 12 else ''} ({trade_dir})")
    return months


def main() -> None:
    global TRADE, FIN, CIQ, CHUNK, NROWS
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=None,
                    help="시작월(YYYY-MM-DD). 미지정이면 원천 폴더의 첫 월")
    ap.add_argument("--end", default=None,
                    help="종료(미포함). 미지정이면 원천 폴더의 마지막 월까지")
    ap.add_argument("--trade-dir", default=str(TRADE), help="공용 무역 원천 (imp_ship_YYYYMM.parquet)")
    ap.add_argument("--fin-dir", default=str(FIN), help="공용 재무층")
    ap.add_argument("--ciq-dir", default=str(CIQ),
                    help="CIQ 참조 (company · ownership_pit · ownership_snapshot)")
    ap.add_argument("--out-dir", default=str(OUT))
    ap.add_argument("--override", default=None,
                    help="명세 §7.2 crosswalk_overrides.csv (parquet 도 가능). "
                         "status=approved & pi_approved_by 있는 행만 적용")
    ap.add_argument("--join", choices=["equi", "asof"], default="equi",
                    help="재무 결합 방식. equi=(회사,cal_year[,cal_quarter]) 일치, "
                         "asof=명세 §3.3 기준시점 직전 완료 기간(소급 2년)")
    ap.add_argument("--force", action="store_true", help="이미 있는 월도 다시 만든다")
    ap.add_argument("--chunk", type=int, default=CHUNK,
                    help=f"기록 청크 행 수(기본 {CHUNK:,}). 메모리가 적으면 줄인다 — 값은 안 바뀐다")
    ap.add_argument("--nrows", type=int, default=None,
                    help="[디버그·스모크 전용] 각 월 파일의 앞 N 행만 읽는다. "
                         "실산출에는 절대 쓰지 말 것")
    a = ap.parse_args()

    TRADE, FIN, CIQ = Path(a.trade_dir), Path(a.fin_dir), Path(a.ciq_dir)
    CHUNK, NROWS = a.chunk, a.nrows
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    months = resolve_months(TRADE, a.start, a.end)
    t0 = datetime.now()
    print(f"[1] 참조 자료  (대상 {len(months)}개월: {months[0]}~{months[-1]}) "
          f"· 재무 결합 **{a.join}**"
          + (f" · ⚠️ 디버그: 월당 앞 {NROWS:,}행만" if NROWS else ""))

    per, wide, val_cols, conv_cols = load_fin_layer()
    per_by_ptype = {pt: eq_frame(per, pt) for pt in {b[2] for b in BLOCKS}}
    co = load_company()
    ovctx = OverrideContext(a.override, CIQ) if a.override else None
    if ovctx is None:
        print("  override: 파일 없음 → 적용 0건")
    else:
        i = ovctx.info
        print(f"  override: {i['n_total']:,}행 중 적용 대상(approved+PI 승인) {i['n_approved']:,}"
              f" · 미적용 {i['n_total']-i['n_approved']:,} {i['by_status']}"
              + (f" · ⚠️ approved 인데 pi_approved_by 없음 {i['n_approved_without_pi']}"
                 if i["n_approved_without_pi"] else ""))

    print("\n[2] 월별 빌드")
    stats = []
    for ym in months:
        p = out / f"shipment_master_{ym}.parquet"
        if p.exists() and not a.force:
            print(f"  {p.name:<34} 건너뜀(존재)")
            continue
        ts = datetime.now()
        df = load_month(ym, co, ovctx)
        n0, v0 = len(df), df["valueofgoodsusd"].sum()
        df = attach_keys(df, per_by_ptype, per, a.join)
        assert len(df) == n0, f"{ym}: 행 수 변동 {n0:,} → {len(df):,}"
        n, ncol = write_month(df, p, wide, val_cols, conv_cols)
        stats.append((ym, n, ncol, v0, p.stat().st_size))
        print(f"  {p.name:<34} {n:>9,}행 × {ncol:,}열  "
              f"{p.stat().st_size/1e6:>7.0f}MB  ({(datetime.now()-ts).seconds}s)")
        del df; gc.collect()

    built = [s[0] for s in stats]
    if not built and ovctx is not None:
        print("\n  ⚠️ --override 를 줬지만 빌드한 월이 없다(기존 파일 존재) — 적용하려면 --force")
    if built:
        ip = write_override_impact(out, ovctx, built)
        if ovctx is not None:
            n = ovctx.n
            print(f"\n  override 영향: 기업 {len(ovctx.pids):,} · 선적 {n['n_shipments']:,} "
                  f"(${ovctx.value/1e6:,.1f}M) · UP 변경 {n['n_shipments_up_changed']:,} · "
                  f"관계 변경 {n['n_shipments_relationship_changed']:,} · "
                  f"force_unmatched {n['n_shipments_force_unmatched']:,} → {ip.name}")
    tot = sum(s[1] for s in stats)
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) · {len(stats)}개월 {tot:,}행 → {out}")


if __name__ == "__main__":
    main()
