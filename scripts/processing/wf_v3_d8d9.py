# -*- coding: utf-8 -*-
r"""
wf_v3_d8d9.py — 명세 §6 소유구조 변화·관계전환 진단

산출 (`output\tables\wf2024\` + v3 폴더):
    d8_ownership_change_summary.csv    기업 변경·거래 노출·판정불가 비율 (건수/금액)
    d8_ownership_change_monthly.csv    월별 변경 노출 및 판정 가능성
    d8_firm_ownership_change.parquet   기업별 원자료 (up_changed_2024 · 변경일 · 판정가능)
    d9_relationship_transition_summary.csv   관계전환 pair·선적·금액 비율과 방향
    d9_relationship_transition_pairs.parquet 전환 pair, 최초·최종 상태, 관련 금액

## 명세가 정한 것

§6.1 기업의 연중 UP 변경
     PIT 구간을 날짜순 정렬 → 2024년 중 효력이 시작된 구간의 UP 이 **직전 유효구간**과
     다르면 변경. **동일 UP 로 이어지는 인접 구간은 변경으로 세지 않는다.**
     `up_change_date` = 새 UP 구간의 startDate. 여러 번이면 `n_up_changes_2024` 에 모두.

§6.2 판정 가능성
     `up_change_assessable_2024=1` 은 PIT 구간의 합집합이 `[2024-01-01, 2025-01-01)` 을
     **공백 없이 덮을 때만**. 스냅샷 대체만 있는 기업은 판정 불가.

§6.3 거래의 변경 노출 — 리포트 3종을 건수·금액 기준으로 각각
     1. 전체 거래 기준 하한   = 노출 거래 / 전체 표준필터 거래
     2. 판정 가능 거래 기준   = 노출 거래 / 판정가능 거래
     3. 판정 불가 비율        = 판정불가 거래 / 전체 거래
     금액 기준 분모는 `value_usd` 비결측 거래의 합이며 **비결측률을 함께 보고**한다.

§6.4 관계전환 — `dim_relationship` 이 이미 **선적 단위**로 계산해 둔 것을 집계한다.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
CIQ = Path(r"C:\panjiva\data\staging\source\ciq_ref")
TABLES = Path(r"C:\panjiva\projects\20251201\output\tables\wf2024")

SHIP_COLS = ["panjivarecordid", "arrivaldate", "valueofgoodsusd",
             "con_ciqid", "shp_ciqid", "con_up", "shp_up",
             "con_ownership_is_fallback", "shp_ownership_is_fallback", "relationship"]
L = []


def say(s=""):
    print(s)
    L.append(s)


def md(df, fmt="{:,.2f}"):
    def f(v):
        if isinstance(v, float):
            return "" if pd.isna(v) else fmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
    return "\n".join(["| " + " | ".join(map(str, df.columns)) + " |",
                      "|" + "|".join(["---"] * len(df.columns)) + "|"]
                     + ["| " + " | ".join(f(v) for v in r) + " |"
                        for r in df.itertuples(index=False)])


# ---------------------------------------------------------------------------
# §6.1 · §6.2 — 기업 단위 소유구조 변화
# ---------------------------------------------------------------------------
def firm_ownership_change(ciq: Path, w0: str, w1: str) -> pd.DataFrame:
    """PIT 구간에서 기업별 연중 UP 변경과 판정 가능성을 계산한다."""
    p = pd.read_parquet(ciq / "ownership_pit.parquet",
                        columns=["companyid", "ultimate_parent_companyid",
                                 "start_date", "end_date"])
    # ⚠️ `end_date` 에 **`9999-12-31`** 이 들어 있다 — "지금도 유효한 개방 구간" 표시다.
    #    pandas 의 나노초 타임스탬프 상한은 **2262-04-11** 이라 그대로 변환하면
    #    OutOfBoundsDatetime 으로 죽는다. 상한을 2100 년으로 잘라서 쓴다 — 비교 결과는
    #    같고, 2262 로 자르면 날짜 뺄셈에서 int64 오버플로가 난다.
    CAP = pd.Timestamp("2100-01-01")
    p["start_date"] = p.start_date.astype("datetime64[us]").clip(upper=CAP) \
        .astype("datetime64[ns]")
    p["end_date"] = p.end_date.astype("datetime64[us]").clip(upper=CAP) \
        .astype("datetime64[ns]")
    W0, W1 = pd.Timestamp(w0), pd.Timestamp(w1)
    p = p.sort_values(["companyid", "start_date", "end_date"], kind="mergesort")

    # --- §6.1: 동일 UP 로 이어지는 인접 구간을 하나로 본다 ---
    same_co = p.companyid == p.companyid.shift()
    same_up = p.ultimate_parent_companyid == p.ultimate_parent_companyid.shift()
    p["_run_start"] = ~(same_co & same_up)          # 새 UP 구간이 시작되는 행
    runs = p[p._run_start].copy()
    runs["_is_first"] = runs.companyid != runs.companyid.shift()   # 회사의 첫 구간
    # 변경 = 2024년 중 효력이 시작된 run 이면서 그 회사의 첫 run 이 아닌 것
    chg = runs[(~runs._is_first) & runs.start_date.between(W0, W1, inclusive="left")]
    g = chg.groupby("companyid").agg(
        n_up_changes_2024=("start_date", "size"),
        up_change_date=("start_date", "min"),
        up_change_date_last=("start_date", "max")).reset_index()

    # --- §6.2: 구간 합집합이 창을 공백 없이 덮는가 ---
    q = p[(p.end_date > W0) & (p.start_date < W1)].copy()
    q["s"] = q.start_date.clip(lower=W0)
    q["e"] = q.end_date.clip(upper=W1)
    q = q.sort_values(["companyid", "s"], kind="mergesort")
    prev_max_e = q.groupby("companyid")["e"].cummax().shift()
    cont = q.companyid == q.companyid.shift()
    # ⚠️ CIQ 의 PIT 구간은 **닫힌 구간**이고 다음 구간이 **정확히 1초 뒤**에 시작한다.
    #    (2024 창 실측: 연속 구간 쌍 229,827개 중 99.97% 가 정확히 1초 간격.
    #     진짜 공백은 80건뿐 — 1분 초과 3건 + 1일 초과 77건.)
    #    허용치를 안 주면 **모든 변경 기업이 "공백 있음"으로 잘못 판정**된다.
    ONE_SEC = pd.Timedelta(seconds=1)
    q["_gap"] = cont & (q.s > prev_max_e + ONE_SEC)
    cov = q.groupby("companyid").agg(min_s=("s", "min"), max_e=("e", "max"),
                                     any_gap=("_gap", "max")).reset_index()
    cov["up_change_assessable_2024"] = (
        (cov.min_s <= W0) & (cov.max_e >= W1) & (~cov.any_gap)).astype("int8")

    out = cov[["companyid", "up_change_assessable_2024"]].merge(g, on="companyid",
                                                                how="outer")
    out["n_up_changes_2024"] = out.n_up_changes_2024.fillna(0).astype("Int64")
    out["up_changed_2024"] = (out.n_up_changes_2024 > 0).astype("int8")
    out["up_change_assessable_2024"] = out.up_change_assessable_2024.fillna(0).astype("int8")
    return out


# ---------------------------------------------------------------------------
# §6.3 — 거래의 변경 노출
# ---------------------------------------------------------------------------
def trade_exposure(ship: pd.DataFrame, firm: pd.DataFrame) -> pd.DataFrame:
    f = firm.set_index("companyid")
    chg, ass = f["up_changed_2024"], f["up_change_assessable_2024"]
    for side in ("con", "shp"):
        k = ship[f"{side}_ciqid"]
        ship[f"{side}_up_changed_2024"] = k.map(chg).astype("Int8")
        a = k.map(ass).astype("Int8")
        # ⚠️ 명세 §3.2 — 스냅샷 대체로 채운 행은 **연중 변화 판정 근거로 쓰지 않는다**
        ship[f"{side}_up_change_assessable"] = a.where(
            ship[f"{side}_ownership_is_fallback"] != 1, 0).astype("Int8")
    # ⚠️ 결측(= CIQ 매칭 실패 등으로 판정 자체를 못한 당사자)은 **"변경 관측 안 됨"** 으로
    #    센다. 명세 §6.3 이 이 지표를 "전체 거래 기준 **하한**" 이라 부르는 이유다 —
    #    모르는 것을 변경으로 세지 않으므로 실제 노출은 이보다 클 수 있다.
    ship["any_party_up_changed_2024"] = (
        ship.con_up_changed_2024.eq(1).fillna(False)
        | ship.shp_up_changed_2024.eq(1).fillna(False)).astype("int8")
    ship["up_change_exposure_assessable"] = (
        ship.con_up_change_assessable.eq(1).fillna(False)
        & ship.shp_up_change_assessable.eq(1).fillna(False)).astype("int8")
    return ship


def exposure_report(ship: pd.DataFrame) -> pd.DataFrame:
    v = ship.valueofgoodsusd
    hasv = v.notna()
    n_all, v_all = len(ship), float(v[hasv].sum())
    exp_ = ship.any_party_up_changed_2024 == 1
    ass = ship.up_change_exposure_assessable == 1
    rows = [
        {"지표": "1. 전체 거래 기준 하한", "분자": "노출 거래", "분모": "전체 표준필터 거래",
         "선적": int(exp_.sum()), "선적 분모": n_all, "선적 비율(%)": exp_.mean() * 100,
         "금액($B)": float(v[exp_ & hasv].sum()) / 1e9, "금액 분모($B)": v_all / 1e9,
         "금액 비율(%)": float(v[exp_ & hasv].sum()) / v_all * 100},
        {"지표": "2. 판정 가능 거래 기준", "분자": "노출 거래", "분모": "판정가능 거래",
         "선적": int((exp_ & ass).sum()), "선적 분모": int(ass.sum()),
         "선적 비율(%)": (exp_ & ass).sum() / max(int(ass.sum()), 1) * 100,
         "금액($B)": float(v[exp_ & ass & hasv].sum()) / 1e9,
         "금액 분모($B)": float(v[ass & hasv].sum()) / 1e9,
         "금액 비율(%)": float(v[exp_ & ass & hasv].sum())
                       / max(float(v[ass & hasv].sum()), 1) * 100},
        {"지표": "3. 판정 불가 비율", "분자": "판정불가 거래", "분모": "전체 거래",
         "선적": int((~ass).sum()), "선적 분모": n_all, "선적 비율(%)": (~ass).mean() * 100,
         "금액($B)": float(v[~ass & hasv].sum()) / 1e9, "금액 분모($B)": v_all / 1e9,
         "금액 비율(%)": float(v[~ass & hasv].sum()) / v_all * 100},
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
def main() -> None:
    global V1, V3, CIQ, TABLES
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01", help="미포함")
    ap.add_argument("--v1-dir", default=str(V1))
    ap.add_argument("--v3-dir", default=str(V3))
    ap.add_argument("--ciq-dir", default=str(CIQ))
    ap.add_argument("--tables-dir", default=str(TABLES))
    a = ap.parse_args()
    V1, V3, CIQ = Path(a.v1_dir), Path(a.v3_dir), Path(a.ciq_dir)
    TABLES = Path(a.tables_dir); TABLES.mkdir(parents=True, exist_ok=True)
    t0 = datetime.now()

    say("# d8 · d9 — 소유구조 변화와 관계전환 (명세 §6)\n")
    say(f"**생성일** {date.today()} · **기간** {a.start} ~ {a.end}(미포함) · "
        f"**스크립트** `wf_v3_d8d9.py`\n")

    print("[1] PIT 구간에서 기업별 UP 변경 계산")
    firm = firm_ownership_change(CIQ, a.start, a.end)
    firm.to_parquet(V3 / "d8_firm_ownership_change.parquet", index=False,
                    compression="zstd")
    n_ass = int(firm.up_change_assessable_2024.sum())
    n_chg = int(firm.up_changed_2024.sum())
    say("\n## d8-1. 기업 단위 (명세 §6.1·§6.2)\n")
    say(md(pd.DataFrame([{
        "PIT 기록 있는 기업": len(firm),
        "판정 가능 (구간이 창을 공백없이 덮음)": n_ass,
        "판정 가능 비율(%)": n_ass / len(firm) * 100,
        "연중 UP 변경 기업": n_chg,
        "판정가능 중 변경 비율(%)":
            int(((firm.up_changed_2024 == 1)
                 & (firm.up_change_assessable_2024 == 1)).sum()) / max(n_ass, 1) * 100,
    }])))
    say("\n> ⚠️ **판정 불가 기업의 변경 여부는 알 수 없다** — 없다는 뜻이 아니다. "
        "PIT 구간이 2024년을 부분적으로만 덮으면 그 밖에서 일어난 변경을 볼 수 없다.")
    say(f"\n변경 횟수 분포: "
        + " · ".join(f"{int(k)}회 {int(v):,}개"
                     for k, v in firm.n_up_changes_2024.value_counts().sort_index().head(6).items()))

    print("[2] 선적별 변경 노출")
    months = pd.date_range(a.start, a.end, freq="MS").strftime("%Y%m")[:-1]
    ship = pd.concat([pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                                      columns=SHIP_COLS) for m in months],
                     ignore_index=True)
    ship = trade_exposure(ship, firm)
    rep = exposure_report(ship)
    rep.to_csv(TABLES / "d8_ownership_change_summary.csv", index=False,
               encoding="utf-8-sig")
    say("\n## d8-2. 거래의 변경 노출 (명세 §6.3)\n")
    say(md(rep))
    hasv = ship.valueofgoodsusd.notna()
    say(f"\n- 금액 비결측률: **{hasv.mean()*100:.1f}%** "
        f"({int(hasv.sum()):,} / {len(ship):,}건) — 금액 기준 분모는 이 부분집합이다")

    mon = ship.assign(ym=ship.arrivaldate.dt.strftime("%Y%m")).groupby("ym").agg(
        n_ship=("panjivarecordid", "size"),
        n_exposed=("any_party_up_changed_2024", "sum"),
        n_assessable=("up_change_exposure_assessable", "sum"),
        value_usd=("valueofgoodsusd", "sum")).reset_index()
    mon["exposed_share_n"] = (mon.n_exposed / mon.n_ship * 100).round(3)
    mon["assessable_share_n"] = (mon.n_assessable / mon.n_ship * 100).round(3)
    mon.to_csv(TABLES / "d8_ownership_change_monthly.csv", index=False,
               encoding="utf-8-sig")
    say("\n### 월별\n")
    say(md(mon, "{:,.3f}"))

    print("[3] 관계전환 집계")
    rel = pd.read_parquet(V3 / "dim_relationship.parquet")
    cls = rel[rel.n_classified_shipments.notna() & (rel.n_classified_shipments > 0)]
    tr = cls[cls.relationship_changed_2024 == 1]
    tr.to_parquet(V3 / "d9_relationship_transition_pairs.parquet", index=False,
                  compression="zstd")
    d9 = pd.DataFrame([{
        "분류가능 pair": len(cls),
        "관계전환 pair": len(tr),
        "전환 pair 비율(%)": len(tr) / max(len(cls), 1) * 100,
        "전환 pair 의 선적": int(tr.n_shipments.sum()),
        "전환 pair 의 금액($B)": float(tr.value_usd.sum()) / 1e9,
        "전체 분류가능 금액 대비(%)":
            float(tr.value_usd.sum()) / max(float(cls.value_usd.sum()), 1) * 100,
        "전환 건수 합": int(tr.n_transitions.sum()),
        "within→arms": int(tr.within_to_arms.sum()),
        "arms→within": int(tr.arms_to_within.sum()),
    }])
    d9.to_csv(TABLES / "d9_relationship_transition_summary.csv", index=False,
              encoding="utf-8-sig")
    say("\n## d9. 관계분류 전환 (명세 §6.4)\n")
    say(md(d9))
    say("\n> **전환은 선적 단위로 센다** — 같은 pair 의 분류 가능 선적을 날짜순으로 놓고 "
        "직전 선적과 값이 달라질 때마다 1건이다. 미매칭 선적은 건너뛴다.")
    say("\n> ⚠️ **기업의 최종모회사가 변해도 관계전환은 0 일 수 있다.** 양측이 계속 같은 "
        "가족이거나 계속 다른 가족이면 분류가 안 바뀐다(명세 §6.4). 반대로 UP 이 안 변해도 "
        "**상대방**이 바뀌면 전환이 생긴다 — d8 과 d9 는 다른 것을 재는 지표다.")

    (V3 / "95_d8d9_report.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {TABLES}")


if __name__ == "__main__":
    main()
