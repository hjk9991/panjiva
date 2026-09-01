# -*- coding: utf-8 -*-
r"""
wf_v3_90_checks.py — v3 검증. 명세 §11 게이트 중 v3 가 책임지는 항목.

산출: `--dir` 안에 `90_checks.md`

  G3   키 유일성 (패널 6종)
  G4   **HS 균등배분 후 금액·중량·TEU 합계가 선적층과 대사**된다
  G6·G7 관계 분해 합계와 분류가능 100%
  G8   `within_share` 가 [0,1] 이고 분자·분모가 재계산된다
  G9   PIT 변경 이벤트가 **동일 UP 인접구간을 중복 변경으로 세지 않는다**
  G10  §11-10 구성요소 합 — 판정가능·노출 + 판정가능·무변경 + 판정불가 = 전체 (선적·금액)
  G11  관계전환 pair 는 **실제로 연중 두 관계 상태가 모두 관측**된다 (+ 전환일 정합)
  G13  v1 · v2 와 총계가 대사된다
  G14  컬럼 소문자 · 코드성 식별자 정수 보존

  v3 추가 게이트 (명세 §11 번호와 무관):
  G15  `panel_firm_export_quarter` 금액·선적 수 = 원천 `exp_ship_*` 의 매칭 수출자 선적
  G16  `panel_firm_origin_quarter` 금액·선적 수 = v1 수입자(con) 매칭 선적
  G17  `dim_relationship.within_share_is_count_based` ⇔ 분류가능 금액 0/결측 & 분류가능 선적 > 0
  G18  `panel_firm_quarter.entry/exit_assessable` NaN 0 · 값 {0,1} · int8
  G19  `*_hhi_partners` 0 인 행 없음 · 비결측 값 (0,1]

d8 표는 `--tables-dir`(기본 `output\tables\wf{start 연도}[_asof]` — v1 스키마로 결합 방식 판별)
에서 읽는다. as-of 판을 검증할 때 equi 표를 읽던 하드코딩은 없앴다.
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wf_v3_d8d9 import detect_join_v1, default_tables_dir  # noqa: E402

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V2 = Path(r"C:\panjiva\data\staging\tom_v2_2024")
V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
SRC = Path(r"C:\panjiva\data\staging\source\trade")
CIQ = Path(r"C:\panjiva\data\staging\source\ciq_ref")

KEYS = {"panel_pair_month": ["con_ciqid", "shp_ciqid", "ym"],
        "dim_relationship": ["pair_id"],
        "panel_firm_quarter": ["companyid", "trade_quarter"],
        "panel_firm_origin_hs": ["con_ciqid", "origin_country", "hs6"],
        "panel_firm_export_quarter": ["companyid", "trade_quarter"],
        "panel_firm_origin_quarter": ["con_ciqid", "shpmtorigin", "trade_quarter"]}
L, GATE = [], {}


def say(s=""):
    print(s)
    L.append(s)


def gate(name, ok, detail=""):
    GATE[name] = bool(ok)
    say(f"- **{name}**: **{'PASS' if ok else 'FAIL'}**" + (f" — {detail}" if detail else ""))


def md(df, fmt="{:,.2f}"):
    def f(v):
        if isinstance(v, float):
            return "" if pd.isna(v) else fmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return "" if v is None else str(v)
    return "\n".join(["| " + " | ".join(map(str, df.columns)) + " |",
                      "|" + "|".join(["---"] * len(df.columns)) + "|"]
                     + ["| " + " | ".join(f(v) for v in r) + " |"
                        for r in df.itertuples(index=False)])


def main() -> None:
    global V1, V2, V3, SRC, CIQ
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01")
    ap.add_argument("--dir", default=str(V3))
    ap.add_argument("--v1-dir", default=str(V1))
    ap.add_argument("--v2-dir", default=str(V2))
    ap.add_argument("--src-dir", default=str(SRC))
    ap.add_argument("--ciq-dir", default=str(CIQ))
    ap.add_argument("--tables-dir", default=None,
                    help="d8 표 폴더. 기본: output\\tables\\wf{start 연도}[_asof] (v1 스키마로 판별)")
    a = ap.parse_args()
    V3, V1, V2 = Path(a.dir), Path(a.v1_dir), Path(a.v2_dir)
    SRC, CIQ = Path(a.src_dir), Path(a.ciq_dir)
    join = detect_join_v1(V1, a.start)
    TABLES = Path(a.tables_dir) if a.tables_dir else default_tables_dir(a.start, join)
    months = list(pd.date_range(a.start, a.end, freq="MS").strftime("%Y%m")[:-1])

    say("# v3 within-firm 분석패널 — 검증 결과\n")
    say(f"**검증일** {date.today()} · **대상** `{V3}` · **결합** {join} · "
        f"**기간** {a.start} ~ {a.end}(미포함) · **d8 표** `{TABLES}` · "
        f"**스크립트** `wf_v3_90_checks.py`\n")
    docs = [n for n in ("DECISIONS.md", "COLUMNS.md") if (V3 / n).exists()]
    if docs:
        say("결정 근거·컬럼 뜻은 같은 폴더의 " + " · ".join(f"`{n}`" for n in docs) + ".\n")

    print("입력 로드...")
    ship = pd.concat([pd.read_parquet(
        V1 / f"shipment_master_{m}.parquet",
        columns=["panjivarecordid", "arrivaldate", "valueofgoodsusd", "weightkg",
                 "volumeteu", "con_ciqid", "shp_ciqid", "relationship",
                 "self_shipment"]) for m in months], ignore_index=True)
    pm = pd.read_parquet(V3 / "panel_pair_month.parquet")
    rel = pd.read_parquet(V3 / "dim_relationship.parquet")
    fo = pd.read_parquet(V3 / "panel_firm_origin_hs.parquet")

    # ------------------------------------------------------------------ 규모
    say("\n## 산출물\n")
    rows = []
    for n in KEYS:
        p = V3 / f"{n}.parquet"
        if p.exists():
            f = pq.ParquetFile(p)
            rows.append({"패널": n, "행": f.metadata.num_rows,
                         "열": len(f.schema_arrow.names),
                         "크기(MB)": p.stat().st_size / 1e6})
    say(md(pd.DataFrame(rows), "{:,.0f}"))

    # ------------------------------------------------------------------ G3
    say("\n## G3 — 키 유일성\n")
    for n, k in KEYS.items():
        p = V3 / f"{n}.parquet"
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=k)
        gate(f"G3/{n}", int(d.duplicated(k).sum()) == 0,
             f"키 `{tuple(k)}` 중복 {int(d.duplicated(k).sum()):,}행")
        del d

    # ------------------------------------------------------------------ G13
    say("\n## G13 — v1 · v2 와 총계 대사\n")
    both = ship.con_ciqid.notna() & ship.shp_ciqid.notna()
    v = ship.valueofgoodsusd.fillna(0)
    t = pd.DataFrame([
        {"항목": "양측 CIQ 매칭 선적 (v1)", "선적": int(both.sum()),
         "금액($B)": float(v[both].sum()) / 1e9},
        {"항목": "panel_pair_month 합계", "선적": int(pm.n_shipments.sum()),
         "금액($B)": float(pm.value_usd.sum()) / 1e9},
        {"항목": "dim_relationship 합계", "선적": int(rel.n_shipments.sum()),
         "금액($B)": float(rel.value_usd.sum()) / 1e9},
    ])
    t["선적 차이"] = t.선적 - t.선적.iloc[0]
    t["금액 차이($)"] = (t["금액($B)"] - t["금액($B)"].iloc[0]) * 1e9
    say(md(t))
    gate("G13/pair_month", int(pm.n_shipments.sum()) == int(both.sum())
         and abs(float(pm.value_usd.sum()) - float(v[both].sum())) < 1)
    gate("G13/relationship", int(rel.n_shipments.sum()) == int(both.sum())
         and abs(float(rel.value_usd.sum()) - float(v[both].sum())) < 1)

    fq = V3 / "panel_firm_quarter.parquet"
    if fq.exists():
        n3 = pq.ParquetFile(fq).metadata.num_rows
        n2 = pq.ParquetFile(V2 / "03_firm.parquet").metadata.num_rows
        gate("G13/firm_quarter", n3 == n2,
             f"v3 {n3:,}행 vs v2 03_firm {n2:,}행 (X-2: v2 를 base 로 쓰므로 같아야 한다)")

    # ------------------------------------------------------------------ G4
    say("\n## G4 — HS 균등배분 후 합계 대사\n")
    say("다중 HS 선적의 금액을 HS6 개수로 나눠 배분했다. **배분해도 총액은 변하지 않아야** 한다.\n")
    hs = pd.concat([pd.read_parquet(SRC / f"imp_hs_{m}.parquet",
                                    columns=["panjivarecordid", "hs6"])
                    for m in months], ignore_index=True).drop_duplicates()
    base = ship.loc[ship.con_ciqid.notna(),
                    ["panjivarecordid", "valueofgoodsusd", "weightkg", "volumeteu"]]
    ref = base[base.panjivarecordid.isin(set(hs.panjivarecordid))]
    t = pd.DataFrame([
        {"항목": "배분 전 — 수입자 매칭 + HS 보유 선적",
         "선적": int(ref.panjivarecordid.nunique()),
         "금액($)": float(ref.valueofgoodsusd.fillna(0).sum()),
         "중량(kg)": float(ref.weightkg.fillna(0).sum()),
         "TEU": float(ref.volumeteu.fillna(0).sum())},
        {"항목": "배분 후 — panel_firm_origin_hs",
         "선적": int(fo.n_shipments.sum()), "금액($)": float(fo.value_usd.sum()),
         "중량(kg)": float(fo.weight_kg.sum()), "TEU": float(fo.teu.sum())},
    ])
    say(md(t, "{:,.2f}"))
    dv = abs(t["금액($)"].iloc[1] - t["금액($)"].iloc[0])
    dw = abs(t["중량(kg)"].iloc[1] - t["중량(kg)"].iloc[0])
    dt_ = abs(t["TEU"].iloc[1] - t["TEU"].iloc[0])
    gate("G4", dv < 1 and dw < 1 and dt_ < 0.01,
         f"금액 차이 ${dv:.2f} · 중량 {dw:.2f}kg · TEU {dt_:.4f}")
    say("\n> 선적 수는 배분 후가 더 많다 — 한 선적이 여러 HS 행에 나뉘어 세어지기 때문이다. "
        "**금액·중량·TEU 는 배분해도 총액이 보존**되므로 그것으로 대사한다.")

    # ------------------------------------------------------------------ G6·G7
    say("\n## G6 · G7 — 관계 분해\n")
    s = pm[["value_within_firm", "value_arms", "value_unmatched", "value_usd",
            "value_classified", "n_within_firm", "n_arms", "n_unmatched",
            "n_shipments", "n_classified"]].sum()
    tot_v = s.value_within_firm + s.value_arms + s.value_unmatched
    tot_n = s.n_within_firm + s.n_arms + s.n_unmatched
    say(md(pd.DataFrame([
        {"기준": "금액($B)", "within_firm": s.value_within_firm / 1e9,
         "arms_length": s.value_arms / 1e9, "unmatched": s.value_unmatched / 1e9,
         "합": tot_v / 1e9, "패널 총계": s.value_usd / 1e9,
         "차이": tot_v - s.value_usd},
        {"기준": "건수", "within_firm": s.n_within_firm, "arms_length": s.n_arms,
         "unmatched": s.n_unmatched, "합": tot_n, "패널 총계": s.n_shipments,
         "차이": tot_n - s.n_shipments}])))
    gate("G6", abs(tot_v - s.value_usd) < 1 and tot_n == s.n_shipments)
    share = (s.value_within_firm + s.value_arms) / s.value_classified * 100
    gate("G7", abs(share - 100) < 1e-9, f"분류가능 합계 {share:.6f}%")
    say(f"\n- **분류 가능 거래 중 그룹내 비중**: 금액 "
        f"**{s.value_within_firm/s.value_classified*100:.2f}%** · "
        f"건수 **{s.n_within_firm/s.n_classified*100:.2f}%** (명세 §4.4 분모)")

    # ------------------------------------------------------------------ G8
    say("\n## G8 — `within_share` 범위·재계산\n")
    ws = pm.within_share.dropna()
    recalc = (pm.value_within_firm / pm.value_classified).where(pm.value_classified > 0)
    bad = int((recalc.notna() & pm.within_share_value.notna()
               & (np.abs(recalc - pm.within_share_value) > 1e-12)).sum())
    wf = pm.within_firm
    mism = int((wf.notna() & (wf.eq(1) != (pm.within_share > 0.5))).sum())
    say(md(pd.DataFrame([{
        "비결측": len(ws), "최소": float(ws.min()), "최대": float(ws.max()),
        "[0,1] 벗어남": int(((ws < 0) | (ws > 1)).sum()),
        "분자/분모 재계산 불일치": bad,
        "within_firm=(share>0.5) 불일치": mism,
        "건수기준으로 대체된 행": int(pm.within_share_is_count_based.sum()),
        "정확히 0.5 인 행": int((pm.within_share == 0.5).sum())}]), "{:,.4f}"))
    gate("G8", ((ws < 0) | (ws > 1)).sum() == 0 and bad == 0 and mism == 0)
    say("\n> 정확히 0.5 인 행은 명세 §5 문언(`within_share > 0.5`)대로 **`within_firm=0`** 이다.")

    # ------------------------------------------------------------------ G9
    say("\n## G9 — PIT 변경 이벤트가 동일 UP 인접구간을 중복으로 세지 않는가\n")
    d8 = V3 / "d8_firm_ownership_change.parquet"
    if not d8.exists():
        say("- `d8_firm_ownership_change.parquet` 없음 — 건너뜀 (`wf_v3_d8d9.py` 먼저 실행)")
    else:
        p = pd.read_parquet(CIQ / "ownership_pit.parquet",
                            columns=["companyid", "ultimate_parent_companyid",
                                     "start_date", "end_date"])
        CAP = pd.Timestamp("2100-01-01")
        for c in ("start_date", "end_date"):
            p[c] = p[c].astype("datetime64[us]").clip(upper=CAP).astype("datetime64[ns]")
        W0, W1 = pd.Timestamp(a.start), pd.Timestamp(a.end)
        p = p.sort_values(["companyid", "start_date", "end_date"], kind="mergesort")
        # 병합 없이 그냥 세면 (인접 동일 UP 을 중복으로 셈) 몇 건이 되는가
        naive = p[(p.companyid == p.companyid.shift())
                  & p.start_date.between(W0, W1, inclusive="left")]
        n_naive = int(naive.groupby("companyid").size().sum())
        f = pd.read_parquet(d8, columns=["n_up_changes_2024"])
        n_real = int(f.n_up_changes_2024.sum())
        say(md(pd.DataFrame([
            {"셈 방식": "인접 동일 UP 을 병합하지 않고 셈 (틀린 방식)", "변경 건수": n_naive},
            {"셈 방식": "동일 UP 인접구간 병합 후 (명세 §6.1)", "변경 건수": n_real},
            {"셈 방식": "차이 = 중복 제거된 건수", "변경 건수": n_naive - n_real}])))
        gate("G9", n_real <= n_naive,
             f"병합이 실제로 {n_naive - n_real:,}건을 걸러냈다")

    # ------------------------------------------------------------------ G10·G11
    say("\n## G10 · G11 — d8 · d9 정합\n")
    say("- **G10**: 명세 §11-10 — **판정가능·노출(2행) + 판정가능·무변경(4행) + 판정불가(3행) "
        "= 전체 거래**, 선적과 금액 모두. 전체 분모는 v1 선적 수와 같아야 한다.")
    f10 = TABLES / "d8_ownership_change_summary.csv"
    if not f10.exists():
        say(f"  - `{f10}` 없음 — 건너뜀 (`wf_v3_d8d9.py --tables-dir` 와 같은 폴더여야 한다)")
    else:
        r = pd.read_csv(f10, encoding="utf-8-sig")
        if len(r) < 4:
            gate("G10", False, "d8 summary 가 4행 미만(구판) — `wf_v3_d8d9.py` 를 다시 돌릴 것")
        else:
            n_all, v_all = int(r["선적 분모"].iloc[0]), float(r["금액 분모($B)"].iloc[0])
            parts = {"판정가능·노출 (2행)": (int(r["선적"].iloc[1]), float(r["금액($B)"].iloc[1])),
                     "판정가능·무변경 (4행)": (int(r["선적"].iloc[3]), float(r["금액($B)"].iloc[3])),
                     "판정불가 (3행)": (int(r["선적"].iloc[2]), float(r["금액($B)"].iloc[2]))}
            n_sum = sum(x[0] for x in parts.values())
            v_sum = sum(x[1] for x in parts.values())
            say(md(pd.DataFrame(
                [{"구성요소": k, "선적": n, "금액($B)": v} for k, (n, v) in parts.items()]
                + [{"구성요소": "합", "선적": n_sum, "금액($B)": v_sum},
                   {"구성요소": "전체 분모 (1행)", "선적": n_all, "금액($B)": v_all},
                   {"구성요소": "v1 선적 (금액은 비결측 합)", "선적": len(ship),
                    "금액($B)": float(ship.valueofgoodsusd.sum()) / 1e9}]), "{:,.6f}"))
            dv = abs(v_sum - v_all) * 1e9
            dv1 = abs(v_all * 1e9 - float(ship.valueofgoodsusd.sum()))
            gate("G10", n_sum == n_all == len(ship) and dv < 1e3 and dv1 < 1,
                 f"선적 {n_sum:,} = {n_all:,} = v1 {len(ship):,} · 금액 합 차이 ${dv:,.2f} · "
                 f"분모 vs v1 차이 ${dv1:,.2f}")

    tr = rel[rel.relationship_changed_2024 == 1]
    ok11 = bool(((tr.first_state != tr.last_state) | (tr.n_transitions >= 2)).all()) \
        and bool((tr.n_transitions >= 1).all())
    det11 = (f"전환 pair {len(tr):,}개 전부 두 상태가 실제로 관측됨 "
             f"(전환 {int(tr.n_transitions.sum()):,}건: "
             f"within→arms {int(tr.within_to_arms.sum()):,} · "
             f"arms→within {int(tr.arms_to_within.sum()):,})")
    if "first_transition_date" in tr.columns:
        # 명세 §6.5 전환일 — 전환 pair 는 전부 있고 순서가 맞아야, 비전환 pair 는 NaT 여야 한다
        no_tr = rel[rel.n_transitions.fillna(0) == 0]
        ok_dt = bool(tr.first_transition_date.notna().all()) \
            and bool((tr.first_transition_date <= tr.last_transition_date).all()) \
            and bool(no_tr.first_transition_date.isna().all())
        ok11 = ok11 and ok_dt
        det11 += f" · 전환일 정합 {'OK' if ok_dt else '불일치'}"
    gate("G11", ok11, det11)

    # ------------------------------------------------------------------ G14
    say("\n## G14 — 컬럼명·타입\n")
    bad_up, bad_int = [], []
    for n in KEYS:
        p = V3 / f"{n}.parquet"
        if not p.exists():
            continue
        sch = pq.ParquetFile(p).schema_arrow
        bad_up += [f"{n}.{c}" for c in sch.names if c != c.lower()]
        for c in ("con_ciqid", "shp_ciqid", "companyid", "con_up", "shp_up"):
            if c in sch.names and not pa.types.is_integer(sch.field(c).type):
                bad_int.append(f"{n}.{c}")
    gate("G14", not bad_up and not bad_int,
         f"대문자 컬럼 {len(bad_up)}개 · 식별자 비정수 {len(bad_int)}개"
         + (f" — {', '.join((bad_up + bad_int)[:5])}" if (bad_up or bad_int) else ""))

    # ------------------------------------------------------------------ G15~G19
    say("\n## G15 ~ G19 — v3 추가 게이트 (명세 §11 번호와 무관)\n")
    # G15 수출 패널 = 원천 exp_ship 의 shp_ciqid_original 비결측 선적
    fxp = V3 / "panel_firm_export_quarter.parquet"
    if fxp.exists():
        fx = pd.read_parquet(fxp, columns=["n_ship", "value_usd"])
        files = [SRC / f"exp_ship_{m}.parquet" for m in months]
        files = [f for f in files if f.exists()]
        if files:
            e = pd.concat([pd.read_parquet(f, columns=["shp_ciqid_original", "valueofgoodsusd"])
                           for f in files], ignore_index=True)
            e = e[e.shp_ciqid_original.notna()]
            n_src, v_src = len(e), float(e.valueofgoodsusd.fillna(0).sum())
            del e
        else:
            n_src, v_src = 0, 0.0
        n_p, v_p = int(fx.n_ship.sum()), float(fx.value_usd.sum())
        gate("G15/export_panel", n_p == n_src and abs(v_p - v_src) < 1,
             f"패널 선적 {n_p:,} vs 원천 매칭 수출 선적 {n_src:,} · 금액 차이 ${abs(v_p - v_src):,.2f} "
             f"(수출 월 파일 {len(files)}/{len(months)}개월)")
    else:
        say("- `panel_firm_export_quarter.parquet` 없음 — G15 건너뜀")
    # G16 원산국 패널 = v1 con 매칭 선적
    fop = V3 / "panel_firm_origin_quarter.parquet"
    if fop.exists():
        fo2 = pd.read_parquet(fop, columns=["n_ship", "value_usd"])
        con = ship.con_ciqid.notna()
        n_p, v_p = int(fo2.n_ship.sum()), float(fo2.value_usd.sum())
        n_c, v_c = int(con.sum()), float(v[con].sum())
        gate("G16/origin_panel", n_p == n_c and abs(v_p - v_c) < 1,
             f"패널 선적 {n_p:,} vs v1 수입자 매칭 선적 {n_c:,} · 금액 차이 ${abs(v_p - v_c):,.2f}")
    else:
        say("- `panel_firm_origin_quarter.parquet` 없음 — G16 건너뜀")
    # G17 건수 대체 플래그 ⇔ 분류가능 금액 0/결측 & 분류가능 선적 > 0
    if "within_share_is_count_based" in rel.columns:
        exp17 = (rel.value_classified.fillna(0) <= 0) & (rel.n_classified.fillna(0) > 0)
        mism = int(((rel.within_share_is_count_based == 1) != exp17).sum())
        gate("G17/count_based_flag", mism == 0,
             f"flag=1 {int((rel.within_share_is_count_based == 1).sum()):,}행 · 정의와 불일치 {mism}행 "
             f"· 분류가능 선적 0 인 pair(flag 0·share NaN) "
             f"{int((rel.n_classified.fillna(0) == 0).sum()):,}")
    else:
        gate("G17/count_based_flag", False, "`within_share_is_count_based` 열 없음")
    # G18 · G19 panel_firm_quarter 플래그·HHI
    if fq.exists():
        af = pd.read_parquet(fq, columns=["entry_assessable", "exit_assessable"])
        bad = sum(int(af[c].isna().sum()) + int((~af[c].isin([0, 1])).sum()) for c in af.columns)
        dt_ok = all(str(af[c].dtype) == "int8" for c in af.columns)
        gate("G18/assessable_flags", bad == 0 and dt_ok,
             f"NaN·{{0,1}} 밖 {bad}건 · dtype {af.entry_assessable.dtype}/{af.exit_assessable.dtype} · "
             f"entry_assessable=0 {int((af.entry_assessable == 0).sum()):,}행 · "
             f"exit_assessable=0 {int((af.exit_assessable == 0).sum()):,}행")
        hh = pd.read_parquet(fq, columns=["imp_hhi_partners", "exp_hhi_partners"])
        z = int((hh.imp_hhi_partners == 0).sum()) + int((hh.exp_hhi_partners == 0).sum())
        nn = pd.concat([hh.imp_hhi_partners.dropna(), hh.exp_hhi_partners.dropna()])
        out_ = int(((nn <= 0) | (nn > 1 + 1e-9)).sum())
        gate("G19/hhi_range", z == 0 and out_ == 0,
             f"0 인 행 {z} · (0,1] 밖 {out_} · 비결측 imp {int(hh.imp_hhi_partners.notna().sum()):,} "
             f"/ exp {int(hh.exp_hhi_partners.notna().sum()):,} · 최소 {float(nn.min()):.4f}")
    else:
        say("- `panel_firm_quarter.parquet` 없음 — G18 · G19 건너뜀")

    # ------------------------------------------------------------------ 요약
    say("\n---\n\n## 요약\n")
    fails = [k for k, v in GATE.items() if not v]
    say(f"**게이트 {len(GATE)}개 중 {len(GATE)-len(fails)}개 통과**"
        + (f"\n\n실패: {', '.join(fails)}" if fails else " — 전항 PASS"))

    (V3 / "90_checks.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n→ {V3 / '90_checks.md'}")


if __name__ == "__main__":
    main()
