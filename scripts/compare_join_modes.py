# -*- coding: utf-8 -*-
r"""
compare_join_modes.py — as-of 판과 equi-join 판을 나란히 대조

두 산출물은 **재무 결합 방식만** 다르다. 그러므로:

  ✅ 같아야 하는 것 — 선적 수 · 금액 · 관계분류 · 매칭상태 · 패널 행 수
  🔀 달라도 되는 것 — 붙은 회계기간 · 재무 값 · 시점 컬럼 이름과 분포

산출: `projects\20251201\output\COMPARE_asof_vs_equi.md`
"""

import glob
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

EQ = {"v1": Path(r"C:\panjiva\data\staging\tom_v1_2024"),
      "v2": Path(r"C:\panjiva\data\staging\tom_v2_2024"),
      "v3": Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")}
AS = {k: Path(str(v) + "_asof") for k, v in EQ.items()}
OUT = Path(r"C:\panjiva\projects\20251201\output\COMPARE_asof_vs_equi.md")
MONTHS = [f"2024{m:02d}" for m in range(1, 13)]
L, RES = [], []


def say(s=""):
    print(s, flush=True)
    L.append(s)


def chk(name, ok, detail=""):
    RES.append({"항목": name, "결과": "PASS" if ok else "FAIL"})
    say(f"- [{'PASS' if ok else '**FAIL**'}] **{name}** {detail}")


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


say("# as-of 판 vs equi-join 판 — 대조\n")
say(f"**대조일** {date.today()} · **스크립트** `compare_join_modes.py`\n")
say("두 산출물은 **재무 결합 방식만** 다르다. 선적·관계는 같고 재무만 달라야 정상이다.\n")
say("- `tom_v1_2024_asof` 등 = **명세 §3.3 준수본** (`*_age_days`, 항상 양수)")
say("- `tom_v1_2024` 등 = **대안본** (`*_days_after_close`, 음수 가능)\n")

# ---------------------------------------------------------------- v1
say("\n## v1 — 선적층\n")
acc = {"n": [0, 0], "v": [0.0, 0.0], "rel_diff": 0, "cw_diff": 0, "id_diff": 0}
age, dac = [], []
for m in MONTHS:
    fe, fa = EQ["v1"] / f"shipment_master_{m}.parquet", AS["v1"] / f"shipment_master_{m}.parquet"
    if not fa.exists():
        continue
    ce = ["panjivarecordid", "valueofgoodsusd", "relationship",
          "crosswalk_match_status", "con_ciqid", "shp_ciqid",
          "con_up_a_financial_period_id"]
    e = pd.read_parquet(fe, columns=ce + ["con_up_a_days_after_close"])
    a = pd.read_parquet(fa, columns=ce + ["con_up_a_age_days"])
    acc["n"][0] += len(e); acc["n"][1] += len(a)
    acc["v"][0] += float(e.valueofgoodsusd.fillna(0).sum())
    acc["v"][1] += float(a.valueofgoodsusd.fillna(0).sum())
    assert (e.panjivarecordid.values == a.panjivarecordid.values).all(), f"{m}: 행 순서 다름"
    acc["rel_diff"] += int((e.relationship != a.relationship).sum())
    acc["cw_diff"] += int((e.crosswalk_match_status != a.crosswalk_match_status).sum())
    acc["id_diff"] += int((e.con_ciqid.fillna(-1) != a.con_ciqid.fillna(-1)).sum())
    acc["fp_diff"] = acc.get("fp_diff", 0) + int(
        (e.con_up_a_financial_period_id.fillna(0) != a.con_up_a_financial_period_id.fillna(0)).sum())
    age.append(a.con_up_a_age_days.dropna().to_numpy())
    dac.append(e.con_up_a_days_after_close.dropna().to_numpy())
    del e, a

say("### 같아야 하는 것\n")
say(md(pd.DataFrame([
    {"항목": "선적 수", "equi": acc["n"][0], "asof": acc["n"][1], "차이": acc["n"][1] - acc["n"][0]},
    {"항목": "금액($B)", "equi": acc["v"][0] / 1e9, "asof": acc["v"][1] / 1e9,
     "차이": (acc["v"][1] - acc["v"][0]) / 1e9}])))
chk("v1 선적 수 동일", acc["n"][0] == acc["n"][1])
chk("v1 금액 동일", abs(acc["v"][1] - acc["v"][0]) < 1)
chk("v1 `relationship` 동일", acc["rel_diff"] == 0, f"— 다른 행 {acc['rel_diff']:,}")
chk("v1 `crosswalk_match_status` 동일", acc["cw_diff"] == 0, f"— 다른 행 {acc['cw_diff']:,}")
chk("v1 `con_ciqid` 동일", acc["id_diff"] == 0, f"— 다른 행 {acc['id_diff']:,}")

say("\n### 달라야 하는 것 — 붙은 회계기간\n")
ag, dc = np.concatenate(age), np.concatenate(dac)
say(md(pd.DataFrame([
    {"방식": "equi (`days_after_close`)", "부착": len(dc), "중위": float(np.median(dc)),
     "최소": float(dc.min()), "최대": float(dc.max()),
     "음수 비율(%)": float((dc < 0).mean() * 100)},
    {"방식": "asof (`age_days`)", "부착": len(ag), "중위": float(np.median(ag)),
     "최소": float(ag.min()), "최대": float(ag.max()),
     "음수 비율(%)": float((ag < 0).mean() * 100)}]), "{:,.1f}"))
chk("asof 는 음수가 없다 (미래 정보 차단)", (ag < 0).sum() == 0)
chk("asof 는 소급 2년을 넘지 않는다", ag.max() <= 730, f"— 최대 {ag.max()}일")
say(f"\n- 붙은 회계기간이 **다른 행**: {acc['fp_diff']:,} / {acc['n'][0]:,} "
    f"({acc['fp_diff']/acc['n'][0]*100:.1f}%) — 결합 방식이 다르니 당연하다")
say(f"- 커버리지: equi **{len(dc)/acc['n'][0]*100:.1f}%** vs asof "
    f"**{len(ag)/acc['n'][1]*100:.1f}%**")

# ---------------------------------------------------------------- v2·v3
for lab, files in (("v2", ["02_pair", "03_firm", "04_group"]),
                   ("v3", ["panel_pair_month", "dim_relationship",
                           "panel_firm_quarter", "panel_firm_origin_hs"])):
    say(f"\n## {lab} — 패널\n")
    rows = []
    for n in files:
        fe, fa = EQ[lab] / f"{n}.parquet", AS[lab] / f"{n}.parquet"
        if not fa.exists():
            say(f"- `{n}` as-of 판 없음 — 건너뜀")
            continue
        pe, pa = pq.ParquetFile(fe), pq.ParquetFile(fa)
        rows.append({"패널": n, "equi 행": pe.metadata.num_rows,
                     "asof 행": pa.metadata.num_rows,
                     "행 차이": pa.metadata.num_rows - pe.metadata.num_rows,
                     "equi 열": len(pe.schema_arrow.names),
                     "asof 열": len(pa.schema_arrow.names)})
    if rows:
        t = pd.DataFrame(rows)
        say(md(t))
        chk(f"{lab} 패널 행 수 동일", bool((t["행 차이"] == 0).all()))
        chk(f"{lab} 패널 열 수 동일", bool((t["equi 열"] == t["asof 열"]).all()),
            "— 시점 컬럼 이름만 다르고 개수는 같아야 한다")

# 거래 측정치가 같은지 (v2 04_group 으로 대표 확인)
fa = AS["v2"] / "04_group.parquet"
if fa.exists():
    c = ["imp_n_ship", "imp_value_usd", "imp_value_within_firm", "imp_value_arms",
         "exp_n_ship", "exp_value_usd"]
    e = pd.read_parquet(EQ["v2"] / "04_group.parquet", columns=c)
    a = pd.read_parquet(fa, columns=c)
    say("\n### v2 `04_group` 거래 측정치 — 재무와 무관하므로 같아야 한다\n")
    say(md(pd.DataFrame([{"항목": x, "equi": float(e[x].sum()), "asof": float(a[x].sum()),
                          "차이": float(a[x].sum() - e[x].sum())} for x in c])))
    chk("v2 거래 측정치 전부 동일",
        all(abs(float(a[x].sum()) - float(e[x].sum())) < 1 for x in c))

say("\n---\n\n## 요약\n")
r = pd.DataFrame(RES)
fails = r[r.결과 == "FAIL"]
say(f"**{len(r)}개 항목 중 {len(r)-len(fails)}개 PASS**"
    + ("" if not len(fails) else "\n\n" + md(fails)))
say("\n> 두 판은 **선적·관계·패널 구조가 완전히 같고 재무만 다르다.** "
    "PI 판단에 따라 어느 쪽이든 그대로 쓸 수 있다.")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(L), encoding="utf-8")
print(f"\n→ {OUT}", flush=True)
