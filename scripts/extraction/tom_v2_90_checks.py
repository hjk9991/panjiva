# -*- coding: utf-8 -*-
r"""
tom_v2_90_checks.py — v2 패널 검증. 명세 §11 게이트 중 v2 가 책임지는 항목.

산출: `--dir` 안에 `90_checks.md`

  G8   `within_share` 가 [0,1] 범위이고 분자·분모가 재계산된다
  G13  v1(선적 base)과 선적 수·금액이 대사된다 — 세 패널 각각
  G14  컬럼 소문자 · 코드성 식별자 정수 보존
  V-A  키 유일성 (세 패널)
  V-B  관계 분해 합계 = 총액 (within_firm + arms_length + unmatched)
  V-C  equi-join 정합 — 블록의 cal_year·cal_quarter 가 분기와 일치, 미부착 행은 메타 결측
  V-D  03 과 04 의 총계가 서로 대사된다 (같은 무역을 다른 단위로 집계했으므로)
  V-E  재무 커버리지 — 행·금액 기준, 블록별
"""

import argparse
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(r"C:\panjiva\data\staging\tom_v2_2024")
V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")

PANELS = {
    "02_pair": (["shp_ciqid", "con_ciqid", "trade_quarter"],
                ["shp_a_", "shp_q_", "shp_up_a_", "shp_up_q_",
                 "con_a_", "con_q_", "con_up_a_", "con_up_q_"], ""),
    "03_firm": (["companyid", "trade_quarter"],
                ["fin_a_", "fin_q_", "up_a_", "up_q_"], "imp_"),
    "04_group": (["ultimate_parent_companyid", "trade_quarter"],
                 ["fin_a_", "fin_q_"], "imp_"),
}
L = []
TCOL = "days_after_close"


def time_col(names) -> str:
    """결합 방식을 컬럼 이름으로 판별 — asof 는 `age_days`, equi 는 `days_after_close`."""
    return "age_days" if any(n.endswith("_age_days") for n in names) else "days_after_close"


def say(s=""):
    print(s)
    L.append(s)


def md(df, fmt="{:,.2f}"):
    def f(v):
        if isinstance(v, float):
            return fmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return str(v)
    return "\n".join(["| " + " | ".join(map(str, df.columns)) + " |",
                      "|" + "|".join(["---"] * len(df.columns)) + "|"]
                     + ["| " + " | ".join(f(v) for v in r) + " |"
                        for r in df.itertuples(index=False)])


def v1_totals(v1: Path) -> dict:
    """v1 에서 대사 기준값을 뽑는다 — 어느 키가 있는 선적의 건수·금액."""
    acc = {k: [0, 0.0] for k in ["all", "pair", "con_ciqid", "shp_ciqid",
                                 "con_up", "shp_up"]}
    for f in sorted(v1.glob("shipment_master_*.parquet")):
        d = pd.read_parquet(f, columns=["valueofgoodsusd", "con_ciqid", "shp_ciqid",
                                        "con_up", "shp_up"])
        v = d.valueofgoodsusd.fillna(0)
        acc["all"][0] += len(d); acc["all"][1] += v.sum()
        m = d.con_ciqid.notna() & d.shp_ciqid.notna()
        acc["pair"][0] += int(m.sum()); acc["pair"][1] += float(v[m].sum())
        for k in ["con_ciqid", "shp_ciqid", "con_up", "shp_up"]:
            m = d[k].notna()
            acc[k][0] += int(m.sum()); acc[k][1] += float(v[m].sum())
        del d
    return acc


def main():
    global OUT, V1
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(OUT))
    ap.add_argument("--v1-dir", default=str(V1))
    a = ap.parse_args()
    OUT, V1 = Path(a.dir), Path(a.v1_dir)

    global TCOL
    # 결합 방식은 **존재하는 첫 패널**의 스키마로 판별한다 (`--only` 로 일부만 만든 폴더도 통한다)
    present = [OUT / f"{n}.parquet" for n in PANELS if (OUT / f"{n}.parquet").exists()]
    if not present:
        raise SystemExit(f"검증할 패널이 없다: {OUT}")
    TCOL = time_col(pq.ParquetFile(present[0]).schema_arrow.names)
    ASOF = TCOL == "age_days"

    say("# v2 패널 — 검증 결과\n")
    say(f"**검증일** {date.today()} · **대상** `{OUT}` · **선적 base** `{V1}` · "
        f"**결합** {'asof' if ASOF else 'equi'}\n")
    say(f"**재무 결합 방식**: **{'as-of (명세 §3.3)' if ASOF else 'equi-join'}** "
        f"— 시점 컬럼 `*_{TCOL}` (판별 근거: `{present[0].name}` 스키마)\n")
    docs = [n for n in ("DECISIONS.md", "COLUMNS.md") if (OUT / n).exists()]
    if docs:
        say("같은 폴더의 " + " · ".join(
            {"DECISIONS.md": "`DECISIONS.md`(결정 근거)", "COLUMNS.md": "`COLUMNS.md`(컬럼 뜻)"}[d]
            for d in docs) + " 참조.\n")

    print("v1 대사 기준값 계산 중...")
    t1 = v1_totals(V1)
    say(f"\n**선적 base (v1)**: {t1['all'][0]:,}건 · ${t1['all'][1]/1e9:,.1f}B\n")

    rows, gate = [], {}
    for name, (keys, blocks, agg_prefix) in PANELS.items():
        f = OUT / f"{name}.parquet"
        if not f.exists():
            say(f"- `{name}.parquet` 없음 — 건너뜀")
            continue
        sch = pq.ParquetFile(f).schema_arrow
        n_row, n_col = pq.ParquetFile(f).metadata.num_rows, len(sch.names)
        say(f"\n---\n\n## {name} — {n_row:,}행 × {n_col:,}열\n")

        # 필요한 컬럼만 읽는다 (02_pair 는 6,400열이라 전부 읽으면 안 된다)
        need = list(keys) + ["period_start", "cal_year", "cal_quarter"]
        for b in blocks:
            need += [f"{b}{c}" for c in ("financial_period_id", "period_end",
                                         "cal_year", "cal_quarter", TCOL)]
        pre = ["", "imp_", "exp_"] if agg_prefix else [""]
        for p in pre:
            need += [f"{p}{c}" for c in
                     ("n_ship", "value_usd", "value_within_firm", "value_arms",
                      "value_unmatched", "value_classified", "value_self",
                      "n_within_firm", "n_arms", "n_classified",
                      "within_share_value", "within_share_count", "within_share",
                      "within_firm", "relationship_mixed")]
        need = [c for c in dict.fromkeys(need) if c in sch.names]
        d = pd.read_parquet(f, columns=need)

        # ---- V-A 키 유일성
        dup = int(d.duplicated(keys).sum())
        say(f"- **V-A** 키 `{tuple(keys)}` 중복: **{dup:,}행** — "
            f"{'PASS' if dup == 0 else 'FAIL'}")
        gate[f"{name}/V-A"] = dup == 0

        # ---- G13 v1 대사
        say("\n### G13 — v1 선적 base 와 대사\n")
        r = []
        if name == "02_pair":
            r.append(("양측 식별 선적", t1["pair"], int(d.n_ship.sum()), d.value_usd.sum()))
        else:
            kmap = {"03_firm": ("con_ciqid", "shp_ciqid"),
                    "04_group": ("con_up", "shp_up")}[name]
            r.append(("수입 측", t1[kmap[0]], int(d.imp_n_ship.sum()),
                      d.imp_value_usd.sum()))
            r.append(("수출 측", t1[kmap[1]], int(d.exp_n_ship.sum()),
                      d.exp_value_usd.sum()))
        tt = pd.DataFrame([{"구분": lab, "v1 선적": ref[0], "패널 선적": n,
                            "선적 차이": n - ref[0], "v1 금액($B)": ref[1] / 1e9,
                            "패널 금액($B)": v / 1e9, "금액 차이($)": v - ref[1]}
                           for lab, ref, n, v in r])
        say(md(tt))
        ok13 = bool((tt["선적 차이"] == 0).all() and (tt["금액 차이($)"].abs() < 1).all())
        say(f"\n- **G13**: **{'PASS' if ok13 else 'FAIL'}**")
        gate[f"{name}/G13"] = ok13

        # ---- V-B 관계 분해 합계
        say("\n### V-B — 관계 분해 합계 = 총액\n")
        r = []
        for p in pre:
            if f"{p}value_usd" not in d:
                continue
            s = d[[f"{p}value_within_firm", f"{p}value_arms",
                   f"{p}value_unmatched", f"{p}value_usd", f"{p}value_self",
                   f"{p}value_classified"]].sum()
            tot = s[f"{p}value_within_firm"] + s[f"{p}value_arms"] + s[f"{p}value_unmatched"]
            r.append({"묶음": p or "(전체)",
                      "within_firm($B)": s[f"{p}value_within_firm"] / 1e9,
                      "arms_length($B)": s[f"{p}value_arms"] / 1e9,
                      "unmatched($B)": s[f"{p}value_unmatched"] / 1e9,
                      "합($B)": tot / 1e9, "value_usd($B)": s[f"{p}value_usd"] / 1e9,
                      "차이($)": tot - s[f"{p}value_usd"],
                      "그중 self($B)": s[f"{p}value_self"] / 1e9,
                      "분류가능($B)": s[f"{p}value_classified"] / 1e9})
        tb = pd.DataFrame(r)
        say(md(tb))
        okb = bool((tb["차이($)"].abs() < 1).all())
        say(f"\n- **V-B**: **{'PASS' if okb else 'FAIL'}**")
        gate[f"{name}/V-B"] = okb

        # ---- G8 within_share
        say("\n### G8 — `within_share` 범위·재계산\n")
        r = []
        for p in pre:
            c = f"{p}within_share"
            if c not in d:
                continue
            s = d[c].dropna()
            recalc = (d[f"{p}value_within_firm"] / d[f"{p}value_classified"]).where(
                d[f"{p}value_classified"] > 0)
            bad = int((recalc.notna() & d[f"{p}within_share_value"].notna()
                       & (np.abs(recalc - d[f"{p}within_share_value"]) > 1e-12)).sum())
            wf = d[f"{p}within_firm"]
            mism = int(((wf == 1) != (d[c] > 0.5)).sum() - wf.isna().sum())
            r.append({"묶음": p or "(전체)", "비결측": len(s),
                      "최소": float(s.min()) if len(s) else np.nan,
                      "최대": float(s.max()) if len(s) else np.nan,
                      "[0,1] 벗어남": int(((s < 0) | (s > 1)).sum()),
                      "분자/분모 재계산 불일치": bad,
                      "within_firm=(share>0.5) 불일치": max(mism, 0)})
        tg = pd.DataFrame(r)
        say(md(tg, "{:,.4f}"))
        ok8 = bool((tg["[0,1] 벗어남"] == 0).all()
                   and (tg["분자/분모 재계산 불일치"] == 0).all())
        say(f"\n- **G8**: **{'PASS' if ok8 else 'FAIL'}**")
        gate[f"{name}/G8"] = ok8

        # ---- V-C 결합 정합 + V-E 커버리지
        say(f"\n### V-C·V-E — {'as-of' if ASOF else 'equi-join'} 정합과 재무 커버리지\n")
        if ASOF:
            say("**명세 §3.3 as-of** — 분기 시작일보다 먼저 끝난 회계기간 중 가장 최근. "
                "`age_days` 가 1~730 안에 있고 결산일이 기준일보다 앞서야 한다.\n")
        else:
            say("조인 키는 **분기 시작일의 달력 연·분기**다. 붙은 회계기간의 라벨과 "
                "일치해야 한다.\n")
        ty, tq = d.period_start.dt.year, d.period_start.dt.quarter
        vcol = f"{agg_prefix}value_usd" if agg_prefix else "value_usd"
        r = []
        for b in blocks:
            h = d[f"{b}financial_period_id"].notna()
            leak = int((~h & d[f"{b}cal_year"].notna()).sum())     # 미부착인데 메타가 남음
            ages = d[f"{b}{TCOL}"].dropna().to_numpy().astype("int64")
            if ASOF:
                # ⚠️ as-of 는 달력 라벨로 붙지 않으므로 `cal_year` 일치를 볼 이유가 없다.
                bad = (int((h & (d[f"{b}{TCOL}"] < 1)).sum())
                       + int((h & (d[f"{b}{TCOL}"] > 730)).sum())
                       + int((h & (d[f"{b}period_end"] >= d.period_start)).sum()))
            else:
                q = d[f"{b}cal_quarter"]
                bad = (int((h & (d[f"{b}cal_year"] != ty)).sum())
                       + int((h & q.notna() & (q != tq)).sum()))
            r.append({"블록": b.rstrip("_"), "부착 행": int(h.sum()),
                      "행 커버(%)": h.mean() * 100,
                      "금액 커버(%)": d.loc[h, vcol].sum() / d[vcol].sum() * 100,
                      f"{TCOL} 중위": float(np.median(ages)) if len(ages) else np.nan,
                      "진행중(음수)%": float((ages < 0).mean() * 100) if len(ages) else np.nan,
                      "위반" if ASOF else "키 불일치": bad,
                      "미부착인데 메타 남음": leak})
        tc = pd.DataFrame(r)
        say(md(tc, "{:,.1f}"))
        kc = "위반" if ASOF else "키 불일치"
        okc = bool((tc[kc] == 0).all() and (tc["미부착인데 메타 남음"] == 0).all())
        say(f"\n- **V-C**: **{'PASS' if okc else 'FAIL'}**")
        gate[f"{name}/V-C"] = okc

        # ---- G14
        upper = [c for c in sch.names if c != c.lower()]
        idc = [c for c in keys if "quarter" not in c] + \
              [f"{b}financial_period_id" for b in blocks]
        badt = [c for c in idc if c in sch.names
                and not pa.types.is_integer(sch.field(c).type)]
        pdm = json.loads(sch.metadata[b"pandas"].decode())
        nul = sum(1 for c in pdm["columns"]
                  if str(c.get("numpy_type", "")).startswith(("Int", "UInt")))
        say(f"\n- **G14** 대문자 컬럼 {len(upper)}개 · 식별자 비정수 {len(badt)}개 · "
            f"nullable 정수 복원 {nul}개 — "
            f"{'PASS' if not upper and not badt else 'FAIL'}")
        gate[f"{name}/G14"] = not upper and not badt

        rows.append({"패널": name, "행": n_row, "열": n_col,
                     "크기(MB)": f.stat().st_size / 1e6})
        del d

    # ---- V-D 03·04 교차대사
    f3, f4 = OUT / "03_firm.parquet", OUT / "04_group.parquet"
    if f3.exists() and f4.exists():
        say("\n---\n\n## V-D — 03 과 04 교차대사\n")
        say("같은 무역을 **법인 단위**와 **기업집단 단위**로 각각 집계한 것이라 총계가 같아야 한다.\n")
        c = ["imp_n_ship", "imp_value_usd", "exp_n_ship", "exp_value_usd"]
        d3, d4 = pd.read_parquet(f3, columns=c), pd.read_parquet(f4, columns=c)
        r = []
        for col in c:
            r.append({"항목": col, "03_firm": d3[col].sum(), "04_group": d4[col].sum(),
                      "차이": d3[col].sum() - d4[col].sum()})
        td = pd.DataFrame(r)
        say(md(td))

        # 차이가 있으면 원인을 v1 에서 직접 세어 대사한다 — 추측하지 않는다
        if (td["차이"].abs() > 0.5).any():
            say("\n### 차이 원인 — v1 에서 직접 대사\n")
            say("**crosswalk 은 성공했는데 최종모회사가 없는 선적**은 03(법인 단위)에는 "
                "들어가고 04(집단 단위)에서는 빠진다. 그 수를 v1 에서 세어 본다.\n")
            acc = {"con": [0, 0.0], "shp": [0, 0.0]}
            firms = {}
            for g in sorted(V1.glob("shipment_master_*.parquet")):
                x = pd.read_parquet(g, columns=[
                    "valueofgoodsusd", "con_ciqid", "con_up", "shp_ciqid", "shp_up",
                    "con_ciq_name", "shp_ciq_name"])
                vv = x.valueofgoodsusd.fillna(0)
                for side in ("con", "shp"):
                    m = x[f"{side}_ciqid"].notna() & x[f"{side}_up"].isna()
                    acc[side][0] += int(m.sum()); acc[side][1] += float(vv[m].sum())
                    for cid, nm in zip(x.loc[m, f"{side}_ciqid"],
                                       x.loc[m, f"{side}_ciq_name"]):
                        firms.setdefault(int(cid), nm)
                del x
            say(md(pd.DataFrame([
                {"측": "수입(imp)", "선적": acc["con"][0], "금액($)": acc["con"][1],
                 "03−04 선적차": int(td.loc[td["항목"] == "imp_n_ship", "차이"].iloc[0]),
                 "03−04 금액차($)": float(td.loc[td["항목"] == "imp_value_usd", "차이"].iloc[0])},
                {"측": "수출(exp)", "선적": acc["shp"][0], "금액($)": acc["shp"][1],
                 "03−04 선적차": int(td.loc[td["항목"] == "exp_n_ship", "차이"].iloc[0]),
                 "03−04 금액차($)": float(td.loc[td["항목"] == "exp_value_usd", "차이"].iloc[0])},
            ])))
            okd = (acc["con"][0] == int(td.loc[td["항목"] == "imp_n_ship", "차이"].iloc[0])
                   and acc["shp"][0] == int(td.loc[td["항목"] == "exp_n_ship", "차이"].iloc[0]))
            say(f"\n- **V-D** 차이가 위 원인으로 **완전히 설명됨**: "
                f"**{'PASS' if okd else 'FAIL — 설명되지 않는 차이가 남았다'}**")
            gate["03·04/V-D"] = okd
            say(f"\n해당 법인 **{len(firms)}개**: "
                + " · ".join(f"`{k}`({v if isinstance(v, str) else '이름없음'})"
                             for k, v in sorted(firms.items())[:10]))
            say("\n> **왜 UP 이 없나**: 이들은 `panjivaCompanyCrossRef` 에는 있어 crosswalk 은 "
                "성공하지만, `ciqCompanyUltimateParentPIT` 구간이 거래일을 덮지 않고 "
                "스냅샷(`ciqCompanyUltimateParent`)에도 기록이 없다. 일부는 "
                "**`ciqCompany` 자체에 없어 이름·국가도 결측**이다 — crosswalk 에만 존재하는 ID 다.")
        else:
            gate["03·04/V-D"] = True
            say("\n- **V-D**: **PASS** (차이 없음)")

        say("\n> ⚠️ 두 패널은 **총계는 대사되지만 행 단위가 다르다**. 명세 §8.2 대로 "
            "**서로 UNION 하지 않는다** — 합치면 같은 무역을 두 번 센다.")

    say("\n---\n\n## 요약\n")
    say(md(pd.DataFrame(rows), "{:,.0f}"))
    say("")
    fails = [k for k, v in gate.items() if not v]
    say(f"**게이트 {len(gate)}개 중 {len(gate)-len(fails)}개 통과**"
        + (f" · 실패: {', '.join(fails)}" if fails else " — 전항 PASS"))

    (OUT / "90_checks.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n→ {OUT / '90_checks.md'}")


if __name__ == "__main__":
    main()
