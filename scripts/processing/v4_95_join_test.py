# -*- coding: utf-8 -*-
r"""
v4_95_join_test.py — 따로 둔 두 쪽이 실제로 붙는가. **v4_join 모듈을 그대로 검증한다** —
여기서 통과한 코드가 곧 사용자가 import 하는 코드다 (README 스니펫 복사 금지).

검사: J1 행 증식 없음 · J2 커버리지(법인 vs 모회사) · J3 wide 값 = long 원자료 ·
      J5 연간 join · J6 to_usd 방향 (환산 후 값이 상식 범위인가)
FAIL 이 있으면 exit 1.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from v4_common import OUT_FULL
from v4_join import attach_financials, load_fin, load_trade, to_usd

R = []


def chk(no, name, ok, detail):
    R.append({"no": no, "check": name, "result": "PASS" if ok else "**FAIL**", "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {no}. {name} — {detail}")


TRADE_COLS = ["trade_quarter", "cal_year", "cal_quarter", "shp_panjivaid", "con_panjivaid",
              "shp_ciqid", "con_ciqid", "shp_up", "con_up", "shp_name", "con_name",
              "hs6", "value_usd", "is_intra_group"]
FIN_META = ["financial_period_id", "companyid", "period_type_id", "cal_year", "cal_quarter",
            "period_end", "currency", "fx_per_usd"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(OUT_FULL))
    a = ap.parse_args()
    out = Path(a.dir)

    t = load_trade(out, columns=TRADE_COLS)
    n0, V = len(t), t.value_usd.sum()
    cat = pd.read_csv(out / "ciq_dataitem_catalog.csv")
    want = [28, 1007, 15, 10, 21, 4051, 4173, 1275]
    use = cat.set_index("data_item_id").loc[want, "column_name"].tolist()
    wq = load_fin("quarter", out, columns=FIN_META + use)
    wa = load_fin("annual", out, columns=FIN_META + use)
    print(f"무역 {n0:,}행 ${V/1e9:,.0f}B · 분기재무 {len(wq):,}행 · 연간재무 {len(wa):,}행")

    dupq = int(wq.duplicated(["companyid", "cal_year", "cal_quarter"]).sum())
    dupa = int(wa.duplicated(["companyid", "cal_year"]).sum())
    chk("J1a", "재무 키 유일 (분기·연간)", dupq == 0 and dupa == 0,
        f"분기 중복 {dupq} · 연간 중복 {dupa}")

    # J1b — attach_financials 는 행이 늘면 스스로 AssertionError 를 던진다
    j = attach_financials(t, wq, side="shp")
    j = attach_financials(j, wq, side="con")
    chk("J1b", "attach_financials 양측 적용 후 행 증식 없음 (모듈 내장 검증 포함)",
        len(j) == n0, f"{n0:,} -> {len(j):,}")

    rev_s, rev_c = f"shp_fin_{use[0]}", f"con_fin_{use[0]}"
    ms, mc = j[rev_s].notna(), j[rev_c].notna()
    both = ms & mc
    chk("J2", "커버리지 (모회사 키, 분기 매출)", bool(both.any()),
        f"수출자 {ms.mean()*100:.1f}%행/{j.loc[ms,'value_usd'].sum()/V*100:.1f}%금액 · "
        f"수입자 {mc.mean()*100:.1f}%/{j.loc[mc,'value_usd'].sum()/V*100:.1f}% · "
        f"**양측 {both.mean()*100:.1f}%행/{j.loc[both,'value_usd'].sum()/V*100:.1f}%금액**")

    j2 = attach_financials(t, wq, side="con", key="ciqid")
    m2 = j2[rev_c].notna()
    chk("J2b", "법인 키 대비 모회사 키의 이득", True,
        f"수입자 법인 {j2.loc[m2,'value_usd'].sum()/V*100:.1f}%금액 vs "
        f"모회사 {j.loc[mc,'value_usd'].sum()/V*100:.1f}%금액")
    del j2

    # J3 — wide 값이 long 원자료와 같은가 (매출 전수)
    per = pd.read_parquet(out / "ciq_fin_period.parquet",
                          columns=["financial_period_id", "companyid", "period_type_id",
                                   "cal_year", "cal_quarter", "is_preferred"])
    per = per[(per.period_type_id == 2) & (per.is_preferred == 1)]
    parts = []
    for p in sorted(out.glob("ciq_fin_long_*.parquet")):
        lg = pd.read_parquet(p, columns=["financial_period_id", "data_item_id", "value"])
        parts.append(lg[lg.data_item_id == want[0]])
    raw = (pd.concat(parts, ignore_index=True)
           .drop_duplicates("financial_period_id", keep="first")
           .merge(per, on="financial_period_id"))
    cmp_ = raw.merge(wq[["companyid", "cal_year", "cal_quarter", use[0]]],
                     on=["companyid", "cal_year", "cal_quarter"], how="inner")
    bad = int((~np.isclose(cmp_["value"], cmp_[use[0]], rtol=0, atol=1e-9,
                           equal_nan=True)).sum())
    chk("J3", "wide 값 = long 원자료 (매출 전수 대조)", bad == 0,
        f"불일치 {bad:,} / {len(cmp_):,}행")
    del raw, cmp_, parts

    ja = attach_financials(t, wa, side="shp")
    ja = attach_financials(ja, wa, side="con")
    ba = ja[rev_s].notna() & ja[rev_c].notna()
    chk("J5", "연간 wide 도 동일하게 붙음 (주기 자동 판별)", len(ja) == n0,
        f"행 {n0:,} -> {len(ja):,} · 양측커버 {ba.mean()*100:.1f}%행/"
        f"{ja.loc[ba,'value_usd'].sum()/V*100:.1f}%금액")
    del ja

    # J6 — to_usd 방향 검증: 비-USD 대기업 매출을 환산하면 상식 범위($1M~$10T)여야 한다
    nz = j[both & j.shp_fin_currency.ne("USD")].nlargest(50, "value_usd")
    usd = to_usd(nz[rev_s], nz["shp_fin_fx_per_usd"]) * 1e6   # 백만 -> 달러
    ok6 = bool(((usd > 1e6) & (usd < 1e13)).all()) if len(nz) else False
    chk("J6", "to_usd 환산 방향 (비-USD 상위 50 무역기업 매출이 $1M~$10T 범위)",
        ok6, f"{len(nz)}건 · 범위 ${usd.min()/1e9:.1f}B ~ ${usd.max()/1e9:.1f}B" if len(nz) else "표본 없음")

    ex = (j[both & (j.is_intra_group == 1)]
          .nlargest(6, "value_usd")[["trade_quarter", "shp_name", "shp_up", "con_name",
                                     "con_up", "hs6", "value_usd", rev_s, rev_c,
                                     "shp_fin_currency", "con_fin_currency"]])

    md = [f"# v4 결합 테스트 ({out.name}) — 따로 둔 두 쪽이 실제로 붙는가", "",
          f"**검증일**: {pd.Timestamp.now():%Y-%m-%d} · **스크립트**: "
          "`scripts\\processing\\v4_95_join_test.py`", "",
          "**붙이는 코드의 정본은 `scripts\\processing\\v4_join.py` 다.** 이 테스트는 그 모듈의",
          "`load_trade` / `load_fin` / `attach_financials` / `to_usd` 를 그대로 호출해 검증한다 —",
          "README 의 예시를 베끼지 말고 모듈을 import 할 것.", "",
          "## 결과", "", "| # | 검사 | 결과 | 상세 |", "|---|---|---|---|"]
    for r in R:
        md.append(f"| {r['no']} | {r['check']} | {r['result']} | {r['detail']} |")
    md += ["", "## 붙인 모양 (그룹내 거래 상위)", "",
           "| 분기 | 수출자 | UP | 수입자 | UP | HS | 무역액($M) | 수출자매출 | 수입자매출 | 통화 |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for r in ex.itertuples():
        md.append(f"| {r.trade_quarter} | {str(r.shp_name)[:20]} | {r.shp_up} | "
                  f"{str(r.con_name)[:20]} | {r.con_up} | {r.hs6} | {r.value_usd/1e6:,.0f} | "
                  f"{getattr(r, rev_s):,.0f} | {getattr(r, rev_c):,.0f} | "
                  f"{r.shp_fin_currency}/{r.con_fin_currency} |")
    md += ["", "> 재무 값은 **백만 · 원표시통화**. USD 는 `v4_join.to_usd(value, fx_per_usd)` — "
           "내부가 **나눗셈**이고 J6 이 방향을 검증한다.", "",
           "## 쓰는 법", "", "```python",
           'import sys; sys.path.insert(0, r"C:\\panjiva\\projects\\20251201\\scripts\\processing")',
           "from v4_join import load_trade, load_fin, attach_financials, to_usd",
           "t = load_trade()                       # 연도 파일 자동 결합",
           'f = load_fin("quarter")                # 또는 "annual"',
           't = attach_financials(t, f, side="shp")',
           't = attach_financials(t, f, side="con")',
           "```", "",
           "- 카탈로그 밖 계정: `v4_join.load_fin_long_items([id...])`",
           "- 법인 자신 기준: `attach_financials(..., key=\"ciqid\")` (커버리지는 크게 떨어짐)"]
    (out / "95_join_test.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    fails = sum(1 for r in R if "FAIL" in r["result"])
    print(f"\n-> {out / '95_join_test.md'}   FAIL {fails}개")
    print(ex.to_string(index=False))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
