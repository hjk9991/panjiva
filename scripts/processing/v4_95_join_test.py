# -*- coding: utf-8 -*-
r"""
v4_95_join_test.py — 따로 둔 두 쪽이 실제로 붙는가. **v4_join 모듈을 그대로 검증한다** —
여기서 통과한 코드가 곧 사용자가 import 하는 코드다 (README 스니펫 복사 금지).

검사: J1a 재무 키 유일 · J1b 행 증식 없음 · J2 커버리지 실검사(비율 범위·양측<=각측·행 보존) ·
      J2b 법인 키 경로 · J3 wide 값 = long 원자료 · J4 items= 계정 선택 경로 ·
      J5 연간 join (cal_quarter 없는 무역에도) · J6 to_usd 방향 · J7 주기·중복 검증이 예외를 내는가 ·
      J8 load_fin_long_items(years=) = wide
FAIL 이 있으면 exit 1.

사용:
    python v4_95_join_test.py                                   # v4_pairhs_full (무역·재무 같은 폴더)
    python v4_95_join_test.py --dir <무역폴더> --fin-dir <재무폴더> --out <md 폴더>
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from v4_common import OUT_FULL, file_years, write_manifest
from v4_join import attach_financials, load_fin, load_fin_long_items, load_trade, to_usd

R = []


def chk(no, name, ok, detail):
    R.append({"no": no, "check": name, "result": "PASS" if ok else "**FAIL**", "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {no}. {name} — {detail}")
    return ok


def raises(fn, exc=Exception, needle=None):
    """fn() 이 exc 를 내면 (True, 메시지), 아니면 (False, 설명)."""
    try:
        fn()
    except exc as e:
        msg = str(e)
        return (needle is None or needle in msg), msg[:80]
    except Exception as e:                       # noqa: BLE001
        return False, f"다른 예외 {type(e).__name__}: {str(e)[:60]}"
    return False, "예외 없음"


TRADE_COLS = ["trade_quarter", "cal_year", "cal_quarter", "shp_panjivaid", "con_panjivaid",
              "shp_ciqid", "con_ciqid", "shp_up", "con_up", "shp_name", "con_name",
              "hs6", "value_usd", "is_intra_group"]
FIN_META = ["financial_period_id", "companyid", "period_type_id", "cal_year", "cal_quarter",
            "period_end", "currency", "fx_per_usd"]


def main():
    ap = argparse.ArgumentParser(description="v4 결합 테스트 (v4_join 모듈 검증)")
    ap.add_argument("--dir", default=str(OUT_FULL), help="무역 팩트 폴더")
    ap.add_argument("--fin-dir", default=None, help="재무 산출 폴더 (기본 = --dir)")
    ap.add_argument("--out", default=None, help="95_join_test.md 를 쓸 폴더 (기본 = --dir)")
    a = ap.parse_args()
    tdir = Path(a.dir)
    fdir = Path(a.fin_dir) if a.fin_dir else tdir
    out = Path(a.out) if a.out else tdir
    out.mkdir(parents=True, exist_ok=True)

    trade_files = sorted(tdir.glob("trade_pair_hs_quarter_*.parquet"))
    t = load_trade(tdir, columns=TRADE_COLS)
    n0, V = len(t), t.value_usd.sum()
    cat = pd.read_csv(fdir / "ciq_dataitem_catalog.csv")
    want = [28, 1007, 15, 10, 21, 4051, 4173, 1275]
    use = cat.set_index("data_item_id").loc[want, "column_name"].tolist()
    wq = load_fin("quarter", fdir, columns=FIN_META + use)
    wa = load_fin("annual", fdir, columns=FIN_META + use)
    print(f"무역 {n0:,}행 ${V/1e9:,.0f}B ({tdir}) · 분기재무 {len(wq):,}행 · 연간재무 {len(wa):,}행 ({fdir})")

    dupq = int(wq.duplicated(["companyid", "cal_year", "cal_quarter"]).sum())
    dupa = int(wa.duplicated(["companyid", "cal_year"]).sum())
    chk("J1a", "재무 키 유일 (분기·연간)", dupq == 0 and dupa == 0,
        f"분기 중복 {dupq} · 연간 중복 {dupa}")

    # J1b — attach_financials 는 행이 늘면 스스로 AssertionError 를 던진다
    j = attach_financials(t, wq, side="shp")
    j = attach_financials(j, wq, side="con")
    chk("J1b", "attach_financials 양측 적용 후 행 증식 없음 (모듈 내장 검증 포함)",
        len(j) == n0, f"{n0:,} -> {len(j):,}")

    # J2 — 커버리지 실검사: 비율이 [0,1] · 양측 <= 각측 · 행 보존 · 붙은 행이 있음
    rev_s, rev_c = f"shp_fin_{use[0]}", f"con_fin_{use[0]}"
    ms, mc = j[rev_s].notna(), j[rev_c].notna()
    both = ms & mc
    rs, rc, rb = ms.mean(), mc.mean(), both.mean()
    vs, vc, vb = (j.loc[ms, "value_usd"].sum() / V, j.loc[mc, "value_usd"].sum() / V,
                  j.loc[both, "value_usd"].sum() / V)
    ok2 = (len(j) == n0 and both.any()
           and all(0.0 <= x <= 1.0 for x in [rs, rc, rb, vs, vc, vb])
           and rb <= min(rs, rc) and vb <= min(vs, vc)
           and int(both.sum()) == int((ms & mc).sum()))
    chk("J2", "커버리지 (모회사 키, 분기 매출) — 비율∈[0,1] · 양측<=각측 · 행 보존", ok2,
        f"수출자 {rs*100:.1f}%행/{vs*100:.1f}%금액 · 수입자 {rc*100:.1f}%/{vc*100:.1f}% · "
        f"**양측 {rb*100:.1f}%행/{vb*100:.1f}%금액**")

    # J2b — 법인 키 경로: 행 보존 · 비율 범위. 모회사 키와의 비교는 참고값
    j2 = attach_financials(t, wq, side="con", key="ciqid")
    m2 = j2[rev_c].notna()
    v2 = j2.loc[m2, "value_usd"].sum() / V
    chk("J2b", "법인 키(key='ciqid') 경로 — 행 보존 · 비율∈[0,1] (모회사 키 대비는 참고)",
        len(j2) == n0 and 0.0 <= v2 <= 1.0 and 0.0 <= m2.mean() <= 1.0,
        f"수입자 법인 {m2.mean()*100:.1f}%행/{v2*100:.1f}%금액 vs 모회사 {rc*100:.1f}%/{vc*100:.1f}% "
        f"(배율 {vc / v2 if v2 else float('nan'):.1f}x)")
    del j2

    # J3 — wide 값이 long 원자료와 같은가 (매출 전수)
    per = pd.read_parquet(fdir / "ciq_fin_period.parquet",
                          columns=["financial_period_id", "companyid", "period_type_id",
                                   "cal_year", "cal_quarter", "is_preferred"])
    per = per[(per.period_type_id == 2) & (per.is_preferred == 1)]
    parts = []
    for p in sorted(fdir.glob("ciq_fin_long_*.parquet")):
        lg = pd.read_parquet(p, columns=["financial_period_id", "data_item_id", "value"],
                             filters=[("data_item_id", "==", want[0])])
        parts.append(lg)
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

    # J4 — items= 로 계정을 고르면: 메타 자동 포함 · 고른 계정만 · 값은 전체 부착과 동일
    j4 = attach_financials(t, wq, side="con", items=[want[0], use[1]])
    new_cols = [c for c in j4.columns if c not in t.columns]
    exp_cols = [f"con_fin_{c}" for c in FIN_META if c not in ("companyid", "cal_year", "cal_quarter")] \
        + [rev_c, f"con_fin_{use[1]}"]
    same_vals = j4[rev_c].equals(j[rev_c]) and j4[f"con_fin_{use[1]}"].equals(j[f"con_fin_{use[1]}"])
    chk("J4", "items=[id, 열이름] 계정 선택 — 메타 자동 포함 · 고른 계정만 · 값 = 전체 부착",
        len(j4) == n0 and set(new_cols) == set(exp_cols) and same_vals,
        f"새 열 {len(new_cols)}개 (기대 {len(exp_cols)}) · 값 동일 {same_vals}")
    del j4

    # J5 — 연간: 주기 자동 판별 + 무역에 cal_quarter 가 없어도 붙는다 + freq 명시 검증
    ja = attach_financials(t, wa, side="shp")
    ja = attach_financials(ja, wa, side="con", freq="annual")
    ba = ja[rev_s].notna() & ja[rev_c].notna()
    tq = t.drop(columns=["cal_quarter"])
    jq = attach_financials(tq, wa, side="con")
    chk("J5", "연간 wide — 주기 자동 판별 · freq='annual' 명시 · cal_quarter 없는 무역에도 결합",
        len(ja) == n0 and len(jq) == n0 and "cal_quarter" not in jq.columns,
        f"행 {n0:,} -> {len(ja):,} / {len(jq):,} · 양측커버 {ba.mean()*100:.1f}%행/"
        f"{ja.loc[ba,'value_usd'].sum()/V*100:.1f}%금액")
    del ja, jq, tq

    # J6 — to_usd 방향 검증: 비-USD 대기업 매출을 환산하면 상식 범위($1M~$10T)여야 한다
    nz = j[both & j.shp_fin_currency.ne("USD")].nlargest(50, "value_usd")
    usd = to_usd(nz[rev_s], nz["shp_fin_fx_per_usd"]) * 1e6   # 백만 -> 달러
    ok6 = bool(((usd > 1e6) & (usd < 1e13)).all()) if len(nz) else False
    chk("J6", "to_usd 환산 방향 (비-USD 상위 50 무역기업 매출이 $1M~$10T 범위)",
        ok6, f"{len(nz)}건 · 범위 ${usd.min()/1e9:.1f}B ~ ${usd.max()/1e9:.1f}B" if len(nz) else "표본 없음")

    # J7 — 잘못된 입력에 명확한 예외를 내는가 (조용히 틀린 결합을 막는 것이 목적)
    head = t.head(1000)
    cases = {
        "빈 프레임": lambda: attach_financials(head, wq.iloc[0:0], side="con"),
        "주기 혼합": lambda: attach_financials(head, pd.concat([wq.head(3), wa.head(3)]), side="con"),
        "freq 불일치": lambda: attach_financials(head, wq.head(10), side="con", freq="annual"),
        "키 중복": lambda: attach_financials(head, pd.concat([wq.head(5), wq.head(5)]), side="con"),
        "없는 계정": lambda: attach_financials(head, wq.head(5), side="con", items=[999999999]),
        "period_type_id 없음": lambda: attach_financials(
            head, wq.head(5).drop(columns=["period_type_id"]), side="con"),
    }
    res7 = {k: raises(fn, (ValueError, KeyError)) for k, fn in cases.items()}
    ok7 = all(v[0] for v in res7.values())
    # freq 명시로 period_type_id 없는 wide 도 붙는다
    j7 = attach_financials(head, wq.drop(columns=["period_type_id"]), side="con", freq="quarter")
    ok7 = ok7 and len(j7) == len(head)
    chk("J7", "잘못된 입력에 예외 (빈 프레임·주기 혼합·freq 불일치·키 중복·없는 계정·주기 열 없음) · freq 명시로 우회 가능",
        ok7, " · ".join(f"{k}:{'예외' if v[0] else 'X'}" for k, v in res7.items()))

    # J8 — load_fin_long_items(years=) 가 wide 와 같은 값을 준다 (무역 최근 연도 하나)
    lyears = file_years(sorted(fdir.glob("ciq_fin_long_*.parquet")))
    ty = [y for y in file_years(trade_files) if y in lyears]
    detail8, ok8 = "long 파일과 겹치는 무역 연도 없음", False
    if ty:
        y8 = ty[-1]
        w8 = load_fin_long_items([want[0]], out_dir=fdir, period_type_id=2, years=[y8])
        c8 = w8.merge(wq[["companyid", "cal_year", "cal_quarter", use[0]]],
                      on=["companyid", "cal_year", "cal_quarter"], how="inner")
        bad8 = int((~np.isclose(c8[want[0]], c8[use[0]], rtol=0, atol=1e-9, equal_nan=True)).sum())
        uniq8 = int(w8.duplicated(["companyid", "cal_year", "cal_quarter"]).sum())
        r8b = raises(lambda: load_fin_long_items([want[0]], out_dir=fdir, period_type_id=2,
                                                 years=[y8], preferred_only=False), ValueError, "중복")
        ok8 = bad8 == 0 and uniq8 == 0 and len(c8) > 0
        detail8 = (f"years=[{y8}] {len(w8):,}행 · wide 와 겹침 {len(c8):,}행 불일치 {bad8} · 키 중복 {uniq8} · "
                   f"preferred_only=False -> {'중복 예외' if r8b[0] else '중복 없음(반환)'}")
        del w8, c8
    chk("J8", "load_fin_long_items(years=) 값 = wide · 키 유일 · preferred_only=False 는 중복 시 예외", ok8, detail8)

    ex = (j[both & (j.is_intra_group == 1)]
          .nlargest(6, "value_usd")[["trade_quarter", "shp_name", "shp_up", "con_name",
                                     "con_up", "hs6", "value_usd", rev_s, rev_c,
                                     "shp_fin_currency", "con_fin_currency"]])

    md = [f"# v4 결합 테스트 ({tdir.name}) — 따로 둔 두 쪽이 실제로 붙는가", "",
          f"**검증일**: {pd.Timestamp.now():%Y-%m-%d} · **스크립트**: "
          "`scripts\\processing\\v4_95_join_test.py`", "",
          f"무역 `{tdir}` ({n0:,}행) · 재무 `{fdir}`", "",
          "**붙이는 코드의 정본은 `scripts\\processing\\v4_join.py` 다.** 이 테스트는 그 모듈의",
          "`load_trade` / `load_fin` / `attach_financials` / `to_usd` / `load_fin_long_items` 를 그대로 호출해 검증한다 —",
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
           "from v4_join import load_trade, load_fin, attach_financials, to_usd, load_fin_long_items",
           "t = load_trade(years=range(2015, 2025))            # 연도 파일 자동 결합",
           'f = load_fin("quarter")                            # 또는 "annual" — 주기는 period_type_id 로 판별',
           't = attach_financials(t, f, side="shp")            # 전 계정 -> shp_fin_*',
           't = attach_financials(t, f, side="con", items=[28, 1007])   # 매출·총자산만 (+ 기간 메타 자동)',
           't["shp_rev_usd"] = to_usd(t["shp_fin_total_revenues_28"], t["shp_fin_fx_per_usd"])',
           "```", "",
           "- `attach_financials(trade, fin_wide, side, key='up', freq=None, items=None, how='left')` — "
           "주기 혼합·빈 프레임·키 중복·없는 계정은 예외, 행 증식(len>원래)도 예외, 결과가 8GB 이상으로 추정되면 경고.",
           "- 연간 결합은 무역에 `cal_quarter` 가 없어도 된다 (`freq='annual'` 명시 가능).",
           "- 카탈로그 밖 계정: `load_fin_long_items([id...], period_type_id=2, years=range(2019, 2026))` — "
           "`years` 없이 부르면 long 22파일(10.5억 행) 전수 스캔 경고가 뜬다.",
           "- 법인 자신 기준: `attach_financials(..., key=\"ciqid\")` (커버리지는 크게 떨어짐 — J2b)"]
    md_path = out / "95_join_test.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")

    fails = sum(1 for r in R if "FAIL" in r["result"])
    write_manifest(out, "join_test",
                   inputs=trade_files + [fdir / "ciq_fin_wide_quarter.parquet",
                                         fdir / "ciq_fin_wide_annual.parquet",
                                         fdir / "ciq_fin_period.parquet"],
                   outputs=[md_path],
                   extra={"checks": len(R), "fails": fails, "trade_dir": str(tdir), "fin_dir": str(fdir)})
    print(f"\n-> {md_path}   FAIL {fails}개")
    print(ex.to_string(index=False))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
