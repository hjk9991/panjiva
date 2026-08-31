# -*- coding: utf-8 -*-
r"""
an_20260826_v4_joined_2020_2022.py — v4 결합 테스트 산출물.

무역(2020~2022) x 재무를 con·shp 양쪽 모회사(UP) 키로 붙인 것 2개:
  joined_quarter.parquet  분기 재무 기준  (up, cal_year, cal_quarter)
  joined_annual.parquet   연간 재무 기준  (up, cal_year)

계정: 전 410계정을 양쪽에 붙이면 ~840열 x 9M행 (메모리 ~60GB) 이라 비현실적 —
핵심 17계정 + 기간 메타만 붙인다. 다른 계정이 필요하면 ITEM_IDS 만 바꿔 재실행.

산출: C:\panjiva\data\staging\v4_joined_2020_2022\
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, r"C:\panjiva\projects\20251201\scripts\processing")
from v4_join import load_trade, load_fin, attach_financials   # noqa: E402
from v4_common import OUT_FULL, write_manifest                # noqa: E402

OUT = Path(r"C:\panjiva\data\staging\v4_joined_2020_2022")
YEARS = range(2020, 2023)          # 2020·2021·2022 (range 끝은 미포함)

# 핵심 계정 — 손익 7 + 재무상태 8 + 현금흐름 2
ITEM_IDS = [28, 34, 10, 21, 400, 4051, 15,            # 매출·매출원가·매출총이익·영업이익·EBIT·EBITDA·순이익
            1007, 1002, 1043, 1004, 1049, 4173, 4364, 1275,   # 총자산·현금·재고·유형자산·장기부채·총부채·순부채·자기자본
            2021, 2006]                                # CapEx·감가상각

FIN_META = ["financial_period_id", "companyid", "period_type_id", "cal_year", "cal_quarter",
            "fiscal_year", "fiscal_quarter", "period_end", "currency", "fx_per_usd",
            "restatement_type_id", "latest_period_flag", "n_data_items"]


def build(freq, trade):
    cat = pd.read_csv(OUT_FULL / "ciq_dataitem_catalog.csv")
    m = cat.set_index("data_item_id")["column_name"]
    cols = [m[i] for i in ITEM_IDS if i in m.index]
    missing = [i for i in ITEM_IDS if i not in m.index]
    if missing:
        print(f"  (카탈로그에 없어 제외된 계정 id: {missing})")
    fin = load_fin(freq, columns=FIN_META + cols)

    t = attach_financials(trade, fin, side="shp")
    t = attach_financials(t, fin, side="con")

    rev_s, rev_c = f"shp_fin_{m[28]}", f"con_fin_{m[28]}"
    both = t[rev_s].notna() & t[rev_c].notna()
    V = t.value_usd.sum()
    print(f"  [{freq}] {len(t):,}행 x {t.shape[1]}열 · 재무커버(매출 기준): "
          f"shp {t[rev_s].notna().mean()*100:.1f}%행 · con {t[rev_c].notna().mean()*100:.1f}%행 · "
          f"양측 {both.mean()*100:.1f}%행/{t.loc[both,'value_usd'].sum()/V*100:.1f}%금액")

    p = OUT / f"joined_{freq}.parquet"
    t.to_parquet(p, index=False, compression="zstd")
    print(f"  -> {p.name}  {p.stat().st_size/1e6:,.0f}MB")
    return p, len(t), t.shape[1]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"무역 로드: {YEARS.start}~{YEARS.stop - 1}")
    trade = load_trade(years=YEARS)
    print(f"  {len(trade):,}행")

    pq_, nq, cq = build("quarter", trade)
    pa_, na, ca = build("annual", trade)

    (OUT / "README_읽어보세요.md").write_text(f"""# v4 결합 테스트 산출물 (2020~2022)

**생성**: 2026-08-26 · `projects\\20251201\\scripts\\analysis\\an_20260826_v4_joined_2020_2022.py`
**원료**: `v4_pairhs_full` (무역 {YEARS.start}~{YEARS.stop - 1} + 재무 wide) — 원료는 변경 없음

| 파일 | 내용 | 규모 |
|---|---|---|
| `joined_quarter.parquet` | 무역 + **분기** 재무 양쪽 (`up` x `cal_year` x `cal_quarter`) | {nq:,}행 x {cq}열 |
| `joined_annual.parquet` | 무역 + **연간** 재무 양쪽 (`up` x `cal_year`) | {na:,}행 x {ca}열 |

- 재무는 **모회사(`shp_up`/`con_up`) 기준**으로 `shp_fin_*` / `con_fin_*` 접두어로 붙어 있다.
- 계정은 핵심 17개(매출·원가·이익 계열 7 + 재무상태 8 + CapEx·감가상각) + 기간 메타.
  전 410계정 양쪽 부착은 ~840열이라 뺐다 — 다른 계정이 필요하면 스크립트의 `ITEM_IDS` 수정 후 재실행.
- 값은 **백만 단위·원표시통화**. USD 는 `v4_join.to_usd(값, *_fin_fx_per_usd)` (나눗셈).
- 안 붙은 행(NaN)은 모회사 재무가 CIQ 에 없는 것 — 커버리지·편향은 `v4_pairhs_full\\README` §2.
""", encoding="utf-8")
    write_manifest(OUT, "joined_2020_2022",
                   inputs=[OUT_FULL / f"trade_pair_hs_quarter_{y}.parquet" for y in YEARS]
                          + [OUT_FULL / "ciq_fin_wide_quarter.parquet",
                             OUT_FULL / "ciq_fin_wide_annual.parquet"],
                   outputs=[pq_, pa_])
    print(f"\n완료 -> {OUT}")


if __name__ == "__main__":
    main()
