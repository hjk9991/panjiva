# -*- coding: utf-8 -*-
r"""
v4_join.py — v4 무역 팩트에 CIQ 재무를 붙이는 **정본 코드**.

README 의 스니펫을 베끼지 말고 이 모듈을 import 해서 쓴다. `v4_95_join_test.py` 가
이 함수들을 그대로 검증하므로, 여기 있는 코드 = 테스트된 코드다.

사용:
    import sys; sys.path.insert(0, r"C:\panjiva\projects\20251201\scripts\processing")
    from v4_join import load_trade, load_fin, attach_financials, to_usd

    t = load_trade()                                   # 연도 파일들 자동 결합
    f = load_fin("quarter")                            # 또는 "annual"
    t = attach_financials(t, f, side="shp")            # 수출자 모회사 재무
    t = attach_financials(t, f, side="con")            # 수입자 모회사 재무
    t["shp_rev_usd"] = to_usd(t["shp_fin_total_revenues_28"], t["shp_fin_fx_per_usd"])
"""

from pathlib import Path

import pandas as pd

from v4_common import OUT_FULL


def load_trade(out_dir=OUT_FULL, columns=None, years=None):
    """연도별 trade_pair_hs_quarter_YYYY.parquet 를 이어붙여 돌려준다."""
    paths = sorted(Path(out_dir).glob("trade_pair_hs_quarter_*.parquet"))
    if years:
        keep = {str(y) for y in years}
        paths = [p for p in paths if p.stem.split("_")[-1] in keep]
    if not paths:
        raise FileNotFoundError(f"trade_pair_hs_quarter_*.parquet 없음: {out_dir}")
    return pd.concat([pd.read_parquet(p, columns=columns) for p in paths],
                     ignore_index=True)


def load_fin(freq="quarter", out_dir=OUT_FULL, columns=None):
    """ciq_fin_wide_{quarter|annual}.parquet 를 읽는다."""
    assert freq in ("quarter", "annual")
    return pd.read_parquet(Path(out_dir) / f"ciq_fin_wide_{freq}.parquet", columns=columns)


def attach_financials(trade, fin_wide, side, key="up", how="left"):
    """무역 팩트 한쪽 당사자에 재무를 equi-join 한다. 행 수는 절대 늘지 않는다(검증됨).

    side : "shp"(수출자) 또는 "con"(수입자)
    key  : "up"(최종모회사 — 기본. 커버리지 14배) 또는 "ciqid"(법인 자신)

    분기 wide 는 (companyid, cal_year, cal_quarter), 연간 wide 는 (companyid, cal_year) 로
    붙는다 — 어느 쪽인지는 fin_wide 의 period_type_id 로 자동 판별한다.
    재무 컬럼은 f"{side}_fin_" 접두어로 붙는다. join 키(cal_year·cal_quarter)는 접두어 없이
    무역 쪽 컬럼 그대로 남는다.
    """
    assert side in ("shp", "con")
    assert key in ("up", "ciqid")
    annual = int(fin_wide["period_type_id"].iloc[0]) == 1
    join_keys = [f"{side}_{key}", "cal_year"] + ([] if annual else ["cal_quarter"])

    r = fin_wide.add_prefix(f"{side}_fin_").rename(columns={
        f"{side}_fin_companyid": f"{side}_{key}",
        f"{side}_fin_cal_year": "cal_year",
        f"{side}_fin_cal_quarter": "cal_quarter"})
    if annual:                                   # 연간은 cal_quarter 를 키로 쓰지 않는다
        r = r.drop(columns=["cal_quarter"])

    n0 = len(trade)
    out = trade.merge(r, on=join_keys, how=how)
    if len(out) != n0:
        raise AssertionError(
            f"join 후 행 증식 {n0:,} -> {len(out):,} — 재무 쪽 키가 유일하지 않다. "
            "wide 빌드가 is_preferred 로 걸러졌는지 확인할 것.")
    return out


def to_usd(value, fx_per_usd):
    """원표시통화(백만) -> USD(백만). **나눗셈이다** — fx_per_usd 는 1 USD 당 현지통화
    (KRW 1,476 · JPY 157). 곱하면 원화 기업이 1,476배 부풀려진다.
    비율·주식수·EPS 계정에는 쓰지 말 것 (통화 금액 계정 전용)."""
    return value / fx_per_usd


def load_fin_long_items(item_ids, out_dir=OUT_FULL, period_type_id=2, preferred_only=True):
    """카탈로그 밖 계정을 long 에서 wide 로 뽑는다.

    반환: (companyid, cal_year, cal_quarter) x item_id 피벗. attach_financials 대신
    직접 merge 하려면 period_type_id·is_preferred 유의 (README §1).
    """
    out_dir = Path(out_dir)
    per = pd.read_parquet(out_dir / "ciq_fin_period.parquet",
                          columns=["financial_period_id", "companyid", "period_type_id",
                                   "cal_year", "cal_quarter", "is_preferred",
                                   "is_preferred_year"])
    flag = "is_preferred_year" if period_type_id == 1 else "is_preferred"
    per = per[per.period_type_id == period_type_id]
    if preferred_only:
        per = per[per[flag] == 1]
    parts = []
    for p in sorted(out_dir.glob("ciq_fin_long_*.parquet")):
        lg = pd.read_parquet(p, columns=["financial_period_id", "data_item_id", "value"])
        parts.append(lg[lg.data_item_id.isin(set(item_ids))])
    lg = pd.concat(parts, ignore_index=True).drop_duplicates(
        ["financial_period_id", "data_item_id"], keep="first")
    w = lg.merge(per, on="financial_period_id").pivot(
        index=["companyid", "cal_year", "cal_quarter"],
        columns="data_item_id", values="value")
    return w.reset_index()
