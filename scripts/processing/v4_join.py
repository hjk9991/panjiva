# -*- coding: utf-8 -*-
r"""
v4_join.py — v4 무역 팩트에 CIQ 재무를 붙이는 **정본 코드**.

README 의 스니펫을 베끼지 말고 이 모듈을 import 해서 쓴다. `v4_95_join_test.py` 가
이 함수들을 그대로 검증하므로, 여기 있는 코드 = 테스트된 코드다.

사용:
    import sys; sys.path.insert(0, r"C:\panjiva\projects\20251201\scripts\processing")
    from v4_join import load_trade, load_fin, attach_financials, to_usd, load_fin_long_items

    t = load_trade(years=range(2015, 2025))                       # 연도 파일들 자동 결합
    f = load_fin("quarter")                                        # 또는 "annual"
    t = attach_financials(t, f, side="shp")                        # 수출자 모회사 재무 -> shp_fin_* (전 계정)
    t = attach_financials(t, f, side="con", items=[28, 1007])      # 수입자: 매출·총자산만 (+ 기간 메타 자동)
    t["shp_rev_usd"] = to_usd(t["shp_fin_total_revenues_28"], t["shp_fin_fx_per_usd"])

    # 연간 재무 — freq 는 fin_wide 의 period_type_id 로 자동 판별. 명시하면 검증한다.
    ta = attach_financials(t, load_fin("annual"), side="con", freq="annual")
    # 법인 자신 기준 (모회사 대신) — 커버리지는 크게 떨어진다
    tc = attach_financials(t, f, side="con", key="ciqid")
    # 카탈로그 밖 계정: long 에서 직접 (years 로 읽을 파일을 줄일 것 — 전수는 10.5억 행 스캔)
    w = load_fin_long_items([2006, 2021], period_type_id=2, years=range(2019, 2026))

`attach_financials` 계약:
  - 주기: fin_wide.period_type_id 가 단일 값(1 연간 / 2 분기)이어야 한다. 비어 있거나 혼합이면 예외.
    period_type_id 열이 없으면 freq= 를 명시해야 한다.
  - 조인 키: 분기 (companyid, cal_year, cal_quarter) / 연간 (companyid, cal_year). 연간은 무역 쪽에
    cal_quarter 가 없어도 된다.
  - 조인 전 fin_wide 가 키 기준 유일한지 검사한다 (중복이면 예외 — is_preferred 로 걸렀는지 확인).
  - 행 증식(len(out) > len(trade)) 이면 예외. inner/right 로 줄어드는 것은 허용.
  - 결과 열: 재무 열은 f"{side}_fin_" 접두어, 조인 키(cal_year·cal_quarter·{side}_{key})는 무역 쪽 그대로.
  - items= 로 계정을 고르면 기간 메타(FIN_META 중 존재하는 것)는 자동 포함된다.
"""

import numbers
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from v4_common import OUT_FULL

# wide 의 기간 메타 열 (v4_ciq_fin_build.build_wide 의 keep 과 같다) — items= 선택 시 자동 포함
FIN_META = ["financial_period_id", "companyid", "period_type_id",
            "cal_year", "cal_quarter", "fiscal_year", "fiscal_quarter", "period_end",
            "currency", "fx_per_usd", "restatement_type_id", "restatement_type",
            "latest_period_flag", "n_data_items"]

PERIOD_FREQ = {1: "annual", 2: "quarter"}      # wide 는 이 둘만 (반기 10 은 long 에서)
MEM_WARN_GB = 8.0


def load_trade(out_dir=OUT_FULL, columns=None, years=None):
    """연도별 trade_pair_hs_quarter_YYYY.parquet 를 이어붙여 돌려준다."""
    paths = sorted(Path(out_dir).glob("trade_pair_hs_quarter_*.parquet"))
    if years:
        keep = {str(y) for y in years}
        paths = [p for p in paths if p.stem.split("_")[-1] in keep]
    if not paths:
        raise FileNotFoundError(f"trade_pair_hs_quarter_*.parquet 없음: {out_dir} (years={years})")
    return pd.concat([pd.read_parquet(p, columns=columns) for p in paths],
                     ignore_index=True)


def load_fin(freq="quarter", out_dir=OUT_FULL, columns=None):
    """ciq_fin_wide_{quarter|annual}.parquet 를 읽는다."""
    assert freq in ("quarter", "annual")
    return pd.read_parquet(Path(out_dir) / f"ciq_fin_wide_{freq}.parquet", columns=columns)


def detect_freq(fin_wide, freq=None):
    """fin_wide.period_type_id 로 주기를 판별·검증한다 -> "annual" | "quarter"."""
    if len(fin_wide) == 0:
        raise ValueError("fin_wide 가 비어 있다 — 붙일 재무가 없다 (load_fin 결과·필터를 확인)")
    if "period_type_id" not in fin_wide.columns:
        if freq is None:
            raise ValueError("fin_wide 에 period_type_id 열이 없어 주기를 판별할 수 없다 — "
                             "freq='quarter'|'annual' 을 명시하거나 열을 포함해 읽을 것")
        detected = None
    else:
        pts = sorted(int(x) for x in fin_wide["period_type_id"].dropna().unique())
        if len(pts) != 1:
            raise ValueError(f"fin_wide 의 period_type_id 가 단일이 아니다 {pts} — "
                             "연간(1)·분기(2) 중 하나로 걸러서 줄 것")
        detected = PERIOD_FREQ.get(pts[0])
        if detected is None:
            raise ValueError(f"period_type_id={pts[0]} 는 wide 로 붙일 수 없다 (연간 1·분기 2 만). "
                             "반기(10) 는 load_fin_long_items(ids, period_type_id=10) 로")
    if freq is None:
        return detected
    if freq not in PERIOD_FREQ.values():
        raise ValueError(f"freq={freq!r} — 'quarter' 또는 'annual'")
    if detected is not None and freq != detected:
        raise ValueError(f"freq={freq!r} 로 지정했지만 fin_wide 의 period_type_id 는 {detected} 다")
    return freq


def resolve_items(fin_wide, items):
    """items(data_item_id 정수 또는 열 이름) -> wide 열 이름 목록. 없는 것은 예외."""
    cols = list(fin_wide.columns)
    by_id = {}
    for c in cols:
        tail = c.rsplit("_", 1)[-1]
        if tail.isdigit():
            by_id.setdefault(int(tail), c)
    out, missing = [], []
    for it in items:
        if isinstance(it, str) and it in cols:
            out.append(it)
        elif isinstance(it, numbers.Integral) or (isinstance(it, str) and it.isdigit()):
            c = by_id.get(int(it))
            (out if c else missing).append(c if c else it)
        else:
            missing.append(it)
    if missing:
        raise KeyError(f"fin_wide 에 없는 계정: {missing} — ciq_dataitem_catalog.csv 의 data_item_id / "
                       "column_name 으로 지정할 것 (카탈로그 밖 계정은 load_fin_long_items)")
    return list(dict.fromkeys(out))


def attach_financials(trade, fin_wide, side, key="up", freq=None, items=None, how="left"):
    """무역 팩트 한쪽 당사자에 재무를 equi-join 한다. 행은 절대 늘지 않는다(검증됨).

    side  : "shp"(수출자) 또는 "con"(수입자)
    key   : "up"(최종모회사 — 기본. 커버리지 6배) 또는 "ciqid"(법인 자신)
    freq  : None 이면 fin_wide.period_type_id 로 자동 판별 ("quarter"|"annual"). 명시하면 검증.
    items : None 이면 fin_wide 의 모든 열. [28, 1007, "total_revenues_28", ...] 로 계정 선택 —
            기간 메타(FIN_META) 는 자동 포함.
    how   : "left"(기본) · "inner" · "right" · "outer". 증식 판정은 len(out) > len(trade) 만.

    분기 wide 는 (companyid, cal_year, cal_quarter), 연간 wide 는 (companyid, cal_year) 로 붙는다.
    재무 열은 f"{side}_fin_" 접두어. join 키(cal_year·cal_quarter·{side}_{key})는 무역 쪽 그대로.
    """
    if side not in ("shp", "con"):
        raise ValueError(f"side={side!r} — 'shp' 또는 'con'")
    if key not in ("up", "ciqid"):
        raise ValueError(f"key={key!r} — 'up' 또는 'ciqid'")
    if how not in ("left", "inner", "right", "outer"):
        raise ValueError(f"how={how!r}")
    freq = detect_freq(fin_wide, freq)
    annual = freq == "annual"

    fin_keys = ["companyid", "cal_year"] + ([] if annual else ["cal_quarter"])
    join_keys = [f"{side}_{key}", "cal_year"] + ([] if annual else ["cal_quarter"])
    miss = [k for k in fin_keys if k not in fin_wide.columns]
    if miss:
        raise KeyError(f"fin_wide 에 조인 키 열이 없다: {miss}")
    miss = [k for k in join_keys if k not in trade.columns]
    if miss:
        raise KeyError(f"trade 에 조인 키 열이 없다: {miss}" +
                       (" (연간 결합엔 cal_quarter 가 필요 없다)" if annual else ""))

    if items is not None:
        picked = resolve_items(fin_wide, items)
        meta = [c for c in FIN_META if c in fin_wide.columns]
        fin_wide = fin_wide[list(dict.fromkeys(meta + picked))]

    dup = int(fin_wide.duplicated(fin_keys).sum())
    if dup:
        raise ValueError(f"fin_wide 가 키 {tuple(fin_keys)} 기준으로 유일하지 않다 (중복 {dup:,}행) — "
                         "wide 는 is_preferred(_year) 로 걸러져 있어야 한다. long 에서 직접 만들었다면 "
                         "ciq_fin_period 의 is_preferred 로 거를 것")

    n_cols_out = trade.shape[1] + fin_wide.shape[1] - len(fin_keys)
    est_gb = len(trade) * n_cols_out * 8 / 1e9
    if est_gb >= MEM_WARN_GB:
        print(f"[경고] attach_financials: 결과 약 {len(trade):,}행 x {n_cols_out}열 ≈ {est_gb:.1f}GB "
              f"(행x열x8B 추정, 문자열 열은 더 큼). items= 로 계정을 고르거나 trade 를 years=/columns= 로 줄일 것.")

    r = fin_wide.add_prefix(f"{side}_fin_").rename(columns={
        f"{side}_fin_companyid": f"{side}_{key}",
        f"{side}_fin_cal_year": "cal_year",
        f"{side}_fin_cal_quarter": "cal_quarter"})
    if annual and "cal_quarter" in r.columns:     # 연간은 cal_quarter 를 키로 쓰지 않는다
        r = r.drop(columns=["cal_quarter"])

    n0 = len(trade)
    out = trade.merge(r, on=join_keys, how=how)
    if len(out) > n0:
        raise AssertionError(
            f"join 후 행 증식 {n0:,} -> {len(out):,} — 재무 쪽 키가 유일하지 않다. "
            "wide 빌드가 is_preferred 로 걸러졌는지 확인할 것.")
    return out


def to_usd(value, fx_per_usd):
    """원표시통화(백만) -> USD(백만). **나눗셈이다** — fx_per_usd 는 1 USD 당 현지통화
    (KRW 1,476 · JPY 157). 곱하면 원화 기업이 1,476배 부풀려진다.
    비율·주식수·EPS 계정에는 쓰지 말 것 (통화 금액 계정 전용)."""
    return value / fx_per_usd


def load_fin_long_items(item_ids, out_dir=OUT_FULL, period_type_id=2, years=None,
                        preferred_only=True):
    """카탈로그 밖 계정을 long 에서 wide 로 뽑는다.

    item_ids       : data_item_id 목록
    period_type_id : 1 연간 · 2 분기(기본) · 10 반기
    years          : 읽을 ciq_fin_long_YYYY.parquet 의 연도(파일명 = 원천 fin_data 연도 = period_end 연도,
                     cal_year 와 1월 결산 기업에서 ±1 어긋날 수 있으니 여유를 둘 것). None 이면 전수 —
                     **전 파일(실측 22개, 10.5억 행) 스캔** 이라 수 분~십수 분 걸린다.
    preferred_only : True 면 is_preferred(_year)==1 인 기간만 (키 유일). False 면 중복 키가 생길 수 있어
                     pivot 전에 명확한 예외를 낸다 — 그 경우 financial_period_id 단위로 직접 다룰 것.

    반환: (companyid, cal_year[, cal_quarter]) x item_id 피벗 (연간은 cal_quarter 없음).
          attach_financials 대신 직접 merge 하려면 period_type_id·is_preferred 유의 (README §1).
    """
    out_dir = Path(out_dir)
    ids = sorted({int(i) for i in item_ids})
    files = sorted(out_dir.glob("ciq_fin_long_*.parquet"))
    if years is not None:
        keep = {int(y) for y in years}
        files = [p for p in files if int(p.stem.split("_")[-1]) in keep]
    if not files:
        raise FileNotFoundError(f"ciq_fin_long_*.parquet 없음: {out_dir} (years={years})")
    n_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    if years is None:
        print(f"[비용 경고] load_fin_long_items: ciq_fin_long 파일 {len(files)}개 · {n_rows:,}행 전수 스캔 "
              f"(수 분~십수 분). years= 로 읽을 파일을 줄일 수 있다.")
    else:
        print(f"load_fin_long_items: 파일 {len(files)}개 · {n_rows:,}행 스캔 (years={sorted(keep)})")

    annual = int(period_type_id) == 1
    flag = "is_preferred_year" if annual else "is_preferred"
    keys = ["companyid", "cal_year"] + ([] if annual else ["cal_quarter"])
    per = pd.read_parquet(out_dir / "ciq_fin_period.parquet",
                          columns=["financial_period_id", "companyid", "period_type_id",
                                   "cal_year", "cal_quarter", "is_preferred", "is_preferred_year"])
    per = per[per.period_type_id == period_type_id]
    if preferred_only:
        per = per[per[flag] == 1]
    per = per[["financial_period_id"] + keys]

    parts = []
    for p in files:
        lg = pd.read_parquet(p, columns=["financial_period_id", "data_item_id", "value"],
                             filters=[("data_item_id", "in", ids)])
        if len(lg):
            parts.append(lg)
    if not parts:
        raise ValueError(f"계정 {ids} 가 long 에 없다 (files={len(files)})")
    lg = (pd.concat(parts, ignore_index=True)
          .drop_duplicates(["financial_period_id", "data_item_id"], keep="first"))
    del parts
    lg = lg.merge(per, on="financial_period_id", how="inner")
    dup = int(lg.duplicated(keys + ["data_item_id"]).sum())
    if dup:
        raise ValueError(
            f"키 {tuple(keys + ['data_item_id'])} 중복 {dup:,}행 — pivot 불가. "
            + ("preferred_only=False 라서 같은 (회사, 연, 분기) 에 회계기간이 여럿이다: "
               "financial_period_id 를 인덱스로 직접 pivot 하거나 preferred_only=True 로."
               if not preferred_only else
               "ciq_fin_period 의 is_preferred 플래그가 키 기준 유일하지 않다 — 재무 빌드를 확인할 것."))
    w = lg.pivot(index=keys, columns="data_item_id", values="value")
    w.columns = [int(c) for c in w.columns]
    return w.reset_index()
