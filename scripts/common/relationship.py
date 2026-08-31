# -*- coding: utf-8 -*-
r"""
relationship.py — 관계분류·매칭상태 판정 (v1·v2·v3 공용)

원천(`data\staging\source\`)은 **사실만** 담는다. 이 모듈이 그 사실로부터 **판정**을 만든다.
판정 규칙이 바뀌어도 원천 재추출 없이 이 파일만 고치면 세 버전에 동시 반영된다.

명세: `shared memory\BECRS_Matching_Project\04_2024연간파일럿_통합명세.md` §4
결정: `data\staging\source\DECISIONS.md` §1·§4·§5

입력으로 필요한 원천 컬럼 (수입 선적 기준):
    con_ciqid / shp_ciqid          CIQ companyId (override 적용 후. 없으면 _original)
    con_up / shp_up                거래일 기준 최종 모회사 companyId

사용:
    from relationship import add_relationship
    df = add_relationship(df)      # 아래 6개 컬럼이 추가된다
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["add_relationship", "RELATIONSHIP_VALUES", "MATCH_STATUS_VALUES"]

# 명세 §4.3 — 3분류
RELATIONSHIP_VALUES = ("within_firm", "arms_length", "unmatched")

# 명세 §4.2 — 4값
MATCH_STATUS_VALUES = ("both", "consignee_only", "shipper_only", "none")


def _status(con_ok: pd.Series, shp_ok: pd.Series) -> pd.Series:
    """양측 성공 여부 → 4값 상태."""
    return pd.Series(
        np.select(
            [con_ok & shp_ok, con_ok & ~shp_ok, ~con_ok & shp_ok],
            ["both", "consignee_only", "shipper_only"],
            default="none",
        ),
        index=con_ok.index, dtype="string",
    )


def add_relationship(
    df: pd.DataFrame,
    con_id: str = "con_ciqid",
    shp_id: str = "shp_ciqid",
    con_up: str = "con_up",
    shp_up: str = "shp_up",
) -> pd.DataFrame:
    """관계분류·매칭상태·사유·self 플래그를 계산해 붙인다.

    추가되는 컬럼:
      crosswalk_match_status   Panjiva→CIQ 연결 여부 (both/consignee_only/shipper_only/none)
      ownership_match_status   연결된 기업의 ultimate parent 확인 여부 (같은 4값)
      relationship             within_firm / arms_length / unmatched
      unmatched_reason         결측 단계와 방향. 매칭 성공이면 'matched'
      self_shipment            양측 companyId 가 동일하면 1 (자기→자기)
      intra_group              within_firm=1 / arms_length=0 / unmatched=결측

    ⚠️ 용어: 명세는 `entity_match_status` 라 쓰지만 **`crosswalk_match_status`** 로 쓴다.
       그 단계가 하는 일이 정확히 crosswalk(`panjivaCompanyCrossRef`) 조회이고,
       02(이송미) 업무지시서가 crosswalk 를 핵심 용어로 쓴다. PI 변경요청 대상.
       (DECISIONS.md §4)
    """
    out = df.copy()

    # 1단계: crosswalk 로 CIQ 기업을 찾았는가
    con_e = out[con_id].notna()
    shp_e = out[shp_id].notna()
    out["crosswalk_match_status"] = _status(con_e, shp_e)

    # 2단계: 그 기업의 ultimate parent 를 확인했는가
    # 실측(H1 724만): crosswalk 성공 시 UP 은 거의 항상 채워진다(미확인 3건).
    # 스냅샷 대체가 CIQ 전 기업을 덮기 때문. 그래도 명세가 요구하므로 만든다.
    con_o = con_e & out[con_up].notna()
    shp_o = shp_e & out[shp_up].notna()
    out["ownership_match_status"] = _status(con_o, shp_o)

    # 최종 관계 (명세 §4.3)
    both_ok = con_o & shp_o
    same_family = both_ok & (out[con_up] == out[shp_up])
    # 동일 법인 거래는 within_firm (명세 §4.3). UP 이 결측이어도 법인이 같으면 같은 가족이다.
    same_entity = con_e & shp_e & (out[con_id] == out[shp_id])

    out["relationship"] = pd.Series(
        np.select(
            [same_family | same_entity, both_ok],
            ["within_firm", "arms_length"],
            default="unmatched",
        ),
        index=out.index, dtype="string",
    )

    # ⚠️ ultimate parent 결측을 arms_length 로 코딩하면 안 된다(명세 §4.3).
    #    위 np.select 는 both_ok(=양측 UP 확인) 일 때만 arms_length 를 주므로 안전하다.

    # 결측 사유 — 어느 단계에서 어느 쪽이 실패했나 (명세 §4.3 권장값)
    out["unmatched_reason"] = pd.Series(
        np.select(
            [
                out["relationship"] != "unmatched",
                ~con_e & ~shp_e,
                ~con_e,
                ~shp_e,
                ~con_o & ~shp_o,
                ~con_o,
                ~shp_o,
            ],
            [
                "matched",
                "entity_unmatched_both",
                "entity_unmatched_consignee",
                "entity_unmatched_shipper",
                "ownership_unmatched_both",
                "ownership_unmatched_consignee",
                "ownership_unmatched_shipper",
            ],
            default="matched",
        ),
        index=out.index, dtype="string",
    )

    # self — 회의 결론: within_firm 에 포함하고 분모에도 넣되, 플래그로 남겨
    # "재고 이동일 뿐"이라는 판단이 서면 손쉽게 빼고 재계산할 수 있게 한다.
    out["self_shipment"] = same_entity.astype("int8")

    # 분석용 요약 — 미매칭은 0 이 아니라 결측(시장거래로 세면 안 된다)
    out["intra_group"] = (
        out["relationship"].map({"within_firm": 1, "arms_length": 0}).astype("Int64")
    )
    return out


def summarize(df: pd.DataFrame, value_col: str = "valueofgoodsusd") -> pd.DataFrame:
    """관계분류 분포를 건수·금액으로 요약 (검증·리포트용).

    명세 §4.4: 구성비의 분모는 **분류 가능 거래**(within_firm + arms_length)다.
    전체 대비 매칭률을 같은 표에 병기한다.
    """
    n, v = len(df), df[value_col]
    g = df.groupby("relationship", dropna=False).agg(
        n=("relationship", "size"), value=(value_col, "sum"))
    classified = g.loc[g.index.isin(["within_firm", "arms_length"])]
    g["share_of_all_n"] = (g["n"] / n * 100).round(2)
    g["share_of_all_value"] = (g["value"] / v.sum() * 100).round(2)
    g["share_of_classified_n"] = (g["n"] / classified["n"].sum() * 100).round(2)
    g["share_of_classified_value"] = (
        g["value"] / classified["value"].sum() * 100).round(2)
    # 분류 불가 행은 분류 기준 비중이 의미 없다
    g.loc[~g.index.isin(["within_firm", "arms_length"]),
          ["share_of_classified_n", "share_of_classified_value"]] = np.nan
    return g.reset_index()
