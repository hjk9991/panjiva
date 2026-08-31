# -*- coding: utf-8 -*-
r"""
scan_hs_value_anomaly.py — HS 오분류가 금액을 부풀린 사례 전수 탐색

## 왜 이 스캔이 성립하는가

`valueOfGoodsUSD` 는 관측값이 아니라 **Panjiva 추정치**다(팀 함정 8). 실측으로 확인한
추정 방식은 **`HS별 단가 × 중량`** 이다 — Mosaic 건에서 HS 9503 으로 잘못 분류된 인산염의
kg 당 단가($7.24)가 HS 9503 전체 중위값($7.19)과 **1.01배**로 일치했다.

따라서 **HS 가 틀리면 금액도 반드시 함께 틀어진다.** 두 가지 방법으로 찾는다.

  방법 A  **벌크 + 고단가** — 1만 톤 넘는 벌크 화물인데 kg 당 단가가 비정상적으로 높다.
          벌크로 오는 원자재는 kg 당 $0.05~1 수준이다.

  방법 B  **같은 쌍이 여러 HS 로 갈림** — 같은 (수입자, 수출자) 가 같은 규모의 화물을
          보내는데 HS 가 여러 개이고 단가가 10배 이상 벌어진다. Mosaic 패턴이다.

산출: `projects\20251201\output\SCAN_hs_value_anomaly.md`
      `projects\20251201\output\tables\hs_value_anomaly_candidates.csv`
"""

import glob
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
OUT = Path(r"C:\panjiva\projects\20251201\output\SCAN_hs_value_anomaly.md")
CSV = Path(r"C:\panjiva\projects\20251201\output\tables\hs_value_anomaly_candidates.csv")

BULK_KG = 10_000_000        # 1만 톤
HI_UNIT = 5.0               # kg 당 $5 — 벌크 원자재로는 비현실적
RATIO = 10.0                # 같은 쌍 안에서 단가가 10배 이상 벌어지면 의심

HS2_NAME = {
    "25": "소금·황·토석", "26": "광석·슬래그", "27": "광물성연료", "28": "무기화학",
    "29": "유기화학", "31": "비료", "38": "각종 화학공업 생산품", "39": "플라스틱",
    "44": "목재", "47": "펄프", "48": "지류", "61": "편물의류", "62": "직물의류",
    "64": "신발", "68": "석·시멘트 제품", "72": "철강", "73": "철강제품",
    "74": "구리", "76": "알루미늄", "84": "기계", "85": "전기기기", "87": "자동차",
    "90": "광학·의료기기", "94": "가구", "95": "완구·게임", "10": "곡물",
    "12": "채유용 종자", "23": "사료", "30": "의료용품", "40": "고무",
}
L = []


def say(s=""):
    print(s, flush=True)
    L.append(s)


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


def hs2n(h):
    return HS2_NAME.get(str(h)[:2], "")


# ---------------------------------------------------------------------------
say("# HS 오분류로 금액이 부풀려진 사례 — 전수 탐색\n")
say(f"**탐색일** {date.today()} · **스크립트** `scan_hs_value_anomaly.py` · "
    "**대상** 2024년 미국 수입 전수\n")
say("`valueOfGoodsUSD` 는 **`HS별 단가 × 중량`으로 추정된 값**이므로 HS 가 틀리면 금액도 "
    "함께 틀어진다. 실측 근거: Mosaic 건의 kg 당 단가($7.24)가 HS 9503 전체 중위값($7.19)과 "
    "**1.01배** 일치.\n")

print("입력 로드...", flush=True)
c = ["panjivarecordid", "arrivaldate", "conname", "shpname", "con_ciqid", "shp_ciqid",
     "hs6", "hs2", "n_hs6", "valueofgoodsusd", "weightkg", "shpmtorigin", "relationship"]
d = pd.concat([pd.read_parquet(f, columns=c)
               for f in sorted(glob.glob(str(V1 / "shipment_master_2024*.parquet")))],
              ignore_index=True)
n_all, v_all = len(d), float(d.valueofgoodsusd.fillna(0).sum())
d = d[d.valueofgoodsusd.notna() & d.weightkg.notna() & (d.weightkg > 0)].copy()
d["unit"] = d.valueofgoodsusd / d.weightkg
say(f"**전수 {n_all:,}건 · ${v_all/1e9:,.1f}B** 중 금액·중량이 다 있는 "
    f"{len(d):,}건(${d.valueofgoodsusd.sum()/1e9:,.1f}B)을 본다.\n")

# --- HS6 별 단가 중위 (추정 단가의 근사) ---
hs_unit = d.groupby("hs6")["unit"].median().rename("hs_median_unit")

# ---------------------------------------------------------------- 방법 A
say("\n## 방법 A — 벌크 화물인데 단가가 비현실적으로 높다\n")
say(f"기준: 중량 **{BULK_KG/1e6:,.0f}천 톤 초과** 이면서 kg 당 단가 **${HI_UNIT:,.0f} 초과**.\n")
say("벌크선으로 오는 원자재(광석·곡물·비료·연료)는 kg 당 $0.05~1 수준이다. "
    "그보다 훨씬 비싼 품목이 벌크로 오는 일은 드물다.\n")

bulk = d[d.weightkg > BULK_KG]
say(f"- 1만 톤 초과 선적: **{len(bulk):,}건 · ${bulk.valueofgoodsusd.sum()/1e9:,.1f}B**")
say(f"- 그 중위 단가: **${bulk.unit.median():.3f}/kg**\n")

A = bulk[bulk.unit > HI_UNIT].copy()
say(f"**의심 {len(A):,}건 · ${A.valueofgoodsusd.sum()/1e9:,.2f}B**\n")
if len(A):
    g = (A.groupby(["hs6", "conname", "shpname", "shpmtorigin"], dropna=False)
         .agg(건수=("valueofgoodsusd", "size"), 금액=("valueofgoodsusd", "sum"),
              중량=("weightkg", "sum"), 단가=("unit", "median")).reset_index()
         .nlargest(20, "금액"))
    g["품목"] = g.hs6.map(hs2n)
    g["중량(천톤)"] = g.중량 / 1e6
    g["금액($B)"] = g.금액 / 1e9
    say(md(g[["hs6", "품목", "conname", "shpname", "shpmtorigin",
              "건수", "중량(천톤)", "금액($B)", "단가"]].rename(
        columns={"conname": "수입자", "shpname": "수출자",
                 "shpmtorigin": "원산지", "단가": "단가($/kg)"}), "{:,.2f}"))

# ---------------------------------------------------------------- 방법 B
say("\n## 방법 B — 같은 (수입자, 수출자) 쌍이 여러 HS 로 갈리고 단가가 크게 벌어진다\n")
say(f"기준: 같은 쌍에서 **HS 가 2개 이상**이고 **최고/최저 중위단가 비 {RATIO:,.0f}배 이상**, "
    f"양쪽 다 벌크({BULK_KG/1e6:,.0f}천 톤 초과 선적 포함).\n")
say("같은 거래관계에서 같은 규모의 화물이 오는데 HS 만 다르고 단가가 수십 배 차이나면, "
    "**어느 한쪽이 잘못 분류된 것**이다.\n")

bp = (bulk.groupby(["conname", "shpname", "hs6"], dropna=False)
      .agg(n=("valueofgoodsusd", "size"), v=("valueofgoodsusd", "sum"),
           kg=("weightkg", "sum"), u=("unit", "median"),
           kg_med=("weightkg", "median")).reset_index())
pair = bp.groupby(["conname", "shpname"], dropna=False).agg(
    n_hs=("hs6", "nunique"), u_max=("u", "max"), u_min=("u", "min"),
    v_tot=("v", "sum")).reset_index()
pair = pair[(pair.n_hs >= 2) & (pair.u_min > 0)]
pair["배수"] = pair.u_max / pair.u_min
B = pair[pair.배수 >= RATIO].nlargest(15, "v_tot")
say(f"**의심 쌍 {len(pair[pair.배수 >= RATIO]):,}개**\n")
if len(B):
    B2 = B.copy()
    B2["총액($B)"] = B2.v_tot / 1e9
    say(md(B2[["conname", "shpname", "n_hs", "u_min", "u_max", "배수", "총액($B)"]]
          .rename(columns={"conname": "수입자", "shpname": "수출자", "n_hs": "HS 수",
                           "u_min": "최저단가", "u_max": "최고단가"}), "{:,.3f}"))

    say("\n### 상위 5개 쌍의 HS 내역\n")
    for _, r in B.head(5).iterrows():
        sub = bp[(bp.conname == r.conname) & (bp.shpname == r.shpname)] \
            .sort_values("v", ascending=False)
        say(f"\n**{str(r.conname)[:34]}  ←  {str(r.shpname)[:34]}**\n")
        t = sub.copy()
        t["품목"] = t.hs6.map(hs2n)
        t["금액($B)"] = t.v / 1e9
        t["중량(천톤)"] = t.kg / 1e6
        t["중위중량(천톤)"] = t.kg_med / 1e6
        say(md(t[["hs6", "품목", "n", "금액($B)", "중량(천톤)", "중위중량(천톤)", "u"]]
              .rename(columns={"n": "건수", "u": "단가($/kg)"}), "{:,.3f}"))

# ---------------------------------------------------------------- 영향
say("\n---\n\n## 금액 영향 추정\n")
say("의심 선적의 금액을 **같은 쌍의 최저 단가**(= 실제 화물로 추정되는 쪽)로 다시 계산하면:\n")
rows = []
for _, r in pair[pair.배수 >= RATIO].iterrows():
    sub = bp[(bp.conname == r.conname) & (bp.shpname == r.shpname)]
    lo = sub.u.min()
    hi_rows = sub[sub.u > lo * RATIO]
    if not len(hi_rows):
        continue
    cur = float(hi_rows.v.sum())
    fix = float((hi_rows.kg * lo).sum())
    rows.append({"수입자": r.conname, "수출자": r.shpname,
                 "현재 금액($B)": cur / 1e9, "재계산($B)": fix / 1e9,
                 "과대 추정($B)": (cur - fix) / 1e9,
                 "배수": cur / fix if fix > 0 else np.nan})
imp = pd.DataFrame(rows).nlargest(12, "과대 추정($B)") if rows else pd.DataFrame()
if len(imp):
    say(md(imp, "{:,.2f}"))
    tot = pd.DataFrame(rows)["과대 추정($B)"].sum()
    say(f"\n- **의심 쌍 전체의 과대 추정 합계: 약 ${tot:,.1f}B**")
    say(f"- 2024년 수입 총액 ${v_all/1e9:,.1f}B 의 **{tot*1e9/v_all*100:.2f}%**")
    wf = d[d.relationship == "within_firm"].valueofgoodsusd.sum()
    say(f"- within_firm 총액 ${wf/1e9:,.1f}B 대비: "
        f"**{tot*1e9/wf*100:.2f}%** (그룹내 거래에 집중돼 있다면 영향이 더 크다)")

say("\n> ⚠️ **이 표는 '의심 후보'이지 확정 오류가 아니다.** 같은 쌍이 실제로 다른 품목을 "
    "함께 거래할 수도 있다. **원본 `panjivaUSImpHSCode` 와 화물 정황(중량·수량·단위·선박)을 "
    "함께 보고 판정해야 한다.**")
say("\n> 확정된 사례는 **Mosaic Fertilizer ← Compania Minera Miski Mayo** 한 건이다. "
    "같은 광산이 같은 규모(5~8만 톤)의 화물을 보내는데 HS 950300(완구, $7.878/kg)과 "
    "HS 251020(인산칼슘, $0.097/kg)으로 갈려 있고, S&P 원본이 `Classified: 9503.00` 임을 "
    "Snowflake 에서 직접 확인했다.")

# ---------------------------------------------------------------- CSV
if len(A):
    CSV.parent.mkdir(parents=True, exist_ok=True)
    out = A[["panjivarecordid", "arrivaldate", "conname", "shpname", "con_ciqid",
             "shp_ciqid", "hs6", "hs2", "n_hs6", "shpmtorigin", "weightkg",
             "valueofgoodsusd", "unit", "relationship"]].sort_values(
        "valueofgoodsusd", ascending=False)
    out.to_csv(CSV, index=False, encoding="utf-8-sig")
    say(f"\n---\n\n의심 선적 전수 **{len(out):,}건**을 `{CSV.name}` 로 저장했다 "
        "(02 검토·S&P 문의용).")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(L), encoding="utf-8")
print(f"\n→ {OUT}", flush=True)
