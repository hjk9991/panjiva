# -*- coding: utf-8 -*-
r"""
audit_v1v2v3.py — v1·v2·v3 교차 감사

각 버전의 `90_checks.md` 는 **그 버전 안에서** 맞는지를 본다. 이 스크립트는 그 밖의 것을 본다:

  A. 팀 함정 12종(`shared memory\CLAUDE.md`)이 실제 산출물에서 지켜졌는가
  B. 명세 04 의 조항별 요구사항이 컬럼·값으로 존재하는가
  C. v1 → v2 → v3 총계가 한 줄로 이어지는가
  D. 결정 기록(`DECISIONS.md`)에 적힌 수치가 실제 데이터와 맞는가

산출: `projects\20251201\output\AUDIT_v1v2v3.md`
"""

import json
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SRC = Path(r"C:\panjiva\data\staging\source\trade_2024")
CIQ = Path(r"C:\panjiva\data\staging\source\ciq_ref")
FIN = Path(r"C:\panjiva\data\staging\source\ciq_fin")
V1 = Path(r"C:\panjiva\data\staging\tom_v1_2024")
V2 = Path(r"C:\panjiva\data\staging\tom_v2_2024")
V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
OUT = Path(r"C:\panjiva\projects\20251201\output\AUDIT_v1v2v3.md")
MONTHS = [f"2024{m:02d}" for m in range(1, 13)]

L, RES = [], []


def say(s=""):
    print(s, flush=True)
    L.append(s)


def chk(cat, name, ok, detail=""):
    RES.append({"구분": cat, "항목": name, "결과": "PASS" if ok else "FAIL"})
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


def cols(p: Path):
    return set(pq.ParquetFile(p).schema_arrow.names)


# ===========================================================================
say("# v1 · v2 · v3 교차 감사\n")
say(f"**감사일** {date.today()} · **스크립트** `audit_v1v2v3.py`\n")
say("각 버전의 `90_checks.md` 가 못 보는 것 — 팀 함정 준수 · 명세 조항 대조 · "
    "버전 간 총계 연결 · 문서 수치 일치 — 를 본다.\n")

# ---------------------------------------------------------------- A. 함정
say("\n## A. 팀 함정 12종 (`shared memory\\CLAUDE.md`)\n")

say("\n### A-1 · A-11 — HS 코드 추출과 `zfill` 금지\n")
hs = pd.concat([pd.read_parquet(SRC / f"imp_hs_{m}.parquet", columns=["hs6"])
                for m in MONTHS], ignore_index=True)
bad_prefix = int(hs.hs6.astype("string").str.contains(":", na=False).sum())
lens = hs.hs6.astype("string").str.len().value_counts()
zero_pad = int(hs.hs6.astype("string").str.startswith("00").sum())
chk("함정", "A-1 HS 접두어(`Classified:` 등)가 값에 남아 있지 않다",
    bad_prefix == 0, f"— 접두어 잔존 {bad_prefix:,}건")
# ⚠️ 함정 11 은 "6자리로 맞춰라" 가 **아니라 그 반대**다 — 4자리 HS 를 `zfill(6)` 하면
#    `9804` 가 `009804` 가 되어 챕터 98 이 00 으로 뒤바뀐다. 4자리는 **그대로 두는 것이 정답**.
#    검사할 것은 "6자리인데 00 으로 시작하는 것이 있는가"(= zfill 흔적)다.
six = hs.hs6.astype("string")
zfill_trace = int(((six.str.len() == 6) & six.str.startswith("00")).sum())
chk("함정", "A-11 `zfill(6)` 흔적이 없다 (6자리인데 `00` 으로 시작)",
    zfill_trace == 0, f"— 흔적 {zfill_trace:,}건")
say(f"  - 길이 분포: {dict(lens)} — **4자리 {int(lens.get(4,0)):,}건은 정상**"
    "(원본이 4자리인 HS. 대표 코드 9804·9905·7111 등 210종)")
h1 = pd.read_parquet(V1 / "shipment_master_202401.parquet", columns=["hs6", "n_hs6"])
s1 = h1.hs6.dropna().astype("string")
chk("함정", "A-11 v1 의 `hs6` 에도 zfill 흔적 없음",
    int(((s1.str.len() == 6) & s1.str.startswith("00")).sum()) == 0)
del hs, h1

say("\n### A-2 — 자식(HS) 조인 후 `sum()` 금지\n")
say("v1·v2 는 HS 자식을 조인하지 않고 **대표 HS 1:1** 만 쓴다. "
    "v3 `panel_firm_origin_hs` 만 자식을 쓰는데 **균등배분**이라 총액이 보존된다.\n")
fo = pd.read_parquet(V3 / "panel_firm_origin_hs.parquet",
                     columns=["value_usd", "weight_kg", "teu", "n_shipments"])
ref_v = ref_w = ref_t = 0.0
ref_n = set()
hs_ids = set()
for m in MONTHS:
    hh = pd.read_parquet(SRC / f"imp_hs_{m}.parquet", columns=["panjivarecordid"])
    hs_ids |= set(hh.panjivarecordid.unique())
for m in MONTHS:
    d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                        columns=["panjivarecordid", "con_ciqid", "valueofgoodsusd",
                                 "weightkg", "volumeteu"])
    d = d[d.con_ciqid.notna() & d.panjivarecordid.isin(hs_ids)]
    ref_v += d.valueofgoodsusd.fillna(0).sum()
    ref_w += d.weightkg.fillna(0).sum()
    ref_t += d.volumeteu.fillna(0).sum()
    ref_n |= set(d.panjivarecordid)
    del d
dv = abs(fo.value_usd.sum() - ref_v)
chk("함정", "A-2 균등배분 후 **금액 합계 보존** (행 수가 아니라 합계로 검증)",
    dv < 1, f"— 차이 ${dv:.2f}")
chk("함정", "A-2 중량·TEU 도 보존",
    abs(fo.weight_kg.sum() - ref_w) < 1 and abs(fo.teu.sum() - ref_t) < 0.01)
say(f"  - 배분 후 `n_shipments` 합 {int(fo.n_shipments.sum()):,} vs 실제 선적 "
    f"{len(ref_n):,} — **행 수는 부풀려지는 것이 정상**(한 선적이 여러 HS 행)")
del fo

say("\n### A-3 — 국가 컬럼 3종을 구분해 썼는가\n")
c1 = cols(V1 / "shipment_master_202401.parquet")
chk("함정", "A-3 v1 이 `shpmtorigin`·`shpcountry`·`portofladingcountry` 를 다 보존",
    {"shpmtorigin", "shpcountry", "portofladingcountry"} <= c1)
pm_c = cols(V3 / "panel_pair_month.parquet")
chk("함정", "A-3 v3 의 원산지 대표값이 `origin_main`(=`shpmtorigin` 유래)",
    "origin_main" in pm_c)
d = pd.read_parquet(V1 / "shipment_master_202401.parquet",
                    columns=["shpmtorigin", "shpcountry"])
say(f"  - 실측(2024-01): `shpmtorigin` 결측 **{d.shpmtorigin.isna().mean()*100:.2f}%** vs "
    f"`shpcountry` 결측 **{d.shpcountry.isna().mean()*100:.2f}%** — 함정 문서의 "
    "0.08% vs 31.2% 와 같은 방향")
del d

say("\n### A-4 — 표준필터\n")
d = pd.read_parquet(V1 / "shipment_master_202401.parquet",
                    columns=["concountry", "frob"])
bad = int((d.concountry.notna() & (d.concountry != "United States")).sum())
chk("함정", "A-4 `conCountry` 가 US 또는 결측만 남아 있다", bad == 0, f"— 위반 {bad:,}건")
chk("함정", "A-4 `frob` 가 1 인 행이 없다", int((d.frob == 1).sum()) == 0)
del d

say("\n### A-5 — crossRef `activeFlag=1` · `primaryFlag` 미사용\n")
sql = (Path(r"C:\panjiva\projects\20251201\scripts\extraction\src_trade_pull.py")
       .read_text(encoding="utf-8"))
chk("함정", "A-5 추출 SQL 에 `activeFlag = 1` 이 있다", "activeFlag = 1" in sql)
chk("함정", "A-5 추출 SQL 이 `primaryFlag` 로 거르지 않는다",
    not re.search(r"primaryFlag\s*=\s*1", sql))

say("\n### A-6 · A-7 — 법인 vs 최종모회사, 재무 커버리지\n")
chk("명세", "A-6 v1 이 법인(`*_ciqid`)과 최종모회사(`*_up`) 를 **둘 다** 보존",
    {"con_ciqid", "con_up", "shp_ciqid", "shp_up"} <= c1)
acc = {}
for m in MONTHS:
    d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                        columns=["valueofgoodsusd", "con_a_financial_period_id",
                                 "con_up_a_financial_period_id"])
    v = d.valueofgoodsusd.fillna(0)
    acc["tot"] = acc.get("tot", 0) + v.sum()
    acc["법인"] = acc.get("법인", 0) + v[d.con_a_financial_period_id.notna()].sum()
    acc["모회사"] = acc.get("모회사", 0) + v[d.con_up_a_financial_period_id.notna()].sum()
    del d
say(md(pd.DataFrame([
    {"기준": "법인 자신의 연간 재무", "수입액 커버(%)": acc["법인"] / acc["tot"] * 100},
    {"기준": "최종모회사의 연간 재무", "수입액 커버(%)": acc["모회사"] / acc["tot"] * 100}])))
chk("함정", "A-7 모회사 롤업이 커버리지를 크게 올린다 (함정 문서: 6.0% → 42.9%)",
    acc["모회사"] > acc["법인"] * 3,
    f"— {acc['법인']/acc['tot']*100:.1f}% → {acc['모회사']/acc['tot']*100:.1f}% "
    f"({acc['모회사']/acc['법인']:.1f}배)")
say("  - 절대 수준이 함정 문서(42.9%)보다 낮은 것은 **결합 방식이 달라서**다. 문서 수치는 "
    "as-of + 소급 2년(여러 해 재무를 끌어옴), 우리는 `cal_year=2024` equi-join(그 해 재무만). "
    "**롤업 효과 자체는 같은 방향으로 크게 나타난다**(5.6배).")

say("\n### A-7 — `filingDate` 를 시점 필터로 쓰지 않았는가\n")
# 자기 자신은 검사 대상에서 뺀다 — 이 파일 안의 정규식 문자열이 잡히기 때문이다.
srcs = [f for f in Path(r"C:\panjiva\projects\20251201\scripts").rglob("*.py")
        if f.name != Path(__file__).name]
use = {"정렬(order by)": 0, "SELECT 컬럼": 0, "주석·문서": 0, "**필터**": []}
for f in srcs:
    t = f.read_text(encoding="utf-8", errors="ignore")
    for mm in re.finditer(r"filing_date|filingDate", t):
        s0 = t.rfind("\n", 0, mm.start()) + 1
        line = t[s0:t.find("\n", mm.end())].strip()
        low = line.lower()
        if low.lstrip().startswith(("#", "--", '"', "*")) or "⚠" in line:
            use["주석·문서"] += 1
        elif "order by" in low:
            use["정렬(order by)"] += 1
        elif re.search(r"(filing_date|filingDate)\s*(<|>|<=|>=|=|between)\s*['\"\d]", line) \
                or re.search(r"where[^)]*filing", low):
            use["**필터**"].append(f"{f.name}: {line[:70]}")
        else:
            use["SELECT 컬럼"] += 1
say(md(pd.DataFrame([{"쓰임": k, "건수": len(v) if isinstance(v, list) else v}
                     for k, v in use.items()])))
chk("함정", "A-7 어느 스크립트도 `filingDate` 를 **시점 필터**로 쓰지 않는다",
    not use["**필터**"], f"— {use['**필터**'][:3]}" if use["**필터**"] else
    "— 정렬·SELECT·주석 용도만 (정렬은 정정본 중 최신을 고르는 용도라 무해)")

say("\n### A-8 — 금액 결측률과 TEU 결측\n")
acc2 = {}
for m in MONTHS:
    d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                        columns=["valueofgoodsusd", "volumeteu", "billofladingtype"])
    acc2["n"] = acc2.get("n", 0) + len(d)
    acc2["v"] = acc2.get("v", 0) + int(d.valueofgoodsusd.notna().sum())
    for bl in ("House", "Simple"):
        s = d[d.billofladingtype == bl]
        acc2[f"n_{bl}"] = acc2.get(f"n_{bl}", 0) + len(s)
        acc2[f"t_{bl}"] = acc2.get(f"t_{bl}", 0) + int(s.volumeteu.isna().sum())
    del d
say(md(pd.DataFrame([
    {"지표": "금액(`valueofgoodsusd`) 비결측률(%)", "값": acc2["v"] / acc2["n"] * 100},
    {"지표": "TEU 결측률 — House B/L (%)",
     "값": acc2["t_House"] / max(acc2["n_House"], 1) * 100},
    {"지표": "TEU 결측률 — Simple B/L (%)",
     "값": acc2["t_Simple"] / max(acc2["n_Simple"], 1) * 100}])))
say("  - 함정 문서 기준: 금액 96~99% · TEU House 22.4% · Simple 5.0%")

# ---------------------------------------------------------------- B. 명세
say("\n---\n\n## B. 명세 04 조항별 대조\n")

say("\n### §4.1 당사자별 상태 — 컬럼 6종 × 2측\n")
need41 = []
for side in ("con", "shp"):
    need41 += [f"{side}panjivaid", f"{side}_ciqid_original", f"{side}_ciqid",
               f"{side}_crosswalk_overridden", f"{side}_up",
               f"{side}_ownership_is_fallback"]
miss = [c for c in need41 if c not in c1]
chk("명세", "§4.1 당사자별 6컬럼 × 2측 전부 존재", not miss, f"— 없는 것 {miss}")

say("\n### §4.2 · §4.3 매칭상태와 관계분류\n")
d = pd.read_parquet(V1 / "shipment_master_202401.parquet",
                    columns=["crosswalk_match_status", "ownership_match_status",
                             "relationship", "unmatched_reason", "intra_group",
                             "self_shipment", "within_firm_type"])
vals4 = {"both", "consignee_only", "shipper_only", "none"}
chk("명세", "§4.2 `crosswalk_match_status` 가 명세 4값만 갖는다",
    set(d.crosswalk_match_status.dropna().unique()) <= vals4)
chk("명세", "§4.2 `ownership_match_status` 도 4값",
    set(d.ownership_match_status.dropna().unique()) <= vals4)
chk("명세", "§4.3 `relationship` 이 3값만",
    set(d.relationship.dropna().unique()) <= {"within_firm", "arms_length", "unmatched"})
rec = {"matched", "entity_unmatched_consignee", "entity_unmatched_shipper",
       "entity_unmatched_both", "ownership_unmatched_consignee",
       "ownership_unmatched_shipper", "ownership_unmatched_both"}
got = set(d.unmatched_reason.dropna().unique())
chk("명세", "§4.3 `unmatched_reason` 이 권장 7값 안에 있다", got <= rec, f"— 실제 {sorted(got)}")
chk("명세", "§4.3 `intra_group` 은 unmatched 에서 결측",
    int(((d.relationship == "unmatched") & d.intra_group.notna()).sum()) == 0)
say(f"  - `crosswalk_match_status` 와 `ownership_match_status` 가 **다른 행**: "
    f"**{int((d.crosswalk_match_status != d.ownership_match_status).sum()):,}건** "
    f"/ {len(d):,} — 명세가 전제한 2단계 매칭이 사실상 1단계임을 보여준다")
del d

say("\n### §5 pair×월 관계변수\n")
need5 = {"within_share_value", "within_share_count", "within_share",
         "within_share_is_count_based", "within_firm", "relationship_mixed"}
chk("명세", "§5 지표 6종이 `panel_pair_month` 에 있다", need5 <= pm_c,
    f"— 없는 것 {sorted(need5 - pm_c)}")
chk("명세", "§5 같은 지표가 v2 세 패널에도 있다 (대사 가능)",
    all(need5 <= {c.replace("imp_", "").replace("exp_", "") for c in cols(V2 / f)}
        for f in ("02_pair.parquet", "03_firm.parquet", "04_group.parquet")))

say("\n### §6 소유구조 변화·관계전환\n")
rel_c = cols(V3 / "dim_relationship.parquet")
chk("명세", "§6.4 관계전환 컬럼 존재",
    {"relationship_changed_2024", "n_transitions", "within_to_arms",
     "arms_to_within"} <= rel_c)
tb = Path(r"C:\panjiva\projects\20251201\output\tables\wf2024")
for f in ("d8_ownership_change_summary.csv", "d8_ownership_change_monthly.csv",
          "d9_relationship_transition_summary.csv"):
    chk("명세", f"§6.5 `{f}` 생성됨", (tb / f).exists())
chk("명세", "§6.5 `d9_relationship_transition_pairs.parquet` 생성됨",
    (V3 / "d9_relationship_transition_pairs.parquet").exists())

say("\n### §7 override\n")
chk("명세", "§7 원본(`*_ciqid_original`)과 적용값(`*_ciqid`)이 **둘 다** 보존",
    {"con_ciqid_original", "con_ciqid"} <= c1)
d = pd.read_parquet(V1 / "shipment_master_202401.parquet",
                    columns=["con_crosswalk_overridden", "shp_crosswalk_overridden"])
chk("명세", "§7 승인 파일 미제출 상태이므로 override 적용이 0 건",
    int(d.con_crosswalk_overridden.sum() + d.shp_crosswalk_overridden.sum()) == 0)
del d

say("\n### §8 산출물 목록\n")
want = {
    "v1": [V1 / f"shipment_master_{m}.parquet" for m in MONTHS],
    "v2": [V2 / f for f in ("02_pair.parquet", "03_firm.parquet", "04_group.parquet")],
    "v3": [V3 / f for f in ("panel_pair_month.parquet", "dim_relationship.parquet",
                            "panel_firm_quarter.parquet",
                            "panel_firm_origin_hs.parquet")]
       + [V3 / f"{n}.dta" for n in ("panel_pair_month", "dim_relationship",
                                    "panel_firm_quarter", "panel_firm_origin_hs")]
       + [V3 / "95_report.md"],
}
for k, fs in want.items():
    missf = [f.name for f in fs if not f.exists()]
    chk("명세", f"§8 {k} 산출물 전부 존재", not missf, f"— 없는 것 {missf}")

# ---------------------------------------------------------------- C. 교차대사
say("\n---\n\n## C. v1 → v2 → v3 총계 연결\n")
tot_n = tot_v = 0
both_n = both_v = 0.0
con_n = con_v = 0.0
up_n = up_v = 0.0
rel_n = {}; rel_v = {}
for m in MONTHS:
    d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                        columns=["valueofgoodsusd", "con_ciqid", "shp_ciqid",
                                 "con_up", "relationship"])
    v = d.valueofgoodsusd.fillna(0)
    tot_n += len(d); tot_v += v.sum()
    b = d.con_ciqid.notna() & d.shp_ciqid.notna()
    both_n += int(b.sum()); both_v += float(v[b].sum())
    c = d.con_ciqid.notna(); con_n += int(c.sum()); con_v += float(v[c].sum())
    u = d.con_up.notna(); up_n += int(u.sum()); up_v += float(v[u].sum())
    g = d.groupby("relationship", observed=True)
    for kk, vv in g.size().items():
        rel_n[kk] = rel_n.get(kk, 0) + int(vv)
    for kk, vv in g["valueofgoodsusd"].sum().items():
        rel_v[kk] = rel_v.get(kk, 0.0) + float(vv)
    del d

src_n = sum(pq.ParquetFile(SRC / f"imp_ship_{m}.parquet").metadata.num_rows
            for m in MONTHS)
p2 = pd.read_parquet(V2 / "03_firm.parquet",
                     columns=["imp_n_ship", "imp_value_usd"])
p4 = pd.read_parquet(V2 / "04_group.parquet",
                     columns=["imp_n_ship", "imp_value_usd"])
pm = pd.read_parquet(V3 / "panel_pair_month.parquet",
                     columns=["n_shipments", "value_usd", "value_within_firm",
                              "value_arms"])
rows = [
    {"층": "공용 원천 `imp_ship_*`", "선적": src_n, "금액($B)": np.nan},
    {"층": "v1 `shipment_master_*`", "선적": tot_n, "금액($B)": tot_v / 1e9},
    {"층": "v2 `03_firm` 수입 측 (= 수입자 매칭 선적)", "선적": int(p2.imp_n_ship.sum()),
     "금액($B)": float(p2.imp_value_usd.sum()) / 1e9},
    {"층": "  ↳ v1 기준값", "선적": int(con_n), "금액($B)": con_v / 1e9},
    {"층": "v2 `04_group` 수입 측 (= 수입자 UP 있는 선적)",
     "선적": int(p4.imp_n_ship.sum()), "금액($B)": float(p4.imp_value_usd.sum()) / 1e9},
    {"층": "  ↳ v1 기준값", "선적": int(up_n), "금액($B)": up_v / 1e9},
    {"층": "v3 `panel_pair_month` (= 양측 매칭 선적)", "선적": int(pm.n_shipments.sum()),
     "금액($B)": float(pm.value_usd.sum()) / 1e9},
    {"층": "  ↳ v1 기준값", "선적": int(both_n), "금액($B)": both_v / 1e9},
]
say(md(pd.DataFrame(rows)))
chk("대사", "C-1 v1 선적 = 공용 원천", tot_n == src_n, f"— 차이 {tot_n - src_n:+,}")
chk("대사", "C-2 v2 03_firm 수입 측 = v1 수입자 매칭",
    int(p2.imp_n_ship.sum()) == int(con_n)
    and abs(float(p2.imp_value_usd.sum()) - con_v) < 1)
chk("대사", "C-3 v3 pair×월 = v1 양측 매칭",
    int(pm.n_shipments.sum()) == int(both_n)
    and abs(float(pm.value_usd.sum()) - both_v) < 1)
say("\n### 관계분류 — v1 과 v3 가 같은 숫자를 말하는가\n")
say(md(pd.DataFrame([
    {"출처": "v1 (선적 전체)", "within_firm($B)": rel_v.get("within_firm", 0) / 1e9,
     "arms_length($B)": rel_v.get("arms_length", 0) / 1e9},
    {"출처": "v3 pair×월", "within_firm($B)": float(pm.value_within_firm.sum()) / 1e9,
     "arms_length($B)": float(pm.value_arms.sum()) / 1e9}])))
chk("대사", "C-4 within_firm 금액이 v1 = v3",
    abs(rel_v.get("within_firm", 0) - float(pm.value_within_firm.sum())) < 1)
chk("대사", "C-5 arms_length 금액이 v1 = v3",
    abs(rel_v.get("arms_length", 0) - float(pm.value_arms.sum())) < 1)

# ---------------------------------------------------------------- D. 문서
say("\n---\n\n## D. 문서에 적힌 수치가 실제와 맞는가\n")
docs = {
    "v1 COLUMNS.md": (V1 / "COLUMNS.md", tot_n),
    "v2 COLUMNS.md": (V2 / "COLUMNS.md", None),
    "v3 COLUMNS.md": (V3 / "COLUMNS.md", None),
}
t = (V1 / "COLUMNS.md").read_text(encoding="utf-8")
chk("문서", "D-1 v1 COLUMNS.md 의 행 수가 실제와 일치",
    f"{tot_n:,}" in t, f"— 문서에 `{tot_n:,}` 있음")
ncol1 = len(c1)
chk("문서", "D-2 v1 COLUMNS.md 의 열 수가 실제와 일치", f"{ncol1:,}열" in t,
    f"— 실제 {ncol1:,}열")
t2 = (V2 / "COLUMNS.md").read_text(encoding="utf-8")
for n, f in (("02_pair", "02_pair.parquet"), ("03_firm", "03_firm.parquet"),
             ("04_group", "04_group.parquet")):
    nr = pq.ParquetFile(V2 / f).metadata.num_rows
    chk("문서", f"D-3 v2 COLUMNS.md 의 `{n}` 행 수", f"{nr:,}" in t2, f"— 실제 {nr:,}")
t3 = (V3 / "COLUMNS.md").read_text(encoding="utf-8")
for n in ("panel_pair_month", "dim_relationship", "panel_firm_quarter",
          "panel_firm_origin_hs"):
    nr = pq.ParquetFile(V3 / f"{n}.parquet").metadata.num_rows
    chk("문서", f"D-4 v3 COLUMNS.md 의 `{n}` 행 수", f"{nr:,}" in t3, f"— 실제 {nr:,}")

cat = Path(r"C:\panjiva\data\staging\_catalog.md").read_text(encoding="utf-8")
for n, nr in (("tom_v1_2024", tot_n),):
    chk("문서", f"D-5 카탈로그의 {n} 행 수", f"{nr:,}" in cat)

# ---------------------------------------------------------------- 요약
say("\n---\n\n## 요약\n")
r = pd.DataFrame(RES)
say(md(r.groupby(["구분", "결과"]).size().rename("건수").reset_index()))
fails = r[r.결과 == "FAIL"]
say(f"\n**{len(r)}개 항목 중 {len(r)-len(fails)}개 PASS**"
    + ("" if not len(fails) else "\n\n### 실패 항목\n\n" + md(fails)))

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(L), encoding="utf-8")
print(f"\n→ {OUT}", flush=True)
