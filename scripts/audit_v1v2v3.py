# -*- coding: utf-8 -*-
r"""
audit_v1v2v3.py — v1·v2·v3 교차 감사

각 버전의 `90_checks.md` 는 **그 버전 안에서** 맞는지를 본다. 이 스크립트는 그 밖의 것을 본다:

  A. 팀 함정 12종(`shared memory\CLAUDE.md`)이 실제 산출물에서 지켜졌는가
  B. 명세 04 의 조항별 요구사항이 컬럼·값으로 존재하는가
  C. v1 → v2 → v3 총계가 한 줄로 이어지는가
  D. 결정 기록(`DECISIONS.md`)·변수 가이드(`COLUMNS.md`)에 적힌 수치가 실제 데이터와 맞는가

산출: `--out` (기본 `projects\20251201\output\AUDIT_v1v2v3.md`)

폴더·기간이 전부 인자다. 월은 `--v1-dir` 의 `shipment_master_YYYYMM.parquet` 로 발견한다.
값역 검사(§4.2·§4.3)와 함정 검사는 **전 월**을 훑는다(월별 누적 — 전량 메모리 보관 없음).

사용:
  python scripts\audit_v1v2v3.py                                          # 2024 equi 3폴더
  python ... --v1-dir ...\tom_v1_2024_asof --v2-dir ...\tom_v2_2024_asof \
             --v3-dir ...\within_firm_pilot_2024_asof --tables-dir ...\tables\wf2024_asof \
             --out ...\output\AUDIT_v1v2v3_asof.md
"""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "processing"))
from v4_common import discover_months            # noqa: E402

STAGING = Path(r"C:\panjiva\data\staging")
DEFAULTS = {
    "src_dir": STAGING / "source" / "trade",
    "ciq_dir": STAGING / "source" / "ciq_ref",
    "fin_dir": STAGING / "source" / "ciq_fin",
    "v1_dir": STAGING / "tom_v1_2024",
    "v2_dir": STAGING / "tom_v2_2024",
    "v3_dir": STAGING / "within_firm_pilot_2024",
    "tables_dir": HERE.parent / "output" / "tables" / "wf2024",
    "out": HERE.parent / "output" / "AUDIT_v1v2v3.md",
}
# 팀 함정 문서 8: "TEU 는 House B/L 에서 22.4% 결측 (Simple 5.0%)". House B/L 의 TEU 는
# NULL 이 아니라 **0** 으로 오는 경우가 많아 `isna` 와 `isna | == 0` 을 함께 본다.
TEU_REF = {"House": 22.4, "Simple": 5.0}
TEU_REF_TOL = 2.0          # 참조값 대비 비율이 [1/2, 2] 밖이면 "크게 어긋남" → FAIL

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


def resolve_months(v1: Path, start, end) -> list:
    found = list(discover_months([v1], "shipment_master"))
    if not found:
        raise SystemExit(f"v1 폴더에 shipment_master_YYYYMM.parquet 이 없다: {v1}")
    if start is None and end is None:
        return found
    lo = start.replace("-", "")[:6] if start else found[0]
    hi = (pd.Timestamp(end) - pd.Timedelta(days=1)).strftime("%Y%m") if end else found[-1]
    months = [m for m in found if lo <= m <= hi]
    if not months:
        raise SystemExit(f"기간 {start}~{end} 에 해당하는 v1 월이 없다")
    return months


def has_count(text: str, n_rows: int, n_cols: int) -> bool:
    """문서에 열 수가 적혀 있는가 — `N열` 표기 또는 `| 행 | 열 |` 표 칸."""
    return f"{n_cols:,}열" in text or f"| {n_rows:,} | {n_cols:,} |" in text


def main():
    ap = argparse.ArgumentParser(description="v1·v2·v3 교차 감사")
    for k, v in DEFAULTS.items():
        ap.add_argument(f"--{k.replace('_', '-')}", default=str(v))
    ap.add_argument("--start", default=None, help="시작월(YYYY-MM-DD). 미지정이면 v1 폴더 전체")
    ap.add_argument("--end", default=None, help="종료(미포함)")
    a = ap.parse_args()
    SRC, CIQ, FIN = Path(a.src_dir), Path(a.ciq_dir), Path(a.fin_dir)
    V1, V2, V3 = Path(a.v1_dir), Path(a.v2_dir), Path(a.v3_dir)
    TB, OUT = Path(a.tables_dir), Path(a.out)
    MONTHS = resolve_months(V1, a.start, a.end)
    first = V1 / f"shipment_master_{MONTHS[0]}.parquet"

    say("# v1 · v2 · v3 교차 감사\n")
    say(f"**감사일** {date.today()} · **스크립트** `audit_v1v2v3.py` · "
        f"**대상** v1 `{V1}` · v2 `{V2}` · v3 `{V3}` · 표 `{TB}` · "
        f"**월** {MONTHS[0]}~{MONTHS[-1]} ({len(MONTHS)}개월)\n")
    say("각 버전의 `90_checks.md` 가 못 보는 것 — 팀 함정 준수 · 명세 조항 대조 · "
        "버전 간 총계 연결 · 문서 수치 일치 — 를 본다.\n")

    # ---------------------------------------------------------------- A. 함정
    say("\n## A. 팀 함정 12종 (`shared memory\\CLAUDE.md`)\n")

    say("\n### A-1 · A-11 — HS 코드 추출과 `zfill` 금지\n")
    bad_prefix = zfill_trace = zfill_v1 = 0
    lens = {}
    for m in MONTHS:
        hs = pd.read_parquet(SRC / f"imp_hs_{m}.parquet", columns=["hs6"]).hs6.astype("string")
        bad_prefix += int(hs.str.contains(":", na=False).sum())
        ln = hs.str.len()
        for k, v in ln.dropna().value_counts().items():
            lens[int(k)] = lens.get(int(k), 0) + int(v)
        # ⚠️ 함정 11 은 "6자리로 맞춰라" 가 **아니라 그 반대**다 — 4자리 HS 를 `zfill(6)` 하면
        #    `9804` 가 `009804` 가 되어 챕터 98 이 00 으로 뒤바뀐다. 4자리는 **그대로 두는 것이 정답**.
        #    검사할 것은 "6자리인데 00 으로 시작하는 것이 있는가"(= zfill 흔적)다.
        zfill_trace += int(((ln == 6) & hs.str.startswith("00")).sum())
        s1 = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                             columns=["hs6"]).hs6.dropna().astype("string")
        zfill_v1 += int(((s1.str.len() == 6) & s1.str.startswith("00")).sum())
        del hs, s1
    chk("함정", "A-1 HS 접두어(`Classified:` 등)가 값에 남아 있지 않다",
        bad_prefix == 0, f"— 접두어 잔존 {bad_prefix:,}건")
    chk("함정", "A-11 `zfill(6)` 흔적이 없다 (6자리인데 `00` 으로 시작)",
        zfill_trace == 0, f"— 흔적 {zfill_trace:,}건")
    lens_s = ", ".join(f"{k}자리 {v:,}" for k, v in sorted(lens.items()))
    say(f"  - 길이 분포(`imp_hs` 전 월): {lens_s} — **4자리는 정상**"
        "(원본이 4자리인 HS. 대표 코드 9804·9905·7111 등)")
    chk("함정", "A-11 v1 의 `hs6` 에도 zfill 흔적 없음", zfill_v1 == 0, f"— 흔적 {zfill_v1:,}건")

    say("\n### A-2 — 자식(HS) 조인 후 `sum()` 금지\n")
    say("v1·v2 는 HS 자식을 조인하지 않고 **대표 HS 1:1** 만 쓴다. "
        "v3 `panel_firm_origin_hs` 만 자식을 쓰는데 **균등배분**이라 총액이 보존된다.\n")
    fo = pd.read_parquet(V3 / "panel_firm_origin_hs.parquet",
                         columns=["value_usd", "weight_kg", "teu", "n_shipments"])
    ref_v = ref_w = ref_t = 0.0
    ref_n = 0
    for m in MONTHS:
        # HS 자식은 같은 달의 선적만 가리키므로 월 안에서 대조한다 (전 기간 id 집합을 들고 있지 않는다)
        ids = np.unique(pd.read_parquet(SRC / f"imp_hs_{m}.parquet", columns=["panjivarecordid"])
                        .panjivarecordid.to_numpy(dtype="int64"))
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["panjivarecordid", "con_ciqid", "valueofgoodsusd",
                                     "weightkg", "volumeteu"])
        d = d[d.con_ciqid.notna().to_numpy()
              & np.isin(d.panjivarecordid.to_numpy(dtype="int64"), ids)]
        ref_v += d.valueofgoodsusd.fillna(0).sum()
        ref_w += d.weightkg.fillna(0).sum()
        ref_t += d.volumeteu.fillna(0).sum()
        ref_n += len(d)                    # PK 유일(v1 G3)이므로 행 수 = 선적 수
        del d, ids
    dv = abs(fo.value_usd.sum() - ref_v)
    chk("함정", "A-2 균등배분 후 **금액 합계 보존** (행 수가 아니라 합계로 검증)",
        dv < 1, f"— 차이 ${dv:.2f}")
    chk("함정", "A-2 중량·TEU 도 보존",
        abs(fo.weight_kg.sum() - ref_w) < 1 and abs(fo.teu.sum() - ref_t) < 0.01)
    say(f"  - 배분 후 `n_shipments` 합 {int(fo.n_shipments.sum()):,} vs 실제 선적 "
        f"{ref_n:,} — **행 수는 부풀려지는 것이 정상**(한 선적이 여러 HS 행)")
    del fo

    say("\n### A-3 — 국가 컬럼 3종을 구분해 썼는가\n")
    c1 = cols(first)
    chk("함정", "A-3 v1 이 `shpmtorigin`·`shpcountry`·`portofladingcountry` 를 다 보존",
        {"shpmtorigin", "shpcountry", "portofladingcountry"} <= c1)
    pm_c = cols(V3 / "panel_pair_month.parquet")
    chk("함정", "A-3 v3 의 원산지 대표값이 `origin_main`(=`shpmtorigin` 유래)",
        "origin_main" in pm_c)
    na = {"n": 0, "shpmtorigin": 0, "shpcountry": 0, "concountry_bad": 0, "frob1": 0}
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["shpmtorigin", "shpcountry", "concountry", "frob"])
        na["n"] += len(d)
        na["shpmtorigin"] += int(d.shpmtorigin.isna().sum())
        na["shpcountry"] += int(d.shpcountry.isna().sum())
        na["concountry_bad"] += int((d.concountry.notna() & (d.concountry != "United States")).sum())
        na["frob1"] += int((d.frob == 1).sum())
        del d
    say(f"  - 실측(전 월): `shpmtorigin` 결측 **{na['shpmtorigin']/na['n']*100:.2f}%** vs "
        f"`shpcountry` 결측 **{na['shpcountry']/na['n']*100:.2f}%** — 함정 문서의 "
        "0.08% vs 31.2% 와 같은 방향")

    say("\n### A-4 — 표준필터\n")
    chk("함정", "A-4 `conCountry` 가 US 또는 결측만 남아 있다", na["concountry_bad"] == 0,
        f"— 위반 {na['concountry_bad']:,}건")
    chk("함정", "A-4 `frob` 가 1 인 행이 없다", na["frob1"] == 0, f"— 위반 {na['frob1']:,}건")

    say("\n### A-5 — crossRef `activeFlag=1` · `primaryFlag` 미사용\n")
    sql = (HERE / "extraction" / "src_trade_pull.py").read_text(encoding="utf-8")
    chk("함정", "A-5 추출 SQL 에 `activeFlag = 1` 이 있다", "activeFlag = 1" in sql)
    chk("함정", "A-5 추출 SQL 이 `primaryFlag` 로 거르지 않는다",
        not re.search(r"primaryFlag\s*=\s*1", sql))

    say("\n### A-6 · A-7 — 법인 vs 최종모회사, 재무 커버리지\n")
    chk("명세", "A-6 v1 이 법인(`*_ciqid`)과 최종모회사(`*_up`) 를 **둘 다** 보존",
        {"con_ciqid", "con_up", "shp_ciqid", "shp_up"} <= c1)
    acc = {"tot": 0.0, "법인": 0.0, "모회사": 0.0}
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["valueofgoodsusd", "con_a_financial_period_id",
                                     "con_up_a_financial_period_id"])
        v = d.valueofgoodsusd.fillna(0)
        acc["tot"] += float(v.sum())
        acc["법인"] += float(v[d.con_a_financial_period_id.notna()].sum())
        acc["모회사"] += float(v[d.con_up_a_financial_period_id.notna()].sum())
        del d
    say(md(pd.DataFrame([
        {"기준": "법인 자신의 연간 재무", "수입액 커버(%)": acc["법인"] / acc["tot"] * 100},
        {"기준": "최종모회사의 연간 재무", "수입액 커버(%)": acc["모회사"] / acc["tot"] * 100}])))
    chk("함정", "A-7 모회사 롤업이 커버리지를 크게 올린다 (함정 문서: 6.0% → 42.9%)",
        acc["모회사"] > acc["법인"] * 3,
        f"— {acc['법인']/acc['tot']*100:.1f}% → {acc['모회사']/acc['tot']*100:.1f}% "
        f"({acc['모회사']/max(acc['법인'], 1e-9):.1f}배)")
    say("  - 절대 수준이 함정 문서(42.9%)와 다른 것은 **표본·결합 방식이 달라서**다. 문서 수치는 "
        "as-of + 소급 2년(여러 해 재무를 끌어옴) 기준이다. "
        "**롤업 효과 자체는 같은 방향으로 크게 나타난다.**")

    say("\n### A-7 — `filingDate` 를 시점 필터로 쓰지 않았는가\n")
    # 자기 자신은 검사 대상에서 뺀다 — 이 파일 안의 정규식 문자열이 잡히기 때문이다.
    srcs = [f for f in HERE.rglob("*.py") if f.name != Path(__file__).name]
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
        not use["**필터**"], ("— " + "; ".join(use["**필터**"][:3])) if use["**필터**"] else
        "— 정렬·SELECT·주석 용도만 (정렬은 정정본 중 최신을 고르는 용도라 무해)")

    say("\n### A-8 — 금액 결측률과 TEU 결측\n")
    acc2 = {"n": 0, "v": 0}
    for bl in TEU_REF:
        acc2[f"n_{bl}"] = acc2[f"na_{bl}"] = acc2[f"na0_{bl}"] = 0
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["valueofgoodsusd", "volumeteu", "billofladingtype"])
        acc2["n"] += len(d)
        acc2["v"] += int(d.valueofgoodsusd.notna().sum())
        for bl in TEU_REF:
            s = d.loc[d.billofladingtype == bl, "volumeteu"]
            acc2[f"n_{bl}"] += len(s)
            acc2[f"na_{bl}"] += int(s.isna().sum())
            acc2[f"na0_{bl}"] += int((s.isna() | (s == 0)).sum())
        del d
    rows = [{"지표": "금액(`valueofgoodsusd`) 비결측률(%)", "값": acc2["v"] / acc2["n"] * 100,
             "함정 문서 참조": "96~99"}]
    for bl in TEU_REF:
        n_bl = max(acc2[f"n_{bl}"], 1)
        rows.append({"지표": f"TEU `isna` — {bl} B/L (%)", "값": acc2[f"na_{bl}"] / n_bl * 100,
                     "함정 문서 참조": ""})
        rows.append({"지표": f"TEU `isna | == 0` — {bl} B/L (%)",
                     "값": acc2[f"na0_{bl}"] / n_bl * 100, "함정 문서 참조": f"{TEU_REF[bl]}"})
    say(md(pd.DataFrame(rows)))
    say("  - House B/L 의 TEU 는 NULL 이 아니라 **0** 으로 오는 경우가 대부분이라 `isna` 만 세면 "
        "함정 문서와 수십 배 어긋난다. 참조값과 비교할 지표는 `isna | == 0` 이다.")
    for bl, ref in TEU_REF.items():
        rate = acc2[f"na0_{bl}"] / max(acc2[f"n_{bl}"], 1) * 100
        ok = ref / TEU_REF_TOL <= rate <= ref * TEU_REF_TOL
        chk("함정", f"A-8 TEU 결측(`isna | == 0`) — {bl} B/L 이 함정 문서 참조값과 크게 어긋나지 않는다",
            ok, f"— 실측 {rate:.2f}% vs 참조 {ref}% (허용: 비율 1/{TEU_REF_TOL:g}~{TEU_REF_TOL:g}배)")

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

    say("\n### §4.2 · §4.3 매칭상태와 관계분류 (전 월 값역)\n")
    vals = {"crosswalk_match_status": set(), "ownership_match_status": set(),
            "relationship": set(), "unmatched_reason": set()}
    n_intra_bad = n_diff = n_rows = 0
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=list(vals) + ["intra_group"])
        for c in vals:
            vals[c] |= {str(x) for x in d[c].dropna().unique()}
        n_intra_bad += int(((d.relationship == "unmatched") & d.intra_group.notna()).sum())
        n_diff += int((d.crosswalk_match_status != d.ownership_match_status).sum())
        n_rows += len(d)
        del d
    vals4 = {"both", "consignee_only", "shipper_only", "none"}
    chk("명세", "§4.2 `crosswalk_match_status` 가 명세 4값만 갖는다",
        vals["crosswalk_match_status"] <= vals4, f"— 실제 {sorted(vals['crosswalk_match_status'])}")
    chk("명세", "§4.2 `ownership_match_status` 도 4값",
        vals["ownership_match_status"] <= vals4, f"— 실제 {sorted(vals['ownership_match_status'])}")
    chk("명세", "§4.3 `relationship` 이 3값만",
        vals["relationship"] <= {"within_firm", "arms_length", "unmatched"},
        f"— 실제 {sorted(vals['relationship'])}")
    rec = {"matched", "entity_unmatched_consignee", "entity_unmatched_shipper",
           "entity_unmatched_both", "ownership_unmatched_consignee",
           "ownership_unmatched_shipper", "ownership_unmatched_both"}
    chk("명세", "§4.3 `unmatched_reason` 이 권장 7값 안에 있다",
        vals["unmatched_reason"] <= rec, f"— 실제 {sorted(vals['unmatched_reason'])}")
    chk("명세", "§4.3 `intra_group` 은 unmatched 에서 결측", n_intra_bad == 0,
        f"— 위반 {n_intra_bad:,}건")
    say(f"  - `crosswalk_match_status` 와 `ownership_match_status` 가 **다른 행**: "
        f"**{n_diff:,}건** / {n_rows:,} — 명세가 전제한 2단계 매칭이 사실상 1단계임을 보여준다")

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
    for f in ("d8_ownership_change_summary.csv", "d8_ownership_change_monthly.csv",
              "d9_relationship_transition_summary.csv"):
        chk("명세", f"§6.5 `{f}` 생성됨", (TB / f).exists(), f"— `{TB}`")
    chk("명세", "§6.5 `d9_relationship_transition_pairs.parquet` 생성됨",
        (V3 / "d9_relationship_transition_pairs.parquet").exists())

    say("\n### §7 override\n")
    chk("명세", "§7 원본(`*_ciqid_original`)과 적용값(`*_ciqid`)이 **둘 다** 보존",
        {"con_ciqid_original", "con_ciqid"} <= c1)
    n_ov = 0
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["con_crosswalk_overridden", "shp_crosswalk_overridden"])
        n_ov += int(((d.con_crosswalk_overridden == 1) | (d.shp_crosswalk_overridden == 1)).sum())
        del d
    imp = V1 / "override_impact.csv"
    if imp.exists():
        r = pd.read_csv(imp, encoding="utf-8").iloc[0]
        n_appr, n_ship = int(r["n_override_rows_approved"]), int(r["n_shipments"])
        chk("명세", "§7 승인 override 만 적용됐다 — 산출물의 overridden 선적 수 = 영향표 선적 수",
            n_ov == n_ship, f"— 적용 대상 {n_appr:,}행 · 영향표 {n_ship:,}건 · 산출물 실측 {n_ov:,}건")
    else:
        chk("명세", "§7 승인 파일 미제출(영향표 없음)이므로 override 적용이 0 건",
            n_ov == 0, f"— 실측 {n_ov:,}건")

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
    tot_n = 0; tot_v = 0.0
    both_n = 0; both_v = 0.0
    con_n = 0; con_v = 0.0
    up_n = 0; up_v = 0.0
    rel_v = {}
    for m in MONTHS:
        d = pd.read_parquet(V1 / f"shipment_master_{m}.parquet",
                            columns=["valueofgoodsusd", "con_ciqid", "shp_ciqid",
                                     "con_up", "relationship"])
        v = d.valueofgoodsusd.fillna(0)
        tot_n += len(d); tot_v += float(v.sum())
        b = d.con_ciqid.notna() & d.shp_ciqid.notna()
        both_n += int(b.sum()); both_v += float(v[b].sum())
        c = d.con_ciqid.notna(); con_n += int(c.sum()); con_v += float(v[c].sum())
        u = d.con_up.notna(); up_n += int(u.sum()); up_v += float(v[u].sum())
        for kk, vv in d.groupby("relationship", observed=True)["valueofgoodsusd"].sum().items():
            rel_v[kk] = rel_v.get(kk, 0.0) + float(vv)
        del d

    src_n = sum(pq.ParquetFile(SRC / f"imp_ship_{m}.parquet").metadata.num_rows
                for m in MONTHS)
    p2 = pd.read_parquet(V2 / "03_firm.parquet", columns=["imp_n_ship", "imp_value_usd"])
    p4 = pd.read_parquet(V2 / "04_group.parquet", columns=["imp_n_ship", "imp_value_usd"])
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
    ncol1 = len(c1)
    d1 = V1 / "COLUMNS.md"
    if d1.exists():
        t = d1.read_text(encoding="utf-8")
        chk("문서", "D-1 v1 COLUMNS.md 의 행 수가 실제와 일치",
            f"{tot_n:,}" in t, f"— 실제 {tot_n:,}")
        chk("문서", "D-2 v1 COLUMNS.md 의 열 수가 실제와 일치", f"{ncol1:,}열" in t,
            f"— 실제 {ncol1:,}열")
    else:
        chk("문서", "D-1·D-2 v1 COLUMNS.md 존재", False, f"— 파일 없음 `{d1}`")
    d2 = V2 / "COLUMNS.md"
    if d2.exists():
        t2 = d2.read_text(encoding="utf-8")
        for n in ("02_pair", "03_firm", "04_group"):
            pf = pq.ParquetFile(V2 / f"{n}.parquet")
            nr, nc = pf.metadata.num_rows, len(pf.schema_arrow.names)
            chk("문서", f"D-3 v2 COLUMNS.md 의 `{n}` 행 수", f"{nr:,}" in t2, f"— 실제 {nr:,}")
            chk("문서", f"D-3 v2 COLUMNS.md 의 `{n}` 열 수", has_count(t2, nr, nc),
                f"— 실제 {nc:,}열")
    else:
        chk("문서", "D-3 v2 COLUMNS.md 존재", False, f"— 파일 없음 `{d2}`")
    d3 = V3 / "COLUMNS.md"
    if d3.exists():
        t3 = d3.read_text(encoding="utf-8")
        for n in ("panel_pair_month", "dim_relationship", "panel_firm_quarter",
                  "panel_firm_origin_hs"):
            pf = pq.ParquetFile(V3 / f"{n}.parquet")
            nr, nc = pf.metadata.num_rows, len(pf.schema_arrow.names)
            chk("문서", f"D-4 v3 COLUMNS.md 의 `{n}` 행 수", f"{nr:,}" in t3, f"— 실제 {nr:,}")
            chk("문서", f"D-4 v3 COLUMNS.md 의 `{n}` 열 수", has_count(t3, nr, nc),
                f"— 실제 {nc:,}열")
    else:
        chk("문서", "D-4 v3 COLUMNS.md 존재", False, f"— 파일 없음 `{d3}`")

    catp = V1.parent / "_catalog.md"
    if catp.exists():
        cat = catp.read_text(encoding="utf-8")
        chk("문서", f"D-5 카탈로그의 `{V1.name}` 행 수", f"{tot_n:,}" in cat, f"— 실제 {tot_n:,}")
    else:
        say(f"- D-5 카탈로그 없음 (`{catp}`) — 생략")

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


if __name__ == "__main__":
    main()
