# -*- coding: utf-8 -*-
r"""
compare_join_modes.py — as-of 판과 equi-join 판을 나란히 대조 (+ 문서 대조 게이트)

두 산출물은 **재무 결합 방식만** 다르다. 그러므로:

  ✅ 같아야 하는 것 — 선적 수 · 금액 · 관계분류 · 매칭상태 · 패널 행 수 · 열 수
  🔀 달라도 되는 것 — 붙은 회계기간 · 재무 값 · 시점 컬럼 이름과 분포

문서 대조 게이트 (W2): 3쌍(v1·v2·v3) × {README.md, COLUMNS.md, DECISIONS.md} 에 대해
equi/asof 두 파일을 읽어
  ① `<!-- JOIN:equi -->…<!-- /JOIN -->` / `<!-- JOIN:asof -->…<!-- /JOIN -->` 블록 제거
     (여러 줄, 중첩 없음 — 결합방식에 따라 달라도 되는 단락은 이 마커로 감싼다)
  ② 폴더명 정규화 (asof 폴더명 → equi 폴더명, 표 폴더 `wf2024_asof` → `wf2024`)
  ③ `age_days` → `days_after_close`
한 뒤 **줄 단위 완전 일치**(줄 끝 공백 무시, 연속 빈 줄은 하나로)를 검사한다.
다르면 FAIL + 처음 다른 5줄, 파일이 없으면 FAIL("파일 없음").

산출: `--out` (기본 `projects\20251201\output\COMPARE_asof_vs_equi.md`)

사용:
  python scripts\compare_join_modes.py                       # 2024: *_asof 접미 유도
  python ... --v1-dir ...\tom_v1_2018 --v1-asof-dir ...\tom_v1_2018_pit   # 접미가 다르면 명시
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "processing"))
from v4_common import discover_months            # noqa: E402

STAGING = Path(r"C:\panjiva\data\staging")
EQ_DEFAULT = {"v1": STAGING / "tom_v1_2024", "v2": STAGING / "tom_v2_2024",
              "v3": STAGING / "within_firm_pilot_2024"}
TABLES_DEFAULT = HERE.parent / "output" / "tables" / "wf2024"
OUT_DEFAULT = HERE.parent / "output" / "COMPARE_asof_vs_equi.md"
DOCS = ("README.md", "COLUMNS.md", "DECISIONS.md")

# 블록은 줄 단위로 들어낸다 — 닫는 마커 뒤의 줄바꿈까지 삼켜서, 한쪽 문서에만 블록이 있어도
# 빈 줄이 남아 거짓 FAIL 이 되지 않게 한다 (중첩 없음, 여러 줄 허용)
JOIN_BLOCK = re.compile(r"[ \t]*<!--\s*JOIN:(?:equi|asof)\s*-->.*?<!--\s*/JOIN\s*-->[ \t]*\n?",
                        re.S)

L, RES = [], []


def say(s=""):
    print(s, flush=True)
    L.append(s)


def chk(name, ok, detail=""):
    RES.append({"항목": name, "결과": "PASS" if ok else "FAIL"})
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


# ---------------------------------------------------------------- 문서 대조 (W2)
def normalize_doc(text: str, renames) -> list:
    """JOIN 블록 제거 → 폴더명 정규화 → `age_days`→`days_after_close` → 줄 목록.

    renames : [(asof 이름, equi 이름), ...] — 긴 이름부터 치환한다(부분 문자열 충돌 방지).
    줄 끝 공백은 무시하고, 연속 빈 줄은 하나로 줄이며, 앞뒤 빈 줄은 버린다(블록을 들어낸
    자리의 빈 줄 차이가 거짓 FAIL 이 되지 않게).
    """
    t = JOIN_BLOCK.sub("", text.replace("\r\n", "\n"))
    for src, dst in sorted(renames, key=lambda p: -len(p[0])):
        if src != dst:
            t = t.replace(src, dst)
    t = t.replace("age_days", "days_after_close")
    out = []
    for ln in t.split("\n"):
        ln = ln.rstrip()
        if ln == "" and out and out[-1] == "":
            continue
        out.append(ln)
    while out and out[0] == "":
        out.pop(0)
    while out and out[-1] == "":
        out.pop()
    return out


def first_diffs(a: list, b: list, k: int = 5) -> list:
    """두 줄 목록의 처음 다른 k 줄 → [(줄번호(1-based, a 기준), a줄, b줄)]. 길이가 다르면 빈 줄로 채운다."""
    out = []
    for i in range(max(len(a), len(b))):
        x = a[i] if i < len(a) else "<끝>"
        y = b[i] if i < len(b) else "<끝>"
        if x != y:
            out.append((i + 1, x, y))
            if len(out) >= k:
                break
    return out


def doc_gate(pairs: dict, renames) -> None:
    """3쌍 × 3문서 대조. pairs = {"v1": (equi_dir, asof_dir), ...}"""
    say("\n## 문서 대조 — asof 폴더 문서가 equi 폴더 문서와 (결합방식 단락 빼고) 같은가\n")
    say("정규화: `<!-- JOIN:equi|asof -->…<!-- /JOIN -->` 블록 제거 → 폴더명 → "
        "`age_days`→`days_after_close` → 줄 단위 비교(줄 끝 공백 무시, 연속 빈 줄 하나로).\n")
    for lab, (eq, asf) in pairs.items():
        for doc in DOCS:
            fe, fa = eq / doc, asf / doc
            missing = [str(p) for p in (fe, fa) if not p.exists()]
            if missing:
                chk(f"문서 {lab} `{doc}` equi = asof", False,
                    "— 파일 없음: " + " · ".join(f"`{m}`" for m in missing))
                continue
            le = normalize_doc(fe.read_text(encoding="utf-8"), renames)
            la = normalize_doc(fa.read_text(encoding="utf-8"), renames)
            diffs = first_diffs(le, la)
            chk(f"문서 {lab} `{doc}` equi = asof", not diffs,
                f"— 정규화 후 {len(le):,}/{len(la):,}줄"
                + ("" if not diffs else f", 처음 다른 줄 {len(diffs)}개 아래"))
            if diffs:
                say("")
                say("  | 줄 | equi | asof |")
                say("  |---|---|---|")
                for i, x, y in diffs:
                    esc = lambda s: s.replace("|", "\\|")[:120]      # noqa: E731
                    say(f"  | {i} | `{esc(x)}` | `{esc(y)}` |")
                say("")


def resolve_months(v1: Path, start, end) -> list:
    found = list(discover_months([v1], "shipment_master"))
    if not found:
        raise SystemExit(f"v1 폴더에 shipment_master_YYYYMM.parquet 이 없다: {v1}")
    lo = start.replace("-", "")[:6] if start else found[0]
    hi = (pd.Timestamp(end) - pd.Timedelta(days=1)).strftime("%Y%m") if end else found[-1]
    months = [m for m in found if lo <= m <= hi]
    if not months:
        raise SystemExit(f"기간 {start}~{end} 에 해당하는 v1 월이 없다")
    return months


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="as-of 판 vs equi-join 판 대조")
    for k, v in EQ_DEFAULT.items():
        ap.add_argument(f"--{k}-dir", default=str(v), help=f"{k} equi 판 폴더")
        ap.add_argument(f"--{k}-asof-dir", default=None,
                        help=f"{k} as-of 판 폴더 (기본: --{k}-dir + --asof-suffix)")
    ap.add_argument("--asof-suffix", default="_asof", help="as-of 폴더명 접미 (기본 `_asof`)")
    ap.add_argument("--tables-dir", default=str(TABLES_DEFAULT), help="equi 판 통계표 폴더")
    ap.add_argument("--tables-asof-dir", default=None, help="as-of 판 통계표 폴더 (기본: 접미 유도)")
    ap.add_argument("--out", default=str(OUT_DEFAULT))
    ap.add_argument("--start", default=None, help="시작월(YYYY-MM-DD). 미지정이면 equi v1 폴더 전체")
    ap.add_argument("--end", default=None, help="종료(미포함)")
    a = ap.parse_args()

    EQ = {k: Path(getattr(a, f"{k}_dir")) for k in EQ_DEFAULT}
    AS = {k: Path(getattr(a, f"{k}_asof_dir") or (str(EQ[k]) + a.asof_suffix)) for k in EQ_DEFAULT}
    TB_EQ = Path(a.tables_dir)
    TB_AS = Path(a.tables_asof_dir or (str(TB_EQ) + a.asof_suffix))
    OUT = Path(a.out)
    MONTHS = resolve_months(EQ["v1"], a.start, a.end)
    renames = [(AS[k].name, EQ[k].name) for k in EQ] + [(TB_AS.name, TB_EQ.name)]

    say("# as-of 판 vs equi-join 판 — 대조\n")
    say(f"**대조일** {date.today()} · **스크립트** `compare_join_modes.py` · "
        f"**월** {MONTHS[0]}~{MONTHS[-1]} ({len(MONTHS)}개월)\n")
    say("두 산출물은 **재무 결합 방식만** 다르다. 선적·관계는 같고 재무만 달라야 정상이다.\n")
    say(f"- `{AS['v1'].name}` 등 = **명세 §3.3 준수본(정본)** (`*_age_days`, 항상 양수)")
    say(f"- `{EQ['v1'].name}` 등 = **대안본** (`*_days_after_close`, 음수 가능)\n")
    say("| 판 | v1 | v2 | v3 | 통계표 |\n|---|---|---|---|---|")
    say(f"| equi | `{EQ['v1']}` | `{EQ['v2']}` | `{EQ['v3']}` | `{TB_EQ}` |")
    say(f"| asof | `{AS['v1']}` | `{AS['v2']}` | `{AS['v3']}` | `{TB_AS}` |\n")

    # ---------------------------------------------------------------- v1
    say("\n## v1 — 선적층\n")
    acc = {"n": [0, 0], "v": [0.0, 0.0], "rel_diff": 0, "cw_diff": 0, "id_diff": 0, "fp_diff": 0}
    age, dac = [], []
    n_cmp = 0
    for m in MONTHS:
        fe, fa = EQ["v1"] / f"shipment_master_{m}.parquet", AS["v1"] / f"shipment_master_{m}.parquet"
        if not fa.exists():
            continue
        n_cmp += 1
        ce = ["panjivarecordid", "valueofgoodsusd", "relationship",
              "crosswalk_match_status", "con_ciqid", "shp_ciqid",
              "con_up_a_financial_period_id"]
        e = pd.read_parquet(fe, columns=ce + ["con_up_a_days_after_close"])
        s = pd.read_parquet(fa, columns=ce + ["con_up_a_age_days"])
        acc["n"][0] += len(e); acc["n"][1] += len(s)
        acc["v"][0] += float(e.valueofgoodsusd.fillna(0).sum())
        acc["v"][1] += float(s.valueofgoodsusd.fillna(0).sum())
        assert (e.panjivarecordid.values == s.panjivarecordid.values).all(), f"{m}: 행 순서 다름"
        acc["rel_diff"] += int((e.relationship != s.relationship).sum())
        acc["cw_diff"] += int((e.crosswalk_match_status != s.crosswalk_match_status).sum())
        acc["id_diff"] += int((e.con_ciqid.fillna(-1) != s.con_ciqid.fillna(-1)).sum())
        acc["fp_diff"] += int((e.con_up_a_financial_period_id.fillna(0)
                               != s.con_up_a_financial_period_id.fillna(0)).sum())
        age.append(s.con_up_a_age_days.dropna().to_numpy(dtype="int16"))
        dac.append(e.con_up_a_days_after_close.dropna().to_numpy(dtype="int16"))
        del e, s
    if n_cmp == 0:
        chk("v1 as-of 판 존재", False, f"— `{AS['v1']}` 에 겹치는 월 파일 없음")
    else:
        say(f"비교한 월: {n_cmp}/{len(MONTHS)}\n")
        say("### 같아야 하는 것\n")
        say(md(pd.DataFrame([
            {"항목": "선적 수", "equi": acc["n"][0], "asof": acc["n"][1], "차이": acc["n"][1] - acc["n"][0]},
            {"항목": "금액($B)", "equi": acc["v"][0] / 1e9, "asof": acc["v"][1] / 1e9,
             "차이": (acc["v"][1] - acc["v"][0]) / 1e9}])))
        chk("v1 선적 수 동일", acc["n"][0] == acc["n"][1])
        chk("v1 금액 동일", abs(acc["v"][1] - acc["v"][0]) < 1)
        chk("v1 `relationship` 동일", acc["rel_diff"] == 0, f"— 다른 행 {acc['rel_diff']:,}")
        chk("v1 `crosswalk_match_status` 동일", acc["cw_diff"] == 0, f"— 다른 행 {acc['cw_diff']:,}")
        chk("v1 `con_ciqid` 동일", acc["id_diff"] == 0, f"— 다른 행 {acc['id_diff']:,}")

        say("\n### 달라야 하는 것 — 붙은 회계기간\n")
        ag, dc = np.concatenate(age), np.concatenate(dac)
        say(md(pd.DataFrame([
            {"방식": "equi (`days_after_close`)", "부착": len(dc), "중위": float(np.median(dc)),
             "최소": float(dc.min()), "최대": float(dc.max()),
             "음수 비율(%)": float((dc < 0).mean() * 100)},
            {"방식": "asof (`age_days`)", "부착": len(ag), "중위": float(np.median(ag)),
             "최소": float(ag.min()), "최대": float(ag.max()),
             "음수 비율(%)": float((ag < 0).mean() * 100)}]), "{:,.1f}"))
        chk("asof 는 음수가 없다 (미래 정보 차단)", int((ag < 0).sum()) == 0)
        chk("asof 는 소급 2년을 넘지 않는다", int(ag.max()) <= 730, f"— 최대 {int(ag.max())}일")
        say(f"\n- 붙은 회계기간이 **다른 행**: {acc['fp_diff']:,} / {acc['n'][0]:,} "
            f"({acc['fp_diff']/acc['n'][0]*100:.1f}%) — 결합 방식이 다르니 당연하다")
        say(f"- 커버리지: equi **{len(dc)/acc['n'][0]*100:.1f}%** vs asof "
            f"**{len(ag)/acc['n'][1]*100:.1f}%**")

    # ---------------------------------------------------------------- v2·v3
    # 열 수는 **실제 parquet 스키마**에서 읽는다 (문서 수치가 아니다)
    for lab, files in (("v2", ["02_pair", "03_firm", "04_group"]),
                       ("v3", ["panel_pair_month", "dim_relationship",
                               "panel_firm_quarter", "panel_firm_origin_hs"])):
        say(f"\n## {lab} — 패널\n")
        rows = []
        for n in files:
            fe, fa = EQ[lab] / f"{n}.parquet", AS[lab] / f"{n}.parquet"
            if not fe.exists() or not fa.exists():
                say(f"- `{n}` {'equi' if not fe.exists() else 'as-of'} 판 없음 — 건너뜀")
                continue
            pe, pa_ = pq.ParquetFile(fe), pq.ParquetFile(fa)
            rows.append({"패널": n, "equi 행": pe.metadata.num_rows,
                         "asof 행": pa_.metadata.num_rows,
                         "행 차이": pa_.metadata.num_rows - pe.metadata.num_rows,
                         "equi 열": len(pe.schema_arrow.names),
                         "asof 열": len(pa_.schema_arrow.names)})
        if rows:
            t = pd.DataFrame(rows)
            say(md(t))
            chk(f"{lab} 패널 행 수 동일", bool((t["행 차이"] == 0).all()))
            chk(f"{lab} 패널 열 수 동일", bool((t["equi 열"] == t["asof 열"]).all()),
                "— 시점 컬럼 이름만 다르고 개수는 같아야 한다")

    # 거래 측정치가 같은지 (v2 04_group 으로 대표 확인)
    fa = AS["v2"] / "04_group.parquet"
    if fa.exists() and (EQ["v2"] / "04_group.parquet").exists():
        c = ["imp_n_ship", "imp_value_usd", "imp_value_within_firm", "imp_value_arms",
             "exp_n_ship", "exp_value_usd"]
        e = pd.read_parquet(EQ["v2"] / "04_group.parquet", columns=c)
        s = pd.read_parquet(fa, columns=c)
        say("\n### v2 `04_group` 거래 측정치 — 재무와 무관하므로 같아야 한다\n")
        say(md(pd.DataFrame([{"항목": x, "equi": float(e[x].sum()), "asof": float(s[x].sum()),
                              "차이": float(s[x].sum() - e[x].sum())} for x in c])))
        chk("v2 거래 측정치 전부 동일",
            all(abs(float(s[x].sum()) - float(e[x].sum())) < 1 for x in c))

    # ---------------------------------------------------------------- 문서 대조
    doc_gate({k: (EQ[k], AS[k]) for k in EQ}, renames)

    say("\n---\n\n## 요약\n")
    r = pd.DataFrame(RES)
    fails = r[r.결과 == "FAIL"]
    say(f"**{len(r)}개 항목 중 {len(r)-len(fails)}개 PASS**"
        + ("" if not len(fails) else "\n\n" + md(fails)))
    if not len(fails):
        say("\n> 두 판은 **선적·관계·패널 구조·문서가 완전히 같고 재무만 다르다.** "
            "PI 판단에 따라 어느 쪽이든 그대로 쓸 수 있다(정본은 as-of).")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"\n→ {OUT}", flush=True)


if __name__ == "__main__":
    main()
