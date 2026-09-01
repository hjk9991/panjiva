# -*- coding: utf-8 -*-
r"""
v4_10_verify_sources.py — 원천 parquet 이 빌드에 들어가도 되는 상태인지 확인한다.

  0. 짝 확인 — imp_ship 이 있는 달은 imp_hs 도 있어야 한다
  1. 전체 디코딩 — 손상 파일 감지 (2026-08-20 동시 write 사고의 교훈: 손상은 조용하다)
  2. Snowflake 월별 건수 대사 — 표준필터 후 count 를 원본과 비교 (허용오차 0.1%:
     피드 백필로 과거 월 건수가 미세하게 자랄 수 있다. 초과 시 FAIL)

FAIL 이 있으면 exit 1. 결과: <out>\10_source_verify.md — 요약줄("N개월 중 M개 PASS") + 표
(기본은 편차>0 또는 FAIL 인 월만, --full-table 이면 전체 월). _manifest.json 에 stage verify_sources 로 기록.

사용:
    python v4_10_verify_sources.py [--out <dir>] [--src <dir>...] [--skip-decode] [--skip-snowflake] [--full-table]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent / "extraction"))
from ex_20260824_import_coverage_by_year import connect_kwargs   # noqa: E402

from v4_common import (DEFAULT_TRADE_SRC, OUT_FULL, discover_months, verify_parquets,  # noqa: E402
                       write_manifest)

SQL = """
select to_char(arrivalDate, 'YYYYMM') as ym, count(*) as n
from panjivaUSImport
where arrivalDate >= '{d0}' and arrivalDate < '{d1}'
  and (conCountry = 'United States' or conCountry is null)
  and (frob is null or frob <> 1)
group by 1 order by 1
"""


def main():
    ap = argparse.ArgumentParser(description="v4 원천 parquet 검증")
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--src", nargs="*", default=None)
    ap.add_argument("--skip-decode", action="store_true")
    ap.add_argument("--skip-snowflake", action="store_true")
    ap.add_argument("--tolerance", type=float, default=0.001)
    ap.add_argument("--full-table", action="store_true", help="월별 대사 표에 전체 월을 적는다 (기본: 편차>0·FAIL 만)")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC

    months = discover_months(src_dirs)
    hs = discover_months(src_dirs, prefix="imp_hs")
    if not months:
        raise SystemExit(f"원천 월 없음: {src_dirs}")
    print(f"원천 {len(months)}개월 ({min(months)}~{max(months)})")
    body = [f"대상: imp_ship {len(months)}개 ({min(months)}~{max(months)}) + imp_hs {len(hs)}개 · "
            f"폴더 {[str(s) for s in src_dirs]}", ""]
    fails = 0
    summary = []

    # 0. 짝 확인 — ship 이 있는 달은 hs 도 있어야 한다
    missing_hs = sorted(set(months) - set(hs))
    if missing_hs:
        fails += 1
        body.append(f"- **FAIL** imp_hs 짝 없음: {missing_hs}")
        summary.append(f"짝 FAIL({len(missing_hs)}개월)")
        print(f"  [FAIL] imp_hs 짝 없음: {missing_hs}")
    else:
        body.append("- [PASS] 모든 월에 imp_ship·imp_hs 짝 존재")
        summary.append("짝 PASS")

    # 1. 디코딩
    if not a.skip_decode:
        print("[1] 전체 디코딩 검증")
        bad = verify_parquets(list(months.values()) + list(hs.values()))
        nf = len(months) + len(hs)
        if bad:
            fails += 1
            body += ["", "**FAIL — 손상 파일:**"] + [f"- `{p}`: {e}" for p, e in bad]
            summary.append(f"디코딩 FAIL({len(bad)}/{nf})")
        else:
            body.append(f"- [PASS] {nf}개 파일 전체 디코딩 성공 (손상 0)")
            summary.append(f"디코딩 {nf}/{nf} PASS")
            print(f"  손상 0 / {nf}개")
    else:
        body.append("- (생략) 디코딩 검증 --skip-decode")
        summary.append("디코딩 생략")

    # 2. Snowflake 대사
    if not a.skip_snowflake:
        print("[2] Snowflake 월별 건수 대사")
        import snowflake.connector
        d0 = f"{min(months)[:4]}-{min(months)[4:]}-01"
        y1, m1 = int(max(months)[:4]), int(max(months)[4:])
        d1 = f"{y1 + (m1 == 12):04d}-{(m1 % 12) + 1:02d}-01"
        with snowflake.connector.connect(**connect_kwargs()) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL.format(d0=d0, d1=d1))
                sf = dict(cur.fetchall())
        rows = []
        for ym, p in months.items():
            local = pq.ParquetFile(p).metadata.num_rows
            remote = int(sf.get(ym, 0))
            diff = abs(local - remote) / max(remote, 1)
            ok = diff <= a.tolerance
            fails += (not ok)
            rows.append((ym, local, remote, diff, ok))
            if not ok:
                print(f"  [FAIL] {ym}: 로컬 {local:,} vs Snowflake {remote:,} ({diff*100:.2f}%)")
        nbad = sum(1 for r in rows if not r[4])
        npass = len(rows) - nbad
        worst = max(rows, key=lambda r: r[3])
        summary.append(f"건수 대사 {npass}/{len(rows)}개월 PASS")
        shown = rows if a.full_table else [r for r in rows if (not r[4]) or r[3] > 0]
        body += ["", f"- 월별 건수 대사 (허용오차 {a.tolerance*100:.1f}%): **{len(rows)}개월 중 {npass}개 PASS**"
                 f"{'' if nbad == 0 else f' · **FAIL {nbad}개월**'} · 최대 편차 {worst[0]} {worst[3]*100:.3f}% · "
                 f"로컬 합 {sum(r[1] for r in rows):,} vs Snowflake 합 {sum(r[2] for r in rows):,}",
                 "", ("전체 월" if a.full_table else "편차>0 또는 FAIL 인 월만 (전체는 --full-table)")
                 + f" — 표 {len(shown)}행", "",
                 "| 월 | 로컬 | Snowflake | 편차 | 판정 |", "|---|---|---|---|---|"]
        for ym, lo, re_, df_, ok in shown:
            body.append(f"| {ym} | {lo:,} | {re_:,} | {df_*100:.3f}% | {'PASS' if ok else '**FAIL**'} |")
        if not shown:
            body.append("| (편차 0 · 전 월 PASS) | | | | |")
        print(f"  {len(rows)}개월 중 {npass}개 PASS · FAIL {nbad}개월 · 최대 편차 {worst[3]*100:.3f}%")
    else:
        body.append("- (생략) Snowflake 대사 --skip-snowflake")
        summary.append("건수 대사 생략")

    lines = [f"# 원천 검증 ({pd.Timestamp.now():%Y-%m-%d %H:%M})", "",
             f"**요약**: {' · '.join(summary)} · FAIL {fails}건", ""] + body
    md_path = out / "10_source_verify.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_manifest(out, "verify_sources", inputs=list(months.values()) + list(hs.values()),
                   outputs=[md_path],
                   extra={"months": len(months), "first": min(months), "last": max(months),
                          "fails": fails, "summary": summary})
    print(f"\n-> {md_path}   FAIL {fails}건")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
