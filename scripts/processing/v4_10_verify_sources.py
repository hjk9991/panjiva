# -*- coding: utf-8 -*-
r"""
v4_10_verify_sources.py — 원천 parquet 이 빌드에 들어가도 되는 상태인지 확인한다.

  1. 전체 디코딩 — 손상 파일 감지 (2026-08-20 동시 write 사고의 교훈: 손상은 조용하다)
  2. Snowflake 월별 건수 대사 — 표준필터 후 count 를 원본과 비교 (허용오차 0.1%:
     피드 백필로 과거 월 건수가 미세하게 자랄 수 있다. 초과 시 FAIL)

FAIL 이 있으면 exit 1. 결과: <out>\10_source_verify.md
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent / "extraction"))
from ex_20260824_import_coverage_by_year import connect_kwargs   # noqa: E402

from v4_common import DEFAULT_TRADE_SRC, OUT_FULL, discover_months, verify_parquets  # noqa: E402

SQL = """
select to_char(arrivalDate, 'YYYYMM') as ym, count(*) as n
from panjivaUSImport
where arrivalDate >= '{d0}' and arrivalDate < '{d1}'
  and (conCountry = 'United States' or conCountry is null)
  and (frob is null or frob <> 1)
group by 1 order by 1
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUT_FULL))
    ap.add_argument("--src", nargs="*", default=None)
    ap.add_argument("--skip-decode", action="store_true")
    ap.add_argument("--skip-snowflake", action="store_true")
    ap.add_argument("--tolerance", type=float, default=0.001)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    src_dirs = [Path(s) for s in a.src] if a.src else DEFAULT_TRADE_SRC

    months = discover_months(src_dirs)
    hs = discover_months(src_dirs, prefix="imp_hs")
    print(f"원천 {len(months)}개월 ({min(months)}~{max(months)})")
    lines = [f"# 원천 검증 ({pd.Timestamp.now():%Y-%m-%d %H:%M})", "",
             f"대상: imp_ship {len(months)}개 + imp_hs {len(hs)}개 · 폴더 {[str(s) for s in src_dirs]}", ""]
    fails = 0

    # 짝 확인 — ship 이 있는 달은 hs 도 있어야 한다
    missing_hs = sorted(set(months) - set(hs))
    if missing_hs:
        fails += 1
        lines.append(f"**FAIL** imp_hs 짝 없음: {missing_hs}")
        print(f"  [FAIL] imp_hs 짝 없음: {missing_hs}")
    else:
        lines.append(f"- [PASS] 모든 월에 imp_ship·imp_hs 짝 존재")

    if not a.skip_decode:
        print("[1] 전체 디코딩 검증")
        bad = verify_parquets(list(months.values()) + list(hs.values()))
        if bad:
            fails += 1
            lines += ["", "**FAIL — 손상 파일:**"] + [f"- `{p}`: {e}" for p, e in bad]
        else:
            lines.append(f"- [PASS] {len(months) + len(hs)}개 파일 전체 디코딩 성공 (손상 0)")
            print(f"  손상 0 / {len(months) + len(hs)}개")

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
        worst = max(rows, key=lambda r: r[3])
        lines += ["", f"- 월별 건수 대사 (허용오차 {a.tolerance*100:.1f}%): "
                  f"{'[PASS]' if nbad == 0 else f'**FAIL {nbad}개월**'} · "
                  f"최대 편차 {worst[0]} {worst[3]*100:.3f}%",
                  "", "| 월 | 로컬 | Snowflake | 편차 | 판정 |", "|---|---|---|---|---|"]
        for ym, lo, re_, df_, ok in rows:
            if not ok or df_ > 0:
                lines.append(f"| {ym} | {lo:,} | {re_:,} | {df_*100:.3f}% | "
                             f"{'PASS' if ok else '**FAIL**'} |")
        print(f"  편차>0 인 월만 표에 기록 · FAIL {nbad}개월 · 최대 편차 {worst[3]*100:.3f}%")

    (out / "10_source_verify.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n-> {out / '10_source_verify.md'}   FAIL {fails}건")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
