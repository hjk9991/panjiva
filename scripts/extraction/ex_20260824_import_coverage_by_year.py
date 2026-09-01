# -*- coding: utf-8 -*-
r"""
ex_20260824_import_coverage_by_year.py — panjivaUSImport 의 **연도별 실제 커버리지** 확인.

배경: 팀 문서(`DATA_GUIDE.md`)는 "US Import 2.33억 건(2007-01~현재)" 이라 적고 있는데,
프로브에서 2007-01 이 **1,838건**, 2010-01 이 **605,906건** 으로 330배 차이가 났다.
v4 를 전 기간으로 넓히기 전에 **언제부터 데이터가 실제로 차 있는지**를 확정해야 한다.

표준필터가 과거 연도를 죽이는 것인지 원본이 비어 있는 것인지 구분하기 위해
**필터 전/후를 나란히** 센다. 집계 쿼리라 데이터 전송이 없다.

산출: <out>\00_import_coverage_by_year.csv (기본 out = v4_pairhs_full — v4_00_period_probe.py 가 읽는다)

사용:
    python ex_20260824_import_coverage_by_year.py                       # 2005-01-01 ~ 내년 1월 1일
    python ex_20260824_import_coverage_by_year.py --start 2007-01-01 --end 2026-01-01 --out <dir>
    python ex_20260824_import_coverage_by_year.py --print-sql           # 실행 없이 SQL 만
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

OUT_DEFAULT = Path(r"C:\panjiva\data\staging\v4_pairhs_full")

SQL = """
select year(arrivalDate) as yr,
       count(*)                                                   as raw_rows,
       count_if(conCountry = 'United States' or conCountry is null) as pass_country,
       count_if(frob is null or frob <> 1)                         as pass_frob,
       count_if((conCountry = 'United States' or conCountry is null)
                and (frob is null or frob <> 1))                   as pass_both,
       count_if(conPanjivaId is not null)                          as has_con_id,
       count_if(shpPanjivaId is not null)                          as has_shp_id
from panjivaUSImport
where arrivalDate >= '{d0}' and arrivalDate < '{d1}'
group by 1 order by 1
"""


def connect_kwargs():
    envp = Path.home() / ".snowflake.env"
    for line in envp.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return dict(user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
                account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
                schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"))


def main():
    ap = argparse.ArgumentParser(description="panjivaUSImport 연도별 커버리지 집계 (Snowflake)")
    ap.add_argument("--out", default=str(OUT_DEFAULT), help="csv 를 쓸 폴더")
    ap.add_argument("--start", default="2005-01-01", help="arrivalDate >= (기본 2005-01-01)")
    ap.add_argument("--end", default=f"{datetime.now().year + 1}-01-01",
                    help="arrivalDate < (기본 내년 1월 1일 = 올해까지 전부)")
    ap.add_argument("--print-sql", action="store_true", help="실행하지 않고 SQL 만 출력")
    a = ap.parse_args()
    for d in (a.start, a.end):
        datetime.strptime(d, "%Y-%m-%d")          # 형식 검증 (SQL 리터럴로 들어간다)
    sql = SQL.format(d0=a.start, d1=a.end)
    if a.print_sql:
        print(sql)
        return
    import snowflake.connector
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    with snowflake.connector.connect(**connect_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            d = cur.fetch_pandas_all()
    d.columns = [c.lower() for c in d.columns]
    d["filter_keeps_pct"] = (d.pass_both / d.raw_rows * 100).round(1)
    d["con_id_pct"] = (d.has_con_id / d.raw_rows * 100).round(1)
    d["shp_id_pct"] = (d.has_shp_id / d.raw_rows * 100).round(1)
    d.to_csv(out / "00_import_coverage_by_year.csv", index=False, encoding="utf-8-sig")
    print(d.to_string(index=False))
    print(f"\n합계 raw {d.raw_rows.sum():,} · 필터후 {d.pass_both.sum():,}")
    print(f"-> {out / '00_import_coverage_by_year.csv'}")


if __name__ == "__main__":
    main()
