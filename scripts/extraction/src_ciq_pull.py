# -*- coding: utf-8 -*-
r"""
src_ciq_pull.py — v1·v2·v3 공용 CIQ 참조 원천 추출

결정 근거: C:\panjiva\data\staging\source\DECISIONS.md §3 (C-1~C-6)

원칙 — **원천은 넓게, 좁히는 결정은 가공 단계로**:
  재무는 계정을 고르지 않고 **long 형태 그대로** 받는다. wide 피벗(계정 선택)은 가공에서.
  → 나중에 다른 계정이 필요해져도 재추출 없이 가공만 재실행하면 된다.

산출 (C:\panjiva\data\staging\source\ciq_ref\):
  fin_period.parquet        회계기간 메타 (회사×기간, 주기 1·2·10)
  fin_data_YYYY.parquet     재무 값 long (기간×계정) — 연도별 청크
  company.parquet           기업 기준정보 전체 (4,081만 × 23)
  ownership_pit.parquet     소유구조 시점별 전체
  ownership_snapshot.parquet 소유구조 스냅샷 전체
  fx_rate.parquet           환율 (latestSnapFlag=1 만 — 안 걸면 조인 시 6배 증식)
  ref_*.parquet             코드표 5종
  _run_log.md               실행 로그

사용법:
  python scripts\extraction\src_ciq_pull.py                # 전체
  python ... --only company fx                             # 일부만
  python ... --fin-years 2020 2021 2022 2023 2024          # 재무 연도 청크 지정
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector

OUT = Path(r"C:\panjiva\data\staging\source\ciq_ref")
PERIOD_TYPES = "(1, 2, 10)"        # C-1: 연간·분기·반기. YTD/LTM 은 계산 가능하므로 제외

# 재무 값은 6.8억 행이라 연도로 쪼개 받는다(중단·재개 가능, 메모리 안전)
FIN_YEARS_DEFAULT = list(range(1990, 2027))

SQL = {
    # --- 회계기간 메타 (회사 × 기간) ---
    "fin_period": """
select fp.financialPeriodId as financial_period_id,
       fp.companyId         as companyid,
       fp.periodTypeId      as period_type_id,
       pt.periodTypeName    as period_type,
       fp.periodEndDate     as period_end,
       fp.calendarYear      as cal_year,
       fp.calendarQuarter   as cal_quarter,
       fp.fiscalYear        as fiscal_year,
       fp.fiscalQuarter     as fiscal_quarter,
       fp.filingDate        as filing_date,
       fp.currencyId        as currencyid,
       cu.ISOCode           as currency,
       fp.restatementTypeId as restatement_type_id,
       rt.restatementTypeName as restatement_type,
       fp.latestPeriodFlag  as latest_period_flag,
       fi.isRestatementTypeId as is_restatement_type_id,
       fi.filingDate        as instance_filing_date
from ciqLatestInstanceFinPeriod fp
left join ciqPeriodType      pt on pt.periodTypeId = fp.periodTypeId
left join ciqCurrency        cu on cu.currencyId = fp.currencyId
left join ciqRestatementType rt on rt.restatementTypeId = fp.restatementTypeId
left join ciqFinInstance     fi on fi.financialPeriodId = fp.financialPeriodId
                               and fi.latestForFinancialPeriodFlag = 1
where fp.periodTypeId in {pt}
""",
    # --- 기업 기준정보 (전체 23컬럼) ---
    "company": """
select c.companyId as companyid, c.companyName as companyname,
       c.companyTypeId, ct.companyTypeName as company_type,
       c.companyStatusTypeId, cs.companyStatusTypeName as company_status,
       c.simpleIndustryId, si.simpleIndustryDescription as industry,
       c.countryId, g.country, g.isoCountry2 as country_iso2,
       c.stateId, c.incorporationCountryId,
       gi.country as incorporation_country, gi.isoCountry2 as incorporation_iso2,
       c.incorporationStateId,
       c.city, c.streetAddress, c.streetAddress2, c.streetAddress3, c.streetAddress4,
       c.zipCode, c.webPage, c.officePhoneValue, c.officeFaxValue, c.otherPhoneValue,
       c.yearFounded, c.monthFounded, c.dayFounded, c.reportingTemplateTypeId
from ciqCompany c
left join ciqCompanyType       ct on ct.companyTypeId = c.companyTypeId
left join ciqCompanyStatusType cs on cs.companyStatusTypeId = c.companyStatusTypeId
left join ciqSimpleIndustry    si on si.simpleIndustryId = c.simpleIndustryId
left join ciqCountryGeo        g  on g.countryId = c.countryId
left join ciqCountryGeo        gi on gi.countryId = c.incorporationCountryId
""",
    "ownership_pit": """
select companyId as companyid, ultimateParentCompanyId as ultimate_parent_companyid,
       startDate as start_date, endDate as end_date, dateTypeId as date_type_id
from ciqCompanyUltimateParentPIT
""",
    "ownership_snapshot": """
select companyId as companyid, ultimateParentCompanyId as ultimate_parent_companyid
from ciqCompanyUltimateParent
""",
    # C-5: latestSnapFlag=1 필수 — 하루 스냅이 6종이라 안 걸면 조인 시 6배 증식
    "fx_rate": """
select er.currencyId as currencyid, cu.ISOCode as currency,
       er.priceDate as price_date, er.priceClose as fx_per_usd,
       er.carryForwardFlag as carry_forward_flag
from ciqExchangeRate er
left join ciqCurrency cu on cu.currencyId = er.currencyId
where er.latestSnapFlag = 1
""",
    "ref_currency": "select currencyId as currencyid, ISOCode as currency, currencyName as currency_name from ciqCurrency",
    "ref_country": "select countryId as countryid, country, isoCountry2 as iso2, isoCountry3 as iso3 from ciqCountryGeo",
    "ref_industry": "select simpleIndustryId as simple_industry_id, simpleIndustryDescription as industry from ciqSimpleIndustry",
    "ref_company_type": "select companyTypeId as company_type_id, companyTypeName as company_type from ciqCompanyType",
    "ref_company_status": "select companyStatusTypeId as company_status_type_id, companyStatusTypeName as company_status from ciqCompanyStatusType",
}

# 재무 값 long — 연도 청크. 계정을 고르지 않는다(C-1).
SQL_FIN_DATA = """
select d.financialPeriodId as financial_period_id,
       d.dataItemId        as data_item_id,
       d.dataItemValue     as value,
       d.unitTypeId        as unit_type_id
from ciqFinancialData d
join ciqLatestInstanceFinPeriod fp on fp.financialPeriodId = d.financialPeriodId
where fp.periodTypeId in {pt}
  and fp.periodEndDate >= '{y}-01-01' and fp.periodEndDate < '{y1}-01-01'
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


def log_line(path, name, rows, size, t0, note=""):
    with path.open("a", encoding="utf-8") as fh:
        fh.write("| %s | %s | %s | %.1f | %s |\n"
                 % (t0.strftime("%Y-%m-%d %H:%M"), name,
                    "{:,}".format(rows), size / 1e6, note))


def fetch_to_parquet(cur, sql, path):
    """배치로 받아 순차 기록 — 대용량(1억행+)을 메모리에 통째로 올리지 않는다.

    fetch_pandas_all() 은 결과 전체를 조립하므로 연속 메모리 블록을 요구해
    1.6억 행에서 실패했다(RAM 총량 문제가 아니라 파편화 문제).
    fetch_pandas_batches() 는 조각으로 주므로 그때그때 써 내려간다.
    """
    cur.execute(sql)
    writer, schema, total = None, None, 0
    tmp = path.with_suffix(".tmp")
    try:
        for chunk in cur.fetch_pandas_batches():
            chunk.columns = [c.lower() for c in chunk.columns]
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                # 배치마다 decimal 정밀도가 달라질 수 있어(예: decimal(9,6) vs (14,6))
                # 첫 배치 스키마로 고정하고, 숫자형은 넉넉한 타입으로 승격해 둔다.
                schema = pa.schema([
                    f.with_type(pa.float64())
                    if pa.types.is_decimal(f.type) else f
                    for f in tbl.schema
                ]).with_metadata(tbl.schema.metadata)
                writer = pq.ParquetWriter(tmp, schema, compression="snappy")
            writer.write_table(tbl.cast(schema))
            total += len(chunk)
            del chunk, tbl
    finally:
        if writer is not None:
            writer.close()
    if total == 0:
        tmp.unlink(missing_ok=True)
        return 0
    tmp.replace(path)          # 완결된 것만 최종 이름으로 (중단 시 부분 파일이 남지 않음)
    return total


def run_one(cur, name, sql, out_dir, log, force=False):
    p = out_dir / ("%s.parquet" % name)
    if p.exists() and not force:
        print("  %-26s 건너뜀(존재)" % p.name)
        return
    t0 = datetime.now()
    n = fetch_to_parquet(cur, sql, p)
    if n == 0:
        print("  %-26s (0행)" % p.name)
        return
    sz = p.stat().st_size
    print("  %-26s %13s행  %8.1fMB  (%ds)"
          % (p.name, "{:,}".format(n), sz / 1e6, (datetime.now() - t0).seconds))
    log_line(log, p.name, n, sz, t0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(OUT))
    ap.add_argument("--only", nargs="*", help="특정 산출물만 (예: company fx_rate fin_data)")
    ap.add_argument("--fin-years", nargs="*", type=int, default=FIN_YEARS_DEFAULT)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    log = out / "_run_log.md"
    if not log.exists():
        log.write_text("# CIQ 참조 원천 추출 실행 로그 (src_ciq_pull.py)\n\n"
                       "| 실행시각 | 파일 | 행수 | 크기MB | 비고 |\n|---|---|---|---|---|\n",
                       encoding="utf-8")

    targets = args.only or (list(SQL.keys()) + ["fin_data"])
    print("출력: %s\n대상: %s" % (out, ", ".join(targets)))

    conn_kw = connect_kwargs()
    with snowflake.connector.connect(**conn_kw) as conn:
        with conn.cursor() as cur:
            for name in targets:
                if name == "fin_data":
                    continue
                if name not in SQL:
                    print("  ! 알 수 없는 대상: %s" % name)
                    continue
                print("\n[%s]" % name)
                run_one(cur, name, SQL[name].format(pt=PERIOD_TYPES), out, log, args.force)

            if "fin_data" in targets:
                print("\n[fin_data] 재무 값 long — 연도 청크 %d개" % len(args.fin_years))
                for y in args.fin_years:
                    nm = "fin_data_%d" % y
                    p = out / ("%s.parquet" % nm)
                    if p.exists() and not args.force:
                        print("  %-26s 건너뜀(존재)" % p.name)
                        continue
                    t0 = datetime.now()
                    n = fetch_to_parquet(
                        cur, SQL_FIN_DATA.format(pt=PERIOD_TYPES, y=y, y1=y + 1), p)
                    if n == 0:
                        print("  %-26s (0행 — 건너뜀)" % p.name)
                        continue
                    sz = p.stat().st_size
                    print("  %-26s %13s행  %8.1fMB  (%ds)"
                          % (p.name, "{:,}".format(n), sz / 1e6,
                             (datetime.now() - t0).seconds))
                    log_line(log, p.name, n, sz, t0, "재무 long")

    print("\n완료. 실행 로그: %s" % log)


if __name__ == "__main__":
    main()
