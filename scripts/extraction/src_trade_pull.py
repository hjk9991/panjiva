# -*- coding: utf-8 -*-
r"""
src_trade_pull.py — v1·v2·v3 공용 무역 원천 추출 (Panjiva 수입·수출, 월별)

결정 근거: C:\panjiva\data\staging\source\DECISIONS.md  §1~2 (T-1~T-8)
명세     : shared memory\BECRS_Matching_Project\04_2024연간파일럿_통합명세.md §10

원칙 — **원천은 사실만, 판정은 가공 층에서**:
  넣는 것 : *_ciqid_original(crosswalk 조회) · *_up(PIT 조회) · *_ownership_is_fallback
  빼는 것 : relationship · *_match_status · unmatched_reason · self_shipment
            → 판정 규칙이 바뀌어도 원천 재추출 없이 가공만 재실행하면 된다.

산출 (기간·경로는 전부 인자. H1 하드코딩 없음):
  <out>/imp_ship_YYYYMM.parquet   선적 1건 = 1행
  <out>/imp_hs_YYYYMM.parquet     선적 × HS6 = 1행 (균등배분·단일HS 필터용)
  <out>/exp_ship_YYYYMM.parquet   수출 선적 (상대방 식별자 없음 — 구조적)
  <out>/exp_hs_YYYYMM.parquet
  <out>/_run_log.md               월별 실행 로그 (쿼리기간·행수·고유수·크기·시각)

사용법:
  python scripts\extraction\src_trade_pull.py --start 2024-01-01 --end 2025-01-01
  python ... --months 202407 202408        # 특정 월만
  python ... --include conFullAddress      # 기본 제외 컬럼 되살리기
  python ... --exclude carrier vessel      # 추가 제외
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import snowflake.connector

OUT_DEFAULT = Path(r"C:\panjiva\data\staging\source\trade_2024")

# T-2 기본 제외: 긴 텍스트(압축 불가) · 기존 컬럼과 중복 표기 · 피드 내부값
DEFAULT_EXCLUDE = {
    "conFullAddress", "shpFullAddress", "conRoute", "shpRoute",
    "weightOriginalFormat", "conOriginalFormat", "shpOriginalFormat",
    "notifyParty", "pacVerToFeedPop",
}

# T-1: 전체 컬럼이 기본. Snowflake 실측 스키마 순서 그대로.
IMP_ALL = [
    "panjivaRecordId", "billOfLadingNumber", "arrivalDate", "conName", "conFullAddress",
    "conRoute", "conCity", "conStateRegion", "conPostalCode", "conCountry", "conPanjivaId",
    "conOriginalFormat", "shpName", "shpFullAddress", "shpRoute", "shpCity",
    "shpStateRegion", "shpPostalCode", "shpCountry", "shpPanjivaId", "shpOriginalFormat",
    "carrier", "notifyParty", "notifyPartyScac", "billOfLadingType",
    "masterBillOfLadingNumber",   # T-3 보존: 대형 통합선적 식별 → TEU 결측·다중HS 해석용
    "shpmtOrigin", "shpmtDestination", "shpmtDestinationRegion", "portOfUnlading",
    "portOfUnladingRegion", "portOfLading", "portOfLadingRegion", "portOfLadingCountry",
    "placeOfReceipt", "transportMethod", "vessel", "vesselVoyageId", "vesselIMO",
    "isContainerized", "volumeTEU", "quantity", "measurement", "weightKg", "weightT",
    "weightOriginalFormat", "valueOfGoodsUSD", "frob", "manifestNumber", "inBondCode",
    "numberOfContainers", "hasLCL", "fileDate", "pacVerToFeedPop",
]
EXP_ALL = [
    "panjivaRecordId", "billOfLadingNumber", "shpmtDate", "shpName", "shpFullAddress",
    "shpRoute", "shpCity", "shpStateRegion", "shpPostalCode", "shpCountry", "shpPanjivaId",
    "shpOriginalFormat", "carrier", "shpmtDestination", "portOfLading", "portOfLadingRegion",
    "portOfLadingCountry", "portOfUnlading", "portOfUnladingRegion", "portOfUnladingCountry",
    "placeOfReceipt", "vesselCountry", "vessel", "transportFlightCode", "isContainerized",
    "volumeTEU", "itemQuantity", "weightKg", "weightT", "weightOriginalFormat",
    "valueOfGoodsUSD", "equipmentType", "equipmentDimensions", "dividedLCL", "fileDate",
    "pacVerToFeedPop",
]

# T-8 표준필터. 수출 테이블에는 frob 컬럼 자체가 없고 적재항 100% 미국이라 필터 불필요.
IMP_FILTER = ("      and (i.conCountry = 'United States' or i.conCountry is null)\n"
              "      and (i.frob is null or i.frob <> 1)")

# T-4 HS 대표값 — 자식 값은 4,000자 초과 시 토큰 중간이 잘려 여러 행이 되므로
#     hsCodeId(저장 순서)로 '구분자 없이' 이어붙여야 코드가 복원된다.
HS_AGG = """hs_agg as (
    select h.panjivaRecordId,
           listagg(h.hsCode, '') within group (order by h.hsCodeId) as allhs
    from {hs_table} h join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
)"""

HS_REST = """hs_rep as (
    select panjivaRecordId,
           replace(coalesce(
                regexp_substr(allhs, 'Classified: ?([0-9][0-9.]*)', 1, 1, 'e', 1),
                regexp_substr(allhs, 'Parsed: ?([0-9][0-9.]*)',     1, 1, 'e', 1),
                regexp_substr(allhs, 'Manual: ?([0-9][0-9.]*)',     1, 1, 'e', 1)
           ), '.', '') as hs_raw
    from hs_agg
),
hs_cnt as (
    select a.panjivaRecordId,
           count(distinct left(replace(regexp_substr(t.value,'[0-9][0-9.]*'),'.',''),6)) as n_hs6
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
    group by a.panjivaRecordId
)"""

# crosswalk: activeFlag=1 만, identifierValue 당 1행(낮은 companyId 승자)
XR_CTE = """xr as (
    select identifierValue, companyId from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
),
pit_wk as (
    select companyId, ultimateParentCompanyId, startDate, endDate
    from ciqCompanyUltimateParentPIT
    where startDate <= '{date_end}' and endDate >= '{date_start}'
)"""

NL = "\n"


def ship_sql(direction, cols, date_start, date_end):
    """선적 층. 수입은 양측, 수출은 화주만 crosswalk·PIT 조인."""
    imp = direction == "imp"
    tbl = "panjivaUSImport" if imp else "panjivaUSExport"
    hs_tbl = "panjivaUSImpHSCode" if imp else "panjivaUSExpHSCode"
    datecol = "arrivalDate" if imp else "shpmtDate"
    sides = [("con", "conPanjivaId"), ("shp", "shpPanjivaId")] if imp else [("shp", "shpPanjivaId")]

    sel = (",%s           " % NL).join("i." + c for c in cols)
    joins, outs = [], []
    for pfx, idcol in sides:
        joins.append(
            "    left join xr x{p} on x{p}.identifierValue = b.{i}{nl}"
            "    left join pit_wk p{p} on p{p}.companyId = x{p}.companyId{nl}"
            "                         and b.{d} >= p{p}.startDate and b.{d} <= p{p}.endDate{nl}"
            "    left join ciqCompanyUltimateParent s{p} on s{p}.companyId = x{p}.companyId"
            .format(p=pfx, i=idcol, d=datecol, nl=NL))
        outs.append(
            "       x{p}.companyId as {p}_ciqid_original,{nl}"
            "       coalesce(p{p}.ultimateParentCompanyId,"
            " s{p}.ultimateParentCompanyId) as {p}_up,{nl}"
            "       iff(x{p}.companyId is not null"
            " and p{p}.ultimateParentCompanyId is null, 1, 0) as {p}_ownership_is_fallback"
            .format(p=pfx, nl=NL))

    return """
with base as (
    select {sel}
    from {tbl} i
    where i.{datecol} >= '{d0}' and i.{datecol} < '{d1}'
{filt}
),
{hs_agg},
{hs_rest},
{xr}
select b.*,
       left(r.hs_raw, 6) as hs6,
       left(r.hs_raw, 2) as hs2,
       length(r.hs_raw)  as hs_ndigits,
       coalesce(c.n_hs6, 0) as n_hs6,
       iff(coalesce(c.n_hs6, 0) > 1, 1, 0) as hs_is_multi,
{outs}
from base b
    left join hs_rep r on r.panjivaRecordId = b.panjivaRecordId
    left join hs_cnt c on c.panjivaRecordId = b.panjivaRecordId
{joins}
""".format(sel=sel, tbl=tbl, datecol=datecol, d0=date_start, d1=date_end,
           filt=IMP_FILTER if imp else "",
           hs_agg=HS_AGG.format(hs_table=hs_tbl), hs_rest=HS_REST,
           xr=XR_CTE.format(date_start=date_start, date_end=date_end),
           outs=(",%s" % NL).join(outs), joins=NL.join(joins))


def hs_sql(direction, date_start, date_end):
    """HS 자식 층 — 선적 × 서로 다른 HS6 하나씩. 배분 계산용 토큰 정보 포함."""
    imp = direction == "imp"
    tbl = "panjivaUSImport" if imp else "panjivaUSExport"
    hs_tbl = "panjivaUSImpHSCode" if imp else "panjivaUSExpHSCode"
    datecol = "arrivalDate" if imp else "shpmtDate"
    return """
with base as (
    select i.panjivaRecordId
    from {tbl} i
    where i.{datecol} >= '{d0}' and i.{datecol} < '{d1}'
{filt}
),
{hs_agg},
tok as (
    select a.panjivaRecordId,
           replace(regexp_substr(t.value, '[0-9][0-9.]*'), '.', '') as code,
           t.index as tok_idx
    from hs_agg a, lateral split_to_table(a.allhs, ';') t
    where regexp_like(t.value, '.*(Classified|Parsed|Manual): ?[0-9].*')
)
select panjivaRecordId, left(code, 6) as hs6,
       count(*) as n_tokens,
       min(tok_idx) as first_tok_idx,
       min(length(code)) as hs_ndigits_min
from tok
where length(code) >= 2
group by 1, 2
""".format(tbl=tbl, datecol=datecol, d0=date_start, d1=date_end,
           filt=IMP_FILTER if imp else "", hs_agg=HS_AGG.format(hs_table=hs_tbl))


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


def month_bounds(start, end):
    """[start, end) 를 월 경계로 쪼갠다 — 기간이 전부 인자라 H1 하드코딩이 없다."""
    rng = pd.date_range(start, end, freq="MS")
    return [(d.strftime("%Y%m"), d.strftime("%Y-%m-%d"),
             (d + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")) for d in rng[:-1]]


def save(df, path):
    df.columns = [c.lower() for c in df.columns]
    for c in ("hs6", "hs2"):                    # 코드성 컬럼은 문자열 (앞자리 0 보존)
        if c in df.columns:
            s = df[c].astype("string").str.strip()
            df[c] = s.mask(s.eq("") | s.isna(), pd.NA)
    for c in df.columns:                        # 결측 있는 ID 는 Int64 (소수점 표기 방지)
        if c.endswith(("panjivaid", "_ciqid_original", "_up", "recordid")):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    # 원자적 쓰기: .tmp 에 다 쓴 뒤 rename — 도중에 죽거나 겹쳐도 깨진 .parquet 이 남지 않는다
    tmp = path.parent / (path.name + ".tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    os.replace(tmp, path)
    return path.stat().st_size


def acquire_lock(out):
    """같은 출력 폴더에 대한 동시 실행 차단 — 2026-08-20 동시 write 로 5개 파일이 손상된
    사고의 재발 방지. 락 파일에 PID 를 남기고, 살아있는 PID 면 즉시 중단한다."""
    import psutil
    lock = out / "_pull.lock"
    if lock.exists():
        try:
            other = int(lock.read_text(encoding="utf-8").split()[0])
        except (ValueError, IndexError):
            other = None
        if other and other != os.getpid() and psutil.pid_exists(other):
            raise SystemExit(
                f"중단: {lock} — PID {other} 가 같은 폴더에 추출 중입니다. "
                "끝나기를 기다리거나, 죽은 프로세스가 확실하면 락 파일을 지우고 재실행하세요.")
    lock.write_text(f"{os.getpid()} {datetime.now():%Y-%m-%d %H:%M:%S}", encoding="utf-8")
    return lock


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-01-01", help="이 날짜 미만(exclusive)")
    ap.add_argument("--months", nargs="*", help="YYYYMM 만 골라서")
    ap.add_argument("--out-dir", default=str(OUT_DEFAULT))
    ap.add_argument("--include", nargs="*", default=[], help="기본 제외 컬럼 되살리기")
    ap.add_argument("--exclude", nargs="*", default=[], help="추가 제외 컬럼")
    ap.add_argument("--directions", nargs="*", default=["imp", "exp"])
    ap.add_argument("--force", action="store_true", help="기존 파일 덮어쓰기")
    ap.add_argument("--print-sql", action="store_true")
    ap.add_argument("--low-priority", action="store_true",
                    help="공용 머신 배려 — 프로세스 우선순위를 Below Normal 로")
    args = ap.parse_args()

    if args.low_priority:
        import psutil
        psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    excl = (DEFAULT_EXCLUDE - set(args.include)) | set(args.exclude)
    keep = {"imp": [c for c in IMP_ALL if c not in excl],
            "exp": [c for c in EXP_ALL if c not in excl]}

    months = month_bounds(args.start, args.end)
    if args.months:
        months = [m for m in months if m[0] in set(args.months)]
    print("기간 %s ~ %s · %d개월 · 출력 %s" % (args.start, args.end, len(months), out))
    print("컬럼: 수입 %d/%d · 수출 %d/%d  (제외 %d개: %s)"
          % (len(keep["imp"]), len(IMP_ALL), len(keep["exp"]), len(EXP_ALL),
             len(excl), ", ".join(sorted(excl))))

    if args.print_sql:
        print(ship_sql("imp", keep["imp"], months[0][1], months[0][2]))
        return

    log = out / "_run_log.md"
    if not log.exists():
        log.write_text("# 무역 원천 추출 실행 로그 (src_trade_pull.py)\n\n"
                       "| 실행시각 | 파일 | 쿼리기간 | 행수 | 고유선적 | 크기MB |\n"
                       "|---|---|---|---|---|---|\n", encoding="utf-8")

    conn_kw = connect_kwargs()
    lock = acquire_lock(out)
    try:
        run_months(months, args, keep, out, log, conn_kw)
    finally:
        lock.unlink(missing_ok=True)
    print("\n완료. 실행 로그: %s" % log)


def run_months(months, args, keep, out, log, conn_kw):
    for ym, d0, d1 in months:
        print("\n[%s] %s ~ %s" % (ym, d0, d1))
        with snowflake.connector.connect(**conn_kw) as conn:
            with conn.cursor() as cur:
                for direction in args.directions:
                    for kind, sql in (("ship", ship_sql(direction, keep[direction], d0, d1)),
                                      ("hs", hs_sql(direction, d0, d1))):
                        p = out / ("%s_%s_%s.parquet" % (direction, kind, ym))
                        if p.exists() and not args.force:
                            print("  %-28s 건너뜀(존재)" % p.name)
                            continue
                        t0 = datetime.now()
                        cur.execute(sql)
                        df = cur.fetch_pandas_all()
                        size = save(df, p)
                        uq = (df["panjivarecordid"].nunique()
                              if "panjivarecordid" in df.columns else len(df))
                        print("  %-28s %9s행  %7.1fMB  (%ds)"
                              % (p.name, "{:,}".format(len(df)), size / 1e6,
                                 (datetime.now() - t0).seconds))
                        with log.open("a", encoding="utf-8") as fh:
                            fh.write("| %s | %s | %s~%s | %s | %s | %.1f |\n"
                                     % (t0.strftime("%Y-%m-%d %H:%M"), p.name, d0, d1,
                                        "{:,}".format(len(df)), "{:,}".format(uq), size / 1e6))


if __name__ == "__main__":
    main()
