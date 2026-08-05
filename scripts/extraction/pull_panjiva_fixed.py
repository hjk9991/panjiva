r"""
pull_panjiva_fixed.py — A 서버 팀 표준 추출 스크립트(`scripts/extraction/pull_panjiva.py`)의 교정본.

⚠️ 이 파일은 **로컬 검토용 초안**이다. A 서버 반영 여부는 별도 판단.
   결함 근거와 실측치는 같은 폴더의 REVIEW_A-server.md 참조.

원본 대비 고친 것 (요약 — 상세는 REVIEW_A-server.md):
  1. HS 필터가 `hsCode LIKE '85%'` 였다 → **항상 0행**. 실제 값은 'Classified: 8528.51' 형태.
     → 접두어 3종(Classified/Parsed/Manual)을 인식하는 정규식 + EXISTS 필터로 교체.
  2. 원산지를 `shpCountry`(화주 주소지국, 결측 31%)로 걸었다 → 한국산의 49%가 조용히 누락.
     → `--origin`(shpmtOrigin, 결측 0.08%)과 `--shipper-country`(shpCountry)를 **분리**해 의도를 강제.
  3. 자식테이블(HS)을 그대로 LEFT JOIN해 금액·TEU를 내려받았다 → 선적 1건이 여러 행이 되어
     downstream 합계가 중복(실측 TEU ×3). → 선적당 **대표 HS 1개**로 1:1 유지.
  4. `panjivaCompanyCrossRef` 조인에 `activeFlag=1`도 1:N 정리도 없었다 → 행 증식.
     → activeFlag=1 + identifierValue당 1행으로 축약. 모회사(ultimateParent)도 함께 부착.
  5. 미국 실착 화물 필터가 없었다 → `conCountry='United States' or null` + `frob<>1` 추가.
  6. `cur.fetchall()` → Decimal이 object로 굳어 parquet이 커진다. `fetch_pandas_all()`(Arrow)로 교체.
  7. 컬럼명이 대문자로 저장됐다 → 저장 직전 소문자화.
  8. `--out` 기본값이 상대경로라 git 폴더에 데이터가 생겼다 → 필수 인자로 변경.

사용법 (A의 VS Code 터미널, 팀 공용 환경):

    # 1) 최초 1회: %USERPROFILE%\.snowflake.env 에 접속정보 (docs/snowflake-data-workflow.md §2)

    # 2) 연결·로직 확인 — 1주치만 (LIMIT이 아니라 기간을 줄인다: 필터 정합성까지 같이 확인됨)
    python pull_panjiva_fixed.py --smoke --origin "South Korea" --hs-prefix 85 `
        --out C:\panjiva\data\staging\_smoke.parquet

    # 3) 실제 추출: 한국산 전기기기(HS 85), 2020~2024
    python pull_panjiva_fixed.py --year-start 2020 --year-end 2024 `
        --origin "South Korea" --hs-prefix 85 `
        --out C:\panjiva\data\staging\us_imports_kr_hs85_2020_2024.parquet

의존성: snowflake-connector-python, pandas, pyarrow, python-dotenv
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# 1. 접속 설정 — 비밀번호는 홈 폴더의 .env 에서만 읽는다(git 유출 방지)
# ---------------------------------------------------------------------------
load_dotenv(Path.home() / ".snowflake.env")
load_dotenv(Path.cwd() / ".env", override=True)   # 작업 폴더의 .env 가 있으면 덮어씀(단발 실험용)

for _k in ("SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
    if _k not in os.environ:
        sys.exit(f"{_k} 가 없습니다. 홈 폴더의 .snowflake.env 를 확인하세요 "
                 f"(docs/snowflake-data-workflow.md §2).")

CONN_KW = dict(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
)

# HS 코드가 자식 테이블에 적히는 형태. 실측(10만행 샘플): Classified 78.2% · Parsed 20.0% ·
# Manual 희소 · NULL 1.3%. 셋 다 유효한 코드이므로 전부 인식해야 한다.
HS_PREFIXES = "(Classified|Parsed|Manual)"


def hs_regex(prefix: str) -> str:
    """HS 접두 자릿수(2~6)를 실제 저장 형태('8528.51')에 맞는 정규식으로.
    4자리 뒤에 점이 올 수 있으므로 그 자리에 [.]? 를 끼운다."""
    p = "".join(ch for ch in prefix if ch.isdigit())
    if not p:
        raise ValueError(f"HS 접두어는 숫자여야 합니다: {prefix!r}")
    if len(p) > 6:
        raise ValueError("Panjiva HS는 6자리까지입니다(실측 Classified의 99.9%가 6자리)")
    body = p if len(p) <= 4 else p[:4] + "[.]?" + p[4:]
    return f"{HS_PREFIXES}: ?{body}"


# ---------------------------------------------------------------------------
# 2. 쿼리 — 읽기전용 공유 DB이므로 자기완결 CTE 하나로 끝낸다(임시테이블 생성 불가)
# ---------------------------------------------------------------------------
QUERY = """
with base as (
    -- 선적 1건 = 1행. 이 층에서만 금액·TEU·중량을 다룬다(자식 조인 전).
    select i.panjivaRecordId, i.arrivalDate, i.fileDate,
           i.conPanjivaId, i.conName, i.conOriginalFormat, i.conCountry,
           i.shpPanjivaId, i.shpName, i.shpCountry,
           i.shpmtOrigin, i.portOfLading, i.portOfUnlading,
           i.weightKg, i.valueOfGoodsUSD, i.volumeTEU, i.numberOfContainers
    from panjivaUSImport i
    where i.arrivalDate >= '{date_start}' and i.arrivalDate < '{date_end}'
      and (i.conCountry = 'United States' or i.conCountry is null)   -- 미국 실착 화물만
      {frob_clause}                                                  -- 타국행 통과화물 제외
      {origin_clause}
      {shipper_country_clause}
      {hs_exists_clause}
),
hs_agg as (
    -- 자식 값은 한 칸에 ';' 로 이어붙어 있고, 길면 여러 행으로 잘린다(토큰 중간이 끊김).
    -- 그래서 반드시 listagg 로 순서대로 재결합한 뒤 파싱한다.
    select h.panjivaRecordId,
           listagg(h.hsCode, '|') within group (order by h.hsCode) as allhs
    from panjivaUSImpHSCode h
    join base b on b.panjivaRecordId = h.panjivaRecordId
    group by h.panjivaRecordId
),
hs_rep as (
    -- 선적당 대표 HS 1개: 첫 Classified → 없으면 Parsed → 없으면 Manual (1:1 유지)
    select panjivaRecordId,
           left(replace(coalesce(
                regexp_substr(allhs, 'Classified: ?([0-9.]+)', 1, 1, 'e', 1),
                regexp_substr(allhs, 'Parsed: ?([0-9.]+)',     1, 1, 'e', 1),
                regexp_substr(allhs, 'Manual: ?([0-9.]+)',     1, 1, 'e', 1)
           ), '.', ''), 6) as hs6
    from hs_agg
),
hs_cnt as (
    -- 선적에 실제로 몇 개의 서로 다른 HS6 이 있는지 (다중-HS 선적 식별용)
    select a.panjivaRecordId, count(distinct replace(f.value::string, '.', '')) as n_hs6
    from hs_agg a,
         lateral flatten(input => regexp_substr_all(
             a.allhs, '(Classified|Parsed|Manual): ?([0-9]{{4}}[.]?[0-9]{{2}})', 1, 1, 'e', 2)) f
    group by a.panjivaRecordId
),
xr as (
    -- Panjiva 기업 ID -> CIQ companyId. activeFlag=1 만. primaryFlag 로 거르면 안 된다(6.8%만 1).
    -- identifierValue 당 1행으로 축약해 조인이 행을 늘리지 않게 한다.
    select identifierValue, companyId
    from panjivaCompanyCrossRef
    where activeFlag = 1 and identifierValue is not null
    qualify row_number() over (partition by identifierValue order by companyId) = 1
)
select b.panjivaRecordId, b.arrivalDate, b.fileDate,
       b.conPanjivaId, b.conName, b.conOriginalFormat, b.conCountry,
       b.shpPanjivaId, b.shpName, b.shpCountry,
       b.shpmtOrigin, b.portOfLading, b.portOfUnlading,
       b.weightKg, b.valueOfGoodsUSD, b.volumeTEU, b.numberOfContainers,
       r.hs6,
       left(r.hs6, 2)                as hs2,
       coalesce(c.n_hs6, 0)          as n_hs6,
       xc.companyId                  as con_companyId,
       upc.ultimateParentCompanyId   as con_parentId,
       xs.companyId                  as shp_companyId,
       ups.ultimateParentCompanyId   as shp_parentId
from base b
left join hs_rep r  on r.panjivaRecordId = b.panjivaRecordId
left join hs_cnt c  on c.panjivaRecordId = b.panjivaRecordId
left join xr  xc    on xc.identifierValue = b.conPanjivaId
left join xr  xs    on xs.identifierValue = b.shpPanjivaId
left join ciqCompanyUltimateParent upc on upc.companyId = xc.companyId
left join ciqCompanyUltimateParent ups on ups.companyId = xs.companyId
"""


def build_sql(date_start, date_end, origin, shipper_country, hs_prefix, keep_frob):
    def lit(s):
        return s.replace("'", "''")

    return QUERY.format(
        date_start=date_start,
        date_end=date_end,
        frob_clause="" if keep_frob else "and (i.frob is null or i.frob <> 1)",
        origin_clause=f"and i.shpmtOrigin = '{lit(origin)}'" if origin else "",
        shipper_country_clause=(
            f"and i.shpCountry = '{lit(shipper_country)}'" if shipper_country else ""),
        hs_exists_clause=(
            "and exists (select 1 from panjivaUSImpHSCode h "
            f"where h.panjivaRecordId = i.panjivaRecordId "
            f"and regexp_like(h.hsCode, '.*{hs_regex(hs_prefix)}.*'))" if hs_prefix else ""),
    )


# ---------------------------------------------------------------------------
# 3. 실행
# ---------------------------------------------------------------------------
def run_query(sql: str) -> pd.DataFrame:
    """Arrow 경로로 받는다. fetchall() 은 Decimal 을 object 로 만들어 parquet 을 부풀린다."""
    with snowflake.connector.connect(**CONN_KW) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]   # Snowflake 는 대문자로 돌려준다
    return df


def tidy(df: pd.DataFrame) -> pd.DataFrame:
    """저장 직전 타입 정리. parquet 은 '쓸 때의 타입'을 그대로 굳히므로 여기가 마지막 방어선."""
    for c in ("hs6", "hs2"):
        if c in df.columns:
            # 코드성 컬럼은 반드시 문자열로. 정수로 저장하면 앞자리 0이 영구 소실된다.
            n = 6 if c == "hs6" else 2
            df[c] = df[c].astype("string").str.strip().str.zfill(n)
            df.loc[df[c].isna() | (df[c] == ""), c] = pd.NA
    for c in ("weightkg", "valueofgoodsusd", "volumeteu", "numberofcontainers"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # ID는 결측 때문에 float 으로 내려온다. 그대로 두면 조인 키가 '432056386.0' 이 되고
    # 큰 값에서 정밀도도 잃는다 → 결측 허용 정수형(Int64)으로 고정.
    for c in ("panjivarecordid", "conpanjivaid", "shppanjivaid",
              "con_companyid", "con_parentid", "shp_companyid", "shp_parentid", "n_hs6"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("arrivaldate", "filedate"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def report(df: pd.DataFrame) -> None:
    """검수용 요약. '행이 0개'와 '중복이 생겼다'를 여기서 잡는다."""
    n = len(df)
    print(f"\n[검수] {n:,} 행")
    if n == 0:
        print("  ⚠️ 0행이다. 필터를 하나씩 빼면서 어디서 걸리는지 확인할 것.")
        return
    dup = n - df["panjivarecordid"].nunique()
    print(f"  선적 중복      : {dup:,}  (0이어야 정상 — 0이 아니면 자식 조인이 행을 늘린 것)")
    print(f"  기간           : {df['arrivaldate'].min():%Y-%m-%d} ~ {df['arrivaldate'].max():%Y-%m-%d}")
    if "hs6" in df:
        print(f"  HS6 결측       : {df['hs6'].isna().mean():.1%}")
        print(f"  다중-HS 선적   : {(df['n_hs6'] > 1).mean():.1%}  (금액을 HS별로 쪼갤 수 없는 건들)")
    for col, label in (("con_companyid", "수입자"), ("shp_companyid", "수출자")):
        if col in df:
            print(f"  CIQ 매칭 {label} : {df[col].notna().mean():.1%}")
    print(f"  금액 결측      : {df['valueofgoodsusd'].isna().mean():.1%}  "
          f"(※ valueOfGoodsUSD 는 Panjiva 추정치 — 금액 기반 수치엔 각주 필요)")


def main() -> None:
    p = argparse.ArgumentParser(description="Panjiva 미국 수입 선적 추출 (parquet)")
    p.add_argument("--year-start", type=int, default=2024)
    p.add_argument("--year-end", type=int, default=2024, help="이 해의 12월 31일까지 포함")
    p.add_argument("--origin", default=None, metavar="COUNTRY",
                   help="화물 원산지국 shpmtOrigin (결측 0.08%%). '한국산 수입' 은 이것")
    p.add_argument("--shipper-country", default=None, metavar="COUNTRY",
                   help="화주 소재국 shpCountry (결측 31%%). 원산지와 다른 개념이니 용도를 확인할 것")
    p.add_argument("--hs-prefix", default=None,
                   help="HS 앞 2~6자리. 예: 85 = 전기기기. 생략하면 전 품목")
    p.add_argument("--keep-frob", action="store_true",
                   help="미국에 하역하지 않고 통과하는 화물(frob=1)도 포함")
    p.add_argument("--out", required=True, help="저장 경로(.parquet). git 폴더 밖의 데이터 영역으로")
    p.add_argument("--smoke", action="store_true",
                   help="2024-03-01~03-08 1주치만. 연결과 필터 정합성을 싸게 확인")
    p.add_argument("--print-sql", action="store_true", help="SQL만 출력하고 종료")
    args = p.parse_args()

    if args.origin and args.shipper_country:
        print("[주의] --origin 과 --shipper-country 를 함께 걸면 교집합만 남는다. "
              "의도한 것이 맞는지 확인할 것.", file=sys.stderr)
    if not args.origin and not args.shipper_country and not args.hs_prefix and not args.smoke:
        sys.exit("필터가 하나도 없다. 전체 2.33억 행을 받으려는 것이 아니라면 "
                 "--origin / --hs-prefix 중 하나는 지정할 것.")

    if args.smoke:
        date_start, date_end = "2024-03-01", "2024-03-08"
    else:
        date_start, date_end = f"{args.year_start}-01-01", f"{args.year_end + 1}-01-01"

    sql = build_sql(date_start, date_end, args.origin, args.shipper_country,
                    args.hs_prefix, args.keep_frob)
    if args.print_sql:
        print(sql)
        return

    out = Path(args.out)
    if out.suffix.lower() != ".parquet":
        sys.exit(f"저장 형식은 parquet 입니다: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"[1/3] Snowflake 조회 중... ({date_start} ~ {date_end}"
          + (f", origin={args.origin}" if args.origin else "")
          + (f", shpCountry={args.shipper_country}" if args.shipper_country else "")
          + (f", HS {args.hs_prefix}*" if args.hs_prefix else "") + ")")
    df = tidy(run_query(sql))

    print(f"[2/3] {len(df):,} 행 수신, 메모리 {df.memory_usage(deep=True).sum()/1e6:,.1f} MB")
    report(df)

    print(f"\n[3/3] 저장 -> {out}")
    df.to_parquet(out, index=False, compression="snappy")
    print(f"  파일 크기 {out.stat().st_size/1e6:,.1f} MB")
    print("  ※ 데이터 카탈로그(_catalog.md)에 한 줄 기록할 것: 내용·기간·행수·이 스크립트·추출일·담당")


if __name__ == "__main__":
    main()
