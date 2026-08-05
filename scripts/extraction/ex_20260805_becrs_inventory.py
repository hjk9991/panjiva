"""
ex_20260805_becrs_inventory.py — BECRS 테이블 존재 여부 인벤토리 (과제 03 착수 점검)

배경
----
`shared memory\\data_dictionary.md`(2026-08 실측)의 구독 테이블 목록에는 BECRS
(Business Entity Cross Reference Service) 계열 테이블이 **한 건도 없다**.
문서화된 crosswalk는 `panjivaCompanyCrossRef`(무역ID↔companyId),
`ciqCompanyCrossRef`(회사↔외부식별자), `ciqCompanyUltimateParent`(현재 소유구조)뿐이다.

과제 01·02·03이 전부 BECRS를 축으로 설계되어 있으므로, 착수 전에
**계정에 BECRS 뷰가 실제로 있는지**를 메타데이터로 확인한다.
(INFORMATION_SCHEMA 조회 — 데이터 스캔 없음, 크레딧 부담 없음)

사용법
------
    python scripts\\extraction\\ex_20260805_becrs_inventory.py
    python scripts\\extraction\\ex_20260805_becrs_inventory.py --save   # parquet 저장

산출
----
stdout 요약 + (--save 시) C:\\panjiva\\data\\staging\\becrs_table_inventory_20260805.parquet
"""

import argparse
import os
from pathlib import Path

import pandas as pd
import snowflake.connector

# ---------------------------------------------------------------------------
# 1. 접속 — 팀 규약은 홈 폴더 .snowflake.env. 이 머신에는 아직 없어서
#    기존 연구용 .env를 대체 경로로 둔다 (자격증명은 저장소 밖에만 존재).
#    python-dotenv가 없는 인터프리터에서도 돌도록 KEY=VALUE를 직접 읽는다.
# ---------------------------------------------------------------------------
ENV_CANDIDATES = [
    Path.home() / ".snowflake.env",
    Path.home() / "OneDrive" / "Research" / "Panjiva" / ".env",
]


def load_env_file(path: Path) -> None:
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_env_file(env_path)
        break
else:  # pragma: no cover
    raise SystemExit(f"자격증명 파일을 찾을 수 없습니다: {ENV_CANDIDATES}")

CONN_KW = dict(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
)

# BECRS 및 소유구조/크로스워크 관련어. 벤더가 이름을 어떻게 붙였는지 모르므로 넓게 훑는다.
PATTERNS = [
    "%BECRS%",
    "%BUSINESSENTITY%",
    "%BUS_ENTITY%",
    "%ENTITYCROSS%",
    "%ENTITY_CROSS%",
    "%CROSSREF%",
    "%CROSS_REF%",
    "%XREF%",
    "%RELATION%",
    "%HIERARCH%",
    "%OWNERSHIP%",
    "%PARENT%",
    "%SUBSID%",
    "%AFFILIAT%",
]


def show(cur, stmt: str) -> pd.DataFrame:
    """SHOW 계열 — Arrow가 아니라 일반 fetch (메타데이터라 타입 이슈 없음)."""
    cur.execute(stmt)
    cols = [c[0].lower() for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", action="store_true", help="결과를 staging parquet으로 저장")
    ap.add_argument("--list-all", action="store_true",
                    help="XPRESSFEED 뷰 전체 목록 + 식별자 타입 마스터까지 출력")
    args = ap.parse_args()

    with snowflake.connector.connect(**CONN_KW) as conn:
        with conn.cursor() as cur:
            print("=" * 78)
            print("[1] 계정에서 보이는 데이터베이스")
            print("=" * 78)
            dbs = show(cur, "show databases")
            name_col = "name" if "name" in dbs.columns else dbs.columns[1]
            db_names = dbs[name_col].tolist()
            print(dbs[[c for c in ("name", "origin", "owner", "kind") if c in dbs.columns]]
                  .to_string(index=False))

            print()
            print("=" * 78)
            print("[2] 데이터베이스별 스키마")
            print("=" * 78)
            for db in db_names:
                try:
                    sch = show(cur, f'show schemas in database "{db}"')
                    keep = [s for s in sch["name"].tolist() if s != "INFORMATION_SCHEMA"]
                    print(f"  {db}: {keep}")
                except Exception as exc:  # 권한 없는 DB는 건너뛴다
                    print(f"  {db}: (조회 불가 — {type(exc).__name__})")

            print()
            print("=" * 78)
            print("[3] BECRS·소유구조 관련어로 테이블/뷰 검색")
            print("=" * 78)
            frames = []
            for db in db_names:
                if db.upper() in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                    continue
                like_clause = " or ".join(f"table_name ilike '{p}'" for p in PATTERNS)
                sql = f"""
                    select table_catalog, table_schema, table_name, table_type, row_count
                    from "{db}".information_schema.tables
                    where {like_clause}
                    order by table_name
                """
                try:
                    cur.execute(sql)
                    got = cur.fetch_pandas_all()
                    got.columns = [c.lower() for c in got.columns]
                    if len(got):
                        frames.append(got)
                    print(f"  {db}: {len(got)}건")
                except Exception as exc:
                    print(f"  {db}: (조회 불가 — {type(exc).__name__}: {exc})")

            hits = (pd.concat(frames, ignore_index=True) if frames
                    else pd.DataFrame(columns=["table_catalog", "table_schema",
                                               "table_name", "table_type", "row_count"]))

            print()
            print("-" * 78)
            becrs = hits[hits["table_name"].str.upper().str.contains("BECRS|BUSINESSENTITY|BUS_ENTITY")]
            print(f"BECRS 직접 매칭: {len(becrs)}건")
            if len(becrs):
                print(becrs.to_string(index=False))
            print(f"소유구조·크로스워크 후보 전체: {len(hits)}건")
            if len(hits):
                print(hits.to_string(index=False))

            # [4] 전체 뷰 수 — 문서(120뷰)와 대조해 구독 범위가 바뀌었는지 확인
            print()
            print("=" * 78)
            print("[4] 기본 스키마 전체 오브젝트 수 (문서 대조용)")
            print("=" * 78)
            cur.execute("""
                select table_schema, table_type, count(*) as n
                from MI_XPRESSCLOUD.information_schema.tables
                group by 1, 2 order by 1, 2
            """)
            tot = cur.fetch_pandas_all()
            tot.columns = [c.lower() for c in tot.columns]
            print(tot.to_string(index=False))

            if args.list_all:
                # [5] 이름 패턴에 걸리지 않는 관계 테이블이 숨어 있는지 전수 확인
                print()
                print("=" * 78)
                print("[5] XPRESSFEED 뷰 전체 목록")
                print("=" * 78)
                cur.execute("""
                    select table_name
                    from MI_XPRESSCLOUD.information_schema.tables
                    where table_schema = 'XPRESSFEED'
                    order by table_name
                """)
                allv = cur.fetch_pandas_all()
                names = allv[allv.columns[0]].tolist()
                for i in range(0, len(names), 3):
                    print("  " + "".join(n.ljust(42) for n in names[i:i + 3]))

                # [6] BECRS가 '별도 테이블'이 아니라 '식별자 타입'으로 들어와 있을 가능성
                print()
                print("=" * 78)
                print("[6] ciqCrossRefIdentifierType — 식별자 타입 마스터 전체")
                print("=" * 78)
                cur.execute("""
                    select identifierTypeId, identifierTypeName
                    from MI_XPRESSCLOUD.XPRESSFEED.ciqCrossRefIdentifierType
                    order by identifierTypeId
                """)
                idt = cur.fetch_pandas_all()
                idt.columns = [c.lower() for c in idt.columns]
                print(f"  총 {len(idt)}종")
                print(idt.to_string(index=False))

    if args.save and len(hits):
        out = Path(r"C:\panjiva\data\staging\becrs_table_inventory_20260805.parquet")
        hits.to_parquet(out, index=False)
        print(f"\n저장: {out}  ({len(hits)}행)")


if __name__ == "__main__":
    main()
