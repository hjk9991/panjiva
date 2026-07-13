# Panjiva/CapIQ — Snowflake 데이터 워크플로

> **대상**: A 서버에서 Panjiva/CapIQ 데이터를 조회·추출하는 팀원
> **전제**: [A 서버 온보딩](onboarding-A-remote.md)을 마친 상태 (VS Code로 A 접속 + Python 공용 환경)
> **막히면**: 이 문서를 Claude Code에 붙여넣고 에러 메시지 전문과 함께 질문하세요.

---

## 0. 아키텍처 이해 — 어디서 뭐가 실행되는가

S&P Global은 Panjiva/CapIQ 데이터를 **Snowflake 데이터 공유(share)** 방식으로 제공합니다.
데이터는 Snowflake 클라우드에 있고, 우리는 쿼리만 던집니다.

```
Snowflake 클라우드                         A 서버
┌────────────────────────┐               ┌──────────────────────────┐
│ PANJIVA/CIQ 공유 DB     │── 쿼리 결과 ──▶│ C:\panjiva\data\staging  │
│ (SQL은 여기서 실행됨.    │   (추출물만)    │  → 검수 후 data\raw 이동  │
│  A의 성능과 무관)        │               │ 분석은 추출된 parquet으로  │
└────────────────────────┘               └──────────────────────────┘
```

**핵심 원칙 3가지:**

1. **무거운 연산(필터·집계·조인)은 Snowflake에서** 끝내고, A에는 **분석에 필요한 결과만** 내려받습니다. 원본 테이블 통째 다운로드 금지 (수백 GB + 크레딧 낭비).
2. 쿼리는 A를 거칠 필요가 없습니다 — 본인 자리 컴퓨터에서 직접 Snowflake에 붙어도 됩니다. 다만 **대용량 추출물의 저장은 A에만** 합니다 (데이터 반출 금지 원칙).
3. 추출에 쓴 SQL/스크립트는 반드시 git에 남깁니다 (재현성 — 아래 5절).

---

## 1. 사전 준비

- [ ] 본인 Snowflake 계정 사용자명/비밀번호 (관리자에게 요청)

접속 정보 (팀 공통, S&P Xpressfeed 제공):

| 항목 | 값 |
|---|---|
| 계정 식별자 (account) | `vlc67107.us-east-1` |
| 웨어하우스 (warehouse) | `XF_READER_KoreaDevelopment_WH` |
| 데이터베이스 (database) | `MI_XPRESSCLOUD` |
| 스키마 (schema) | `XPRESSFEED` |

> Panjiva와 CapIQ 테이블이 **모두 `MI_XPRESSCLOUD.XPRESSFEED` 안에** 있습니다 (별도 DB 아님).

## 2. 접속 정보 설정 (A에서, 계정당 1회)

비밀번호를 코드에 적으면 git에 딸려 올라가는 사고가 납니다. **본인 홈 폴더의 `.env` 파일**에 두세요.

A의 VS Code 터미널(SSH: A)에서:

```powershell
@"
SNOWFLAKE_ACCOUNT=vlc67107.us-east-1
SNOWFLAKE_USER=<본인 Snowflake 사용자명>
SNOWFLAKE_PASSWORD=<본인 비밀번호>
SNOWFLAKE_WAREHOUSE=XF_READER_KoreaDevelopment_WH
SNOWFLAKE_DATABASE=MI_XPRESSCLOUD
SNOWFLAKE_SCHEMA=XPRESSFEED
"@ | Out-File -Encoding utf8 $env:USERPROFILE\.snowflake.env
```

> `.env` 파일은 본인 홈 폴더(`C:\Users\사번.김형진B\`)에 있으므로 다른 팀원이 볼 수 없습니다.
> **절대 `C:\panjiva\projects`(git 관리 폴더) 안에 만들지 마세요.**

python-dotenv가 공용 환경에 없다면 (최초 1회, 아무나):

```powershell
C:\panjiva\envs\main\Scripts\pip install python-dotenv
```

## 3. 접속 테스트

노트북이나 `.py` 파일에서:

```python
import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(Path.home() / ".snowflake.env")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
)

cur = conn.cursor()
cur.execute("SELECT current_user(), current_warehouse()")
print(cur.fetchone())
```

본인 사용자명과 웨어하우스가 출력되면 성공.

> 기관이 SSO(브라우저 로그인)를 쓰는 경우: `password` 대신 `authenticator="externalbrowser"`.
> 단, SSH 세션에서는 브라우저를 못 띄우므로 이 경우 본인 자리 컴퓨터에서 실행하거나 관리자와 키페어 인증을 상의하세요.

## 4. 데이터 탐색 — 뭐가 있는지 먼저 보기

```python
# Panjiva 테이블 목록 (Xpressfeed에는 테이블이 수천 개라 반드시 LIKE로 좁힐 것)
cur.execute("SHOW TABLES LIKE 'panjiva%' IN SCHEMA MI_XPRESSCLOUD.XPRESSFEED")
for row in cur.fetchall():
    print(row[1])

# CapIQ 테이블 목록
cur.execute("SHOW TABLES LIKE 'ciq%' IN SCHEMA MI_XPRESSCLOUD.XPRESSFEED")

# 테이블 구조 확인
cur.execute("DESCRIBE TABLE MI_XPRESSCLOUD.XPRESSFEED.panjivaUSImport")
```

### 주요 테이블 (팀에서 쓰는 것 위주)

| 테이블 | 내용 |
|---|---|
| `panjivaUSImport` | 미국 수입 선적 건별 데이터 (arrivalDate, conPanjivaId/conName=수입자, shpPanjivaId/shpName=수출자, shpCountry, 항구, weightKg, valueOfGoodsUSD, volumeTEU 등) |
| `panjivaUSImpHSCode` | 선적 건별 HS코드 (panjivaRecordId로 조인) |
| `panjivaCompanyCrossRef` | **Panjiva 기업 ID ↔ CapIQ companyId 매핑** — 무역 데이터와 재무 데이터를 잇는 다리 |
| CIQ 기업 기본정보 테이블군 | 기업 개요·산업분류·소재지 등 (`base_files_company_foundation` 스키마 문서 참고) |
| CIQ 재무 테이블군 | 최신 재무제표 항목 (`intraday_latest_financials` 스키마 문서 참고) |

> 전체 컬럼 정의는 팀 공유 스키마 문서(PDF 4종: Panjiva US 스키마, 크로스레퍼런스, CIQ 기업기본, CIQ 재무)를 참고하세요.
> Panjiva↔CapIQ 연계 분석의 조인 경로: `panjivaUSImport.conPanjivaId(또는 shpPanjivaId)` → `panjivaCompanyCrossRef.identifierValue` → `companyId` → CIQ 테이블.

**새 테이블을 다룰 때의 순서** (크레딧 절약 + 실수 방지):

```sql
-- 1) 몇 행인지부터
SELECT COUNT(*) FROM <테이블>;

-- 2) 소량 샘플로 구조 파악
SELECT * FROM <테이블> LIMIT 100;

-- 3) 그다음에 본 쿼리 작성
```

## 5. 추출 워크플로 (표준 절차)

### 5-1. 추출 스크립트 작성 규칙

모든 추출은 **스크립트 파일로** 작성해서 git에 남깁니다. 위치와 이름:

```
<본인 clone: C:\panjiva\projects\사번>\scripts\extraction\
    pull_panjiva.py                            ← 팀 표준 추출 스크립트 (아래 참고)
    ex_20260713_us_imports_kr_2020_2025.py    ← ex_날짜_내용요약.py
```

스크립트 안에 **SQL 전문, 추출 사유, 저장 경로**가 들어가야 합니다. 이게 있어야 나중에 "이 parquet이 뭐였지?"가 없습니다.

repo의 `scripts/extraction/pull_panjiva.py`가 팀 표준 스크립트입니다 — 미국 수입 데이터를 국가·연도·HS코드로 잘라 받는 전 과정이 구현되어 있으니, 새 추출 스크립트는 이걸 복사해서 시작하세요. 사용 예 (본인 clone 폴더에서):

```powershell
# 연결 테스트 (1,000행만)
python scripts\extraction\pull_panjiva.py --smoke

# 실제 추출: 한국발 전기기기(HS 85), 2020~2024
python scripts\extraction\pull_panjiva.py --year-start 2020 --year-end 2024 `
    --shp-country "South Korea" --hs-prefix 85 `
    --out C:\panjiva\data\staging\korea_electronics_2020_2024.parquet
```

### 5-2. 소량 추출 (수백만 행 이하) — 기본 패턴

```python
import pandas as pd

SQL = """
SELECT imp.panjivaRecordId, imp.arrivalDate, imp.conName, imp.shpName,
       imp.shpCountry, imp.valueOfGoodsUSD, imp.weightKg
FROM MI_XPRESSCLOUD.XPRESSFEED.panjivaUSImport imp
WHERE imp.shpCountry = 'South Korea'
  AND imp.arrivalDate BETWEEN '2020-01-01' AND '2025-12-31'
"""

cur.execute(SQL)
df = cur.fetch_pandas_all()

out = r"C:\panjiva\data\staging\us_imports_kr_2020_2025.parquet"
df.to_parquet(out, index=False)
print(f"{len(df):,} rows → {out}")
```

- 저장 형식은 **parquet** (csv 대비 용량 1/5~1/10, 타입 보존, 읽기 수십 배 빠름)
- 저장 위치는 `C:\panjiva\data\staging\` (팀원 쓰기 가능 구역)

### 5-3. 대량 추출 (수천만 행 이상) — 배치 패턴

한 번에 메모리에 안 올라가는 크기는 배치로 나눠 받습니다:

```python
import pyarrow.parquet as pq
import pyarrow as pa

cur.execute(SQL)

writer = None
total = 0
for batch_df in cur.fetch_pandas_batches():
    table = pa.Table.from_pandas(batch_df)
    if writer is None:
        writer = pq.ParquetWriter(out, table.schema)
    writer.write_table(table)
    total += len(batch_df)
    print(f"{total:,} rows...")
writer.close()
```

> 수억 행 규모라면 이 방식도 느립니다. 그 경우 Snowflake 쪽에서 `COPY INTO @stage`로 내보내고
> `GET`으로 받는 방식이 빠른데, 처음 할 때는 Claude Code에 이 문서를 주고 같이 작성하세요.

### 5-4. 추출 후 정리

1. `staging`의 파일을 열어 **행 수·기간·컬럼 검수**
2. 관리자가 `C:\panjiva\data\raw\`로 이동 (여기부터는 읽기 전용 — 실수로 덮어쓰기 방지)
3. `C:\panjiva\data\raw\_catalog.md`에 한 줄 기록:

```markdown
| 파일 | 내용 | 기간 | 행수 | 추출스크립트 | 추출일 | 담당 |
|---|---|---|---|---|---|---|
| us_imports_kr_2020_2025.parquet | 미국 수입, 한국발 | 2020-2025 | 12,345,678 | extraction/ex_20260713_....py | 2026-07-13 | 20220829 |
```

## 6. 비용·성능 수칙

Snowflake는 **웨어하우스 가동 시간만큼 과금**됩니다 (쿼리가 돌 때만). 규칙:

- `SELECT *` 로 전체 테이블을 받지 않기 — 필요한 컬럼·기간·국가만
- 본 쿼리 전에 `LIMIT 100`으로 결과 모양 확인
- 반복 분석에 쓸 데이터는 **한 번 추출해서 parquet으로** — 같은 쿼리를 매일 다시 돌리지 않기
- 대형 조인·집계는 Snowflake에서, 이후 가공은 A의 pandas에서 (분업)
- 실수로 폭주하는 쿼리는 Snowflake 웹 콘솔(Snowsight) → Query History에서 취소 가능

## 7. 자주 나는 문제

| 증상 | 원인/해결 |
|---|---|
| `250001: Could not connect` | 계정 식별자 오타. `vlc67107.us-east-1` 그대로인지 확인 |
| `Object does not exist or not authorized` | DB/테이블 이름 오타 또는 권한 없음. `SHOW DATABASES`로 보이는지 먼저 확인, 안 보이면 관리자에게 역할(role) 확인 요청 |
| `No active warehouse` | 접속 시 warehouse 미지정. `.env`의 `SNOWFLAKE_WAREHOUSE` 확인 또는 `cur.execute("USE WAREHOUSE <이름>")` |
| `fetch_pandas_all` 메모리 부족 | 5-3의 배치 패턴으로 전환, 또는 쿼리에서 기간을 쪼개 여러 번 추출 |
| staging에 저장 시 권한 거부 | `C:\panjiva\data\staging` 경로가 맞는지 확인. `raw` 등 다른 하위 폴더는 읽기 전용 |
| 비밀번호 만료/잠김 | Snowflake 웹 콘솔에서 직접 변경, 안 되면 관리자에게 |

---

## (부록) 관리자 체크리스트

- [ ] 팀원별 Snowflake 계정 발급 (개인별 계정 — 공용 계정 금지, 사용량 추적을 위해)
- [ ] 스키마 문서 PDF 4종(Panjiva US, 크로스레퍼런스, CIQ 기업기본, CIQ 재무)을 팀 공유 위치(mPower/OneDrive)에 배포
- [ ] staging 폴더 생성 + 권한 (A의 관리자 PowerShell, 1회):

```powershell
mkdir C:\panjiva\data\staging
icacls C:\panjiva\data\staging /grant "research:(OI)(CI)M"
mkdir C:\panjiva\data\raw
```

- [ ] 웨어하우스 `AUTO_SUSPEND`(예: 60초) 설정 확인 — 쿼리 안 돌 때 과금 방지
- [ ] `C:\panjiva\data\raw\_catalog.md` 빈 파일 생성 (표 헤더만)
- [ ] 주기적으로 staging 검수 → raw 이동
