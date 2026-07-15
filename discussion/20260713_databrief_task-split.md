# 기타보고서(Panjiva 데이터 소개) — 트랙별 업무 정의서

> **날짜:** 2026-07-13
> **근거 문서:** [20260710_databrief_scope.md](20260710_databrief_scope.md) (보고서 스코프 — 섹션·플래그 번호는 이 문서 기준)
> **체제:** 실무 3트랙 + 감독 1인. 역할 배정 후 각 트랙 담당자는 작업 로그(`discussion\track○-*.md`)를 만들어 진행 상황을 기록합니다.

## 배정표 (팀 회의에서 확정)

| 트랙 | 성격 | 주로 쓰는 도구 | 담당자 |
|---|---|---|---|
| 트랙 1 — 서버측 집계 & 커버리지 | SQL 집계 중심, 독립 실행 가능 | Snowflake SQL, (약간의) Python | (미정) |
| 트랙 2 — 미시 데이터 파이프라인 | 다운로드·클리닝, 전체의 병목 | Python (pandas/pyarrow) | (미정) |
| 트랙 3 — 문헌·벤치마크·심층 분석 | 문헌조사 + 트랙2 산출물 분석 | 문헌 검색, Python/Stata | (미정) |
| 감독 | 지시·검수·raw 승격 | discussion, GitHub | 관리자 |

배정 기준 제안: SQL에 익숙한 사람 → 트랙 1, Python 데이터 처리에 강한 사람 → 트랙 2, 문헌·글쓰기에 강한 사람 → 트랙 3.

---

## 트랙 1 — 서버측 집계 & 커버리지 검증

**미션**: 보고서 섹션 1(데이터 개괄)과 2-1(미국 무역 흐름)의 모든 수치·표·그림. 다운로드 없이 Snowflake 집계 SQL로 해결.

**할 일:**

- [ ] Panjiva 포함 항목/필드 정리표 (스키마 PDF 4종 기반, 보고서 §1용)
- [ ] 해상무역 커버리지 더블체크: Panjiva 연도별 총수입액 집계 ↔ Census 미국 해상수입액 대조 (§1)
- [ ] US 무역 대비 Panjiva 비중 — 수입·수출 **따로**, Census/Comtrade 총계 대비 (§1) ※ 해상 only이므로 <100%가 정상 (플래그 6 각주)
- [ ] 수출국(원산지) 비중 & 추세, top 5–10 (§2-1)
- [ ] 수입 품목 비중 & 추세 — HS 2자리 + BEC 분류 (§2-1) ※ BEC 매핑 테이블은 트랙 2가 제공 (조율 포인트 1)
- [ ] 외부 총계와의 비교 표·그림 (§2-1)
- [ ] CIQ 매칭 비중 집계: consignee-only / shipper-only / both / none (§2-2 도입부)
- [ ] 위 비교에 쓸 외부 데이터(Census 무역총계, Comtrade) 수집 → `C:\panjiva\data\staging\external\`

**산출물**: `output\tables\tab_coverage_*.csv`, `output\figures\fig_trade_*.png` + 생성 스크립트 `scripts\analysis\track1_*.py` (SQL 포함)

**주의(스코프 문서 C절)**: 플래그 3 (`valueOfGoodsUSD`는 Panjiva 추정치 — 금액 수치 전부 각주), 플래그 6 (해상 only 분모 프레이밍)

---

## 트랙 2 — 미시 데이터 파이프라인

**미션**: 섹션 2-2·2-3 분석의 원료가 되는 미시 데이터셋을 다운로드·클리닝해서 `processed`에 공급. **트랙 3이 이 산출물을 기다리므로 최우선 착수.**

**할 일** (순서대로):

- [ ] `pull_panjiva.py --smoke`로 연결 확인
- [ ] **파일럿**: 1개월치(예: 2023-01)만 본 스펙으로 추출 → 감독자 검수 (스키마·행수·용량·조인 정합성) ← **품질 게이트 ①: 통과 전 본 다운로드 금지**
- [ ] 본 다운로드: both-matched(글로벌) ∪ (shipper-매칭 ∩ 한국), 양측 companyId/parent + HS 포함, ~28–30GB → `staging` ※ 사전에 A 디스크 여유 확인, 연도별 분할 추출 권장
- [ ] 참조 테이블 다운로드: CIQ Base (운영기업 — matched 기업만이면 초소량), BECRS parent map (<0.5GB)
- [ ] HS→BEC concordance (UN) 확보 → 매핑 테이블로 정리, **버전 명시** (플래그 5) → 트랙 1에 공유 (조율 포인트 1)
- [ ] 클리닝 core: 타입캐스팅 → 양측 companyId/parent 부착 → **within-family flag** (con_parent == shp_parent) → HS2/HS6 파생 → 산업(CIQ Base) 조인 → shpCountry 정리 → **한국 shipper 서브셋 플래그**
- [ ] `processed`에 분석용 parquet 저장 + `_catalog.md` 기록

**산출물**: `C:\panjiva\data\processed\`의 분석용 데이터셋 + `scripts\extraction\`, `scripts\processing\`의 스크립트

**주의**: 플래그 2 (한국 수출기업 분석용 서브셋은 consignee-미매칭 선적도 **포함**해야 함 — 빼면 비랜덤 과소계상), 플래그 4 (BECRS는 현재 스냅샷 — 소급 적용 한계를 `_catalog.md`에 명기)

---

## 트랙 3 — 문헌·벤치마크·심층 분석

**미션**: 보고서 섹션 2-2(within-family)·2-3(한국→미국 수출)의 분석과, 섹션 1의 문헌 파트. 전반부는 독립 작업, 후반부는 트랙 2 산출물 사용.

**할 일 — 1부 (트랙 2 대기 없이 바로):**

- [ ] Panjiva 활용 선행연구 조사 → 정리표 (저자·연도·활용 방식·저널) (§1)
- [ ] Census **Related-Party Trade** 통계 수집 (연간, 수입의 ~40%대) — within-family 결과의 외부 벤치마크 (플래그 1)
- [ ] 한국→미국 수출 총계(외부 소스) 수집 — §2-3 커버리지 비교용
- [ ] 방법론 각주 문안 작성: C절 플래그 6개를 보고서에 들어갈 문장으로 (특히 플래그 1의 "하한/식별된 거래 중" 프레이밍)

**할 일 — 2부 (트랙 2의 processed 데이터가 나온 후):**

- [ ] within-family 크기 & 추세 — 전체 / 특정 산업 / 특정 품목 (§2-2)
- [ ] 주요 수출국발 수입 중 within-family 크기 & 추세 (§2-2)
- [ ] Related-Party Trade 벤치마크와 비교 표 (플래그 1 프레이밍 적용)
- [ ] 한국 수출기업 분석: 기업 수·집중도·HS 구성·family 내 비중·추세 (§2-3)
- [ ] Panjiva 커버리지: 한국→미국 수출 외부 총계 대비 (§2-3)

**산출물**: `output\figures\fig_family_*.png`, `fig_kr_*.png`, `output\tables\`, 문헌 정리 `draft\literature.md`, 스크립트 `scripts\analysis\track3_*.py`

**주의**: 플래그 1 (within-family는 both-matched에서만 식별 — 프레이밍 필수), 플래그 3 (금액 각주)

---

## 감독 (관리자)

- `00_directions.md`로 지시, 트랙별 작업 로그 3개로 진행 파악 (매일 pull 또는 GitHub 웹)
- **품질 게이트 ①**: 트랙 2 파일럿 검수 후 본 다운로드 승인
- **품질 게이트 ②**: 표·그림에 C절 플래그 각주가 반영됐는지 최종 확인
- `staging` → `raw` 승격 + `raw\_catalog.md` 기록 (권한상 감독자만 가능)
- 보고서 조립(`draft\`) 및 최종 편집

---

## 일정·의존성

```
주차 1            주차 2~3              주차 3~4
트랙1: 집계 SQL·외부수집 ───→ §1·§2-1 표·그림 완성 ──┐
트랙2: smoke→파일럿→[게이트①]→본 다운로드→클리닝 ──→│→ 보고서 조립 → [게이트②]
트랙3: 문헌·벤치마크(1부) ──→ (트랙2 대기) → 2부 분석 ──┘
```

**조율 포인트:**

1. **HS→BEC concordance** — 트랙 2가 만들고 트랙 1·3이 사용. reader 계정이라 Snowflake 업로드 불가 → 서버에서는 HS6까지 집계하고 BEC 매핑은 로컬(Python)에서 조인
2. **재무 enrichment (CIQ Financials)는 일단 스코프 제외** — 3주차에 진도 보고 넣을지 결정 (넣으면 트랙 2가 다운로드, 트랙 3이 활용)
3. 무거운 Snowflake 쿼리·대용량 다운로드 전에는 팀 채널 공지 (A 자원·크레딧 공유)

**공통 규칙**: 작업 흐름은 [docs/team-workflow.md](../docs/team-workflow.md), 추출 절차는 [docs/snowflake-data-workflow.md](../docs/snowflake-data-workflow.md) 준수. 모든 산출물은 스크립트로 재생성 가능해야 함.
