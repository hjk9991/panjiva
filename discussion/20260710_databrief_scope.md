# 기타보고서(Panjiva 데이터 소개) — 스코프 & 데이터 작업 정리

**날짜:** 2026-07-10
**용도:** 보고서 내용 확정 + 데이터 다운로드·클리닝 방향 (팀 공유)

---

## A. 보고서 목적

S&P Panjiva 데이터를 **소개**하고, 그것으로 볼 수 있는 **descriptive statistics**를 보여준다. 분석(회귀 등)이 아니라 데이터 소개·기술통계 중심.

---

## B. 보고서 구성(안)

### 1. Panjiva 데이터 개괄
- **포함 항목/필드** — 어떤 정보가 들어있는지 (근거: `Doc/01_dataset_overview.md`).
- **활용 문헌** — Panjiva를 쓴 선행연구 (→ 별도 문헌조사).
- **해상무역 100% 커버 더블체크** — Panjiva측 "미국 해상수입 전부 포함" 확인 + 외부 검증 (Panjiva 수입액 vs Census 미국 해상수입액 대조).
- **US 무역 대비 Panjiva 비중** — 수입·수출 **따로**, 외부 총계(Census/Comtrade) 대비. (해상 only → 총수입 대비 <100%가 정상: 항공·육로 제외.)

### 2. 볼 수 있는 다양한 statistics

**2-1. 미국의 무역 흐름** *(기업정보 불필요 → CIQ-matched 안 씀)*
- 수출국(원산지) 비중 & 추세 (top 5–10 국가/지역).
- 수입 품목 비중 & 추세 (HS 2자리, **BEC 분류**).
- 위 수치는 **미국 총 데이터와 비교** (외부 소스).

**2-2. firm-to-firm trade**
- **CIQ 매칭 비중:** consignee-only / shipper-only / both-matched (나머지 = none-matched, **다운로드 불필요**).
- **within-family trade** *(consignee·shipper **both-matched** 샘플 기준)* — "family" = 양측이 **같은 ultimate parent**(BECRS).
  - 전체 US 수입 중 within-family 크기 & 추세 (전체 / 특정 산업 / 특정 품목).
  - 특정 주요 수출국발 수입 중 within-family 크기 & 추세 (전체 / 산업 / 품목).
  - (선택) 관여 기업의 **재무 특성** — major 재무 변수만, 재무 있는 기업에 한해 붙임(아래 D 참조).

**2-3. 한국 → 미국 수출** *(shipper=한국 기업)*
- 한국 수출기업 분석 (수출기업 수·집중도·HS 구성·family 내 비중·추세 등).
- **전체 한국→미국 수출(외부 소스)과 비교** → Panjiva 커버리지.
- (선택) 한국 수출기업의 **재무 특성** — 재무 있는 기업에 한해 붙임(아래 D 참조).

---

## C. 방법론 관련 참고 플래그

1. **within-family는 both-matched에서만 식별됨.** 기밀·미매칭 선적 속 관계사 거래는 안 보임 → "US 수입의 X%가 family 내"는 **하한**이거나 **'식별된 firm-to-firm 거래 중'** 기준으로 프레이밍. 외부 벤치마크: 미국 **Census "Related-Party Trade"**(연간, 수입의 ~40%대) — 여기에 비교하면 대표성·하한 정도를 정직하게 제시 가능.
2. **한국 수출기업 완전포착 → consignee-미매칭 포함 필수.** 미국 바이어가 기밀이라고 한국 exporter 선적을 빼면 그 기업 수출액이 **비랜덤하게 과소계상**됨.
3. **`valueOfGoodsUSD` = Panjiva 추정치**(신고값 아님) → 금액 기반 수치 전반에 각주.
4. **BECRS = 현재 스냅샷** — parent 소유구조를 과거연도에 소급 적용 → 추세 해석 caveat.
5. **BEC = 외부 concordance**(HS→BEC, UN) 파생 → 버전 명시.
6. **해상 only** 분모 프레이밍(항공·캐/멕 육로 제외).

---

## D. 데이터 다운로드 & 클리닝 — high-level

**핵심: 보고서 대부분은 서버측 집계 SQL로 해결. 무거운 선적 단위 미시 다운로드는 Panjiva 1건뿐이고, 나머지(CIQ Base·BECRS·(선택)재무)는 회사 단위 소형 참조 테이블이다.**

| 무엇                                     | 방식     | 다운로드 범위                                                         | 추정 용량 · 공수          | 비고                                                                                       |
| -------------------------------------- | ------ | --------------------------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------- |
| 섹션 1 + 2-1 + firm-to-firm 매칭비중         | 집계 SQL | 다운로드 없음 (서버 SUM/GROUP BY)                                       | 요약표만(수십 KB) · 공수 M  | full Panjiva 대상 집계                                                                       |
| 2-2 within-family<br>2-3 한국수출<br>*(미시 core)* | 다운로드   | both-matched(글로벌) ∪ (shipper-매칭∩한국)                             | ~28–30GB(추정) · 공수 M | 양측 companyId/parent + HS 포함                                                              |
| CIQ Base                               | 다운로드   | 운영기업 필터 전체 (보고서용은 matched 기업만 가능 → 비고)                          | ~5–10GB(추정) · 공수 S  | 수입자 + 한국 수출기업 산업분류. matched만 받으면 초소량                                                     |
| BECRS parent map                       | 다운로드   | 전체 (또는 Panjiva pull에 조인 흡수)                                     | <0.5GB · 공수 S       | within-family flag용. parent는 Panjiva 다운로드에 이미 부착됨                                        |
| CIQ Financials (선택)                    | 다운로드   | matched∩재무보유, major 변수만                                         | <1GB(추정) · 공수 M     | left-join enrichment, **샘플 restrict 아님**. major = 매출·총자산·순이익·자본·고용·EBITDA 등 소수(417 전부 X) |
| 외부데이터                                  | 외부     | Census 무역총계·Related-Party·한국발수입 / Comtrade / HS→BEC concordance | 소량 · 공수 M           | share·비교·BEC 파생용                                                                         |

**클리닝** *(항목별 세부는 다음에 one-by-one 논의)*:
- core: 타입캐스팅 + 양측 companyId/parent 부착 + **within-family flag**(con_parent == shp_parent) + HS2/HS6 파생 + 산업(CIQ Base) 조인 + shpCountry 정리 + **한국 shipper 서브셋 플래그**.
- **HS→BEC concordance 조인** (신규 reference 레이어).
- 외부 US·한국 총계 병합.
- ※ 이름 표준화 불필요(CIQ `companyId` 사용).
- **재무 enrichment 넣을 경우:** major 변수만 long→wide pivot + 단위/FX 정규화 필요 — 변수 수가 적어 **부담 경감**(전체 417 항목 pivot 대비). 넣지 않으면 이 단계 전부 스킵.
