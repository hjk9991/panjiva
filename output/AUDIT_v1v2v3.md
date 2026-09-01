# v1 · v2 · v3 교차 감사

**감사일** 2026-09-01 · **스크립트** `audit_v1v2v3.py` · **대상** v1 `C:\panjiva\data\staging\tom_v1_2024_asof` · v2 `C:\panjiva\data\staging\tom_v2_2024_asof` · v3 `C:\panjiva\data\staging\within_firm_pilot_2024_asof` · 표 `C:\panjiva\projects\20251201\output\tables\wf2024_asof` · **월** 202401~202412 (12개월)

각 버전의 `90_checks.md` 가 못 보는 것 — 팀 함정 준수 · 명세 조항 대조 · 버전 간 총계 연결 · 문서 수치 일치 — 를 본다.


## A. 팀 함정 12종 (`shared memory\CLAUDE.md`)


### A-1 · A-11 — HS 코드 추출과 `zfill` 금지

- [PASS] **A-1 HS 접두어(`Classified:` 등)가 값에 남아 있지 않다** — 접두어 잔존 0건
- [PASS] **A-11 `zfill(6)` 흔적이 없다 (6자리인데 `00` 으로 시작)** — 흔적 0건
  - 길이 분포(`imp_hs` 전 월): 4자리 9,162, 6자리 19,813,646 — **4자리는 정상**(원본이 4자리인 HS. 대표 코드 9804·9905·7111 등)
- [PASS] **A-11 v1 의 `hs6` 에도 zfill 흔적 없음** — 흔적 0건

### A-2 — 자식(HS) 조인 후 `sum()` 금지

v1·v2 는 HS 자식을 조인하지 않고 **대표 HS 1:1** 만 쓴다. v3 `panel_firm_origin_hs` 만 자식을 쓰는데 **균등배분**이라 총액이 보존된다.

- [PASS] **A-2 균등배분 후 **금액 합계 보존** (행 수가 아니라 합계로 검증)** — 차이 $0.00
- [PASS] **A-2 중량·TEU 도 보존** 
  - 배분 후 `n_shipments` 합 8,506,447 vs 실제 선적 7,334,195 — **행 수는 부풀려지는 것이 정상**(한 선적이 여러 HS 행)

### A-3 — 국가 컬럼 3종을 구분해 썼는가

- [PASS] **A-3 v1 이 `shpmtorigin`·`shpcountry`·`portofladingcountry` 를 다 보존** 
- [PASS] **A-3 v3 의 원산지 대표값이 `origin_main`(=`shpmtorigin` 유래)** 
  - 실측(전 월): `shpmtorigin` 결측 **0.05%** vs `shpcountry` 결측 **33.11%** — 함정 문서의 0.08% vs 31.2% 와 같은 방향

### A-4 — 표준필터

- [PASS] **A-4 `conCountry` 가 US 또는 결측만 남아 있다** — 위반 0건
- [PASS] **A-4 `frob` 가 1 인 행이 없다** — 위반 0건

### A-5 — crossRef `activeFlag=1` · `primaryFlag` 미사용

- [PASS] **A-5 추출 SQL 에 `activeFlag = 1` 이 있다** 
- [PASS] **A-5 추출 SQL 이 `primaryFlag` 로 거르지 않는다** 

### A-6 · A-7 — 법인 vs 최종모회사, 재무 커버리지

- [PASS] **A-6 v1 이 법인(`*_ciqid`)과 최종모회사(`*_up`) 를 **둘 다** 보존** 
| 기준 | 수입액 커버(%) |
|---|---|
| 법인 자신의 연간 재무 | 5.65 |
| 최종모회사의 연간 재무 | 31.62 |
- [PASS] **A-7 모회사 롤업이 커버리지를 크게 올린다 (함정 문서: 6.0% → 42.9%)** — 5.7% → 31.6% (5.6배)
  - 절대 수준이 함정 문서(42.9%)와 다른 것은 **표본·결합 방식이 달라서**다. 문서 수치는 as-of + 소급 2년(여러 해 재무를 끌어옴) 기준이다. **롤업 효과 자체는 같은 방향으로 크게 나타난다.**

### A-7 — `filingDate` 를 시점 필터로 쓰지 않았는가

| 쓰임 | 건수 |
|---|---|
| 정렬(order by) | 3 |
| SELECT 컬럼 | 15 |
| 주석·문서 | 4 |
| **필터** | 0 |
- [PASS] **A-7 어느 스크립트도 `filingDate` 를 **시점 필터**로 쓰지 않는다** — 정렬·SELECT·주석 용도만 (정렬은 정정본 중 최신을 고르는 용도라 무해)

### A-8 — 금액 결측률과 TEU 결측

| 지표 | 값 | 함정 문서 참조 |
|---|---|---|
| 금액(`valueofgoodsusd`) 비결측률(%) | 98.96 | 96~99 |
| TEU `isna` — House B/L (%) | 0.12 |  |
| TEU `isna | == 0` — House B/L (%) | 43.94 | 22.4 |
| TEU `isna` — Simple B/L (%) | 4.22 |  |
| TEU `isna | == 0` — Simple B/L (%) | 4.39 | 5.0 |
  - House B/L 의 TEU 는 NULL 이 아니라 **0** 으로 오는 경우가 대부분이라 `isna` 만 세면 함정 문서와 수십 배 어긋난다. 참조값과 비교할 지표는 `isna | == 0` 이다.
- [PASS] **A-8 TEU 결측(`isna | == 0`) — House B/L 이 함정 문서 참조값과 크게 어긋나지 않는다** — 실측 43.94% vs 참조 22.4% (허용: 비율 1/2~2배)
- [PASS] **A-8 TEU 결측(`isna | == 0`) — Simple B/L 이 함정 문서 참조값과 크게 어긋나지 않는다** — 실측 4.39% vs 참조 5.0% (허용: 비율 1/2~2배)

---

## B. 명세 04 조항별 대조


### §4.1 당사자별 상태 — 컬럼 6종 × 2측

- [PASS] **§4.1 당사자별 6컬럼 × 2측 전부 존재** — 없는 것 []

### §4.2 · §4.3 매칭상태와 관계분류 (전 월 값역)

- [PASS] **§4.2 `crosswalk_match_status` 가 명세 4값만 갖는다** — 실제 ['both', 'consignee_only', 'none', 'shipper_only']
- [PASS] **§4.2 `ownership_match_status` 도 4값** — 실제 ['both', 'consignee_only', 'none', 'shipper_only']
- [PASS] **§4.3 `relationship` 이 3값만** — 실제 ['arms_length', 'unmatched', 'within_firm']
- [PASS] **§4.3 `unmatched_reason` 이 권장 7값 안에 있다** — 실제 ['entity_unmatched_both', 'entity_unmatched_consignee', 'entity_unmatched_shipper', 'matched', 'ownership_unmatched_consignee', 'ownership_unmatched_shipper']
- [PASS] **§4.3 `intra_group` 은 unmatched 에서 결측** — 위반 0건
  - `crosswalk_match_status` 와 `ownership_match_status` 가 **다른 행**: **20건** / 18,134,776 — 명세가 전제한 2단계 매칭이 사실상 1단계임을 보여준다

### §5 pair×월 관계변수

- [PASS] **§5 지표 6종이 `panel_pair_month` 에 있다** — 없는 것 []
- [PASS] **§5 같은 지표가 v2 세 패널에도 있다 (대사 가능)** 

### §6 소유구조 변화·관계전환

- [PASS] **§6.4 관계전환 컬럼 존재** 
- [PASS] **§6.5 `d8_ownership_change_summary.csv` 생성됨** — `C:\panjiva\projects\20251201\output\tables\wf2024_asof`
- [PASS] **§6.5 `d8_ownership_change_monthly.csv` 생성됨** — `C:\panjiva\projects\20251201\output\tables\wf2024_asof`
- [PASS] **§6.5 `d9_relationship_transition_summary.csv` 생성됨** — `C:\panjiva\projects\20251201\output\tables\wf2024_asof`
- [PASS] **§6.5 `d9_relationship_transition_pairs.parquet` 생성됨** 

### §7 override

- [PASS] **§7 원본(`*_ciqid_original`)과 적용값(`*_ciqid`)이 **둘 다** 보존** 
- [PASS] **§7 승인 파일 미제출(영향표 없음)이므로 override 적용이 0 건** — 실측 0건

### §8 산출물 목록

- [PASS] **§8 v1 산출물 전부 존재** — 없는 것 []
- [PASS] **§8 v2 산출물 전부 존재** — 없는 것 []
- [**FAIL**] **§8 v3 산출물 전부 존재** — 없는 것 ['panel_firm_quarter.dta']

---

## C. v1 → v2 → v3 총계 연결

| 층 | 선적 | 금액($B) |
|---|---|---|
| 공용 원천 `imp_ship_*` | 18,134,776 |  |
| v1 `shipment_master_*` | 18,134,776 | 1,910.73 |
| v2 `03_firm` 수입 측 (= 수입자 매칭 선적) | 7,356,007 | 1,241.36 |
|   ↳ v1 기준값 | 7,356,007 | 1,241.36 |
| v2 `04_group` 수입 측 (= 수입자 UP 있는 선적) | 7,356,006 | 1,241.36 |
|   ↳ v1 기준값 | 7,356,006 | 1,241.36 |
| v3 `panel_pair_month` (= 양측 매칭 선적) | 3,270,527 | 662.76 |
|   ↳ v1 기준값 | 3,270,527 | 662.76 |
- [PASS] **C-1 v1 선적 = 공용 원천** — 차이 +0
- [PASS] **C-2 v2 03_firm 수입 측 = v1 수입자 매칭** 
- [PASS] **C-3 v3 pair×월 = v1 양측 매칭** 

### 관계분류 — v1 과 v3 가 같은 숫자를 말하는가

| 출처 | within_firm($B) | arms_length($B) |
|---|---|---|
| v1 (선적 전체) | 206.00 | 456.76 |
| v3 pair×월 | 206.00 | 456.76 |
- [PASS] **C-4 within_firm 금액이 v1 = v3** 
- [PASS] **C-5 arms_length 금액이 v1 = v3** 

---

## D. 문서에 적힌 수치가 실제와 맞는가

- [PASS] **D-1 v1 COLUMNS.md 의 행 수가 실제와 일치** — 실제 18,134,776
- [PASS] **D-2 v1 COLUMNS.md 의 열 수가 실제와 일치** — 실제 1,428열
- [PASS] **D-3 v2 COLUMNS.md 의 `02_pair` 행 수** — 실제 504,865
- [PASS] **D-3 v2 COLUMNS.md 의 `02_pair` 열 수** — 실제 5,196열
- [PASS] **D-3 v2 COLUMNS.md 의 `03_firm` 행 수** — 실제 612,269
- [PASS] **D-3 v2 COLUMNS.md 의 `03_firm` 열 수** — 실제 2,632열
- [PASS] **D-3 v2 COLUMNS.md 의 `04_group` 행 수** — 실제 532,035
- [PASS] **D-3 v2 COLUMNS.md 의 `04_group` 열 수** — 실제 1,343열
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_pair_month` 행 수** — 실제 852,303
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_pair_month` 열 수** — 실제 36열
- [PASS] **D-4 v3 COLUMNS.md 의 `dim_relationship` 행 수** — 실제 246,317
- [PASS] **D-4 v3 COLUMNS.md 의 `dim_relationship` 열 수** — 실제 38열
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_firm_quarter` 행 수** — 실제 612,269
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_firm_quarter` 열 수** — 실제 2,641열
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_firm_origin_hs` 행 수** — 실제 993,394
- [PASS] **D-4 v3 COLUMNS.md 의 `panel_firm_origin_hs` 열 수** — 실제 14열
- [PASS] **D-5 카탈로그의 `tom_v1_2024_asof` 행 수** — 실제 18,134,776

---

## 요약

| 구분 | 결과 | 건수 |
|---|---|---|
| 대사 | PASS | 5 |
| 명세 | FAIL | 1 |
| 명세 | PASS | 18 |
| 문서 | PASS | 17 |
| 함정 | PASS | 15 |

**56개 항목 중 55개 PASS**

### 실패 항목

| 구분 | 항목 | 결과 |
|---|---|---|
| 명세 | §8 v3 산출물 전부 존재 | FAIL |