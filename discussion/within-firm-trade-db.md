# 작업명: Within-Firm / Arm's Length 무역 DB (Panjiva × CIQ)

- 담당: 20251201(김영수)
- 시작: 2026-08-06
- 설계문서: `lit_review_panjiva_ciq_within_firm_trade_20260805.md` (star schema L0→L3)
- 상태: 진행 중 (2024 H1 파일럿 완료, 본구축 전 확정사항 대기)
- ※ `trade-ownership-master.md`(BECRS 과제 03)와는 **별개 작업**. 검증된 SQL 패턴만 재사용.

## 기록 (최신이 위)

### 2026-08-06 — 2024 H1(2분기) 파일럿 구축 완료

**한 일**: 설계문서의 L0→L3 아키텍처를 6개월치로 관통 구축하고, 기초통계(§3.6) 5종과
진단 7종을 산출했다. 검증 게이트 5종을 전부 통과했다.

- **사용 데이터**: 없음 (Snowflake 직접 추출, 396초)
- **신규 스크립트**:
  - `scripts\extraction\ex_20260806_wf2q_pull.py` (L0, 월별 청크·재개 가능)
  - `scripts\processing\wf2q_10_build_dims.py` (L1 개체해소)
  - `scripts\processing\wf2q_20_build_facts.py` (L2 선적사실)
  - `scripts\processing\wf2q_30_build_panels.py` (L3 분석패널 + Stata 핸드오프)
  - `scripts\processing\wf2q_40_stats_report.py` (기초통계·진단)
- **산출물**: `C:\panjiva\data\staging\within_firm_pilot_2q\` — `data\staging\_catalog.md` 기록 완료
  - L1 `dim_company` 654,360 / `dim_group_link`(PIT) 223,961
  - L2 `fact_shipment` 7,237,772 · `fact_shipment_hs` 8,054,305 ·
    `fact_shipment_export` 1,522,080 · `fact_shipment_export_hs` 1,900,491
  - L3 `panel_pair_month` 420,381(pair 174,629) · `dim_relationship` 174,629 ·
    `panel_firm_quarter` 146,655 · `panel_firm_origin_hs` 547,425 (+ `.dta` 3종)
  - 표: `output\tables\wf2q\` (t1~t5b, d1~d7) / 리포트: `95_report.md`

**검증 게이트 (전부 통과)**

| 게이트 | 결과 |
|---|---|
| G1 1주 대사 (2024-03-01~07) | **254,873건 정확 일치** + 관계비중 재현 |
| G2 PK 유일 (수입·수출) | 중복 0 |
| G3 HS 배분 복원 (**합계** 기준) | weight·value·TEU 3종 완전 일치 |
| G4 패널 키 유일 | pair_month·firm_quarter 중복 0 |
| G5 pair 층 금액 대사 | $319,307,279,696 완전 일치 |

**확정한 정의** (사용자 승인)

- 기간 2024-01-01~06-30, 수입 전체 origin + 수출 둘 다 (한·미는 필터)
- `within_firm` = **거래일 기준 PIT 동일 ultimate parent** (self·parent_sub·sibling 통합).
  구간 부재 시 현 스냅샷 fallback + 플래그.
- pair-월 `within_firm` = 금액가중 within 비중 > 0.5 (`within_share` 병행 저장)
- `kor_mnc_link` = 한국계 UP & origin=KR & within
- ISIC 연결은 파일럿에서 보류 (HS6 키까지만)

**주요 실측 (2024 H1)**

- 수입 7,237,772건 / $935B. 양측 CIQ 매칭 **22.3%(금액 34.1%)**
- **within 금액비중 31.9~35.4%/월** (건수 기준 13~17% — 그룹 내 거래가 대형 선적에 집중)
- 수입기업당 파트너: 중위 1 · p99 25 · 최대 370 (문헌의 극단 왜도 재현)
- 한국 origin: 월 4.4~5.7만 건, within 금액 20~26%, `kor_mnc_link` 월 ~80 pair(~$0.7B)
- 마진 분해 로그 항등식 `decomp_check` ≈ 0 (월간 변동은 대부분 intensive)
- 포워더 제외 시 within 비중 15.4%→17.3%(건수) — 민감도 크지 않음

**설계문서와 달랐던 점 (실측)**

1. **수출 BoL 에 consignee 식별자가 아예 없다** (PANJIVAUSEXPORT 36컬럼 전수 확인)
   → 수출측 pair·within 산출 불가. 기업×목적지까지만.
   목적지는 `coalesce(shpmtDestination 49.5%, portOfUnladingCountry)` 후 결측 10%.
2. **수출에도 `valueOfGoodsUSD` 가 있다** (97.8% 채움) — 설계문서 §5.1 의 "수출측 금액 부재"는
   우리 피드에 해당하지 않는다. Census 단위가치 대치의 시급성이 낮아진다.
3. **redaction 이 사실상 0** — 문헌(원시 BoL 10~14%)과 달리 Panjiva 정제 단계에서 이미 처리된 것으로
   보인다. 플래그·진단 체계는 유지했다.
4. **분기재무 커버리지가 낮다** (firm-quarter 의 1.3~2.5%). 분기 보고가 상장 대형사 위주.
5. 지분율·1단계 모자관계 테이블 부재 → `related_minority`(10–50%) **산출 불가**
   (`91_질문_김영수_BECRS_구독범위.md` 참조). 스키마에 자리만 예약.

**다음 할 일 / 질문**

- [ ] PI/사용자: **본구축에서 연간 재무 병행** 여부 확정 (분기만으로는 커버리지 1.3~2.5%)
- [ ] PI/사용자: 본구축 기간 범위 (2007~ 전체 vs 특정 구간) 및 수출측 활용 범위
- [ ] HS6→ISIC Rev.4 concordance 확보 → `panel_firm_origin_hs` 에 연결 (§4.6 그룹 집계 연결고리)
- [ ] `dim_group_trade_potential` 적재 (현재 스키마 스텁만)
- [ ] Panjiva id 분열 이름·주소 조화 (현재 v1 은 CIQ 매칭 기업만 entity 통합)
- [ ] Census 대비 HS2 커버리지 비율표 (해상운송 한계 진단, §5.1)
