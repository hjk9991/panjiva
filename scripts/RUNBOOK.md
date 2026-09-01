# RUNBOOK — 아무 기간이나 다시 만드는 법

**지금 만들어진 것은 2024년(v1~v3)과 2007-07~2025(v4)이지만, 어느 기간이든 인자만 바꿔 다시 만들 수 있다.**
스크립트 안에 박힌 연도는 전부 `default=` 값이거나 디스크(원천 파일명)에서 발견한 것이다.

담당: 03 김영수 · 명세: `shared memory\BECRS_Matching_Project\04_2024연간파일럿_통합명세.md` §10
**갱신**: 2026-09-01 검토 반영 — 원천 폴더 통합(`source\trade`), as-of 판 절차, v3 신규 패널, override, 교차 검증 도구 인자화.

> ⚠️ **빌드 전에 가용 RAM 을 확인할 것.** v1 은 월당 약 20GB 를 쓴다. Data Wrangler 나
> Jupyter 커널이 메모리를 잡고 있으면 **세그폴트(exit 139)로 조용히 죽는다** — 파이썬
> 에러 메시지 없이 중간 파일만 남는다(실제로 v1 as-of 빌드가 1월만 쓰고 죽었다).
>
> ```powershell
> Get-CimInstance Win32_OperatingSystem | %{ "가용 {0:N1} GB" -f ($_.FreePhysicalMemory/1MB) }
> Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
>     Select ProcessId, @{n='GB';e={[math]::Round($_.WorkingSetSize/1GB,1)}}
> ```
>
> **Data Wrangler 로 v1·v2·v3 파일을 통째로 열지 말 것.** 열이 1,428~5,196개라
> 디스크 163MB 짜리가 메모리에서 **30GB 이상**이 된다(결측이 86% 라 압축만 잘 될 뿐이다).
> 필요한 열만 뽑아 작은 파일을 만들어 열 것.

> ⚠️ 실행할 때 항상 **`$env:PYTHONIOENCODING='utf-8'`** (PowerShell) 을 붙인다. 콘솔이 cp949 라
> 스크립트의 `—`(em dash) 출력에서 `UnicodeEncodeError` 로 죽는다(데이터 문제가 아니라 진행 전 중단).
> 파이썬은 팀 공용 환경 `C:\panjiva\envs\main\Scripts\python.exe` (pandas 3.0 / pyarrow 25).

---

## 0. 전체 흐름

```
 [Snowflake]
     │
     ├─ src_trade_pull.py  ──→  source\trade\           (무역 원천: 수입 2007-07~2025-12, 수출 2024 — SQL 포함)
     └─ src_ciq_pull.py    ──→  source\ciq_ref\         (CIQ 원천: 기업·소유구조 PIT/스냅샷·재무 전 기간·환율)
                                      │
                          src_ciq_fin_build.py --start --end --join
                                      ↓
                                source\ciq_fin\         (v1~v3 공용 재무층 — 거래 시작−2년 ~ 종료)
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        ↓                             ↓                             ↓
   tom_v1_...  --join equi|asof  tom_v2_...  --join           wf_v3_...  (v1·v2 폴더를 _asof 로 지정)
   (선적 감사층)                  (표준 통합층)                  (within-firm 패널 + 수출·원산국 패널)
                                                                    │
                          compare_join_modes.py (두 판 대조 + 문서 대조) · audit_v1v2v3.py (교차 감사)

   v4_build_all.py  ──→  v4_pairhs_full\   (쌍×분기×HS6 무역 팩트 + 전 기간 재무층, 별도 파이프라인 — §8)
```

**Snowflake 를 다시 치는 것은 `src_*_pull.py` 둘뿐이다.** 그 아래는 전부 로컬 파일 가공이라 재접속이 필요 없다.
**두 판(equi / as-of)** 은 같은 코드에 `--join` 인자만 다르고, 정본은 명세 §3.3 의 **as-of 판(`*_asof` 폴더)** 이다.

---

## 1. 무역 원천 — `src_trade_pull.py`

SQL 이 스크립트 안에 있고 **기간·방향·컬럼이 전부 인자**다. 표준필터(미국 실착·통과화물
제외)와 HS 재결합(4,000자 절단 대응), crosswalk(`activeFlag=1`, 동일 panjivaid 는 낮은 companyId)·
PIT(닫힌 구간) 조회가 SQL 안에 들어 있다. 출력 폴더 기본값은 **`source\trade`** — 새 월·새 연도는
그냥 여기에 받으면 된다(가공 스크립트가 파일명 `YYYYMM` 으로 월을 발견한다).

```powershell
# 2026년 상반기를 추가로 받기 (수입만)
python scripts\extraction\src_trade_pull.py --start 2026-01-01 --end 2026-07-01 --directions imp

# 특정 월만 다시
python ... --months 202403 202404 --force

# 컬럼을 더 빼거나 더 받기 · SQL 만 보기
python ... --exclude conFullAddress notifyParty --include someExtraCol
python ... --print-sql
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간. `--end` 는 **미포함**. 월 경계로 자동 분할된다 |
| `--months` | 특정 월만 (`YYYYMM` 나열). `--start/--end` 대신 쓴다 |
| `--directions` | `imp` `exp` (기본 둘 다). 수출은 현재 2024 만 받아 두었다 |
| `--include` `--exclude` | 받을/뺄 컬럼 이름 |
| `--out-dir` | 출력 폴더 (기본 `source\trade`) |
| `--force` | 이미 있는 월도 다시 받는다 (기본은 건너뜀 — 명세 §10) |
| `--print-sql` | SQL 만 출력 |

산출: `imp_ship_YYYYMM.parquet` · `imp_hs_YYYYMM.parquet` (수출은 `exp_*`) · `_run_log.md`(월별 쿼리기간·행수·고유선적·크기)
· `_crosswalk_dup.md`(실행마다 crosswalk 중복 건수 1줄 — 명세 §3.1). 추출 직후 **행 수 ≠ 고유 선적 수**면 예외를 던지고
파일을 남기지 않는다(구간조인 행 증식 감지).

> 수출 B/L 에는 **상대방(consignee) 식별자가 없다**(구조적). `frob` 컬럼도 없다
> (수출은 100% 미국 선적항이라 필요 없음). 원천 폴더의 규칙·이력은 `source\DECISIONS.md`, 컬럼은 `source\trade\COLUMNS.md`.

## 2. CIQ 원천 — `src_ciq_pull.py`

```powershell
# 재무를 특정 연도만 (period_end 연도 기준으로 파일이 쪼개진다)
python scripts\extraction\src_ciq_pull.py --only fin_data --fin-years 2027

# 기업·소유구조·환율만 다시
python ... --only company ownership_pit ownership_snapshot fx_rate --force
```

| 인자 | 뜻 |
|---|---|
| `--only` | 특정 산출물만 (`company` `fin_period` `fin_data` `ownership_pit` `ownership_snapshot` `fx_rate` `ref_*`) |
| `--fin-years` | 재무 값을 받을 연도 (기본 1990 ~ 현재연도+1) |
| `--out-dir` `--force` | 〃 |

**현재 상태**: 재무 `fin_data_1976~2026` 51파일 **3,666,564,112행**, PIT 43,659,158행, 기업 40,814,746행.
다시 받을 일은 거의 없다 — 새 해가 되면 `--fin-years <새 연도>` 만.

## 3. 공용 재무층 — `src_ciq_fin_build.py` (v1~v3 용)

원천을 무역에 등장하는 회사로 좁혀 재가공한다. **Snowflake 를 치지 않는다.**

```powershell
# 2024 as-of 판용 (시작 2년 소급분 포함 → cal_year 2022~2024)
python scripts\extraction\src_ciq_fin_build.py --start 2024-01-01 --end 2025-01-01 --join asof `
    --trade-dir C:\panjiva\data\staging\source\trade --out-dir C:\panjiva\data\staging\source\ciq_fin

# 전 기간 as-of (2005~2025)
python ... --start 2007-07-01 --end 2026-01-01 --join asof --out-dir ...\source\ciq_fin_full
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` `--join` | 거래 기간과 결합 방식. `--cal-years` 를 안 주면 **`y0 = start.year − (2 if asof else 0)`, `y1 = (end − 1일).year`** 로 유도 |
| `--cal-years A B` | 담을 `cal_year` 범위(양끝 포함) 명시. **`period_end` 가 아니라 `cal_year` 로 자른다** — 무역과 붙이는 키가 그것이기 때문 |
| `--trade-dir` `--ciq-dir` `--out-dir` | 회사 범위를 긁어올 무역 원천(수출 파일이 없는 월은 건너뜀) · CIQ 원천 · 출력 |
| `--force` | 이미 있는 산출을 다시 만든다 |

**기간을 정하는 요령**: **as-of 는 최대 2년 소급하므로 시작 연도 − 2 부터** 있어야 하고, equi 는 거래 연도만 있으면 된다.
시차변수(L1·L4)를 쓸 거면 그만큼 앞을 더 담는다. 현재 `source\ciq_fin` 은 2022~2024 (2024 as-of 판 기준).
근거·플래그 규칙은 `source\ciq_fin\COLUMNS.md` §7.

## 4. v1 선적 감사층 — `tom_v1_shipment_master.py`

```powershell
$env:PYTHONIOENCODING='utf-8'
# as-of 정본 (2024)
python scripts\extraction\tom_v1_shipment_master.py --start 2024-01-01 --end 2025-01-01 --join asof `
    --trade-dir C:\panjiva\data\staging\source\trade --fin-dir C:\panjiva\data\staging\source\ciq_fin `
    --out-dir C:\panjiva\data\staging\tom_v1_2024_asof
# equi 대안본
python ... --join equi --out-dir C:\panjiva\data\staging\tom_v1_2024

# 02(이송미) override 승인 파일을 적용해 다시
python ... --override "C:\panjiva\shared memory\BECRS_Matching_Project\crosswalk_overrides.csv" --force
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간(선택). 없으면 원천 폴더에 있는 **전 기간**. 월별 파일로 나뉜다 |
| `--trade-dir` `--fin-dir` `--ciq-dir` `--out-dir` | 입력 세 곳과 출력 (기본 `source\trade` · `source\ciq_fin` · `source\ciq_ref`) |
| `--join` | `asof`(명세 §3.3, 시점 컬럼 `*_age_days`) / `equi`(시점 컬럼 `*_days_after_close`) |
| `--override` | 명세 §7.2 스키마 csv. **`status=approved` 이고 `pi_approved_by` 가 있는 행만** 적용, `action` replace/force_unmatched, `effective_start/end` 지원, 교체된 행은 UP 을 PIT→스냅샷으로 재조회. 없으면 적용 0건 |
| `--force` | 이미 있는 월도 다시 만든다 |
| `--chunk` `--nrows` | 메모리 청크(기본 400,000) · 디버그용 앞 N 행만(스모크 전용) |

산출: `shipment_master_YYYYMM.parquet` + `override_impact.csv`(적용 건수·수정 기업·선적·금액·UP 변경·관계 변경; 0건이면 0)
+ `override_impact_detail.csv`. **소요**: 월당 약 100초 · 약 170MB · 1,428열 · RAM 약 20GB.

## 5. v1 검증 — `tom_v1_90_checks.py`

```powershell
python scripts\extraction\tom_v1_90_checks.py --dir C:\panjiva\data\staging\tom_v1_2024_asof `
    --trade-dir C:\panjiva\data\staging\source\trade --benchmark none
python ... --dir ...\tom_v1_2024 --benchmark C:\panjiva\data\staging\tom_v1_2024h1     # 겹치는 월만 대사
```

| 인자 | 뜻 |
|---|---|
| `--dir` | 검증할 v1 폴더 (결합 방식은 컬럼 이름으로 자동 감지) |
| `--trade-dir` `--ciq-dir` | 대사할 원천 · UP 재조회 대조용 CIQ |
| `--benchmark` | 기존 검증본. **겹치는 월만 자동으로 골라 비교**한다. `none` 이면 생략 |
| `--expect-months` | 기대 파일 수. 0 이면 실제 개수를 그대로 쓴다 |

산출: `--dir` 안에 `90_checks.md`. G12 는 override 원본 보존과 교체행 UP 재조회를 **실검사**한다.

## 5b. v2 패널 — `tom_v2_panels.py` · `tom_v2_90_checks.py`

선적 base 로 **v1 산출물**을 읽어 패널 3종을 만든다. Snowflake 를 치지 않는다.

```powershell
python scripts\extraction\tom_v2_panels.py --start 2024-01-01 --end 2025-01-01 --join asof `
    --v1-dir C:\panjiva\data\staging\tom_v1_2024_asof --fin-dir C:\panjiva\data\staging\source\ciq_fin `
    --out-dir C:\panjiva\data\staging\tom_v2_2024_asof
python ... --only 03_firm --force                 # 특정 패널만 다시

python scripts\extraction\tom_v2_90_checks.py --dir C:\panjiva\data\staging\tom_v2_2024_asof `
    --v1-dir C:\panjiva\data\staging\tom_v1_2024_asof
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간(선택; 없으면 v1 폴더의 월 전부) |
| `--v1-dir` `--fin-dir` `--ciq-dir` `--out-dir` | 선적 base = v1 산출물 · 공용 재무층 · CIQ · 출력 |
| `--join` | v1 과 같은 결합 방식을 준다 |
| `--only` | `02_pair` `03_firm` `04_group` 중 일부만 |
| `--force` | 이미 있는 패널도 다시 만든다 (기본은 건너뜀) |

**소요**(2024 12개월): 02_pair 172초 · 03_firm 102초 · 04_group 52초 = 약 6분. 파일 630 + 401 + 154 = **1.18GB**.
> v1 을 다시 만들면 v2 도 다시 만들어야 한다 — v2 는 v1 을 읽는다.

## 5c. v3 within-firm 분석패널 — `wf_v3_*.py`

v1·v2 산출물과 공용 원천을 읽는다. **Snowflake 를 치지 않는다.** 순서대로 실행한다.
**as-of 판을 만들려면 `--v1-dir`·`--v2-dir`·`--out-dir` 에 `_asof` 폴더를 준다** — v3 스크립트에는 `--join` 이 없고,
결합 방식은 v1 폴더 스키마에서 자동 감지해 리포트 헤더(`**결합** equi|asof`)와 통계표 폴더(`wf2024` / `wf2024_asof`)를 정한다.

```powershell
$S = 'C:\panjiva\data\staging'; $D = 'scripts\processing'
# 1) 패널 6종 (약 3분): panel_pair_month · dim_relationship · panel_firm_quarter · panel_firm_origin_hs
#                     + panel_firm_export_quarter(미국 수출 firm×분기) · panel_firm_origin_quarter(수입 firm×원산국×분기)
python $D\wf_v3_panels.py --start 2024-01-01 --end 2025-01-01 --v1-dir $S\tom_v1_2024_asof --v2-dir $S\tom_v2_2024_asof `
    --src-dir $S\source\trade --out-dir $S\within_firm_pilot_2024_asof

# 2) 소유구조 변화·관계전환 d8·d9 (약 2분) — 패널 다음에
python $D\wf_v3_d8d9.py --start 2024-01-01 --end 2025-01-01 --v1-dir $S\tom_v1_2024_asof --v3-dir $S\within_firm_pilot_2024_asof `
    --ciq-dir $S\source\ciq_ref

# 3) 통계표 t1~t10 · 진단 d1~d7 + d8·d9 삽입 → 95_report.md (약 1분)
python $D\wf_v3_stats.py --start 2024-01-01 --end 2025-01-01 --v1-dir $S\tom_v1_2024_asof --v3-dir $S\within_firm_pilot_2024_asof `
    --src-dir $S\source\trade

# 4) 검증 — 게이트 22개
python $D\wf_v3_90_checks.py --start 2024-01-01 --end 2025-01-01 --dir $S\within_firm_pilot_2024_asof --v1-dir $S\tom_v1_2024_asof `
    --v2-dir $S\tom_v2_2024_asof --src-dir $S\source\trade --ciq-dir $S\source\ciq_ref

# 5) Stata 핸드오프 (약 5분, .dta 12GB 생성)
python $D\wf_v3_stata.py --v3-dir $S\within_firm_pilot_2024_asof --fin-dir $S\source\ciq_fin
python ... --only dim_relationship                 # 일부만 — 문서의 다른 패널 행은 유지된다
```

| 스크립트 | 산출 | 소요 |
|---|---|---|
| `wf_v3_panels.py` | 패널 6종 (`--start/--end/--v1-dir/--v2-dir/--src-dir/--out-dir`) | 약 3분 |
| `wf_v3_d8d9.py` | `d8_firm_ownership_change` · `d9_relationship_transition_pairs` + `95_d8d9_report.md` + `output\tables\wf…\d8_*·d9_*` (`--tables-dir` 기본 = `output\tables\wf{시작연도}[_asof]`) | 약 2분 |
| `wf_v3_stats.py` | `t1~t10` · `d1~d7` CSV + `95_report.md`(d8·d9 절 포함) | 약 1분 |
| `wf_v3_90_checks.py` | `90_checks.md` (게이트 22개; `--tables-dir` 로 d8 표를 읽는다) | 약 1분 |
| `wf_v3_stata.py` | `.dta` 6종 + `99_stata_varnames.csv` + `99_stata_handoff.md` | 약 5분 |

**의존 순서**: v1 → v2 → v3 패널 → d8·d9 → 통계 → 검증 → Stata. 앞 단계를 다시 만들면 뒤 단계도 다시 만들어야 한다.
`wf_v3_stats.py`·`wf_v3_90_checks.py`·`wf_v3_stata.py` 는 같은 폴더의 `wf_v3_d8d9.py`(결합 감지 헬퍼)·`wf_v3_panels.py`(수출 원천 로더)를 import 하므로 폴더째 있어야 한다.

> ⚠️ `panel_firm_quarter.dta` 는 **12GB** 다(무압축). Stata/**IC** 로는 열리지 않고 RAM 12GB 이상이 필요하다.
> 자세한 건 각 v3 폴더의 `99_stata_handoff.md`.

## 6. 판정 규칙 — `scripts\common\relationship.py`

기간과 무관하다. **v1·v2·v3 가 이 함수 하나를 공유**하므로, 관계분류 규칙을 바꾸면 여기만 고치고 세 버전을 다시 만들면 된다.

```python
from relationship import add_relationship
df = add_relationship(df)     # 관계 3분류 + 매칭상태 2종 + unmatched_reason + self_shipment + intra_group
```

## 6b. 재무 블록 부착 — `scripts\common\finblocks.py`

`(cal_year, cal_quarter)` equi-join 과 as-of(`merge_asof`, 소급 730일, 결산 당일 제외)를 모두 담은 기계다. v2·v3 가 쓴다.

> ⚠️ `tom_v1_shipment_master.py` 에 **같은 내용이 인라인으로 한 벌 더** 있다(v1 이 먼저
> 만들어져서). 규칙을 바꾸면 **두 곳을 함께** 고쳐야 한다(PI 보고 건의 ⑧: 통합 예정).

## 7. 교차 검증 — `compare_join_modes.py` · `audit_v1v2v3.py`

```powershell
# 두 판(equi·asof) 대조 + 문서 대조 게이트 → output\COMPARE_asof_vs_equi.md
python scripts\compare_join_modes.py --v1-dir $S\tom_v1_2024 --v2-dir $S\tom_v2_2024 --v3-dir $S\within_firm_pilot_2024
#   (as-of 폴더는 --asof-suffix 기본 '_asof' 로 유도; --v1-asof-dir 등으로 직접 지정 가능)

# 팀 함정 12종 · 명세 조항 · 버전 간 총계 · 문서 수치 교차 감사 → output\AUDIT_v1v2v3.md
python scripts\audit_v1v2v3.py --v1-dir $S\tom_v1_2024_asof --v2-dir $S\tom_v2_2024_asof --v3-dir $S\within_firm_pilot_2024_asof `
    --src-dir $S\source\trade --ciq-dir $S\source\ciq_ref --fin-dir $S\source\ciq_fin --tables-dir output\tables\wf2024_asof
```

**문서 대조 게이트의 규약**: 각 폴더의 `README.md`·`COLUMNS.md`·`DECISIONS.md` 는 equi·asof 두 판이 **결합 방식 단락만** 다르다.
그 단락은 `<!-- JOIN:equi --> … <!-- /JOIN -->` / `<!-- JOIN:asof --> … <!-- /JOIN -->` 로 감싸고, 나머지 본문은 폴더명(`_asof`)과
`days_after_close`↔`age_days` 토큰만 다르게 둔다. 게이트는 블록을 지우고 토큰을 정규화한 뒤 줄 단위 완전 일치를 요구한다 —
**한쪽 문서만 고치면 FAIL 이 난다.**

## 8. v4 — 쌍×분기×HS6 무역 팩트 + 전 기간 재무층 (`scripts\processing\v4_*.py`)

별도 파이프라인이며 `v4_build_all.py` 한 줄로 재현된다(1 원천검증 → 2 무역 팩트 → 3 무결성 검사 → 4 재무층 →
5 결합 검사 → 6 프로브, `--with-guide` 로 7 가이드 통계). 인자 통로: `--src` `--out` `--trade-years` `--fin-years A B`
`--force-fin` `--benchmark` `--pass N="…"` `--from-step N` `--dry-run`. 재무 연도는 무역 파일 연도 ±2 로 자동 유도.
**새 연도 추가** = 원천을 `source\trade` 에 받고 `v4_build_all.py` 재실행. 설계·컬럼·주의는 `data\staging\v4_pairhs_full\` 의
`00_전체그림.md` · `README_읽어보세요.md` · `COLUMNS.md` · `DECISIONS.md`(§6 재현·새 연도 절차).

---

## 9. 새 기간을 만들 때의 순서와 점검

1. **무역 원천** — `source\trade` 에 이미 수입 2007-07~2025-12 가 있다. 수출은 2024 만 → 필요하면 `src_trade_pull.py --directions exp`
   (Snowflake, 오래 걸림 — 백그라운드로, **단일 프로세스로**).
2. 새로 받은 월은 `_run_log.md` 의 행수·고유선적수와 Snowflake `count(*)` 를 **직접 대사**한다(`v4_10_verify_sources.py` 가 전 월을 대사해 준다).
3. **재무층** → `src_ciq_fin_build.py --start --end --join asof` (수 분). 원천 `fin_data_YYYY` 가 없는 해가 있으면 `src_ciq_pull.py --fin-years` 먼저.
4. **v1** → `tom_v1_shipment_master.py --join asof` (월당 100초 · 20GB RAM) → 5. **검증** `tom_v1_90_checks.py` 전항 PASS 확인.
6. **v2** → **v3**(패널 → d8·d9 → 통계 → 검증 → Stata) → `compare_join_modes.py` · `audit_v1v2v3.py`.
7. 산출 폴더마다 **`README.md` · `COLUMNS.md` · `DECISIONS.md` · `90_checks.md`** 를 둔다(그 폴더 안에서 완결, 다른 폴더 문서 참조 금지).
   두 판을 만들면 문서는 §7 의 JOIN 블록 규약으로 두 벌 유지.
8. `data\staging\_catalog.md` 에 산출물을 기록한다(팀 규약). `shared memory\...\03_산출물_버전맵_2024연간.md` 갱신.

> ⚠️ **`nohup ... &` 로 백그라운드 실행하지 말 것.** 과거에 같은 파일을 두 프로세스가
> 동시에 쓰다 parquet 5개가 손상됐다. 도구의 백그라운드 기능이나 단일 프로세스로 돌린다.

## 10. 전 기간(2007-07~2025, 222개월) 확장 예상 — 2024 실측 소요 기준

| 단계 | 소요(판당) | 용량 | 비고 |
|---|---|---|---|
| 수출 원천 추출 210개월 | Snowflake 약 2시간 | 약 3GB | 수입은 이미 있음 |
| 재무층 | 수 분 | — | `--start 2007-07-01 --end 2026-01-01 --join asof` → cal_year 2005~2025 |
| v1 | 약 6시간 (월 100초) | 약 38GB | RAM 20GB |
| v2 | 약 2시간 | 약 20GB | `02_pair` 5,196열 × 900만 행 — 메모리 위험, PI 보고 건의 ② 참조 |
| v3 | 약 4시간 | 수 GB | d8 기업표 3,300만 행/년 |
| Stata `.dta` | — | **220GB** | 슬림 서브셋만 넘길 것(건의 ⑦) |
| 검증·교차감사·문서 | 약 2시간 | — | |

합계 판당 약 1일(Snowflake 2시간 + 로컬 14시간). equi·asof 둘 다면 2일. **재무 인라인 구조와 `.dta` 규모를 먼저 바꾸지 않으면
용량이 현실적이지 않다** — `95_PI보고_김영수_2024연간파일럿.md` §3-⑩·§2.
