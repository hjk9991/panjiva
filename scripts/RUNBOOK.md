# RUNBOOK — 아무 기간이나 다시 만드는 법

**지금 만들어진 것은 2024년이지만, 어느 기간이든 인자만 바꿔 다시 만들 수 있다.**
스크립트 안에 박힌 연도는 전부 `default=` 값일 뿐이다.

담당: 03 김영수 · 명세: `shared memory\BECRS_Matching_Project\04_2024연간파일럿_통합명세.md` §10

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

> ⚠️ 실행할 때 항상 **`PYTHONIOENCODING=utf-8`** 을 붙인다. 콘솔이 cp949 라 스크립트의
> `—`(em dash) 출력에서 `UnicodeEncodeError` 로 죽는다(데이터 문제가 아니라 진행 전 중단).

---

## 0. 전체 흐름

```
 [Snowflake]
     │
     ├─ src_trade_pull.py  ──→  source\trade_YYYY\      (무역 원천, SQL 포함)
     └─ src_ciq_pull.py    ──→  source\ciq_ref\         (CIQ 원천, SQL 포함)
                                      │
                          src_ciq_fin_build.py
                                      ↓
                                source\ciq_fin\         (공용 재무층)
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        ↓                             ↓                             ↓
   tom_v1_...                    tom_v2_...                    wf_v3_...
   (선적 감사층)                  (표준 통합층)                  (within-firm 패널)
```

**Snowflake 를 다시 치는 것은 위 두 개(`src_*_pull.py`)뿐이다.** 그 아래는 전부 로컬 파일
가공이라 재접속이 필요 없다.

---

## 1. 무역 원천 — `src_trade_pull.py`

SQL 이 스크립트 안에 있고 **기간·방향·컬럼이 전부 인자**다. 표준필터(미국 실착·통과화물
제외)와 HS 재결합(4,000자 절단 대응)이 SQL 안에 들어 있다.

```bash
# 2018~2024 7년치를 새 폴더에
PYTHONIOENCODING=utf-8 python scripts/extraction/src_trade_pull.py \
    --start 2018-01-01 --end 2025-01-01 \
    --out-dir "C:\panjiva\data\staging\source\trade_2018_2024"

# 수입만, 특정 월만 다시
python ... --directions imp --months 202403 202404 --force

# 컬럼을 더 빼거나 더 받기
python ... --exclude conFullAddress notifyParty --include someExtraCol

# SQL 만 찍어보고 실행하지 않기 (검토용)
python ... --print-sql
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간. `--end` 는 **미포함**. 월 경계로 자동 분할된다 |
| `--months` | 특정 월만 (`YYYYMM` 나열). `--start/--end` 대신 쓴다 |
| `--directions` | `imp` `exp` (기본 둘 다) |
| `--include` `--exclude` | 받을/뺄 컬럼 이름 |
| `--out-dir` | 출력 폴더 |
| `--force` | 이미 있는 월도 다시 받는다 (기본은 건너뜀 — 명세 §10) |
| `--print-sql` | SQL 만 출력 |

산출: `imp_ship_YYYYMM.parquet` · `imp_hs_YYYYMM.parquet` (수출은 `exp_*`) · `_run_log.md`

> 수출 B/L 에는 **상대방(consignee) 식별자가 없다**(구조적). `frob` 컬럼도 없다
> (수출은 100% 미국 선적항이라 필요 없음).

## 2. CIQ 원천 — `src_ciq_pull.py`

```bash
# 재무를 1976~2026 전 기간
python scripts/extraction/src_ciq_pull.py --only fin_data \
    --fin-years $(seq 1976 2026)

# 기업·소유구조·환율만 다시
python ... --only company ownership_pit ownership_snapshot fx_rate --force
```

| 인자 | 뜻 |
|---|---|
| `--only` | 특정 산출물만 (`company` `fin_period` `fin_data` `ownership_pit` `fx_rate` …) |
| `--fin-years` | 재무 값을 받을 연도 (`period_end` 기준으로 파일이 쪼개진다) |
| `--out-dir` `--force` | 〃 |

**현재 상태**: 재무 `fin_data_1976~2026` 51파일 **3,666,564,112행**. 다시 받을 일은
거의 없다 — 이미 전 기간이다.

## 3. 공용 재무층 — `src_ciq_fin_build.py`

원천을 무역에 등장하는 회사로 좁혀 재가공한다. **Snowflake 를 치지 않는다.**

```bash
# 2018~2024 무역에 맞춰 재무층을 다시
PYTHONIOENCODING=utf-8 python scripts/extraction/src_ciq_fin_build.py \
    --cal-years 2018 2024 \
    --trade-dir "C:\panjiva\data\staging\source\trade_2018_2024" \
    --out-dir   "C:\panjiva\data\staging\source\ciq_fin_2018_2024"
```

| 인자 | 뜻 |
|---|---|
| `--cal-years` | 담을 `cal_year` 범위(양끝 포함). **`period_end` 가 아니라 `cal_year` 로 자른다** — 무역과 붙이는 키가 그것이기 때문 |
| `--trade-dir` | 회사 범위를 긁어올 무역 원천 |
| `--out-dir` | 출력 폴더 |

**기간을 정하는 요령**: **거래 기간과 같게 잡으면 된다.** equi-join 은 거래 연도의 재무만
쓰므로 소급분이 필요 없다(as-of 시절의 "소급 2년" 개념은 더 이상 없다).

앞으로 더 받는 것은 **lag 용 여유분**일 때만 의미가 있다 — 표본 시작 시점에서
`L1`·`L4` 같은 시차변수를 쓰려면 그만큼 앞 기간이 있어야 한다.

```
거래 2018~2024 · lag 안 씀        →  --cal-years 2018 2024
거래 2018~2024 · 최대 4분기 lag   →  --cal-years 2017 2024
거래 2024      · 연 단위 1기 lag  →  --cal-years 2023 2024
```

> 현재 2024 빌드는 `--cal-years 2022 2024` 로 잡혀 있다. **v1 은 그중 `cal_year=2024`
> 74,096행(33%)만 쓴다.** 나머지 2022~2023 은 v2·v3 패널에서 시차변수를 만들 여유분으로
> 남겨둔 것이고, 있어도 v1 결과에는 아무 영향이 없다(안 쓰는 행일 뿐).

## 4. v1 선적 감사층 — `tom_v1_shipment_master.py`

```bash
PYTHONIOENCODING=utf-8 python scripts/extraction/tom_v1_shipment_master.py \
    --start 2018-01-01 --end 2025-01-01 \
    --trade-dir "C:\panjiva\data\staging\source\trade_2018_2024" \
    --fin-dir   "C:\panjiva\data\staging\source\ciq_fin_2018_2024" \
    --out-dir   "C:\panjiva\data\staging\tom_v1_2018_2024"

# 02(이송미) crosswalk override 승인 파일이 나오면
python ... --override "path\to\crosswalk_override.parquet" --force
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간. 월별 파일로 나뉜다 |
| `--trade-dir` `--fin-dir` `--out-dir` | 입력 두 곳과 출력 |
| `--override` | 승인된 crosswalk override 파일. 없으면 적용 0건 |
| `--force` | 이미 있는 월도 다시 만든다 |

**소요**: 월당 약 100초 · 월당 약 170MB · 1,425열.

## 5. v1 검증 — `tom_v1_90_checks.py`

```bash
PYTHONIOENCODING=utf-8 python scripts/extraction/tom_v1_90_checks.py \
    --dir       "C:\panjiva\data\staging\tom_v1_2018_2024" \
    --trade-dir "C:\panjiva\data\staging\source\trade_2018_2024" \
    --benchmark "C:\panjiva\data\staging\tom_v1_2024"     # 겹치는 월만 대사
```

| 인자 | 뜻 |
|---|---|
| `--dir` | 검증할 v1 폴더 |
| `--trade-dir` | 대사할 원천 |
| `--benchmark` | 기존 검증본. **겹치는 월만 자동으로 골라 비교**한다. `none` 이면 생략 |
| `--expect-months` | 기대 파일 수. 0 이면 실제 개수를 그대로 쓴다 |

산출: `--dir` 안에 `90_checks.md`.

## 5b. v2 패널 — `tom_v2_panels.py` · `tom_v2_90_checks.py`

선적 base 로 **v1 산출물**을 읽어 패널 3종을 만든다. Snowflake 를 치지 않는다.

```bash
PYTHONIOENCODING=utf-8 python scripts/extraction/tom_v2_panels.py     --start 2018-01-01 --end 2025-01-01     --v1-dir  "C:\panjiva\data\staging	om_v1_2018_2024"     --fin-dir "C:\panjiva\data\staging\source\ciq_fin_2018_2024"     --out-dir "C:\panjiva\data\staging	om_v2_2018_2024"

python ... --only 03_firm                 # 특정 패널만 다시

PYTHONIOENCODING=utf-8 python scripts/extraction/tom_v2_90_checks.py     --dir    "C:\panjiva\data\staging	om_v2_2018_2024"     --v1-dir "C:\panjiva\data\staging	om_v1_2018_2024"
```

| 인자 | 뜻 |
|---|---|
| `--start` `--end` | 기간 (v1 에서 읽어올 월) |
| `--v1-dir` | 선적 base = v1 산출물 |
| `--fin-dir` `--out-dir` | 공용 재무층 · 출력 |
| `--only` | `02_pair` `03_firm` `04_group` 중 일부만 |

**소요**(2024 12개월): 02_pair 172초 · 03_firm 102초 · 04_group 52초 = 약 6분.
파일 630 + 401 + 154 = **1.18GB**.

> v1 을 다시 만들면 v2 도 다시 만들어야 한다 — v2 는 v1 을 읽는다.

## 5c. v3 within-firm 분석패널 — `wf_v3_*.py`

v1·v2 산출물과 공용 원천을 읽는다. **Snowflake 를 치지 않는다.** 순서대로 실행한다.

```bash
D=scripts/processing
PY="PYTHONIOENCODING=utf-8 python"

# 1) 패널 4종 (약 3분)
$PY $D/wf_v3_panels.py --start 2018-01-01 --end 2025-01-01     --v1-dir "...	om_v1_2018_2024" --v2-dir "...	om_v2_2018_2024"     --src-dir "...\source	rade_2018_2024" --out-dir "...\within_firm_pilot_2018_2024"

# 2) 소유구조 변화·관계전환 d8·d9 (약 2분) — 패널 다음에
$PY $D/wf_v3_d8d9.py --start ... --end ... --v1-dir ... --v3-dir ... --tables-dir ...

# 3) 통계표 t1~t5 · 진단 d1~d7 (약 2분)
$PY $D/wf_v3_stats.py --start ... --end ... --v1-dir ... --v3-dir ... --src-dir ...

# 4) 검증 — 게이트 15개
$PY $D/wf_v3_90_checks.py --dir ... --v1-dir ... --v2-dir ... --src-dir ... --ciq-dir ...

# 5) Stata 핸드오프 (약 5분, .dta 12GB 생성)
$PY $D/wf_v3_stata.py --v3-dir ... --fin-dir ...
```

| 스크립트 | 산출 | 소요 |
|---|---|---|
| `wf_v3_panels.py` | `panel_pair_month` · `dim_relationship` · `panel_firm_quarter` · `panel_firm_origin_hs` | 약 3분 |
| `wf_v3_d8d9.py` | `d8_*` · `d9_*` + `95_d8d9_report.md` | 약 2분 |
| `wf_v3_stats.py` | `t1~t5` · `d1~d7` CSV + `95_report.md` | 약 2분 |
| `wf_v3_90_checks.py` | `90_checks.md` (게이트 15개) | 약 2분 |
| `wf_v3_stata.py` | `.dta` 4종 + `99_stata_varnames.csv` | 약 5분 |

**의존 순서**: v1 → v2 → v3 패널 → d8·d9 → 통계 → 검증 → Stata.
앞 단계를 다시 만들면 뒤 단계도 다시 만들어야 한다.

> ⚠️ `panel_firm_quarter.dta` 는 **12GB** 다(무압축). Stata/**IC** 로는 열리지 않고
> RAM 12GB 이상이 필요하다. 자세한 건 `99_stata_handoff.md`.

## 6. 판정 규칙 — `scripts\common\relationship.py`

기간과 무관하다. **v1·v2·v3 가 이 함수 하나를 공유**하므로, 관계분류 규칙을 바꾸면
여기만 고치고 세 버전을 다시 만들면 된다.

```python
from relationship import add_relationship
df = add_relationship(df)     # 관계 3분류 + 매칭상태 2종 + unmatched_reason
                              # + self_shipment + intra_group
```

## 6b. 재무 블록 부착 — `scripts\common\finblocks.py`

`(cal_year, cal_quarter)` equi-join 으로 재무 블록을 붙이는 기계다. v2·v3 가 쓴다.

```python
from finblocks import load_fin_layer, attach_block_keys, write_with_blocks
layer = load_fin_layer(FIN_DIR)
df = attach_block_keys(df, [(접두어, 회사id컬럼, periodTypeId튜플), ...],
                       layer["per"], ref_date="period_start")
write_with_blocks(df, out_path, blocks, layer)
```

> ⚠️ `tom_v1_shipment_master.py` 에 **같은 내용이 인라인으로 한 벌 더** 있다(v1 이 먼저
> 만들어져서). 규칙을 바꾸면 **두 곳을 함께** 고쳐야 한다.

---

## 7. 새 기간을 만들 때의 순서와 점검

1. **무역 원천** → `src_trade_pull.py` (Snowflake, 오래 걸림 — 백그라운드로)
2. 행 수를 Snowflake `count(*)` 와 **직접 대사**한다. 월별로 하나씩.
3. **재무층** → `src_ciq_fin_build.py --cal-years` (약 1분)
4. **v1** → `tom_v1_shipment_master.py` (월당 100초)
5. **검증** → `tom_v1_90_checks.py` — 전항 PASS 인지 확인
6. `data\staging\_catalog.md` 에 산출물을 기록한다 (팀 규약)
7. 그 폴더에 `DECISIONS.md`(결정 근거)·`COLUMNS.md`(변수 가이드)를 둔다

> ⚠️ **`nohup ... &` 로 백그라운드 실행하지 말 것.** 과거에 같은 파일을 두 프로세스가
> 동시에 쓰다 parquet 5개가 손상됐다. 도구의 백그라운드 기능이나 단일 프로세스로 돌린다.

## 8. 아직 이 문서에 없는 것

- **v4** (쌍×HS×분기 무역 팩트) — 다른 세션에서 진행 중.
  `scripts\processing\v4_*.py` · 산출물 `data\staging\v4_pairhs_2024\`
