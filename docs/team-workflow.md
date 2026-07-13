# 팀 작업 워크플로 — 데이터 → 분석 → 산출물 → 소통

> **대상**: 팀원 전원 (b, c, d, e + 관리자)
> **전제**: [A 서버 온보딩](onboarding-A-remote.md) 완료 (VS Code로 A 접속 가능한 상태)
> **함께 보기**: 데이터 추출은 [snowflake-data-workflow.md](snowflake-data-workflow.md)
> **막히면**: 이 문서를 Claude Code에 붙여넣고 질문하세요.

---

## 0. 한눈에 보는 구조

작업 공간은 **두 층**으로 나뉩니다. **git이 관리하는 것**(코드·산출물·문서)과 **A 로컬에만 있는 것**(대용량 데이터)입니다.

```
[GitHub: hjk9991/panjiva]  ←── push / pull ──→  각자의 clone (A 서버 위)
                                                C:\panjiva\projects\<사번>\
                                                ├── scripts\      ← 코드
                                                │   ├── extraction\   (Snowflake 추출)
                                                │   ├── processing\   (데이터 정리)
                                                │   └── analysis\     (통계 분석)
                                                ├── output\       ← 산출물
                                                │   ├── figures\      (그림)
                                                │   └── tables\       (표)
                                                ├── draft\        ← 원고
                                                ├── discussion\   ← 팀 소통 기록 (md)
                                                └── docs\         ← 이 문서들

[A 로컬 공유 — git 밖, 대용량이라 GitHub에 올리지 않음]
C:\panjiva\data\raw\        원본 데이터 (읽기 전용 — 관리자만 쓰기)
C:\panjiva\data\staging\    Snowflake 추출 착륙지 (팀원 쓰기 가능)
C:\panjiva\data\processed\  정리 완료된 분석용 데이터 (팀원 쓰기 가능)
```

**핵심 개념**: 팀원 각자가 **자기 사번 폴더에 자기 clone**을 가집니다. 같은 폴더를 5명이 동시에 편집하면 저장 충돌이 나기 때문에, 각자 자기 복사본에서 작업하고 **GitHub을 통해 합칩니다**. 데이터(`C:\panjiva\data`)만 전원이 공유합니다.

---

## 1. 최초 1회 세팅 (팀원별, 5분)

1. **GitHub 계정**을 만들고 (이미 있으면 생략) 사용자명을 관리자에게 알려 `hjk9991/panjiva` repo의 **collaborator 초대**를 받습니다. 메일로 온 초대를 수락하세요.

2. **A의 VS Code 터미널(SSH: A)에서** git 사용자 정보 설정:

```powershell
git config --global user.name "본인이름(사번)"          # 예: "임희현(20220829)"
git config --global user.email "GitHub가입이메일"
```

> 커밋에 이 이름이 찍혀서 "누가 무엇을 했는지"가 기록됩니다. 반드시 본인 것으로 설정하세요.

3. **자기 사번 폴더로 clone**:

```powershell
git clone https://github.com/hjk9991/panjiva.git C:\panjiva\projects\<본인사번>
```

4. VS Code에서 `File → Open Folder` → `C:\panjiva\projects\<본인사번>` 열기. 앞으로 모든 작업은 이 폴더에서 합니다.

5. **첫 push 때 인증**: 사용자명 + 비밀번호를 물으면, 비밀번호 자리에 GitHub **Personal Access Token(PAT)** 을 넣습니다.
   - 본인 컴퓨터 브라우저에서 github.com → Settings → Developer settings → Personal access tokens → Generate (repo 권한 체크)
   - 한 번 입력하면 Windows 자격 증명 관리자에 저장되어 다시 묻지 않습니다.
   - 이 과정이 헷갈리면 Claude Code에 "GitHub PAT 만들어서 git 인증하는 법"을 물어보세요.

---

## 2. 하루 작업 사이클

```
① git pull          ← 시작 전: 다른 팀원의 변경사항 받기
② 작업               ← 스크립트 작성·수정, 분석 실행, 산출물 저장
③ discussion 기록    ← 작업 로그 md 업데이트 (5절)
④ git add/commit/push ← 끝나면: 내 변경사항 공유
```

VS Code 터미널 명령어로는:

```powershell
git pull                                # ① 아침에 자리 앉으면 제일 먼저
# ... 작업 ...                           # ②③
git add -A                              # ④ 변경된 파일 전부 담기
git commit -m "무엇을 왜 했는지 한 줄"     #    한글 커밋 메시지 OK
git push                                #    GitHub에 올리기
```

> git 명령이 어색하면 VS Code 왼쪽의 **Source Control 탭**(가지 모양 아이콘)으로 클릭만으로도 됩니다:
> 변경 파일 확인 → `+`(stage) → 메시지 입력 → Commit → Sync.

**규칙은 단 두 개입니다: "시작 전 pull, 끝나면 push."** 이것만 지키면 충돌의 90%가 예방됩니다.

---

## 3. 무엇을 어디에 저장하나

| 만든 것 | 저장 위치 | git |
|---|---|---|
| Snowflake에서 갓 추출한 데이터 | `C:\panjiva\data\staging\` | ✗ (대용량) |
| 정리·병합 완료된 분석용 데이터 | `C:\panjiva\data\processed\` | ✗ (대용량) |
| 추출/정리/분석 스크립트 | `scripts\extraction·processing·analysis\` | ✓ |
| 그림 (png, pdf) | `output\figures\` | ✓ |
| 표 (csv, tex, xlsx) | `output\tables\` | ✓ |
| 원고 (docx, tex, md) | `draft\` | ✓ |
| 작업 기록·지시사항 | `discussion\` | ✓ |

### 파일명 규칙

- **processed 데이터**: `<내용>_<기간>.parquet` (예: `us_imports_kr_2020_2025_clean.parquet`) — 만들 때마다 `C:\panjiva\data\processed\_catalog.md`에 한 줄 기록 (어느 스크립트로 만들었는지 포함)
- **output**: `fig_` / `tab_` 접두사 + 내용 (예: `fig_trade_trend_by_hs.png`, `tab_summary_stats.csv`)
- **원칙: output의 모든 파일은 scripts의 스크립트로 재생성 가능해야 합니다.** 수작업으로 그림을 고치지 마세요 — 스크립트를 고치고 다시 생성하세요. 어느 스크립트가 만들었는지는 스크립트 안에 저장 경로가 있으니 검색(`Ctrl+Shift+F`)으로 찾을 수 있습니다.

### 스크립트 안에서 경로 쓰는 법

clone 위치가 사람마다 다르므로 (사번 폴더), **repo 안은 상대 경로, 데이터는 절대 경로**로 씁니다:

```python
from pathlib import Path

DATA = Path(r"C:\panjiva\data")                    # 데이터: 절대 경로 (전원 공통)
REPO = Path(__file__).resolve().parents[2]          # repo 루트: 스크립트 기준 상대
OUT  = REPO / "output"

df = pd.read_parquet(DATA / "processed" / "us_imports_kr_2020_2025_clean.parquet")
# ... 분석 ...
fig.savefig(OUT / "figures" / "fig_trade_trend_by_hs.png", dpi=200)
```

---

## 4. 충돌 없이 협업하는 규칙 (git)

git 충돌은 **두 사람이 같은 파일의 같은 부분을 동시에 고쳤을 때만** 납니다. 그래서:

1. **스크립트 파일은 기본적으로 담당자 1명** — 남의 스크립트를 고칠 일이 있으면 discussion에서 먼저 말하고 진행
2. **시작 전 pull, 끝나면 push** — 오래 안 올리고 묵히는 것이 충돌의 최대 원인
3. **커밋은 작게, 자주** — 하루 몰아서 1번보다 작업 단위마다 여러 번이 안전
4. **대용량 파일(50MB 이상)은 git에 넣지 않기** — 데이터는 무조건 `C:\panjiva\data`로. (`.gitignore`가 parquet/csv를 자동으로 걸러주지만, `output\tables\`의 소형 csv는 예외로 허용되어 있습니다)

**충돌이 났을 때** (push가 거부되거나 `CONFLICT` 메시지):

```powershell
git status    # 어떤 파일이 충돌인지 확인
```

당황하지 말고 `git status` 출력 전체를 Claude Code에 붙여넣고 "충돌 해결 도와줘"라고 하세요. 데이터가 사라지는 일은 없습니다 — git은 양쪽 버전을 모두 보존합니다.

---

## 5. discussion 폴더 — 팀 소통 기록

말로 하면 흩어지는 논의를 **md 파일로 repo에 남기는** 구조입니다. push하면 그게 곧 팀 공지입니다.

```
discussion\
├── 00_directions.md          ← 지시사항·디렉션 (관리자/PI가 작성, 최신이 맨 위)
├── _template_task-log.md     ← 작업 로그 템플릿 (복사해서 시작)
├── us-imports-extraction.md  ← 작업 단위 로그 (예시)
└── hs85-analysis.md          ← 작업 단위 로그 (예시)
```

**운영 규칙:**

1. **`00_directions.md`** — 지시사항은 여기에만. 팀원은 매일 pull 후 이 파일부터 확인
2. **작업 하나 = 로그 파일 하나** — 새 작업을 시작하면 `_template_task-log.md`를 복사해 `<작업명>.md` 생성
3. **진행 상황이 바뀔 때마다 로그 맨 위에 추가** (최신이 위) — "어떤 데이터로, 어떤 스크립트를 돌려서, 뭐가 나왔고, 다음은 뭐" 형식
4. 다른 사람 작업에 의견이 있으면 그 사람 로그 파일에 직접 코멘트를 추가하고 push

템플릿 (`_template_task-log.md`):

```markdown
# 작업명: (예: 한국발 HS85 수입 추이 분석)

- 담당: 사번(이름)
- 시작: YYYY-MM-DD
- 관련 지시: 00_directions.md YYYY-MM-DD 항목
- 상태: 진행 중 / 검토 요청 / 완료

## 기록 (최신이 위)

### YYYY-MM-DD
- 한 일:
- 사용 데이터: (processed 파일명)
- 사용/수정 스크립트: (scripts 경로)
- 산출물: (output 경로)
- 다음 할 일 / 질문:
```

---

## 6. 엔드투엔드 예시 — 전체 흐름 한 바퀴

> 시나리오: "2020~2024 한국발 전기기기(HS 85) 수입 추이를 그려라"는 지시를 받았다.

1. `git pull` → `discussion\00_directions.md`에서 과제 확인
2. `discussion\hs85-trend.md` 로그 파일 생성 (템플릿 복사)
3. **추출**: [snowflake-data-workflow.md](snowflake-data-workflow.md) 절차대로 `scripts\extraction\pull_panjiva.py` 실행 → `C:\panjiva\data\staging\korea_electronics_2020_2024.parquet`
4. **정리**: `scripts\processing\clean_hs85.py` 작성·실행 → `C:\panjiva\data\processed\hs85_monthly.parquet` + `_catalog.md` 기록
5. **분석**: `scripts\analysis\hs85_trend.py` 작성·실행 → `output\figures\fig_hs85_trend.png`
6. **기록**: `discussion\hs85-trend.md`에 오늘 항목 추가
7. `git add -A` → `git commit -m "HS85 수입 추이: 추출·정리·그림 v1"` → `git push`

팀원 누구든 `git pull` 하면 스크립트·그림·로그가 그대로 받아지고, 데이터가 필요하면 `_catalog.md`를 보고 `C:\panjiva\data\processed`에서 찾으면 됩니다.

---

## 7. (부록) 관리자 체크리스트

- [ ] 팀원 GitHub 사용자명 수집 → repo Settings → Collaborators 초대
- [ ] processed 폴더 생성 + 권한 (A의 관리자 PowerShell, 1회):

```powershell
mkdir C:\panjiva\data\processed
icacls C:\panjiva\data\processed /grant "research:(OI)(CI)M"
```

- [ ] `C:\panjiva\data\processed\_catalog.md` 빈 파일 생성 (표 헤더만)
- [ ] `discussion\00_directions.md`에 첫 지시사항 작성
- [ ] 기존에 `C:\panjiva\projects` 바로 아래에서 작업하던 것이 있다면 각자 사번 clone으로 이전
