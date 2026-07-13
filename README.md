# Panjiva 경제정책 연구 협업 저장소

A 서버(고성능 PC)에 원격 접속해 Panjiva/CapIQ 데이터를 분석하는 팀 저장소입니다.

## 처음 왔다면 — 읽는 순서

| 순서 | 문서 | 내용 |
|---|---|---|
| ① | [docs/onboarding-A-remote.md](docs/onboarding-A-remote.md) | **최초 서버 접속 방법** — SSH 키 생성, 첫 접속, VS Code 원격 연결, Python/Stata 사용 (신규 팀원은 여기부터) |
| ② | [docs/team-workflow.md](docs/team-workflow.md) | 일상 작업 흐름 — 폴더 구조, git 사용법(clone·pull·push), 저장 위치 규칙, discussion 소통 |
| ③ | [docs/snowflake-data-workflow.md](docs/snowflake-data-workflow.md) | Panjiva/CapIQ 데이터를 Snowflake에서 추출하는 방법 |

> 아직 A 서버에 접속할 수 없는 상태(= 이 repo를 clone하기 전)라면, ① 문서를 관리자에게
> 파일로 받아서 보세요. 막히면 문서를 Claude Code에 붙여넣고 에러 메시지와 함께 질문하면 됩니다.

## 폴더 안내

```
scripts/      추출(extraction)·정리(processing)·분석(analysis) 코드
output/       그림(figures)·표(tables) — 스크립트로 재생성 가능해야 함
draft/        원고
discussion/   팀 소통 기록 — 00_directions.md(지시사항)부터 확인
docs/         위 안내 문서 3종
```

대용량 데이터는 이 repo에 없습니다 — A 서버의 `C:\panjiva\data\` (raw/staging/processed)에 있고, 절대 서버 밖으로 복사하지 않습니다.
