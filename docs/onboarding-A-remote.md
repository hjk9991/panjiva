# A 서버 원격 작업환경 온보딩 가이드

> **대상**: 경제정책 연구팀원 (본인 컴퓨터에서 A 서버에 원격 접속해 작업하려는 사람)
> **소요 시간**: 약 15~20분
> **이 문서 사용법**: 막히는 부분이 생기면 이 문서 전체를 본인 컴퓨터의 Claude Code에 붙여넣고
> "지금 O단계인데 이런 에러가 났어" 라고 물어보세요. 에러 메시지는 **전문을 그대로** 붙여넣는 것이 중요합니다.

---

## 0. 전체 구조 이해

```
팀원 컴퓨터 (당신)                        A 서버 (고성능 PC, 김형진B)
┌──────────────────┐                  ┌─────────────────────────────┐
│ VS Code          │── SSH (내부망) ──▶│ C:\panjiva\projects\<사번>    │
│ (Remote-SSH 확장) │  192.168.7.83   │   ← 각자의 작업장 (git clone) │
└──────────────────┘                  │ C:\panjiva\data      ← 데이터 │
                                      │   (읽기 전용, 반출 금지)        │
                                      │ C:\panjiva\envs\main ← 공용   │
                                      │   Python 가상환경             │
                                      │ Stata 19, MATLAB 설치됨       │
                                      └─────────────────────────────┘
```

- 코드 편집·실행은 **전부 A 위에서** 일어납니다. 당신 컴퓨터는 화면과 키보드 역할만 합니다.
- 계정은 **본인 사번**입니다 (예: `20230315`). 관리자에게 임시 비밀번호를 받으세요.
- 모든 팀원은 `research` 그룹 소속이며, 데이터 폴더는 읽기만 가능합니다.
- 대용량 SQL 작업은 A를 거치지 않고 **각자 Snowflake에 직접 접속**해도 됩니다.

### 시작 전 준비물

- [ ] 본인 사번 계정 + 임시 비밀번호 (관리자에게 요청)
- [ ] A 서버 IP: `192.168.7.83` (사내망 접속 상태여야 함)
- [ ] 본인 컴퓨터에 VS Code 설치

---

## 1단계. SSH 키 생성 (본인 컴퓨터에서)

PowerShell을 열고 (관리자 권한 불필요):

```powershell
ssh-keygen -t ed25519
```

질문이 나오면 전부 그냥 Enter. 그다음 공개키를 출력:

```powershell
type $env:USERPROFILE\.ssh\id_ed25519.pub
```

`ssh-ed25519 AAAA... 이름@컴퓨터` 형태의 **한 줄**이 나옵니다.

- 이 줄(공개키)은 노출되어도 안전합니다. 메신저/메일로 관리자에게 보내도 됩니다.
- 절대 보내면 안 되는 것: `id_ed25519` 파일 (개인키). 이건 본인 컴퓨터 밖으로 나가면 안 됩니다.

## 2단계. 첫 접속 — 비밀번호 사용 (본인 컴퓨터에서)

```powershell
ssh 본인사번@192.168.7.83
```

- 처음 접속하면 `The authenticity of host ... can't be established. Are you sure you want to continue connecting?` 이 나옵니다. **정상입니다.** `yes` 입력.
- 비밀번호 입력 시 화면에 아무것도 표시되지 않는 것도 정상입니다. **한/영 상태를 확인**하고 입력하세요.
- 성공하면 `PS C:\Users\본인사번.김형진B>` 같은 A 서버의 프롬프트가 뜹니다.
  (폴더명 뒤에 `.김형진B`가 붙는 것은 정상입니다.)

## 3단계. 공개키 등록 (방금 접속한 SSH 세션 안에서)

**접속한 상태 그대로** 아래를 실행합니다. `<공개키 한 줄>` 자리에 1단계에서 복사한 줄 전체를 넣으세요:

```powershell
mkdir $env:USERPROFILE\.ssh
Add-Content $env:USERPROFILE\.ssh\authorized_keys "<공개키 한 줄>"
exit
```

- 반드시 `$env:USERPROFILE` 경로를 그대로 쓰세요 (홈 폴더 이름을 직접 칠 경우 오타 위험).
- 공개키는 **중간에 줄바꿈 없이 한 줄**로 들어가야 합니다.

## 4단계. 키 인증 확인 (본인 컴퓨터에서)

```powershell
ssh 본인사번@192.168.7.83
```

이번에는 **비밀번호를 묻지 않고** 바로 접속되어야 합니다. 성공하면 `exit`로 나오세요.

## 5단계. SSH 접속 별칭 등록 (본인 컴퓨터에서)

메모장 등으로 `C:\Users\본인윈도우계정\.ssh\config` 파일을 만들고 (확장자 없음):

```
Host A
    HostName 192.168.7.83
    User 본인사번
    IdentityFile ~/.ssh/id_ed25519
```

저장 후 `ssh A` 만으로 접속되는지 확인하세요.

## 6단계. VS Code 원격 연결 (본인 컴퓨터에서)

1. VS Code → 확장(Extensions) 탭 → **"Remote - SSH"** (Microsoft) 설치
2. **중요 설정**: `F1` → `Preferences: Open User Settings (JSON)` → 아래 항목 추가:

```json
{
    "remote.SSH.remotePlatform": {
        "A": "windows"
    }
}
```

3. `F1` → `Remote-SSH: Connect to Host` → **A** 선택
   - 첫 접속은 1~2분 걸립니다 (A에 VS Code Server 자동 설치, 최초 1회만)
   - 완료되면 좌측 하단에 `SSH: A` 초록 표시
4. `File → Open Folder` → 본인 작업 폴더 `C:\panjiva\projects\본인사번` 열기
   (이 폴더는 [team-workflow.md](team-workflow.md) 1절의 git clone으로 만들어집니다 — 아직 없다면 그 절차를 먼저)
5. 확장 탭에서 **Python**, **Jupyter** 확장을 "SSH: A에 설치" 버튼으로 원격 쪽에 설치

### 동작 확인

`` Ctrl+` `` 로 터미널을 열고:

```powershell
hostname     # 김형진B 가 나오면 A에서 실행되고 있는 것
```

## 7단계. Python 공용 환경 사용

팀 공용 가상환경이 `C:\panjiva\envs\main`에 준비되어 있습니다.

- **VS Code에서**: `F1` → `Python: Select Interpreter` → `C:\panjiva\envs\main\Scripts\python.exe` 선택
- **터미널에서 직접 쓸 때**:

```powershell
C:\panjiva\envs\main\Scripts\Activate.ps1
python --version
```

- 패키지 추가가 필요하면 누구나 `pip install 패키지명` 하면 되고, 공용 환경이므로 **설치 후 팀에 공지**해 주세요.
- 기본 설치됨: pandas, numpy, pyarrow, matplotlib, jupyterlab, snowflake-connector-python

## 8단계. Stata 19 원격 사용

SSH 환경에는 화면이 없어서 Stata GUI 창은 띄울 수 없습니다. 대신 두 가지 방식을 씁니다.

### 방식 1 — 배치 실행 (do파일 통째로)

VS Code 터미널(SSH: A)에서:

```powershell
& "C:\Program Files\StataNow19\StataMP-64.exe" /e do C:\panjiva\projects\분석파일.do
```

- 실행 후 같은 폴더에 `분석파일.log`가 생깁니다. 결과는 로그로 확인.
- 실행 파일 이름은 에디션에 따라 다릅니다: `StataMP-64.exe` / `StataSE-64.exe` / `StataBE-64.exe`.
  `dir "C:\Program Files\StataNow19\"` 로 확인하세요.

### 방식 2 — 노트북에서 대화형 (추천)

Stata 17+부터 공식 Python 연동(pystata)이 있어 **Jupyter/VS Code 노트북 셀에서 Stata를 대화형으로** 쓸 수 있습니다.

최초 1회 (아무나 한 명이 하면 팀 전체 적용):

```powershell
C:\panjiva\envs\main\Scripts\pip install stata_setup
```

노트북(.ipynb) 첫 셀에서:

```python
import stata_setup
stata_setup.config("C:/Program Files/StataNow19", "mp")   # 에디션에 맞게 "mp"/"se"/"be"
```

이후 셀에서 `%%stata` 매직으로 Stata 명령 실행:

```python
%%stata
sysuse auto
summarize price mpg
```

결과 표와 그래프가 노트북에 바로 표시됩니다. Python(pandas)과 Stata 간 데이터 주고받기도 됩니다 (`%%stata -d df` 등 — 자세한 건 Claude Code에 물어보세요).

### ⚠️ 라이선스 주의

Stata 라이선스가 단일 사용자용이면 **동시에 여러 명이 Stata를 돌리면 안 됩니다.**
무거운 Stata 작업 전에는 팀 채널에 공지하고, 동시 사용 규칙은 관리자와 확인하세요.

---

## 협업 규칙

| 폴더 | 용도 | 권한 |
|---|---|---|
| `C:\panjiva\projects\<사번>` | 각자의 git clone — 코드·산출물·원고·discussion | 본인 작업 공간 |
| `C:\panjiva\data\raw` | 원본 대용량 데이터 | **읽기 전용** |
| `C:\panjiva\data\staging` | Snowflake 추출 착륙지 | 팀 전원 수정 가능 |
| `C:\panjiva\data\processed` | 정리 완료된 분석용 데이터 | 팀 전원 수정 가능 |

- **데이터는 절대 A 밖으로 복사하지 않습니다** (Panjiva/CapIQ 라이선스상 재배포 금지).
- 일상 작업 절차(git 사용법, 저장 위치 규칙, discussion 소통)는 [team-workflow.md](team-workflow.md)를 따릅니다.
- 산출물(그림·표)은 repo의 `output\` 폴더에 저장합니다. 최종본의 사외 공유만 OneDrive/mPower를 씁니다.
- 메모리를 많이 먹는 무거운 작업은 실행 전에 팀 채널에 공지합니다.
- 무거운 SQL(집계·조인)은 가급적 Snowflake 쪽에서 처리하고, A에는 필요한 결과만 내려받습니다.
- Panjiva/CapIQ 데이터 조회·추출 절차는 별도 문서 참고: [snowflake-data-workflow.md](snowflake-data-workflow.md)

---

## 트러블슈팅 (실제 겪었던 문제들)

### "Are you sure you want to continue connecting (yes/no)?"
정상입니다. 첫 접속 시 서버 지문 확인 절차이니 `yes`.

### 비밀번호 입력 후 `Connection reset by 192.168.7.83 port 22`
두 가지 원인이 있었습니다 (모두 A 쪽 문제, **관리자에게 요청**):
1. 계정에 "첫 로그인 시 비밀번호 변경" 플래그가 걸려 있는 경우. SSH는 비밀번호 변경 화면을 못 띄워서 연결을 끊습니다.
   → 관리자가 `net user 사번 임시비번 /logonpasswordchg:no` 실행
2. A의 SSH 기본 셸이 존재하지 않는 프로그램(예: 미설치된 PowerShell 7)으로 지정된 경우
   → 현재는 해결되어 있음. 재발 시 관리자가 레지스트리 `HKLM:\SOFTWARE\OpenSSH`의 `DefaultShell` 확인

### `Permission denied (publickey,password,keyboard-interactive)`
- 비밀번호 오타 또는 **한/영 상태** 확인이 1순위
- 반복 실패 시 관리자에게 요청: A에서 `Get-WinEvent -LogName "OpenSSH/Operational" -MaxEvents 15 | Format-List TimeCreated, Message` 실행하면 서버가 기록한 실제 원인이 보입니다
- 로그에 `invalid user`가 나오면 계정명 문제, `Failed password`면 순수 비밀번호 문제

### 키 등록했는데 여전히 비밀번호를 물어봄
- 공개키가 **한 줄로** 들어갔는지 확인 (중간 줄바꿈 금지)
- 등록 경로가 실제 홈 폴더인지 확인: SSH 세션에서 `type $env:USERPROFILE\.ssh\authorized_keys`
- 홈 폴더는 `C:\Users\사번.김형진B` 형태입니다. `C:\Users\사번`이 아닙니다.

### VS Code "Could not establish connection"
1. 설정 JSON에 `"remote.SSH.remotePlatform": {"A": "windows"}` 가 있는지 확인 (6단계 2번)
2. `F1` → `Remote-SSH: Kill VS Code Server on Host` → A → 재접속
3. 그래도 안 되면: `View → Output → 드롭다운에서 "Remote - SSH"` 로그 전체를 복사해서 Claude Code에 붙여넣고 물어보기
4. 참고: 과거에 A에서 일반 사용자의 WMI 조회가 막혀 같은 에러가 났었습니다. 로그에
   `gcim : 액세스가 거부되었습니다` / `no sshd parent proc` 가 보이면 관리자에게 알리세요.
   (해결책: `wmimgmt.msc` → Root\CIMV2 보안 → research 그룹에 **"계정 사용" + "원격 사용"** 허용.
   SSH는 네트워크 로그인이라 "원격 사용" 권한까지 필요합니다. 이미 적용되어 있어 신규 팀원은 해당 없음)

### `Activate.ps1 cannot be loaded` (실행 정책 오류)
관리자에게 A에서 `Set-ExecutionPolicy -Scope LocalMachine RemoteSigned` 실행 요청 (이미 적용되어 있으면 해당 없음)

### A가 응답하지 않음
- 본인 컴퓨터가 사내망에 연결되어 있는지 확인
- A가 꺼져 있거나 절전 모드일 수 있음 → 관리자에게 확인

---

## Claude Code에 물어볼 때 팁

1. **이 문서 전체를 먼저 붙여넣고** 상황을 설명하세요. 환경(서버 IP, 폴더 구조, 계정 체계)을 알아야 정확한 답이 나옵니다.
2. 에러 메시지는 요약하지 말고 **전문을 그대로** 붙여넣으세요.
3. "본인 컴퓨터에서 할 일"과 "관리자가 A에서 할 일"을 구분해 달라고 하세요. A의 관리자 권한이 필요한 작업은 직접 할 수 없습니다.
4. VS Code 접속 문제는 `View → Output → "Remote - SSH"` 로그가, SSH 접속 문제는 관리자의
   `Get-WinEvent -LogName "OpenSSH/Operational"` 로그가 가장 확실한 단서입니다.

---

## (부록) 관리자용 — 신규 팀원 추가 체크리스트

A의 관리자 PowerShell에서:

```powershell
# 1) 계정 생성 (비밀번호 변경 강제 금지 — SSH와 충돌)
net user 사번 "임시비밀번호" /add /logonpasswordchg:no

# 2) research 그룹 추가
net localgroup research 사번 /add

# 3) 휴직자는 비활성화해 두기
net user 사번 /active:no    # 복귀 시 /active:yes
```

- WMI 권한, 폴더 권한, 실행 정책은 `research` **그룹**에 걸려 있으므로 그룹 추가만 하면 자동 적용됩니다.
- 전원 키 인증 확인 후 `C:\ProgramData\ssh\sshd_config`에서 `PasswordAuthentication no` 설정 + `Restart-Service sshd` (신규 팀원 온보딩 시에는 임시로 다시 `yes`로 열었다가 잠그기).
