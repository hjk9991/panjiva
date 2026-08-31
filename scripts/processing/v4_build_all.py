# -*- coding: utf-8 -*-
r"""
v4_build_all.py — v4 전체 파이프라인 단일 진입점. **재현 = 이 명령 한 줄.**

    python v4_build_all.py                 # 원천 검증 -> 무역 -> 검증 -> 재무 -> 결합검증
    python v4_build_all.py --from-step 3   # 3단계부터 (재무만 다시)
    python v4_build_all.py --skip-verify   # 원천 검증 생략 (이미 확인했을 때)

각 단계는 **별도 프로세스**로 실행한다 — 실패 시 그 자리에서 멈추고(exit≠0 전파),
단계가 끝날 때마다 메모리가 완전히 반환된다. 각 단계 스크립트가 스스로
_manifest.json 에 입력·출력을 기록한다.

전제: 원천 추출(src_trade_pull.py)은 별도로 완료되어 있어야 한다 — 1단계가 그걸 검증한다.
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent

STEPS = [
    ("1_verify_sources", [sys.executable, str(HERE / "v4_10_verify_sources.py")]),
    ("2_trade_build",    [sys.executable, str(HERE / "v4_trade_pair_hs_quarter.py")]),
    ("3_trade_checks",   [sys.executable, str(HERE / "v4_90_checks.py")]),
    ("4_fin_build",      [sys.executable, str(HERE / "v4_ciq_fin_build.py")]),
    ("5_join_test",      [sys.executable, str(HERE / "v4_95_join_test.py")]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-step", type=int, default=1, help="이 번호 단계부터 시작 (1~5)")
    ap.add_argument("--skip-verify", action="store_true")
    ap.add_argument("--low-priority", action="store_true",
                    help="공용 머신 배려 — 전 단계를 Below Normal 우선순위로")
    a = ap.parse_args()

    if a.low_priority:
        import psutil
        psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)   # 자식에게 상속됨

    for i, (name, cmd) in enumerate(STEPS, start=1):
        if i < a.from_step or (a.skip_verify and i == 1):
            print(f"=== [{i}/5] {name} — 건너뜀")
            continue
        t0 = datetime.now()
        print(f"\n=== [{i}/5] {name}  ({t0:%H:%M:%S}) " + "=" * 40, flush=True)
        r = subprocess.run(cmd)
        dt = (datetime.now() - t0).total_seconds()
        if r.returncode != 0:
            print(f"\nXX {name} 실패 (exit {r.returncode}, {dt:.0f}s) — 여기서 중단. "
                  f"고친 뒤 `--from-step {i}` 로 재개.")
            sys.exit(r.returncode)
        print(f"=== [{i}/5] {name} 완료 ({dt:.0f}s)")
    print("\n전체 파이프라인 완료.")


if __name__ == "__main__":
    main()
