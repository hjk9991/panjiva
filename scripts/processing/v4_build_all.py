# -*- coding: utf-8 -*-
r"""
v4_build_all.py — v4 전체 파이프라인 단일 진입점. **재현 = 이 명령 한 줄.**

    python v4_build_all.py                        # 원천검증 -> 무역 -> 검증 -> 재무 -> 결합검증 -> 프로브
    python v4_build_all.py --from-step 4          # 4단계(재무)부터
    python v4_build_all.py --skip-verify          # 원천 검증 생략 (이미 확인했을 때)
    python v4_build_all.py --out <dir> --src <dir> --trade-years 2024 2024 --fin-years 2022 2026
    python v4_build_all.py --with-guide           # 7단계 가이드 통계(전 기간 20~30분)까지
    python v4_build_all.py --pass 4="--years 2003 2027" --pass 3="--benchmark ''"   # 단계별 추가 인자
    python v4_build_all.py --dry-run              # 실행할 명령만 출력

단계 (번호 = --from-step 기준)
  1 v4_10_verify_sources.py     원천 디코딩 + Snowflake 건수 대사          (--skip-verify 로 생략)
  2 v4_trade_pair_hs_quarter.py 무역 팩트 빌드 (연도별 parquet + 버림 회계)
  3 v4_90_checks.py             무역 무결성 검증 (FAIL 시 중단)
  4 v4_ciq_fin_build.py         재무 long/wide — 연도 범위는 무역 파일명에서 유도(±2). 기존 산출 있으면
                                건너뜀 (--force-fin)
  5 v4_95_join_test.py          결합 검증
  6 v4_00_period_probe.py       연도별 사용가능성 프로브 md/csv (수 분 — 기본 포함, --skip-probe)
  7 v4_91_guide_stats.py        가이드 메모용 통계 json (원천 전수 — --with-guide 로만)

인자 통로: --src / --out 은 그것을 받는 모든 단계에, --trade-years 는 2단계, --fin-years·--force-fin 은
4단계, --benchmark 는 3단계에 전달된다. 그 밖의 단계별 인자는 --pass N="..." (N 은 번호 또는 이름).

각 단계는 **별도 프로세스**로 실행한다 — 실패 시 그 자리에서 멈추고(exit≠0 전파),
단계가 끝날 때마다 메모리가 완전히 반환된다. 각 단계 스크립트가 스스로 _manifest.json 에
입력·출력을 기록한다 (stage: verify_sources / trade_build / trade_checks / fin_build / join_test /
period_probe / guide_stats).

전제: 원천 추출(src_trade_pull.py)은 별도로 완료되어 있어야 한다 — 1단계가 그걸 검증한다.
"""

import argparse
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from v4_common import OUT_FULL

HERE = Path(__file__).parent


def split_args(s):
    """--pass 의 값 문자열 -> 토큰 리스트 (Windows 경로의 백슬래시를 살리려고 posix=False)."""
    toks = shlex.split(s, posix=False)
    return [t[1:-1] if len(t) >= 2 and t[0] == t[-1] and t[0] in "\"'" else t for t in toks]


def build_steps(a):
    """(번호, 이름, 스크립트, 인자, 실행여부) 목록."""
    out = ["--out", a.out]
    src = (["--src", *a.src]) if a.src else []
    steps = [
        (1, "verify_sources", "v4_10_verify_sources.py", out + src, not a.skip_verify),
        (2, "trade_build", "v4_trade_pair_hs_quarter.py",
         out + src + (["--years", *map(str, a.trade_years)] if a.trade_years else []), True),
        (3, "trade_checks", "v4_90_checks.py",
         out + src + (["--benchmark", a.benchmark] if a.benchmark is not None else []), True),
        (4, "fin_build", "v4_ciq_fin_build.py",
         ["--trade-dir", a.out, "--out", a.out]
         + (["--years", *map(str, a.fin_years)] if a.fin_years else [])
         + (["--force"] if a.force_fin else []), True),
        (5, "join_test", "v4_95_join_test.py", ["--dir", a.out], True),
        (6, "period_probe", "v4_00_period_probe.py", ["--in", a.out, "--out", a.out], not a.skip_probe),
        (7, "guide_stats", "v4_91_guide_stats.py",
         out + src + ["--trade-dir", a.out, "--fin-dir", a.out], a.with_guide),
    ]
    extra = {}
    for item in a.pass_ or []:
        if "=" not in item:
            raise SystemExit(f"--pass 형식: N=\"인자들\" (받은 값: {item!r})")
        k, v = item.split("=", 1)
        extra.setdefault(k.strip().lower(), []).extend(split_args(v))
    res = []
    for n, name, script, args, enabled in steps:
        args = args + extra.get(str(n), []) + extra.get(name, [])
        res.append((n, name, [sys.executable, str(HERE / script)] + args, enabled))
    return res


def main():
    ap = argparse.ArgumentParser(description="v4 파이프라인 단일 진입점 (단계·인자 통로는 아래 설명)",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__)
    ap.add_argument("--from-step", type=int, default=1, help="이 번호 단계부터 시작 (1~7)")
    ap.add_argument("--skip-verify", action="store_true", help="1단계(원천 검증) 생략")
    ap.add_argument("--skip-probe", action="store_true", help="6단계(프로브) 생략")
    ap.add_argument("--with-guide", action="store_true", help="7단계(가이드 통계, 원천 전수) 포함")
    ap.add_argument("--src", nargs="*", default=None, help="무역 원천 폴더(들) -> 1·2·3·7단계")
    ap.add_argument("--out", default=str(OUT_FULL), help="산출 폴더 (무역·재무·검증 md 전부)")
    ap.add_argument("--trade-years", nargs=2, type=int, default=None, help="2단계: 이 범위 연도만 빌드")
    ap.add_argument("--fin-years", nargs=2, type=int, default=None,
                    help="4단계: 재무 cal_year 범위 (기본 = 무역 연도 ±2 유도)")
    ap.add_argument("--force-fin", action="store_true", help="4단계: 기존 재무 산출이 있어도 다시")
    ap.add_argument("--benchmark", default=None, help="3단계: 2024 동결본 경로 ('' 이면 11번 생략)")
    ap.add_argument("--pass", dest="pass_", action="append", metavar='N="args"',
                    help='단계별 추가 인자 (예: --pass 4="--years 2003 2027"). 여러 번 가능')
    ap.add_argument("--dry-run", action="store_true", help="실행하지 않고 명령만 출력")
    ap.add_argument("--low-priority", action="store_true",
                    help="공용 머신 배려 — 전 단계를 Below Normal 우선순위로")
    a = ap.parse_args()

    if a.low_priority and not a.dry_run:
        import psutil
        psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)   # 자식에게 상속됨

    steps = build_steps(a)
    N = len(steps)
    for i, name, cmd, enabled in steps:
        if i < a.from_step or not enabled:
            print(f"=== [{i}/{N}] {name} — 건너뜀")
            continue
        if a.dry_run:
            print(f"=== [{i}/{N}] {name}: " + subprocess.list2cmdline(cmd))
            continue
        t0 = datetime.now()
        print(f"\n=== [{i}/{N}] {name}  ({t0:%H:%M:%S}) " + "=" * 40, flush=True)
        print("    " + subprocess.list2cmdline(cmd[1:]), flush=True)
        r = subprocess.run(cmd)
        dt = (datetime.now() - t0).total_seconds()
        if r.returncode != 0:
            print(f"\nXX {name} 실패 (exit {r.returncode}, {dt:.0f}s) — 여기서 중단. "
                  f"고친 뒤 `--from-step {i}` 로 재개.")
            sys.exit(r.returncode)
        print(f"=== [{i}/{N}] {name} 완료 ({dt:.0f}s)")
    print("\n전체 파이프라인 완료." if not a.dry_run else "\n(dry-run) 실행 안 함.")


if __name__ == "__main__":
    main()
