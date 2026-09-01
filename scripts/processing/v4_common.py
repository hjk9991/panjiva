# -*- coding: utf-8 -*-
r"""
v4_common.py — v4 파이프라인 공용 유틸.

- discover_months : 원천 폴더(들)에서 실제 존재하는 월을 찾는다. 기간 하드코딩을 없애는 장치 —
                    "몇 년부터 몇 년까지" 라는 지식을 스크립트가 갖지 않고 디스크가 갖는다.
- to_quarters     : 월 목록 -> {분기: [월...]}
- file_years      : 연도 파일 경로들 -> 연도 리스트 (재무 연도 범위를 무역 파일명에서 유도할 때)
- write_manifest  : 빌드마다 입력·출력 파일의 행수/크기/시각/패키지 버전을 _manifest.json 에 기록.
                    "재현" 의 근거 — 나중에 결과가 다르면 무엇이 달랐는지 여기서 찾는다.
                    무역 빌드뿐 아니라 검증(10/90/95)·프로브(00)·가이드통계(91)도 stage 로 기록한다.
- verify_parquets : parquet 전체 디코딩 검증 (손상 감지).
- free_ram_gb     : 가용 RAM (공용 머신 — 큰 실행 전 확인용)
"""

import json
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

# 무역 원천은 폴더 하나다 (2026-09-01 통합: 구 trade_2024 + trade_hist -> trade).
#   imp_ship·imp_hs : 2007-07 ~ (있는 만큼)   exp_ship·exp_hs : 2024 만
#   새 연도 원천은 이 폴더에 imp_ship_YYYYMM.parquet + imp_hs_YYYYMM.parquet 짝으로 넣기만 하면
#   discover_months 가 찾는다 — 스크립트 수정 불필요.
TRADE = Path(r"C:\panjiva\data\staging\source\trade")
CIQ_REF = Path(r"C:\panjiva\data\staging\source\ciq_ref")
OUT_FULL = Path(r"C:\panjiva\data\staging\v4_pairhs_full")
OUT_2024 = Path(r"C:\panjiva\data\staging\v4_pairhs_2024")

DEFAULT_TRADE_SRC = [TRADE]   # 여러 폴더를 주면 앞 폴더가 우선 (discover_months)


def discover_months(src_dirs=None, prefix="imp_ship", years=None):
    """폴더들에서 {YYYYMM: 파일경로}. 같은 월이 여러 폴더에 있으면 앞 폴더가 이긴다.

    years : (y0, y1) 또는 연도 iterable — 그 연도 월만 (부분 실행용).
    """
    src_dirs = src_dirs or DEFAULT_TRADE_SRC
    out = {}
    for d in src_dirs:
        for p in sorted(Path(d).glob(f"{prefix}_*.parquet")):
            ym = p.stem.split("_")[-1]
            if len(ym) == 6 and ym.isdigit():
                out.setdefault(ym, p)
    if years is not None:
        ys = (set(range(int(years[0]), int(years[1]) + 1)) if isinstance(years, tuple)
              else {int(y) for y in years})
        out = {ym: p for ym, p in out.items() if int(ym[:4]) in ys}
    return dict(sorted(out.items()))


def file_years(paths):
    """trade_pair_hs_quarter_YYYY.parquet 류 경로들에서 연도 리스트 (파일명 끝 4자리)."""
    ys = set()
    for p in paths:
        tail = Path(p).stem.split("_")[-1]
        if len(tail) == 4 and tail.isdigit():
            ys.add(int(tail))
    return sorted(ys)


def free_ram_gb():
    """가용 물리 메모리(GB). psutil 이 없으면 None — 실행 전 공용 머신 배려용."""
    try:
        import psutil
        return psutil.virtual_memory().available / 1e9
    except Exception:
        return None


def to_quarters(months):
    """['200707', ...] -> {'2007Q3': ['200707','200708','200709'], ...} (있는 월만)."""
    q = {}
    for ym in sorted(months):
        label = f"{ym[:4]}Q{(int(ym[4:6]) - 1) // 3 + 1}"
        q.setdefault(label, []).append(ym)
    return q


def file_info(path):
    p = Path(path)
    info = {"file": str(p), "size_bytes": p.stat().st_size}
    if p.suffix == ".parquet":
        info["rows"] = pq.ParquetFile(p).metadata.num_rows
    return info


def write_manifest(out_dir, stage, inputs, outputs, extra=None):
    """out_dir/_manifest.json 에 stage 기록을 추가한다 (리스트 append)."""
    import pandas, pyarrow, sys
    rec = {
        "stage": stage,
        "when": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "pandas": pandas.__version__,
        "pyarrow": pyarrow.__version__,
        "inputs": [file_info(p) for p in inputs],
        "outputs": [file_info(p) for p in outputs],
    }
    if extra:
        rec["extra"] = extra
    mf = Path(out_dir) / "_manifest.json"
    hist = json.loads(mf.read_text(encoding="utf-8")) if mf.exists() else []
    hist = [h for h in hist if h.get("stage") != stage] + [rec]   # 같은 stage 는 최신으로 교체
    mf.write_text(json.dumps(hist, indent=2, ensure_ascii=False), encoding="utf-8")
    return mf


def verify_parquets(paths, verbose=True):
    """전체 디코딩 검증. 손상 파일 목록을 돌려준다 (빈 리스트 = 전부 정상)."""
    bad = []
    for p in paths:
        try:
            pf = pq.ParquetFile(p)
            for i in range(pf.metadata.num_row_groups):
                pf.read_row_group(i)
        except Exception as e:
            bad.append((str(p), repr(e)))
            if verbose:
                print(f"  [손상] {p}: {e}")
    return bad
