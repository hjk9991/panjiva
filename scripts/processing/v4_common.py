# -*- coding: utf-8 -*-
r"""
v4_common.py — v4 파이프라인 공용 유틸.

- discover_months : 원천 폴더(들)에서 실제 존재하는 월을 찾는다. 기간 하드코딩을 없애는 장치 —
                    "몇 년부터 몇 년까지" 라는 지식을 스크립트가 갖지 않고 디스크가 갖는다.
- to_quarters     : 월 목록 -> {분기: [월...]}
- write_manifest  : 빌드마다 입력·출력 파일의 행수/크기/시각/패키지 버전을 _manifest.json 에 기록.
                    "재현" 의 근거 — 나중에 결과가 다르면 무엇이 달랐는지 여기서 찾는다.
- verify_parquets : parquet 전체 디코딩 검증 (손상 감지).
"""

import json
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

TRADE_2024 = Path(r"C:\panjiva\data\staging\source\trade_2024")
TRADE_HIST = Path(r"C:\panjiva\data\staging\source\trade_hist")
CIQ_REF = Path(r"C:\panjiva\data\staging\source\ciq_ref")
OUT_FULL = Path(r"C:\panjiva\data\staging\v4_pairhs_full")
OUT_2024 = Path(r"C:\panjiva\data\staging\v4_pairhs_2024")

DEFAULT_TRADE_SRC = [TRADE_2024, TRADE_HIST]   # 앞 폴더 우선 (검증 완료본이 이긴다)


def discover_months(src_dirs=None, prefix="imp_ship"):
    """폴더들에서 {YYYYMM: 파일경로}. 같은 월이 여러 폴더에 있으면 앞 폴더가 이긴다."""
    src_dirs = src_dirs or DEFAULT_TRADE_SRC
    out = {}
    for d in src_dirs:
        for p in sorted(Path(d).glob(f"{prefix}_*.parquet")):
            ym = p.stem.split("_")[-1]
            if len(ym) == 6 and ym.isdigit():
                out.setdefault(ym, p)
    return dict(sorted(out.items()))


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
