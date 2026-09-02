# -*- coding: utf-8 -*-
r"""
wf_v3_stata.py — v3 패널을 Stata `.dta` 로 내보낸다 (명세 §8.3 "Stata 핸드오프")

산출 (v3 폴더):
    panel_pair_month.dta · dim_relationship.dta
    panel_firm_quarter.dta · panel_firm_origin_hs.dta
    panel_firm_export_quarter.dta · panel_firm_origin_quarter.dta
    99_stata_varnames.csv     패널 ↔ 짧은 변수명 ↔ 원래 컬럼명 ↔ 계정 한글명 대조표
    99_stata_handoff.md       변환 결과 — `--only` 로 일부만 변환해도 **나머지 패널 행은
                              parquet 메타·기존 .dta·대조표로 채워 문서를 통째로 덮지 않는다**

parquet 은 Stata 가 기본으로 못 읽는다. 같은 내용을 `.dta` 로 한 벌 더 둔다 —
**parquet 이 정본이고 `.dta` 는 사본**이다.

## Stata 제약과 대응

  S-1 변수명 32자   Stata 변수명은 최대 32자다. 재무 계정 이름이 그보다 길다
                    (`fin_a_ebt_excl_unusual_items_4_usd` = 34자).
                    → **`{블록}i{data_item_id}[_usd]`** 로 바꾼다. `data_item_id` 가
                      계정당 유일하므로 **이름 충돌이 구조적으로 불가능**하다.
                      원래 이름은 **변수 레이블**로 넣어 `describe` 에서 다 보인다.
  S-2 변수 개수     `panel_firm_quarter` 의 열 수는 parquet 스키마에서 **실측**한다.
                    2,048 을 넘으면 Stata/IC 로는 못 읽는다 → SE·MP 필요 경고를 낸다.
                    (김영수 연구원 확인: SE 또는 MP 사용)
  S-3 문자열 결측   Stata 에는 문자열 결측이 없어 **`""` 가 된다.**
                    HS 코드·국가명은 정상값이 `""` 일 수 없으므로 뜻이 헷갈리지 않지만,
                    `misstable` 로는 안 잡히니 아래 §대조표와 리포트에 명시한다.
  S-4 정수→실수     nullable 정수(`Int64`)는 Stata 에서 실수형이 된다. **값과 결측은
                    그대로 보존**된다(왕복 검증 완료). Stata 쪽 정상 동작이다.
  S-5 포맷 버전     118 (Stata 14 이상, UTF-8 변수명·레이블 지원).
  S-6 대형 패널     `to_stata` 는 전 행·전 열을 **한 덩어리 레코드 배열**로 만들어
                    (`panel_firm_quarter` 2024 실측 12.1GiB) 순간 메모리가 데이터의
                    3배쯤 필요하다 → 예상 크기가 `--max-shot-gb`(기본 4GiB)를 넘으면
                    행을 청크로 나눠 `.dta` 조각을 쓰고 **Stata 배치(`append`)로 잇는다.**
                    값·타입 보존은 단발 변환과 동일하다(청크 dtype 은 parquet 스키마에서
                    오므로 균일하고, 문자열 폭은 append 가 최대 폭으로 승격 = 단발과 같음).
                    변환 후 행·열 수를 Stata 로그로 검증한다.
"""

import argparse
import gc
import math
import re
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wf_v3_d8d9 import detect_join  # noqa: E402

V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
FIN = Path(r"C:\panjiva\data\staging\source\ciq_fin")

PANELS = ["panel_pair_month", "dim_relationship",
          "panel_firm_quarter", "panel_firm_origin_hs",
          "panel_firm_export_quarter", "panel_firm_origin_quarter"]
VARNAMES = "99_stata_varnames.csv"
STATA_IC_MAX_VARS = 2048
STATA_EXE = r"C:\Program Files\StataNow19\StataMP-64.exe"
BLOCKS = ("fin_a_", "fin_q_", "up_a_", "up_q_",
          "shp_a_", "shp_q_", "shp_up_a_", "shp_up_q_",
          "con_a_", "con_q_", "con_up_a_", "con_up_q_")
# `{블록}{계정이름}_{id}` 또는 `{블록}{계정이름}_{id}_usd`
ACCT = re.compile(r"^(" + "|".join(BLOCKS) + r")(.+)_(\d+)(_usd)?$")


def short_names(cols, cat: pd.DataFrame) -> tuple:
    """긴 재무 계정 이름을 `{블록}i{id}[_usd]` 로 줄이고, 레이블 사전을 만든다."""
    ko = dict(zip(cat.data_item_id, cat.item_name_ko.fillna("")))
    en = dict(zip(cat.data_item_id, cat.item_name))
    ren, lab, rows = {}, {}, []
    for c in cols:
        m = ACCT.match(c)
        if not m:
            if len(c) > 32:
                raise SystemExit(f"32자를 넘는데 계정이 아닌 컬럼: {c}")
            continue
        blk, _, iid, usd = m.group(1), m.group(2), int(m.group(3)), m.group(4) or ""
        new = f"{blk}i{iid}{usd}"
        if len(new) > 32:
            raise SystemExit(f"줄인 이름도 32자 초과: {new}")
        ren[c] = new
        nm = en.get(iid, "?")
        unit = "USD 환산" if usd else "원표시통화"
        lab[new] = f"{blk.rstrip('_')} {nm} ({unit})"[:80]
        rows.append({"stata_name": new, "parquet_name": c, "block": blk.rstrip("_"),
                     "data_item_id": iid, "item_name": nm,
                     "item_name_ko": ko.get(iid, ""), "unit": unit})
    return ren, lab, pd.DataFrame(rows)


def stata_append(tmp: Path, out: Path, exe: Path, k: int, n: int, nvar: int) -> None:
    """청크 `.dta` k개를 Stata 배치 `append` 로 이어 `out` 으로 저장하고 행·열을 검증한다."""
    do = ["clear all", "set more off", 'use "chunk_0000.dta", clear']
    do += [f'append using "chunk_{i:04d}.dta"' for i in range(1, k)]
    do += [f'save "{out}", replace', "quietly count",
           'display "NOBS=" r(N)', 'display "NVARS=" c(k)']
    (tmp / "append.do").write_text("\n".join(do) + "\n", encoding="utf-8")
    subprocess.run([str(exe), "/e", "do", "append.do"], cwd=tmp, check=False)
    logf = tmp / "append.log"
    log = logf.read_text(encoding="utf-8", errors="replace") if logf.exists() else ""
    mo = re.search(r"NOBS=(\d+)", log)
    mv = re.search(r"NVARS=(\d+)", log)
    got = (int(mo.group(1)) if mo else -1, int(mv.group(1)) if mv else -1)
    if got != (n, nvar):
        raise SystemExit(f"Stata append 검증 실패 — 기대 {n:,}×{nvar:,}, "
                         f"결과 {got[0]:,}×{got[1]:,}. 로그: {logf}")
    shutil.rmtree(tmp)


def export(path: Path, out: Path, cat: pd.DataFrame,
           stata_exe: str = STATA_EXE, max_shot_gb: float = 4.0,
           chunk_gb: float = 2.0) -> dict:
    d = pd.read_parquet(path)
    n0, c0 = len(d), d.shape[1]
    ren, lab, table = short_names(d.columns, cat)
    if ren:
        d = d.rename(columns=ren)
    table.insert(0, "panel", path.stem)

    # S-3 — 문자열 결측은 Stata 에서 `""` 가 된다. 어느 컬럼이 몇 건인지 남긴다.
    strcols = [c for c in d.columns if d[c].dtype == "object"
               or str(d[c].dtype) in ("string", "str")]
    smiss = {c: int(d[c].isna().sum()) for c in strcols if d[c].isna().any()}
    for c in strcols:
        d[c] = d[c].astype("object").where(d[c].notna(), "")

    kw = dict(write_index=False, version=118, variable_labels=lab or None,
              data_label=f"v3 {path.stem} ({date.today()})")
    est = n0 * d.shape[1] * 8  # 레코드 배열 크기 근사(f8 기준; S-6)
    if est <= max_shot_gb * 2**30:
        d.to_stata(out, **kw)
    else:
        exe = Path(stata_exe)
        if not exe.exists():
            raise SystemExit(f"S-6 청크 변환에 Stata 가 필요한데 없음: {exe} (--stata-exe)")
        rows = max(1, int(n0 * (chunk_gb * 2**30) / est))
        k = math.ceil(n0 / rows)
        print(f"  단발 변환엔 레코드 배열 약 {est / 2**30:.1f}GiB 필요 — "
              f"{k}개 청크({rows:,}행씩)로 나눠 Stata append (S-6)")
        tmp = out.parent / f"_dta_chunks_{out.stem}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir()
        for i, a0 in enumerate(range(0, n0, rows)):
            d.iloc[a0:a0 + rows].to_stata(tmp / f"chunk_{i:04d}.dta", **kw)
        nvar = d.shape[1]
        del d
        gc.collect()  # Stata 가 최종본(≈est)을 올릴 자리를 먼저 비운다
        stata_append(tmp, out, exe, k, n0, nvar)
    return {"table": table, "str_missing": smiss, "n": n0, "ncol": c0,
            "n_ren": len(ren), "size_mb": out.stat().st_size / 1e6}


def load_prev_varnames(v3: Path, schemas: dict) -> pd.DataFrame:
    """기존 `99_stata_varnames.csv`. `panel` 열이 없는 구판이면 열을 만들고, 각 행의
    `parquet_name` 이 어느 패널 스키마에 있는지로 귀속시킨다(못 찾으면 빈 문자열)."""
    p = v3 / VARNAMES
    if not p.exists():
        return pd.DataFrame()
    prev = pd.read_csv(p, encoding="utf-8-sig")
    if "panel" not in prev.columns:
        prev.insert(0, "panel", "")
    prev["panel"] = prev.panel.fillna("").astype(str)
    blank = prev.panel == ""
    for name, cols in schemas.items():
        hit = blank & prev.parquet_name.isin(cols)
        prev.loc[hit, "panel"] = name
        blank = prev.panel == ""
    return prev


def main() -> None:
    global V3, FIN
    ap = argparse.ArgumentParser()
    ap.add_argument("--v3-dir", default=str(V3))
    ap.add_argument("--fin-dir", default=str(FIN))
    ap.add_argument("--only", nargs="*", choices=PANELS,
                    help="일부만 변환. 나머지 패널 행은 parquet 메타·기존 .dta·대조표로 채운다")
    ap.add_argument("--stata-exe", default=STATA_EXE,
                    help="S-6 청크 변환에 쓸 Stata 실행파일")
    ap.add_argument("--max-shot-gb", type=float, default=4.0,
                    help="레코드 배열 예상 크기(GiB)가 이보다 크면 청크 변환 (S-6)")
    ap.add_argument("--chunk-gb", type=float, default=2.0, help="청크 하나의 목표 크기(GiB)")
    a = ap.parse_args()
    V3, FIN = Path(a.v3_dir), Path(a.fin_dir)
    t0 = datetime.now()
    cat = pd.read_csv(FIN / "ciq_dataitem_catalog.csv", encoding="utf-8-sig")
    todo = list(a.only or PANELS)
    join = detect_join(V3 / "panel_firm_quarter.parquet",
                       asof_col="fin_a_age_days", equi_col="fin_a_days_after_close")
    # 존재하는 패널의 스키마 (행·열 메타는 parquet 푸터만 읽는다)
    meta = {}
    for name in PANELS:
        p = V3 / f"{name}.parquet"
        if p.exists():
            pf = pq.ParquetFile(p)
            meta[name] = (pf.metadata.num_rows, list(pf.schema_arrow.names))
    prev = load_prev_varnames(V3, {n: set(m[1]) for n, m in meta.items()})

    L = []
    L.append("# v3 Stata 핸드오프\n")
    L.append(f"**생성일** {date.today()} · **대상** `{V3}` · **결합** {join} · "
             f"**스크립트** `wf_v3_stata.py` · **포맷** Stata 118 (v14 이상)\n")
    L.append("> **parquet 이 정본이고 `.dta` 는 사본이다.** 값이 다르면 parquet 을 믿는다.\n")
    L.append("\n## 변환 결과\n")
    if a.only:
        L.append(f"이번 실행은 `--only {' '.join(todo)}` — 나머지 패널 행은 parquet 메타, "
                 "기존 `.dta` 크기, 대조표의 행 수로 채웠다(마지막 열 참조).\n")
    L.append("| 패널 | 행 | 열 | 이름 줄인 변수 | .dta 크기(MB) | .dta 상태 |")
    L.append("|---|---|---|---|---|---|")

    tables, smiss_all = [], {}
    for name in PANELS:
        if name not in meta:
            print(f"  {name}.parquet 없음 — 건너뜀")
            L.append(f"| `{name}` | | | | | parquet 없음 |")
            continue
        n_rows, cols = meta[name]
        dta = V3 / f"{name}.dta"
        if name in todo:
            print(f"[{name}] 변환 중...")
            r = export(V3 / f"{name}.parquet", dta, cat,
                       stata_exe=a.stata_exe, max_shot_gb=a.max_shot_gb,
                       chunk_gb=a.chunk_gb)
            if len(r["table"]):
                tables.append(r["table"])
            if r["str_missing"]:
                smiss_all[name] = r["str_missing"]
            print(f"  → {name}.dta")
            L.append(f"| `{name}` | {r['n']:,} | {r['ncol']:,} | {r['n_ren']:,} | "
                     f"{r['size_mb']:,.0f} | 이번 실행에서 변환 |")
        else:
            n_ren = int((prev.panel == name).sum()) if len(prev) else 0
            if dta.exists():
                sz, st = f"{dta.stat().st_size / 1e6:,.0f}", \
                    f"기존 유지 ({datetime.fromtimestamp(dta.stat().st_mtime).date()})"
            else:
                sz, st = "", ".dta 없음 — 미변환"
            L.append(f"| `{name}` | {n_rows:,} | {len(cols):,} | {n_ren:,} | {sz} | {st} |")

    # 대조표 — 변환한 패널의 행은 새로 쓰고, 나머지 패널의 행은 기존 csv 에서 유지한다
    keep = prev[~prev.panel.isin(todo)] if len(prev) else pd.DataFrame()
    new = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
    t = pd.concat([x for x in (keep, new) if len(x)], ignore_index=True) \
        if (len(keep) or len(new)) else pd.DataFrame()
    if len(t):
        t = (t.drop_duplicates(["panel", "stata_name"])
              .sort_values(["panel", "stata_name"], kind="mergesort"))
        t.to_csv(V3 / VARNAMES, index=False, encoding="utf-8-sig")
        L.append("\n## 변수명 대조표\n")
        L.append(f"`{VARNAMES}` — **{t.stata_name.nunique():,}개** 계정의 "
                 "`panel` ↔ `stata_name` ↔ `parquet_name` ↔ 계정 한글명"
                 + (" (이번 실행에서 변환하지 않은 패널의 행은 기존 것을 유지)" if a.only else "")
                 + ".\n")
        L.append("Stata 변수명이 32자를 넘을 수 없어 재무 계정을 "
                 "**`{블록}i{data_item_id}[_usd]`** 로 줄였다.\n")
        L.append("```")
        for r in t.head(4).itertuples(index=False):
            L.append(f"{r.stata_name:<20} <- {r.parquet_name}")
        L.append("```\n")
        L.append("**원래 이름은 변수 레이블에 있다** — Stata 에서 바로 확인된다:\n")
        L.append("```stata\nuse panel_firm_quarter.dta, clear\n"
                 "describe fin_a_i28*\nlookfor revenue\n```\n")
    else:
        L.append("\n## 변수명 대조표\n")
        L.append("이름을 줄인 변수가 없다(재무 계정 열이 있는 패널을 변환하지 않았고 기존 "
                 f"`{VARNAMES}` 도 없음).\n")

    if smiss_all:
        L.append("\n## ⚠️ 문자열 결측은 `\"\"` 가 된다\n")
        L.append("Stata 에는 문자열 결측이 없다. parquet 의 결측이 빈 문자열로 바뀌므로 "
                 "`misstable` 로는 잡히지 않는다. **`== \"\"` 로 세야 한다.**\n")
        L.append("| 패널 | 컬럼 | 결측 건수 |")
        L.append("|---|---|---|")
        for k, v in smiss_all.items():
            for c, n in sorted(v.items(), key=lambda x: -x[1]):
                L.append(f"| `{k}` | `{c}` | {n:,} |")
        L.append("\n숫자 결측(`.`)은 정상 보존된다. nullable 정수는 실수형이 되지만 "
                 "**값과 결측 위치는 그대로**다(왕복 검증 완료).\n")
        if a.only:
            L.append("(이번 실행에서 변환한 패널만 집계 — 나머지는 이전 실행의 기록을 볼 것)\n")

    L.append("\n## Stata 에디션\n")
    if "panel_firm_quarter" in meta:
        nvar = len(meta["panel_firm_quarter"][1])
        if nvar > STATA_IC_MAX_VARS:
            L.append(f"`panel_firm_quarter` 는 **{nvar:,}개 변수**다(parquet 스키마 실측). "
                     f"**Stata/IC({STATA_IC_MAX_VARS:,}개 한계)로는 열리지 않는다** — "
                     "SE 또는 MP 가 필요하다.\n")
        else:
            L.append(f"`panel_firm_quarter` 는 {nvar:,}개 변수다(parquet 스키마 실측) — "
                     f"Stata/IC({STATA_IC_MAX_VARS:,}개 한계)로도 열린다.\n")
    else:
        L.append("`panel_firm_quarter.parquet` 이 없어 변수 수를 확인하지 못했다.\n")

    (V3 / "99_stata_handoff.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {V3}")


if __name__ == "__main__":
    main()
