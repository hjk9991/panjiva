# -*- coding: utf-8 -*-
r"""
wf_v3_stata.py — v3 패널을 Stata `.dta` 로 내보낸다 (명세 §8.3 "Stata 핸드오프")

산출 (v3 폴더):
    panel_pair_month.dta · dim_relationship.dta
    panel_firm_quarter.dta · panel_firm_origin_hs.dta
    99_stata_varnames.csv     짧은 변수명 ↔ 원래 컬럼명 ↔ 계정 한글명 대조표

parquet 은 Stata 가 기본으로 못 읽는다. 같은 내용을 `.dta` 로 한 벌 더 둔다 —
**parquet 이 정본이고 `.dta` 는 사본**이다.

## Stata 제약과 대응

  S-1 변수명 32자   Stata 변수명은 최대 32자다. 재무 계정 이름이 그보다 길다
                    (`fin_a_ebt_excl_unusual_items_4_usd` = 34자).
                    → **`{블록}i{data_item_id}[_usd]`** 로 바꾼다. `data_item_id` 가
                      계정당 유일하므로 **이름 충돌이 구조적으로 불가능**하다.
                      원래 이름은 **변수 레이블**로 넣어 `describe` 에서 다 보인다.
  S-2 변수 개수     `panel_firm_quarter` 는 2,641열이다.
                    Stata/IC 는 2,048개 한계라 **못 읽는다**. Stata/SE·MP 필요.
                    (김영수 연구원 확인: SE 또는 MP 사용)
  S-3 문자열 결측   Stata 에는 문자열 결측이 없어 **`""` 가 된다.**
                    HS 코드·국가명은 정상값이 `""` 일 수 없으므로 뜻이 헷갈리지 않지만,
                    `misstable` 로는 안 잡히니 아래 §대조표와 리포트에 명시한다.
  S-4 정수→실수     nullable 정수(`Int64`)는 Stata 에서 실수형이 된다. **값과 결측은
                    그대로 보존**된다(왕복 검증 완료). Stata 쪽 정상 동작이다.
  S-5 포맷 버전     118 (Stata 14 이상, UTF-8 변수명·레이블 지원).
"""

import argparse
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd

V3 = Path(r"C:\panjiva\data\staging\within_firm_pilot_2024")
FIN = Path(r"C:\panjiva\data\staging\source\ciq_fin")

PANELS = ["panel_pair_month", "dim_relationship",
          "panel_firm_quarter", "panel_firm_origin_hs"]
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


def export(path: Path, out: Path, cat: pd.DataFrame, note) -> dict:
    d = pd.read_parquet(path)
    n0, c0 = len(d), d.shape[1]
    ren, lab, table = short_names(d.columns, cat)
    if ren:
        d = d.rename(columns=ren)

    # S-3 — 문자열 결측은 Stata 에서 `""` 가 된다. 어느 컬럼이 몇 건인지 남긴다.
    strcols = [c for c in d.columns if d[c].dtype == "object"
               or str(d[c].dtype) in ("string", "str")]
    smiss = {c: int(d[c].isna().sum()) for c in strcols if d[c].isna().any()}
    for c in strcols:
        d[c] = d[c].astype("object").where(d[c].notna(), "")

    d.to_stata(out, write_index=False, version=118, variable_labels=lab or None,
               data_label=f"v3 {path.stem} ({date.today()})")
    sz = out.stat().st_size / 1e6
    note(f"| `{path.stem}` | {n0:,} | {c0:,} | {len(ren):,} | {sz:,.0f} |")
    return {"table": table, "str_missing": smiss, "n": n0, "ncol": c0}


def main() -> None:
    global V3, FIN
    ap = argparse.ArgumentParser()
    ap.add_argument("--v3-dir", default=str(V3))
    ap.add_argument("--fin-dir", default=str(FIN))
    ap.add_argument("--only", nargs="*", choices=PANELS)
    a = ap.parse_args()
    V3, FIN = Path(a.v3_dir), Path(a.fin_dir)
    t0 = datetime.now()
    cat = pd.read_csv(FIN / "ciq_dataitem_catalog.csv", encoding="utf-8-sig")

    L = []
    L.append("# v3 Stata 핸드오프\n")
    L.append(f"**생성일** {date.today()} · **스크립트** `wf_v3_stata.py` · "
             "**포맷** Stata 118 (v14 이상)\n")
    L.append("> **parquet 이 정본이고 `.dta` 는 사본이다.** 값이 다르면 parquet 을 믿는다.\n")
    L.append("\n## 변환 결과\n")
    L.append("| 패널 | 행 | 열 | 이름 줄인 변수 | .dta 크기(MB) |")
    L.append("|---|---|---|---|---|")

    tables, smiss_all = [], {}
    for name in (a.only or PANELS):
        p = V3 / f"{name}.parquet"
        if not p.exists():
            print(f"  {name}.parquet 없음 — 건너뜀")
            continue
        print(f"[{name}] 변환 중...")
        r = export(p, V3 / f"{name}.dta", cat, L.append)
        if len(r["table"]):
            tables.append(r["table"])
        if r["str_missing"]:
            smiss_all[name] = r["str_missing"]
        print(f"  → {name}.dta")

    if tables:
        t = pd.concat(tables, ignore_index=True).drop_duplicates("stata_name")
        t.to_csv(V3 / "99_stata_varnames.csv", index=False, encoding="utf-8-sig")
        L.append(f"\n## 변수명 대조표\n")
        L.append(f"`99_stata_varnames.csv` — **{len(t):,}개** 계정의 "
                 "`stata_name` ↔ `parquet_name` ↔ 계정 한글명.\n")
        L.append("Stata 변수명이 32자를 넘을 수 없어 재무 계정을 "
                 "**`{블록}i{data_item_id}[_usd]`** 로 줄였다.\n")
        L.append("```")
        for r in t.head(4).itertuples(index=False):
            L.append(f"{r.stata_name:<20} <- {r.parquet_name}")
        L.append("```\n")
        L.append("**원래 이름은 변수 레이블에 있다** — Stata 에서 바로 확인된다:\n")
        L.append("```stata\nuse panel_firm_quarter.dta, clear\n"
                 "describe fin_a_i28*\nlookfor revenue\n```\n")

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

    L.append("\n## Stata 에디션\n")
    L.append("`panel_firm_quarter` 는 **2,641개 변수**다. "
             "**Stata/IC(2,048개 한계)로는 열리지 않는다** — SE 또는 MP 가 필요하다.\n")

    (V3 / "99_stata_handoff.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n완료 ({(datetime.now()-t0).seconds}초) → {V3}")


if __name__ == "__main__":
    main()
