# -*- coding: utf-8 -*-
r"""
ex_20260825_ref_dataitem_pull.py — CIQ 계정 사전 `ciqDataItem` 전체를 참조표로 받는다.

배경: v4 재무 카탈로그가 `shared memory\ciq_dataitems.md`(사람이 쓴 마크다운)를 정규식으로
파싱해 계정 **이름**을 얻고 있었다. 문서 서식이 바뀌면 조용히 깨진다. 이름의 정본은
`ciqDataItem` 테이블이므로 그걸 받아 둔다 (재무제표 분류(statement/section)는 여전히
md 카탈로그에만 있으므로 md 파싱은 분류용으로 유지).

산출: source\ciq_ref\ref_dataitem.parquet
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ex_20260824_import_coverage_by_year import connect_kwargs   # noqa: E402

import snowflake.connector                                       # noqa: E402

OUT = Path(r"C:\panjiva\data\staging\source\ciq_ref\ref_dataitem.parquet")

SQL = "select dataItemId, dataItemName from ciqDataItem order by dataItemId"


def main():
    with snowflake.connector.connect(**connect_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            d = cur.fetch_pandas_all()
    d.columns = [c.lower() for c in d.columns]
    tmp = OUT.parent / (OUT.name + ".tmp")
    d.to_parquet(tmp, index=False)
    import os
    os.replace(tmp, OUT)
    print(f"{len(d):,}행 -> {OUT}")


if __name__ == "__main__":
    main()
