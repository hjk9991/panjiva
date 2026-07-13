"""
pull_panjiva.py — minimal skeleton for pulling Panjiva US-import shipments
from S&P Global's Snowflake warehouse and saving them as Parquet.

USAGE (VS Code terminal on server A, with the team venv active):

    # 1. one-time: put your credentials in %USERPROFILE%\.snowflake.env
    #    (see docs/snowflake-data-workflow.md section 2 — never commit this file)

    # 2. smoke test (pulls 1000 rows of Korea-origin imports)
    python scripts\extraction\pull_panjiva.py --smoke

    # 3. real pull
    python scripts\extraction\pull_panjiva.py --year-start 2020 --year-end 2024 `
        --shp-country "South Korea" --hs-prefix 85 `
        --out C:\panjiva\data\staging\korea_electronics_2020_2024.parquet
"""

import argparse
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# 1. Connection settings
# ---------------------------------------------------------------------------
# Credentials come from the per-user env file (home dir), so nothing personal
# lands in git history. A local ./.env can override for one-off experiments.
load_dotenv(Path.home() / ".snowflake.env")
load_dotenv(override=True)

CONN_KW = dict(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "vlc67107.us-east-1"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XF_READER_KoreaDevelopment_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "MI_XPRESSCLOUD"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "XPRESSFEED"),
)


# ---------------------------------------------------------------------------
# 2. The query
# ---------------------------------------------------------------------------
# {placeholders} are filled in by Python at runtime. We keep it parameterized
# by country, year range, and HS-code prefix so the same script can pull
# different slices without editing SQL.
QUERY_TEMPLATE = """
SELECT
    imp.panjivaRecordId,
    imp.arrivalDate,
    imp.conPanjivaId,                 -- consignee (US importer) Panjiva id
    imp.conName,                      -- consignee raw name
    imp.shpPanjivaId,                 -- shipper (foreign exporter) Panjiva id
    imp.shpName,                      -- shipper raw name
    imp.shpCountry,
    imp.portOfLading,
    imp.portOfUnlading,
    imp.weightKg,
    imp.valueOfGoodsUSD,
    imp.volumeTEU,
    hs.hsCode,
    ccr_con.companyId AS consignee_companyId,
    ccr_shp.companyId AS shipper_companyId
FROM panjivaUSImport            imp
LEFT JOIN panjivaUSImpHSCode    hs      ON hs.panjivaRecordId   = imp.panjivaRecordId
LEFT JOIN panjivaCompanyCrossRef ccr_con ON ccr_con.identifierValue = imp.conPanjivaId
LEFT JOIN panjivaCompanyCrossRef ccr_shp ON ccr_shp.identifierValue = imp.shpPanjivaId
WHERE imp.arrivalDate BETWEEN '{date_start}' AND '{date_end}'
  AND imp.shpCountry  = '{shp_country}'
  AND hs.hsCode LIKE '{hs_prefix}%'
{limit_clause}
"""


# ---------------------------------------------------------------------------
# 3. The puller
# ---------------------------------------------------------------------------
def run_query(sql: str) -> pd.DataFrame:
    """Open a connection, run sql, return rows as a pandas DataFrame, close."""
    with snowflake.connector.connect(**CONN_KW) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def pull(
    year_start: int,
    year_end: int,
    shp_country: str,
    hs_prefix: str,
    out_path: Path,
    smoke: bool = False,
) -> None:
    sql = QUERY_TEMPLATE.format(
        date_start=f"{year_start}-01-01",
        date_end=f"{year_end}-12-31",
        shp_country=shp_country,
        hs_prefix=hs_prefix,
        limit_clause="LIMIT 1000" if smoke else "",
    )
    print("[1/3] sending query to Snowflake...")
    df = run_query(sql)
    print(f"[2/3] received {len(df):,} rows, {df.memory_usage(deep=True).sum()/1e6:.1f} MB")

    print(f"[3/3] writing -> {out_path}")
    df.to_parquet(out_path, index=False)
    print("done.")


# ---------------------------------------------------------------------------
# 4. CLI
# ---------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year-start",  type=int, default=2023)
    p.add_argument("--year-end",    type=int, default=2023)
    p.add_argument("--shp-country", default="South Korea")
    p.add_argument("--hs-prefix",   default="85",
                   help="2- to 6-digit HS prefix; SQL LIKE match. 85 = electrical machinery")
    p.add_argument("--out",         default="panjiva_pull.parquet")
    p.add_argument("--smoke", action="store_true",
                   help="cap rows at 1000 for a connectivity test")
    args = p.parse_args()

    pull(
        year_start=args.year_start,
        year_end=args.year_end,
        shp_country=args.shp_country,
        hs_prefix=args.hs_prefix,
        out_path=Path(args.out),
        smoke=args.smoke,
    )


if __name__ == "__main__":
    main()
