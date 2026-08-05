r"""
wf2q_10_build_dims.py — within-firm 파일럿(2024 H1) L1 개체해소 층

입력:  L0\imp_ship_*.parquet, exp_ship_*.parquet, company.parquet, pit_intervals.parquet
산출:  L1\dim_company.parquet    한 행 = Panjiva 기업 id 1개 (수입 양측 + 수출 shipper 전수)
       L1\dim_group_link.parquet 한 행 = 기업 × PIT 소유구조 구간
       L1\diag_dims.md           redaction 패턴 프로파일·포워더 플래그 규모·검증 결과

설계문서 §3.1(개체 해소)·§3.2(포워더/redaction)·§3.4(dim_group_link) 대응.
v1 규칙:
  * entity_id = CIQ companyId (매칭 성사 기업 한정; 미매칭은 panjiva id 그대로)
  * is_redacted = 이름 결측/공백 또는 알려진 은폐 패턴 (패턴별 빈도를 diag 에 기록)
  * is_forwarder = 이름 키워드 ∪ CIQ 운송·물류 산업 (v1 본 플래그)
  * is_forwarder_heur = 파트너수·HS2 다양성 동시 극단 (민감도용 별도 플래그,
    v1 본 플래그에 미포함 — 설계문서 §3.2 의 세 기준 중 (ii))

사용법:  python scripts\processing\wf2q_10_build_dims.py
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

STAGE = Path(r"C:\panjiva\data\staging\within_firm_pilot_2q")
L0, L1 = STAGE / "L0", STAGE / "L1"
MONTHS = ["202401", "202402", "202403", "202404", "202405", "202406"]

# ---------------------------------------------------------------------------
# 규칙 정의
# ---------------------------------------------------------------------------
# B/L 관행상 실당사자를 가리는 기재 (redaction/negotiable B/L)
REDACTION_PATTERNS = [
    r"^TO (THE )?ORDER( OF)?\b", r"^ORDER( OF)?$", r"^N/?A$", r"^NOT AVAILABLE",
    r"^UNKNOWN", r"^UNAVAILABLE", r"^CONFIDENTIAL", r"^WITHHELD",
    r"^SAME AS (CONSIGNEE|SHIPPER)", r"^-+$",
]

# 이름 키워드 (name_std 기준: 대문자·구두점 제거 후)
FORWARDER_KEYWORDS = [
    "LOGISTIC", "FORWARD", "FREIGHT", "SHIPPING", "TRANSPORT", "EXPRESS",
    "CARGO", "NVOCC", "CONSOLIDAT", "CUSTOMS BROKER", "SUPPLY CHAIN",
    "3PL", "LINER AGENC",
]
FORWARDER_NAMES = [
    "DHL", "KUEHNE", "NAGEL", "DSV", "EXPEDITORS", "C H ROBINSON", "CH ROBINSON",
    "SINOTRANS", "PANALPINA", "GEODIS", "SCHENKER", "NIPPON EXPRESS",
    "KERRY", "AGILITY", "HELLMANN", "BOLLORE", "CEVA", "YUSEN", "KINTETSU",
    "DACHSER", "MAINFREIGHT", "UPS", "FEDEX", "TNT", "MAERSK", "MSC", "CMA CGM",
    "HAPAG", "EVERGREEN LINE", "COSCO", "ONE OCEAN NETWORK", "OOCL", "ZIM ",
    "YANG MING", "HMM ", "WAN HAI",
]
# CIQ 산업(GICS simpleIndustry) 운송·물류 판정
FORWARDER_INDUSTRY_RE = r"Logistic|Marine|Freight|Transport"

LEGAL_SUFFIXES = [
    "CO LTD", "COLTD", "CO LIMITED", "COMPANY LIMITED", "LIMITED", "LTD",
    "INCORPORATED", "INC", "CORPORATION", "CORP", "LLC", "L L C", "PLC",
    "GMBH", "S A", "SA DE CV", "S A DE C V", "BV", "B V", "NV", "N V",
    "PTE", "PVT", "PT ", "JSC", "JOINT STOCK COMPANY", "CO",
]
_suffix_re = re.compile(
    r"\b(" + "|".join(re.escape(s.strip()) for s in
                      sorted(LEGAL_SUFFIXES, key=len, reverse=True)) + r")\b\.?$")


def name_std(s: pd.Series) -> pd.Series:
    """표준화 기업명 v1: 대문자 → 구두점 제거 → 공백 정규화 → 말미 법인 접미사 반복 제거."""
    out = (s.astype("string").str.upper()
             .str.replace(r"[^\w\s&]", " ", regex=True)
             .str.replace(r"\s+", " ", regex=True).str.strip())

    # 접미사는 겹쳐 붙는 경우가 있어("... CO LTD") 두 번 벗긴다
    for _ in range(2):
        out = out.str.replace(_suffix_re, "", regex=True).str.strip()
    return out.mask(out.eq(""), pd.NA)


def is_redacted(raw: pd.Series) -> pd.Series:
    up = raw.astype("string").str.upper().str.strip()
    flag = up.isna() | up.eq("")
    for pat in REDACTION_PATTERNS:
        flag |= up.str.contains(pat, regex=True, na=False)
    return flag.astype("int8")


def is_forwarder_name(std: pd.Series) -> pd.Series:
    flag = pd.Series(False, index=std.index)
    s = std.fillna("")
    for kw in FORWARDER_KEYWORDS + FORWARDER_NAMES:
        flag |= s.str.contains(kw, regex=False)
    return flag


# ---------------------------------------------------------------------------
def main() -> None:
    L1.mkdir(parents=True, exist_ok=True)
    diag: list[str] = ["# L1 dim 빌드 진단 (wf2q_10_build_dims.py)", ""]

    # ---- 1. 청크에서 당사자 목록 수집 -------------------------------------
    print("[1/4] L0 청크에서 당사자 수집")
    parts = []          # pcid 별 집계 조각
    pair_counts = []    # 휴리스틱용 (consignee-shipper 페어·HS2)
    for m in MONTHS:
        f = L0 / f"imp_ship_{m}.parquet"
        if not f.exists():
            sys.exit(f"누락: {f} — 추출(ex_20260806_wf2q_pull.py)을 먼저 완료하세요")
        cols = ["conpanjivaid", "conname", "concountry", "con_companyid",
                "shppanjivaid", "shpname", "shpcountry", "shp_companyid", "hs2"]
        df = pd.read_parquet(f, columns=cols)
        for side, role in (("con", "consignee"), ("shp", "shipper")):
            g = (df.groupby(f"{side}panjivaid", dropna=True)
                   .agg(name=(f"{side}name", "first"),
                        country=(f"{side}country", "first"),
                        ciq=(f"{side}_companyid", "first"),
                        n=(f"{side}panjivaid", "size")))
            g["role"] = role
            parts.append(g.reset_index().rename(columns={f"{side}panjivaid": "pcid"}))
        pair_counts.append(
            df.groupby(["conpanjivaid", "shppanjivaid"], dropna=False)
              .agg(n=("hs2", "size"), n_hs2=("hs2", "nunique")).reset_index())
        del df

        fe = L0 / f"exp_ship_{m}.parquet"
        de = pd.read_parquet(fe, columns=["shppanjivaid", "shpname", "shpcountry",
                                          "shp_companyid"])
        g = (de.groupby("shppanjivaid", dropna=True)
               .agg(name=("shpname", "first"), country=("shpcountry", "first"),
                    ciq=("shp_companyid", "first"), n=("shppanjivaid", "size")))
        g["role"] = "exporter_us"
        parts.append(g.reset_index().rename(columns={"shppanjivaid": "pcid"}))
        del de
        print(f"  {m} 완료")

    allp = pd.concat(parts, ignore_index=True)
    dim = (allp.sort_values("n", ascending=False)      # 최다 등장 이름을 대표로
               .groupby("pcid")
               .agg(name_raw=("name", "first"),
                    country=("country", "first"),
                    ciq_companyid=("ciq", "first"),
                    n_shipments=("n", "sum"),
                    roles=("role", lambda s: "|".join(sorted(set(s)))))
               .reset_index())
    diag += [f"- 고유 Panjiva 기업 id: **{len(dim):,}**",
             f"- CIQ 매칭: {dim['ciq_companyid'].notna().sum():,} "
             f"({dim['ciq_companyid'].notna().mean():.1%})"]

    # ---- 2. 이름 표준화·redaction·포워더 ---------------------------------
    print("[2/4] 이름 표준화·플래그")
    dim["name_std"] = name_std(dim["name_raw"])
    dim["is_redacted"] = is_redacted(dim["name_raw"])

    # redaction 패턴별 빈도 (진단)
    up = dim["name_raw"].astype("string").str.upper().str.strip()
    diag += ["", "## redaction 패턴 프로파일", "",
             "| 패턴 | 기업 수 |", "|---|---|",
             f"| (이름 결측/공백) | {(up.isna() | up.eq('')).sum():,} |"]
    for pat in REDACTION_PATTERNS:
        diag.append(f"| `{pat}` | {up.str.contains(pat, regex=True, na=False).sum():,} |")

    co = pd.read_parquet(L0 / "company.parquet")
    ind = co.set_index("companyid")["industry"]
    dim["ciq_industry"] = dim["ciq_companyid"].map(ind)
    fw_name = is_forwarder_name(dim["name_std"])
    fw_ind = dim["ciq_industry"].astype("string") \
        .str.contains(FORWARDER_INDUSTRY_RE, regex=True, na=False)
    dim["is_forwarder"] = (fw_name | fw_ind).astype("int8")
    diag += ["", "## 포워더 플래그 (v1 = 이름 키워드 ∪ CIQ 운송·물류 산업)", "",
             f"- 이름 키워드: {fw_name.sum():,} / 산업: {fw_ind.sum():,} / "
             f"합집합: {int(dim['is_forwarder'].sum()):,} "
             f"({dim['is_forwarder'].mean():.1%})"]

    # ---- 3. 휴리스틱 (파트너수 × HS2 다양성 동시 극단) --------------------
    print("[3/4] 포워더 휴리스틱")
    pc = (pd.concat(pair_counts, ignore_index=True)
            .groupby(["conpanjivaid", "shppanjivaid"], dropna=False).sum().reset_index())
    heur_flags = {}
    for side, other in (("conpanjivaid", "shppanjivaid"), ("shppanjivaid", "conpanjivaid")):
        g = pc.groupby(side).agg(n_partners=(other, "nunique"), n=("n", "sum"))
        hs = (pd.concat(pair_counts, ignore_index=True)
                .groupby(side)["n_hs2"].max())          # 청크 최대 — 근사, 진단용
        g = g.join(hs.rename("n_hs2"))
        big = g[g["n"] >= 50]                            # 최소 규모 컷
        if len(big):
            p99p = big["n_partners"].quantile(0.99)
            p99h = big["n_hs2"].quantile(0.99)
            flagged = big[(big["n_partners"] >= p99p) & (big["n_hs2"] >= p99h)].index
            heur_flags.update({pid: 1 for pid in flagged})
            diag.append(f"- {side}: p99 파트너수 {p99p:.0f} · p99 HS2 {p99h:.0f} → "
                        f"극단 {len(flagged):,}개")
    dim["is_forwarder_heur"] = dim["pcid"].map(heur_flags).fillna(0).astype("int8")

    dim.to_parquet(L1 / "dim_company.parquet", index=False)
    print(f"  -> dim_company.parquet {len(dim):,}행")

    # ---- 4. dim_group_link (PIT) + 구간 검증 ------------------------------
    print("[4/4] dim_group_link")
    pit = pd.read_parquet(L0 / "pit_intervals.parquet")
    pit["source_version"] = "ciq_up_pit"
    pit = pit.sort_values(["companyid", "start_date"])
    # 게이트: 회사 내 구간 겹침 0 (1주치 실측 성질 재확인)
    prev_end = pit.groupby("companyid")["end_date"].shift()
    overlap = (pit["start_date"] <= prev_end).sum()
    pit.to_parquet(L1 / "dim_group_link.parquet", index=False)
    n_multi = (pit.groupby("companyid").size() > 1).sum()
    diag += ["", "## dim_group_link (PIT)", "",
             f"- 구간 {len(pit):,}행 / 기업 {pit['companyid'].nunique():,} / "
             f"이력 2구간 이상 {n_multi:,}",
             f"- **구간 겹침: {overlap}건** (0 이어야 함)"]
    if overlap:
        sys.exit(f"[게이트 실패] PIT 구간 겹침 {overlap}건 — 선적 조인 전 원인 확인 필요")

    (L1 / "diag_dims.md").write_text("\n".join(diag) + "\n", encoding="utf-8")
    print(f"완료. 진단: {L1 / 'diag_dims.md'}")


if __name__ == "__main__":
    main()
