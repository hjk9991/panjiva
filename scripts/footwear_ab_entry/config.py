"""Immutable scope configuration for the licensed athletic footwear package."""

from dataclasses import dataclass
from pathlib import Path
import platform
import re
from types import MappingProxyType


OUTPUT_ROOT = Path(r"C:\panjiva\data\staging\ab_entry_footwear_v1")
APPROVED_ACCOUNT = "VLC67107"
APPROVED_DATABASE = "MI_XPRESSCLOUD"
APPROVED_SCHEMA = "XPRESSFEED"
APPROVED_WAREHOUSE = "XF_READER_KOREADEVELOPMENT_WH"
APPROVED_ROLE = "XF_READER_KOREADEVELOPMENT"
START_QUARTER = "2014Q1"
END_QUARTER = "2025Q4"

# Single finished-goods game: contract-manufactured athletic footwear.
GAMES = ("finished",)

ENTRY_VALUE_USD = 100_000
ENTRY_CORE_VALUE_USD = 1_000_000
ENTRY_CORE_SHIPMENTS = 3
PRE_ENTRY_YEARS = 2
PARENT_CANDIDATE_WARN_ROWS_PER_TARGET = 250
PARENT_CANDIDATE_MAX_ROWS_PER_TARGET = 2_000


@dataclass(frozen=True)
class HsFamily:
    """One probed HS6 family and its reviewed athletic status."""

    hs6: str
    status: str


# Athletic status is decidable at six digits (sports-footwear subheadings),
# unlike the tire pilot where statistical depth was required.  The
# pre-registered boundary escalation fired on 2026-08-17: sports codes
# captured only 35.1% of strategic importer value, so the brand-attributed
# general families were reviewed into the estimation market under a separate
# label (athletic_escalated_general).  The Deckers UGG contamination in the
# escalated label is a documented measurement caveat with a sports-only
# robustness cut.
HS_FAMILIES = MappingProxyType(
    {
        "640219": HsFamily("640219", "included"),
        "640319": HsFamily("640319", "included"),
        "640411": HsFamily("640411", "included"),
        "640419": HsFamily("640419", "included_escalated"),
        "640299": HsFamily("640299", "included_escalated"),
        "640399": HsFamily("640399", "included_escalated"),
    }
)
SPORTS_FAMILIES = tuple(
    family for family, spec in HS_FAMILIES.items() if spec.status == "included"
)
ESCALATED_FAMILIES = tuple(
    family
    for family, spec in HS_FAMILIES.items()
    if spec.status == "included_escalated"
)
ELIGIBLE_FAMILIES = (*SPORTS_FAMILIES, *ESCALATED_FAMILIES)
PROBE_FAMILIES = tuple(HS_FAMILIES)

# Structural window: pre-tariff regime with rich manifest attribution
# (2026-08-17 approval after the confidentiality-collapse diagnosis); the
# 2019-2021 tariff response is out-of-window validation.
STRUCTURAL_WINDOW_YEARS = (2016, 2017, 2018)

# G10: market-level Panjiva origin value shares against official US import
# value shares (approved 2026-08-17, replacing the Nike global-production
# anchor: 10-K factory shares measure global production, not US-bound
# imports, and the two diverge sharply pre-tariff).  Anchors are UN Comtrade
# US general imports (reporter 842, flow M, CIF primary value) for the six
# eligible HS6 families, pooled 2016-2018, retrieved 2026-08-17 from
# comtradeapi.un.org/public/v1/preview; Greater China aggregates China,
# Hong Kong, Taiwan and Macau partners.  Documented tolerance components:
# seaborne-manifest versus all-mode coverage, declared versus customs value,
# and shares conditional on a reported (non-UNKNOWN) origin token.  Firm
# attribution quality is covered by the G7 manual-review gate.
GREATER_CHINA_ORIGINS = ("China", "Hong Kong", "Taiwan", "Macau")
G10_IMPORT_ANCHOR_SHARES = MappingProxyType(
    {"GREATER_CHINA": 0.5568, "Vietnam": 0.2582, "Indonesia": 0.0730}
)
G10_TOLERANCE = 0.10

# "Nike, Inc" is deliberately specific: a bare "nike" substring floods the
# candidate safeguard with unrelated company names.
MANUFACTURER_PARENT_TARGETS = (
    "Nike, Inc",
    "Deckers Outdoor",
    "Under Armour",
    "Skechers",
)
MANUFACTURER_KEYS = ("NIKE", "DECKERS", "UNDER_ARMOUR", "SKECHERS")
MANUFACTURER_DESCRIPTION_ALIASES = MappingProxyType(
    {
        "NIKE": ("nike", "jordan", "converse"),
        "DECKERS": ("hoka",),
        "UNDER_ARMOUR": ("under armour",),
        "SKECHERS": ("skechers",),
    }
)


def _quarter_index(label: str) -> int:
    match = re.fullmatch(r"([0-9]{4})Q([1-4])", label)
    if match is None:
        raise ValueError(f"invalid quarter syntax: {label!r}")
    return int(match.group(1)) * 4 + int(match.group(2)) - 1


def iter_quarters():
    """Yield the approved quarterly extraction window in chronological order."""

    start = _quarter_index(START_QUARTER)
    end = _quarter_index(END_QUARTER)
    if start > end:
        raise ValueError("START_QUARTER must be before or equal to END_QUARTER")
    for index in range(start, end + 1):
        year, zero_based_quarter = divmod(index, 4)
        yield f"{year}Q{zero_based_quarter + 1}"


def _runtime_platform() -> str:
    return platform.system()


def validate_output_path(path: Path | str) -> Path:
    """Accept a path only when it resolves inside the licensed output root."""

    if _runtime_platform() != "Windows":
        raise RuntimeError("licensed runtime writes require Windows")
    target = Path(path).resolve(strict=False)
    root = OUTPUT_ROOT.resolve(strict=False)
    if not target.is_relative_to(root):
        raise ValueError(f"path is outside licensed output root: {target}")
    return target
