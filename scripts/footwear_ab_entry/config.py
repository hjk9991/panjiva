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
# unlike the tire pilot where statistical depth was required.  Escalation
# families are probed but stay out of the estimation market unless the
# pre-registered brand-attributed boundary review admits them.
HS_FAMILIES = MappingProxyType(
    {
        "640219": HsFamily("640219", "included"),
        "640319": HsFamily("640319", "included"),
        "640411": HsFamily("640411", "included"),
        "640419": HsFamily("640419", "escalation_candidate"),
        "640299": HsFamily("640299", "escalation_candidate"),
        "640399": HsFamily("640399", "escalation_candidate"),
    }
)
SPORTS_FAMILIES = tuple(
    family for family, spec in HS_FAMILIES.items() if spec.status == "included"
)
PROBE_FAMILIES = tuple(HS_FAMILIES)

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
