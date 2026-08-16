"""Immutable scope configuration for the licensed tire package."""

from dataclasses import dataclass
from pathlib import Path
import platform
import re
from types import MappingProxyType


OUTPUT_ROOT = Path(r"C:\panjiva\data\staging\ab_entry_tire_v1")
APPROVED_ACCOUNT = "VLC67107"
APPROVED_DATABASE = "MI_XPRESSCLOUD"
APPROVED_SCHEMA = "XPRESSFEED"
APPROVED_WAREHOUSE = "XF_READER_KOREADEVELOPMENT_WH"
APPROVED_ROLE = "XF_READER_KOREADEVELOPMENT"
START_QUARTER = "2014Q1"
END_QUARTER = "2025Q4"

ENTRY_VALUE_USD = 100_000
ENTRY_CORE_VALUE_USD = 1_000_000
ENTRY_CORE_SHIPMENTS = 3
PRE_ENTRY_YEARS = 2
PARENT_CANDIDATE_WARN_ROWS_PER_TARGET = 250
PARENT_CANDIDATE_MAX_ROWS_PER_TARGET = 2_000


@dataclass(frozen=True)
class RawGroup:
    """One broad upstream-material probe family and its review status."""

    hs_prefix: str
    status: str


RAW_GROUPS = MappingProxyType(
    {
        "4001": RawGroup("4001", "included"),
        "4002": RawGroup("4002", "included"),
        "4005": RawGroup("4005", "included"),
        "280300": RawGroup("280300", "included"),
        "5902": RawGroup("5902", "included"),
        "7312": RawGroup("7312", "requires_review"),
        "7217": RawGroup("7217", "requires_review"),
        "7228": RawGroup("7228", "requires_review"),
    }
)

# Probe both finished-tire HS6 families, but estimate only from the reviewed
# codes. Reviewed 2026-08-16 against observed Panjiva codes: HS6 4011.10 is
# passenger-car tires by heading definition, so every observed 401110-family
# reporting depth (6/8/10-digit, incl. historical rim-diameter suffixes) is
# passenger; HS6 4011.20 mixes light and medium/heavy truck, so only explicit
# on-highway light-truck statistical codes qualify.
FINISHED_PROBE_PREFIXES = ("401110", "401120")
REVIEWED_ESTIMATION_CODES = (
    "401110",
    "40111010",
    "40111050",
    "4011101000",
    "4011101010",
    "4011101020",
    "4011101030",
    "4011101040",
    "4011101050",
    "4011101060",
    "4011101070",
    "4011105000",
    "4011201005",
    "4011205010",
)

MANUFACTURER_PARENT_TARGETS = (
    "Michelin",
    "Goodyear",
    "Hankook Tire & Technology",
)
MANUFACTURER_KEYS = ("MICHELIN", "GOODYEAR", "HANKOOK")
MANUFACTURER_DESCRIPTION_ALIASES = MappingProxyType(
    {
        "MICHELIN": ("michelin", "bf goodrich", "bfgoodrich"),
        "GOODYEAR": ("goodyear", "dunlop", "cooper tire"),
        "HANKOOK": ("hankook", "laufenn"),
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
