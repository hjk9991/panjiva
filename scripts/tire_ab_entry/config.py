"""Immutable scope configuration for the licensed tire package."""

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType


OUTPUT_ROOT = Path(r"C:\panjiva\data\staging\ab_entry_tire_v1")
START_QUARTER = "2014Q1"
END_QUARTER = "2025Q4"

ENTRY_VALUE_USD = 100_000
ENTRY_CORE_VALUE_USD = 1_000_000
ENTRY_CORE_SHIPMENTS = 3
PRE_ENTRY_YEARS = 2


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
# passenger and on-highway light-truck statistical reporting codes.
FINISHED_PROBE_PREFIXES = ("401110", "401120")
REVIEWED_ESTIMATION_CODES = (
    "4011101000",
    "4011105000",
    "4011201005",
    "4011205010",
)

MANUFACTURER_PARENT_TARGETS = (
    "Michelin",
    "Goodyear",
    "Hankook Tire & Technology",
)


def iter_quarters():
    """Yield the approved quarterly extraction window in chronological order."""

    return (
        f"{year}Q{quarter}"
        for year in range(2014, 2026)
        for quarter in range(1, 5)
    )


def validate_output_path(path: Path | str) -> bool:
    """Accept a path only when it resolves inside the licensed output root."""

    target = Path(path).resolve(strict=False)
    root = OUTPUT_ROOT.resolve(strict=False)
    if not target.is_relative_to(root):
        raise ValueError(f"path is outside licensed output root: {target}")
    return True
