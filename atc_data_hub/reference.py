from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .models import FlightDestination

DEFAULT_TERMINAL_AIRPORTS = {"ZGSZ", "ZGSD", "VMMC", "ZGNT", "ZGUH", "ZGHZ"}
DEFAULT_AIRPORT_TRAILS = {"ZGSZ", "ZGSD", "VMMC"}
DEFAULT_SECTOR_INFO = [
    "",
    "HE",
    "HN",
    "AS",
    "ARW",
    "ARE",
    "ASL",
    "AD",
    "AA",
    "TM",
    "SE",
    "SW",
    "TZ",
    "A1",
    "A2",
    "A3",
    "A4",
    "A5",
    "A6",
    "A7",
]


@dataclass(slots=True)
class FixInfo:
    name: str
    lat: str
    lon: str


@dataclass(slots=True)
class TransferPointRule:
    transfer_point: str
    keyfix: str

    def matches(self, route: str) -> bool:
        route_text = f" {route.upper()} "
        rule_text = self.keyfix.upper().strip()
        return bool(rule_text) and rule_text in route_text


@dataclass(slots=True)
class HotSpotArea:
    name: str
    shape: str
    lower_m: int
    upper_m: int
    radius_m: float = 0.0
    flight_destination: FlightDestination = FlightDestination.OVERFLY
    points: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ReferenceData:
    terminal_airports: set[str] = field(default_factory=lambda: set(DEFAULT_TERMINAL_AIRPORTS))
    airport_trails: set[str] = field(default_factory=lambda: set(DEFAULT_AIRPORT_TRAILS))
    sector_info: list[str] = field(default_factory=lambda: list(DEFAULT_SECTOR_INFO))
    sector_capacity: list[list[int]] = field(default_factory=list)
    transfer_point_rules: list[TransferPointRule] = field(default_factory=list)
    fixes: dict[str, FixInfo] = field(default_factory=dict)
    radio_stations: list[str] = field(default_factory=list)
    hot_spots: list[HotSpotArea] = field(default_factory=list)

    def sector_name(self, sector_index: int) -> str:
        if 0 <= sector_index < len(self.sector_info):
            return self.sector_info[sector_index].strip()
        return ""

    def classify_flight(self, adep: str, adst: str) -> FlightDestination:
        adep = adep.strip().upper()
        adst = adst.strip().upper()
        if adst in self.terminal_airports and adep in self.terminal_airports:
            return FlightDestination.INSIDE
        if adst in self.terminal_airports:
            return FlightDestination.INBOUND
        if adep in self.terminal_airports:
            return FlightDestination.OUTBOUND
        return FlightDestination.OUTSIDE

    def resolve_transfer_fix(self, route: str) -> str:
        route = route.strip().upper()
        for rule in self.transfer_point_rules:
            if rule.matches(route):
                return rule.transfer_point
        return ""

    def capacity_row(self, index: int) -> list[int]:
        if 0 <= index < len(self.sector_capacity):
            return self.sector_capacity[index]
        return []


def load_reference_data(sys_config_root: Path | None) -> ReferenceData:
    reference = ReferenceData()
    if not sys_config_root:
        return reference

    sys_config_root = Path(sys_config_root)
    if not sys_config_root.exists():
        return reference

    _load_terminal_airports(reference, sys_config_root / "TerminalAirports.txt")
    _load_airport_trails(reference, sys_config_root / "AirportTrails.txt")
    _load_sector_info(reference, sys_config_root / "SectorInfo.txt")
    _load_sector_capacity(reference, sys_config_root / "SectorCapacity.txt")
    _load_transfer_fix_rules(reference, sys_config_root / "TransPtKeyFix.txt")
    _load_radio_stations(reference, sys_config_root / "RadioStations.txt")
    _load_hot_spots(reference, sys_config_root / "HotSpot.txt")

    map_data_root = sys_config_root.parent / "MapData"
    # Fix.txt may live directly in sys_config_root (flat layout) or in a
    # sibling MapData/ directory (legacy SysConfig layout).
    fix_path = sys_config_root / "Fix.txt"
    if not fix_path.exists():
        fix_path = map_data_root / "Fix.txt"
    _load_fixes(reference, fix_path)
    return reference


def _read_text_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "gb2312", "gbk"):
        try:
            text = path.read_text(encoding=encoding)
            return text.replace("\r", "").split("\n")
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore").replace("\r", "").split("\n")


def _clean_lines(lines: Iterable[str]) -> list[str]:
    cleaned: list[str] = []
    for line in lines:
        value = line.strip()
        if not value or value.startswith("//"):
            continue
        cleaned.append(value)
    return cleaned


def _load_terminal_airports(reference: ReferenceData, path: Path) -> None:
    values = _clean_lines(_read_text_lines(path))
    if values:
        reference.terminal_airports = {item.upper() for item in values}


def _load_airport_trails(reference: ReferenceData, path: Path) -> None:
    values = _clean_lines(_read_text_lines(path))
    if values:
        reference.airport_trails = {item.upper() for item in values}


def _load_sector_info(reference: ReferenceData, path: Path) -> None:
    values = _clean_lines(_read_text_lines(path))
    if values:
        reference.sector_info = [""] + [item.split("//", 1)[0].strip() for item in values]


def _load_sector_capacity(reference: ReferenceData, path: Path) -> None:
    rows: list[list[int]] = []
    for line in _read_text_lines(path):
        value = line.strip()
        if not value or value.startswith("//"):
            continue
        cells = [cell for cell in value.split("\t") if cell != ""]
        try:
            rows.append([int(cell) for cell in cells])
        except ValueError:
            continue
    if rows:
        reference.sector_capacity = rows


def _load_transfer_fix_rules(reference: ReferenceData, path: Path) -> None:
    rules: list[TransferPointRule] = []
    for line in _clean_lines(_read_text_lines(path)):
        parts = [part.strip() for part in line.split("\t") if part.strip()]
        if len(parts) != 2:
            continue
        rules.append(TransferPointRule(parts[0].upper(), parts[1].upper()))
    if rules:
        reference.transfer_point_rules = rules


def _load_fixes(reference: ReferenceData, path: Path) -> None:
    fixes: dict[str, FixInfo] = {}
    for line in _clean_lines(_read_text_lines(path)):
        parts = [part.strip() for part in line.split("\t")]
        if len(parts) < 3:
            continue
        fixes[parts[0].upper()] = FixInfo(parts[0].upper(), parts[1], parts[2])
    if fixes:
        reference.fixes = fixes


def _load_radio_stations(reference: ReferenceData, path: Path) -> None:
    values = _clean_lines(_read_text_lines(path))
    if values:
        reference.radio_stations = values


def _load_hot_spots(reference: ReferenceData, path: Path) -> None:
    lines = _read_text_lines(path)
    result: list[HotSpotArea] = []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line or line.startswith("//"):
            index += 1
            continue
        header = line.split("//", 1)[0].strip()
        parts = header.split()
        if not parts:
            index += 1
            continue

        kind = parts[0].upper()
        if kind == "ROUTE" and len(parts) >= 7:
            count = int(parts[2])
            result.append(
                HotSpotArea(
                    name=parts[1],
                    shape="R",
                    lower_m=int(parts[3]),
                    upper_m=int(parts[4]),
                    radius_m=float(parts[5]),
                    flight_destination=_parse_destination(parts[6]),
                    points=_collect_hot_spot_points(lines, index + 1, count),
                )
            )
            index += 1 + count
            continue
        if kind == "POLOGON" and len(parts) >= 6:
            count = int(parts[2])
            result.append(
                HotSpotArea(
                    name=parts[1],
                    shape="P",
                    lower_m=int(parts[3]),
                    upper_m=int(parts[4]),
                    flight_destination=_parse_destination(parts[5]),
                    points=_collect_hot_spot_points(lines, index + 1, count),
                )
            )
            index += 1 + count
            continue
        if kind == "CIRCLE" and len(parts) >= 6:
            result.append(
                HotSpotArea(
                    name=parts[1],
                    shape="C",
                    lower_m=int(parts[3]),
                    upper_m=int(parts[4]),
                    radius_m=float(parts[2]),
                    flight_destination=_parse_destination(parts[5]),
                    points=_collect_hot_spot_points(lines, index + 1, 1),
                )
            )
            index += 2
            continue
        index += 1
    if result:
        reference.hot_spots = result


def _collect_hot_spot_points(lines: list[str], start: int, count: int) -> list[str]:
    result: list[str] = []
    for idx in range(start, min(start + count, len(lines))):
        value = lines[idx].split("//", 1)[0].strip()
        if value:
            result.append(value)
    return result


def _parse_destination(value: str) -> FlightDestination:
    text = value.strip()
    for item in FlightDestination:
        if item.value.lower() == text.lower():
            return item
    return FlightDestination.OVERFLY
