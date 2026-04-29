"""Polygon geometry helpers for terminal-area containment checks."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Sequence


def _dms_to_decimal(degrees: int, minutes: int, seconds: int) -> float:
    return degrees + minutes / 60.0 + seconds / 3600.0


def parse_fdrg(path: str | Path) -> list[tuple[float, float]]:
    """Parse FDRG.txt and return a list of (lat, lon) decimal-degree vertices.

    Each line is expected in the format:
        DD,MM,SSN  DDD,MM,SSE
    e.g.  22,52,54N  113,29,00E
    """
    pattern = re.compile(
        r"(\d+),(\d+),(\d+)([NS])\s+(\d+),(\d+),(\d+)([EW])"
    )
    vertices: list[tuple[float, float]] = []
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = pattern.search(line)
        if not m:
            continue
        lat_deg, lat_min, lat_sec, lat_hemi = (
            int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4).upper()
        )
        lon_deg, lon_min, lon_sec, lon_hemi = (
            int(m.group(5)), int(m.group(6)), int(m.group(7)), m.group(8).upper()
        )
        lat = _dms_to_decimal(lat_deg, lat_min, lat_sec)
        if lat_hemi == "S":
            lat = -lat
        lon = _dms_to_decimal(lon_deg, lon_min, lon_sec)
        if lon_hemi == "W":
            lon = -lon
        vertices.append((lat, lon))
    return vertices


class TerminalArea:
    """Horizontal polygon + altitude ceiling for a terminal area.

    Uses the ray-casting algorithm for point-in-polygon tests.
    """

    def __init__(
        self,
        vertices: Sequence[tuple[float, float]],
        ceiling_m: float = 4500.0,
        floor_m: float = 0.0,
        airports: frozenset[str] | None = None,
    ) -> None:
        self._vertices: list[tuple[float, float]] = list(vertices)
        self.ceiling_m = ceiling_m
        self.floor_m = floor_m
        self.airports: frozenset[str] = airports or frozenset()

    # ------------------------------------------------------------------
    # Core geometry
    # ------------------------------------------------------------------

    def contains_point(self, lat: float, lon: float) -> bool:
        """Return True when (lat, lon) falls strictly inside the polygon."""
        n = len(self._vertices)
        if n < 3:
            return False
        inside = False
        x, y = lon, lat  # treat lon as x, lat as y for 2-D check
        j = n - 1
        for i in range(n):
            xi, yi = self._vertices[i][1], self._vertices[i][0]
            xj, yj = self._vertices[j][1], self._vertices[j][0]
            if ((yi > y) != (yj > y)) and (
                x < (xj - xi) * (y - yi) / (yj - yi) + xi
            ):
                inside = not inside
            j = i
        return inside

    def inside(self, lat: float, lon: float, altitude_m: float) -> bool:
        """Return True when the point is within both the polygon and altitude bounds."""
        # altitude_m == 0 typically means "field absent / on ground" in CAT062;
        # treat it as invalid and reject early.
        if altitude_m <= 0:
            return False
        if altitude_m < self.floor_m or altitude_m > self.ceiling_m:
            return False
        return self.contains_point(lat, lon)

    def is_terminal_airport(self, icao: str) -> bool:
        """Return True when *icao* is one of the airports within this terminal area."""
        return icao.strip().upper() in self.airports

    def both_inside(self, adep: str, adst: str) -> bool:
        """Return True when both departure and destination are terminal airports."""
        if not adep or not adst:
            return False
        return self.is_terminal_airport(adep) and self.is_terminal_airport(adst)

    @classmethod
    def from_fdrg(
        cls,
        fdrg_path: str | Path,
        ceiling_m: float = 4500.0,
        floor_m: float = 0.0,
        airports: Sequence[str] | None = None,
    ) -> "TerminalArea":
        """Create a TerminalArea from a legacy FDRG.txt polygon file."""
        vertices = parse_fdrg(fdrg_path)
        airport_set = frozenset(a.strip().upper() for a in (airports or []))
        return cls(vertices, ceiling_m=ceiling_m, floor_m=floor_m, airports=airport_set)

    @classmethod
    def from_json(
        cls,
        json_path: str | Path,
        ceiling_m: float | None = None,
        floor_m: float | None = None,
        airports: Sequence[str] | None = None,
    ) -> "TerminalArea":
        """Create a TerminalArea from a JSON configuration file.

        The JSON file should contain:
            vertices  - list of [lat, lon] decimal-degree pairs
            ceiling_m - optional vertical ceiling override (metres)
            floor_m   - optional vertical floor override (metres)
            airports  - optional list of ICAO airport codes

        Parameters passed to this method take precedence over JSON values.
        """
        raw = json.loads(Path(json_path).read_text(encoding="utf-8"))
        vertices = [
            (float(lat), float(lon))
            for lat, lon in raw["vertices"]
        ]
        effective_ceiling = ceiling_m if ceiling_m is not None else float(raw.get("ceiling_m", 4500.0))
        effective_floor = floor_m if floor_m is not None else float(raw.get("floor_m", 0.0))

        json_airports = [a.strip().upper() for a in raw.get("airports", [])]
        override_airports = [a.strip().upper() for a in (airports or [])]
        airport_set = frozenset(override_airports or json_airports)

        return cls(
            vertices,
            ceiling_m=effective_ceiling,
            floor_m=effective_floor,
            airports=airport_set,
        )
