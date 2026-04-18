from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta

from ..models import RadarTrack


class Cat062ParseError(ValueError):
    pass


@dataclass(slots=True)
class _Cursor:
    data: bytes
    index: int
    end: int

    def remaining(self) -> int:
        return self.end - self.index

    def skip(self, size: int) -> None:
        self.require(size)
        self.index += size

    def require(self, size: int) -> None:
        if self.index + size > self.end:
            raise Cat062ParseError(f"CAT062 数据不足: 需要 {size} 字节, 剩余 {self.remaining()} 字节")

    def read(self, size: int) -> bytes:
        self.require(size)
        chunk = self.data[self.index : self.index + size]
        self.index += size
        return chunk

    def read_u8(self) -> int:
        self.require(1)
        value = self.data[self.index]
        self.index += 1
        return value


class Cat062Parser:
    def parse_datagram(self, payload: bytes, received_at: datetime | None = None) -> list[RadarTrack]:
        if len(payload) < 3:
            return []

        received_at = received_at or datetime.utcnow()
        declared_length = int.from_bytes(payload[1:3], byteorder="big", signed=False)
        total_length = min(len(payload), declared_length) if declared_length >= 3 else len(payload)

        records: list[RadarTrack] = []
        index = 3
        while index < total_length:
            track, next_index = self._parse_record(payload, index, total_length, received_at)
            if next_index <= index:
                break
            index = next_index
            if track.track_number >= 0 or track.primary_callsign:
                records.append(track)
        return records

    def _parse_record(
        self,
        payload: bytes,
        start_index: int,
        end: int,
        received_at: datetime,
    ) -> tuple[RadarTrack, int]:
        cursor = _Cursor(payload, start_index, end)
        fspecs = self._read_fspecs(cursor)
        fs1 = fspecs[0] if len(fspecs) > 0 else 0
        fs2 = fspecs[1] if len(fspecs) > 1 else 0
        fs3 = fspecs[2] if len(fspecs) > 2 else 0
        fs4 = fspecs[3] if len(fspecs) > 3 else 0
        fs5 = fspecs[4] if len(fspecs) > 4 else 0

        track = RadarTrack(received_at=received_at)

        bit010 = (fs1 & 0x80) >> 7
        bit015 = (fs1 & 0x20) >> 5
        bit070 = (fs1 & 0x10) >> 4
        bit105 = (fs1 & 0x08) >> 3
        bit100 = (fs1 & 0x04) >> 2
        bit185 = (fs1 & 0x02) >> 1

        bit210 = (fs2 & 0x80) >> 7
        bit060 = (fs2 & 0x40) >> 6
        bit245 = (fs2 & 0x20) >> 5
        bit380 = (fs2 & 0x10) >> 4
        bit040 = (fs2 & 0x08) >> 3
        bit080 = (fs2 & 0x04) >> 2
        bit290 = (fs2 & 0x02) >> 1

        bit200 = (fs3 & 0x80) >> 7
        bit295 = (fs3 & 0x40) >> 6
        bit136 = (fs3 & 0x20) >> 5
        bit130 = (fs3 & 0x10) >> 4
        bit135 = (fs3 & 0x08) >> 3
        bit220 = (fs3 & 0x04) >> 2
        bit390 = (fs3 & 0x02) >> 1

        bit270 = (fs4 & 0x80) >> 7
        bit300 = (fs4 & 0x40) >> 6
        bit110 = (fs4 & 0x20) >> 5
        bit120 = (fs4 & 0x10) >> 4
        bit510 = (fs4 & 0x08) >> 3
        bit500 = (fs4 & 0x04) >> 2
        bit340 = (fs4 & 0x02) >> 1

        _ = fs5

        if bit010:
            cursor.skip(2)
        if bit015:
            cursor.skip(1)

        if bit070:
            seconds = self._read_u24(cursor) / 128.0
            base_day = datetime.combine(received_at.date(), time.min)
            track.time_of_track = base_day + timedelta(seconds=seconds)

        if bit105:
            track.latitude = self._read_i32(cursor) * 180.0 / 33554432.0
            track.longitude = self._read_i32(cursor) * 180.0 / 33554432.0

        if bit100:
            track.cartesian_x_m = int(self._read_i24(cursor) / 2)
            track.cartesian_y_m = int(self._read_i24(cursor) / 2)

        if bit185:
            vx = self._read_i16(cursor)
            vy = self._read_i16(cursor)
            speed_x = vx * 0.25 * 3.6
            speed_y = vy * 0.25 * 3.6
            track.spdx_kmh = speed_x
            track.spdy_kmh = speed_y
            track.speed_kmh = math.sqrt(speed_x * speed_x + speed_y * speed_y)
            track.heading_deg = self._cal_heading(speed_x, speed_y)

        if bit210:
            cursor.skip(2)

        if bit060:
            track.ssr = self._read_ssr(cursor)

        if bit245:
            cursor.skip(1)
            track.target_id = self._decode_ia5_callsign(cursor.read(6)).strip()

        if bit380:
            self._parse_380(cursor, track)

        if bit040:
            track.track_number = self._read_u16(cursor)

        if bit080:
            track.flight_plan_correlated = self._parse_080(cursor)

        if bit290:
            self._parse_290(cursor)

        if bit200:
            cursor.skip(1)

        if bit295:
            self._parse_295(cursor)

        if bit136:
            track.flight_level_m = self._read_i16(cursor) * 25 * 0.3048

        if bit130:
            cursor.skip(2)

        if bit135:
            value = self._read_u16(cursor)
            track.qnh_applied = bool(value & 0x8000)
            track.qnh_height_m = (value & 0x7FFF) * 25 * 0.3048

        if bit220:
            cursor.skip(2)

        if bit390:
            self._parse_390(cursor, track)

        if bit270:
            self._parse_270(cursor)

        if bit300:
            cursor.skip(1)

        if bit110:
            self._parse_110(cursor)

        if bit120:
            cursor.skip(2)

        if bit510:
            self._parse_510(cursor)

        if bit500:
            self._parse_500(cursor)

        if bit340:
            self._parse_340(cursor)

        if track.time_of_track is None:
            track.time_of_track = received_at
        return track, cursor.index

    def _read_fspecs(self, cursor: _Cursor) -> list[int]:
        fspecs: list[int] = []
        while True:
            value = cursor.read_u8()
            fspecs.append(value)
            if value & 0x01 == 0:
                break
            if len(fspecs) >= 5:
                break
        return fspecs

    def _parse_080(self, cursor: _Cursor) -> int:
        correlated = 0
        fx1 = cursor.read_u8() & 0x01
        if fx1:
            second = cursor.read_u8()
            correlated = (second & 0x10) >> 4
            fx2 = second & 0x01
            if fx2:
                third = cursor.read_u8()
                if third & 0x01:
                    cursor.skip(1)
        return correlated

    def _parse_290(self, cursor: _Cursor) -> None:
        octet1 = cursor.read_u8()
        flags1 = [
            (octet1 & 0x80, 1),
            (octet1 & 0x40, 1),
            (octet1 & 0x20, 1),
            (octet1 & 0x10, 1),
            (octet1 & 0x08, 2),
            (octet1 & 0x04, 1),
            (octet1 & 0x02, 1),
        ]
        fx1 = octet1 & 0x01
        octet2 = cursor.read_u8() if fx1 else None
        if octet2 is not None:
            flags2 = [
                (octet2 & 0x80, 1),
                (octet2 & 0x40, 1),
                (octet2 & 0x20, 1),
            ]
            for enabled, size in flags2:
                if enabled:
                    cursor.skip(size)
        for enabled, size in flags1:
            if enabled:
                cursor.skip(size)

    def _parse_295(self, cursor: _Cursor) -> None:
        octets: list[int] = []
        while True:
            octet = cursor.read_u8()
            octets.append(octet)
            if octet & 0x01 == 0:
                break
            if len(octets) >= 5:
                break

        size_map = [
            [1, 1, 1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1, 1, 1],
            [1, 1, 1],
        ]
        for idx, octet in enumerate(octets):
            sizes = size_map[idx]
            bits = [0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02][: len(sizes)]
            for mask, size in zip(bits, sizes, strict=False):
                if octet & mask:
                    cursor.skip(size)

    def _parse_270(self, cursor: _Cursor) -> None:
        octet = cursor.read_u8()
        if octet & 0x01:
            octet = cursor.read_u8()
            if octet & 0x01:
                octet = cursor.read_u8()
                if octet & 0x01:
                    cursor.skip(1)

    def _parse_110(self, cursor: _Cursor) -> None:
        octet = cursor.read_u8()
        sizes = [(0x80, 1), (0x40, 4), (0x20, 6), (0x10, 2), (0x08, 2), (0x04, 1), (0x02, 1)]
        for mask, size in sizes:
            if octet & mask:
                cursor.skip(size)

    def _parse_510(self, cursor: _Cursor) -> None:
        chunk = cursor.read(3)
        if chunk[2] & 0x01:
            cursor.skip(3)

    def _parse_500(self, cursor: _Cursor) -> None:
        octets: list[int] = []
        while True:
            octet = cursor.read_u8()
            octets.append(octet)
            if octet & 0x01 == 0:
                break
            if len(octets) >= 2:
                break
        size_map = [[4, 2, 4, 1, 1, 2, 2], [1]]
        for idx, octet in enumerate(octets):
            sizes = size_map[idx]
            bits = [0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02][: len(sizes)]
            for mask, size in zip(bits, sizes, strict=False):
                if octet & mask:
                    cursor.skip(size)

    def _parse_340(self, cursor: _Cursor) -> None:
        octet = cursor.read_u8()
        sizes = [(0x80, 2), (0x40, 4), (0x20, 2), (0x10, 2), (0x08, 2), (0x04, 1)]
        for mask, size in sizes:
            if octet & mask:
                cursor.skip(size)

    def _parse_380(self, cursor: _Cursor, track: RadarTrack) -> None:
        octets: list[int] = []
        while True:
            octet = cursor.read_u8()
            octets.append(octet)
            if octet & 0x01 == 0:
                break
            if len(octets) >= 4:
                break

        bit_sets = [
            {
                "ADR": bool(octets[0] & 0x80),
                "ID": bool(octets[0] & 0x40),
                "MHG": bool(octets[0] & 0x20),
                "IAS": bool(octets[0] & 0x10),
                "TAS": bool(octets[0] & 0x08),
                "SAL": bool(octets[0] & 0x04),
                "FSS": bool(octets[0] & 0x02),
            }
        ]
        if len(octets) > 1:
            bit_sets.append(
                {
                    "TIS": bool(octets[1] & 0x80),
                    "TID": bool(octets[1] & 0x40),
                    "COM": bool(octets[1] & 0x20),
                    "SAB": bool(octets[1] & 0x10),
                    "ACS": bool(octets[1] & 0x08),
                    "BVR": bool(octets[1] & 0x04),
                    "GVR": bool(octets[1] & 0x02),
                }
            )
        if len(octets) > 2:
            bit_sets.append(
                {
                    "RAN": bool(octets[2] & 0x80),
                    "TAR": bool(octets[2] & 0x40),
                    "TAN": bool(octets[2] & 0x20),
                    "GSP": bool(octets[2] & 0x10),
                    "VUN": bool(octets[2] & 0x08),
                    "MET": bool(octets[2] & 0x04),
                    "EMC": bool(octets[2] & 0x02),
                }
            )
        if len(octets) > 3:
            bit_sets.append(
                {
                    "POS": bool(octets[3] & 0x80),
                    "GAL": bool(octets[3] & 0x40),
                    "PUN": bool(octets[3] & 0x20),
                    "MB": bool(octets[3] & 0x10),
                    "IAR": bool(octets[3] & 0x08),
                    "MAC": bool(octets[3] & 0x04),
                    "BPS": bool(octets[3] & 0x02),
                }
            )

        first = bit_sets[0]
        if first.get("ADR"):
            cursor.skip(3)
        if first.get("ID"):
            callsign = self._decode_ia5_callsign(cursor.read(6)).strip()
            if not track.target_id:
                track.target_id = callsign
        if first.get("MHG"):
            cursor.skip(2)
        if first.get("IAS"):
            cursor.skip(2)
        if first.get("TAS"):
            cursor.skip(2)
        if first.get("SAL"):
            cursor.skip(2)
        if first.get("FSS"):
            value = self._read_u16(cursor)
            track.selected_altitude_m = int((value & 0x1FFF) * 25 * 0.3048)

        if len(bit_sets) > 1:
            second = bit_sets[1]
            if second.get("TIS"):
                cursor.skip(1)
            if second.get("TID"):
                rep = cursor.read_u8()
                cursor.skip(15 * rep)
            if second.get("COM"):
                cursor.skip(2)
            if second.get("SAB"):
                cursor.skip(2)
            if second.get("ACS"):
                cursor.skip(7)
            if second.get("BVR"):
                cursor.skip(2)
            if second.get("GVR"):
                cursor.skip(2)

        if len(bit_sets) > 2:
            third = bit_sets[2]
            if third.get("RAN"):
                cursor.skip(2)
            if third.get("TAR"):
                cursor.skip(2)
            if third.get("TAN"):
                cursor.skip(2)
            if third.get("GSP"):
                cursor.skip(2)
            if third.get("VUN"):
                cursor.skip(1)
            if third.get("MET"):
                cursor.skip(8)
            if third.get("EMC"):
                cursor.skip(1)

        if len(bit_sets) > 3:
            fourth = bit_sets[3]
            if fourth.get("POS"):
                cursor.skip(6)
            if fourth.get("GAL"):
                cursor.skip(2)
            if fourth.get("PUN"):
                cursor.skip(1)
            if fourth.get("MB"):
                rep = cursor.read_u8()
                cursor.skip(8 * rep)
            if fourth.get("IAR"):
                cursor.skip(2)
            if fourth.get("MAC"):
                cursor.skip(2)
            if fourth.get("BPS"):
                cursor.skip(2)

    def _parse_390(self, cursor: _Cursor, track: RadarTrack) -> None:
        octets: list[int] = []
        while True:
            octet = cursor.read_u8()
            octets.append(octet)
            if octet & 0x01 == 0:
                break
            if len(octets) >= 3:
                break

        first = {
            "TAG": bool(octets[0] & 0x80),
            "CSN": bool(octets[0] & 0x40),
            "IFI": bool(octets[0] & 0x20),
            "FCT": bool(octets[0] & 0x10),
            "TAC": bool(octets[0] & 0x08),
            "WTC": bool(octets[0] & 0x04),
            "DEP": bool(octets[0] & 0x02),
        }
        second = {
            "DST": False,
            "RDS": False,
            "CFL": False,
            "CTL": False,
            "TOD": False,
            "AST": False,
            "STS": False,
        }
        third = {"STD": False, "STA": False, "PEM": False, "PEC": False}
        if len(octets) > 1:
            second = {
                "DST": bool(octets[1] & 0x80),
                "RDS": bool(octets[1] & 0x40),
                "CFL": bool(octets[1] & 0x20),
                "CTL": bool(octets[1] & 0x10),
                "TOD": bool(octets[1] & 0x08),
                "AST": bool(octets[1] & 0x04),
                "STS": bool(octets[1] & 0x02),
            }
        if len(octets) > 2:
            third = {
                "STD": bool(octets[2] & 0x80),
                "STA": bool(octets[2] & 0x40),
                "PEM": bool(octets[2] & 0x20),
                "PEC": bool(octets[2] & 0x10),
            }

        if first["TAG"]:
            cursor.skip(2)
        if first["CSN"]:
            track.acid = cursor.read(7).decode("utf-8", errors="ignore").strip()
        if first["IFI"]:
            cursor.skip(4)
        if first["FCT"]:
            cursor.skip(1)
        if first["TAC"]:
            track.aircraft_type = cursor.read(4).decode("utf-8", errors="ignore").strip()
        if first["WTC"]:
            track.wtc = chr(cursor.read_u8()).strip()
        if first["DEP"]:
            track.adep = cursor.read(4).decode("utf-8", errors="ignore").strip()
        if second["DST"]:
            track.adst = cursor.read(4).decode("utf-8", errors="ignore").strip()
        if second["RDS"]:
            track.runway = cursor.read(3).decode("utf-8", errors="ignore").strip()
        if second["CFL"]:
            track.cfl_m = self._read_i16(cursor) * 25 * 0.3048
        if second["CTL"]:
            cursor.require(2)
            first_byte = cursor.read_u8()
            second_byte = cursor.read_u8()
            _ = first_byte
            track.sector_index = second_byte
        if second["TOD"]:
            rep = cursor.read_u8()
            cursor.skip(rep * 4)
        if second["AST"]:
            cursor.skip(6)
        if second["STS"]:
            cursor.skip(1)
        if third["STD"]:
            track.sid = cursor.read(7).decode("utf-8", errors="ignore").strip()
        if third["STA"]:
            track.star = cursor.read(7).decode("utf-8", errors="ignore").strip()
        if third["PEM"]:
            cursor.skip(2)
        if third["PEC"]:
            cursor.skip(7)

    def _read_ssr(self, cursor: _Cursor) -> str:
        b1 = cursor.read_u8() & 0x0F
        b2 = cursor.read_u8()
        value = b1 * 256 + b2
        digits: list[str] = []
        for _ in range(4):
            digits.append(str(value & 0x07))
            value >>= 3
        return "".join(reversed(digits))

    def _decode_ia5_callsign(self, payload: bytes) -> str:
        if len(payload) != 6:
            return ""
        codes = [
            (payload[0] & 0xFC) >> 2,
            ((payload[0] & 0x03) << 4) | ((payload[1] & 0xF0) >> 4),
            ((payload[1] & 0x0F) << 2) | ((payload[2] & 0xC0) >> 6),
            payload[2] & 0x3F,
            (payload[3] & 0xFC) >> 2,
            ((payload[3] & 0x03) << 4) | ((payload[4] & 0xF0) >> 4),
            ((payload[4] & 0x0F) << 2) | ((payload[5] & 0xC0) >> 6),
            payload[5] & 0x3F,
        ]
        return "".join(self._ia5_to_ascii(code) for code in codes)

    def _ia5_to_ascii(self, value: int) -> str:
        if value == 0:
            return " "
        if value <= 26:
            return chr(value + 64)
        return chr(value)

    def _read_u16(self, cursor: _Cursor) -> int:
        return int.from_bytes(cursor.read(2), byteorder="big", signed=False)

    def _read_i16(self, cursor: _Cursor) -> int:
        return int.from_bytes(cursor.read(2), byteorder="big", signed=True)

    def _read_u24(self, cursor: _Cursor) -> int:
        return int.from_bytes(cursor.read(3), byteorder="big", signed=False)

    def _read_i24(self, cursor: _Cursor) -> int:
        raw = cursor.read(3)
        value = int.from_bytes(raw, byteorder="big", signed=False)
        if value & 0x800000:
            value -= 0x1000000
        return value

    def _read_i32(self, cursor: _Cursor) -> int:
        return int.from_bytes(cursor.read(4), byteorder="big", signed=True)

    def _cal_heading(self, speed_x: float, speed_y: float) -> float:
        if speed_x == 0 and speed_y == 0:
            return 0.0
        return (math.degrees(math.atan2(speed_x, speed_y)) + 360.0) % 360.0
