from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any

from .utils import encode_bytes, decode_bytes, format_date, format_datetime, parse_date, parse_datetime


class FlightDestination(str, Enum):
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"
    INSIDE = "Inside"
    OUTSIDE = "Outside"
    OVERFLY = "OverFly"


@dataclass(slots=True)
class TrackPoint:
    timestamp: datetime
    lat: float
    lon: float
    altitude_m: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": format_datetime(self.timestamp),
            "lat": self.lat,
            "lon": self.lon,
            "altitude_m": self.altitude_m,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TrackPoint":
        timestamp = parse_datetime(raw.get("timestamp")) or datetime.utcnow()
        return cls(
            timestamp=timestamp,
            lat=float(raw.get("lat", 0.0)),
            lon=float(raw.get("lon", 0.0)),
            altitude_m=float(raw.get("altitude_m", 0.0)),
        )


@dataclass(slots=True)
class TrailRecord:
    callsign: str = ""
    trail_time: datetime | None = None
    trail_type: int = 0
    points: list[TrackPoint] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "callsign": self.callsign,
            "trail_time": format_datetime(self.trail_time),
            "trail_type": self.trail_type,
            "points": [point.to_dict() for point in self.points],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TrailRecord":
        return cls(
            callsign=str(raw.get("callsign", "")),
            trail_time=parse_datetime(raw.get("trail_time")),
            trail_type=int(raw.get("trail_type", 0)),
            points=[TrackPoint.from_dict(item) for item in raw.get("points", [])],
        )


@dataclass(slots=True)
class RadarTrack:
    track_number: int = -1
    time_of_track: datetime | None = None
    received_at: datetime | None = None
    ssr: str = ""
    target_id: str = ""
    acid: str = ""
    spdx_kmh: float = 0.0
    spdy_kmh: float = 0.0
    speed_kmh: float = 0.0
    heading_deg: float = 0.0
    flight_level_m: float = 0.0
    qnh_height_m: float = 0.0
    selected_altitude_m: int = 0
    qnh_applied: bool = False
    altitude_status: str = "m"
    aircraft_type: str = ""
    wtc: str = ""
    adep: str = ""
    dep: str = ""
    adst: str = ""
    dst: str = ""
    runway: str = ""
    cfl_m: float = 0.0
    sector_index: int = 0
    sector_name: str = ""
    sid: str = ""
    star: str = ""
    flight_plan_correlated: int = 0
    latitude: float = 0.0
    longitude: float = 0.0
    cartesian_x_m: int | None = None
    cartesian_y_m: int | None = None
    flight_destination: FlightDestination = FlightDestination.OUTSIDE
    voice_texts: list[str] = field(default_factory=list)
    wav_file_paths: list[str] = field(default_factory=list)
    warning_text: str = ""
    speech_warning: bool = False
    trail: TrailRecord = field(default_factory=TrailRecord)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def primary_callsign(self) -> str:
        return (self.acid or self.target_id or "").strip()

    def refresh_altitude_status(self, previous_level_m: float | None) -> None:
        if previous_level_m is None:
            self.altitude_status = "m"
            return
        current = round(self.flight_level_m / 10)
        previous = round(previous_level_m / 10)
        if current > previous:
            self.altitude_status = "c"
        elif current < previous:
            self.altitude_status = "d"
        else:
            self.altitude_status = "m"

    def append_voice_text(self, text: str, wav_path: str | None = None) -> None:
        if text:
            self.voice_texts.append(text)
        if wav_path:
            self.wav_file_paths.append(wav_path)

    def to_dict(self) -> dict[str, Any]:
        return {
            "track_number": self.track_number,
            "time_of_track": format_datetime(self.time_of_track),
            "received_at": format_datetime(self.received_at),
            "ssr": self.ssr,
            "target_id": self.target_id,
            "acid": self.acid,
            "spdx_kmh": self.spdx_kmh,
            "spdy_kmh": self.spdy_kmh,
            "speed_kmh": self.speed_kmh,
            "heading_deg": self.heading_deg,
            "flight_level_m": self.flight_level_m,
            "qnh_height_m": self.qnh_height_m,
            "selected_altitude_m": self.selected_altitude_m,
            "qnh_applied": self.qnh_applied,
            "altitude_status": self.altitude_status,
            "aircraft_type": self.aircraft_type,
            "wtc": self.wtc,
            "adep": self.adep,
            "dep": self.dep,
            "adst": self.adst,
            "dst": self.dst,
            "runway": self.runway,
            "cfl_m": self.cfl_m,
            "sector_index": self.sector_index,
            "sector_name": self.sector_name,
            "sid": self.sid,
            "star": self.star,
            "flight_plan_correlated": self.flight_plan_correlated,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "cartesian_x_m": self.cartesian_x_m,
            "cartesian_y_m": self.cartesian_y_m,
            "flight_destination": self.flight_destination.value,
            "voice_texts": list(self.voice_texts),
            "wav_file_paths": list(self.wav_file_paths),
            "warning_text": self.warning_text,
            "speech_warning": self.speech_warning,
            "trail": self.trail.to_dict(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "RadarTrack":
        destination = raw.get("flight_destination", FlightDestination.OUTSIDE.value)
        try:
            flight_destination = FlightDestination(destination)
        except ValueError:
            flight_destination = FlightDestination.OUTSIDE
        return cls(
            track_number=int(raw.get("track_number", -1)),
            time_of_track=parse_datetime(raw.get("time_of_track")),
            received_at=parse_datetime(raw.get("received_at")),
            ssr=str(raw.get("ssr", "")),
            target_id=str(raw.get("target_id", "")),
            acid=str(raw.get("acid", "")),
            spdx_kmh=float(raw.get("spdx_kmh", 0.0)),
            spdy_kmh=float(raw.get("spdy_kmh", 0.0)),
            speed_kmh=float(raw.get("speed_kmh", 0.0)),
            heading_deg=float(raw.get("heading_deg", 0.0)),
            flight_level_m=float(raw.get("flight_level_m", 0.0)),
            qnh_height_m=float(raw.get("qnh_height_m", 0.0)),
            selected_altitude_m=int(raw.get("selected_altitude_m", 0)),
            qnh_applied=bool(raw.get("qnh_applied", False)),
            altitude_status=str(raw.get("altitude_status", "m")),
            aircraft_type=str(raw.get("aircraft_type", "")),
            wtc=str(raw.get("wtc", "")),
            adep=str(raw.get("adep", "")),
            dep=str(raw.get("dep", "")),
            adst=str(raw.get("adst", "")),
            dst=str(raw.get("dst", "")),
            runway=str(raw.get("runway", "")),
            cfl_m=float(raw.get("cfl_m", 0.0)),
            sector_index=int(raw.get("sector_index", 0)),
            sector_name=str(raw.get("sector_name", "")),
            sid=str(raw.get("sid", "")),
            star=str(raw.get("star", "")),
            flight_plan_correlated=int(raw.get("flight_plan_correlated", 0)),
            latitude=float(raw.get("latitude", 0.0)),
            longitude=float(raw.get("longitude", 0.0)),
            cartesian_x_m=raw.get("cartesian_x_m"),
            cartesian_y_m=raw.get("cartesian_y_m"),
            flight_destination=flight_destination,
            voice_texts=list(raw.get("voice_texts", [])),
            wav_file_paths=list(raw.get("wav_file_paths", [])),
            warning_text=str(raw.get("warning_text", "")),
            speech_warning=bool(raw.get("speech_warning", False)),
            trail=TrailRecord.from_dict(raw.get("trail", {})),
            metadata=dict(raw.get("metadata", {})),
        )


@dataclass(slots=True)
class FlightPlan:
    callsign: str = ""
    adep: str = ""
    adest: str = ""
    ssr: str = ""
    aircraft_type: str = ""
    flight_rules: str = ""
    route: str = ""
    transfer_fix: str = ""
    dof: date | None = None
    etd: datetime | None = None
    eet_minutes: int = 0
    atd: datetime | None = None
    eta: datetime | None = None
    ata: datetime | None = None
    source_message_type: str = ""
    last_message_time: datetime | None = None
    terminal_time_seconds: int = 0  # 在终端区内的累计飞行秒数
    terminal_enter_time: datetime | None = None  # 首次进入终端区时间（UTC）
    terminal_exit_time: datetime | None = None   # 最后一次离开终端区时间（UTC）
    procedure: str = ""   # 飞行程序代号（SID 或 STAR），从 CAT062 获取
    runway: str = ""      # 使用跑道，从 CAT062 获取

    @property
    def key(self) -> tuple[str, str, str, date | None]:
        return (self.callsign, self.adep, self.adest, self.dof)

    def apply_update(self, other: "FlightPlan", action: str) -> None:
        self.callsign = other.callsign or self.callsign
        self.adep = other.adep or self.adep
        self.adest = other.adest or self.adest
        self.ssr = other.ssr or self.ssr
        self.aircraft_type = other.aircraft_type or self.aircraft_type
        self.flight_rules = other.flight_rules or self.flight_rules
        self.route = other.route or self.route
        self.transfer_fix = other.transfer_fix or self.transfer_fix
        # DEP/ARR 的 dof 是由收报时间初步推算的，可能因昨日回退而不准确；
        # 此时以原计划（self）的 dof 为准，不允许 DEP/ARR 覆盖已有 dof。
        if action not in {"DEP", "ARR"}:
            self.dof = other.dof or self.dof
        self.eet_minutes = other.eet_minutes or self.eet_minutes
        self.source_message_type = action or self.source_message_type
        self.last_message_time = other.last_message_time or self.last_message_time
        # CAT062 字段：不覆盖已有非空值（SID/STAR/跑道一经确定不回退）
        self.procedure = self.procedure or other.procedure
        self.runway = self.runway or other.runway

        if action == "FPL":
            self.etd = other.etd or self.etd
            self.eta = other.eta or self.eta
        elif action == "DEP":
            self.atd = other.atd or self.atd
            if not self.etd and other.atd:
                self.etd = other.atd
            if self.eet_minutes and self.atd:
                self.eta = self.atd + timedelta(minutes=self.eet_minutes)
        elif action == "ARR":
            self.ata = other.ata or self.ata
        elif action == "DLA":
            self.etd = other.etd or self.etd
            if self.eet_minutes and self.etd:
                self.eta = self.etd + timedelta(minutes=self.eet_minutes)
        else:
            self.etd = other.etd or self.etd
            self.atd = other.atd or self.atd
            self.eta = other.eta or self.eta
            self.ata = other.ata or self.ata

    def to_dict(self) -> dict[str, Any]:
        return {
            "callsign": self.callsign,
            "adep": self.adep,
            "adest": self.adest,
            "ssr": self.ssr,
            "aircraft_type": self.aircraft_type,
            "flight_rules": self.flight_rules,
            "route": self.route,
            "transfer_fix": self.transfer_fix,
            "dof": format_date(self.dof),
            "etd": format_datetime(self.etd),
            "eet_minutes": self.eet_minutes,
            "atd": format_datetime(self.atd),
            "eta": format_datetime(self.eta),
            "ata": format_datetime(self.ata),
            "source_message_type": self.source_message_type,
            "last_message_time": format_datetime(self.last_message_time),
            "terminal_time_seconds": self.terminal_time_seconds,
            "terminal_enter_time": format_datetime(self.terminal_enter_time),
            "terminal_exit_time": format_datetime(self.terminal_exit_time),
            "procedure": self.procedure,
            "runway": self.runway,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FlightPlan":
        return cls(
            callsign=str(raw.get("callsign", "")),
            adep=str(raw.get("adep", "")),
            adest=str(raw.get("adest", "")),
            ssr=str(raw.get("ssr", "")),
            aircraft_type=str(raw.get("aircraft_type", "")),
            flight_rules=str(raw.get("flight_rules", "")),
            route=str(raw.get("route", "")),
            transfer_fix=str(raw.get("transfer_fix", "")),
            dof=parse_date(raw.get("dof")),
            etd=parse_datetime(raw.get("etd")),
            eet_minutes=int(raw.get("eet_minutes", 0)),
            atd=parse_datetime(raw.get("atd")),
            eta=parse_datetime(raw.get("eta")),
            ata=parse_datetime(raw.get("ata")),
            source_message_type=str(raw.get("source_message_type", "")),
            last_message_time=parse_datetime(raw.get("last_message_time")),
            terminal_time_seconds=int(raw.get("terminal_time_seconds", 0)),
            terminal_enter_time=parse_datetime(raw.get("terminal_enter_time")),
            terminal_exit_time=parse_datetime(raw.get("terminal_exit_time")),
            procedure=str(raw.get("procedure", "")),
            runway=str(raw.get("runway", "")),
        )


@dataclass(slots=True)
class AftnMessage:
    utc_time: datetime | None = None
    message_type: str = ""
    message_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "utc_time": format_datetime(self.utc_time),
            "message_type": self.message_type,
            "message_text": self.message_text,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "AftnMessage":
        return cls(
            utc_time=parse_datetime(raw.get("utc_time")),
            message_type=str(raw.get("message_type", "")),
            message_text=str(raw.get("message_text", "")),
        )


@dataclass(slots=True)
class VoiceRecord:
    received_at: datetime
    wav_begin_time: datetime | None = None
    processed_command: str = ""
    callsign: str = ""
    send_ip: str = ""
    frequency: str = ""
    sector: str = ""
    speaker: str = ""
    duration: float = 0.0
    wav_file_path: str = ""
    raw_payload: bytes = b""

    @property
    def event_time(self) -> datetime:
        return self.wav_begin_time or self.received_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "received_at": format_datetime(self.received_at),
            "wav_begin_time": format_datetime(self.wav_begin_time),
            "processed_command": self.processed_command,
            "callsign": self.callsign,
            "send_ip": self.send_ip,
            "frequency": self.frequency,
            "sector": self.sector,
            "speaker": self.speaker,
            "duration": self.duration,
            "wav_file_path": self.wav_file_path,
            "raw_payload": encode_bytes(self.raw_payload),
        }


    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "VoiceRecord":
        return cls(
            received_at=parse_datetime(raw.get("received_at")) or datetime.utcnow(),
            wav_begin_time=parse_datetime(raw.get("wav_begin_time")),
            processed_command=str(raw.get("processed_command", "")),
            callsign=str(raw.get("callsign", "")),
            send_ip=str(raw.get("send_ip", "")),
            frequency=str(raw.get("frequency", "")),
            sector=str(raw.get("sector", "")),
            speaker=str(raw.get("speaker", "")),
            duration=float(raw.get("duration", 0.0)),
            wav_file_path=str(raw.get("wav_file_path", "")),
            raw_payload=decode_bytes(raw.get("raw_payload", "")) if raw.get("raw_payload") else b"",
        )


@dataclass(slots=True)
class SectorSortie:
    callsign: str
    from_to: str
    enter_time: datetime
    track_number: int = -1

    def to_dict(self) -> dict[str, Any]:
        return {
            "callsign": self.callsign,
            "from_to": self.from_to,
            "enter_time": format_datetime(self.enter_time),
            "track_number": self.track_number,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SectorSortie":
        return cls(
            callsign=str(raw.get("callsign", "")),
            from_to=str(raw.get("from_to", "")),
            enter_time=parse_datetime(raw.get("enter_time")) or datetime.utcnow(),
            track_number=int(raw.get("track_number", -1)),
        )


@dataclass(slots=True)
class ChannelOccupied:
    date: date
    channels: dict[str, list[int]] = field(default_factory=dict)

    DEFAULT_SECTORS = ("HN", "HE", "ARW", "AS", "AD", "ARE", "ASL")
    BUCKETS_PER_DAY = 24 * 6

    @classmethod
    def create_empty(cls, day: date | None = None) -> "ChannelOccupied":
        current_day = day or datetime.utcnow().date()
        return cls(
            date=current_day,
            channels={sector: [0 for _ in range(cls.BUCKETS_PER_DAY)] for sector in cls.DEFAULT_SECTORS},
        )


    def ensure_sector(self, sector: str) -> list[int]:
        if sector not in self.channels:
            self.channels[sector] = [0 for _ in range(self.BUCKETS_PER_DAY)]
        return self.channels[sector]

    def add_duration(self, sector: str, timestamp: datetime, seconds: int) -> None:
        bucket = timestamp.hour * 6 + timestamp.minute // 10
        slots = self.ensure_sector(sector)
        if 0 <= bucket < self.BUCKETS_PER_DAY:
            slots[bucket] += max(0, seconds)

    def bucket_values(self, sector: str, hour: int) -> list[int]:
        slots = self.ensure_sector(sector)
        start = max(0, min(self.BUCKETS_PER_DAY, hour * 6))
        return slots[start : start + 6]

    def reset_for_day(self, new_day: date) -> None:
        self.date = new_day
        for sector in list(self.channels):
            self.channels[sector] = [0 for _ in range(self.BUCKETS_PER_DAY)]

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": format_date(self.date),
            "channels": {key: list(value) for key, value in self.channels.items()},
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ChannelOccupied":
        channel = cls(
            date=parse_date(raw.get("date")) or datetime.utcnow().date(),
            channels={key: list(map(int, value)) for key, value in raw.get("channels", {}).items()},
        )

        for sector in cls.DEFAULT_SECTORS:
            channel.ensure_sector(sector)
        return channel
