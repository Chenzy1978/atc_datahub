from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from .config import TrackRegion
from .geometry import TerminalArea
from .models import (
    AftnMessage,
    ChannelOccupied,
    FlightDestination,
    FlightPlan,
    RadarTrack,
    SectorSortie,
    TrackPoint,
    TrailRecord,
    VoiceRecord,
)
from .reference import ReferenceData
from .utils import format_date, format_datetime, parse_date, parse_datetime, percentage_over_capacity

SORTIE_KEYS = ("HE", "HN", "AS", "ARW", "ARE", "ASL", "AD", "TM")
TERMINAL_KEY = "TM"
SORTIE_SNAPSHOT_FILES = {
    "HE": "tempSector1_CSs.data",
    "HN": "tempSector2_CSs.data",
    "AS": "tempSector3_CSs.data",
    "ARW": "tempSector4_CSs.data",
    "ARE": "tempSector5_CSs.data",
    "ASL": "tempSector6_CSs.data",
    "AD": "tempSector7_CSs.data",
    TERMINAL_KEY: "Terminal_CSs.data",
}
CHANNEL_SECTOR_ALIASES = {
    "HN": "HN",
    "HE": "HE",
    "ARW": "ARW",
    "AS": "AS",
    "AD": "AD",
    "ARE": "ARE",
    "ASL": "ASL",
}



@dataclass(slots=True)
class HourlySortieReport:
    hour_start: datetime
    sortie_counts: dict[str, int]
    sector_opened: dict[str, int]
    runway_usage: dict[str, int]
    airport_flow: dict[str, int]
    over_capacity: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "hour_start": format_datetime(self.hour_start),
            "sortie_counts": dict(self.sortie_counts),
            "sector_opened": dict(self.sector_opened),
            "runway_usage": dict(self.runway_usage),
            "airport_flow": dict(self.airport_flow),
            "over_capacity": dict(self.over_capacity),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "HourlySortieReport":
        return cls(
            hour_start=parse_datetime(raw.get("hour_start")) or datetime.utcnow(),
            sortie_counts={key: int(value) for key, value in raw.get("sortie_counts", {}).items()},
            sector_opened={key: int(value) for key, value in raw.get("sector_opened", {}).items()},
            runway_usage={key: int(value) for key, value in raw.get("runway_usage", {}).items()},
            airport_flow={key: int(value) for key, value in raw.get("airport_flow", {}).items()},
            over_capacity={key: str(value) for key, value in raw.get("over_capacity", {}).items()},
        )


class ProtectorState:
    def __init__(
        self,
        reference_data: ReferenceData | None = None,
        track_region: TrackRegion | None = None,
        terminal_area: TerminalArea | None = None,
    ) -> None:
        now = datetime.utcnow()
        self.reference_data = reference_data or ReferenceData()
        self.track_region = track_region
        self.terminal_area = terminal_area
        self.current_day = now.date()
        self.current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        self.tracks: dict[int, RadarTrack] = {}
        self.flight_plans: dict[tuple[str, str, str, date | None], FlightPlan] = {}
        self.aftn_messages: list[AftnMessage] = []
        self.voice_records: list[VoiceRecord] = []
        self.trails: list[TrailRecord] = []
        self.sector_sorties: dict[str, list[SectorSortie]] = {key: [] for key in SORTIE_KEYS}
        self.channel_occupied = ChannelOccupied.create_empty(self.current_day)
        self.hourly_reports: list[HourlySortieReport] = []
        self._runway_seen_in_hour: dict[str, set[str]] = {}
        self._airport_flow_seen_in_hour: dict[str, set[str]] = {
            "arrival": set(),
            "departure": set(),
            "inside": set(),
        }
        # track_number -> datetime when the track first entered the terminal area
        # in the current "inside" segment; None means currently outside.
        self._track_terminal_state: dict[int, datetime | None] = {}
        # Set of FlightPlan keys whose terminal data is already finalised (flush done).
        # Once a key is in this set, _add_terminal_seconds will not accumulate further.
        # This prevents snapshot-restore followed by re-ingestion of the same track
        # from double-counting terminal time.
        self._terminal_locked: set[tuple] = set()
        # DOF dates that have been updated since last persistence
        self.dirty_dofs: set[date] = set()

    def ingest_radar_track(self, track: RadarTrack) -> RadarTrack:
        timestamp = track.time_of_track or track.received_at or datetime.utcnow()
        previous = self.tracks.get(track.track_number) if track.track_number >= 0 else None
        merged = self._merge_track(previous, track)
        merged.received_at = track.received_at or merged.received_at or timestamp
        merged.time_of_track = track.time_of_track or merged.time_of_track or merged.received_at or timestamp
        merged.sector_name = merged.sector_name or self.reference_data.sector_name(merged.sector_index)
        if merged.adep or merged.adst:
            merged.flight_destination = self.reference_data.classify_flight(merged.adep, merged.adst)
        self._apply_matching_flight_plan(merged)
        merged.refresh_altitude_status(previous.flight_level_m if previous else None)
        self._append_trail_point(merged)
        self._register_sorties(merged)
        self._record_hourly_activity(merged)
        self._just_exited_terminal = self._update_terminal_time(merged, previous)
        if merged.track_number >= 0:
            self.tracks[merged.track_number] = merged
        return merged

    def record_aftn_message(self, message: AftnMessage) -> None:
        self.aftn_messages.append(message)

    def upsert_flight_plan(self, plan: FlightPlan, action: str) -> FlightPlan:
        existing = self._find_existing_flight_plan(plan, action)
        if existing is None:
            plan.source_message_type = action or plan.source_message_type
            self.flight_plans[plan.key] = plan
            target = plan
        else:
            old_key = existing.key
            existing.apply_update(plan, action)
            if existing.key != old_key:
                self.flight_plans.pop(old_key, None)
                self.flight_plans[existing.key] = existing
            target = existing
        # Mark the DOF as dirty for later persistence
        if target.dof:
            self.dirty_dofs.add(target.dof)
        self._refresh_tracks_by_flight_plan(target)
        return target

    def add_voice_record(self, record: VoiceRecord) -> None:
        self.voice_records.append(record)
        sector = self.normalize_channel_sector(record.sector)
        if sector:
            self.channel_occupied.add_duration(sector, record.event_time, max(0, int(round(record.duration))))
        if record.callsign:
            track = self.find_track_by_callsign(record.callsign)
            if track is not None:
                track.append_voice_text(record.processed_command, record.wav_file_path)


    def find_track_by_callsign(self, callsign: str) -> RadarTrack | None:
        normalized = callsign.strip().upper()
        if not normalized:
            return None
        for track in self.tracks.values():
            if track.primary_callsign.strip().upper() == normalized:
                return track
        return None

    def cleanup_stale_tracks(self, now: datetime, stale_seconds: int) -> list[TrailRecord]:
        cutoff = now - timedelta(seconds=max(1, stale_seconds))
        archived: list[TrailRecord] = []
        for track_number, track in list(self.tracks.items()):
            last_seen = track.received_at or track.time_of_track or now
            if last_seen >= cutoff:
                continue
            trail = self._archive_track_trail(track, now)
            if trail is not None:
                archived.append(trail)
            self.tracks.pop(track_number, None)
        return archived

    def cleanup_old_sorties(self, now: datetime, hours: int = 2) -> None:
        cutoff = now - timedelta(hours=hours)
        for key in self.sector_sorties:
            self.sector_sorties[key] = [item for item in self.sector_sorties[key] if item.enter_time >= cutoff]

    def cleanup_old_flight_plans(self, now: datetime, days: int = 2) -> None:
        """删除执飞日早于 today-days 的飞行计划，避免列表无限增长。"""
        cutoff = now.date() - timedelta(days=max(1, days))
        for key, plan in list(self.flight_plans.items()):
            if plan.dof and plan.dof < cutoff:
                self.flight_plans.pop(key, None)

    def prune_flight_plans_after_daily_save(self, now: datetime, utc_now: datetime | None = None) -> None:
        utc_now = utc_now or now
        for key, plan in list(self.flight_plans.items()):
            if self._should_prune_flight_plan_after_daily_save(plan, now, utc_now):
                self.flight_plans.pop(key, None)

    def _should_prune_flight_plan_after_daily_save(
        self,
        plan: FlightPlan,
        now: datetime,
        utc_now: datetime,
    ) -> bool:
        # DOF+1 天之后仍然保留（已保存到 CSV），只清理更早的
        if plan.dof and now.date() > plan.dof + timedelta(days=2):
            return True
        # 已到达（ATA）且到达时间超过 24 小时，从内存清理，CSV 中已存在
        if plan.ata is not None and utc_now - plan.ata > timedelta(hours=24):
            return True
        # 预计到达时间已过 12 小时且未实际到达，计划可能未执行
        if plan.eta is not None and utc_now - plan.eta > timedelta(hours=12):
            if plan.ata is None:
                return True
        return False




    def finalize_hour(self, hour_start: datetime) -> HourlySortieReport:
        hour_end = hour_start + timedelta(hours=1)
        sortie_counts = {
            key: sum(1 for item in self.sector_sorties[key] if hour_start <= item.enter_time < hour_end)
            for key in SORTIE_KEYS
        }
        sector_opened = {
            sector: int(any(value >= 20 for value in self.channel_occupied.bucket_values(sector, hour_start.hour)))
            for sector in ChannelOccupied.DEFAULT_SECTORS
        }
        runway_usage = {key: len(value) for key, value in sorted(self._runway_seen_in_hour.items()) if value}
        airport_flow = {
            "arrival": len(self._airport_flow_seen_in_hour["arrival"]),
            "departure": len(self._airport_flow_seen_in_hour["departure"]),
            "inside": len(self._airport_flow_seen_in_hour["inside"]),
        }
        over_capacity = {
            key: percentage_over_capacity(sortie_counts[key], self._capacity_for_sector_hour(key, hour_start.hour))
            for key in SORTIE_KEYS
            if key != TERMINAL_KEY
        }
        report = HourlySortieReport(
            hour_start=hour_start,
            sortie_counts=sortie_counts,
            sector_opened=sector_opened,
            runway_usage=runway_usage,
            airport_flow=airport_flow,
            over_capacity=over_capacity,
        )
        self.hourly_reports.append(report)
        self._runway_seen_in_hour.clear()
        for bucket in self._airport_flow_seen_in_hour.values():
            bucket.clear()
        self.current_hour_start = hour_end.replace(minute=0, second=0, microsecond=0)
        self.cleanup_old_sorties(hour_end)
        return report

    def rollover_day(self, new_day: date) -> None:
        if new_day <= self.current_day:
            return
        self.current_day = new_day
        self.channel_occupied.reset_for_day(new_day)
        self.aftn_messages = [item for item in self.aftn_messages if (item.utc_time or datetime.min).date() >= new_day]
        self.voice_records = [item for item in self.voice_records if item.event_time.date() >= new_day]
        self.trails.clear()
        self.hourly_reports = [item for item in self.hourly_reports if item.hour_start.date() >= new_day - timedelta(days=2)]


    def daily_flight_plans(self, day: date) -> list[FlightPlan]:
        """返回执飞日为 day 的所有飞行计划（严格按 dof 匹配）。"""
        return [plan for plan in self.flight_plans.values() if plan.dof == day]

    def daily_aftn_messages(self, day: date) -> list[AftnMessage]:
        return [item for item in self.aftn_messages if (item.utc_time or datetime.min).date() == day]

    def daily_voice_records(self, day: date) -> list[VoiceRecord]:
        return [item for item in self.voice_records if item.event_time.date() == day]


    def daily_trails(self, day: date) -> list[TrailRecord]:
        return [item for item in self.trails if (item.trail_time or datetime.min).date() == day]

    def daily_hourly_reports(self, day: date) -> list[HourlySortieReport]:
        return sorted((item for item in self.hourly_reports if item.hour_start.date() == day), key=lambda item: item.hour_start)

    def monthly_hourly_reports(self, year: int, month: int) -> list[HourlySortieReport]:
        return sorted(
            (item for item in self.hourly_reports if item.hour_start.year == year and item.hour_start.month == month),
            key=lambda item: item.hour_start,
        )

    def snapshot_payloads(self) -> dict[str, Any]:
        sortie_payload = {
            key: [item.to_dict() for item in self.sector_sorties.get(key, [])]
            for key in SORTIE_KEYS
        }
        return {
            "tempFdr.data": [item.to_dict() for item in self.tracks.values()],
            "tempFPLN.data": [item.to_dict() for item in self.flight_plans.values()],
            "tempAFTNMsg.data": [item.to_dict() for item in self.aftn_messages],
            "tempChannelOccupied.data": self.channel_occupied.to_dict(),
            "tempRuntime.data": {
                "current_day": format_date(self.current_day),
                "current_hour_start": format_datetime(self.current_hour_start),
                "hourly_reports": [item.to_dict() for item in self.hourly_reports],
                "runway_seen_in_hour": {key: sorted(value) for key, value in self._runway_seen_in_hour.items()},
                "airport_flow_seen_in_hour": {
                    key: sorted(value) for key, value in self._airport_flow_seen_in_hour.items()
                },
            },
            "sortie_files": {
                SORTIE_SNAPSHOT_FILES[key]: sortie_payload.get(key, [])
                for key in SORTIE_KEYS
            },
        }

    def restore_from_snapshot_payloads(self, payloads: dict[str, Any]) -> None:
        self.tracks = {}
        for raw in payloads.get("tempFdr.data", []):
            track = RadarTrack.from_dict(raw)
            if track.track_number >= 0:
                self.tracks[track.track_number] = track

        self.flight_plans = {}
        for raw in payloads.get("tempFPLN.data", []):
            plan = FlightPlan.from_dict(raw)
            self.flight_plans[plan.key] = plan

        # Lock any plans that already have a terminal_exit_time — they were fully
        # finalised before the snapshot was taken and must not accumulate further.
        self._terminal_locked = {
            key for key, plan in self.flight_plans.items()
            if plan.terminal_exit_time is not None
        }

        self.aftn_messages = [AftnMessage.from_dict(raw) for raw in payloads.get("tempAFTNMsg.data", [])]
        self.voice_records = [VoiceRecord.from_dict(raw) for raw in payloads.get("voice_records", [])]
        self.trails = []

        channel_payload = payloads.get("tempChannelOccupied.data")
        if channel_payload:
            self.channel_occupied = ChannelOccupied.from_dict(channel_payload)
        else:
            self.channel_occupied = ChannelOccupied.create_empty(self.current_day)

        self.sector_sorties = {key: [] for key in SORTIE_KEYS}
        sortie_payloads = payloads.get("sortie_files", {})
        for key, file_name in SORTIE_SNAPSHOT_FILES.items():
            values = sortie_payloads.get(file_name, payloads.get(file_name, []))
            self.sector_sorties[key] = [SectorSortie.from_dict(raw) for raw in values]

        runtime_payload = payloads.get("tempRuntime.data", {})
        self.current_day = parse_date(runtime_payload.get("current_day")) or self.current_day
        self.current_hour_start = parse_datetime(runtime_payload.get("current_hour_start")) or self.current_hour_start
        self.hourly_reports = [HourlySortieReport.from_dict(raw) for raw in runtime_payload.get("hourly_reports", [])]
        self._runway_seen_in_hour = {
            key: set(map(str, values)) for key, values in runtime_payload.get("runway_seen_in_hour", {}).items()
        }
        airport_payload = runtime_payload.get("airport_flow_seen_in_hour", {})
        self._airport_flow_seen_in_hour = {
            "arrival": set(map(str, airport_payload.get("arrival", []))),
            "departure": set(map(str, airport_payload.get("departure", []))),
            "inside": set(map(str, airport_payload.get("inside", []))),
        }
        for key in SORTIE_KEYS:
            self.sector_sorties.setdefault(key, [])
        for sector in ChannelOccupied.DEFAULT_SECTORS:
            self.channel_occupied.ensure_sector(sector)

    def normalize_channel_sector(self, sector: str) -> str:
        return CHANNEL_SECTOR_ALIASES.get(sector.strip().upper(), "")

    def _merge_track(self, previous: RadarTrack | None, current: RadarTrack) -> RadarTrack:
        if previous is None:
            return current

        def fill_string(name: str) -> None:
            if not getattr(current, name):
                setattr(current, name, getattr(previous, name))

        def fill_number(name: str) -> None:
            if not getattr(current, name) and getattr(previous, name):
                setattr(current, name, getattr(previous, name))

        for name in (
            "ssr",
            "target_id",
            "acid",
            "aircraft_type",
            "wtc",
            "adep",
            "dep",
            "adst",
            "dst",
            "runway",
            "sector_name",
            "sid",
            "star",
            "warning_text",
        ):
            fill_string(name)

        for name in (
            "spdx_kmh",
            "spdy_kmh",
            "speed_kmh",
            "heading_deg",
            "flight_level_m",
            "qnh_height_m",
            "selected_altitude_m",
            "cfl_m",
            "sector_index",
            "flight_plan_correlated",
            "latitude",
            "longitude",
        ):
            fill_number(name)

        if current.cartesian_x_m is None and previous.cartesian_x_m is not None:
            current.cartesian_x_m = previous.cartesian_x_m
        if current.cartesian_y_m is None and previous.cartesian_y_m is not None:
            current.cartesian_y_m = previous.cartesian_y_m
        if current.received_at is None:
            current.received_at = previous.received_at
        if current.time_of_track is None:
            current.time_of_track = previous.time_of_track
        if current.flight_destination == FlightDestination.OUTSIDE and previous.flight_destination != FlightDestination.OUTSIDE:
            current.flight_destination = previous.flight_destination
        current.voice_texts = list(previous.voice_texts) + list(current.voice_texts)
        current.wav_file_paths = list(previous.wav_file_paths) + list(current.wav_file_paths)
        current.speech_warning = current.speech_warning or previous.speech_warning
        current.trail = self._copy_trail(previous.trail)
        merged_metadata = dict(previous.metadata)
        merged_metadata.update(current.metadata)
        current.metadata = merged_metadata
        return current

    def _copy_trail(self, trail: TrailRecord) -> TrailRecord:
        return TrailRecord(
            callsign=trail.callsign,
            trail_time=trail.trail_time,
            trail_type=trail.trail_type,
            points=[TrackPoint.from_dict(point.to_dict()) for point in trail.points],
        )

    def _append_trail_point(self, track: RadarTrack) -> None:
        if abs(track.latitude) < 0.0001 and abs(track.longitude) < 0.0001:
            return
        trail = track.trail or TrailRecord()
        if not trail.callsign:
            trail.callsign = track.primary_callsign
        trail.trail_time = track.time_of_track or track.received_at or datetime.utcnow()
        trail.trail_type = 0
        point = TrackPoint(
            timestamp=trail.trail_time or datetime.utcnow(),
            lat=track.latitude,
            lon=track.longitude,
            altitude_m=track.flight_level_m or track.qnh_height_m,
        )
        last = trail.points[-1] if trail.points else None
        if last is None or (abs(last.lat - point.lat) > 0.0001 or abs(last.lon - point.lon) > 0.0001):
            trail.points.append(point)
        if len(trail.points) > 360:
            trail.points = trail.points[-360:]
        track.trail = trail

    def _register_sorties(self, track: RadarTrack) -> None:
        callsign = track.primary_callsign.strip().upper()
        if not callsign:
            return
        relation = self._build_from_to(track)
        if not relation:
            return
        enter_time = track.time_of_track or track.received_at or datetime.utcnow()
        sector_name = track.sector_name.strip().upper()
        if sector_name in self.sector_sorties:
            self._append_sortie(sector_name, callsign, relation, enter_time, track.track_number)
        if self._is_terminal_track(track):
            self._append_sortie(TERMINAL_KEY, callsign, relation, enter_time, track.track_number)

    def _append_sortie(
        self,
        key: str,
        callsign: str,
        relation: str,
        enter_time: datetime,
        track_number: int,
    ) -> None:
        sorties = self.sector_sorties.setdefault(key, [])
        for item in reversed(sorties):
            if item.callsign == callsign and item.from_to == relation and abs((enter_time - item.enter_time).total_seconds()) <= 7200:
                return
        sorties.append(
            SectorSortie(callsign=callsign, from_to=relation, enter_time=enter_time, track_number=track_number)
        )

    def _is_terminal_track(self, track: RadarTrack) -> bool:
        if self.track_region is None:
            return False
        if abs(track.latitude) < 0.0001 and abs(track.longitude) < 0.0001:
            return False
        if not self.track_region.contains(track.latitude, track.longitude):
            return False
        return track.flight_destination in {
            FlightDestination.INBOUND,
            FlightDestination.OUTBOUND,
            FlightDestination.INSIDE,
        }

    def _build_from_to(self, track: RadarTrack) -> str:
        if track.adep and track.adst:
            return f"{track.adep}-{track.adst}"
        if track.adep:
            return f"{track.adep}-"
        if track.adst:
            return f"-{track.adst}"
        return ""

    def _record_hourly_activity(self, track: RadarTrack) -> None:
        identifier = track.primary_callsign.strip().upper() or f"TN{track.track_number}"
        if track.runway:
            self._runway_seen_in_hour.setdefault(track.runway, set()).add(identifier)
        if track.flight_destination == FlightDestination.INBOUND:
            self._airport_flow_seen_in_hour["arrival"].add(identifier)
        elif track.flight_destination == FlightDestination.OUTBOUND:
            self._airport_flow_seen_in_hour["departure"].add(identifier)
        elif track.flight_destination == FlightDestination.INSIDE:
            self._airport_flow_seen_in_hour["inside"].add(identifier)

    def _find_existing_flight_plan(self, plan: FlightPlan, action: str) -> FlightPlan | None:
        # 1. 精确命中（callsign + adep + adest + dof）
        target = self.flight_plans.get(plan.key)
        if target is not None:
            return target

        if plan.dof is None:
            return None

        # 2. 昨日延误/跨日回退：
        #    DEP: 今日收到起飞报，但昨日有同航班计划且尚无 ATD → 关联到昨日计划
        #    ARR: 今日收到落地报，但前两日有同航班计划且尚无 ATA → 关联到对应计划
        #         （ARR 报文可能延迟 1~2 天才到达，需要扩大回退范围）
        #    dof 保持原计划不变（不修改 plan.dof）
        if action in {"DEP", "ARR"}:
            if action == "ARR":
                # ARR：先查 dof-1，再查 dof-2
                for lookback in (1, 2):
                    lookup_key = (plan.callsign, plan.adep, plan.adest, plan.dof - timedelta(days=lookback))
                    candidate = self.flight_plans.get(lookup_key)
                    if candidate is not None and candidate.ata is None:
                        return candidate
            else:
                # DEP：只查 dof-1
                yesterday_key = (plan.callsign, plan.adep, plan.adest, plan.dof - timedelta(days=1))
                fallback = self.flight_plans.get(yesterday_key)
                if fallback is not None and fallback.atd is None:
                    return fallback

        return None


    def _refresh_tracks_by_flight_plan(self, plan: FlightPlan) -> None:
        callsign = plan.callsign.strip().upper()
        if not callsign:
            return
        for track in self.tracks.values():
            if track.primary_callsign.strip().upper() != callsign:
                continue
            self._enrich_track_from_plan(track, plan)

    def _apply_matching_flight_plan(self, track: RadarTrack) -> None:
        callsign = track.primary_callsign.strip().upper()
        if not callsign:
            return
        candidates = [plan for plan in self.flight_plans.values() if plan.callsign.strip().upper() == callsign]
        if not candidates and track.ssr:
            candidates = [plan for plan in self.flight_plans.values() if plan.ssr == track.ssr]
        if not candidates:
            return
        candidates.sort(key=lambda item: item.last_message_time or datetime.min, reverse=True)
        self._enrich_track_from_plan(track, candidates[0])

    def _enrich_track_from_plan(self, track: RadarTrack, plan: FlightPlan) -> None:
        track.adep = track.adep or plan.adep
        track.adst = track.adst or plan.adest
        track.aircraft_type = track.aircraft_type or plan.aircraft_type
        track.ssr = track.ssr or plan.ssr
        if track.adep or track.adst:
            track.flight_destination = self.reference_data.classify_flight(track.adep, track.adst)
        route = plan.route.strip()
        if route:
            track.metadata.setdefault("route", route)
        if plan.transfer_fix:
            track.metadata.setdefault("transfer_fix", plan.transfer_fix)
        if plan.etd:
            track.metadata.setdefault("etd", format_datetime(plan.etd))
        if plan.atd:
            track.metadata.setdefault("atd", format_datetime(plan.atd))
        if plan.eta:
            track.metadata.setdefault("eta", format_datetime(plan.eta))
        if plan.ata:
            track.metadata.setdefault("ata", format_datetime(plan.ata))

        # 将 CAT062 中的 SID/STAR/跑道回写到飞行计划
        # 规则：track 有非空值时才更新，不用空值覆盖已有记录
        if track.runway:
            plan.runway = track.runway
        if track.sid:
            plan.procedure = track.sid
        elif track.star:
            plan.procedure = track.star

    def _archive_track_trail(self, track: RadarTrack, now: datetime) -> TrailRecord | None:
        trail = track.trail
        if trail is None or len(trail.points) < 2:
            return None
        archived = self._copy_trail(trail)
        archived.callsign = archived.callsign or track.primary_callsign
        archived.trail_time = archived.trail_time or track.time_of_track or track.received_at or now
        self.trails.append(archived)
        return archived

    # ------------------------------------------------------------------
    # Terminal area flight-time accounting
    # ------------------------------------------------------------------

    def _update_terminal_time(self, track: RadarTrack, previous: RadarTrack | None) -> bool:
        """Accumulate time spent inside the terminal area for the given track.

        Rules:
        - Overfly tracks (OUTSIDE / OVERFLY flight_destination): skip entirely.
        - Both adep and adst are terminal airports (inside->inside): skip.
        - Altitude > ceiling: treat as outside.
        - On each radar update, if currently inside, accumulate the delta since
          the last "enter" timestamp.

        Returns:
            True if the track just exited the terminal area in this update
            (i.e. was inside on the previous report and is now outside), so
            that callers can decide whether to flush the flight plan to disk.
        """
        if self.terminal_area is None:
            return False
        if track.track_number < 0:
            return False

        # Skip overfly and inside->inside
        dest = track.flight_destination
        if dest in {FlightDestination.OUTSIDE, FlightDestination.OVERFLY}:
            self._track_terminal_state.pop(track.track_number, None)
            return False
        adep = (track.adep or "").strip().upper()
        adst = (track.adst or "").strip().upper()
        if adep and adst and self.terminal_area.both_inside(adep, adst):
            self._track_terminal_state.pop(track.track_number, None)
            return False

        alt = track.flight_level_m or track.qnh_height_m
        lat, lon = track.latitude, track.longitude
        now_in = (
            alt > 0
            and (abs(lat) > 0.0001 or abs(lon) > 0.0001)
            and self.terminal_area.inside(lat, lon, alt)
        )

        current_time = track.time_of_track or track.received_at or datetime.utcnow()
        prev_enter = self._track_terminal_state.get(track.track_number)

        if now_in:
            if prev_enter is None:
                # Just entered
                self._track_terminal_state[track.track_number] = current_time
                # 记录首次进区域时间到飞行计划（不覆盖已有值）
                plan = self._find_flight_plan_for_track(track)
                if plan is not None and plan.terminal_enter_time is None:
                    plan.terminal_enter_time = current_time
            # else: already inside, accumulate on next message or when leaving
            return False
        else:
            if prev_enter is not None:
                # Just exited — accumulate the segment
                delta = max(0.0, (current_time - prev_enter).total_seconds())
                self._add_terminal_seconds(track, int(delta))
                self._track_terminal_state[track.track_number] = None
                # 记录最后一次出区域时间到飞行计划
                plan = self._find_flight_plan_for_track(track)
                if plan is not None:
                    plan.terminal_exit_time = current_time
                return True  # signal: just exited terminal area
            return False

    def _add_terminal_seconds(self, track: RadarTrack, seconds: int) -> None:
        """Find the best-matching flight plan and add *seconds* to its terminal_time_seconds.

        Accumulation is skipped for plans that have already been finalised (locked).
        This prevents double-counting after a snapshot restore.
        """
        if seconds <= 0:
            return
        plan = self._find_flight_plan_for_track(track)
        if plan is not None and plan.key not in self._terminal_locked:
            plan.terminal_time_seconds += seconds

    def _find_flight_plan_for_track(self, track: RadarTrack) -> FlightPlan | None:
        """Find the flight plan most closely matching this track.

        Matching priority:
        1. callsign + adep + adst + dof (exact)
        2. callsign + adep + adst (ignore dof, pick most recent)
        3. callsign only (pick most recent, last resort)
        """
        callsign = track.primary_callsign.strip().upper()
        if not callsign:
            return None

        adep = (track.adep or "").strip().upper()
        adst = (track.adst or "").strip().upper()
        track_time = track.time_of_track or track.received_at
        # plan.dof is Beijing date; compare against Beijing date of the track time
        # to avoid cross-midnight mismatch (e.g. 23:59 UTC = next day Beijing).
        _BEIJING = timezone(timedelta(hours=8))
        if track_time:
            aware = track_time if track_time.tzinfo else track_time.replace(tzinfo=timezone.utc)
            track_date: date | None = aware.astimezone(_BEIJING).date()
        else:
            track_date = None

        candidates: list[FlightPlan] = []
        for plan in self.flight_plans.values():
            if plan.callsign.strip().upper() != callsign:
                continue
            candidates.append(plan)

        if not candidates:
            return None

        # Score each candidate: higher is better
        def _score(plan: FlightPlan) -> tuple[int, datetime]:
            score = 0
            plan_adep = (plan.adep or "").strip().upper()
            plan_adst = (plan.adest or "").strip().upper()
            if adep and plan_adep == adep:
                score += 2
            if adst and plan_adst == adst:
                score += 2
            if track_date and plan.dof == track_date:
                score += 1
            return (score, plan.last_message_time or datetime.min)

        candidates.sort(key=_score, reverse=True)
        return candidates[0]

    def flush_terminal_time_for_track(self, track: RadarTrack) -> None:
        """Force-close any open terminal segment for this track and accumulate it.

        Call this just before writing the flight plan to disk (e.g. on landing
        or when climb above 4800 m for departures).
        """
        if track.track_number < 0:
            return
        prev_enter = self._track_terminal_state.get(track.track_number)
        if prev_enter is None:
            return
        current_time = track.time_of_track or track.received_at or datetime.utcnow()
        delta = max(0.0, (current_time - prev_enter).total_seconds())
        if delta > 0:
            self._add_terminal_seconds(track, int(delta))
        # 记录强制关闭时的出区域时间
        plan = self._find_flight_plan_for_track(track)
        if plan is not None:
            plan.terminal_exit_time = current_time
            # Lock this plan so snapshot-restore cannot cause further accumulation
            self._terminal_locked.add(plan.key)
        self._track_terminal_state[track.track_number] = None

    def _capacity_for_sector_hour(self, sector_key: str, hour: int) -> int:
        if sector_key == TERMINAL_KEY:
            return 0
        if sector_key not in self.reference_data.sector_info:
            return 0
        index = self.reference_data.sector_info.index(sector_key)
        row = self.reference_data.capacity_row(index)
        if 0 <= hour < len(row):
            return row[hour]
        return 0


def iter_sortie_snapshot_items(payloads: dict[str, Any]) -> Iterable[tuple[str, list[dict[str, Any]]]]:
    sortie_files = payloads.get("sortie_files", {})
    for key in SORTIE_KEYS:
        yield SORTIE_SNAPSHOT_FILES[key], sortie_files.get(SORTIE_SNAPSHOT_FILES[key], [])
