from __future__ import annotations

import csv
import json
import logging
import struct
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import AppConfig
from .models import AftnMessage, ChannelOccupied, FlightPlan, VoiceRecord
from .state import HourlySortieReport, ProtectorState, SORTIE_KEYS, SORTIE_SNAPSHOT_FILES, iter_sortie_snapshot_items
from .utils import atomic_write_text, datetime_to_oadate, ensure_directory, ensure_parent, format_date, format_datetime

_logger = logging.getLogger("atc_data_hub.storage")

FPLN_HEADERS = [
    "航班号",
    "应答机编码",
    "移交点",
    "ADEP",
    "执飞日",
    "ETD",
    "ATD",
    "ADST",
    "ETA",
    "ATA",
    "机型",
    "进区域时间",
    "出区域时间",
    "区域飞行时间",
    "飞行程序",
    "使用跑道",
    "航路",
]


class StorageManager:

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.records_root = config.paths.records_root
        self.runtime_root = config.paths.runtime_root
        self.snapshot_root = self.runtime_root / "Record" / "Temp"
        self.radar_root = self.records_root / "Radar"
        self.aftn_root = self.records_root / "AFTN"
        self.agsr_root = self.records_root / "AGSR"
        self.channel_root = self.records_root / "Channel"
        self.trail_root = self.records_root / "Trail"
        self.sortie_root = self.records_root / "Sortie"
        self.warning_root = self.records_root / "Warning"
        self._radar_buffers: dict[Path, bytearray] = {}
        self._radar_buffer_count = 0
        self._radar_flush_threshold = max(1, config.runtime.radar_flush_every_messages)

    def ensure_layout(self) -> None:
        for path in (
            self.records_root,
            self.runtime_root,
            self.snapshot_root,
            self.radar_root,
            self.aftn_root,
            self.agsr_root,
            self.channel_root,
            self.sortie_root,
            self.warning_root,
        ):
            ensure_directory(path)

    def append_radar_payload(self, received_at: datetime, payload: bytes) -> None:
        path = self.radar_file_path(received_at)
        header = struct.pack("<d", datetime_to_oadate(received_at))
        bucket = self._radar_buffers.setdefault(path, bytearray())
        bucket.extend(header)
        bucket.extend(payload)
        self._radar_buffer_count += 1
        if self._radar_buffer_count >= self._radar_flush_threshold:
            self.flush_radar_buffers()

    def flush_radar_buffers(self) -> None:
        for path, buffer in list(self._radar_buffers.items()):
            if not buffer:
                continue
            ensure_parent(path)
            with path.open("ab") as handle:
                handle.write(buffer)
            self._radar_buffers[path] = bytearray()
        self._radar_buffer_count = 0

    def radar_file_path(self, moment: datetime) -> Path:
        beijing = moment + timedelta(hours=8)  # UTC -> Beijing Time (CST)
        suffix = 0 if moment.minute < 30 else 1
        file_name = f"RD{beijing:%y%m%d%H}_{suffix}.rcd"
        return self.radar_root / file_name

    def persist_daily_outputs(
        self,
        state: ProtectorState,
        day: date,
        utc_day: date | None = None,
    ) -> None:
        # FPLN/sortie are keyed by flight-plan DOF (Beijing date); strict match only.
        flight_plans = state.daily_flight_plans(day)
        self._safe_write("write_fpln_csv", self.write_fpln_csv, day, flight_plans)
        self._safe_write(
            "write_sortie_reports",
            self.write_sortie_reports,
            day,
            state.daily_hourly_reports(day),
            state.monthly_hourly_reports(day.year, day.month),
        )
        # AFTN messages and voice records are archived by their own UTC date,
        # not by the flight-plan DOF, to avoid the UTC/Beijing cross-day mismatch.
        aftn_day = utc_day if utc_day is not None else day
        self._safe_write("write_aftn_messages", self.write_aftn_messages, aftn_day, state.daily_aftn_messages(aftn_day))
        self._safe_write("write_voice_records", self.write_voice_records, aftn_day, state.daily_voice_records(aftn_day))
        self._safe_write("write_channel_occupied", self.write_channel_occupied, state.channel_occupied)

    def _safe_write(self, label: str, fn, *args, **kwargs) -> None:  # type: ignore[type-arg]
        """Call *fn* with *args*; log a warning on failure instead of crashing."""
        try:
            fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("persist_daily_outputs: %s failed: %s", label, exc)

    def write_fpln_csv(self, day: date, flight_plans: list[FlightPlan]) -> Path:
        month_dir = ensure_directory(self.aftn_root / day.strftime("%Y%m"))
        path = month_dir / f"FPLN{day:%Y%m%d}.csv"
        rows: list[list[str]] = [list(FPLN_HEADERS)]
        plans = sorted(
            flight_plans,
            key=lambda item: (
                item.callsign,
                item.adep,
                item.etd or datetime.min,
                item.adest,
            ),
        )
        for item in plans:
            rows.append(
                [
                    item.callsign,
                    item.ssr,
                    item.transfer_fix,
                    item.adep,
                    self._format_fpln_day(item.dof),
                    self._format_fpln_time(item.etd),
                    self._format_fpln_time(item.atd),
                    item.adest,
                    self._format_fpln_time(item.eta),
                    self._format_fpln_time(item.ata),
                    item.aircraft_type,
                    self._format_fpln_time(item.terminal_enter_time),
                    self._format_fpln_time(item.terminal_exit_time),
                    self._format_terminal_time(item.terminal_time_seconds),
                    item.procedure,
                    item.runway,
                    item.route,
                ]
            )
        self._write_csv(path, rows)
        return path



    def write_aftn_messages(self, day: date, messages: list[AftnMessage]) -> Path:
        month_dir = ensure_directory(self.aftn_root / day.strftime("%Y%m"))
        path = month_dir / f"AFTNMsg{day:%Y%m%d}{self.config.compatibility.data_file_extension}"
        payload = [message.to_dict() for message in sorted(messages, key=lambda item: item.utc_time or datetime.min)]
        self._write_json(path, payload)
        return path

    def write_voice_records(self, day: date, records: list[VoiceRecord]) -> Path:
        path = self.agsr_root / f"SR{day:%y%m%d}{self.config.compatibility.data_file_extension}"
        payload = [record.to_dict() for record in sorted(records, key=lambda item: item.event_time)]
        self._write_json(path, payload)
        return path


    def write_channel_occupied(self, channel: ChannelOccupied) -> Path:
        path = self.channel_root / f"Channel{channel.date:%y%m%d}{self.config.compatibility.data_file_extension}"
        self._write_json(path, channel.to_dict())
        return path


    def write_sortie_reports(
        self,
        day: date,
        daily_reports: list[HourlySortieReport],
        monthly_reports: list[HourlySortieReport],
    ) -> tuple[Path, Path]:
        month_csv = self.sortie_root / f"SortieData{day:%y%m}.csv"
        day_txt = self.sortie_root / f"SortieData{day:%y%m%d}.txt"
        self._write_monthly_sortie_csv(month_csv, monthly_reports)
        self._write_daily_sortie_txt(day_txt, day, daily_reports)
        return month_csv, day_txt

    def save_snapshot(self, state: ProtectorState) -> None:
        ensure_directory(self.snapshot_root)
        payloads = state.snapshot_payloads()
        simple_files = {
            "tempFdr.data",
            "tempFPLN.data",
            "tempAFTNMsg.data",
            "tempChannelOccupied.data",
            "tempRuntime.data",
        }
        for file_name in simple_files:
            if file_name not in payloads:
                continue
            self._write_json(self.snapshot_root / file_name, payloads[file_name])
        for file_name, values in iter_sortie_snapshot_items(payloads):
            self._write_json(self.snapshot_root / file_name, values)

    def load_snapshot(self, state: ProtectorState) -> bool:
        if not self.snapshot_root.exists():
            return False
        payloads: dict[str, Any] = {}
        for file_name in (
            "tempFdr.data",
            "tempFPLN.data",
            "tempAFTNMsg.data",
            "tempChannelOccupied.data",
            "tempRuntime.data",
        ):
            path = self.snapshot_root / file_name
            data = self._read_json(path)
            if data is not None:
                payloads[file_name] = data
        payloads["sortie_files"] = {}
        for file_name in SORTIE_SNAPSHOT_FILES.values():
            path = self.snapshot_root / file_name
            data = self._read_json(path)
            if data is not None:
                payloads["sortie_files"][file_name] = data
        if not payloads:
            return False
        state.restore_from_snapshot_payloads(payloads)
        return True

    def _format_fpln_day(self, value: date | None) -> str:
        return f"{value:%d}" if value else ""

    def _format_fpln_time(self, value: datetime | None) -> str:
        return value.strftime("%d %H:%M") if value else ""

    def _format_terminal_time(self, seconds: int) -> str:
        """Format terminal flight time as HH:MM:SS; empty string when zero."""
        if not seconds:
            return ""
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _write_monthly_sortie_csv(self, path: Path, reports: list[HourlySortieReport]) -> None:
        rows: list[list[str]] = [
            [
                "HourStart",
                *SORTIE_KEYS,
                *[f"Open_{sector}" for sector in ChannelOccupied.DEFAULT_SECTORS],
                "Arrival",
                "Departure",
                "Inside",
                "Runways",
                "OverCapacity",
            ]
        ]


        for report in sorted(reports, key=lambda item: item.hour_start):
            runway_text = "; ".join(f"{key}:{value}" for key, value in sorted(report.runway_usage.items()))
            over_text = "; ".join(
                f"{key}:{value}" for key, value in sorted(report.over_capacity.items()) if value
            )
            rows.append(
                [
                    format_datetime(report.hour_start) or "",
                    *[str(report.sortie_counts.get(key, 0)) for key in SORTIE_KEYS],
                    *[str(report.sector_opened.get(sector, 0)) for sector in ChannelOccupied.DEFAULT_SECTORS],
                    str(report.airport_flow.get("arrival", 0)),
                    str(report.airport_flow.get("departure", 0)),
                    str(report.airport_flow.get("inside", 0)),
                    runway_text,
                    over_text,
                ]
            )

        self._write_csv(path, rows)

    def _write_daily_sortie_txt(self, path: Path, day: date, reports: list[HourlySortieReport]) -> None:
        lines = [f"ATC Data Hub Sortie Summary {day.isoformat()}", ""]
        if not reports:
            lines.append("No hourly sortie records.")
        for report in sorted(reports, key=lambda item: item.hour_start):
            lines.append(f"[{report.hour_start:%H}:00]")
            lines.append(
                "  Sortie: "
                + ", ".join(f"{key}={report.sortie_counts.get(key, 0)}" for key in SORTIE_KEYS)
            )
            lines.append(
                "  Opened: "
                + ", ".join(
                    f"{key}={report.sector_opened.get(key, 0)}" for key in ChannelOccupied.DEFAULT_SECTORS
                )
            )

            if report.runway_usage:
                lines.append(
                    "  Runway: "
                    + ", ".join(f"{key}={value}" for key, value in sorted(report.runway_usage.items()))
                )
            lines.append(
                "  AirportFlow: "
                f"arrival={report.airport_flow.get('arrival', 0)}, "
                f"departure={report.airport_flow.get('departure', 0)}, "
                f"inside={report.airport_flow.get('inside', 0)}"
            )
            if any(report.over_capacity.values()):
                lines.append(
                    "  OverCapacity: "
                    + ", ".join(f"{key}={value}" for key, value in sorted(report.over_capacity.items()) if value)
                )
            lines.append("")
        atomic_write_text(path, "\n".join(lines).rstrip() + "\n")

    def _write_csv(self, path: Path, rows: list[list[str]]) -> None:
        ensure_parent(path)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerows(rows)

    def _write_json(self, path: Path, payload: Any) -> None:
        atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))

    def _read_json(self, path: Path) -> Any | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
