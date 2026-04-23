from __future__ import annotations

import json
import logging
import select
import signal
import socket
import struct
from datetime import date, datetime, timezone

from pathlib import Path
from types import FrameType

from .config import AppConfig
from .geometry import TerminalArea
from .models import VoiceRecord
from .parsers import AftnParser, Cat062Parser
from .reference import load_reference_data
from .state import ProtectorState
from .storage import StorageManager
from .utils import ensure_directory, parse_datetime

LOGGER_NAME = "atc_data_hub"

# Departure: commit terminal time to disk once track climbs above this altitude
DEPARTURE_COMMIT_ALT_M = 4800.0
# Arrival: commit terminal time once track descends below this altitude (on ground)
ARRIVAL_GROUND_ALT_M = 10.0


class ProtectorApplication:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(LOGGER_NAME)
        self.reference_data = load_reference_data(config.paths.sys_config_root)

        # Build terminal area from config (optional)
        ta_cfg = config.terminal_area
        terminal_area: TerminalArea | None = None
        if ta_cfg.json_path and ta_cfg.json_path.exists():
            try:
                terminal_area = TerminalArea.from_json(
                    ta_cfg.json_path,
                    ceiling_m=ta_cfg.ceiling_m,
                    floor_m=ta_cfg.floor_m,
                    airports=ta_cfg.airports,
                )
                self.logger.info(
                    "terminal area loaded from JSON: %d vertices, ceiling=%.0fm, floor=%.0fm, airports=%s",
                    len(terminal_area._vertices),
                    terminal_area.ceiling_m,
                    terminal_area.floor_m,
                    terminal_area.airports,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("failed to load terminal area from JSON: %s", exc)
        elif ta_cfg.fdrg_path and ta_cfg.fdrg_path.exists():
            try:
                terminal_area = TerminalArea.from_fdrg(
                    ta_cfg.fdrg_path,
                    ceiling_m=ta_cfg.ceiling_m,
                    floor_m=ta_cfg.floor_m,
                    airports=ta_cfg.airports,
                )
                self.logger.info(
                    "terminal area loaded from FDRG.txt: %d vertices, ceiling=%.0fm, floor=%.0fm, airports=%s",
                    len(terminal_area._vertices),
                    terminal_area.ceiling_m,
                    terminal_area.floor_m,
                    terminal_area.airports,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("failed to load terminal area: %s", exc)

        self.state = ProtectorState(
            reference_data=self.reference_data,
            track_region=config.runtime.track_region,
            terminal_area=terminal_area,
        )
        self.storage = StorageManager(config)
        self.cat062_parser = Cat062Parser()
        self.aftn_parser = AftnParser(self.reference_data)
        self._stop_requested = False
        self._sockets: dict[str, socket.socket] = {}
        self._last_snapshot_at: datetime | None = None
        self._last_housekeeping_at: datetime | None = None
        # track_number -> last altitude, used for departure commit trigger
        self._track_prev_alt: dict[int, float] = {}

    def run(self) -> None:
        self._configure_logging()
        self.storage.ensure_layout()
        restored = self.storage.load_snapshot(self.state)
        self.logger.info("snapshot restored=%s", restored)
        self._open_sockets()
        self._install_signal_handlers()
        now = datetime.utcnow()
        if not restored:
            self.state.current_day = now.date()
            self.state.current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        self._last_snapshot_at = now
        self._last_housekeeping_at = now
        self.logger.info("%s started", self.config.system_name)
        try:
            while not self._stop_requested:
                readable = list(self._sockets.values())
                ready, _, _ = select.select(readable, [], [], 0.5)
                loop_now = datetime.utcnow()
                self._run_housekeeping(loop_now)
                for sock in ready:
                    self._receive_from_socket(sock, loop_now)
        finally:
            self.shutdown()


    def request_stop(self) -> None:
        self._stop_requested = True

    def shutdown(self) -> None:
        self.logger.info("shutting down")
        now = datetime.utcnow()
        self._flush_day_and_snapshots(now)
        self.storage.flush_radar_buffers()
        for sock in self._sockets.values():
            try:
                sock.close()
            except OSError:
                pass
        self._sockets.clear()

    def _configure_logging(self) -> None:
        ensure_directory(self.config.paths.logs_root)
        log_path = self.config.paths.logs_root / f"atc-data-hub-{datetime.now():%Y%m%d}.log"
        handlers: list[logging.Handler] = [
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            handlers=handlers,
            force=True,
        )

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: FrameType | None) -> None:
            self.logger.info("received signal %s", signum)
            self.request_stop()

        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(signum, handler)
            except ValueError:
                continue

    def _open_sockets(self) -> None:
        self._sockets = {
            "radar": self._create_socket(
                self.config.network.radar.bind_host,
                self.config.network.radar.port,
                self.config.network.radar.multicast_group,
                self.config.network.radar.interface_ip,
            ),
            "aftn": self._create_socket(
                self.config.network.aftn.bind_host,
                self.config.network.aftn.port,
                self.config.network.aftn.multicast_group,
                self.config.network.aftn.interface_ip,
            ),
            "speech": self._create_socket(
                self.config.network.speech.bind_host,
                self.config.network.speech.port,
                self.config.network.speech.multicast_group,
                self.config.network.speech.interface_ip,
            ),
        }

    def _create_socket(
        self,
        bind_host: str,
        port: int,
        multicast_group: str | None,
        interface_ip: str,
    ) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((bind_host, port))
        except OSError:
            sock.bind(("", port))
        if multicast_group:
            membership = struct.pack(
                "=4s4s",
                socket.inet_aton(multicast_group),
                socket.inet_aton(interface_ip or "0.0.0.0"),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        sock.setblocking(False)
        return sock

    def _receive_from_socket(self, sock: socket.socket, received_at: datetime) -> None:
        try:
            payload, address = sock.recvfrom(65535)
        except BlockingIOError:
            return
        except OSError as exc:
            self.logger.warning("recv failed: %s", exc)
            return

        for name, item in self._sockets.items():
            if item is not sock:
                continue
            try:
                if name == "radar":
                    self._handle_radar_payload(payload, received_at)
                elif name == "aftn":
                    self._handle_aftn_payload(payload, received_at)
                else:
                    self._handle_speech_payload(payload, received_at)
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("handle %s payload from %s failed: %s", name, address, exc)
            break

    def _handle_radar_payload(self, payload: bytes, received_at: datetime) -> None:
        tracks = self.cat062_parser.parse_datagram(payload, received_at=received_at)
        if not tracks:
            return
        self.storage.append_radar_payload(received_at, payload)
        for track in tracks:
            merged = self.state.ingest_radar_track(track)
            self._check_departure_commit(merged)
            # Extra trigger: departure exits the terminal area horizontally
            if self.state._just_exited_terminal:
                self._check_outbound_exit_commit(merged)

    def _handle_aftn_payload(self, payload: bytes, received_at: datetime) -> None:
        result = self.aftn_parser.parse(payload, received_at=received_at)
        self.state.record_aftn_message(result.message)
        if not result.accepted or result.flight_plan is None:
            if result.errors:
                # Unsupported message types are expected noise; only warn on parse failures.
                is_unsupported = any("不支持的 AFTN 报文类型" in e for e in result.errors)
                if is_unsupported:
                    self.logger.debug("AFTN ignored (unsupported type): %s", result.action or "(unknown)")
                else:
                    self.logger.warning("AFTN parse rejected: %s", "; ".join(result.errors))
            return
        plan = self.state.upsert_flight_plan(result.flight_plan, result.action)
        # On ARR: close any open terminal segment first
        if result.action == "ARR":
            self._flush_terminal_by_callsign(plan.callsign)
        # Flight plan is marked as dirty in state.upsert_flight_plan
        # Persistence will happen during the next snapshot interval
        self._log_flight_plan(result.action, plan)

    def _handle_speech_payload(self, payload: bytes, received_at: datetime) -> None:
        record = self._parse_voice_record(payload, received_at)
        if record is None:
            return
        self.state.add_voice_record(record)

    def _parse_voice_record(self, payload: bytes, received_at: datetime) -> VoiceRecord | None:
        try:
            raw = json.loads(payload.decode("utf-8", errors="ignore"))
        except json.JSONDecodeError:
            self.logger.warning("speech payload is not valid JSON")
            return None

        wav_begin_time = self._parse_utc_time_as_local(raw.get("WavBeginTime"))
        return VoiceRecord(
            received_at=received_at,
            wav_begin_time=wav_begin_time,
            processed_command=str(raw.get("ProcessedCommand", "")),
            callsign=str(raw.get("Callsign", "")),
            send_ip=str(raw.get("SendIp", "")),
            frequency=str(raw.get("Frequency", "")),
            sector=str(raw.get("Sector", "")),
            speaker=str(raw.get("Speaker", "")),
            duration=float(raw.get("Duration", 0.0) or 0.0),
            wav_file_path=str(raw.get("WavFilePath", "")),
            raw_payload=payload,
        )

    def _parse_utc_time_as_local(self, value: object) -> datetime | None:
        parsed = self._parse_optional_time(value)
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone().replace(tzinfo=None)

    def _parse_optional_time(self, value: object) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        text = str(value).strip()
        if not text:
            return None
        parsed = parse_datetime(text)
        if parsed is not None:
            return parsed
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None


    def _run_housekeeping(self, now: datetime) -> None:
        if self._last_housekeeping_at is None:
            self._last_housekeeping_at = now
        if (now - self._last_housekeeping_at).total_seconds() < self.config.runtime.housekeeping_interval_seconds:
            return
        previous_day = self.state.current_day
        previous_hour_start = self.state.current_hour_start
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)

        self.state.cleanup_stale_tracks(now, self.config.runtime.stale_track_seconds)
        self.state.cleanup_old_flight_plans(now, days=2)

        if current_hour_start > previous_hour_start:
            self.storage.flush_radar_buffers()
            self.state.finalize_hour(previous_hour_start)
            self.storage.write_sortie_reports(
                previous_hour_start.date(),
                self.state.daily_hourly_reports(previous_hour_start.date()),
                self.state.monthly_hourly_reports(previous_hour_start.year, previous_hour_start.month),
            )

        if now.date() > previous_day:
            # now is UTC; previous_day is UTC date.
            # Only save flight plans whose DOF == previous_day; DOF is Beijing date,
            # so this intentionally excludes plans that belong to the next day.
            self.storage.persist_daily_outputs(
                self.state, previous_day, utc_day=previous_day
            )
            self.state.prune_flight_plans_after_daily_save(now, utc_now=datetime.utcnow())
            self.state.rollover_day(now.date())



        if self._last_snapshot_at is None:
            self._last_snapshot_at = now
        if (now - self._last_snapshot_at).total_seconds() >= self.config.runtime.snapshot_interval_seconds:
            self._flush_day_and_snapshots(now)
            self._last_snapshot_at = now

        self._last_housekeeping_at = now

    def _flush_day_and_snapshots(self, now: datetime) -> None:
        utc_day = now.date()  # now is UTC
        # Persist flight plans for all dirty DOFs; AFTN messages use UTC date
        for dof in list(self.state.dirty_dofs):
            self.storage.persist_daily_outputs(self.state, dof, utc_day=utc_day)
        self.state.dirty_dofs.clear()

        # Also persist current day (channel occupied, sortie, etc.)
        self.storage.persist_daily_outputs(self.state, self.state.current_day, utc_day=utc_day)

        # Save snapshot
        self.storage.save_snapshot(self.state)

    def _check_departure_commit(self, track) -> None:  # type: ignore[type-arg]
        """Commit terminal time to disk at appropriate altitude thresholds.

        - Outbound (departure): commit when climbing through DEPARTURE_COMMIT_ALT_M.
        - Inbound (arrival): commit when descending through ARRIVAL_GROUND_ALT_M
          (safety net; ARR AFTN message is the primary trigger).
        """
        if self.state.terminal_area is None:
            return
        tn = track.track_number
        if tn < 0:
            return
        from .models import FlightDestination  # local import to avoid cycles

        alt = track.flight_level_m or track.qnh_height_m
        prev_alt = self._track_prev_alt.get(tn, alt)
        self._track_prev_alt[tn] = alt

        dest = track.flight_destination
        if dest == FlightDestination.OUTBOUND:
            # Trigger when crossing from below DEPARTURE_COMMIT_ALT_M to above
            if prev_alt < DEPARTURE_COMMIT_ALT_M <= alt:
                self.state.flush_terminal_time_for_track(track)
                self.storage.persist_daily_outputs(
                    self.state, self.state.current_day, utc_day=self.state.current_day
                )
                self.logger.debug(
                    "departure commit: %s alt %.0f->%.0f m, terminal time flushed",
                    track.primary_callsign, prev_alt, alt,
                )
        elif dest == FlightDestination.INBOUND:
            # Safety net: radar shows the aircraft is on or near the ground
            if prev_alt >= ARRIVAL_GROUND_ALT_M > alt > 0:
                self.state.flush_terminal_time_for_track(track)
                self.storage.persist_daily_outputs(
                    self.state, self.state.current_day, utc_day=self.state.current_day
                )
                self.logger.debug(
                    "arrival commit (radar): %s alt %.0f->%.0f m, terminal time flushed",
                    track.primary_callsign, prev_alt, alt,
                )

    def _check_outbound_exit_commit(self, track) -> None:  # type: ignore[type-arg]
        """Commit terminal time when a departure track exits the terminal area horizontally.

        This handles the case where an outbound flight leaves the terminal area
        boundary before climbing through DEPARTURE_COMMIT_ALT_M (e.g. visual
        departure, missed approach turned outbound, etc.).
        The time has already been accumulated by _update_terminal_time; we only
        need to flush the flight plan to disk here.
        """
        from .models import FlightDestination  # local import to avoid cycles

        dest = track.flight_destination
        if dest != FlightDestination.OUTBOUND:
            return
        self.storage.persist_daily_outputs(
            self.state, self.state.current_day, utc_day=self.state.current_day
        )
        self.logger.debug(
            "departure commit (area exit): %s exited terminal area, terminal time flushed",
            track.primary_callsign,
        )

    def _log_flight_plan(self, action: str, plan: "FlightPlan") -> None:  # noqa: F821
        """Log a one-line summary of a newly added or updated flight plan."""
        dof_str = f"{plan.dof:%Y-%m-%d}" if plan.dof else "-"
        etd_str = f"{plan.etd:%d %H%M}" if plan.etd else "-"
        atd_str = f"{plan.atd:%d %H%M}" if plan.atd else ""
        eta_str = f"{plan.eta:%d %H%M}" if plan.eta else "-"
        ata_str = f"{plan.ata:%d %H%M}" if plan.ata else ""

        parts = [
            f"[{action}]",
            plan.callsign or "-",
            f"{plan.adep}->{plan.adest}",
            plan.aircraft_type or "-",
            f"DOF={dof_str}",
            f"ETD={etd_str}",
        ]
        if atd_str:
            parts.append(f"ATD={atd_str}")
        parts.append(f"ETA={eta_str}")
        if ata_str:
            parts.append(f"ATA={ata_str}")
        if plan.ssr:
            parts.append(f"SSR={plan.ssr}")
        self.logger.info(" ".join(parts))

    def _flush_terminal_by_callsign(self, callsign: str) -> None:
        """Flush terminal time for any active track matching *callsign*."""
        track = self.state.find_track_by_callsign(callsign)
        if track is not None:
            self.state.flush_terminal_time_for_track(track)


def run_application(config: AppConfig) -> None:
    ProtectorApplication(config).run()
