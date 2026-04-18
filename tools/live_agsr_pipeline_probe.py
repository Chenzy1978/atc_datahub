from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from atc_data_hub.app import ProtectorApplication
from atc_data_hub.config import load_app_config



def create_socket(bind_host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    try:
        sock.bind((bind_host, port))
    except OSError:
        sock.close()
        raise
    sock.settimeout(1.0)
    return sock


def summarize_record(record: Any) -> dict[str, Any]:
    return {
        "received_at": record.received_at.isoformat(timespec="milliseconds"),
        "event_time": record.event_time.isoformat(timespec="seconds"),
        "wav_begin_time": record.wav_begin_time.isoformat(timespec="seconds") if record.wav_begin_time else None,
        "callsign": record.callsign,
        "sector": record.sector,
        "speaker": record.speaker,
        "duration": record.duration,
        "frequency": record.frequency,
        "send_ip": record.send_ip,
        "wav_file_path": record.wav_file_path,
        "processed_command": record.processed_command,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Live AGSR persist/channel probe")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "default.json"))
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    config = load_app_config(args.config)
    app = ProtectorApplication(config)
    app.storage.ensure_layout()
    endpoint = config.network.speech

    started_at = datetime.now()
    sector_counter: Counter[str] = Counter()
    speaker_counter: Counter[str] = Counter()
    result: dict[str, Any] = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "duration_seconds": args.duration,
        "bind_host": endpoint.bind_host,
        "port": endpoint.port,
        "records_root": str(app.storage.records_root),
        "packets_received": 0,
        "records_parsed": 0,
        "json_decode_errors": 0,
        "parse_errors": [],
        "first_packet_from": None,
        "first_packet_at": None,
        "last_packet_at": None,
        "sector_counts": {},
        "speaker_counts": {},
        "message_samples": [],
        "voice_days": [],
        "voice_output_files": [],
        "channel_output_file": None,
        "channel_non_zero_buckets": {},
    }

    try:
        sock = create_socket(endpoint.bind_host, endpoint.port)
    except OSError as exc:
        result["socket_error"] = str(exc)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    deadline = time.time() + max(args.duration, 1)
    try:
        while time.time() < deadline:
            try:
                payload, address = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError as exc:
                result.setdefault("socket_errors", []).append(str(exc))
                break

            received_at = datetime.now()
            result["packets_received"] += 1
            result["last_packet_at"] = received_at.isoformat(timespec="milliseconds")
            if result["first_packet_from"] is None:
                result["first_packet_from"] = f"{address[0]}:{address[1]}"
                result["first_packet_at"] = received_at.isoformat(timespec="milliseconds")

            try:
                record = app._parse_voice_record(payload, received_at)
            except json.JSONDecodeError as exc:
                result["json_decode_errors"] += 1
                if len(result["parse_errors"]) < args.sample_limit:
                    result["parse_errors"].append(
                        {
                            "from": f"{address[0]}:{address[1]}",
                            "received_at": received_at.isoformat(timespec="milliseconds"),
                            "payload_length": len(payload),
                            "error": f"JSONDecodeError: {exc}",
                            "payload_preview": payload[:200].decode("utf-8", errors="ignore"),
                        }
                    )
                continue
            except Exception as exc:  # noqa: BLE001
                if len(result["parse_errors"]) < args.sample_limit:
                    result["parse_errors"].append(
                        {
                            "from": f"{address[0]}:{address[1]}",
                            "received_at": received_at.isoformat(timespec="milliseconds"),
                            "payload_length": len(payload),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                continue

            if record is None:
                continue

            app.state.add_voice_record(record)
            result["records_parsed"] += 1
            sector_counter[record.sector or ""] += 1
            speaker_counter[record.speaker or ""] += 1
            if len(result["message_samples"]) < args.sample_limit:
                result["message_samples"].append(
                    {
                        "from": f"{address[0]}:{address[1]}",
                        "payload_length": len(payload),
                        **summarize_record(record),
                    }
                )
    finally:
        sock.close()

    voice_days = sorted({record.event_time.date() for record in app.state.voice_records})
    for day in voice_days:
        path = app.storage.write_voice_records(day, app.state.daily_voice_records(day))
        result["voice_output_files"].append(str(path))
    channel_path = app.storage.write_channel_occupied(app.state.channel_occupied)
    result["channel_output_file"] = str(channel_path)

    result["voice_days"] = [day.isoformat() for day in voice_days]
    result["sector_counts"] = dict(sector_counter)
    result["speaker_counts"] = dict(speaker_counter)
    result["channel_non_zero_buckets"] = {
        sector: [
            {
                "bucket_index": index,
                "start": f"{index // 6:02d}:{(index % 6) * 10:02d}",
                "seconds": value,
            }
            for index, value in enumerate(values)
            if value
        ]
        for sector, values in app.state.channel_occupied.channels.items()
        if any(values)
    }

    if voice_days:
        sample_voice_path = Path(result["voice_output_files"][0])
        stored_voice_records = json.loads(sample_voice_path.read_text(encoding="utf-8"))
        result["voice_file_record_count"] = len(stored_voice_records)
        result["voice_file_first_record"] = stored_voice_records[0] if stored_voice_records else None

    stored_channel = json.loads(Path(channel_path).read_text(encoding="utf-8"))
    result["channel_file_keys"] = list(stored_channel.get("channels", {}).keys())
    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["success"] = result["records_parsed"] > 0 and bool(result["channel_non_zero_buckets"])
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
