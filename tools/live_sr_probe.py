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

from atc_data_hub.config import load_app_config
from atc_data_hub.utils import parse_datetime



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


def parse_optional_time(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
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


def summarize_record(raw: dict[str, Any], received_at: datetime) -> dict[str, Any]:
    wav_begin_time = parse_optional_time(raw.get("WavBeginTime"))
    duration = float(raw.get("Duration", 0.0) or 0.0)
    return {
        "received_at": received_at.isoformat(timespec="milliseconds"),
        "wav_begin_time": wav_begin_time.isoformat(timespec="seconds") if wav_begin_time else None,
        "callsign": str(raw.get("Callsign", "")),
        "sector": str(raw.get("Sector", "")),
        "speaker": str(raw.get("Speaker", "")),
        "duration": duration,
        "frequency": str(raw.get("Frequency", "")),
        "send_ip": str(raw.get("SendIp", "")),
        "wav_file_path": str(raw.get("WavFilePath", "")),
        "processed_command": str(raw.get("ProcessedCommand", "")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Live speech UDP probe")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "default.json"))
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    config = load_app_config(args.config)
    endpoint = config.network.speech

    started_at = datetime.now()
    sector_counter: Counter[str] = Counter()
    speaker_counter: Counter[str] = Counter()
    result: dict[str, Any] = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "duration_seconds": args.duration,
        "bind_host": endpoint.bind_host,
        "port": endpoint.port,
        "multicast_group": endpoint.multicast_group,
        "interface_ip": endpoint.interface_ip,
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
                raw = json.loads(payload.decode("utf-8", errors="ignore"))
                summary = summarize_record(raw, received_at)
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

            result["records_parsed"] += 1
            sector_counter[summary["sector"] or ""] += 1
            speaker_counter[summary["speaker"] or ""] += 1
            if len(result["message_samples"]) < args.sample_limit:
                result["message_samples"].append(
                    {
                        "from": f"{address[0]}:{address[1]}",
                        "payload_length": len(payload),
                        **summary,
                    }
                )
    finally:
        sock.close()

    result["sector_counts"] = dict(sector_counter)
    result["speaker_counts"] = dict(speaker_counter)
    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["success"] = result["packets_received"] > 0 and result["records_parsed"] > 0
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
