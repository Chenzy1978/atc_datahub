from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from atc_data_hub.config import load_app_config
from atc_data_hub.parsers.cat062 import Cat062Parser
from atc_data_hub.reference import load_reference_data




def create_socket(bind_host: str, port: int, group: str | None, interface_ip: str) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((bind_host, port))
    except OSError:
        sock.close()
        raise

    if group:
        membership = struct.pack(
            "4s4s",
            socket.inet_aton(group),
            socket.inet_aton(interface_ip or "0.0.0.0"),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)

    sock.settimeout(1.0)
    return sock


def summarize_track(track: Any, sector_names: list[str]) -> dict[str, Any]:
    mapped_sector_name = ""
    if 0 <= track.sector_index < len(sector_names):
        mapped_sector_name = sector_names[track.sector_index].strip()
    return {
        "track_number": track.track_number,
        "callsign": track.primary_callsign,
        "acid": track.acid,
        "target_id": track.target_id,
        "ssr": track.ssr,
        "latitude": track.latitude,
        "longitude": track.longitude,
        "speed_kmh": track.speed_kmh,
        "heading_deg": track.heading_deg,
        "flight_level_m": track.flight_level_m,
        "selected_altitude_m": track.selected_altitude_m,
        "adep": track.adep,
        "adst": track.adst,
        "runway": track.runway,
        "sector_index": track.sector_index,
        "sector_name": mapped_sector_name,
        "raw_sector_name": track.sector_name,
        "sid": track.sid,
        "star": track.star,
    }



def main() -> int:
    parser = argparse.ArgumentParser(description="Live CAT062 multicast probe")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "default.json"))
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    config = load_app_config(args.config)
    endpoint = config.network.radar
    reference_data = load_reference_data(config.paths.sys_config_root)
    cat_parser = Cat062Parser()


    started_at = datetime.now()
    result: dict[str, Any] = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "duration_seconds": args.duration,
        "bind_host": endpoint.bind_host,
        "port": endpoint.port,
        "multicast_group": endpoint.multicast_group,
        "interface_ip": endpoint.interface_ip,
        "packets_received": 0,
        "records_decoded": 0,
        "decode_errors": [],
        "packet_samples": [],
        "non_zero_sector_tracks": [],
        "first_packet_from": None,

        "first_packet_at": None,
        "last_packet_at": None,
    }

    try:
        sock = create_socket(endpoint.bind_host, endpoint.port, endpoint.multicast_group, endpoint.interface_ip)
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
                tracks = cat_parser.parse_datagram(payload, received_at)
                result["records_decoded"] += len(tracks)
                summarized_tracks = [summarize_track(track, reference_data.sector_info) for track in tracks[:3]]
                if len(result["packet_samples"]) < args.sample_limit:
                    result["packet_samples"].append(
                        {
                            "from": f"{address[0]}:{address[1]}",
                            "received_at": received_at.isoformat(timespec="milliseconds"),
                            "payload_length": len(payload),
                            "record_count": len(tracks),
                            "tracks": summarized_tracks,
                        }
                    )
                for item in summarized_tracks:
                    if item["sector_index"] > 0 and len(result["non_zero_sector_tracks"]) < args.sample_limit:
                        result["non_zero_sector_tracks"].append(item)

            except Exception as exc:  # noqa: BLE001
                if len(result["decode_errors"]) < args.sample_limit:
                    result["decode_errors"].append(
                        {
                            "from": f"{address[0]}:{address[1]}",
                            "received_at": received_at.isoformat(timespec="milliseconds"),
                            "payload_length": len(payload),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
    finally:
        sock.close()

    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["success"] = result["packets_received"] > 0 and result["records_decoded"] > 0 and not result["decode_errors"]
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
