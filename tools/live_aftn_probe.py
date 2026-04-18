from __future__ import annotations

import argparse
import json
import socket
import struct
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
from atc_data_hub.parsers.aftn import AftnParser
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


def summarize_result(result: Any) -> dict[str, Any]:
    plan = result.flight_plan
    return {
        "message_type": result.message.message_type,
        "accepted": result.accepted,
        "errors": list(result.errors),
        "raw_text": result.raw_text[:500],
        "flight_plan": None
        if plan is None
        else {
            "callsign": plan.callsign,
            "adep": plan.adep,
            "adest": plan.adest,
            "ssr": plan.ssr,
            "aircraft_type": plan.aircraft_type,
            "flight_rules": plan.flight_rules,
            "route": plan.route,
            "transfer_fix": plan.transfer_fix,
            "dof": plan.dof.isoformat() if plan.dof else None,
            "etd": plan.etd.isoformat(timespec="minutes") if plan.etd else None,
            "atd": plan.atd.isoformat(timespec="minutes") if plan.atd else None,
            "eta": plan.eta.isoformat(timespec="minutes") if plan.eta else None,
            "ata": plan.ata.isoformat(timespec="minutes") if plan.ata else None,
            "eet_minutes": plan.eet_minutes,
            "source_message_type": plan.source_message_type,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Live AFTN multicast probe")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "default.json"))
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    config = load_app_config(args.config)
    endpoint = config.network.aftn
    reference_data = load_reference_data(config.paths.sys_config_root)
    aftn_parser = AftnParser(reference_data)

    started_at = datetime.now()
    type_counter: Counter[str] = Counter()
    result: dict[str, Any] = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "duration_seconds": args.duration,
        "bind_host": endpoint.bind_host,
        "port": endpoint.port,
        "multicast_group": endpoint.multicast_group,
        "interface_ip": endpoint.interface_ip,
        "packets_received": 0,
        "accepted_messages": 0,
        "rejected_messages": 0,
        "first_packet_from": None,
        "first_packet_at": None,
        "last_packet_at": None,
        "message_type_counts": {},
        "parse_errors": [],
        "message_samples": [],
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
                parsed = aftn_parser.parse(payload, received_at)
            except Exception as exc:  # noqa: BLE001
                result["rejected_messages"] += 1
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

            message_type = parsed.message.message_type or parsed.action or "UNKNOWN"
            type_counter[message_type] += 1
            if parsed.accepted:
                result["accepted_messages"] += 1
            else:
                result["rejected_messages"] += 1
                if parsed.errors and len(result["parse_errors"]) < args.sample_limit:
                    result["parse_errors"].append(
                        {
                            "from": f"{address[0]}:{address[1]}",
                            "received_at": received_at.isoformat(timespec="milliseconds"),
                            "payload_length": len(payload),
                            "message_type": message_type,
                            "errors": list(parsed.errors),
                            "raw_text": parsed.raw_text[:500],
                        }
                    )

            if len(result["message_samples"]) < args.sample_limit:
                result["message_samples"].append(
                    {
                        "from": f"{address[0]}:{address[1]}",
                        "received_at": received_at.isoformat(timespec="milliseconds"),
                        "payload_length": len(payload),
                        **summarize_result(parsed),
                    }
                )
    finally:
        sock.close()

    result["message_type_counts"] = dict(type_counter)
    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["success"] = result["packets_received"] > 0 and result["accepted_messages"] > 0
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
