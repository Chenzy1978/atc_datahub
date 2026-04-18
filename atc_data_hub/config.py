from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree


@dataclass(slots=True)
class EndpointConfig:
    bind_host: str
    port: int
    multicast_group: str | None = None
    interface_ip: str = "0.0.0.0"


@dataclass(slots=True)
class PathsConfig:
    records_root: Path
    runtime_root: Path
    logs_root: Path
    sys_config_root: Path | None = None


@dataclass(slots=True)
class TrackRegion:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon


@dataclass(slots=True)
class TerminalAreaConfig:
    fdrg_path: Path | None = None        # path to FDRG.txt polygon file
    ceiling_m: float = 4500.0            # vertical ceiling in metres
    airports: list[str] = None           # ICAO codes of airports inside the terminal area

    def __post_init__(self) -> None:
        if self.airports is None:
            self.airports = []


@dataclass(slots=True)
class RuntimeConfig:
    stale_track_seconds: int
    radar_flush_every_messages: int
    housekeeping_interval_seconds: int
    snapshot_interval_seconds: int
    track_region: TrackRegion


@dataclass(slots=True)
class CompatibilityConfig:
    keep_rcd_format: bool
    data_file_extension: str
    data_file_encoding: str
    notes: list[str]


@dataclass(slots=True)
class NetworkConfig:
    radar: EndpointConfig
    aftn: EndpointConfig
    speech: EndpointConfig


@dataclass(slots=True)
class AppConfig:
    system_name: str
    use_legacy_sysconfig: bool
    paths: PathsConfig
    network: NetworkConfig
    runtime: RuntimeConfig
    compatibility: CompatibilityConfig
    terminal_area: TerminalAreaConfig
    config_file: Path


def load_app_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw = _resolve_relative_paths(raw, _resolve_path_base_dir(path))
    if raw.get("use_legacy_sysconfig"):
        _apply_legacy_sysconfig_overrides(raw)
    return _build_config(raw, path)


def _resolve_path_base_dir(config_file: Path) -> Path:
    project_root = config_file.parent.parent
    if config_file.parent.name.lower() == "config" and (project_root / "atc_data_hub").exists():
        return project_root
    return config_file.parent


def _resolve_relative_paths(raw: dict[str, Any], base_dir: Path) -> dict[str, Any]:

    result = dict(raw)
    paths = dict(result.get("paths", {}))
    for key in ("records_root", "runtime_root", "logs_root", "sys_config_root"):
        value = paths.get(key)
        if not value:
            continue
        candidate = Path(value)
        if not candidate.is_absolute():
            paths[key] = str((base_dir / candidate).resolve())
    result["paths"] = paths
    return result


def _apply_legacy_sysconfig_overrides(raw: dict[str, Any]) -> None:
    sys_config_root = raw.get("paths", {}).get("sys_config_root")
    if not sys_config_root:
        return
    root = Path(sys_config_root)
    if not root.exists():
        return

    ip_setting = root / "IPSetting.xml"
    if ip_setting.exists():
        node = _read_xml_root(ip_setting)
        network = raw.setdefault("network", {})
        radar = network.setdefault("radar", {})
        aftn = network.setdefault("aftn", {})
        speech = network.setdefault("speech", {})

        radar_node = node.find("IPAddrCAT062")
        if radar_node is not None:
            radar["multicast_group"] = radar_node.attrib.get("IP", radar.get("multicast_group"))
            radar["port"] = int(radar_node.attrib.get("PORT", radar.get("port", 0)))

        speech_node = node.find("IPAddrSR")
        if speech_node is not None:
            speech["port"] = int(speech_node.attrib.get("PORT", speech.get("port", 0)))

        aftn_node = node.find("IPAddrAFTN")
        if aftn_node is not None:
            aftn["multicast_group"] = aftn_node.attrib.get("IP", aftn.get("multicast_group"))
            aftn["port"] = int(aftn_node.attrib.get("PORT", aftn.get("port", 0)))



def _read_xml_root(path: Path) -> ElementTree.Element:
    raw_bytes = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "gb2312", "gbk"):
        try:
            text = raw_bytes.decode(encoding)
            return ElementTree.fromstring(text)
        except (UnicodeDecodeError, ElementTree.ParseError):
            continue
    return ElementTree.fromstring(raw_bytes.decode("utf-8", errors="ignore"))


def _build_config(raw: dict[str, Any], config_file: Path) -> AppConfig:

    paths_raw = raw["paths"]
    runtime_raw = raw["runtime"]
    network_raw = raw["network"]
    compatibility_raw = raw["compatibility"]
    ta_raw = raw.get("terminal_area", {})

    # Resolve fdrg_path relative to config file's base directory
    fdrg_path: Path | None = None
    fdrg_value = ta_raw.get("fdrg_path")
    if fdrg_value:
        candidate = Path(fdrg_value)
        if not candidate.is_absolute():
            candidate = (config_file.parent / candidate).resolve()
        fdrg_path = candidate

    return AppConfig(
        system_name=raw.get("system_name", "ATC Data Hub Python"),
        use_legacy_sysconfig=bool(raw.get("use_legacy_sysconfig", False)),
        paths=PathsConfig(
            records_root=Path(paths_raw["records_root"]),
            runtime_root=Path(paths_raw["runtime_root"]),
            logs_root=Path(paths_raw["logs_root"]),
            sys_config_root=Path(paths_raw["sys_config_root"]) if paths_raw.get("sys_config_root") else None,
        ),
        network=NetworkConfig(
            radar=_build_endpoint(network_raw["radar"]),
            aftn=_build_endpoint(network_raw["aftn"]),
            speech=_build_endpoint(network_raw["speech"]),
        ),
        runtime=RuntimeConfig(
            stale_track_seconds=int(runtime_raw["stale_track_seconds"]),
            radar_flush_every_messages=int(runtime_raw["radar_flush_every_messages"]),
            housekeeping_interval_seconds=int(runtime_raw["housekeeping_interval_seconds"]),
            snapshot_interval_seconds=int(runtime_raw["snapshot_interval_seconds"]),
            track_region=TrackRegion(**runtime_raw["track_region"]),
        ),
        compatibility=CompatibilityConfig(
            keep_rcd_format=bool(compatibility_raw["keep_rcd_format"]),
            data_file_extension=str(compatibility_raw["data_file_extension"]),
            data_file_encoding=str(compatibility_raw["data_file_encoding"]),
            notes=list(compatibility_raw.get("notes", [])),
        ),
        terminal_area=TerminalAreaConfig(
            fdrg_path=fdrg_path,
            ceiling_m=float(ta_raw.get("ceiling_m", 4500.0)),
            airports=list(ta_raw.get("airports", [])),
        ),
        config_file=config_file,
    )


def _build_endpoint(raw: dict[str, Any]) -> EndpointConfig:
    return EndpointConfig(
        bind_host=raw.get("bind_host", "0.0.0.0"),
        port=int(raw["port"]),
        multicast_group=raw.get("multicast_group"),
        interface_ip=raw.get("interface_ip", "0.0.0.0"),
    )
