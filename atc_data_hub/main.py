from __future__ import annotations

import argparse
from pathlib import Path

from .app import run_application
from .config import load_app_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ATC Data Hub Python")

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="启动服务")
    run_parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parents[1] / "config" / "default.json"),
        help="配置文件路径",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "run"
    if command != "run":
        parser.print_help()
        return 1
    config = load_app_config(args.config)
    run_application(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
