"""CLI for licensed tire schema and manufacturer-parent probes."""

from __future__ import annotations

import argparse
import json

from scripts.ab_entry_pilot.extract import connect

from .schema_probe import discover_parent_candidates, probe_schema


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("probe-schema")
    commands.add_parser("discover-parents")
    return parser


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        connection = connect()
    except Exception:
        _print_json(
            {
                "status": "error",
                "command": args.command,
                "error_category": "connection_open_failed",
            }
        )
        return 1

    result = None
    primary_failed = False
    try:
        if args.command == "probe-schema":
            result = probe_schema(connection)
        else:
            result = discover_parent_candidates(connection)
    except Exception:
        primary_failed = True

    close_failed = False
    try:
        connection.close()
    except Exception:
        close_failed = True

    if primary_failed or close_failed:
        payload = {
            "status": "error",
            "command": args.command,
            "error_category": (
                "operation_failed" if primary_failed else "connection_close_failed"
            ),
        }
        if primary_failed and close_failed:
            payload["connection_close_failed"] = True
        _print_json(payload)
        return 1

    _print_json({"status": "ok", "command": args.command, **result})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
