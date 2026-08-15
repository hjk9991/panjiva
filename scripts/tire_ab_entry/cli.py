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
    connection = None
    try:
        connection = connect()
        if args.command == "probe-schema":
            result = probe_schema(connection)
        else:
            result = discover_parent_candidates(connection)
        _print_json({"status": "ok", "command": args.command, **result})
        return 0
    except Exception as error:
        _print_json(
            {
                "status": "error",
                "command": args.command,
                "error_type": type(error).__name__,
            }
        )
        return 1
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
