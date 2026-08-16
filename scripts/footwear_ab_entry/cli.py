"""CLI for licensed tire schema and manufacturer-parent probes."""

from __future__ import annotations

import argparse
import json

from scripts.ab_entry_pilot.extract import connect

from .extract import (
    extract_full,
    load_description_identity,
    load_parent_seed,
    quarter_bounds,
    validate_quarter,
)
from .schema_probe import discover_parent_candidates, probe_schema
from .transforms import build_game_outputs
from .qa import capture_g0_validation, run_runtime_qa


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("probe-schema")
    commands.add_parser("discover-parents")
    validation = commands.add_parser("validate-quarter")
    validation.add_argument("--quarter", required=True)
    extraction = commands.add_parser("extract-full")
    extraction.add_argument("--game", choices=("finished", "both"), required=True)
    build = commands.add_parser("build")
    build.add_argument("--game", choices=("finished",), required=True)
    commands.add_parser("qa")
    return parser


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    parent_ids = None
    parent_seed_sha256 = None
    description_identity = None
    if args.command == "validate-quarter":
        try:
            quarter_bounds(args.quarter)
        except ValueError:
            _print_json(
                {
                    "status": "error",
                    "command": args.command,
                    "error_category": "invalid_quarter",
                }
            )
            return 1
    if args.command in {"validate-quarter", "extract-full", "build", "qa"}:
        try:
            seed_snapshot = load_parent_seed()
            parent_ids = seed_snapshot.parent_ids
            parent_seed_sha256 = seed_snapshot.sha256
            if args.command in {"validate-quarter", "extract-full"}:
                description_identity = load_description_identity()
        except Exception:
            _print_json(
                {
                    "status": "error",
                    "command": args.command,
                    "error_category": "parent_seed_gate_failed",
                }
            )
            return 1
    if args.command in {"build", "qa"}:
        try:
            if args.command == "build":
                result = build_game_outputs(
                    game=args.game,
                    parent_seed_sha256=parent_seed_sha256,
                    manufacturer_parent_ids=parent_ids.values(),
                )
                _print_json({"status": "ok", "command": args.command, **result})
                return 0
            result = run_runtime_qa(parent_seed_sha256=parent_seed_sha256)
            exit_code = int(result.get("exit_code", 1))
            _print_json(
                {
                    "status": "ok" if exit_code == 0 else "failed",
                    "command": args.command,
                    **result,
                }
            )
            return exit_code
        except Exception:
            _print_json(
                {
                    "status": "error",
                    "command": args.command,
                    "error_category": f"{args.command}_operation_failed",
                }
            )
            return 1
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
    g0_failed = False
    try:
        if args.command == "probe-schema":
            result = probe_schema(connection)
        elif args.command == "discover-parents":
            result = discover_parent_candidates(connection)
        elif args.command == "validate-quarter":
            result = validate_quarter(
                connection,
                args.quarter,
                parent_ids=parent_ids,
                parent_seed_sha256=parent_seed_sha256,
                description_identity=description_identity,
            )
            result["g0_validation"] = capture_g0_validation(
                connection,
                args.quarter,
                parent_ids=parent_ids,
                parent_seed_sha256=parent_seed_sha256,
                description_identity=description_identity,
                validation_root=result.get("validation_root"),
            )
            g0_failed = not bool(result["g0_validation"].get("reconciled"))
        else:
            result = extract_full(
                connection,
                game=args.game,
                parent_ids=parent_ids,
                parent_seed_sha256=parent_seed_sha256,
                description_identity=description_identity,
            )
    except Exception:
        primary_failed = True

    close_failed = False
    try:
        connection.close()
    except Exception:
        close_failed = True

    if primary_failed or g0_failed or close_failed:
        payload = {
            "status": "error",
            "command": args.command,
            "error_category": (
                "operation_failed"
                if primary_failed
                else "g0_reconciliation_failed"
                if g0_failed
                else "connection_close_failed"
            ),
        }
        if (primary_failed or g0_failed) and close_failed:
            payload["connection_close_failed"] = True
        _print_json(payload)
        return 1

    _print_json({"status": "ok", "command": args.command, **result})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
