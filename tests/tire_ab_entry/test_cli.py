import json

import scripts.tire_ab_entry.cli as cli_module
from scripts.tire_ab_entry.cli import build_parser


class Connection:
    def __init__(self, calls):
        self.calls = calls

    def close(self):
        self.calls.append("close")


def test_cli_exposes_schema_and_parent_commands():
    parser = build_parser()
    assert parser.parse_args(["probe-schema"]).command == "probe-schema"
    assert parser.parse_args(["discover-parents"]).command == "discover-parents"


def test_probe_schema_command_prints_json_and_closes_connection(monkeypatch, capsys):
    calls = []
    connection = Connection(calls)
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "probe_schema",
        lambda value: calls.append(value) or {"description_mode": "unavailable"},
    )

    assert cli_module.main(["probe-schema"]) == 0

    assert calls == [connection, "close"]
    assert json.loads(capsys.readouterr().out) == {
        "command": "probe-schema",
        "description_mode": "unavailable",
        "status": "ok",
    }


def test_discover_parents_command_prints_counts(monkeypatch, capsys):
    calls = []
    connection = Connection(calls)
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "discover_parent_candidates",
        lambda value: calls.append(value)
        or {"candidate_counts": {"Michelin": 2}, "review_status": "human_selection_required"},
    )

    assert cli_module.main(["discover-parents"]) == 0

    assert calls == [connection, "close"]
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "ok"
    assert output["command"] == "discover-parents"
    assert output["candidate_counts"] == {"Michelin": 2}
    assert output["review_status"] == "human_selection_required"


def test_cli_returns_nonzero_without_leaking_exception_text(monkeypatch, capsys):
    monkeypatch.setattr(
        cli_module,
        "connect",
        lambda: (_ for _ in ()).throw(RuntimeError("password=do-not-print")),
    )

    assert cli_module.main(["probe-schema"]) == 1

    output_text = capsys.readouterr().out
    assert "do-not-print" not in output_text
    assert json.loads(output_text) == {
        "command": "probe-schema",
        "error_type": "RuntimeError",
        "status": "error",
    }
