import json

import scripts.tire_ab_entry.cli as cli_module
from scripts.tire_ab_entry.cli import build_parser


class Connection:
    def __init__(self, calls, *, close_error=None):
        self.calls = calls
        self.close_error = close_error

    def close(self):
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error


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
        or {
            "candidate_counts": {"Michelin": 2},
            "metadata_path": r"C:\panjiva\review\candidates.metadata.json",
            "review_status": "human_selection_required",
        },
    )

    assert cli_module.main(["discover-parents"]) == 0

    assert calls == [connection, "close"]
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "ok"
    assert output["command"] == "discover-parents"
    assert output["candidate_counts"] == {"Michelin": 2}
    assert output["metadata_path"].endswith("candidates.metadata.json")
    assert output["review_status"] == "human_selection_required"


def test_cli_redacts_connection_open_failure(monkeypatch, capsys):
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
        "error_category": "connection_open_failed",
        "status": "error",
    }


def test_cli_with_successful_probe_but_close_failure_emits_only_error(
    monkeypatch, capsys
):
    connection = Connection([], close_error=RuntimeError("password=close-secret"))
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "probe_schema",
        lambda value: {"description_mode": "unavailable"},
    )

    assert cli_module.main(["probe-schema"]) == 1

    output_text = capsys.readouterr().out
    assert "secret" not in output_text
    assert output_text.count("\n") == 1
    assert json.loads(output_text) == {
        "command": "probe-schema",
        "error_category": "connection_close_failed",
        "status": "error",
    }


def test_cli_preserves_primary_category_when_probe_and_close_fail(
    monkeypatch, capsys
):
    connection = Connection([], close_error=RuntimeError("password=close-secret"))
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "probe_schema",
        lambda value: (_ for _ in ()).throw(RuntimeError("password=query-secret")),
    )

    assert cli_module.main(["probe-schema"]) == 1

    output_text = capsys.readouterr().out
    assert "secret" not in output_text
    assert json.loads(output_text) == {
        "command": "probe-schema",
        "connection_close_failed": True,
        "error_category": "operation_failed",
        "status": "error",
    }
