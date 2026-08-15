import json

import scripts.tire_ab_entry.cli as cli_module
from scripts.tire_ab_entry.cli import build_parser
from scripts.tire_ab_entry.extract import ParentSeedSnapshot


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
    assert parser.parse_args(["validate-quarter", "--quarter", "2024Q1"]).quarter == "2024Q1"
    assert parser.parse_args(["extract-full", "--game", "both"]).game == "both"
    assert parser.parse_args(["build", "--game", "raw"]).game == "raw"
    assert parser.parse_args(["qa"]).command == "qa"


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


def test_extract_full_refuses_bad_seed_before_opening_connection(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        cli_module,
        "load_parent_seed",
        lambda: (_ for _ in ()).throw(ValueError("seed secret password=x")),
    )
    monkeypatch.setattr(cli_module, "connect", lambda: calls.append("connect"))
    assert cli_module.main(["extract-full", "--game", "both"]) == 1
    assert calls == []
    output = capsys.readouterr().out
    assert "secret" not in output
    assert json.loads(output)["error_category"] == "parent_seed_gate_failed"


def test_extract_full_passes_games_and_closes_before_single_json(monkeypatch, capsys):
    calls = []
    connection = Connection(calls)
    monkeypatch.setattr(
        cli_module,
        "load_parent_seed",
        lambda: ParentSeedSnapshot(
            {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3}, "seedhash"
        ),
    )
    monkeypatch.setattr(cli_module, "load_description_identity", lambda: None)
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "extract_full",
        lambda value, **kwargs: calls.append((value, kwargs)) or {"completed": 96},
    )
    assert cli_module.main(["extract-full", "--game", "both"]) == 0
    assert calls[-1] == "close"
    invocation = calls[0]
    assert invocation[0] is connection
    assert invocation[1]["game"] == "both"
    assert invocation[1]["parent_seed_sha256"] == "seedhash"
    assert json.loads(capsys.readouterr().out)["completed"] == 96


def test_validate_quarter_uses_isolated_orchestrator(monkeypatch, capsys):
    calls = []
    connection = Connection(calls)
    monkeypatch.setattr(
        cli_module,
        "load_parent_seed",
        lambda: ParentSeedSnapshot(
            {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3}, "seedhash"
        ),
    )
    monkeypatch.setattr(cli_module, "load_description_identity", lambda: None)
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "validate_quarter",
        lambda value, quarter, **kwargs: calls.append((value, quarter, kwargs))
        or {"run_id": "test", "games": ["raw", "finished"]},
    )
    monkeypatch.setattr(
        cli_module,
        "capture_g0_validation",
        lambda value, quarter, **kwargs: calls.append(("g0", quarter, kwargs))
        or {"metrics_path": r"C:\panjiva\_validation\test\g0.parquet", "reconciled": True},
    )
    assert cli_module.main(["validate-quarter", "--quarter", "2024Q1"]) == 0
    assert calls[0][1] == "2024Q1"
    assert calls[1][0] == "g0"
    assert calls[1][2]["parent_seed_sha256"] == "seedhash"
    assert calls[-1] == "close"
    output = json.loads(capsys.readouterr().out)
    assert output["run_id"] == "test"
    assert output["g0_validation"]["metrics_path"].endswith("g0.parquet")


def test_validate_quarter_returns_nonzero_when_g0_does_not_reconcile(monkeypatch, capsys):
    calls = []
    connection = Connection(calls)
    monkeypatch.setattr(
        cli_module,
        "load_parent_seed",
        lambda: ParentSeedSnapshot(
            {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3}, "seedhash"
        ),
    )
    monkeypatch.setattr(cli_module, "load_description_identity", lambda: None)
    monkeypatch.setattr(cli_module, "connect", lambda: connection)
    monkeypatch.setattr(
        cli_module,
        "validate_quarter",
        lambda *args, **kwargs: {
            "run_id": "mismatch",
            "validation_root": r"C:\panjiva\_validation\mismatch",
            "games": [],
        },
    )
    monkeypatch.setattr(
        cli_module,
        "capture_g0_validation",
        lambda *args, **kwargs: {"reconciled": False, "metrics_path": "isolated"},
    )
    assert cli_module.main(["validate-quarter", "--quarter", "2024Q1"]) == 1
    assert calls == ["close"]
    output = json.loads(capsys.readouterr().out)
    assert output["error_category"] == "g0_reconciliation_failed"


def test_validate_quarter_rejects_syntax_before_seed_or_connection(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(cli_module, "load_parent_seed", lambda: calls.append("seed"))
    monkeypatch.setattr(cli_module, "connect", lambda: calls.append("connect"))

    assert cli_module.main(["validate-quarter", "--quarter", "2024q1"]) == 1

    assert calls == []
    assert json.loads(capsys.readouterr().out) == {
        "command": "validate-quarter",
        "error_category": "invalid_quarter",
        "status": "error",
    }


def test_build_and_qa_refuse_missing_seed_before_any_artifact_read(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        cli_module, "load_parent_seed",
        lambda: (_ for _ in ()).throw(ValueError("licensed secret")),
    )
    monkeypatch.setattr(cli_module, "build_game_outputs", lambda **kwargs: calls.append("build"))
    monkeypatch.setattr(cli_module, "run_runtime_qa", lambda **kwargs: calls.append("qa"))
    assert cli_module.main(["build", "--game", "raw"]) == 1
    assert cli_module.main(["qa"]) == 1
    assert calls == []
    output = capsys.readouterr().out
    assert "secret" not in output


def test_build_and_qa_are_offline_and_propagate_qa_exit(monkeypatch, capsys):
    seed = ParentSeedSnapshot({"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3}, "seed")
    monkeypatch.setattr(cli_module, "load_parent_seed", lambda: seed)
    monkeypatch.setattr(cli_module, "load_description_identity", lambda: (_ for _ in ()).throw(AssertionError("offline command does not need schema")))
    monkeypatch.setattr(cli_module, "connect", lambda: (_ for _ in ()).throw(AssertionError("offline command")))
    monkeypatch.setattr(cli_module, "build_game_outputs", lambda **kwargs: {"game": kwargs["game"], "status": "built"})
    assert cli_module.main(["build", "--game", "finished"]) == 0
    monkeypatch.setattr(cli_module, "run_runtime_qa", lambda **kwargs: {"exit_code": 1, "supplier_game_eligible": False})
    assert cli_module.main(["qa"]) == 1
