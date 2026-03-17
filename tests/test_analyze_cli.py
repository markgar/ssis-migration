"""CLI integration tests for analyze.py.

Runs each command via subprocess against the real DailyETLMain.dtsx fixture.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# Paths relative to this test file
_TEST_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _TEST_DIR.parent
_ANALYZE_SCRIPT = _ROOT_DIR / "skills" / "ssis-analyzer" / "scripts" / "analyze.py"
_DTSX_FILE = _ROOT_DIR / "tests" / "fixtures" / "Daily.ETL" / "DailyETLMain.dtsx"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """Run analyze.py with the given arguments."""
    return subprocess.run(
        [sys.executable, str(_ANALYZE_SCRIPT), *args],
        capture_output=True,
        text=True,
    )


def _assert_success(result: subprocess.CompletedProcess[str]) -> None:
    """Assert command succeeded with non-empty output and no tracebacks."""
    assert result.returncode == 0, (
        f"Command failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
    )
    assert result.stdout.strip(), "Expected non-empty stdout"
    # Filter out expected package-loading warnings before checking for tracebacks
    stderr_lines = [
        line for line in result.stderr.splitlines()
        if not line.startswith("Warning:")
    ]
    filtered_stderr = "\n".join(stderr_lines)
    assert "Traceback" not in filtered_stderr, (
        f"Unexpected traceback in stderr:\n{filtered_stderr[:500]}"
    )


# ── Commands that load the .dtsx ─────────────────────────────────────────


class TestOverview:
    def test_overview(self):
        result = _run(str(_DTSX_FILE), "overview")
        _assert_success(result)


class TestExecutionOrder:
    def test_execution_order(self):
        result = _run(str(_DTSX_FILE), "execution-order")
        _assert_success(result)


class TestListConnections:
    def test_list_connections(self):
        result = _run(str(_DTSX_FILE), "list-connections")
        _assert_success(result)


class TestConnectionDetail:
    def test_connection_detail(self):
        result = _run(str(_DTSX_FILE), "connection-detail", "WWI_Source_DB")
        _assert_success(result)


class TestListTasks:
    def test_list_tasks(self):
        result = _run(str(_DTSX_FILE), "list-tasks")
        _assert_success(result)


class TestTaskDetail:
    def test_task_detail(self):
        result = _run(str(_DTSX_FILE), "task-detail", "Load City Dimension")
        _assert_success(result)


class TestListConstraints:
    def test_list_constraints(self):
        result = _run(str(_DTSX_FILE), "list-constraints")
        _assert_success(result)


class TestListDataFlows:
    def test_list_data_flows(self):
        result = _run(str(_DTSX_FILE), "list-data-flows")
        _assert_success(result)


class TestDataFlowDetail:
    def test_data_flow_detail(self):
        result = _run(
            str(_DTSX_FILE),
            "data-flow-detail",
            "Extract Updated City Data to Staging",
        )
        _assert_success(result)


class TestComponentDetail:
    def test_component_detail(self):
        result = _run(
            str(_DTSX_FILE),
            "component-detail",
            "Extract Updated City Data to Staging",
            "Integration_GetCityUpdates",
        )
        _assert_success(result)


class TestColumnLineage:
    def test_column_lineage(self):
        result = _run(
            str(_DTSX_FILE),
            "column-lineage",
            "Extract Updated City Data to Staging",
        )
        _assert_success(result)


class TestListVariables:
    def test_list_variables(self):
        result = _run(str(_DTSX_FILE), "list-variables")
        _assert_success(result)


class TestListParameters:
    def test_list_parameters(self):
        result = _run(str(_DTSX_FILE), "list-parameters")
        _assert_success(result)


class TestVariableRefs:
    def test_variable_refs(self):
        result = _run(str(_DTSX_FILE), "variable-refs")
        _assert_success(result)


class TestExtractSql:
    def test_extract_sql(self):
        result = _run(str(_DTSX_FILE), "extract-sql")
        _assert_success(result)


class TestExtractScripts:
    def test_extract_scripts(self):
        result = _run(str(_DTSX_FILE), "extract-scripts")
        _assert_success(result)


class TestFind:
    def test_find(self):
        result = _run(str(_DTSX_FILE), "find", "City")
        _assert_success(result)


# ── Commands that don't need a real .dtsx ────────────────────────────────


class TestExplain:
    def test_explain(self):
        result = _run("_", "explain", "OLE DB Source")
        _assert_success(result)


class TestListKnownComponents:
    def test_list_known_components(self):
        result = _run("_", "list-known-components")
        _assert_success(result)


# ── Error cases ──────────────────────────────────────────────────────────


class TestErrors:
    def test_missing_file(self):
        result = _run("/nonexistent/path.dtsx", "overview")
        assert result.returncode != 0

    def test_invalid_command(self):
        result = _run(str(_DTSX_FILE), "not-a-real-command")
        assert result.returncode != 0

    def test_non_dtsx_file(self):
        result = _run(str(_ANALYZE_SCRIPT), "overview")
        assert result.returncode != 0

    def test_no_arguments(self):
        result = _run()
        assert result.returncode != 0
