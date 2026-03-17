"""Integration tests — load DailyETLMain.dtsx through the full pipeline."""

from __future__ import annotations

import pytest

from models import ParsedPackage
from ordering import topological_sort


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_executables(execs):
    """Yield every Executable recursively."""
    for ex in execs:
        yield ex
        yield from _all_executables(ex.children)


# ===================================================================
# Package loading
# ===================================================================

class TestPackageLoading:
    def test_load_returns_parsed_package(self, loaded_package):
        assert isinstance(loaded_package, ParsedPackage)

    def test_metadata_name(self, loaded_package):
        assert loaded_package.metadata.name == "DailyETLMain"

    def test_format_version(self, loaded_package):
        assert loaded_package.format_version == 8

    def test_deployment_model(self, loaded_package):
        assert loaded_package.deployment_model == "Project"


# ===================================================================
# Connections
# ===================================================================

class TestConnections:
    def test_connection_count(self, loaded_package):
        assert len(loaded_package.connections) == 2

    def test_connection_names(self, loaded_package):
        names = {c.name for c in loaded_package.connections}
        assert names == {"WWI_DW_Destination_DB", "WWI_Source_DB"}

    def test_connections_are_project_level(self, loaded_package):
        assert all(c.is_project_level for c in loaded_package.connections)

    def test_connection_types(self, loaded_package):
        assert all(c.creation_name == "OLEDB" for c in loaded_package.connections)


# ===================================================================
# Variables
# ===================================================================

class TestVariables:
    def test_variable_count(self, loaded_package):
        assert len(loaded_package.variables) == 4


# ===================================================================
# Executables
# ===================================================================

class TestExecutables:
    def test_top_level_count(self, loaded_package):
        assert len(loaded_package.executables) == 16

    def test_sequence_containers(self, loaded_package):
        containers = [
            ex for ex in loaded_package.executables if len(ex.children) > 0
        ]
        assert len(containers) == 13

    def test_nested_children(self, loaded_package):
        containers = [
            ex for ex in loaded_package.executables if len(ex.children) > 0
        ]
        for container in containers:
            assert len(container.children) == 6, (
                f"{container.name} has {len(container.children)} children, expected 6"
            )

    def test_data_flow_count(self, loaded_package):
        data_flows = [
            ex
            for ex in _all_executables(loaded_package.executables)
            if ex.data_flow is not None
        ]
        assert len(data_flows) == 13


# ===================================================================
# Execution order (topological sort)
# ===================================================================

class TestExecutionOrder:
    def test_order_length(self, loaded_package):
        order = topological_sort(
            loaded_package.executables,
            loaded_package.root_precedence_constraints,
        )
        assert len(order) == 16

    def test_first_tasks(self, loaded_package):
        order = topological_sort(
            loaded_package.executables,
            loaded_package.root_precedence_constraints,
        )
        early_tasks = set(order[:2])
        assert early_tasks == {
            "Calculate ETL Cutoff Time backup",
            "Trim Any Milliseconds",
        }

    def test_dimensions_before_facts(self, loaded_package):
        order = topological_sort(
            loaded_package.executables,
            loaded_package.root_precedence_constraints,
        )
        dimension_keywords = {"City", "Customer", "Employee", "Payment Method",
                              "Stock Item", "Supplier", "Transaction Type"}
        fact_keywords = {"Movement", "Order", "Purchase", "Sale",
                         "Stock Holding", "Transaction"}

        dim_indices = [
            i for i, name in enumerate(order)
            if any(kw in name for kw in dimension_keywords)
        ]
        fact_indices = [
            i for i, name in enumerate(order)
            if any(kw in name for kw in fact_keywords)
            and not any(kw in name for kw in dimension_keywords)
        ]

        assert dim_indices, "No dimension tasks found in order"
        assert fact_indices, "No fact tasks found in order"
        assert max(dim_indices) < min(fact_indices), (
            "All dimensions should appear before the first fact load"
        )


# ===================================================================
# Data flows
# ===================================================================

class TestDataFlows:
    def test_data_flow_components(self, loaded_package):
        """Each data flow has exactly 2 components (source + destination)."""
        for ex in _all_executables(loaded_package.executables):
            if ex.data_flow is not None:
                assert len(ex.data_flow.components) == 2, (
                    f"{ex.name} has {len(ex.data_flow.components)} components, expected 2"
                )

    def test_data_flow_paths(self, loaded_package):
        """Each data flow has exactly 1 path (source → destination)."""
        for ex in _all_executables(loaded_package.executables):
            if ex.data_flow is not None:
                assert len(ex.data_flow.paths) == 1, (
                    f"{ex.name} has {len(ex.data_flow.paths)} paths, expected 1"
                )
