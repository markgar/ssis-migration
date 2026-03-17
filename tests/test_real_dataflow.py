"""Tests for real .dtsx data flow extraction — components, paths, columns, SQL, lineage."""

from __future__ import annotations

from pathlib import Path

import pytest

from loader import load_package
from models import (
    Component,
    ComponentConnection,
    ComponentOutput,
    DataFlow,
    DataFlowPath,
    OutputColumn,
    ParsedPackage,
)

# ---------------------------------------------------------------------------
# Paths (relative to this test file)
# ---------------------------------------------------------------------------
SAMPLES_DIR = Path(__file__).resolve().parent / "fixtures"
DWH_DIR = SAMPLES_DIR / "DWH-Loading"
DAILY_ETL_DIR = SAMPLES_DIR / "Daily.ETL"
CONTROL_FLOW_DIR = SAMPLES_DIR / "ControlFlowTraining"

DIM_CUSTOMER_DTSX = DWH_DIR / "DimCustomer.dtsx"
FACT_INTERNET_SALES_DTSX = DWH_DIR / "Fact_InternetSales.dtsx"
DAILY_ETL_DTSX = DAILY_ETL_DIR / "DailyETLMain.dtsx"
FOREACH_DEMO_DTSX = CONTROL_FLOW_DIR / "ForeachLoopFileEnumDemo.dtsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_executables(execs):
    """Yield every Executable recursively (depth-first)."""
    for ex in execs:
        yield ex
        yield from _all_executables(ex.children)


def _data_flow_executables(pkg: ParsedPackage):
    """Return list of Executables that carry a DataFlow."""
    return [
        ex for ex in _all_executables(pkg.executables)
        if ex.data_flow is not None
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dim_customer_pkg() -> ParsedPackage:
    assert DIM_CUSTOMER_DTSX.exists(), f"Missing fixture: {DIM_CUSTOMER_DTSX}"
    return load_package(DIM_CUSTOMER_DTSX)


@pytest.fixture(scope="module")
def fact_internet_sales_pkg() -> ParsedPackage:
    assert FACT_INTERNET_SALES_DTSX.exists(), f"Missing fixture: {FACT_INTERNET_SALES_DTSX}"
    return load_package(FACT_INTERNET_SALES_DTSX)


@pytest.fixture(scope="module")
def daily_etl_pkg() -> ParsedPackage:
    assert DAILY_ETL_DTSX.exists(), f"Missing fixture: {DAILY_ETL_DTSX}"
    return load_package(DAILY_ETL_DTSX)


@pytest.fixture(scope="module")
def foreach_demo_pkg() -> ParsedPackage:
    assert FOREACH_DEMO_DTSX.exists(), f"Missing fixture: {FOREACH_DEMO_DTSX}"
    return load_package(FOREACH_DEMO_DTSX)


# ===================================================================
# TestDimCustomerDataFlow
# ===================================================================

class TestDimCustomerDataFlow:
    """DimCustomer.dtsx — simple source → transform → destination pipeline."""

    def test_has_one_data_flow(self, dim_customer_pkg):
        dfs = _data_flow_executables(dim_customer_pkg)
        assert len(dfs) == 1

    def test_three_components(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        assert len(df.components) == 3

    def test_component_class_names(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        class_names = {c.class_name for c in df.components}
        assert "OLE DB Source" in class_names
        assert "Derived Column" in class_names
        assert "OLE DB Destination" in class_names

    def test_two_paths(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        assert len(df.paths) == 2

    def test_paths_connect_components(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        comp_names = {c.name for c in df.components}
        for path in df.paths:
            assert path.start_component in comp_names
            assert path.end_component in comp_names

    def test_source_has_output_columns(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        source = next(c for c in df.components if c.class_name == "OLE DB Source")
        non_error_outputs = [o for o in source.outputs if not o.is_error_output]
        assert len(non_error_outputs) >= 1
        columns = non_error_outputs[0].columns
        assert len(columns) > 0

    def test_source_column_names(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        source = next(c for c in df.components if c.class_name == "OLE DB Source")
        non_error_outputs = [o for o in source.outputs if not o.is_error_output]
        col_names = {col.name for col in non_error_outputs[0].columns}
        # Known columns from DimCustomer source query
        for expected in ("CustomerAK", "FirstName", "LastName"):
            assert expected in col_names, f"Expected column '{expected}' in source output"


# ===================================================================
# TestFactInternetSalesDataFlow
# ===================================================================

class TestFactInternetSalesDataFlow:
    """Fact_InternetSales.dtsx — complex pipeline with lookups."""

    def test_has_one_data_flow(self, fact_internet_sales_pkg):
        dfs = _data_flow_executables(fact_internet_sales_pkg)
        assert len(dfs) == 1

    def test_ten_components(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        assert len(df.components) == 10

    def test_nine_paths(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        assert len(df.paths) == 9

    def test_has_lookup_components(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        lookups = [c for c in df.components if "Lookup" in c.class_name]
        assert len(lookups) >= 1, "Expected at least one Lookup component"

    def test_components_have_connections(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        components_with_conns = [
            c for c in df.components if len(c.connections) > 0
        ]
        assert len(components_with_conns) >= 2
        for comp in components_with_conns:
            for conn in comp.connections:
                assert isinstance(conn, ComponentConnection)
                assert conn.connection_manager_name != ""

    def test_components_have_sql_properties(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        sql_components = [
            c for c in df.components
            if "SqlCommand" in c.properties and c.properties["SqlCommand"].strip()
        ]
        assert len(sql_components) >= 1, "Expected components with non-empty SqlCommand"


# ===================================================================
# TestDailyETLDataFlows
# ===================================================================

class TestDailyETLDataFlows:
    """DailyETLMain.dtsx — 13 data flows nested in sequence containers."""

    def test_thirteen_data_flows(self, daily_etl_pkg):
        dfs = _data_flow_executables(daily_etl_pkg)
        assert len(dfs) == 13

    def test_each_has_two_components(self, daily_etl_pkg):
        for ex in _data_flow_executables(daily_etl_pkg):
            assert len(ex.data_flow.components) == 2, (
                f"Data flow '{ex.name}' has {len(ex.data_flow.components)} "
                f"components, expected 2"
            )

    def test_each_has_one_path(self, daily_etl_pkg):
        for ex in _data_flow_executables(daily_etl_pkg):
            assert len(ex.data_flow.paths) == 1, (
                f"Data flow '{ex.name}' has {len(ex.data_flow.paths)} paths, "
                f"expected 1"
            )

    def test_specific_data_flow_exists(self, daily_etl_pkg):
        df_names = {ex.name for ex in _data_flow_executables(daily_etl_pkg)}
        # At least one data flow should reference "City" or "Customer" extraction
        matches = [n for n in df_names if "City" in n or "Customer" in n]
        assert len(matches) >= 1, (
            f"Expected a data flow referencing City or Customer. Found: {df_names}"
        )


# ===================================================================
# TestNestedDataFlow
# ===================================================================

class TestNestedDataFlow:
    """ForeachLoopFileEnumDemo.dtsx — data flow inside a ForEach loop."""

    def test_data_flow_exists_inside_foreach(self, foreach_demo_pkg):
        dfs = _data_flow_executables(foreach_demo_pkg)
        assert len(dfs) >= 1, "Expected at least one data flow in the package"

    def test_data_flow_is_nested(self, foreach_demo_pkg):
        """Data flow should be found by walking children, not at the top level."""
        top_level_dfs = [
            ex for ex in foreach_demo_pkg.executables
            if ex.data_flow is not None
        ]
        # The data flow is nested inside a ForEach loop, not at top level
        all_dfs = _data_flow_executables(foreach_demo_pkg)
        assert len(all_dfs) > len(top_level_dfs), (
            "Data flow should be nested inside a container, not at top level"
        )

    def test_foreach_container_has_children(self, foreach_demo_pkg):
        foreach_containers = [
            ex for ex in _all_executables(foreach_demo_pkg.executables)
            if ex.for_each is not None
        ]
        assert len(foreach_containers) >= 1, "Expected a ForEach loop container"
        container = foreach_containers[0]
        assert len(container.children) >= 1, "ForEach container should have children"

    def test_nested_data_flow_has_components(self, foreach_demo_pkg):
        dfs = _data_flow_executables(foreach_demo_pkg)
        for ex in dfs:
            assert len(ex.data_flow.components) >= 1


# ===================================================================
# TestDataFlowColumnLineage
# ===================================================================

class TestDataFlowColumnLineage:
    """Column lineage tracking in DimCustomer.dtsx."""

    def test_output_columns_have_lineage_ids(self, dim_customer_pkg):
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        for comp in df.components:
            for output in comp.outputs:
                if output.is_error_output:
                    continue
                for col in output.columns:
                    assert col.lineage_id is not None, (
                        f"Column '{col.name}' on '{comp.name}' has no lineage_id"
                    )

    def test_lineage_map_covers_source_columns(self, dim_customer_pkg):
        """Build a lineage map and verify all source output columns are present."""
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow
        lineage_map: dict[int | str, tuple[str, str]] = {}
        for comp in df.components:
            for output in comp.outputs:
                if output.is_error_output:
                    continue
                for col in output.columns:
                    lineage_map[col.lineage_id] = (comp.name, col.name)

        source = next(c for c in df.components if c.class_name == "OLE DB Source")
        non_error_outputs = [o for o in source.outputs if not o.is_error_output]
        for col in non_error_outputs[0].columns:
            assert col.lineage_id in lineage_map

    def test_input_columns_reference_valid_lineage(self, dim_customer_pkg):
        """Input columns should reference lineage IDs produced by upstream outputs."""
        df = _data_flow_executables(dim_customer_pkg)[0].data_flow

        # Build set of all output lineage IDs
        all_lineage_ids: set[int | str] = set()
        for comp in df.components:
            for output in comp.outputs:
                for col in output.columns:
                    all_lineage_ids.add(col.lineage_id)

        # Every input column's lineage_id should reference a known output
        for comp in df.components:
            for inp in comp.inputs:
                for col in inp.columns:
                    assert col.lineage_id in all_lineage_ids, (
                        f"Input column '{col.name}' on '{comp.name}' references "
                        f"unknown lineage_id {col.lineage_id}"
                    )


# ===================================================================
# TestDataFlowSqlExtraction
# ===================================================================

class TestDataFlowSqlExtraction:
    """SQL extraction from Fact_InternetSales.dtsx component properties."""

    def test_sql_components_exist(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        sql_components = [
            c for c in df.components if "SqlCommand" in c.properties
        ]
        assert len(sql_components) >= 1

    def test_sql_is_non_empty(self, fact_internet_sales_pkg):
        """Components with SqlCommand that are sources/lookups have non-empty SQL."""
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        sql_components = [
            c for c in df.components
            if "SqlCommand" in c.properties and c.properties["SqlCommand"].strip()
        ]
        assert len(sql_components) >= 1, "Expected at least one component with SQL"
        for comp in sql_components:
            assert comp.properties["SqlCommand"].strip() != ""

    def test_source_has_select_statement(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        source = next(c for c in df.components if c.class_name == "OLE DB Source")
        assert "SqlCommand" in source.properties
        sql = source.properties["SqlCommand"].lower()
        assert "select" in sql, "OLE DB Source should have a SELECT query"

    def test_lookup_sql_references_dimension(self, fact_internet_sales_pkg):
        df = _data_flow_executables(fact_internet_sales_pkg)[0].data_flow
        lookups = [c for c in df.components if "Lookup" in c.class_name]
        assert len(lookups) >= 1
        # Each lookup should have SQL referencing a dimension table
        for lkp in lookups:
            assert "SqlCommand" in lkp.properties, (
                f"Lookup '{lkp.name}' missing SqlCommand"
            )
            sql = lkp.properties["SqlCommand"].lower()
            assert "select" in sql, (
                f"Lookup '{lkp.name}' SqlCommand should contain SELECT"
            )
