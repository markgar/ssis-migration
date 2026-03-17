"""Cross-reference tests against real .dtsx packages that use variables heavily."""

from __future__ import annotations

from pathlib import Path

import pytest

from loader import load_package
from models import ParsedPackage, VariableReference

# ---------------------------------------------------------------------------
# Paths to sample packages
# ---------------------------------------------------------------------------

SAMPLE_PACKAGES_DIR = Path(__file__).resolve().parent / "fixtures"
DAILY_ETL_DTSX = SAMPLE_PACKAGES_DIR / "Daily.ETL" / "DailyETLMain.dtsx"
DIM_CUSTOMER_DTSX = SAMPLE_PACKAGES_DIR / "DWH-Loading" / "DimCustomer.dtsx"
FOR_LOOP_DTSX = SAMPLE_PACKAGES_DIR / "ControlFlowTraining" / "ForLoopContainerDemo_2.dtsx"
FOREACH_ITEM_DTSX = SAMPLE_PACKAGES_DIR / "ControlFlowTraining" / "ForeachItemEnumDemo.dtsx"
FOREACH_VAR_DTSX = SAMPLE_PACKAGES_DIR / "ControlFlowTraining" / "ForeachVariableEnumDemo.dtsx"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def daily_etl_pkg() -> ParsedPackage:
    assert DAILY_ETL_DTSX.exists(), f"Missing fixture: {DAILY_ETL_DTSX}"
    return load_package(DAILY_ETL_DTSX)


@pytest.fixture(scope="module")
def dim_customer_pkg() -> ParsedPackage:
    assert DIM_CUSTOMER_DTSX.exists(), f"Missing fixture: {DIM_CUSTOMER_DTSX}"
    return load_package(DIM_CUSTOMER_DTSX)


@pytest.fixture(scope="module")
def for_loop_pkg() -> ParsedPackage:
    assert FOR_LOOP_DTSX.exists(), f"Missing fixture: {FOR_LOOP_DTSX}"
    return load_package(FOR_LOOP_DTSX)


@pytest.fixture(scope="module")
def foreach_item_pkg() -> ParsedPackage:
    assert FOREACH_ITEM_DTSX.exists(), f"Missing fixture: {FOREACH_ITEM_DTSX}"
    return load_package(FOREACH_ITEM_DTSX)


@pytest.fixture(scope="module")
def foreach_var_pkg() -> ParsedPackage:
    assert FOREACH_VAR_DTSX.exists(), f"Missing fixture: {FOREACH_VAR_DTSX}"
    return load_package(FOREACH_VAR_DTSX)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _var_names(pkg: ParsedPackage) -> set[str]:
    """Return the set of variable names declared in the package."""
    return {v.name for v in pkg.variables}


def _ref_by_name(pkg: ParsedPackage, name: str) -> VariableReference | None:
    """Find a VariableReference whose variable_name contains *name*."""
    for ref in pkg.variable_references:
        if name in ref.variable_name:
            return ref
    return None


def _all_executables(execs):
    """Yield every Executable recursively."""
    for ex in execs:
        yield ex
        yield from _all_executables(ex.children)


# ===================================================================
# TestDailyETLVariables
# ===================================================================

class TestDailyETLVariables:
    """DailyETLMain.dtsx — 4 variables, used across many tasks."""

    def test_has_four_variables(self, daily_etl_pkg):
        assert len(daily_etl_pkg.variables) == 4

    def test_variable_references_not_empty(self, daily_etl_pkg):
        assert len(daily_etl_pkg.variable_references) > 0

    def test_specific_variables_have_consumers(self, daily_etl_pkg):
        """At least one declared variable should appear as consumed."""
        names = _var_names(daily_etl_pkg)
        consumed_vars = {
            ref.variable_name
            for ref in daily_etl_pkg.variable_references
            if ref.consumed_by
        }
        # At least one package variable should be consumed somewhere
        assert consumed_vars, "Expected at least one variable to have consumers"


# ===================================================================
# TestDimCustomerVariableRefs
# ===================================================================

class TestDimCustomerVariableRefs:
    """DimCustomer.dtsx — 2 variables: CurrentDate, LastSuccessfulModifiedDate."""

    def test_has_two_variables(self, dim_customer_pkg):
        assert len(dim_customer_pkg.variables) == 2

    def test_variable_names(self, dim_customer_pkg):
        names = _var_names(dim_customer_pkg)
        assert "CurrentDate" in names
        assert "LastSuccessfulModifiedDate" in names

    def test_variable_references_exist(self, dim_customer_pkg):
        assert len(dim_customer_pkg.variable_references) > 0

    def test_current_date_consumed_by_multiple_tasks(self, dim_customer_pkg):
        ref = _ref_by_name(dim_customer_pkg, "CurrentDate")
        assert ref is not None, "CurrentDate not found in variable_references"
        assert len(ref.consumed_by) > 1, (
            f"Expected CurrentDate to be consumed by multiple tasks, "
            f"got {ref.consumed_by}"
        )

    def test_last_successful_modified_date_consumed(self, dim_customer_pkg):
        ref = _ref_by_name(dim_customer_pkg, "LastSuccessfulModifiedDate")
        assert ref is not None, (
            "LastSuccessfulModifiedDate not found in variable_references"
        )
        assert len(ref.consumed_by) >= 1, (
            "Expected LastSuccessfulModifiedDate to be consumed by at least one task"
        )


# ===================================================================
# TestForLoopVariableRefs
# ===================================================================

class TestForLoopVariableRefs:
    """ForLoopContainerDemo_2.dtsx — ForLoop with init/eval/assign expressions."""

    def test_package_has_variables(self, for_loop_pkg):
        assert len(for_loop_pkg.variables) > 0

    def test_for_loop_executable_exists(self, for_loop_pkg):
        """The package should contain a ForLoop container."""
        all_exes = list(_all_executables(for_loop_pkg.executables))
        for_loops = [e for e in all_exes if e.for_loop is not None]
        assert len(for_loops) > 0, "Expected at least one ForLoop container"

    def test_for_loop_has_expressions(self, for_loop_pkg):
        """The ForLoop should have init, eval, or assign expressions."""
        all_exes = list(_all_executables(for_loop_pkg.executables))
        for_loops = [e for e in all_exes if e.for_loop is not None]
        assert for_loops, "No ForLoop found"
        fl = for_loops[0].for_loop
        has_expr = bool(fl.init_expression or fl.eval_expression or fl.assign_expression)
        assert has_expr, "ForLoop should have at least one expression"

    def test_variable_references_capture_for_loop(self, for_loop_pkg):
        """Variable references should capture variables used in ForLoop expressions."""
        assert len(for_loop_pkg.variable_references) > 0, (
            "Expected variable_references to be populated for ForLoop package"
        )


# ===================================================================
# TestForEachVariableRefs
# ===================================================================

class TestForEachVariableRefs:
    """ForeachItemEnumDemo.dtsx / ForeachVariableEnumDemo.dtsx — ForEach variable mappings."""

    def test_foreach_item_has_variables(self, foreach_item_pkg):
        assert len(foreach_item_pkg.variables) > 0

    def test_foreach_var_has_variables(self, foreach_var_pkg):
        assert len(foreach_var_pkg.variables) > 0

    def test_foreach_item_has_foreach_container(self, foreach_item_pkg):
        """ForeachItemEnumDemo should have a ForEach container."""
        all_exes = list(_all_executables(foreach_item_pkg.executables))
        foreach_containers = [e for e in all_exes if e.for_each is not None]
        assert len(foreach_containers) > 0, "Expected at least one ForEach container"

    def test_foreach_variable_mappings_create_references(self, foreach_item_pkg):
        """ForEach variable mappings should produce variable references."""
        all_exes = list(_all_executables(foreach_item_pkg.executables))
        foreach_containers = [e for e in all_exes if e.for_each is not None]
        assert foreach_containers, "No ForEach container found"

        # Check that at least one ForEach container has variable mappings
        has_mappings = any(
            len(e.for_each.variable_mappings) > 0
            for e in foreach_containers
        )
        assert has_mappings, "Expected ForEach container to have variable mappings"

        # Variable references should capture the mapped variables
        assert len(foreach_item_pkg.variable_references) > 0, (
            "Expected variable_references from ForEach variable mappings"
        )

    def test_foreach_var_enum_has_variable_references(self, foreach_var_pkg):
        """ForeachVariableEnumDemo should have variable references."""
        assert len(foreach_var_pkg.variable_references) > 0


# ===================================================================
# TestVariableReferenceStructure
# ===================================================================

class TestVariableReferenceStructure:
    """Verify the VariableReference dataclass structure on real data."""

    def test_variable_reference_has_non_empty_name(self, daily_etl_pkg):
        for ref in daily_etl_pkg.variable_references:
            assert ref.variable_name, "variable_name must be non-empty"

    def test_set_by_is_list(self, daily_etl_pkg):
        for ref in daily_etl_pkg.variable_references:
            assert isinstance(ref.set_by, list), (
                f"set_by should be a list, got {type(ref.set_by)}"
            )

    def test_consumed_by_is_list(self, daily_etl_pkg):
        for ref in daily_etl_pkg.variable_references:
            assert isinstance(ref.consumed_by, list), (
                f"consumed_by should be a list, got {type(ref.consumed_by)}"
            )

    def test_set_by_entries_are_strings(self, daily_etl_pkg):
        for ref in daily_etl_pkg.variable_references:
            for entry in ref.set_by:
                assert isinstance(entry, str)

    def test_consumed_by_entries_are_strings(self, daily_etl_pkg):
        for ref in daily_etl_pkg.variable_references:
            for entry in ref.consumed_by:
                assert isinstance(entry, str)

    def test_structure_on_dim_customer(self, dim_customer_pkg):
        """Same structural checks on a different package."""
        for ref in dim_customer_pkg.variable_references:
            assert ref.variable_name
            assert isinstance(ref.set_by, list)
            assert isinstance(ref.consumed_by, list)
