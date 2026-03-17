"""Tests for ordering.py — topological sort and parallel branch detection."""

from models import Executable, ParallelBranch, PrecedenceConstraint
from ordering import detect_parallel_branches, topological_sort


def _exe(name: str) -> Executable:
    """Helper to create a minimal Executable."""
    return Executable(name=name, creation_name="Stub")


def _pc(from_task: str, to_task: str) -> PrecedenceConstraint:
    """Helper to create a minimal PrecedenceConstraint."""
    return PrecedenceConstraint(from_task=from_task, to_task=to_task, value="Success", eval_op="Constraint")


# ---------------------------------------------------------------------------
# topological_sort
# ---------------------------------------------------------------------------

class TestTopologicalSort:
    def test_linear_chain(self):
        children = [_exe("A"), _exe("B"), _exe("C")]
        constraints = [_pc("A", "B"), _pc("B", "C")]
        result = topological_sort(children, constraints)
        assert result == ["A", "B", "C"]

    def test_diamond(self):
        children = [_exe("A"), _exe("B"), _exe("C"), _exe("D")]
        constraints = [_pc("A", "B"), _pc("A", "C"), _pc("B", "D"), _pc("C", "D")]
        result = topological_sort(children, constraints)
        assert result.index("A") < result.index("B")
        assert result.index("A") < result.index("C")
        assert result.index("B") < result.index("D")
        assert result.index("C") < result.index("D")

    def test_no_constraints_returns_all(self):
        children = [_exe("Z"), _exe("A"), _exe("M")]
        result = topological_sort(children, [])
        assert set(result) == {"Z", "A", "M"}
        assert len(result) == 3
        # With no constraints, sorted alphabetically
        assert result == ["A", "M", "Z"]

    def test_single_task(self):
        children = [_exe("Only")]
        result = topological_sort(children, [])
        assert result == ["Only"]

    def test_empty_input(self):
        assert topological_sort([], []) == []

    def test_ignores_constraints_referencing_unknown_tasks(self):
        children = [_exe("A"), _exe("B")]
        constraints = [_pc("A", "B"), _pc("X", "Y")]
        result = topological_sort(children, constraints)
        assert result == ["A", "B"]


# ---------------------------------------------------------------------------
# detect_parallel_branches
# ---------------------------------------------------------------------------

class TestDetectParallelBranches:
    def test_basic_parallel(self):
        children = [_exe("A"), _exe("B"), _exe("C")]
        constraints = [_pc("A", "B"), _pc("A", "C")]
        branches = detect_parallel_branches(children, constraints)
        assert len(branches) == 1
        assert branches[0].shared_predecessor == "A"
        assert sorted(branches[0].tasks) == ["B", "C"]

    def test_no_parallel_when_sequential(self):
        children = [_exe("A"), _exe("B"), _exe("C")]
        constraints = [_pc("A", "B"), _pc("B", "C")]
        branches = detect_parallel_branches(children, constraints)
        assert branches == []

    def test_no_constraints_returns_empty(self):
        children = [_exe("A"), _exe("B")]
        branches = detect_parallel_branches(children, [])
        assert branches == []

    def test_empty_children(self):
        branches = detect_parallel_branches([], [_pc("A", "B")])
        assert branches == []

    def test_diamond_has_parallel_at_top(self):
        children = [_exe("A"), _exe("B"), _exe("C"), _exe("D")]
        constraints = [_pc("A", "B"), _pc("A", "C"), _pc("B", "D"), _pc("C", "D")]
        branches = detect_parallel_branches(children, constraints)
        assert len(branches) == 1
        assert branches[0].shared_predecessor == "A"
        assert sorted(branches[0].tasks) == ["B", "C"]

    def test_multiple_parallel_groups(self):
        children = [_exe("S"), _exe("A"), _exe("B"), _exe("T"), _exe("X"), _exe("Y")]
        constraints = [
            _pc("S", "A"), _pc("S", "B"),
            _pc("T", "X"), _pc("T", "Y"),
        ]
        branches = detect_parallel_branches(children, constraints)
        assert len(branches) == 2
        preds = {b.shared_predecessor for b in branches}
        assert preds == {"S", "T"}
