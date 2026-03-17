"""Topological sort and parallel branch detection for execution ordering."""

from __future__ import annotations

import sys
from collections import deque

from models import Executable, ParallelBranch, PrecedenceConstraint


def topological_sort(
    children: list[Executable],
    constraints: list[PrecedenceConstraint],
) -> list[str]:
    """Return task names in topological order using Kahn's algorithm.

    Parameters
    ----------
    children:
        Executable objects within the current scope.
    constraints:
        Precedence constraints (already resolved to human-readable names).

    Returns
    -------
    list[str]
        Task names ordered so that no task appears before its predecessors.
        The sorted queue is used at each step for deterministic output.
        If a cycle is detected, remaining tasks are appended with a warning.
    """
    task_names = [exe.name for exe in children]
    if not task_names:
        return []

    task_set = set(task_names)

    # Build adjacency list and in-degree count
    adjacency: dict[str, list[str]] = {name: [] for name in task_names}
    in_degree: dict[str, int] = {name: 0 for name in task_names}

    for c in constraints:
        if c.from_task in task_set and c.to_task in task_set:
            adjacency[c.from_task].append(c.to_task)
            in_degree[c.to_task] += 1

    # BFS from zero-in-degree nodes, sorted for determinism
    queue = deque(sorted(name for name in task_names if in_degree[name] == 0))
    result: list[str] = []

    while queue:
        current = queue.popleft()
        result.append(current)
        for successor in adjacency[current]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                # Insert into sorted position for determinism
                _sorted_insert(queue, successor)

    if len(result) < len(task_names):
        remaining = [name for name in task_names if name not in set(result)]
        print(
            f"Warning: cycle detected among tasks {remaining!r}, "
            f"appending in original order",
            file=sys.stderr,
        )
        result.extend(remaining)

    return result


def _sorted_insert(queue: deque[str], item: str) -> None:
    """Insert *item* into *queue* maintaining sorted order."""
    pos = len(queue)
    for i, existing in enumerate(queue):
        if item < existing:
            pos = i
            break
    queue.insert(pos, item)


def detect_parallel_branches(
    children: list[Executable],
    constraints: list[PrecedenceConstraint],
) -> list[ParallelBranch]:
    """Identify groups of tasks that can execute concurrently.

    Tasks that share the same single predecessor with no mutual dependency
    form a parallel branch.

    Returns
    -------
    list[ParallelBranch]
        Sorted by shared predecessor name for deterministic output.
    """
    if not children or not constraints:
        return []

    task_set = {exe.name for exe in children}

    # Build predecessor map: task → set of predecessors
    predecessors: dict[str, set[str]] = {exe.name: set() for exe in children}

    for c in constraints:
        if c.from_task in task_set and c.to_task in task_set:
            predecessors[c.to_task].add(c.from_task)

    # Group tasks by their sole predecessor
    pred_groups: dict[str, list[str]] = {}
    for task_name, preds in predecessors.items():
        if len(preds) == 1:
            pred = next(iter(preds))
            pred_groups.setdefault(pred, []).append(task_name)

    # Filter to groups with 2+ tasks that have no mutual dependency
    branches: list[ParallelBranch] = []
    for pred, group_tasks in sorted(pred_groups.items()):
        if len(group_tasks) < 2:
            continue

        # Check for mutual dependencies within the group
        parallel_tasks: list[str] = []
        for t in group_tasks:
            has_mutual = False
            for other in group_tasks:
                if other == t:
                    continue
                if other in predecessors[t] or t in predecessors[other]:
                    has_mutual = True
                    break
            if not has_mutual:
                parallel_tasks.append(t)

        if len(parallel_tasks) >= 2:
            branches.append(ParallelBranch(
                tasks=sorted(parallel_tasks),
                shared_predecessor=pred,
            ))

    return branches
