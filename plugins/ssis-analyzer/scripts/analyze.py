#!/usr/bin/env python3
"""CLI entry point for SSIS package analysis.

Usage:
    python analyze.py <dtsx_path> <command> [args...]

Commands:
    overview                    Package metadata and summary statistics
    execution-order             Topological execution order and parallel branches
    list-connections            All connection managers
    connection-detail <name>    Full details for a specific connection
    list-tasks                  All tasks/containers in the control flow
    task-detail <name>          Full details for a specific task
    list-constraints            Root-level precedence constraints
    list-data-flows             All Data Flow tasks
    data-flow-detail <name>     Components, paths, and settings for a Data Flow
    component-detail <flow> <component>  Full details for a Data Flow component
    column-lineage <flow>       Column lineage through a Data Flow
    list-variables              All package variables
    list-parameters             All package parameters
    variable-refs [name]        Variable cross-references (writers/readers)
    extract-sql                 All SQL from tasks and components
    extract-scripts             All script code (C#/VB)
    find <term>                 Search across all named objects
    explain <component>         Knowledge base entry for a component type
    list-known-components       All known component types by category
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

# Ensure scripts/ is on the path so imports resolve
sys.path.insert(0, str(Path(__file__).parent))

from cross_reference import build_variable_references
from knowledge import format_knowledge, lookup_component, list_categories
from loader import load_package
from lookups import resolve_ole_variant_type
from models import Executable, ParsedPackage
from ordering import detect_parallel_branches, topological_sort


def _require_package(pkg: ParsedPackage | None) -> ParsedPackage:
    if pkg is None:
        print("ERROR: No package loaded.", file=sys.stderr)
        sys.exit(1)
    return pkg


def cmd_overview(pkg: ParsedPackage) -> None:
    m = pkg.metadata
    print(f"# Package: {m.name}")
    print(f"- Format Version: {m.format_version}")
    print(f"- Creator: {m.creator or 'N/A'}")
    print(f"- Description: {m.description or 'N/A'}")
    print(f"- Protection Level: {m.protection_level or 'N/A'}")
    print(f"- Deployment Model: {pkg.deployment_model or 'Package'}")
    print()
    print("## Summary")
    print(f"- Connections: {len(pkg.connections)}")
    print(f"- Variables: {len(pkg.variables)}")
    print(f"- Parameters: {len(pkg.parameters)}")
    print(f"- Tasks/Containers: {len(pkg.executables)}")
    df_count = sum(1 for e in _all_executables(pkg) if e.data_flow is not None)
    print(f"- Data Flows: {df_count}")


def cmd_execution_order(pkg: ParsedPackage) -> None:
    order = topological_sort(pkg.executables, pkg.root_precedence_constraints)
    print("# Execution Order")
    for i, name in enumerate(order, 1):
        print(f"{i}. {name}")

    branches = detect_parallel_branches(pkg.executables, pkg.root_precedence_constraints)
    if branches:
        print("\n## Parallel Branches")
        for branch in branches:
            print(f"- [{', '.join(branch.tasks)}]")


def cmd_list_connections(pkg: ParsedPackage) -> None:
    print("# Connections")
    print(f"| Name | Type | Project-Level |")
    print(f"|------|------|---------------|")
    for c in pkg.connections:
        print(f"| {c.name} | {c.creation_name or 'N/A'} | {c.is_project_level} |")


def cmd_connection_detail(pkg: ParsedPackage, name: str) -> None:
    conn = next((c for c in pkg.connections if c.name.lower() == name.lower()), None)
    if not conn:
        print(f"Connection '{name}' not found.", file=sys.stderr)
        return
    print(f"# Connection: {conn.name}")
    print(f"- Type: {conn.creation_name or 'N/A'}")
    print(f"- Connection String: {conn.connection_string or 'N/A'}")
    if conn.property_expressions:
        print("- Property Expressions:")
        for k, v in conn.property_expressions.items():
            print(f"  - {k}: {v}")
    if conn.flat_file_columns:
        print(f"- Flat File Columns ({len(conn.flat_file_columns)}):")
        for col in conn.flat_file_columns:
            print(f"  - {col.name} ({col.data_type}, width={col.width})")


def _all_executables(pkg: ParsedPackage) -> list[Executable]:
    result = []
    def walk(exes: list[Executable]):
        for e in exes:
            result.append(e)
            if e.children:
                walk(e.children)
    walk(pkg.executables)
    return result


def cmd_list_tasks(pkg: ParsedPackage) -> None:
    print("# Tasks")
    def print_tree(exes: list[Executable], indent: int = 0):
        for e in exes:
            prefix = "  " * indent
            print(f"{prefix}- {e.name} [{e.creation_name}]")
            if e.children:
                print_tree(e.children, indent + 1)
    print_tree(pkg.executables)


def cmd_task_detail(pkg: ParsedPackage, name: str) -> None:
    all_exes = _all_executables(pkg)
    task = next((e for e in all_exes if e.name.lower() == name.lower()), None)
    if not task:
        print(f"Task '{name}' not found.", file=sys.stderr)
        return
    print(f"# Task: {task.name}")
    print(f"- Type: {task.creation_name}")
    print(f"- Creation Name: {task.creation_name or 'N/A'}")
    if task.description:
        print(f"- Description: {task.description}")
    if task.execute_sql:
        sql = task.execute_sql
        print(f"- Connection: {sql.connection or 'N/A'}")
        print(f"- SQL:\n```sql\n{sql.sql_statement or 'N/A'}\n```")
    if task.script_task:
        st = task.script_task
        print(f"- Script Language: {st.script_language or 'N/A'}")
        print(f"- Entry Point: {st.entry_point or 'N/A'}")
        if st.source_code:
            for fname, code in st.source_code.items():
                print(f"- Source ({fname}):\n```csharp\n{code}\n```")
    if task.for_each:
        fe = task.for_each
        print(f"- Enumerator Type: {fe.enumerator_type or 'N/A'}")
    if task.for_loop:
        fl = task.for_loop
        print(f"- Init: {fl.init_expression or 'N/A'}")
        print(f"- Eval: {fl.eval_expression or 'N/A'}")
        print(f"- Assign: {fl.assign_expression or 'N/A'}")
    if task.expression_task_expression:
        print(f"- Expression:\n```\n{task.expression_task_expression}\n```")
    if task.children:
        child_order = topological_sort(task.children, task.precedence_constraints)
        print(f"- Children ({len(task.children)}):")
        for i, child_name in enumerate(child_order, 1):
            print(f"  {i}. {child_name}")
        if task.precedence_constraints:
            print(f"- Inner Constraints:")
            for pc in task.precedence_constraints:
                expr = f" [{pc.expression}]" if pc.expression else ""
                print(f"  - {pc.from_task} → {pc.to_task} ({pc.value}{expr})")
    if task.variables:
        print(f"- Local Variables: {len(task.variables)}")
        for v in task.variables:
            print(f"  - {v.namespace}::{v.name} = {v.value}")


def cmd_list_constraints(pkg: ParsedPackage) -> None:
    print("# Precedence Constraints")
    for pc in pkg.root_precedence_constraints:
        expr = f" [{pc.expression}]" if pc.expression else ""
        print(f"- {pc.from_task} → {pc.to_task} ({pc.value}{expr})")


def cmd_list_data_flows(pkg: ParsedPackage) -> None:
    print("# Data Flows")
    all_exes = _all_executables(pkg)
    for e in all_exes:
        if e.data_flow:
            df = e.data_flow
            print(f"- **{e.name}**: {len(df.components)} components, {len(df.paths)} paths")


def cmd_data_flow_detail(pkg: ParsedPackage, name: str) -> None:
    all_exes = _all_executables(pkg)
    task = next((e for e in all_exes if e.name.lower() == name.lower() and e.data_flow), None)
    if not task:
        print(f"Data Flow '{name}' not found.", file=sys.stderr)
        return
    df = task.data_flow
    print(f"# Data Flow: {task.name}")
    print(f"\n## Components ({len(df.components)})")
    for c in df.components:
        print(f"- {c.name} [{c.class_name or c.class_id}]")
    print(f"\n## Paths ({len(df.paths)})")
    for p in df.paths:
        print(f"- {p.start_component}.{p.start_output} → {p.end_component}.{p.end_input}")


def cmd_component_detail(pkg: ParsedPackage, flow_name: str, comp_name: str) -> None:
    all_exes = _all_executables(pkg)
    task = next((e for e in all_exes if e.name.lower() == flow_name.lower() and e.data_flow), None)
    if not task:
        print(f"Data Flow '{flow_name}' not found.", file=sys.stderr)
        return
    comp = next((c for c in task.data_flow.components if c.name.lower() == comp_name.lower()), None)
    if not comp:
        print(f"Component '{comp_name}' not found in '{flow_name}'.", file=sys.stderr)
        return
    print(f"# Component: {comp.name}")
    print(f"- Class: {comp.class_name or comp.class_id}")
    if comp.connections:
        print("- Connections:")
        for cc in comp.connections:
            print(f"  - {cc.name}: {cc.connection_manager_name}")
    if comp.properties:
        sql_props = {k: v for k, v in comp.properties.items() if v and "sql" in k.lower()}
        if sql_props:
            print("- SQL Properties:")
            for k, v in sql_props.items():
                if len(v) > 500:
                    print(f"  - {k} (truncated, run `extract-sql` for full query):\n```sql\n{v[:500]}...\n```")
                else:
                    print(f"  - {k}:\n```sql\n{v}\n```")
    if comp.outputs:
        for out in comp.outputs:
            print(f"- Output '{out.name}': {len(out.columns)} columns")
            for col in out.columns:
                print(f"  - {col.name} ({col.data_type}, len={col.length})")
    if comp.inputs:
        for inp in comp.inputs:
            print(f"- Input '{inp.name}': {len(inp.columns)} columns")


def cmd_column_lineage(pkg: ParsedPackage, flow_name: str) -> None:
    all_exes = _all_executables(pkg)
    task = next((e for e in all_exes if e.name.lower() == flow_name.lower() and e.data_flow), None)
    if not task:
        print(f"Data Flow '{flow_name}' not found.", file=sys.stderr)
        return
    df = task.data_flow
    print(f"# Column Lineage: {task.name}")
    lineage_map: dict[int, tuple[str, str]] = {}
    for c in df.components:
        for out in c.outputs:
            for col in out.columns:
                if col.lineage_id is not None:
                    lineage_map[col.lineage_id] = (c.name, col.name)
    for c in df.components:
        if c.inputs:
            for inp in c.inputs:
                if inp.columns:
                    print(f"\n## {c.name} ← Input '{inp.name}'")
                    for col in inp.columns:
                        src = lineage_map.get(col.lineage_id, ("?", "?"))
                        print(f"  - {col.name or f'LineageID={col.lineage_id}'} ← {src[0]}.{src[1]}")


def cmd_list_variables(pkg: ParsedPackage) -> None:
    print("# Variables")
    print("| Namespace | Name | Type | Value | Expression | Scope |")
    print("|-----------|------|------|-------|------------|-------|")
    for v in pkg.variables:
        expr = v.expression or ""
        vtype = resolve_ole_variant_type(v.data_type)
        print(f"| {v.namespace} | {v.name} | {vtype} | {v.value or ''} | {expr} | {v.scope or 'Package'} |")


def cmd_list_parameters(pkg: ParsedPackage) -> None:
    print("# Parameters")
    print("| Name | Type | Default | Required | Sensitive |")
    print("|------|------|---------|----------|-----------|")
    for p in pkg.parameters:
        print(f"| {p.name} | {p.data_type} | {p.default_value or ''} | {p.required} | {p.sensitive} |")


def cmd_variable_refs(pkg: ParsedPackage, name: str | None = None) -> None:
    refs = pkg.variable_references or []
    if name:
        refs = [r for r in refs if name.lower() in r.variable_name.lower()]
    print("# Variable Cross-References")
    for r in refs:
        print(f"\n## {r.variable_name}")
        if r.set_by:
            print(f"- Set by: {', '.join(r.set_by)}")
        if r.consumed_by:
            print(f"- Read by: {', '.join(r.consumed_by)}")


def cmd_extract_sql(pkg: ParsedPackage) -> None:
    print("# All SQL Statements")
    all_exes = _all_executables(pkg)
    for e in all_exes:
        if e.execute_sql and e.execute_sql.sql_statement:
            print(f"\n## {e.name} (Execute SQL Task)")
            print(f"```sql\n{e.execute_sql.sql_statement}\n```")
        if e.data_flow:
            for c in e.data_flow.components:
                if c.properties:
                    for k, v in c.properties.items():
                        if "sql" in k.lower() and v:
                            print(f"\n## {e.name} > {c.name} ({k})")
                            print(f"```sql\n{v}\n```")


def cmd_extract_scripts(pkg: ParsedPackage) -> None:
    print("# All Scripts")
    all_exes = _all_executables(pkg)
    for e in all_exes:
        if e.script_task and e.script_task.source_code:
            st = e.script_task
            print(f"\n## {e.name} (Script Task)")
            print(f"- Language: {st.script_language or 'C#'}")
            print(f"```csharp\n{st.source_code}\n```")
        if e.data_flow:
            for c in e.data_flow.components:
                if c.script_data and c.script_data.source_files:
                    print(f"\n## {e.name} > {c.name} (Script Component)")
                    for fname, code in c.script_data.source_files.items():
                        print(f"\n### {fname}")
                        print(f"```csharp\n{code}\n```")


def cmd_find(pkg: ParsedPackage, term: str) -> None:
    term_lower = term.lower()
    print(f"# Search Results for '{term}'")
    found = False

    for c in pkg.connections:
        if term_lower in c.name.lower():
            print(f"- Connection: {c.name} [{c.creation_name}]")
            found = True

    for e in _all_executables(pkg):
        if term_lower in e.name.lower():
            print(f"- Task: {e.name} [{e.creation_name}]")
            found = True
        if e.data_flow:
            for c in e.data_flow.components:
                if term_lower in c.name.lower():
                    print(f"- Component: {c.name} in {e.name}")
                    found = True

    for v in pkg.variables:
        if term_lower in v.name.lower():
            print(f"- Variable: {v.namespace}::{v.name}")
            found = True

    for p in pkg.parameters:
        if term_lower in p.name.lower():
            print(f"- Parameter: {p.name}")
            found = True

    if not found:
        print("No matches found.")


def cmd_explain(name: str) -> None:
    entry = lookup_component(name)
    if entry:
        print(format_knowledge(entry))
    else:
        print(f"No knowledge base entry found for '{name}'.")


def cmd_list_known_components() -> None:
    categories = list_categories()
    print("# Known SSIS Components")
    for category, components in sorted(categories.items()):
        print(f"\n## {category}")
        for c in components:
            print(f"- {c}")


# ---------------------------------------------------------------------------
# JSON output helpers
# ---------------------------------------------------------------------------

_SQL_TRUNCATE_LEN = 500


def _json_overview(pkg: ParsedPackage) -> dict:
    m = pkg.metadata
    df_count = sum(1 for e in _all_executables(pkg) if e.data_flow is not None)
    return {
        "name": m.name, "format_version": m.format_version,
        "creator": m.creator or None, "description": m.description or None,
        "protection_level": m.protection_level, "deployment_model": pkg.deployment_model or "Package",
        "counts": {
            "connections": len(pkg.connections), "variables": len(pkg.variables),
            "parameters": len(pkg.parameters), "tasks": len(pkg.executables),
            "data_flows": df_count,
        },
    }


def _json_execution_order(pkg: ParsedPackage) -> dict:
    order = topological_sort(pkg.executables, pkg.root_precedence_constraints)
    branches = detect_parallel_branches(pkg.executables, pkg.root_precedence_constraints)
    return {
        "order": order,
        "parallel_branches": [{"tasks": b.tasks, "shared_predecessor": b.shared_predecessor} for b in branches] if branches else [],
    }


def _json_list_connections(pkg: ParsedPackage) -> list:
    return [{"name": c.name, "type": c.creation_name or None, "project_level": c.is_project_level} for c in pkg.connections]


def _json_connection_detail(pkg: ParsedPackage, name: str) -> dict | None:
    conn = next((c for c in pkg.connections if c.name.lower() == name.lower()), None)
    if not conn:
        return {"error": f"Connection '{name}' not found."}
    return asdict(conn)


def _json_task_tree(exes: list[Executable]) -> list:
    result = []
    for e in exes:
        node = {"name": e.name, "type": e.creation_name}
        if e.children:
            node["children"] = _json_task_tree(e.children)
        result.append(node)
    return result


def _json_list_tasks(pkg: ParsedPackage) -> list:
    return _json_task_tree(pkg.executables)


def _json_task_detail(pkg: ParsedPackage, name: str) -> dict | None:
    task = next((e for e in _all_executables(pkg) if e.name.lower() == name.lower()), None)
    if not task:
        return {"error": f"Task '{name}' not found."}
    d = asdict(task)
    # Replace children with summary to avoid huge recursive output
    if task.children:
        child_order = topological_sort(task.children, task.precedence_constraints)
        d["child_execution_order"] = child_order
        d["children"] = [{"name": c.name, "type": c.creation_name} for c in task.children]
    return d


def _json_list_constraints(pkg: ParsedPackage) -> list:
    return [asdict(pc) for pc in pkg.root_precedence_constraints]


def _json_list_data_flows(pkg: ParsedPackage) -> list:
    return [
        {"name": e.name, "components": len(e.data_flow.components), "paths": len(e.data_flow.paths)}
        for e in _all_executables(pkg) if e.data_flow
    ]


def _json_data_flow_detail(pkg: ParsedPackage, name: str) -> dict | None:
    task = next((e for e in _all_executables(pkg) if e.name.lower() == name.lower() and e.data_flow), None)
    if not task:
        return {"error": f"Data Flow '{name}' not found."}
    return {"name": task.name, "data_flow": asdict(task.data_flow)}


def _json_component_detail(pkg: ParsedPackage, flow_name: str, comp_name: str) -> dict | None:
    task = next((e for e in _all_executables(pkg) if e.name.lower() == flow_name.lower() and e.data_flow), None)
    if not task:
        return {"error": f"Data Flow '{flow_name}' not found."}
    comp = next((c for c in task.data_flow.components if c.name.lower() == comp_name.lower()), None)
    if not comp:
        return {"error": f"Component '{comp_name}' not found in '{flow_name}'."}
    return asdict(comp)


def _json_column_lineage(pkg: ParsedPackage, flow_name: str) -> dict | None:
    task = next((e for e in _all_executables(pkg) if e.name.lower() == flow_name.lower() and e.data_flow), None)
    if not task:
        return {"error": f"Data Flow '{flow_name}' not found."}
    df = task.data_flow
    lineage_map: dict[int, tuple[str, str]] = {}
    for c in df.components:
        for out in c.outputs:
            for col in out.columns:
                if col.lineage_id is not None:
                    lineage_map[col.lineage_id] = (c.name, col.name)
    mappings = []
    for c in df.components:
        if c.inputs:
            for inp in c.inputs:
                for col in inp.columns:
                    src = lineage_map.get(col.lineage_id, ("?", "?"))
                    mappings.append({"target_component": c.name, "target_input": inp.name,
                                     "target_column": col.name or f"LineageID={col.lineage_id}",
                                     "source_component": src[0], "source_column": src[1]})
    return {"flow": task.name, "lineage": mappings}


def _json_list_variables(pkg: ParsedPackage) -> list:
    return [
        {"namespace": v.namespace, "name": v.name, "type": resolve_ole_variant_type(v.data_type),
         "value": v.value or None, "expression": v.expression, "scope": v.scope or "Package"}
        for v in pkg.variables
    ]


def _json_list_parameters(pkg: ParsedPackage) -> list:
    return [asdict(p) for p in pkg.parameters]


def _json_variable_refs(pkg: ParsedPackage, name: str | None = None) -> list:
    refs = pkg.variable_references or []
    if name:
        refs = [r for r in refs if name.lower() in r.variable_name.lower()]
    return [asdict(r) for r in refs]


def _json_extract_sql(pkg: ParsedPackage) -> list:
    results = []
    for e in _all_executables(pkg):
        if e.execute_sql and e.execute_sql.sql_statement:
            results.append({"task": e.name, "type": "Execute SQL Task", "sql": e.execute_sql.sql_statement})
        if e.data_flow:
            for c in e.data_flow.components:
                if c.properties:
                    for k, v in c.properties.items():
                        if "sql" in k.lower() and v:
                            results.append({"task": e.name, "component": c.name, "property": k, "sql": v})
    return results


def _json_extract_scripts(pkg: ParsedPackage) -> list:
    results = []
    for e in _all_executables(pkg):
        if e.script_task and e.script_task.source_code:
            results.append({"task": e.name, "type": "Script Task", "language": e.script_task.script_language, "source": e.script_task.source_code})
        if e.data_flow:
            for c in e.data_flow.components:
                if c.script_data and c.script_data.source_files:
                    results.append({"task": e.name, "component": c.name, "type": "Script Component", "source": c.script_data.source_files})
    return results


def _json_find(pkg: ParsedPackage, term: str) -> dict:
    term_lower = term.lower()
    results: dict[str, list] = {"connections": [], "tasks": [], "components": [], "variables": [], "parameters": []}
    for c in pkg.connections:
        if term_lower in c.name.lower():
            results["connections"].append({"name": c.name, "type": c.creation_name})
    for e in _all_executables(pkg):
        if term_lower in e.name.lower():
            results["tasks"].append({"name": e.name, "type": e.creation_name})
        if e.data_flow:
            for c in e.data_flow.components:
                if term_lower in c.name.lower():
                    results["components"].append({"name": c.name, "flow": e.name})
    for v in pkg.variables:
        if term_lower in v.name.lower():
            results["variables"].append({"namespace": v.namespace, "name": v.name})
    for p in pkg.parameters:
        if term_lower in p.name.lower():
            results["parameters"].append({"name": p.name})
    return results


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    # Parse --json flag (can appear anywhere after the dtsx path)
    raw_args = sys.argv[1:]
    json_mode = "--json" in raw_args
    if json_mode:
        raw_args.remove("--json")

    if len(raw_args) < 2:
        print(__doc__)
        sys.exit(1)

    dtsx_path = raw_args[0]
    command = raw_args[1]
    args = raw_args[2:]

    # Commands that don't need a loaded package
    if command == "explain":
        if not args:
            print("Usage: analyze.py <dtsx_path> explain <component_name>", file=sys.stderr)
            sys.exit(1)
        cmd_explain(" ".join(args))
        return
    if command == "list-known-components":
        cmd_list_known_components()
        return

    # Load the package
    path = Path(dtsx_path)
    if not path.exists():
        print(f"ERROR: File not found: {dtsx_path}", file=sys.stderr)
        sys.exit(1)
    if path.suffix.lower() != ".dtsx":
        print(f"ERROR: Expected .dtsx file, got: {path.suffix}", file=sys.stderr)
        sys.exit(1)

    pkg = load_package(path)

    if json_mode:
        json_commands = {
            "overview": lambda: _json_overview(pkg),
            "execution-order": lambda: _json_execution_order(pkg),
            "list-connections": lambda: _json_list_connections(pkg),
            "connection-detail": lambda: _json_connection_detail(pkg, " ".join(args)),
            "list-tasks": lambda: _json_list_tasks(pkg),
            "task-detail": lambda: _json_task_detail(pkg, " ".join(args)),
            "list-constraints": lambda: _json_list_constraints(pkg),
            "list-data-flows": lambda: _json_list_data_flows(pkg),
            "data-flow-detail": lambda: _json_data_flow_detail(pkg, " ".join(args)),
            "component-detail": lambda: _json_component_detail(pkg, args[0], " ".join(args[1:])) if len(args) >= 2 else {"error": "Usage: ... component-detail <flow> <component>"},
            "column-lineage": lambda: _json_column_lineage(pkg, " ".join(args)),
            "list-variables": lambda: _json_list_variables(pkg),
            "list-parameters": lambda: _json_list_parameters(pkg),
            "variable-refs": lambda: _json_variable_refs(pkg, " ".join(args) if args else None),
            "extract-sql": lambda: _json_extract_sql(pkg),
            "extract-scripts": lambda: _json_extract_scripts(pkg),
            "find": lambda: _json_find(pkg, " ".join(args)),
        }
        if command not in json_commands:
            print(f"Unknown command: {command}", file=sys.stderr)
            print(__doc__)
            sys.exit(1)
        result = json_commands[command]()
        print(json.dumps(result, indent=2, default=str))
        return

    commands = {
        "overview": lambda: cmd_overview(pkg),
        "execution-order": lambda: cmd_execution_order(pkg),
        "list-connections": lambda: cmd_list_connections(pkg),
        "connection-detail": lambda: cmd_connection_detail(pkg, " ".join(args)),
        "list-tasks": lambda: cmd_list_tasks(pkg),
        "task-detail": lambda: cmd_task_detail(pkg, " ".join(args)),
        "list-constraints": lambda: cmd_list_constraints(pkg),
        "list-data-flows": lambda: cmd_list_data_flows(pkg),
        "data-flow-detail": lambda: cmd_data_flow_detail(pkg, " ".join(args)),
        "component-detail": lambda: cmd_component_detail(pkg, args[0], " ".join(args[1:])) if len(args) >= 2 else print("Usage: ... component-detail <flow> <component>"),
        "column-lineage": lambda: cmd_column_lineage(pkg, " ".join(args)),
        "list-variables": lambda: cmd_list_variables(pkg),
        "list-parameters": lambda: cmd_list_parameters(pkg),
        "variable-refs": lambda: cmd_variable_refs(pkg, " ".join(args) if args else None),
        "extract-sql": lambda: cmd_extract_sql(pkg),
        "extract-scripts": lambda: cmd_extract_scripts(pkg),
        "find": lambda: cmd_find(pkg, " ".join(args)),
    }

    if command not in commands:
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)

    commands[command]()


if __name__ == "__main__":
    main()
