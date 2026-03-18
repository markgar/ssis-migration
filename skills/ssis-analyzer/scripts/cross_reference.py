"""Variable cross-reference — pattern matching and model walker.

Scans a ParsedPackage model to build a cross-reference table showing
where each variable is set (writers) and consumed (readers).
"""

from __future__ import annotations

import re
from collections import defaultdict

from models import (
    Component,
    EventHandler,
    Executable,
    ParsedPackage,
    PrecedenceConstraint,
    VariableReference,
)

# Type alias for the writer/consumer label accumulator
_LabelMap = dict[str, list[str]]

# ---------------------------------------------------------------------------
# §10.3 — Variable reference pattern matching
# ---------------------------------------------------------------------------

# Pattern 1: Standard bracketed — @[Namespace::VarName]
_BRACKETED_RE = re.compile(r"@\[(\w+::\w+)\]")

# Pattern 2: Project parameter bracketed — @[$Project::Name]
_PROJECT_BRACKETED_RE = re.compile(r"@\[\$Project::(\w+)\]")

# Pattern 3: Script Task runtime access — Dts.Variables["$Project::Name"]
_DTS_VARIABLES_RE = re.compile(r'Dts\.Variables\["\$Project::(\w+)"\]')

# Pattern 4: Bare Namespace::VarName
_BARE_RE = re.compile(r"(\w+::\w+)")

# Pattern 5: Bare $Project::Name (not inside brackets)
_BARE_PROJECT_RE = re.compile(r"\$Project::(\w+)")

# Patterns to strip before bare matching (brackets and Dts.Variables["..."])
_STRIP_BRACKETED_RE = re.compile(r"@\[\$?[^\]]*\]")
_STRIP_DTS_RE = re.compile(r'Dts\.Variables\["[^"]*"\]')

# Configuration variable path — \Package.Variables[User::varName].Properties[Value]
_CONFIG_VAR_RE = re.compile(r"\\[^[]*Variables\[([^\]]+)\]")


def extract_variable_names(text: str | None) -> list[str]:
    """Extract all variable names from an expression string.

    Returns a deduplicated, sorted list of fully-qualified variable names
    (e.g. ``User::varFileName``, ``$Project::batchId``).

    Applies patterns in order: bracketed first, then bare on cleaned text,
    to avoid double-matching.
    """
    if not text:
        return []

    seen: set[str] = set()

    # Pattern 1: @[Namespace::VarName] — standard bracketed
    for m in _BRACKETED_RE.finditer(text):
        seen.add(m.group(1))

    # Pattern 2: @[$Project::Name] — project parameter bracketed
    for m in _PROJECT_BRACKETED_RE.finditer(text):
        seen.add(f"$Project::{m.group(1)}")

    # Pattern 3: Dts.Variables["$Project::Name"] — script runtime access
    for m in _DTS_VARIABLES_RE.finditer(text):
        seen.add(f"$Project::{m.group(1)}")

    # Pattern 4: Bare Namespace::VarName — strip bracketed/Dts patterns
    # first to avoid matching partial substrings inside brackets
    cleaned = _STRIP_BRACKETED_RE.sub(" ", text)
    cleaned = _STRIP_DTS_RE.sub(" ", cleaned)

    # 4a: Bare $Project::Name
    for m in _BARE_PROJECT_RE.finditer(cleaned):
        seen.add(f"$Project::{m.group(1)}")

    # 4b: Bare Namespace::VarName (after stripping $Project:: to avoid partial)
    cleaned2 = _BARE_PROJECT_RE.sub(" ", cleaned)
    for m in _BARE_RE.finditer(cleaned2):
        seen.add(m.group(1))

    return sorted(seen)


# ---------------------------------------------------------------------------
# §10.2 — Cross-reference model walker
# ---------------------------------------------------------------------------


def build_variable_references(
    pkg: ParsedPackage,
) -> list[VariableReference]:
    """Walk the full ParsedPackage model and collect variable references.

    Returns a sorted list of VariableReference objects, each describing
    where a variable is set and consumed. Variables with no references
    are excluded.
    """
    writers: _LabelMap = defaultdict(list)
    consumers: _LabelMap = defaultdict(list)

    # 1. Scan configurations (writers)
    _scan_configurations(pkg, writers)

    # 2. Scan connection manager property expressions (consumers)
    _scan_connection_property_expressions(pkg, consumers)

    # 3. Recursively scan all executables
    for exe in pkg.executables:
        _scan_executable(exe, writers, consumers)

    # 4. Scan root-level event handlers
    for handler in pkg.root_event_handlers:
        _scan_event_handler(handler, writers, consumers)

    # 5. Scan root-level precedence constraints
    for pc in pkg.root_precedence_constraints:
        _scan_precedence_constraint(pc, consumers)

    # Merge writers and consumers into VariableReference objects,
    # deduplicating labels within each list to avoid noisy repetition
    all_vars = set(writers.keys()) | set(consumers.keys())
    refs: list[VariableReference] = []
    for var_name in sorted(all_vars):
        refs.append(
            VariableReference(
                variable_name=var_name,
                set_by=_dedupe(writers.get(var_name, [])),
                consumed_by=_dedupe(consumers.get(var_name, [])),
            )
        )

    return refs


def _dedupe(labels: list[str]) -> list[str]:
    """Remove duplicate labels while preserving insertion order."""
    seen: set[str] = set()
    result: list[str] = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            result.append(label)
    return result


# ---------------------------------------------------------------------------
# Writer scanners
# ---------------------------------------------------------------------------


def _scan_configurations(
    pkg: ParsedPackage,
    writers: _LabelMap,
) -> None:
    """Configurations that target a variable path are writers."""
    for config in pkg.configurations:
        if not config.config_value:
            continue
        m = _CONFIG_VAR_RE.search(config.config_value)
        if m:
            var_name = m.group(1)
            label = f"Configuration: {config.name}"
            writers[var_name].append(label)


def _scan_foreach_variable_mappings(
    exe: Executable,
    writers: _LabelMap,
) -> None:
    """ForEach variable mappings are writers."""
    if exe.for_each is None:
        return
    for mapping in exe.for_each.variable_mappings:
        if mapping.variable_name:
            label = f"ForEach Variable Mapping in '{exe.name}'"
            writers[mapping.variable_name].append(label)


def _scan_execute_sql_result_bindings(
    exe: Executable,
    writers: _LabelMap,
) -> None:
    """Execute SQL result bindings are writers."""
    if exe.execute_sql is None:
        return
    for rb in exe.execute_sql.result_bindings:
        if rb.variable_name:
            label = f"Execute SQL Task '{exe.name}' ResultBinding"
            writers[rb.variable_name].append(label)


def _scan_script_task_rw_variables(
    exe: Executable,
    writers: _LabelMap,
) -> None:
    """Script Task read-write variables are writers."""
    if exe.script_task is None:
        return
    for var_name in exe.script_task.read_write_variables:
        if var_name:
            label = f"Script Task '{exe.name}' ReadWriteVariable"
            writers[var_name].append(label)


def _scan_script_component_rw_variables(
    comp: Component,
    writers: _LabelMap,
) -> None:
    """Script Component read-write variables are writers."""
    if comp.script_data is None:
        return
    for var_name in comp.script_data.read_write_variables:
        if var_name:
            label = f"Script Component '{comp.name}' ReadWriteVariable"
            writers[var_name].append(label)


# ---------------------------------------------------------------------------
# Consumer scanners
# ---------------------------------------------------------------------------


def _scan_connection_property_expressions(
    pkg: ParsedPackage,
    consumers: _LabelMap,
) -> None:
    """Connection manager property expressions are consumers."""
    for conn in pkg.connections:
        for prop_name, expr in sorted(conn.property_expressions.items()):
            var_names = extract_variable_names(expr)
            for var_name in var_names:
                label = (
                    f"Connection Manager '{conn.name}' "
                    f"PropertyExpression ({prop_name})"
                )
                consumers[var_name].append(label)


def _scan_executable_property_expressions(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """Task/Container property expressions are consumers."""
    for prop_name, expr in sorted(exe.property_expressions.items()):
        var_names = extract_variable_names(expr)
        for var_name in var_names:
            label = f"Task '{exe.name}' PropertyExpression ({prop_name})"
            consumers[var_name].append(label)


def _scan_precedence_constraint(
    pc: PrecedenceConstraint,
    consumers: _LabelMap,
) -> None:
    """Precedence constraint expressions are consumers."""
    if not pc.expression:
        return
    var_names = extract_variable_names(pc.expression)
    for var_name in var_names:
        label = f"Precedence Constraint '{pc.from_task} \u2192 {pc.to_task}'"
        consumers[var_name].append(label)


def _scan_execute_sql_parameter_bindings(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """Execute SQL parameter bindings are consumers."""
    if exe.execute_sql is None:
        return
    for pb in exe.execute_sql.parameter_bindings:
        if pb.variable_name:
            label = (
                f"Execute SQL Task '{exe.name}' "
                f"ParameterBinding @{pb.parameter_name}"
            )
            consumers[pb.variable_name].append(label)


def _scan_script_task_ro_variables(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """Script Task read-only variables are consumers."""
    if exe.script_task is None:
        return
    for var_name in exe.script_task.read_only_variables:
        if var_name:
            label = f"Script Task '{exe.name}' ReadOnlyVariable"
            consumers[var_name].append(label)


def _scan_script_component_ro_variables(
    comp: Component,
    consumers: _LabelMap,
) -> None:
    """Script Component read-only variables are consumers."""
    if comp.script_data is None:
        return
    for var_name in comp.script_data.read_only_variables:
        if var_name:
            label = f"Script Component '{comp.name}' ReadOnlyVariable"
            consumers[var_name].append(label)


def _scan_foreach_enumerator_property_expressions(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """ForEach enumerator property expressions are consumers."""
    if exe.for_each is None:
        return
    for prop_name, expr in sorted(exe.for_each.property_expressions.items()):
        var_names = extract_variable_names(expr)
        for var_name in var_names:
            label = (
                f"ForEach Enumerator PropertyExpression ({prop_name}) "
                f"in '{exe.name}'"
            )
            consumers[var_name].append(label)


def _scan_derived_column_expressions(
    comp: Component,
    consumers: _LabelMap,
) -> None:
    """Derived Column expressions referencing variables are consumers."""
    for out in comp.outputs:
        if out.is_error_output:
            continue
        for col in out.columns:
            expr = col.properties.get(
                "FriendlyExpression", col.properties.get("Expression", "")
            )
            if expr:
                var_names = extract_variable_names(expr)
                for var_name in var_names:
                    label = f"Derived Column '{comp.name}' Expression"
                    consumers[var_name].append(label)

    # Also scan replacement columns (input columns)
    for inp in comp.inputs:
        for in_col in inp.columns:
            expr = in_col.properties.get(
                "FriendlyExpression", in_col.properties.get("Expression", "")
            )
            if expr:
                var_names = extract_variable_names(expr)
                for var_name in var_names:
                    label = f"Derived Column '{comp.name}' Expression"
                    consumers[var_name].append(label)


def _scan_execute_package_parameter_assignments(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """Execute Package Task parameter assignments referencing variables are consumers."""
    if exe.execute_package is None:
        return
    for child_param, parent_var in exe.execute_package.parameter_assignments:
        if parent_var:
            var_names = extract_variable_names(parent_var)
            for var_name in var_names:
                label = (
                    f"Execute Package Task '{exe.name}' "
                    f"ParameterAssignment ({child_param})"
                )
                consumers[var_name].append(label)


def _scan_script_task_source_code(
    exe: Executable,
    consumers: _LabelMap,
) -> None:
    """Script Task source code containing $Project:: references are consumers."""
    if exe.script_task is None or exe.script_task.source_code is None:
        return
    for _filename, code in sorted(exe.script_task.source_code.items()):
        for m in _DTS_VARIABLES_RE.finditer(code):
            var_name = f"$Project::{m.group(1)}"
            label = (
                f"Script Task '{exe.name}' "
                f"source code ($Project::{m.group(1)})"
            )
            consumers[var_name].append(label)


# ---------------------------------------------------------------------------
# Recursive walkers
# ---------------------------------------------------------------------------


def _scan_data_flow_components(
    exe: Executable,
    writers: _LabelMap,
    consumers: _LabelMap,
) -> None:
    """Scan data flow components for variable references."""
    if exe.data_flow is None:
        return
    for comp in exe.data_flow.components:
        # Script Component RW (writers) and RO (consumers)
        _scan_script_component_rw_variables(comp, writers)
        _scan_script_component_ro_variables(comp, consumers)

        # Derived Column expressions (consumers)
        if comp.class_name == "Derived Column":
            _scan_derived_column_expressions(comp, consumers)


def _scan_executable(
    exe: Executable,
    writers: _LabelMap,
    consumers: _LabelMap,
) -> None:
    """Recursively scan an executable and its children for variable refs."""
    # Writers
    _scan_foreach_variable_mappings(exe, writers)
    _scan_execute_sql_result_bindings(exe, writers)
    _scan_script_task_rw_variables(exe, writers)

    # Consumers
    _scan_executable_property_expressions(exe, consumers)
    _scan_execute_sql_parameter_bindings(exe, consumers)
    _scan_script_task_ro_variables(exe, consumers)
    _scan_foreach_enumerator_property_expressions(exe, consumers)
    _scan_execute_package_parameter_assignments(exe, consumers)
    _scan_script_task_source_code(exe, consumers)

    # Data flow components
    _scan_data_flow_components(exe, writers, consumers)

    # Precedence constraints within this executable
    for pc in exe.precedence_constraints:
        _scan_precedence_constraint(pc, consumers)

    # Event handlers
    for handler in exe.event_handlers:
        _scan_event_handler(handler, writers, consumers)

    # Recurse into children
    for child in exe.children:
        _scan_executable(child, writers, consumers)


def _scan_event_handler(
    handler: EventHandler,
    writers: _LabelMap,
    consumers: _LabelMap,
) -> None:
    """Scan an event handler's executables for variable refs."""
    for exe in handler.executables:
        _scan_executable(exe, writers, consumers)
    for pc in handler.precedence_constraints:
        _scan_precedence_constraint(pc, consumers)

