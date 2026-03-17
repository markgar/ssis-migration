"""Shared package-loading logic for ssis-doc-mcp.

Provides :func:`load_package` — the single entry point that takes a
``.dtsx`` file path and returns a fully-populated :class:`ParsedPackage`.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

from cross_reference import build_variable_references
from extractors.configurations import extract_configurations
from extractors.connections import (
    build_connection_map,
    discover_conmgr_files,
    extract_connections,
)
from extractors.executables import (
    build_executable_map,
    extract_executables,
    extract_root_event_handlers,
    extract_root_precedence_constraints,
    resolve_constraints,
)
from extractors.log_providers import (
    build_log_provider_map,
    extract_log_providers,
)
from extractors.metadata import extract_metadata
from extractors.parameters import extract_package_parameters
from extractors.variables import extract_package_variables, extract_variables
from models import (
    ConnectionManager,
    Executable,
    ParsedPackage,
    Variable,
)
from xml_helpers import get_format_version

# Pattern to detect $Project:: references in expressions
_PROJECT_PARAM_RE = re.compile(r"\$Project::(\w+)")


def _collect_expressions(executables: list[Executable]) -> list[str]:
    """Collect all expression strings from executables and their children."""
    expressions: list[str] = []
    for exe in executables:
        for expr_val in exe.property_expressions.values():
            expressions.append(expr_val)
        if exe.expression_task_expression:
            expressions.append(exe.expression_task_expression)
        expressions.extend(_collect_expressions(exe.children))
    return expressions


def _scan_project_parameter_refs(
    variables: list[Variable],
    executables: list[Executable],
    connections: list[ConnectionManager] | None = None,
) -> list[Variable]:
    """Scan expressions for ``$Project::`` references and return pseudo-variables.

    Each unique ``$Project::ParamName`` reference found in variable
    expressions, executable property expressions, expression task
    expressions, or connection manager property expressions is returned
    as a :class:`Variable` with ``value="[Project Parameter]"`` and
    ``namespace="Project"``.
    """
    seen: set[str] = set()

    # Gather all expression text to scan
    expression_texts: list[str] = []
    for v in variables:
        if v.expression:
            expression_texts.append(v.expression)
    expression_texts.extend(_collect_expressions(executables))

    # Also scan connection manager property expressions
    if connections:
        for conn in connections:
            for expr_val in conn.property_expressions.values():
                expression_texts.append(expr_val)

    for text in expression_texts:
        for match in _PROJECT_PARAM_RE.finditer(text):
            seen.add(match.group(1))

    # Create pseudo-variables for each unique project parameter, sorted for idempotency
    return [
        Variable(
            namespace="Project",
            name=param_name,
            data_type=0,
            scope="Project",
            source="ProjectReference",
            value="[Project Parameter]",
        )
        for param_name in sorted(seen)
    ]


def _resolve_all_constraints(
    executables: list[Executable],
    exe_map: dict[str, str],
    format_version: int,
) -> None:
    """Recursively resolve precedence constraint references to task names.

    Mutates each executable's ``precedence_constraints`` in place, replacing
    raw GUIDs or refId paths with human-readable names.
    """
    for exe in executables:
        if exe.precedence_constraints:
            exe.precedence_constraints = resolve_constraints(
                exe.precedence_constraints, exe_map, format_version,
            )
        if exe.children:
            _resolve_all_constraints(exe.children, exe_map, format_version)


def load_package(dtsx_path: Path) -> ParsedPackage:
    """Parse a ``.dtsx`` file and return a fully-populated :class:`ParsedPackage`.

    This is the primary entry point for loading an SSIS package.  It
    performs XML parsing, all extraction phases, constraint resolution,
    project-parameter detection, and variable cross-reference building.

    Parameters
    ----------
    dtsx_path:
        Path to a ``.dtsx`` file on disk.

    Returns
    -------
    ParsedPackage
        A fully-populated model ready for emission or interactive querying.

    Raises
    ------
    OSError
        If the file cannot be read.
    xml.etree.ElementTree.ParseError
        If the file is not valid XML.
    Exception
        If extraction fails for any reason.
    """
    tree = ET.parse(str(dtsx_path))
    root = tree.getroot()

    format_version = get_format_version(root)
    parsed_package = extract_metadata(root, format_version)

    # Connection managers (embedded + project-level .conmgr files)
    connections = extract_connections(root, format_version)
    conmgr_connections = discover_conmgr_files(dtsx_path)
    all_connections = connections + conmgr_connections
    parsed_package.connections = all_connections
    parsed_package.connection_map = build_connection_map(all_connections)

    # Package parameters
    parsed_package.parameters = extract_package_parameters(root, format_version)

    # Variables: merge PackageVariable (format 2/6) and Variable lists
    pkg_vars = extract_package_variables(root, format_version)
    root_vars = extract_variables(root, format_version, scope="Package")
    parsed_package.variables = pkg_vars + root_vars

    # Configurations
    parsed_package.configurations = extract_configurations(root, format_version)

    # Log providers (before executables — builds map for logging resolution)
    parsed_package.log_providers = extract_log_providers(root, format_version)
    log_provider_map = build_log_provider_map(parsed_package.log_providers)

    # Executables (data flow pipelines detected inside)
    parsed_package.executables = extract_executables(
        root, format_version, parsed_package.connection_map, log_provider_map
    )

    # Root-level precedence constraints (link top-level executables)
    parsed_package.root_precedence_constraints = (
        extract_root_precedence_constraints(root, format_version)
    )

    # Root-level event handlers (package-scope OnError, etc.)
    parsed_package.root_event_handlers = extract_root_event_handlers(
        root, format_version, parsed_package.connection_map, log_provider_map,
    )

    # Resolve precedence constraint references to human-readable names
    exe_map = build_executable_map(parsed_package.executables)
    _resolve_all_constraints(
        parsed_package.executables, exe_map, format_version
    )
    if parsed_package.root_precedence_constraints:
        parsed_package.root_precedence_constraints = resolve_constraints(
            parsed_package.root_precedence_constraints, exe_map,
            format_version,
        )

    # Detect $Project:: references and add as pseudo-variables
    project_refs = _scan_project_parameter_refs(
        parsed_package.variables,
        parsed_package.executables,
        parsed_package.connections,
    )
    if project_refs:
        parsed_package.variables.extend(project_refs)

    # Variable cross-reference (after all extraction and resolution)
    parsed_package.variable_references = build_variable_references(
        parsed_package
    )

    return parsed_package
