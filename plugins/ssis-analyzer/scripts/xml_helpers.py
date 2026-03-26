"""XML namespace, format-version detection, and property accessor."""

from __future__ import annotations

import os
import re
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models import ConnectionManager
import xml.etree.ElementTree as ET

DTS_NAMESPACE = "www.microsoft.com/SqlServer/Dts"

_NS_PREFIX = f"{{{DTS_NAMESPACE}}}"

SSIS_BOOL_TRUTHY = frozenset({"1", "-1", "True"})

_VERBOSE = os.environ.get("SSIS_ANALYZER_VERBOSE", "").lower() in ("1", "true", "yes")

# Regex for detecting braced GUIDs (e.g., {F5DC126A-...})
BRACED_GUID_RE = re.compile(
    r"^\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}"
    r"-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}$"
)

# Regex for extracting the name between brackets in a refId path
REFID_BRACKET_RE = re.compile(r"\[([^\]]+)\]")

# Suffixes appended by SSIS to connection-manager references
_CONNECTION_SUFFIXES = (":external", ":invalid")


def strip_connection_suffix(value: str) -> str:
    """Strip known SSIS connection-reference suffixes (e.g. ``:external``, ``:invalid``).

    Returns the value unchanged when no suffix is present.
    """
    for suffix in _CONNECTION_SUFFIXES:
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def resolve_connection_ref(
    ref: str,
    connection_map: dict[str, ConnectionManager] | None,
) -> str:
    """Resolve a connection reference to a connection manager display name.

    Handles GUID-based (format 2/6) and refId-based (format 8) references.
    Returns the resolved name, or an ``[unresolved: ...]`` placeholder for
    unresolvable GUIDs.  Returns the input unchanged when *connection_map*
    is ``None`` or *ref* is empty.
    """
    if not ref or connection_map is None:
        return ref

    ref = strip_connection_suffix(ref)

    # Direct key lookup (handles format 8 refId paths stored as-is)
    cm = connection_map.get(ref)
    if cm is not None:
        return cm.name

    # Case-insensitive GUID lookup (format 2/6)
    cm = connection_map.get(ref.upper())
    if cm is not None:
        return cm.name

    # Format 8 fallback: extract name from brackets in refId path
    match = REFID_BRACKET_RE.search(ref)
    if match:
        bracket_name = match.group(1)
        for cm in connection_map.values():
            if cm.name == bracket_name:
                return cm.name
        if _VERBOSE:
            print(
                f"Warning: connection reference {ref!r} not found in "
                f"Connection Map; using extracted name {bracket_name!r}",
                file=sys.stderr,
            )
        return bracket_name

    if ref:
        if _VERBOSE:
            print(
                f"Warning: connection reference {ref!r} not found in "
                f"Connection Map",
                file=sys.stderr,
            )

    # Wrap raw GUIDs in [unresolved: ...] to prevent them from appearing
    # as raw GUIDs in user-facing output (spec §6: no raw GUIDs)
    if BRACED_GUID_RE.match(ref):
        return f"[unresolved: {ref}]"
    return ref


def safe_bool(value: str | None) -> bool:
    """Parse an SSIS boolean property that may be ``'1'``, ``'-1'``, ``'True'``, or absent.

    Returns ``False`` when *value* is ``None`` or not in the truthy set.
    """
    if value is None:
        return False
    return value in SSIS_BOOL_TRUTHY


def safe_int(value: str | None, label: str, default: int = 0) -> int:
    """Parse *value* as int, logging and returning *default* on failure.

    Returns *default* when *value* is ``None``.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        print(
            f"Warning: {label} is not a valid integer ({value!r}), "
            f"using default {default}",
            file=sys.stderr,
        )
        return default


def safe_id(value: str | None, label: str, default: int = 0) -> int | str:
    """Parse *value* as an SSIS identifier — integer in format ≤6, RefId string in format 8.

    Returns *default* when *value* is ``None``.
    Returns ``int`` when *value* is a valid integer string.
    Returns the raw ``str`` when *value* is a RefId path (format 8).
    """
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


def get_format_version(root: ET.Element) -> int:
    """Detect the PackageFormatVersion from a .dtsx root element.

    Resolution order:
      1. Attribute on the root element (format 8+)
      2. Child ``DTS:Property`` element with ``DTS:Name='PackageFormatVersion'``
      3. Default to 2
    """
    # 1. Try attribute
    attr_value = root.get(f"{_NS_PREFIX}PackageFormatVersion")
    if attr_value is not None:
        return safe_int(attr_value, "PackageFormatVersion attribute", 2)

    # 2. Try child property element
    for prop in root.findall(f"{_NS_PREFIX}Property"):
        if prop.get(f"{_NS_PREFIX}Name") == "PackageFormatVersion":
            text = prop.text
            if text is not None:
                return safe_int(text, "PackageFormatVersion property", 2)

    # 3. Default
    return 2


def get_property(
    element: ET.Element,
    name: str,
    format_version: int,
) -> str | None:
    """Read a named property from an element using format-appropriate strategy.

    * Format ≥ 8: reads the ``{DTS namespace}{name}`` attribute.
    * Format < 8: finds a child ``DTS:Property`` whose ``DTS:Name`` equals *name*
      and returns its text content.

    Returns ``None`` when the property is absent.
    """
    if format_version >= 8:
        return element.get(f"{_NS_PREFIX}{name}")

    for prop in element.findall(f"{_NS_PREFIX}Property"):
        if prop.get(f"{_NS_PREFIX}Name") == name:
            return prop.text or ""

    return None
