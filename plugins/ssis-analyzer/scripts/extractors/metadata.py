"""Extract package-level metadata from a .dtsx root element."""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET

from models import PackageMetadata, ParsedPackage
from xml_helpers import get_property

_VERBOSE = os.environ.get("SSIS_ANALYZER_VERBOSE", "").lower() in ("1", "true", "yes")

_ENABLE_CONFIG_TRUTHY = frozenset({"1", "-1", "True"})


def _get_or_default(
    root: ET.Element,
    name: str,
    format_version: int,
    default: str = "",
) -> str:
    """Read a property, returning *default* if absent. Logs missing properties."""
    value = get_property(root, name, format_version)
    if value is None:
        if _VERBOSE:
            print(
                f"Warning: property '{name}' not found, using default",
                file=sys.stderr,
            )
        return default
    return value


def _get_int(
    root: ET.Element,
    name: str,
    format_version: int,
    default: int,
) -> int:
    """Read a property as int, returning *default* if absent or non-numeric."""
    raw = get_property(root, name, format_version)
    if raw is None:
        if _VERBOSE:
            print(
                f"Warning: property '{name}' not found, using default {default}",
                file=sys.stderr,
            )
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        print(
            f"Warning: property '{name}' is not a valid integer ({raw!r}), "
            f"using default {default}",
            file=sys.stderr,
        )
        return default


def _compose_version(
    root: ET.Element,
    format_version: int,
) -> str:
    """Compose a version string from VersionMajor.VersionMinor.VersionBuild."""
    major = get_property(root, "VersionMajor", format_version)
    minor = get_property(root, "VersionMinor", format_version)
    build = get_property(root, "VersionBuild", format_version)

    parts = [major, minor, build]
    if all(p is not None for p in parts):
        return f"{major}.{minor}.{build}"

    # Partial availability — compose what we have
    present = [(name, val) for name, val in
                zip(["VersionMajor", "VersionMinor", "VersionBuild"], parts)
                if val is not None]
    if not present:
        if _VERBOSE:
            print(
                "Warning: no version properties found, using empty version",
                file=sys.stderr,
            )
        return ""

    missing = [name for name, val in
               zip(["VersionMajor", "VersionMinor", "VersionBuild"], parts)
               if val is None]
    for name in missing:
        if _VERBOSE:
            print(
                f"Warning: property '{name}' not found for version composition",
                file=sys.stderr,
            )

    return ".".join(val if val is not None else "0" for val in parts)


def _detect_deployment_model(
    root: ET.Element,
    format_version: int,
) -> str:
    """Detect deployment model from PackageType property."""
    raw = get_property(root, "PackageType", format_version)
    if raw is None:
        if _VERBOSE:
            print(
                "Warning: property 'PackageType' not found, "
                "defaulting to Package deployment model",
                file=sys.stderr,
            )
        return "Package"
    if raw == "5":
        return "Project"
    return "Package"


def extract_metadata(
    root: ET.Element,
    format_version: int,
) -> ParsedPackage:
    """Extract package-level metadata from a .dtsx root element.

    Returns a ``ParsedPackage`` shell with populated ``metadata`` and
    ``deployment_model``. Other fields are left at their defaults for
    later extraction phases to fill.
    """
    name = _get_or_default(root, "ObjectName", format_version)
    if not name:
        print(
            "Warning: ObjectName is empty; package will have no name",
            file=sys.stderr,
        )

    metadata = PackageMetadata(
        name=name,
        description=_get_or_default(root, "Description", format_version),
        creator=_get_or_default(root, "CreatorName", format_version),
        creator_machine=_get_or_default(
            root, "CreatorComputerName", format_version
        ),
        creation_date=_get_or_default(root, "CreationDate", format_version),
        format_version=str(format_version),
        protection_level=_get_int(root, "ProtectionLevel", format_version, 0),
        max_concurrent=_get_int(
            root, "MaxConcurrentExecutables", format_version, -1
        ),
        version=_compose_version(root, format_version),
        last_modified_version=_get_or_default(
            root, "LastModifiedProductVersion", format_version
        ),
        enable_config=_get_or_default(root, "EnableConfig", format_version)
        in _ENABLE_CONFIG_TRUTHY,
    )

    deployment_model = _detect_deployment_model(root, format_version)

    return ParsedPackage(
        metadata=metadata,
        format_version=format_version,
        deployment_model=deployment_model,
    )
