"""Extract DTS:PackageParameter elements into PackageParameter models."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

from lookups import resolve_data_type
from models import PackageParameter
from normalizers import decode_escapes
from xml_helpers import DTS_NAMESPACE, get_property, safe_bool, safe_int

_NS = f"{{{DTS_NAMESPACE}}}"


def extract_package_parameters(
    root: ET.Element,
    format_version: int,
) -> list[PackageParameter]:
    """Extract ``DTS:PackageParameter`` elements from a .dtsx root.

    Package parameters live inside a ``DTS:PackageParameters`` container.
    Each parameter's data type is resolved via :func:`resolve_data_type`.

    *format_version* is retained for API consistency; PackageParameter elements
    are typically found in format 8 packages.
    """
    parameters: list[PackageParameter] = []

    # Find the PackageParameters container
    container = root.find(f"{_NS}PackageParameters")
    if container is None:
        return parameters

    for param_elem in container.findall(f"{_NS}PackageParameter"):
        # In format 8, attributes on the element itself
        name = param_elem.get(f"{_NS}ObjectName")

        # Fall back to property accessor for format 2/6 (rare for parameters)
        if name is None:
            name = get_property(param_elem, "ObjectName", format_version) or ""

        if not name:
            print(
                "Warning: PackageParameter has no ObjectName, skipping",
                file=sys.stderr,
            )
            continue

        name = decode_escapes(name)

        # Data type: attribute first, then property accessor
        raw_dt = param_elem.get(f"{_NS}DataType")
        if raw_dt is None:
            raw_dt = get_property(param_elem, "DataType", format_version)
        data_type_code = safe_int(raw_dt, f"PackageParameter '{name}' DataType")
        data_type = resolve_data_type(data_type_code)

        # Default value from child DTS:Property[@DTS:Name='ParameterValue']
        default_value = ""
        for prop in param_elem.findall(f"{_NS}Property"):
            if prop.get(f"{_NS}Name") == "ParameterValue":
                default_value = decode_escapes(prop.text or "")
                break

        # Required and Sensitive: attributes first, then property accessor
        required_raw = param_elem.get(f"{_NS}Required")
        if required_raw is None:
            required_raw = get_property(param_elem, "Required", format_version)
        required = safe_bool(required_raw)

        sensitive_raw = param_elem.get(f"{_NS}Sensitive")
        if sensitive_raw is None:
            sensitive_raw = get_property(param_elem, "Sensitive", format_version)
        sensitive = safe_bool(sensitive_raw)

        # Description
        desc_raw = param_elem.get(f"{_NS}Description")
        if desc_raw is None:
            desc_raw = get_property(param_elem, "Description", format_version)
        description = decode_escapes(desc_raw) if desc_raw else ""

        parameters.append(
            PackageParameter(
                name=name,
                data_type=data_type,
                default_value=default_value,
                required=required,
                sensitive=sensitive,
                description=description,
            )
        )

    return parameters
