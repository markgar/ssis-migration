"""Extract DTS:PackageVariable and DTS:Variable elements into Variable models."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

from models import Variable
from normalizers import decode_escapes
from xml_helpers import DTS_NAMESPACE, get_property, safe_bool, safe_int

_NS = f"{{{DTS_NAMESPACE}}}"

# Designer namespace variables are layout metadata, not user data
_DESIGNER_NAMESPACE = "dts-designer-1.0"


def extract_package_variables(
    root: ET.Element,
    format_version: int,
) -> list[Variable]:
    """Extract ``DTS:PackageVariable`` elements from a format 2/6 root.

    Filters out designer-namespace variables (``dts-designer-1.0``).
    Sets ``source="PackageVariable"`` and ``scope="Package"`` on each result.

    *format_version* is retained for API consistency; PackageVariable elements
    only appear in format 2/6 packages.
    """
    variables: list[Variable] = []

    for pv_elem in root.findall(f"{_NS}PackageVariable"):
        namespace = get_property(pv_elem, "Namespace", format_version) or ""

        if namespace == _DESIGNER_NAMESPACE:
            continue

        name = get_property(pv_elem, "ObjectName", format_version) or ""
        if not name:
            print(
                "Warning: PackageVariable has no ObjectName, skipping",
                file=sys.stderr,
            )
            continue

        # PackageVariableValue is a property with a DTS:DataType attribute
        raw_value = ""
        raw_data_type = 0

        if format_version >= 8:
            # Format 8: unlikely to have PackageVariable, but handle gracefully
            val_elem = pv_elem.find(f"{_NS}PackageVariableValue")
            if val_elem is not None:
                raw_value = val_elem.text or ""
                raw_data_type = safe_int(
                    val_elem.get(f"{_NS}DataType"),
                    f"PackageVariable '{name}' DataType",
                )
        else:
            # Format 2/6: PackageVariableValue is a DTS:Property child
            for prop in pv_elem.findall(f"{_NS}Property"):
                if prop.get(f"{_NS}Name") == "PackageVariableValue":
                    raw_value = prop.text or ""
                    raw_data_type = safe_int(
                        prop.get(f"{_NS}DataType"),
                        f"PackageVariable '{name}' DataType",
                    )
                    break

        description = get_property(pv_elem, "Description", format_version) or ""
        expression_raw = get_property(pv_elem, "Expression", format_version)
        expression = decode_escapes(expression_raw) if expression_raw else None
        eval_as_expr = safe_bool(
            get_property(pv_elem, "EvaluateAsExpression", format_version)
        )
        dtsid = get_property(pv_elem, "DTSID", format_version) or ""

        variables.append(
            Variable(
                namespace=decode_escapes(namespace),
                name=decode_escapes(name),
                data_type=raw_data_type,
                scope="Package",
                source="PackageVariable",
                value=decode_escapes(raw_value),
                expression=expression,
                evaluate_as_expression=eval_as_expr,
                description=decode_escapes(description),
                dtsid=dtsid,
            )
        )

    return variables


def extract_variables(
    parent: ET.Element,
    format_version: int,
    scope: str = "Package",
) -> list[Variable]:
    """Extract ``DTS:Variable`` elements from a parent element.

    Includes both ``User`` and ``System`` namespace variables.
    Reads ``DTS:VariableValue`` directly (not via property accessor) for both
    format 2/6 and format 8.

    Detects ``DTS:DataSubType="ManagedSerializable"`` for ADO recordset variables.
    """
    variables: list[Variable] = []

    # In format 8, variables may be inside a <DTS:Variables> container
    var_container = parent.find(f"{_NS}Variables")
    if var_container is not None:
        var_elements = list(var_container.findall(f"{_NS}Variable"))
    else:
        var_elements = []

    # Also look for direct children (format 2/6 style)
    var_elements.extend(parent.findall(f"{_NS}Variable"))

    for var_elem in var_elements:
        namespace = get_property(var_elem, "Namespace", format_version) or ""
        name = get_property(var_elem, "ObjectName", format_version) or ""

        if not name:
            print(
                "Warning: Variable has no ObjectName, skipping",
                file=sys.stderr,
            )
            continue

        # Read DTS:VariableValue directly — NOT via property accessor
        var_value_elem = var_elem.find(f"{_NS}VariableValue")
        raw_value = ""
        raw_data_type = 0
        data_sub_type: str | None = None

        if var_value_elem is not None:
            raw_value = var_value_elem.text or ""
            raw_data_type = safe_int(
                var_value_elem.get(f"{_NS}DataType"),
                f"Variable '{name}' DataType",
            )
            sub_type = var_value_elem.get(f"{_NS}DataSubType")
            if sub_type:
                data_sub_type = sub_type

        expression_raw = get_property(var_elem, "Expression", format_version)
        expression = decode_escapes(expression_raw) if expression_raw else None

        eval_as_expr = safe_bool(
            get_property(var_elem, "EvaluateAsExpression", format_version)
        )
        read_only = safe_bool(
            get_property(var_elem, "ReadOnly", format_version)
        )
        description = get_property(var_elem, "Description", format_version) or ""
        dtsid = get_property(var_elem, "DTSID", format_version) or ""

        variables.append(
            Variable(
                namespace=decode_escapes(namespace),
                name=decode_escapes(name),
                data_type=raw_data_type,
                scope=scope,
                source="Variable",
                value=decode_escapes(raw_value),
                expression=expression,
                data_sub_type=data_sub_type,
                evaluate_as_expression=eval_as_expr,
                read_only=read_only,
                description=decode_escapes(description),
                dtsid=dtsid,
            )
        )

    return variables
