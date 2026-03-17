"""Extract DTS:Configuration elements into Configuration models."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

from models import Configuration
from normalizers import decode_escapes
from xml_helpers import DTS_NAMESPACE, get_property, safe_int

_NS = f"{{{DTS_NAMESPACE}}}"


def _enabled_bool(value: str | None) -> bool:
    """Parse the ``Enabled`` configuration property.

    Unlike the standard SSIS boolean (absent → False), the Enabled property
    defaults to ``True`` when absent.  Only an explicit ``"0"`` is false.
    """
    if value is None:
        return True
    return value != "0"


def extract_configurations(
    root: ET.Element,
    format_version: int,
) -> list[Configuration]:
    """Extract ``DTS:Configuration`` elements from a .dtsx root.

    In format 8, configurations may live inside a ``DTS:Configurations``
    container element.  In format 2/6, they are direct children of root.

    Reads properties via :func:`get_property`, parses ``ConfigurationType``
    with :func:`safe_int`, and parses ``Enabled`` as boolean (``"0"`` = false,
    default true).

    *format_version* is retained for API consistency and format-aware property
    access.
    """
    configurations: list[Configuration] = []

    config_elements: list[ET.Element] = []

    # Format 8: look for a DTS:Configurations container
    container = root.find(f"{_NS}Configurations")
    if container is not None:
        config_elements.extend(container.findall(f"{_NS}Configuration"))

    # Also look for direct children (format 2/6 style)
    config_elements.extend(root.findall(f"{_NS}Configuration"))

    for cfg_elem in config_elements:
        name = get_property(cfg_elem, "ObjectName", format_version) or ""
        if not name:
            print(
                "Warning: Configuration has no ObjectName, skipping",
                file=sys.stderr,
            )
            continue

        name = decode_escapes(name)

        config_type_raw = get_property(cfg_elem, "ConfigurationType", format_version)
        config_type = safe_int(
            config_type_raw,
            f"Configuration '{name}' ConfigurationType",
        )

        config_string = get_property(cfg_elem, "ConfigurationString", format_version) or ""
        config_string = decode_escapes(config_string)

        config_value = get_property(cfg_elem, "ConfigurationVariable", format_version) or ""
        config_value = decode_escapes(config_value)

        dtsid = get_property(cfg_elem, "DTSID", format_version) or ""

        enabled_raw = get_property(cfg_elem, "Enabled", format_version)
        enabled = _enabled_bool(enabled_raw)

        configurations.append(
            Configuration(
                name=name,
                config_type=config_type,
                config_string=config_string,
                config_value=config_value,
                enabled=enabled,
                dtsid=dtsid,
            )
        )

    return configurations
