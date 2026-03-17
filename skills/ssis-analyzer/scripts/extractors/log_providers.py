"""Extract log providers from .dtsx packages."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

from models import LogProvider
from normalizers import decode_escapes
from xml_helpers import DTS_NAMESPACE, get_property, safe_bool

_NS = f"{{{DTS_NAMESPACE}}}"


def _extract_single_log_provider(
    el: ET.Element,
    format_version: int,
) -> LogProvider:
    """Extract a single LogProvider from a DTS:LogProvider element."""
    name = decode_escapes(get_property(el, "ObjectName", format_version) or "")
    dtsid = get_property(el, "DTSID", format_version) or ""
    creation_name = get_property(el, "CreationName", format_version) or ""
    config_string = decode_escapes(
        get_property(el, "ConfigString", format_version) or ""
    )
    description = decode_escapes(
        get_property(el, "Description", format_version) or ""
    )
    delay_validation = safe_bool(
        get_property(el, "DelayValidation", format_version)
    )

    return LogProvider(
        name=name,
        dtsid=dtsid,
        creation_name=creation_name,
        config_string=config_string,
        description=description,
        delay_validation=delay_validation,
    )


def extract_log_providers(
    root: ET.Element,
    format_version: int,
) -> list[LogProvider]:
    """Extract all log providers from a .dtsx root element.

    Log providers may be direct children of the root (format 2/3/6)
    or nested inside a ``DTS:LogProviders`` container (format 8).
    Both locations are searched.

    *format_version* is retained for API consistency and future
    format-specific behavior.
    """
    providers: list[LogProvider] = []

    # Format 2/3/6: direct children of root
    for lp_el in root.findall(f"{_NS}LogProvider"):
        providers.append(_extract_single_log_provider(lp_el, format_version))

    # Format 8: nested inside DTS:LogProviders container
    container = root.find(f"{_NS}LogProviders")
    if container is not None:
        for lp_el in container.findall(f"{_NS}LogProvider"):
            providers.append(
                _extract_single_log_provider(lp_el, format_version)
            )

    return providers


def build_log_provider_map(
    providers: list[LogProvider],
) -> dict[str, LogProvider]:
    """Build a DTSID → LogProvider lookup map (case-insensitive keys).

    Each provider is indexed by its DTSID, upper-cased for
    case-insensitive GUID matching.  If two providers share the same
    DTSID, the later one wins (with a warning to stderr).
    """
    lp_map: dict[str, LogProvider] = {}
    for lp in providers:
        key = lp.dtsid.upper()
        if not key:
            continue
        if key in lp_map:
            print(
                f"Warning: duplicate log provider DTSID {lp.dtsid!r}, "
                f"overwriting {lp_map[key].name!r} with {lp.name!r}",
                file=sys.stderr,
            )
        lp_map[key] = lp
    return lp_map
