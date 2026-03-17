"""Tests for extractors.log_providers – extract_log_providers() and build_log_provider_map()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, wrap_xml

from extractors.log_providers import build_log_provider_map, extract_log_providers

_NS = f"{{{DTS_NAMESPACE}}}"


def _make_log_provider_xml(
    name: str,
    dtsid: str = "{LP000000-0000-0000-0000-000000000001}",
    creation_name: str = "DTS.LogProviderTextFile.3",
    config_string: str = "",
    description: str = "",
    delay_validation: str | None = None,
) -> str:
    """Build a format-8 LogProvider XML fragment."""
    desc_attr = f' DTS:Description="{description}"' if description else ""
    dv_attr = f' DTS:DelayValidation="{delay_validation}"' if delay_validation else ""
    cs_attr = f' DTS:ConfigString="{config_string}"' if config_string else ""
    return (
        f'<DTS:LogProvider'
        f'  DTS:ObjectName="{name}"'
        f'  DTS:DTSID="{dtsid}"'
        f'  DTS:CreationName="{creation_name}"'
        f'  {cs_attr}{desc_attr}{dv_attr}'
        f'  xmlns:DTS="{DTS_NAMESPACE}"'
        f" />"
    )


class TestExtractLogProviders:
    """Extract LogProvider elements from package XML."""

    def test_single_log_provider_in_container(self):
        xml = wrap_xml(
            "<DTS:LogProviders>"
            + _make_log_provider_xml(
                name="TextFileLogger",
                creation_name="DTS.LogProviderTextFile.3",
                config_string="C:\\logs\\etl.log",
                description="Text file log provider",
            )
            + "</DTS:LogProviders>"
        )
        result = extract_log_providers(xml, 8)

        assert len(result) == 1
        lp = result[0]
        assert lp.name == "TextFileLogger"
        assert lp.creation_name == "DTS.LogProviderTextFile.3"
        assert lp.config_string == "C:\\logs\\etl.log"
        assert lp.description == "Text file log provider"
        assert lp.delay_validation is False

    def test_delay_validation_true(self):
        xml = wrap_xml(
            "<DTS:LogProviders>"
            + _make_log_provider_xml(name="DelayedLP", delay_validation="1")
            + "</DTS:LogProviders>"
        )
        result = extract_log_providers(xml, 8)
        assert result[0].delay_validation is True

    def test_multiple_log_providers(self):
        xml = wrap_xml(
            "<DTS:LogProviders>"
            + _make_log_provider_xml(name="LP1", dtsid="{LP-1}")
            + _make_log_provider_xml(
                name="LP2",
                dtsid="{LP-2}",
                creation_name="DTS.LogProviderSQLServer.3",
            )
            + "</DTS:LogProviders>"
        )
        result = extract_log_providers(xml, 8)
        assert len(result) == 2
        assert result[0].name == "LP1"
        assert result[1].name == "LP2"

    def test_direct_child_log_provider(self):
        """Format 2/6 style: LogProvider as direct child of root."""
        xml = wrap_xml(
            _make_log_provider_xml(name="DirectLP", dtsid="{LP-DIRECT}")
        )
        result = extract_log_providers(xml, 8)
        assert len(result) == 1
        assert result[0].name == "DirectLP"

    def test_empty_package_returns_empty_list(self):
        xml = wrap_xml("")
        result = extract_log_providers(xml, 8)
        assert result == []


class TestBuildLogProviderMap:
    """build_log_provider_map() returns dict keyed by DTSID (upper-cased)."""

    def test_map_keyed_by_dtsid(self):
        xml = wrap_xml(
            "<DTS:LogProviders>"
            + _make_log_provider_xml(name="MapLP", dtsid="{abc-def}")
            + "</DTS:LogProviders>"
        )
        providers = extract_log_providers(xml, 8)
        lp_map = build_log_provider_map(providers)

        assert "{ABC-DEF}" in lp_map
        assert lp_map["{ABC-DEF}"].name == "MapLP"

    def test_empty_list_returns_empty_map(self):
        lp_map = build_log_provider_map([])
        assert lp_map == {}

    def test_map_multiple_providers(self):
        xml = wrap_xml(
            "<DTS:LogProviders>"
            + _make_log_provider_xml(name="A", dtsid="{AAA}")
            + _make_log_provider_xml(name="B", dtsid="{BBB}")
            + "</DTS:LogProviders>"
        )
        providers = extract_log_providers(xml, 8)
        lp_map = build_log_provider_map(providers)

        assert len(lp_map) == 2
        assert lp_map["{AAA}"].name == "A"
        assert lp_map["{BBB}"].name == "B"

    def test_empty_dtsid_skipped(self):
        from models import LogProvider

        lp = LogProvider(name="NoDtsid", dtsid="", creation_name="test")
        lp_map = build_log_provider_map([lp])
        assert lp_map == {}
