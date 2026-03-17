"""Tests for extractors.configurations – extract_configurations()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, wrap_xml

from extractors.configurations import extract_configurations

_NS = f"{{{DTS_NAMESPACE}}}"


def _make_config_xml(
    name: str,
    config_type: str = "1",
    config_string: str = "",
    config_value: str = "",
    enabled: str | None = None,
    dtsid: str = "{C0000000-0000-0000-0000-000000000001}",
) -> str:
    """Build a format-8 Configuration XML fragment."""
    enabled_attr = f' DTS:Enabled="{enabled}"' if enabled is not None else ""
    return (
        f'<DTS:Configuration'
        f'  DTS:ObjectName="{name}"'
        f'  DTS:ConfigurationType="{config_type}"'
        f'  DTS:ConfigurationString="{config_string}"'
        f'  DTS:ConfigurationVariable="{config_value}"'
        f'  DTS:DTSID="{dtsid}"'
        f'  {enabled_attr}'
        f'  xmlns:DTS="{DTS_NAMESPACE}"'
        f" />"
    )


class TestExtractConfigurationsFormat8:
    """Format 8: Configuration elements inside a DTS:Configurations container."""

    def test_single_configuration(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(
                name="EnvConfig",
                config_type="1",
                config_string="C:\\config.dtsconfig",
                config_value="\\Package.Variables[User::Env].Value",
            )
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)

        assert len(result) == 1
        c = result[0]
        assert c.name == "EnvConfig"
        assert c.config_type == 1
        assert c.config_string == "C:\\config.dtsconfig"
        assert c.config_value == "\\Package.Variables[User::Env].Value"
        assert c.enabled is True  # default when absent

    def test_multiple_configurations(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(name="Config1", config_string="path1.dtsconfig")
            + _make_config_xml(
                name="Config2",
                config_string="path2.dtsconfig",
                dtsid="{C0000000-0000-0000-0000-000000000002}",
            )
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)

        assert len(result) == 2
        assert result[0].name == "Config1"
        assert result[1].name == "Config2"

    def test_enabled_false(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(name="Disabled", enabled="0")
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)
        assert result[0].enabled is False

    def test_enabled_default_true(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(name="DefaultEnabled")
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)
        assert result[0].enabled is True

    def test_dtsid_captured(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(name="WithId", dtsid="{ABCD-1234}")
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)
        assert result[0].dtsid == "{ABCD-1234}"

    def test_empty_package_returns_empty_list(self):
        xml = wrap_xml("")
        result = extract_configurations(xml, 8)
        assert result == []


class TestExtractConfigurationsDirectChildren:
    """Format 2/6: Configuration elements as direct children of root."""

    def test_direct_child_config(self):
        xml = wrap_xml(
            _make_config_xml(name="DirectCfg", config_type="2")
        )
        result = extract_configurations(xml, 8)

        assert len(result) == 1
        assert result[0].name == "DirectCfg"
        assert result[0].config_type == 2

    def test_config_type_values(self):
        xml = wrap_xml(
            "<DTS:Configurations>"
            + _make_config_xml(name="XmlFile", config_type="0")
            + _make_config_xml(
                name="EnvVar",
                config_type="1",
                dtsid="{C0000000-0000-0000-0000-000000000099}",
            )
            + _make_config_xml(
                name="ParentPkg",
                config_type="3",
                dtsid="{C0000000-0000-0000-0000-000000000098}",
            )
            + "</DTS:Configurations>"
        )
        result = extract_configurations(xml, 8)
        assert result[0].config_type == 0
        assert result[1].config_type == 1
        assert result[2].config_type == 3
