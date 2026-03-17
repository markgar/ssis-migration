"""Tests for extractors.parameters – extract_package_parameters()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, wrap_xml

from extractors.parameters import extract_package_parameters

_NS = f"{{{DTS_NAMESPACE}}}"


def _make_param_xml(
    name: str,
    data_type: str = "8",
    default_value: str = "",
    required: str = "0",
    sensitive: str = "0",
    description: str = "",
) -> str:
    """Build a format-8 PackageParameter XML fragment."""
    desc_attr = f' DTS:Description="{description}"' if description else ""
    val_prop = (
        f'<DTS:Property DTS:Name="ParameterValue">{default_value}</DTS:Property>'
        if default_value
        else ""
    )
    return (
        f'<DTS:PackageParameter'
        f'  DTS:ObjectName="{name}"'
        f'  DTS:DataType="{data_type}"'
        f'  DTS:Required="{required}"'
        f'  DTS:Sensitive="{sensitive}"'
        f'  {desc_attr}'
        f'  xmlns:DTS="{DTS_NAMESPACE}">'
        f"  {val_prop}"
        f"</DTS:PackageParameter>"
    )


class TestExtractPackageParameters:
    """Format 8 PackageParameter extraction."""

    def test_single_parameter(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(
                name="ServerName",
                data_type="8",
                default_value="localhost",
                description="Target server",
            )
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)

        assert len(result) == 1
        p = result[0]
        assert p.name == "ServerName"
        assert "DT_" in p.data_type or "type_" in p.data_type  # resolved from code 8
        assert p.default_value == "localhost"
        assert p.description == "Target server"
        assert p.required is False
        assert p.sensitive is False

    def test_required_parameter(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(name="RequiredParam", required="1")
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)
        assert result[0].required is True

    def test_sensitive_parameter(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(name="Password", sensitive="1")
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)
        assert result[0].sensitive is True

    def test_required_and_sensitive(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(name="Secret", required="1", sensitive="1")
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)
        assert result[0].required is True
        assert result[0].sensitive is True

    def test_multiple_parameters(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(name="Param1", default_value="val1")
            + _make_param_xml(name="Param2", default_value="val2")
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)
        assert len(result) == 2
        assert result[0].name == "Param1"
        assert result[1].name == "Param2"

    def test_empty_package_returns_empty_list(self):
        xml = wrap_xml("")
        result = extract_package_parameters(xml, 8)
        assert result == []

    def test_no_container_returns_empty_list(self):
        root = ET.Element(f"{_NS}Executable")
        result = extract_package_parameters(root, 8)
        assert result == []

    def test_integer_data_type(self):
        xml = wrap_xml(
            "<DTS:PackageParameters>"
            + _make_param_xml(name="BatchSize", data_type="3", default_value="100")
            + "</DTS:PackageParameters>"
        )
        result = extract_package_parameters(xml, 8)
        assert "Int" in result[0].data_type or "DT_I4" in result[0].data_type
