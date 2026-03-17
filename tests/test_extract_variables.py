"""Tests for extractors.variables – extract_variables() and extract_package_variables()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, make_variable_xml, wrap_xml

from extractors.variables import extract_package_variables, extract_variables

_NS = f"{{{DTS_NAMESPACE}}}"


class TestExtractVariablesFormat8:
    """Format 8: DTS:Variable elements inside a DTS:Variables container."""

    def test_single_variable(self):
        xml = wrap_xml(
            "<DTS:Variables>"
            f'  {make_variable_xml(name="Counter", namespace="User", data_type="3", value="0")}'
            "</DTS:Variables>"
        )
        result = extract_variables(xml, 8)

        assert len(result) == 1
        v = result[0]
        assert v.name == "Counter"
        assert v.namespace == "User"
        assert v.data_type == 3
        assert v.value == "0"
        assert v.scope == "Package"
        assert v.source == "Variable"
        assert v.expression is None
        assert v.evaluate_as_expression is False

    def test_variable_with_expression(self):
        xml = wrap_xml(
            "<DTS:Variables>"
            f'  {make_variable_xml(name="DynPath", namespace="User", data_type="8", value="/tmp", expression="@[User::BasePath] + @[User::FileName]")}'
            "</DTS:Variables>"
        )
        result = extract_variables(xml, 8)

        assert len(result) == 1
        v = result[0]
        assert v.name == "DynPath"
        assert v.expression == "@[User::BasePath] + @[User::FileName]"
        assert v.evaluate_as_expression is True

    def test_multiple_data_types(self):
        xml = wrap_xml(
            "<DTS:Variables>"
            f'  {make_variable_xml(name="IntVar", namespace="User", data_type="3", value="42")}'
            f'  {make_variable_xml(name="StrVar", namespace="User", data_type="8", value="hello")}'
            f'  {make_variable_xml(name="BoolVar", namespace="User", data_type="11", value="true")}'
            "</DTS:Variables>"
        )
        result = extract_variables(xml, 8)

        assert len(result) == 3
        assert result[0].data_type == 3
        assert result[0].value == "42"
        assert result[1].data_type == 8
        assert result[1].value == "hello"
        assert result[2].data_type == 11
        assert result[2].value == "true"

    def test_empty_package_returns_empty_list(self):
        xml = wrap_xml("")
        result = extract_variables(xml, 8)
        assert result == []

    def test_custom_scope(self):
        xml = wrap_xml(
            "<DTS:Variables>"
            f'  {make_variable_xml(name="TaskVar", namespace="User", data_type="8", value="val")}'
            "</DTS:Variables>"
        )
        result = extract_variables(xml, 8, scope="ExecuteSQL Task")
        assert result[0].scope == "ExecuteSQL Task"

    def test_system_namespace_variable(self):
        xml = wrap_xml(
            "<DTS:Variables>"
            f'  {make_variable_xml(name="PackageName", namespace="System", data_type="8", value="MyPkg")}'
            "</DTS:Variables>"
        )
        result = extract_variables(xml, 8)
        assert len(result) == 1
        assert result[0].namespace == "System"


class TestExtractPackageVariablesFormat2:
    """Format 2/6: DTS:PackageVariable elements with child Property elements."""

    def test_single_package_variable(self):
        xml_str = (
            f'<DTS:Executable xmlns:DTS="{DTS_NAMESPACE}">'
            f"  <DTS:PackageVariable>"
            f'    <DTS:Property DTS:Name="ObjectName">PkgVar1</DTS:Property>'
            f'    <DTS:Property DTS:Name="Namespace">User</DTS:Property>'
            f'    <DTS:Property DTS:Name="DTSID">{{AAA}}</DTS:Property>'
            f'    <DTS:Property DTS:Name="Description">A var</DTS:Property>'
            f'    <DTS:Property DTS:Name="PackageVariableValue" DTS:DataType="8">someValue</DTS:Property>'
            f"  </DTS:PackageVariable>"
            f"</DTS:Executable>"
        )
        root = ET.fromstring(xml_str)
        result = extract_package_variables(root, 2)

        assert len(result) == 1
        v = result[0]
        assert v.name == "PkgVar1"
        assert v.namespace == "User"
        assert v.value == "someValue"
        assert v.data_type == 8
        assert v.scope == "Package"
        assert v.source == "PackageVariable"

    def test_designer_namespace_filtered(self):
        xml_str = (
            f'<DTS:Executable xmlns:DTS="{DTS_NAMESPACE}">'
            f"  <DTS:PackageVariable>"
            f'    <DTS:Property DTS:Name="ObjectName">DesignVar</DTS:Property>'
            f'    <DTS:Property DTS:Name="Namespace">dts-designer-1.0</DTS:Property>'
            f"  </DTS:PackageVariable>"
            f"</DTS:Executable>"
        )
        root = ET.fromstring(xml_str)
        result = extract_package_variables(root, 2)
        assert result == []

    def test_empty_package_returns_empty(self):
        xml = wrap_xml("")
        result = extract_package_variables(xml, 2)
        assert result == []
