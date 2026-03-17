"""Tests for xml_helpers.py — format detection, property access, safe parsers."""

import xml.etree.ElementTree as ET

import pytest

from conftest import make_dts_root, make_dts_root_f2, wrap_xml
from xml_helpers import DTS_NAMESPACE, get_format_version, get_property, safe_bool, safe_int

_NS = f"{{{DTS_NAMESPACE}}}"


# ---------------------------------------------------------------------------
# DTS_NAMESPACE constant
# ---------------------------------------------------------------------------

def test_dts_namespace_value():
    assert DTS_NAMESPACE == "www.microsoft.com/SqlServer/Dts"


# ---------------------------------------------------------------------------
# get_format_version
# ---------------------------------------------------------------------------

class TestGetFormatVersion:
    def test_format_8_via_attribute(self):
        root = make_dts_root(PackageFormatVersion="8")
        assert get_format_version(root) == 8

    def test_format_6_via_child_property(self):
        root = make_dts_root_f2(PackageFormatVersion="6")
        assert get_format_version(root) == 6

    def test_format_2_via_child_property(self):
        root = make_dts_root_f2(PackageFormatVersion="2")
        assert get_format_version(root) == 2

    def test_default_when_absent(self):
        root = ET.Element(f"{_NS}Executable")
        assert get_format_version(root) == 2


# ---------------------------------------------------------------------------
# get_property
# ---------------------------------------------------------------------------

class TestGetProperty:
    def test_attribute_format8(self):
        root = make_dts_root(ObjectName="MyPkg", Description="A test package")
        assert get_property(root, "ObjectName", 8) == "MyPkg"
        assert get_property(root, "Description", 8) == "A test package"

    def test_child_element_format2(self):
        root = make_dts_root_f2(ObjectName="OldPkg", Description="Old format")
        assert get_property(root, "ObjectName", 2) == "OldPkg"
        assert get_property(root, "Description", 2) == "Old format"

    def test_child_element_format6(self):
        root = make_dts_root_f2(ObjectName="Pkg6")
        assert get_property(root, "ObjectName", 6) == "Pkg6"

    def test_missing_property_returns_none(self):
        root = make_dts_root(ObjectName="Pkg")
        assert get_property(root, "NonExistent", 8) is None

    def test_missing_child_property_returns_none(self):
        root = make_dts_root_f2(ObjectName="Pkg")
        assert get_property(root, "NonExistent", 2) is None


# ---------------------------------------------------------------------------
# safe_bool
# ---------------------------------------------------------------------------

class TestSafeBool:
    def test_true_string(self):
        assert safe_bool("True") is True

    def test_false_string(self):
        assert safe_bool("False") is False

    def test_one(self):
        assert safe_bool("1") is True

    def test_zero(self):
        assert safe_bool("0") is False

    def test_minus_one(self):
        assert safe_bool("-1") is True

    def test_none(self):
        assert safe_bool(None) is False

    def test_empty_string(self):
        assert safe_bool("") is False


# ---------------------------------------------------------------------------
# safe_int
# ---------------------------------------------------------------------------

class TestSafeInt:
    def test_valid_integer(self):
        assert safe_int("42", "test") == 42

    def test_negative_integer(self):
        assert safe_int("-1", "test") == -1

    def test_invalid_string_returns_default(self):
        assert safe_int("abc", "test") == 0

    def test_invalid_string_custom_default(self):
        assert safe_int("abc", "test", default=99) == 99

    def test_none_returns_default(self):
        assert safe_int(None, "test") == 0

    def test_none_custom_default(self):
        assert safe_int(None, "test", default=5) == 5

    def test_empty_string_returns_default(self):
        assert safe_int("", "test", default=10) == 10
