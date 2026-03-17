"""Tests for extractors.metadata – extract_metadata()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, make_dts_root, make_dts_root_f2, wrap_xml

from extractors.metadata import extract_metadata

_NS = f"{{{DTS_NAMESPACE}}}"


class TestExtractMetadataFormat8:
    """Format 8: properties stored as DTS: attributes on the root element."""

    def test_basic_fields(self):
        root = make_dts_root(
            ObjectName="MyPackage",
            Description="A test package",
            CreatorName="TestUser",
            CreatorComputerName="DEVBOX",
            CreationDate="2024-01-15",
            ProtectionLevel="1",
            MaxConcurrentExecutables="4",
            VersionMajor="1",
            VersionMinor="2",
            VersionBuild="3",
            LastModifiedProductVersion="15.0.1301.433",
            EnableConfig="1",
        )

        result = extract_metadata(root, 8)

        assert result.metadata.name == "MyPackage"
        assert result.metadata.description == "A test package"
        assert result.metadata.creator == "TestUser"
        assert result.metadata.creator_machine == "DEVBOX"
        assert result.metadata.creation_date == "2024-01-15"
        assert result.metadata.protection_level == 1
        assert result.metadata.max_concurrent == 4
        assert result.metadata.version == "1.2.3"
        assert result.metadata.last_modified_version == "15.0.1301.433"
        assert result.metadata.enable_config is True
        assert result.metadata.format_version == "8"
        assert result.format_version == 8

    def test_project_deployment_model(self):
        root = make_dts_root(ObjectName="ProjPkg", PackageType="5")
        result = extract_metadata(root, 8)
        assert result.deployment_model == "Project"

    def test_package_deployment_model_default(self):
        root = make_dts_root(ObjectName="PkgPkg", PackageType="0")
        result = extract_metadata(root, 8)
        assert result.deployment_model == "Package"

    def test_missing_package_type_defaults_to_package(self):
        root = make_dts_root(ObjectName="NoPkgType")
        result = extract_metadata(root, 8)
        assert result.deployment_model == "Package"

    def test_enable_config_true_flag(self):
        root = make_dts_root(ObjectName="Cfg1", EnableConfig="True")
        assert extract_metadata(root, 8).metadata.enable_config is True

    def test_enable_config_minus_one(self):
        root = make_dts_root(ObjectName="Cfg2", EnableConfig="-1")
        assert extract_metadata(root, 8).metadata.enable_config is True

    def test_enable_config_false(self):
        root = make_dts_root(ObjectName="Cfg3", EnableConfig="0")
        assert extract_metadata(root, 8).metadata.enable_config is False


class TestExtractMetadataFormat2:
    """Format 2/6: properties stored as child DTS:Property elements."""

    def test_basic_fields(self):
        root = make_dts_root_f2(
            ObjectName="LegacyPkg",
            Description="Legacy description",
            CreatorName="LegacyUser",
            CreatorComputerName="OLDBOX",
            CreationDate="2010-06-01",
            ProtectionLevel="0",
            MaxConcurrentExecutables="-1",
            VersionMajor="2",
            VersionMinor="0",
            VersionBuild="10",
            LastModifiedProductVersion="10.0.1234",
            EnableConfig="0",
        )

        result = extract_metadata(root, 2)

        assert result.metadata.name == "LegacyPkg"
        assert result.metadata.description == "Legacy description"
        assert result.metadata.creator == "LegacyUser"
        assert result.metadata.creator_machine == "OLDBOX"
        assert result.metadata.version == "2.0.10"
        assert result.metadata.format_version == "2"
        assert result.format_version == 2

    def test_format_6(self):
        root = make_dts_root_f2(ObjectName="F6Pkg", Description="Six")
        result = extract_metadata(root, 6)
        assert result.metadata.name == "F6Pkg"
        assert result.metadata.format_version == "6"


class TestExtractMetadataDefaults:
    """Missing optional fields fall back to sensible defaults."""

    def test_missing_fields_use_defaults(self):
        root = make_dts_root()
        result = extract_metadata(root, 8)

        assert result.metadata.name == ""
        assert result.metadata.description == ""
        assert result.metadata.creator == ""
        assert result.metadata.creator_machine == ""
        assert result.metadata.creation_date == ""
        assert result.metadata.protection_level == 0
        assert result.metadata.max_concurrent == -1
        assert result.metadata.version == ""
        assert result.metadata.last_modified_version == ""
        assert result.metadata.enable_config is False

    def test_partial_version(self):
        root = make_dts_root(ObjectName="PartialVer", VersionMajor="3")
        result = extract_metadata(root, 8)
        # Missing minor/build filled with "0"
        assert result.metadata.version == "3.0.0"

    def test_returns_parsed_package(self):
        root = make_dts_root(ObjectName="Pkg")
        result = extract_metadata(root, 8)
        assert result.connections == []
        assert result.variables == []
        assert result.parameters == []
