"""Shared test fixtures for ssis-analyzer-skill."""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

# Add scripts/ to sys.path so tests can import modules directly
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "skills" / "ssis-analyzer" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Test fixture directories
SAMPLE_PACKAGES_DIR = Path(__file__).resolve().parent / "fixtures"
DAILY_ETL_DIR = SAMPLE_PACKAGES_DIR / "Daily.ETL"
DAILY_ETL_DTSX = DAILY_ETL_DIR / "DailyETLMain.dtsx"
CONTROL_FLOW_DIR = SAMPLE_PACKAGES_DIR / "ControlFlowTraining"
DWH_DIR = SAMPLE_PACKAGES_DIR / "DWH-Loading"

# DTS namespace used across all XML
DTS_NAMESPACE = "www.microsoft.com/SqlServer/Dts"
_NS = f"{{{DTS_NAMESPACE}}}"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def daily_etl_path() -> Path:
    """Path to the DailyETLMain.dtsx test fixture."""
    assert DAILY_ETL_DTSX.exists(), f"Test fixture not found: {DAILY_ETL_DTSX}"
    return DAILY_ETL_DTSX


@pytest.fixture
def daily_etl_dir() -> Path:
    """Path to the Daily.ETL directory."""
    assert DAILY_ETL_DIR.exists(), f"Test fixture dir not found: {DAILY_ETL_DIR}"
    return DAILY_ETL_DIR


@pytest.fixture
def loaded_package(daily_etl_path):
    """A fully loaded ParsedPackage from DailyETLMain.dtsx."""
    from loader import load_package
    return load_package(daily_etl_path)


# ---------------------------------------------------------------------------
# XML builder helpers for synthetic tests
# ---------------------------------------------------------------------------

def make_dts_root(**attrs: str) -> ET.Element:
    """Build a minimal format-8 DTS root element with attributes."""
    root = ET.Element(f"{_NS}Executable")
    for name, value in attrs.items():
        root.set(f"{_NS}{name}", value)
    return root


def make_dts_root_f2(**props: str) -> ET.Element:
    """Build a minimal format-2/6 root element with child Property elements."""
    root = ET.Element(f"{_NS}Executable")
    for name, value in props.items():
        prop = ET.SubElement(root, f"{_NS}Property")
        prop.set(f"{_NS}Name", name)
        prop.text = value
    return root


def wrap_xml(inner_xml: str) -> ET.Element:
    """Wrap XML fragments in a DTS root element and parse."""
    xml_str = (
        f'<DTS:Executable xmlns:DTS="{DTS_NAMESPACE}">'
        f"{inner_xml}"
        f"</DTS:Executable>"
    )
    return ET.fromstring(xml_str)


def make_connection_xml(
    name: str = "TestConn",
    dtsid: str = "{00000000-0000-0000-0000-000000000001}",
    creation_name: str = "OLEDB",
    connection_string: str = "Provider=SQLOLEDB;Server=.;Database=Test;",
    **extra_attrs: str,
) -> str:
    """Build a format-8 ConnectionManager XML fragment."""
    attrs = " ".join(f'DTS:{k}="{v}"' for k, v in extra_attrs.items())
    return (
        f'<DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[{name}]"'
        f'  DTS:ObjectName="{name}"'
        f'  DTS:DTSID="{dtsid}"'
        f'  DTS:CreationName="{creation_name}"'
        f'  {attrs}'
        f'  xmlns:DTS="{DTS_NAMESPACE}">'
        f"  <DTS:ObjectData>"
        f'    <DTS:ConnectionManager DTS:ConnectionString="{connection_string}" />'
        f"  </DTS:ObjectData>"
        f"</DTS:ConnectionManager>"
    )


def make_variable_xml(
    name: str = "TestVar",
    namespace: str = "User",
    data_type: str = "8",
    value: str = "hello",
    expression: str | None = None,
) -> str:
    """Build a format-8 Variable XML fragment."""
    expr_attr = f' DTS:Expression="{expression}" DTS:EvaluateAsExpression="True"' if expression else ""
    return (
        f'<DTS:Variable DTS:ObjectName="{name}"'
        f'  DTS:Namespace="{namespace}"'
        f'  DTS:DTSID="{{00000000-0000-0000-0000-000000000002}}"'
        f'  xmlns:DTS="{DTS_NAMESPACE}"'
        f"  {expr_attr}>"
        f"  <DTS:VariableValue"
        f'    DTS:DataType="{data_type}">{value}</DTS:VariableValue>'
        f"</DTS:Variable>"
    )
