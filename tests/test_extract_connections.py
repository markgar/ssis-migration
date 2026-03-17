"""Tests for extractors.connections – extract_connections() and build_connection_map()."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from conftest import DTS_NAMESPACE, make_connection_xml, wrap_xml

from extractors.connections import build_connection_map, extract_connections

_NS = f"{{{DTS_NAMESPACE}}}"


class TestExtractConnectionsFormat8:
    """Format 8: ConnectionManager elements inside a DTS:ConnectionManagers container."""

    def test_basic_oledb_connection(self):
        xml = wrap_xml(
            "<DTS:ConnectionManagers>"
            + make_connection_xml(
                name="ProdDB",
                dtsid="{11111111-1111-1111-1111-111111111111}",
                creation_name="OLEDB",
                connection_string="Provider=SQLOLEDB;Data Source=PRODSRV;Initial Catalog=Sales;",
            )
            + "</DTS:ConnectionManagers>"
        )
        result = extract_connections(xml, 8)

        assert len(result) == 1
        cm = result[0]
        assert cm.name == "ProdDB"
        assert cm.dtsid == "{11111111-1111-1111-1111-111111111111}"
        assert cm.creation_name == "OLEDB"
        assert "PRODSRV" in cm.connection_string
        assert cm.parsed_properties.get("Server") == "PRODSRV"
        assert cm.parsed_properties.get("Database") == "Sales"

    def test_connection_with_property_expressions(self):
        inner_xml = (
            f'<DTS:ConnectionManagers xmlns:DTS="{DTS_NAMESPACE}">'
            f'  <DTS:ConnectionManager'
            f'    DTS:refId="Package.ConnectionManagers[DynConn]"'
            f'    DTS:ObjectName="DynConn"'
            f'    DTS:DTSID="{{22222222-2222-2222-2222-222222222222}}"'
            f'    DTS:CreationName="OLEDB">'
            f"    <DTS:ObjectData>"
            f'      <DTS:ConnectionManager DTS:ConnectionString="Provider=SQLOLEDB;Data Source=.;Initial Catalog=Test;" />'
            f"    </DTS:ObjectData>"
            f'    <DTS:PropertyExpression DTS:Name="ServerName">@[User::Server]</DTS:PropertyExpression>'
            f'    <DTS:PropertyExpression DTS:Name="InitialCatalog">@[User::DB]</DTS:PropertyExpression>'
            f"  </DTS:ConnectionManager>"
            f"</DTS:ConnectionManagers>"
        )
        root = wrap_xml(inner_xml)
        result = extract_connections(root, 8)

        assert len(result) == 1
        cm = result[0]
        assert cm.property_expressions["ServerName"] == "@[User::Server]"
        assert cm.property_expressions["InitialCatalog"] == "@[User::DB]"

    def test_flat_file_connection_with_columns(self):
        inner_xml = (
            f'<DTS:ConnectionManagers xmlns:DTS="{DTS_NAMESPACE}">'
            f'  <DTS:ConnectionManager'
            f'    DTS:refId="Package.ConnectionManagers[FlatFile]"'
            f'    DTS:ObjectName="FlatFile"'
            f'    DTS:DTSID="{{33333333-3333-3333-3333-333333333333}}"'
            f'    DTS:CreationName="FLATFILE">'
            f"    <DTS:ObjectData>"
            f'      <DTS:ConnectionManager'
            f'        DTS:ConnectionString="C:\\data\\input.csv"'
            f'        DTS:Format="Delimited"'
            f'        DTS:ColumnNamesInFirstDataRow="1"'
            f'        DTS:RowDelimiter="_x000D__x000A_"'
            f'        DTS:HeaderRowsToSkip="0">'
            f'        <DTS:FlatFileColumn'
            f'          DTS:ObjectName="Col1"'
            f'          DTS:ColumnDelimiter="_x002C_"'
            f'          DTS:DataType="8"'
            f'          DTS:MaximumWidth="100"'
            f'          DTS:TextQualified="1" />'
            f'        <DTS:FlatFileColumn'
            f'          DTS:ObjectName="Col2"'
            f'          DTS:ColumnDelimiter="_x000D__x000A_"'
            f'          DTS:DataType="3"'
            f'          DTS:MaximumWidth="0" />'
            f"      </DTS:ConnectionManager>"
            f"    </DTS:ObjectData>"
            f"  </DTS:ConnectionManager>"
            f"</DTS:ConnectionManagers>"
        )
        root = wrap_xml(inner_xml)
        result = extract_connections(root, 8)

        assert len(result) == 1
        cm = result[0]
        assert cm.creation_name == "FLATFILE"
        assert cm.flat_file_format == "Delimited"
        assert cm.flat_file_header is True
        assert cm.flat_file_columns is not None
        assert len(cm.flat_file_columns) == 2
        assert cm.flat_file_columns[0].name == "Col1"
        assert cm.flat_file_columns[0].delimiter == ","  # decoded from _x002C_
        assert cm.flat_file_columns[0].text_qualified is True
        assert cm.flat_file_columns[1].name == "Col2"

    def test_multiple_connections(self):
        xml = wrap_xml(
            "<DTS:ConnectionManagers>"
            + make_connection_xml(name="Conn1", dtsid="{A1}", creation_name="OLEDB")
            + make_connection_xml(name="Conn2", dtsid="{B2}", creation_name="OLEDB")
            + "</DTS:ConnectionManagers>"
        )
        result = extract_connections(xml, 8)
        assert len(result) == 2
        names = {cm.name for cm in result}
        assert names == {"Conn1", "Conn2"}

    def test_empty_package_returns_empty_list(self):
        xml = wrap_xml("")
        result = extract_connections(xml, 8)
        assert result == []


class TestBuildConnectionMap:
    """build_connection_map() returns dict keyed by DTSID (upper-cased) and refId."""

    def test_map_keyed_by_dtsid(self):
        xml = wrap_xml(
            "<DTS:ConnectionManagers>"
            + make_connection_xml(
                name="MapConn",
                dtsid="{aaaa-bbbb}",
                creation_name="OLEDB",
            )
            + "</DTS:ConnectionManagers>"
        )
        connections = extract_connections(xml, 8)
        cmap = build_connection_map(connections)

        assert "{AAAA-BBBB}" in cmap
        assert cmap["{AAAA-BBBB}"].name == "MapConn"

    def test_map_includes_ref_id(self):
        inner_xml = (
            f'<DTS:ConnectionManagers xmlns:DTS="{DTS_NAMESPACE}">'
            f'  <DTS:ConnectionManager'
            f'    DTS:refId="Package.ConnectionManagers[RefConn]"'
            f'    DTS:ObjectName="RefConn"'
            f'    DTS:DTSID="{{CCCC-DDDD}}"'
            f'    DTS:CreationName="OLEDB">'
            f"    <DTS:ObjectData>"
            f'      <DTS:ConnectionManager DTS:ConnectionString="Provider=SQLOLEDB;" />'
            f"    </DTS:ObjectData>"
            f"  </DTS:ConnectionManager>"
            f"</DTS:ConnectionManagers>"
        )
        root = wrap_xml(inner_xml)
        connections = extract_connections(root, 8)
        cmap = build_connection_map(connections)

        assert "Package.ConnectionManagers[RefConn]" in cmap
        assert cmap["Package.ConnectionManagers[RefConn]"].name == "RefConn"

    def test_empty_connections_returns_empty_map(self):
        cmap = build_connection_map([])
        assert cmap == {}
