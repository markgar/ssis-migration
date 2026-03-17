"""Tests for models.py — dataclass construction, defaults, and mutability."""

from models import (
    Component,
    ConnectionManager,
    DataFlow,
    Executable,
    PackageMetadata,
    ParsedPackage,
    Variable,
)


# ---------------------------------------------------------------------------
# PackageMetadata
# ---------------------------------------------------------------------------

class TestPackageMetadata:
    def test_minimal_construction(self):
        m = PackageMetadata(name="Pkg")
        assert m.name == "Pkg"

    def test_defaults(self):
        m = PackageMetadata(name="Pkg")
        assert m.description == ""
        assert m.creator == ""
        assert m.format_version == ""
        assert m.protection_level == 0
        assert m.max_concurrent == -1
        assert m.enable_config is False

    def test_mutable(self):
        m = PackageMetadata(name="A")
        m.name = "B"
        assert m.name == "B"


# ---------------------------------------------------------------------------
# ConnectionManager
# ---------------------------------------------------------------------------

class TestConnectionManager:
    def test_minimal_construction(self):
        cm = ConnectionManager(name="CM1", dtsid="{00}", creation_name="OLEDB")
        assert cm.name == "CM1"
        assert cm.dtsid == "{00}"
        assert cm.creation_name == "OLEDB"

    def test_defaults(self):
        cm = ConnectionManager(name="CM1", dtsid="{00}", creation_name="OLEDB")
        assert cm.description == ""
        assert cm.connection_string == ""
        assert cm.delay_validation is False
        assert cm.property_expressions == {}
        assert cm.flat_file_format is None
        assert cm.flat_file_columns is None
        assert cm.is_project_level is False
        assert cm.parsed_properties == {}


# ---------------------------------------------------------------------------
# Variable
# ---------------------------------------------------------------------------

class TestVariable:
    def test_minimal_construction(self):
        v = Variable(namespace="User", name="Var1", data_type=8, scope="Package", source="pkg.dtsx")
        assert v.namespace == "User"
        assert v.name == "Var1"
        assert v.data_type == 8

    def test_defaults(self):
        v = Variable(namespace="User", name="V", data_type=3, scope="Pkg", source="x")
        assert v.value == ""
        assert v.expression is None
        assert v.evaluate_as_expression is False
        assert v.read_only is False
        assert v.description == ""
        assert v.dtsid == ""


# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------

class TestExecutable:
    def test_minimal_construction(self):
        e = Executable(name="Task1", creation_name="ExecuteSQLTask")
        assert e.name == "Task1"
        assert e.creation_name == "ExecuteSQLTask"

    def test_defaults(self):
        e = Executable(name="T", creation_name="C")
        assert e.disabled is False
        assert e.children == []
        assert e.precedence_constraints == []
        assert e.variables == []
        assert e.property_expressions == {}
        assert e.event_handlers == []
        assert e.execute_sql is None
        assert e.data_flow is None
        assert e.logging is None
        assert e.max_error_count == 1
        assert e.fail_package_on_failure is False
        assert e.delay_validation is False
        assert e.generic_payload is None

    def test_mutable(self):
        e = Executable(name="T", creation_name="C")
        e.disabled = True
        assert e.disabled is True
        e.children.append(Executable(name="Child", creation_name="X"))
        assert len(e.children) == 1


# ---------------------------------------------------------------------------
# DataFlow
# ---------------------------------------------------------------------------

class TestDataFlow:
    def test_minimal_construction(self):
        df = DataFlow()
        assert df.components == []
        assert df.paths == []

    def test_defaults(self):
        df = DataFlow()
        assert df.default_buffer_max_rows is None
        assert df.default_buffer_size is None
        assert df.engine_threads is None


# ---------------------------------------------------------------------------
# Component
# ---------------------------------------------------------------------------

class TestComponent:
    def test_minimal_construction(self):
        c = Component(id=1, name="Src", class_id="Microsoft.OLEDBSource", class_name="OLE DB Source")
        assert c.id == 1
        assert c.name == "Src"

    def test_defaults(self):
        c = Component(id=1, name="C", class_id="X", class_name="Y")
        assert c.outputs == []
        assert c.inputs == []
        assert c.connections == []
        assert c.properties == {}
        assert c.reference_columns == []
        assert c.parameter_mappings == []
        assert c.script_data is None


# ---------------------------------------------------------------------------
# ParsedPackage
# ---------------------------------------------------------------------------

class TestParsedPackage:
    def test_minimal_construction(self):
        meta = PackageMetadata(name="Pkg")
        pkg = ParsedPackage(metadata=meta, format_version=8)
        assert pkg.metadata.name == "Pkg"
        assert pkg.format_version == 8

    def test_defaults(self):
        meta = PackageMetadata(name="Pkg")
        pkg = ParsedPackage(metadata=meta, format_version=8)
        assert pkg.deployment_model == "Package"
        assert pkg.connections == []
        assert pkg.connection_map == {}
        assert pkg.variables == []
        assert pkg.parameters == []
        assert pkg.configurations == []
        assert pkg.log_providers == []
        assert pkg.executables == []
        assert pkg.root_precedence_constraints == []
        assert pkg.root_event_handlers == []
        assert pkg.variable_references == []

    def test_mutable(self):
        meta = PackageMetadata(name="Pkg")
        pkg = ParsedPackage(metadata=meta, format_version=2)
        pkg.format_version = 8
        assert pkg.format_version == 8
