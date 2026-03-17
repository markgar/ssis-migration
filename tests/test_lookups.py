"""Tests for lookups.py — component class and data type resolution."""

from lookups import resolve_component_class, resolve_data_type


# ---------------------------------------------------------------------------
# resolve_component_class
# ---------------------------------------------------------------------------

class TestResolveComponentClass:
    def test_known_guid(self):
        assert resolve_component_class("{D7A35918-FC18-4E45-B132-663BFEC2CCA8}") == "OLE DB Source"

    def test_guid_case_insensitive(self):
        assert resolve_component_class("{d7a35918-fc18-4e45-b132-663bfec2cca8}") == "OLE DB Source"

    def test_known_short_name(self):
        assert resolve_component_class("Microsoft.OLEDBSource") == "OLE DB Source"
        assert resolve_component_class("Microsoft.Lookup") == "Lookup"

    def test_short_name_derived_column(self):
        assert resolve_component_class("Microsoft.DerivedColumn") == "Derived Column"

    def test_unknown_falls_back_to_description(self):
        assert resolve_component_class("Unknown.Thing", description="My Custom Transform") == "My Custom Transform"

    def test_unknown_no_description_returns_raw(self):
        assert resolve_component_class("Unknown.Thing") == "Unknown.Thing"

    def test_empty_class_id(self):
        assert resolve_component_class("") == ""

    def test_flat_file_source_guid(self):
        assert resolve_component_class("{5ACD952A-F16A-41D8-A681-713640837664}") == "Flat File Source"

    def test_sort_guid(self):
        assert resolve_component_class("{2BF22E78-C52D-4BCB-A01A-9A88B031E90F}") == "Sort"

    def test_managed_component_host_extracts_last_segment(self):
        result = resolve_component_class(
            "Microsoft.ManagedComponentHost",
            user_component_type_name="Microsoft.SqlServer.Dts.Pipeline.DataReaderSourceAdapter",
        )
        assert result == "DataReaderSourceAdapter"


# ---------------------------------------------------------------------------
# resolve_data_type
# ---------------------------------------------------------------------------

class TestResolveDataType:
    def test_known_int32(self):
        assert resolve_data_type(3) == "DT_I4 (Int32)"

    def test_known_string(self):
        assert resolve_data_type(8) == "DT_BSTR (String)"

    def test_known_boolean(self):
        assert resolve_data_type(11) == "DT_BOOL (Boolean)"

    def test_known_unicode_string(self):
        assert resolve_data_type(130) == "DT_WSTR (Unicode String)"

    def test_known_guid(self):
        assert resolve_data_type(72) == "DT_GUID (Guid)"

    def test_known_datetime(self):
        assert resolve_data_type(135) == "DT_DBTIMESTAMP (DateTime)"

    def test_unknown_code(self):
        result = resolve_data_type(9999)
        assert result == "type_9999"
