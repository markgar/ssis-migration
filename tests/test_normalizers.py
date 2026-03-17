"""Tests for normalizers.py — escape decoding and CreationName normalization."""

import pytest

from normalizers import decode_escapes, normalize_creation_name


# ---------------------------------------------------------------------------
# decode_escapes
# ---------------------------------------------------------------------------

class TestDecodeEscapes:
    def test_space(self):
        assert decode_escapes("_x0020_") == " "

    def test_colon(self):
        assert decode_escapes("_x003A_") == ":"

    def test_multiple_escapes(self):
        assert decode_escapes("A_x0020_B_x003A_C") == "A B:C"

    def test_no_escapes(self):
        assert decode_escapes("plain_text") == "plain_text"

    def test_empty_string(self):
        assert decode_escapes("") == ""

    def test_hex_upper_and_lower(self):
        # Both upper and lower hex digits should work
        assert decode_escapes("_x004F_") == "O"  # uppercase hex
        assert decode_escapes("_x006f_") == "o"  # lowercase hex

    def test_consecutive_escapes(self):
        assert decode_escapes("_x0041__x0042_") == "AB"


# ---------------------------------------------------------------------------
# normalize_creation_name
# ---------------------------------------------------------------------------

class TestNormalizeCreationName:
    def test_guid_pipeline(self):
        assert normalize_creation_name("{5918251B-2970-45A4-AB5F-01C3C48C3BBD}") == "Pipeline"

    def test_microsoft_prefix(self):
        assert normalize_creation_name("Microsoft.ExecuteSQLTask") == "ExecuteSQLTask"

    def test_stock_name(self):
        assert normalize_creation_name("STOCK:SEQUENCE") == "Sequence"
        assert normalize_creation_name("STOCK:FOREACHLOOP") == "ForEachLoop"
        assert normalize_creation_name("STOCK:FORLOOP") == "ForLoop"

    def test_short_format8_names(self):
        assert normalize_creation_name("Microsoft.SendMailTask") == "SendMailTask"
        assert normalize_creation_name("Microsoft.ScriptTask") == "ScriptTask"
        assert normalize_creation_name("Microsoft.Package") == "Package"

    def test_substring_match_assembly_qualified(self):
        long_name = "Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask, ..."
        assert normalize_creation_name(long_name) == "ExecuteSQLTask"

    def test_unknown_passthrough(self):
        assert normalize_creation_name("SomeUnknownTask") == "SomeUnknownTask"

    def test_empty_passthrough(self):
        assert normalize_creation_name("") == ""

    def test_ssis_pipeline_format2(self):
        assert normalize_creation_name("SSIS.Pipeline.2") == "Pipeline"
