"""Component class ID resolution and SSIS data type mapping."""

from __future__ import annotations

import sys


# ---------------------------------------------------------------------------
# Component class ID resolution — GUID map (format 2/6)
# ---------------------------------------------------------------------------

_GUID_MAP: dict[str, str] = {
    "{5ACD952A-F16A-41D8-A681-713640837664}": "Flat File Source",
    "{D7A35918-FC18-4E45-B132-663BFEC2CCA8}": "OLE DB Source",
    "{5A0B62E8-D91D-49F5-94A5-7BE58DE508F0}": "OLE DB Destination",
    "{27648839-180F-45E6-838D-AFF53DF682D2}": "Lookup",
    "{2932025B-AB99-40F6-B5B8-783A73F80E24}": "Derived Column",
    "{7FB4A470-3EA9-4F3A-AEA9-B547AC652B3F}": "Conditional Split",
    "{E2568105-9550-4F71-A612-8B8514B2B751}": "Flat File Destination",
    "{AACE008B-11A6-4B29-B5F4-3B6E28E40B5C}": "Aggregate",
    "{2BF22E78-C52D-4BCB-A01A-9A88B031E90F}": "Sort",
    "{30B1A26D-B27C-401A-B86C-E2CBC184E9DC}": "Union All",
    "{EC139FBC-694E-490B-8EA7-35B16B5D9F2D}": "Multicast",
    "{19DBA13E-4A2F-4C26-9485-4A0F88B795F1}": "Row Count",
    "{847A949B-DBFC-4B5A-A2B2-3B1B0B16EFC4}": "Data Conversion",
    "{671046B0-AA63-4C9F-90E4-C06E0B710CE3}": "OLE DB Command",
    "{3C5E3076-E12C-4FB7-B963-C168C78B2E74}": "Merge Join",
    "{0617AA7D-4176-4AF0-8684-96BCAF85B36F}": "Script Component",
    "{2E42D45B-F83C-400F-8D77-61DDE6A7DF29}": "Script Component",
    "{D658C424-8CF0-441C-B3C4-955E183B7FBA}": "Flat File Destination",
}

# ---------------------------------------------------------------------------
# Component class ID resolution — short-name map (format 8)
# ---------------------------------------------------------------------------

_SHORT_NAME_MAP: dict[str, str] = {
    "Microsoft.OLEDBSource": "OLE DB Source",
    "Microsoft.OLEDBDestination": "OLE DB Destination",
    "Microsoft.FlatFileSource": "Flat File Source",
    "Microsoft.FlatFileDestination": "Flat File Destination",
    "Microsoft.ExcelSource": "Excel Source",
    "Microsoft.Lookup": "Lookup",
    "Microsoft.DerivedColumn": "Derived Column",
    "Microsoft.ConditionalSplit": "Conditional Split",
    "Microsoft.Multicast": "Multicast",
    "Microsoft.UnionAll": "Union All",
    "Microsoft.RowCount": "Row Count",
    "Microsoft.DataConvert": "Data Conversion",
    "Microsoft.OLEDBCommand": "OLE DB Command",
    "Microsoft.MergeJoin": "Merge Join",
    "Microsoft.Merge": "Merge",
    "Microsoft.Aggregate": "Aggregate",
    "Microsoft.Sort": "Sort",
    "Microsoft.ManagedComponentHost": "ManagedComponentHost",
    "Microsoft.SCD": "Slowly Changing Dimension",
    "Microsoft.BDD": "Balanced Data Distributor",
    "Microsoft.PctSampling": "Percentage Sampling",
    "Microsoft.RowSampling": "Row Sampling",
}


def resolve_component_class(
    class_id: str,
    description: str = "",
    user_component_type_name: str = "",
) -> str:
    """Resolve a componentClassID to a human-readable display name.

    Resolution order:
      1. GUID map (format 2/6 GUIDs, case-insensitive).
      2. Short-name map (format 8 names).
      3. For ``ManagedComponentHost``, use the last dotted segment of
         *user_component_type_name*.
      4. Fall back to *description* if non-empty.
      5. Fall back to the raw *class_id*.
    """
    # 1. GUID map (case-insensitive for GUIDs)
    name = _GUID_MAP.get(class_id) or _GUID_MAP.get(class_id.upper())
    if name is not None:
        return name

    # 2. Short-name map
    name = _SHORT_NAME_MAP.get(class_id)
    if name is not None:
        # 3. ManagedComponentHost — extract last dotted segment
        if name == "ManagedComponentHost" and user_component_type_name:
            last_segment = user_component_type_name.rsplit(".", 1)[-1]
            if last_segment:
                return last_segment
        return name

    # 4. Description fallback
    if description:
        print(
            f"Warning: unknown componentClassID {class_id!r}, "
            f"using description {description!r}",
            file=sys.stderr,
        )
        return description

    # 5. Raw class_id fallback
    if class_id:
        print(
            f"Warning: unknown componentClassID {class_id!r}, "
            f"no description available",
            file=sys.stderr,
        )
    return class_id


# ---------------------------------------------------------------------------
# OLE Automation variant type codes (used for SSIS variables)
# ---------------------------------------------------------------------------

_OLE_VARIANT_MAP: dict[int, str] = {
    0: "Empty",
    2: "Int16",
    3: "Int32",
    4: "Single",
    5: "Double",
    6: "Currency",
    7: "DateTime",
    8: "String",
    11: "Boolean",
    12: "Object",
    13: "Object",
    16: "SByte",
    17: "Byte",
    18: "UInt16",
    19: "UInt32",
    20: "Int64",
    21: "UInt64",
}


def resolve_ole_variant_type(code: int) -> str:
    """Resolve an OLE variant type code to a human-readable name."""
    display = _OLE_VARIANT_MAP.get(code)
    if display is not None:
        return display
    return f"type_{code}"


# ---------------------------------------------------------------------------
# SSIS data type map
# ---------------------------------------------------------------------------

_DATA_TYPE_MAP: dict[int, str] = {
    2: "DT_I2 (Int16)",
    3: "DT_I4 (Int32)",
    4: "DT_R4 (Single)",
    5: "DT_R8 (Double)",
    6: "DT_CY (Currency)",
    7: "DT_DATE (DateTime)",
    8: "DT_BSTR (String)",
    11: "DT_BOOL (Boolean)",
    13: "DT_UNKNOWN (Object)",
    17: "DT_UI1 (Byte)",
    18: "DT_UI2 (UInt16)",
    19: "DT_UI4 (UInt32)",
    20: "DT_I8 (Int64)",
    21: "DT_UI8 (UInt64)",
    72: "DT_GUID (Guid)",
    128: "DT_BYTES (Byte[])",
    129: "DT_STR (ANSI String)",
    130: "DT_WSTR (Unicode String)",
    131: "DT_NUMERIC (Decimal)",
    133: "DT_DBDATE (Date)",
    134: "DT_DBTIME (Time)",
    135: "DT_DBTIMESTAMP (DateTime)",
    139: "DT_NUMERIC (Decimal)",
    145: "DT_DBTIME2 (Time)",
    146: "DT_DBTIMESTAMP2 (DateTime)",
}


def resolve_data_type(code: int) -> str:
    """Resolve an integer SSIS data-type code to a human-readable string.

    Returns ``type_{code}`` for unknown codes (with a warning to stderr).
    """
    display = _DATA_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown SSIS data type code {code}, using type_{code}",
        file=sys.stderr,
    )
    return f"type_{code}"


# ---------------------------------------------------------------------------
# Lookup-specific resolution
# ---------------------------------------------------------------------------

_CACHE_TYPE_MAP: dict[int, str] = {
    0: "Full",
    1: "Partial",
    2: "None",
}


def resolve_cache_type(value: str) -> str:
    """Resolve a CacheType integer string to a display name.

    Returns ``cache_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: CacheType is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _CACHE_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown CacheType {code}, using cache_{code}",
        file=sys.stderr,
    )
    return f"cache_{code}"


_NO_MATCH_BEHAVIOR_MAP: dict[int, str] = {
    0: "FailComponent",
    1: "IgnoreFailure",
    2: "RedirectRowsToNoMatchOutput",
}


def resolve_no_match_behavior(value: str) -> str:
    """Resolve a NoMatchBehavior integer string to a display name.

    Returns ``nomatch_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: NoMatchBehavior is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _NO_MATCH_BEHAVIOR_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown NoMatchBehavior {code}, using nomatch_{code}",
        file=sys.stderr,
    )
    return f"nomatch_{code}"


# ---------------------------------------------------------------------------
# OLE DB access mode resolution
# ---------------------------------------------------------------------------

_OLEDB_SOURCE_ACCESS_MODE_MAP: dict[int, str] = {
    0: "Table or View",
    1: "Table Name or View Name Variable",
    2: "SQL Command",
    3: "SQL Command from Variable",
}

_OLEDB_DEST_ACCESS_MODE_MAP: dict[int, str] = {
    0: "Table or View",
    1: "Table or View (Fast Load)",
    2: "Table Name or View Name Variable",
    3: "Table Name or View Name Variable (Fast Load)",
}


def resolve_oledb_access_mode(value: str, is_destination: bool = False) -> str:
    """Resolve an OLE DB AccessMode integer string to a display name.

    Uses the destination map when *is_destination* is True.
    Returns ``access_mode_{value}`` for unknown codes (with a warning to stderr).
    """
    mode_map = _OLEDB_DEST_ACCESS_MODE_MAP if is_destination else _OLEDB_SOURCE_ACCESS_MODE_MAP
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: AccessMode is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = mode_map.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown AccessMode {code}, using access_mode_{code}",
        file=sys.stderr,
    )
    return f"access_mode_{code}"


# ---------------------------------------------------------------------------
# Merge Join type resolution
# ---------------------------------------------------------------------------

_JOIN_TYPE_MAP: dict[int, str] = {
    0: "Inner",
    1: "LeftOuter",
    2: "FullOuter",
}


def resolve_join_type(value: str) -> str:
    """Resolve a JoinType integer string to a display name.

    Returns ``join_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: JoinType is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _JOIN_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown JoinType {code}, using join_{code}",
        file=sys.stderr,
    )
    return f"join_{code}"


# ---------------------------------------------------------------------------
# Aggregate type resolution
# ---------------------------------------------------------------------------

_AGGREGATION_TYPE_MAP: dict[int, str] = {
    0: "GroupBy",
    1: "Count",
    2: "CountAll",
    3: "CountDistinct",
    4: "Sum",
    5: "Avg",
    6: "Min",
    7: "Max",
}


def resolve_aggregation_type(value: str) -> str:
    """Resolve an AggregationType integer string to a display name.

    Returns ``agg_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: AggregationType is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _AGGREGATION_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown AggregationType {code}, using agg_{code}",
        file=sys.stderr,
    )
    return f"agg_{code}"


# ---------------------------------------------------------------------------
# SCD column type resolution
# ---------------------------------------------------------------------------

_SCD_COLUMN_TYPE_MAP: dict[int, str] = {
    1: "Business Key",
    2: "Fixed Attribute",
    3: "Historical Attribute",
    4: "Changing Attribute",
}


def resolve_scd_column_type(value: str) -> str:
    """Resolve an SCD ColumnType integer string to a display name.

    Returns ``scd_type_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: SCD ColumnType is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _SCD_COLUMN_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown SCD ColumnType {code}, using scd_type_{code}",
        file=sys.stderr,
    )
    return f"scd_type_{code}"


# ---------------------------------------------------------------------------
# Excel access mode resolution
# ---------------------------------------------------------------------------

_EXCEL_ACCESS_MODE_MAP: dict[int, str] = {
    0: "Table",
    1: "SQL Command",
}


# ---------------------------------------------------------------------------
# Configuration type resolution
# ---------------------------------------------------------------------------

_CONFIG_TYPE_MAP: dict[int, str] = {
    0: "Parent Package Variable",
    1: "XML Configuration File",
    2: "Environment Variable",
    3: "Registry Entry",
    4: "Parent Package Variable (Indirect)",
    5: "SQL Server Table",
}


def resolve_config_type(value: str) -> str:
    """Resolve a ConfigurationType integer string to a display name.

    Returns ``config_type_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: ConfigurationType is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _CONFIG_TYPE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown ConfigurationType {code}, using config_type_{code}",
        file=sys.stderr,
    )
    return f"config_type_{code}"


def resolve_excel_access_mode(value: str) -> str:
    """Resolve an Excel AccessMode integer string to a display name.

    Returns ``excel_mode_{value}`` for unknown codes (with a warning to stderr).
    """
    try:
        code = int(value)
    except (ValueError, TypeError):
        if value:
            print(
                f"Warning: Excel AccessMode is not a valid integer ({value!r}), "
                f"using raw value",
                file=sys.stderr,
            )
        return value
    display = _EXCEL_ACCESS_MODE_MAP.get(code)
    if display is not None:
        return display
    print(
        f"Warning: unknown Excel AccessMode {code}, using excel_mode_{code}",
        file=sys.stderr,
    )
    return f"excel_mode_{code}"


# ---------------------------------------------------------------------------
# Log provider type resolution
# ---------------------------------------------------------------------------

LOG_PROVIDER_TYPE_MAP: dict[str, str] = {
    "DTS.LogProviderTextFile.2": "Text/CSV File",
    "DTS.LogProviderSQLServer.2": "SQL Server Table",
    "DTS.LogProviderEventLog.2": "Windows Event Log",
    "DTS.LogProviderXMLFile.2": "XML File",
    "DTS.LogProviderSQLProfiler.2": "SQL Profiler Trace",
}


def resolve_log_provider_type(creation_name: str) -> str:
    """Resolve a log provider CreationName to a human-readable display type.

    Returns the raw *creation_name* for unknown values (with a warning to stderr).
    """
    display = LOG_PROVIDER_TYPE_MAP.get(creation_name)
    if display is not None:
        return display
    if creation_name:
        print(
            f"Warning: unknown log provider type {creation_name!r}, "
            f"using raw value",
            file=sys.stderr,
        )
    return creation_name
