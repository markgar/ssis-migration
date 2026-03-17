"""Extract connection managers from .dtsx and .conmgr files."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from lookups import resolve_data_type
from models import ConnectionManager, FlatFileColumn
from normalizers import decode_escapes
from xml_helpers import (
    DTS_NAMESPACE,
    get_format_version,
    get_property,
    safe_bool,
    safe_int,
)

_NS = f"{{{DTS_NAMESPACE}}}"

_FILE_USAGE_MAP: dict[int, str] = {
    0: "ExistingFile",
    1: "CreateFile",
    2: "ExistingFolder",
    3: "CreateFolder",
}


def _extract_connection_string(
    obj_data_cm: ET.Element | None,
    format_version: int,
) -> str:
    """Resolve connection string from the ObjectData/ConnectionManager element.

    Location varies by format version:
    - Format 2/6: child property element with Name='ConnectionString'
    - Format 8: attribute {DTS}ConnectionString
    - Fallback: get_property() with 'ConnectionString'
    """
    if obj_data_cm is None:
        return ""

    if format_version >= 8:
        # Format 8: attribute on the ConnectionManager element
        val = obj_data_cm.get(f"{_NS}ConnectionString")
        if val is not None:
            return val

    # Format 2/6: child DTS:Property with DTS:Name='ConnectionString'
    for prop in obj_data_cm.findall(f"{_NS}Property"):
        if prop.get(f"{_NS}Name") == "ConnectionString":
            return prop.text or ""

    # Fallback via get_property on the ObjectData/ConnectionManager
    val = get_property(obj_data_cm, "ConnectionString", format_version)
    if val is not None:
        return val

    return ""


def _extract_property_expressions(
    cm_element: ET.Element,
) -> dict[str, str]:
    """Extract property expressions from DTS:PropertyExpression children."""
    expressions: dict[str, str] = {}
    for expr_el in cm_element.findall(f"{_NS}PropertyExpression"):
        prop_name = expr_el.get(f"{_NS}Name")
        if prop_name is not None:
            expressions[prop_name] = expr_el.text or ""
    return expressions


def _get_obj_data_cm(cm_element: ET.Element) -> ET.Element | None:
    """Get the DTS:ObjectData/DTS:ConnectionManager element."""
    obj_data = cm_element.find(f"{_NS}ObjectData")
    if obj_data is None:
        return None
    return obj_data.find(f"{_NS}ConnectionManager")


def _extract_flat_file_columns(
    obj_data_cm: ET.Element,
    format_version: int,
) -> list[FlatFileColumn]:
    """Extract flat file column definitions from DTS:FlatFileColumn children."""
    columns: list[FlatFileColumn] = []
    for col_el in obj_data_cm.findall(f"{_NS}FlatFileColumn"):
        raw_name = get_property(col_el, "ObjectName", format_version) or ""
        name = decode_escapes(raw_name)

        raw_delimiter = get_property(col_el, "ColumnDelimiter", format_version) or ""
        delimiter = decode_escapes(raw_delimiter)

        raw_data_type = get_property(col_el, "DataType", format_version)
        data_type_code = safe_int(raw_data_type, "FlatFileColumn DataType", 0)
        data_type = resolve_data_type(data_type_code)

        max_width = safe_int(
            get_property(col_el, "MaximumWidth", format_version),
            "FlatFileColumn MaximumWidth",
        )
        precision = safe_int(
            get_property(col_el, "DataPrecision", format_version),
            "FlatFileColumn DataPrecision",
        )
        scale = safe_int(
            get_property(col_el, "DataScale", format_version),
            "FlatFileColumn DataScale",
        )

        raw_text_qual = get_property(col_el, "TextQualified", format_version)
        text_qualified = safe_bool(raw_text_qual)

        raw_width = get_property(col_el, "ColumnWidth", format_version)
        width = safe_int(raw_width, "FlatFileColumn ColumnWidth", 0)

        columns.append(
            FlatFileColumn(
                name=name,
                data_type=data_type,
                delimiter=delimiter,
                width=width,
                max_width=max_width,
                precision=precision,
                scale=scale,
                text_qualified=text_qualified,
            )
        )
    return columns


def _extract_flat_file_specifics(
    cm: ConnectionManager,
    obj_data_cm: ET.Element,
    format_version: int,
) -> None:
    """Populate flat-file-specific fields on a ConnectionManager in place."""
    cm.flat_file_format = get_property(obj_data_cm, "Format", format_version) or ""

    raw_header = get_property(
        obj_data_cm, "ColumnNamesInFirstDataRow", format_version
    )
    cm.flat_file_header = safe_bool(raw_header)

    cm.header_rows_to_skip = safe_int(
        get_property(obj_data_cm, "HeaderRowsToSkip", format_version),
        "HeaderRowsToSkip",
    )

    raw_row_delim = get_property(obj_data_cm, "RowDelimiter", format_version) or ""
    cm.row_delimiter = decode_escapes(raw_row_delim)

    raw_text_qual = get_property(obj_data_cm, "TextQualifier", format_version) or ""
    cm.text_qualifier = decode_escapes(raw_text_qual)

    cm.code_page = safe_int(
        get_property(obj_data_cm, "CodePage", format_version),
        "CodePage",
    )

    raw_unicode = get_property(obj_data_cm, "Unicode", format_version)
    cm.unicode = safe_bool(raw_unicode)

    cm.flat_file_columns = _extract_flat_file_columns(obj_data_cm, format_version)


# ---------------------------------------------------------------------------
# Connection-string type-specific parsers
# ---------------------------------------------------------------------------


def _parse_key_value_pairs(conn_str: str) -> dict[str, str]:
    """Parse a semicolon-delimited ``key=value`` connection string.

    Handles quoted values that may contain embedded semicolons
    (e.g. ``Extended Properties="Excel 12.0;HDR=YES"``).
    """
    result: dict[str, str] = {}
    i = 0
    n = len(conn_str)
    while i < n:
        # Skip leading whitespace / semicolons
        while i < n and conn_str[i] in (" ", ";"):
            i += 1
        if i >= n:
            break

        # Find '=' for key
        eq_pos = conn_str.find("=", i)
        if eq_pos < 0:
            break
        key = conn_str[i:eq_pos].strip()
        i = eq_pos + 1

        # Parse value — may be quoted
        if i < n and conn_str[i] == '"':
            # Find matching close quote
            close = conn_str.find('"', i + 1)
            if close >= 0:
                value = conn_str[i + 1 : close]
                i = close + 1
            else:
                value = conn_str[i + 1 :]
                i = n
        else:
            semi = conn_str.find(";", i)
            if semi >= 0:
                value = conn_str[i:semi]
                i = semi + 1
            else:
                value = conn_str[i:]
                i = n

        if key:
            result[key] = value.strip()

    return result


def _parse_oledb(conn_str: str) -> dict[str, str]:
    """Parse an OLE DB connection string into structured properties."""
    kv = _parse_key_value_pairs(conn_str)
    result: dict[str, str] = {}
    if "Data Source" in kv:
        result["Server"] = kv["Data Source"]
    if "Initial Catalog" in kv:
        result["Database"] = kv["Initial Catalog"]
    if "Provider" in kv:
        result["Provider"] = kv["Provider"]
    return result


def _parse_ado_net(conn_str: str, creation_name: str) -> dict[str, str]:
    """Parse an ADO.NET connection string and derive display type name.

    ADO.NET creation names look like
    ``ADO.NET:System.Data.SqlClient.SqlConnection, System.Data, ...``.
    The display name strips the assembly suffix and shows the short provider,
    e.g. ``ADO.NET (SqlClient)``.
    """
    result = _parse_key_value_pairs(conn_str)
    props: dict[str, str] = {}
    if "Data Source" in result:
        props["Server"] = result["Data Source"]
    if "Initial Catalog" in result:
        props["Database"] = result["Initial Catalog"]
    if "Provider" in result:
        props["Provider"] = result["Provider"]

    # Derive display type name from creation name.
    # Use the penultimate namespace segment (e.g. "SqlClient" from
    # "System.Data.SqlClient.SqlConnection") which matches the common
    # provider short-name convention.
    if ":" in creation_name:
        after_colon = creation_name.split(":", 1)[1]
        class_name = after_colon.split(",", 1)[0].strip()
        segments = class_name.rsplit(".", 2)
        if len(segments) >= 2:
            short = segments[-2]
        else:
            short = segments[-1]
            if short.endswith("Connection"):
                short = short[: -len("Connection")]
        props["DisplayTypeName"] = f"ADO.NET ({short})"
    else:
        props["DisplayTypeName"] = "ADO.NET"

    return props


def _parse_flatfile(conn_str: str) -> dict[str, str]:
    """Parse a FLATFILE connection — the connection string is the file path."""
    return {"FilePath": conn_str}


def _parse_excel(conn_str: str) -> dict[str, str]:
    """Parse an EXCEL connection string (OLE DB style with Extended Properties)."""
    kv = _parse_key_value_pairs(conn_str)
    result: dict[str, str] = {}
    if "Data Source" in kv:
        result["FilePath"] = kv["Data Source"]
    if "Provider" in kv:
        result["Provider"] = kv["Provider"]
    if "Extended Properties" in kv:
        raw_ext = kv["Extended Properties"]
        result["ExtendedProperties"] = raw_ext
        # Parse sub-properties (e.g. "Excel 12.0;HDR=YES")
        for sub in raw_ext.split(";"):
            sub = sub.strip()
            if sub.upper().startswith("HDR="):
                result["HDR"] = sub[4:]
            elif sub and "=" not in sub:
                result["ExcelVersion"] = sub
    return result


def _parse_file(
    conn_str: str,
    file_usage_type: int | None,
) -> dict[str, str]:
    """Parse a FILE connection — the connection string is the file path."""
    result: dict[str, str] = {"FilePath": conn_str}
    if file_usage_type is not None:
        label = _FILE_USAGE_MAP.get(file_usage_type)
        if label is None:
            print(
                f"Warning: unknown FileUsageType {file_usage_type}, "
                f"using raw value",
                file=sys.stderr,
            )
            label = str(file_usage_type)
        result["FileUsageType"] = label
    return result


def _parse_ftp(obj_data_cm: ET.Element | None) -> dict[str, str]:
    """Parse an FTP connection from ``DTS:FtpConnection`` child attributes."""
    result: dict[str, str] = {}
    if obj_data_cm is None:
        return result
    # Try both namespaced and non-namespaced element names
    ftp_el = obj_data_cm.find(f"{_NS}FtpConnection")
    if ftp_el is None:
        ftp_el = obj_data_cm.find("FtpConnection")
    if ftp_el is None:
        return result
    for attr_name in ("ServerName", "ServerPort", "Retries", "ChunkSize"):
        val = ftp_el.get(f"{_NS}{attr_name}")
        if val is None:
            val = ftp_el.get(attr_name)
        if val is not None:
            result[attr_name] = val
    return result


def _parse_http(obj_data_cm: ET.Element | None) -> dict[str, str]:
    """Parse an HTTP connection from ``DTS:HttpConnection`` child attributes."""
    result: dict[str, str] = {}
    if obj_data_cm is None:
        return result
    http_el = obj_data_cm.find(f"{_NS}HttpConnection")
    if http_el is None:
        http_el = obj_data_cm.find("HttpConnection")
    if http_el is None:
        return result
    for attr_name in ("ServerURL",):
        val = http_el.get(f"{_NS}{attr_name}")
        if val is None:
            val = http_el.get(attr_name)
        if val is not None:
            result[attr_name] = val
    return result


def _parse_smoserver(
    obj_data_cm: ET.Element | None,
    obj_data: ET.Element | None = None,
) -> dict[str, str]:
    """Parse an SMOServer connection from its child ``ConnectionString``.

    The ``SMOServerConnectionManager`` element may be a child of
    ``DTS:ObjectData/DTS:ConnectionManager`` or directly under
    ``DTS:ObjectData`` (when no DTS:ConnectionManager wrapper exists).
    """
    result: dict[str, str] = {}

    smo_el: ET.Element | None = None
    for parent in (obj_data_cm, obj_data):
        if parent is None:
            continue
        smo_el = parent.find("SMOServerConnectionManager")
        if smo_el is None:
            smo_el = parent.find(f"{_NS}SMOServerConnectionManager")
        if smo_el is not None:
            break

    if smo_el is None:
        return result

    smo_conn_str = (
        smo_el.get("ConnectionString")
        or smo_el.get(f"{_NS}ConnectionString")
        or ""
    )
    if smo_conn_str:
        kv = _parse_key_value_pairs(smo_conn_str)
        if "SqlServerName" in kv:
            result["SqlServerName"] = kv["SqlServerName"]
        if "UseWindowsAuthentication" in kv:
            result["AuthMode"] = (
                "Windows"
                if kv["UseWindowsAuthentication"].lower() == "true"
                else "SQL Server"
            )
    return result


def _parse_msolap(conn_str: str) -> dict[str, str]:
    """Parse an MSOLAP connection string (same as OLE DB)."""
    return _parse_oledb(conn_str)


def _parse_connection_properties(
    cm: ConnectionManager,
    obj_data_cm: ET.Element | None,
    format_version: int,
    obj_data: ET.Element | None = None,
) -> None:
    """Dispatch to type-specific parser and populate ``parsed_properties``.

    Also extracts ``file_usage_type`` for FILE connections.
    *obj_data* is the ``DTS:ObjectData`` element itself (used by parsers
    that need to search for type-specific children that live outside the
    ``DTS:ConnectionManager`` wrapper).
    """
    creation = cm.creation_name.upper()

    if creation == "OLEDB":
        cm.parsed_properties = _parse_oledb(cm.connection_string)
    elif creation.startswith("ADO.NET"):
        cm.parsed_properties = _parse_ado_net(
            cm.connection_string, cm.creation_name
        )
    elif creation == "FLATFILE":
        cm.parsed_properties = _parse_flatfile(cm.connection_string)
    elif creation == "EXCEL":
        cm.parsed_properties = _parse_excel(cm.connection_string)
    elif creation == "FILE":
        # Extract FileUsageType from the outer connection manager element or
        # the ObjectData child
        if obj_data_cm is not None:
            raw_fut = get_property(obj_data_cm, "FileUsageType", format_version)
            cm.file_usage_type = safe_int(raw_fut, "FileUsageType") if raw_fut is not None else None
        cm.parsed_properties = _parse_file(
            cm.connection_string, cm.file_usage_type
        )
    elif creation == "FTP":
        cm.parsed_properties = _parse_ftp(obj_data_cm)
    elif creation == "HTTP":
        cm.parsed_properties = _parse_http(obj_data_cm)
    elif creation == "SMOSERVER":
        cm.parsed_properties = _parse_smoserver(obj_data_cm, obj_data)
    elif creation.startswith("MSOLAP"):
        cm.parsed_properties = _parse_msolap(cm.connection_string)
    else:
        # Custom/unknown — store raw values
        props: dict[str, str] = {}
        if cm.creation_name:
            props["CreationName"] = cm.creation_name
        if cm.connection_string:
            props["ConnectionString"] = cm.connection_string
        if props:
            cm.parsed_properties = props


def _extract_single_connection(
    cm_el: ET.Element,
    format_version: int,
) -> ConnectionManager:
    """Extract a single ``ConnectionManager`` from a ``DTS:ConnectionManager`` element.

    Shared logic used by both :func:`extract_connections` (for embedded
    connection managers in ``.dtsx`` files) and :func:`extract_conmgr_file`
    (for standalone ``.conmgr`` files).
    """
    # Core properties via get_property()
    raw_name = get_property(cm_el, "ObjectName", format_version) or ""
    name = decode_escapes(raw_name)

    dtsid = get_property(cm_el, "DTSID", format_version) or ""
    creation_name = get_property(cm_el, "CreationName", format_version) or ""
    description = get_property(cm_el, "Description", format_version) or ""

    raw_delay = get_property(cm_el, "DelayValidation", format_version)
    delay_validation = safe_bool(raw_delay)

    # refId — format 8 attribute; may be absent in format 2/6
    ref_id = cm_el.get(f"{_NS}refId") or ""

    if not name:
        print(
            "Warning: ConnectionManager has empty ObjectName",
            file=sys.stderr,
        )
    if not dtsid:
        print(
            f"Warning: ConnectionManager '{name}' has no DTSID",
            file=sys.stderr,
        )

    # Detect project-level connection via refId or DTSID :external suffix
    is_project_level = ref_id.startswith("Project.ConnectionManagers[")
    if not is_project_level and dtsid.endswith(":external"):
        is_project_level = True

    # ObjectData/ConnectionManager sub-element
    obj_data = cm_el.find(f"{_NS}ObjectData")
    obj_data_cm = _get_obj_data_cm(cm_el)

    # Connection string
    connection_string = _extract_connection_string(obj_data_cm, format_version)

    # Property expressions
    property_expressions = _extract_property_expressions(cm_el)

    cm = ConnectionManager(
        name=name,
        dtsid=dtsid,
        creation_name=creation_name,
        description=description,
        connection_string=connection_string,
        delay_validation=delay_validation,
        property_expressions=property_expressions,
        ref_id=ref_id,
        is_project_level=is_project_level,
    )

    # Flat file specifics
    if creation_name == "FLATFILE" and obj_data_cm is not None:
        _extract_flat_file_specifics(cm, obj_data_cm, format_version)

    # Type-specific connection string parsing
    _parse_connection_properties(cm, obj_data_cm, format_version, obj_data)

    return cm


def extract_connections(
    root: ET.Element,
    format_version: int,
) -> list[ConnectionManager]:
    """Extract all connection managers from a .dtsx root element.

    Connection managers may be direct children of the root (format 2/3/6)
    or nested inside a ``DTS:ConnectionManagers`` container (format 8).
    Both locations are searched.

    Returns a list of ``ConnectionManager`` model objects with all core
    properties, connection strings, property expressions, and flat file
    column schemas populated.
    """
    connections: list[ConnectionManager] = []

    # Format 2/3/6: direct children of root
    for cm_el in root.findall(f"{_NS}ConnectionManager"):
        cm = _extract_single_connection(cm_el, format_version)
        connections.append(cm)

    # Format 8: nested inside DTS:ConnectionManagers container
    container = root.find(f"{_NS}ConnectionManagers")
    if container is not None:
        for cm_el in container.findall(f"{_NS}ConnectionManager"):
            cm = _extract_single_connection(cm_el, format_version)
            connections.append(cm)

    return connections


def build_connection_map(
    connections: list[ConnectionManager],
) -> dict[str, ConnectionManager]:
    """Build a lookup map indexed by DTSID (case-insensitive) and refId path.

    Each connection is indexed by its DTSID (upper-cased for case-insensitive
    GUID matching) and, when present, its ``ref_id`` path.  If two connections
    share the same key, the later one wins (with a warning to stderr).

    Returns a plain ``dict`` suitable for O(1) lookup.
    """
    mapping: dict[str, ConnectionManager] = {}
    for cm in connections:
        if cm.dtsid:
            key = cm.dtsid.upper()
            if key in mapping:
                print(
                    f"Warning: duplicate DTSID {cm.dtsid} in connection map; "
                    f"overwriting '{mapping[key].name}' with '{cm.name}'",
                    file=sys.stderr,
                )
            mapping[key] = cm
        if cm.ref_id:
            if cm.ref_id in mapping:
                print(
                    f"Warning: duplicate refId {cm.ref_id} in connection map; "
                    f"overwriting '{mapping[cm.ref_id].name}' with '{cm.name}'",
                    file=sys.stderr,
                )
            mapping[cm.ref_id] = cm
    return mapping


def extract_conmgr_file(path: Path) -> ConnectionManager | None:
    """Parse a single ``.conmgr`` XML file and return a ``ConnectionManager``.

    The root element of a ``.conmgr`` file *is* the ``DTS:ConnectionManager``
    element itself.  Most ``.conmgr`` files store properties as namespaced
    attributes (``{DTS}ObjectName``, etc.) like format 8, regardless of
    whether a ``PackageFormatVersion`` is present.  If attribute-based
    extraction yields empty core properties, we fall back to child-property
    extraction (format 2/6).

    Returns ``None`` and logs a warning to stderr on any parse or I/O error.
    """
    try:
        tree = ET.parse(str(path))
    except (OSError, ET.ParseError) as exc:
        print(
            f"Warning: failed to parse .conmgr file {path}: {exc}",
            file=sys.stderr,
        )
        return None

    root = tree.getroot()

    # .conmgr files typically use attribute-based storage (format 8) even
    # when no PackageFormatVersion is present.  Try that first.
    cm = _extract_single_connection(root, 8)

    # Fall back to child-property extraction if attributes yielded nothing.
    if not cm.name and not cm.creation_name:
        detected_version = get_format_version(root)
        if detected_version < 8:
            cm = _extract_single_connection(root, detected_version)

    cm.is_project_level = True
    return cm


def discover_conmgr_files(dtsx_path: Path) -> list[ConnectionManager]:
    """Discover and parse ``*.conmgr`` files near a ``.dtsx`` file.

    Searches the ``.dtsx`` file's directory and its parent directory.
    Each discovered ``.conmgr`` file is parsed using :func:`extract_conmgr_file`
    and marked as project-level.  Files that fail to parse are skipped with a
    warning to stderr.

    Returns a deterministically-sorted list (by file path) for idempotent output.
    """
    conmgr_files: set[Path] = set()

    dtsx_dir = dtsx_path.parent
    for directory in (dtsx_dir, dtsx_dir.parent):
        try:
            for f in directory.glob("*.conmgr"):
                conmgr_files.add(f.resolve())
        except OSError as exc:
            print(
                f"Warning: failed to scan directory {directory} for "
                f".conmgr files: {exc}",
                file=sys.stderr,
            )

    connections: list[ConnectionManager] = []
    for conmgr_path in sorted(conmgr_files):
        cm = extract_conmgr_file(conmgr_path)
        if cm is not None:
            connections.append(cm)

    return connections
