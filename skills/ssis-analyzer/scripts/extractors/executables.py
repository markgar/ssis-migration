"""Extract executables from a .dtsx root, detecting Pipeline tasks."""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET

from extractors.dataflow import extract_data_flow
from extractors.variables import extract_variables
from models import (
    ASProcessingPayload,
    ConnectionManager,
    DataFlow,
    EventHandler,
    Executable,
    ExecutePackagePayload,
    ExecuteSqlPayload,
    FileSystemPayload,
    ForEachEnumerator,
    ForLoopPayload,
    FtpTaskPayload,
    LoggingOptions,
    LogProvider,
    PrecedenceConstraint,
    ScriptTaskPayload,
    SendMailPayload,
    SqlParameterBinding,
    SqlResultBinding,
    TransferTaskPayload,
    VariableMapping,
)
from normalizers import normalize_creation_name
from xml_helpers import (
    DTS_NAMESPACE,
    get_property,
    resolve_connection_ref,
    safe_bool,
    safe_int,
)

_NS = f"{{{DTS_NAMESPACE}}}"

# Namespace used by Execute SQL Task elements in format 2/6
_SQLTASK_NS = "{www.microsoft.com/sqlserver/dts/tasks/sqltask}"

# Namespace used by Bulk Insert Task elements
_BULKINSERT_NS = "{www.microsoft.com/sqlserver/dts/tasks/bulkinserttask}"

# Namespace used by Web Service Task elements
_WSTASK_NS = "{www.microsoft.com/sqlserver/dts/tasks/webservicetask}"


def _find_child_executables(parent: ET.Element, format_version: int) -> list[ET.Element]:
    """Return direct child DTS:Executable elements from *parent*.

    Format 8 nests them under a ``DTS:Executables`` container;
    earlier formats attach them directly.
    """
    container = parent.find(f"{_NS}Executables")
    if container is not None:
        return container.findall(f"{_NS}Executable")
    return parent.findall(f"{_NS}Executable")


def _extract_property_expressions(
    exe_el: ET.Element,
    format_version: int,
) -> dict[str, str]:
    """Extract ``DTS:PropertyExpression`` children into a property→expression map."""
    result: dict[str, str] = {}
    for pe in exe_el.findall(f"{_NS}PropertyExpression"):
        prop_name = pe.get(f"{_NS}Name")
        if prop_name is None:
            # Format 8 may use plain "Name" attribute
            prop_name = pe.get("Name")
        if prop_name and pe.text:
            result[prop_name] = pe.text
    return result


def _find_pipeline_element(exe_el: ET.Element) -> ET.Element | None:
    """Locate the ``<pipeline>`` element under ``DTS:ObjectData``, if present."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        return None
    return obj_data.find("pipeline")


def _resolve_connection(
    guid: str,
    connection_map: dict[str, ConnectionManager] | None,
) -> str:
    """Resolve a connection reference to a connection name via the connection map."""
    return resolve_connection_ref(guid, connection_map)


# ---------------------------------------------------------------------------
# Execute SQL Task extraction
# ---------------------------------------------------------------------------

_RESULT_TYPE_MAP: dict[str, str] = {
    "1": "None",
    "2": "SingleRow",
    "3": "FullResultSet",
    "4": "XML",
}


def _sql_attr(el: ET.Element, name: str) -> str | None:
    """Read an attribute from a SqlTaskData element, trying both plain and
    namespace-prefixed forms.  Format-8 packages that use ``SQLTask:``
    prefixed attributes have their keys expanded to the full namespace by
    ElementTree, so we need to check both."""
    val = el.get(name)
    if val is not None:
        return val
    return el.get(f"{_SQLTASK_NS}{name}")


def _extract_execute_sql(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> ExecuteSqlPayload | None:
    """Extract Execute SQL Task payload from DTS:ObjectData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: ExecuteSQLTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    # Find the SqlTaskData element — format 8 uses plain <SqlTaskData>,
    # format 2/6 may use <SQLTask:ExecuteSQL> or <SqlTaskData>
    sql_el = obj_data.find("SqlTaskData")
    if sql_el is None:
        sql_el = obj_data.find(f"{_SQLTASK_NS}SqlTaskData")
    if sql_el is None:
        sql_el = obj_data.find(f"{_SQLTASK_NS}ExecuteSQL")
    if sql_el is None:
        # Try any child that contains "SqlTask" or "ExecuteSQL" in tag
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "SqlTask" in tag or "ExecuteSQL" in tag:
                sql_el = child
                break
    if sql_el is None:
        print(
            "Warning: ExecuteSQLTask has no SqlTaskData element in ObjectData",
            file=sys.stderr,
        )
        return None

    sql_statement = _sql_attr(sql_el, "SqlStatementSource") or ""
    # Source type: SqlStmtSourceType (format 8) or SourceType (format 2/6)
    sql_source_type = (
        _sql_attr(sql_el, "SqlStmtSourceType")
        or _sql_attr(sql_el, "SourceType")
        or "DirectInput"
    )
    connection_guid = _sql_attr(sql_el, "Connection") or ""
    connection = _resolve_connection(connection_guid, connection_map)

    # Result type: ResultSetType (format 8) or ResultType (format 2/6)
    result_type_raw = (
        _sql_attr(sql_el, "ResultSetType")
        or _sql_attr(sql_el, "ResultType")
        or "1"
    )
    result_type = _RESULT_TYPE_MAP.get(result_type_raw, result_type_raw)

    is_stored_procedure = safe_bool(_sql_attr(sql_el, "IsStoredProcedure"))
    bypass_prepare = safe_bool(_sql_attr(sql_el, "BypassPrepare"))
    # Timeout: TimeOut (format 8) or CommandTimeout (format 2/6)
    timeout = safe_int(
        _sql_attr(sql_el, "TimeOut") or _sql_attr(sql_el, "CommandTimeout"),
        "ExecuteSQL TimeOut",
    )
    code_page = safe_int(_sql_attr(sql_el, "CodePage"), "ExecuteSQL CodePage")

    # Parameter bindings
    param_bindings: list[SqlParameterBinding] = []
    for pb in sql_el.findall("ParameterBinding"):
        param_bindings.append(_extract_parameter_binding(pb))
    for pb in sql_el.findall(f"{_SQLTASK_NS}ParameterBinding"):
        param_bindings.append(_extract_parameter_binding(pb))

    # Result bindings
    result_bindings: list[SqlResultBinding] = []
    for rb in sql_el.findall("ResultBinding"):
        result_bindings.append(_extract_result_binding(rb))
    for rb in sql_el.findall(f"{_SQLTASK_NS}ResultBinding"):
        result_bindings.append(_extract_result_binding(rb))

    return ExecuteSqlPayload(
        sql_statement=sql_statement,
        sql_source_type=sql_source_type,
        connection=connection,
        result_type=result_type,
        parameter_bindings=param_bindings,
        result_bindings=result_bindings,
        is_stored_procedure=is_stored_procedure,
        bypass_prepare=bypass_prepare,
        timeout=timeout,
        code_page=code_page,
    )


def _extract_parameter_binding(pb: ET.Element) -> SqlParameterBinding:
    """Extract a single ParameterBinding element."""
    from lookups import resolve_data_type

    param_name = _sql_attr(pb, "ParameterName") or ""
    variable_name = _sql_attr(pb, "DtsVariableName") or ""
    direction = _sql_attr(pb, "ParameterDirection") or ""
    data_type_raw = _sql_attr(pb, "DataType")
    data_type = ""
    if data_type_raw is not None:
        dt_int = safe_int(data_type_raw, "ParameterBinding DataType", -1)
        if dt_int >= 0:
            data_type = resolve_data_type(dt_int)
        else:
            data_type = data_type_raw
    size = safe_int(_sql_attr(pb, "ParameterSize"), "ParameterBinding Size")

    return SqlParameterBinding(
        parameter_name=param_name,
        variable_name=variable_name,
        direction=direction,
        data_type=data_type,
        size=size,
    )


def _extract_result_binding(rb: ET.Element) -> SqlResultBinding:
    """Extract a single ResultBinding element."""
    result_name = _sql_attr(rb, "ResultName") or ""
    variable_name = _sql_attr(rb, "DtsVariableName") or ""
    return SqlResultBinding(
        result_name=result_name,
        variable_name=variable_name,
    )


# ---------------------------------------------------------------------------
# ForLoop extraction
# ---------------------------------------------------------------------------


def _extract_for_loop(
    exe_el: ET.Element,
    format_version: int,
) -> ForLoopPayload:
    """Extract ForLoop properties (Init, Eval, Assign expressions)."""
    init_expression = get_property(exe_el, "InitExpression", format_version) or ""
    eval_expression = get_property(exe_el, "EvalExpression", format_version) or ""
    assign_expression = get_property(exe_el, "AssignExpression", format_version) or ""
    return ForLoopPayload(
        init_expression=init_expression,
        eval_expression=eval_expression,
        assign_expression=assign_expression,
    )


# ---------------------------------------------------------------------------
# ForEach Loop extraction
# ---------------------------------------------------------------------------

# Canonical enumerator type from CreationName
_FOREACH_ENUM_MAP: dict[str, str] = {
    "Microsoft.ForEachFileEnumerator": "File",
    "Microsoft.ForEachItemEnumerator": "Item",
    "Microsoft.ForEachADOEnumerator": "ADO",
    "Microsoft.ForEachFromVarEnumerator": "Variable",
    "Microsoft.ForEachNodeListEnumerator": "NodeList",
    "Microsoft.ForEachSMOEnumerator": "SMO",
    "Microsoft.ForEachSchemaRowsetEnumerator": "SchemaRowset",
}

# Substring patterns for format 2/6 assembly-qualified enumerator names
_FOREACH_ENUM_SUBSTRING_MAP: list[tuple[str, str]] = [
    ("ForEachFileEnumerator", "File"),
    ("ForEachItemEnumerator", "Item"),
    ("ForEachADOEnumerator", "ADO"),
    ("ForEachFromVarEnumerator", "Variable"),
    ("ForEachNodeListEnumerator", "NodeList"),
    ("ForEachSMOEnumerator", "SMO"),
    ("ForEachSchemaRowsetEnumerator", "SchemaRowset"),
]


def _extract_foreach_file_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract File enumerator properties from ForEachFileEnumeratorProperties."""
    props: dict[str, str] = {}
    container = obj_data.find("ForEachFileEnumeratorProperties")
    if container is None:
        return props
    for fe_prop in container.findall("FEFEProperty"):
        for attr_name in ("Folder", "FileSpec", "FileNameRetrievalType", "Recurse"):
            val = fe_prop.get(attr_name)
            if val is not None:
                props[attr_name] = val
    return props


def _extract_foreach_item_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract Item enumerator properties from FEIEItems."""
    props: dict[str, str] = {}
    items_el = obj_data.find("FEIEItems")
    if items_el is None:
        return props
    items = items_el.findall("FEIEItem")
    props["ItemCount"] = str(len(items))
    for i, item in enumerate(items):
        values = item.findall("FEIEItemValue")
        for j, val_el in enumerate(values):
            val = val_el.get("Value") or ""
            props[f"Item[{i}][{j}]"] = val
    return props


def _extract_foreach_ado_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract ADO enumerator properties from FEEADO."""
    props: dict[str, str] = {}
    ado_el = obj_data.find("FEEADO")
    if ado_el is None:
        return props
    for attr_name in ("EnumType", "VarName"):
        val = ado_el.get(attr_name)
        if val is not None:
            props[attr_name] = val
    return props


def _extract_foreach_variable_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract Variable enumerator properties from FEEFVE."""
    props: dict[str, str] = {}
    feefve = obj_data.find("FEEFVE")
    if feefve is None:
        return props
    var_name = feefve.get("VariableName")
    if var_name is not None:
        props["VariableName"] = var_name
    return props


def _extract_foreach_nodelist_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract NodeList enumerator properties from FEENODELIST."""
    props: dict[str, str] = {}
    nl_el = obj_data.find("FEENODELIST")
    if nl_el is None:
        return props
    for attr_name in (
        "EnumerationType",
        "OuterXPathString",
        "OuterXPathSourceType",
        "InnerElementType",
        "InnerXPathSourceType",
        "InnerXPathString",
        "SourceType",
        "SourceDocument",
    ):
        val = nl_el.get(attr_name)
        if val is not None:
            props[attr_name] = val
    return props


def _extract_foreach_smo_props(
    obj_data: ET.Element,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str]:
    """Extract SMO enumerator properties from FEESMO.

    Resolves embedded connection GUIDs in the EnumURN.
    """
    props: dict[str, str] = {}
    smo_el = obj_data.find("FEESMO")
    if smo_el is None:
        return props
    enum_urn = smo_el.get("EnumURN") or ""
    if enum_urn:
        props["EnumURN"] = enum_urn
        # Try to resolve embedded connection GUID: @Connection='{GUID}'
        m = re.search(r"@Connection='(\{[^}]+\})'", enum_urn)
        if m and connection_map is not None:
            guid = m.group(1)
            resolved = _resolve_connection(guid, connection_map)
            props["Connection"] = resolved
    return props


def _extract_foreach_schema_rowset_props(obj_data: ET.Element) -> dict[str, str]:
    """Extract SchemaRowset enumerator properties from FEESchemaRowset."""
    props: dict[str, str] = {}
    sr_el = obj_data.find("FEESchemaRowset")
    if sr_el is None:
        return props
    for attr_name in ("Connection", "Schema"):
        val = sr_el.get(attr_name)
        if val is not None:
            props[attr_name] = val
    return props


def _extract_foreach_variable_mappings(
    exe_el: ET.Element,
    format_version: int,
) -> list[VariableMapping]:
    """Extract DTS:ForEachVariableMapping children from a ForEach loop."""
    mappings: list[VariableMapping] = []
    # Look in DTS:ForEachVariableMappings container first
    container = exe_el.find(f"{_NS}ForEachVariableMappings")
    if container is not None:
        elements = container.findall(f"{_NS}ForEachVariableMapping")
    else:
        elements = exe_el.findall(f"{_NS}ForEachVariableMapping")
    for vm_el in elements:
        # Format 8: attributes; format 2/3/6: child DTS:Property elements
        var_name = get_property(vm_el, "VariableName", format_version) or ""
        value_index = safe_int(
            get_property(vm_el, "ValueIndex", format_version),
            "ForEachVariableMapping ValueIndex",
        )
        mappings.append(VariableMapping(variable_name=var_name, value_index=value_index))
    return mappings


def _extract_for_each(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> ForEachEnumerator | None:
    """Extract ForEach loop enumerator configuration."""
    enum_el = exe_el.find(f"{_NS}ForEachEnumerator")
    if enum_el is None:
        print(
            "Warning: ForEachLoop has no DTS:ForEachEnumerator element",
            file=sys.stderr,
        )
        return None

    creation_name_raw = get_property(enum_el, "CreationName", format_version) or ""
    enumerator_type = _FOREACH_ENUM_MAP.get(creation_name_raw, "")
    if not enumerator_type:
        # Try substring matching for format 2/6 assembly-qualified names
        for substring, etype in _FOREACH_ENUM_SUBSTRING_MAP:
            if substring in creation_name_raw:
                enumerator_type = etype
                break
    if not enumerator_type:
        if creation_name_raw:
            print(
                f"Warning: unknown ForEach enumerator CreationName {creation_name_raw!r}",
                file=sys.stderr,
            )
        enumerator_type = creation_name_raw or "Unknown"

    # Extract type-specific properties from enumerator's ObjectData
    props: dict[str, str] = {}
    enum_obj_data = enum_el.find(f"{_NS}ObjectData")
    if enum_obj_data is not None:
        if enumerator_type == "File":
            props = _extract_foreach_file_props(enum_obj_data)
        elif enumerator_type == "Item":
            props = _extract_foreach_item_props(enum_obj_data)
        elif enumerator_type == "ADO":
            props = _extract_foreach_ado_props(enum_obj_data)
        elif enumerator_type == "Variable":
            props = _extract_foreach_variable_props(enum_obj_data)
        elif enumerator_type == "NodeList":
            props = _extract_foreach_nodelist_props(enum_obj_data)
        elif enumerator_type == "SMO":
            props = _extract_foreach_smo_props(enum_obj_data, connection_map)
        elif enumerator_type == "SchemaRowset":
            props = _extract_foreach_schema_rowset_props(enum_obj_data)
        else:
            # Unknown type — extract all attrs from first child element
            for child in enum_obj_data:
                for k, v in child.attrib.items():
                    props[k] = v

    # Variable mappings
    variable_mappings = _extract_foreach_variable_mappings(exe_el, format_version)

    # Property expressions on the enumerator
    enum_property_expressions = _extract_property_expressions(enum_el, format_version)

    return ForEachEnumerator(
        enumerator_type=enumerator_type,
        properties=props,
        variable_mappings=variable_mappings,
        property_expressions=enum_property_expressions,
    )


# ---------------------------------------------------------------------------
# FileSystem Task extraction
# ---------------------------------------------------------------------------


def _extract_file_system(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> FileSystemPayload | None:
    """Extract FileSystem Task payload from DTS:ObjectData/FileSystemData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: FileSystemTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    fs_el = obj_data.find("FileSystemData")
    if fs_el is None:
        print(
            "Warning: FileSystemTask has no FileSystemData element in ObjectData",
            file=sys.stderr,
        )
        return None

    operation = fs_el.get("TaskOperationType") or ""
    overwrite = safe_bool(fs_el.get("TaskOverwriteDestFile"))

    # Source may be a connection GUID or a variable reference
    source_path = fs_el.get("TaskSourcePath") or ""
    is_source_var = safe_bool(fs_el.get("TaskIsSourceVariable"))
    if is_source_var:
        source_connection = source_path
    else:
        source_connection = _resolve_connection(source_path, connection_map)

    # Destination may be a connection GUID or variable reference
    dest_path = fs_el.get("TaskDestinationPath") or ""
    is_dest_var = safe_bool(fs_el.get("TaskIsDestinationVariable"))
    if is_dest_var:
        dest_connection = dest_path
    else:
        dest_connection = _resolve_connection(dest_path, connection_map) if dest_path else ""

    return FileSystemPayload(
        operation=operation,
        source_connection=source_connection,
        dest_connection=dest_connection,
        overwrite=overwrite,
    )


# ---------------------------------------------------------------------------
# SendMail Task extraction
# ---------------------------------------------------------------------------


def _extract_send_mail(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> SendMailPayload | None:
    """Extract SendMail Task payload from DTS:ObjectData/SendMailTaskData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: SendMailTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    sm_el = obj_data.find("SendMailTaskData")
    if sm_el is None:
        print(
            "Warning: SendMailTask has no SendMailTaskData element in ObjectData",
            file=sys.stderr,
        )
        return None

    to = sm_el.get("ToLine") or ""
    cc = sm_el.get("CCLine") or ""
    bcc = sm_el.get("BCCLine") or ""
    subject = sm_el.get("Subject") or ""
    message_source = sm_el.get("MessageSource") or ""
    message_source_type = sm_el.get("MessageSourceType") or ""
    smtp_connection_guid = sm_el.get("SMTPServer") or ""
    smtp_connection = _resolve_connection(smtp_connection_guid, connection_map) if smtp_connection_guid else ""

    return SendMailPayload(
        to=to,
        cc=cc,
        bcc=bcc,
        subject=subject,
        message_source=message_source,
        message_source_type=message_source_type,
        smtp_connection=smtp_connection,
    )


# ---------------------------------------------------------------------------
# Script Task extraction
# ---------------------------------------------------------------------------


def _extract_script_task(
    exe_el: ET.Element,
    format_version: int,
) -> ScriptTaskPayload | None:
    """Extract Script Task payload from DTS:ObjectData/ScriptProject."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: ScriptTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    sp_el = obj_data.find("ScriptProject")
    if sp_el is None:
        print(
            "Warning: ScriptTask has no ScriptProject element in ObjectData",
            file=sys.stderr,
        )
        return None

    script_language = sp_el.get("Language") or ""
    entry_point = sp_el.get("EntryPoint") or ""

    # ReadOnlyVariables and ReadWriteVariables are comma-separated
    ro_raw = sp_el.get("ReadOnlyVariables") or ""
    rw_raw = sp_el.get("ReadWriteVariables") or ""
    read_only = [v.strip() for v in ro_raw.split(",") if v.strip()] if ro_raw else []
    read_write = [v.strip() for v in rw_raw.split(",") if v.strip()] if rw_raw else []

    # Source code from ProjectItem children
    source_code: dict[str, str] = {}
    for pi in sp_el.findall("ProjectItem"):
        name = pi.get("Name") or ""
        text = pi.text or ""
        if name:
            source_code[name] = text

    return ScriptTaskPayload(
        script_language=script_language,
        entry_point=entry_point,
        read_only_variables=read_only,
        read_write_variables=read_write,
        source_code=source_code if source_code else None,
    )


# ---------------------------------------------------------------------------
# Expression Task extraction
# ---------------------------------------------------------------------------


def _extract_expression_task(
    exe_el: ET.Element,
    format_version: int,
) -> str | None:
    """Extract Expression Task expression text.

    The expression may live in:
    1. ``DTS:ObjectData/ExpressionTask/@Expression`` (format 8 common)
    2. ``DTS:Expression`` property on the executable
    3. ``DTS:PropertyExpression`` with name ``"Expression"``
    """
    # Check ObjectData/ExpressionTask first (format 8 pattern)
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is not None:
        expr_el = obj_data.find("ExpressionTask")
        if expr_el is not None:
            expr = expr_el.get("Expression")
            if expr:
                return expr

    # Try DTS:Expression property
    expr = get_property(exe_el, "Expression", format_version)
    if expr:
        return expr

    # Fallback: check PropertyExpression children
    for pe in exe_el.findall(f"{_NS}PropertyExpression"):
        prop_name = pe.get(f"{_NS}Name") or pe.get("Name") or ""
        if prop_name == "Expression" and pe.text:
            return pe.text

    print(
        "Warning: ExpressionTask has no Expression property",
        file=sys.stderr,
    )
    return None


# ---------------------------------------------------------------------------
# Execute Package Task extraction
# ---------------------------------------------------------------------------


def _extract_execute_package(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> ExecutePackagePayload | None:
    """Extract Execute Package Task payload from DTS:ObjectData/ExecutePackageTask."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: ExecutePackageTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    ept_el = obj_data.find("ExecutePackageTask")
    if ept_el is None:
        print(
            "Warning: ExecutePackageTask has no ExecutePackageTask element in ObjectData",
            file=sys.stderr,
        )
        return None

    # Determine reference pattern: project reference vs file-based
    use_project_ref_el = ept_el.find("UseProjectReference")
    use_project_reference = False
    if use_project_ref_el is not None and use_project_ref_el.text:
        use_project_reference = use_project_ref_el.text.strip() in ("True", "1", "-1")

    package_name_el = ept_el.find("PackageName")
    package_name = ""
    if package_name_el is not None and package_name_el.text:
        package_name = package_name_el.text.strip()

    # Connection (file-based pattern)
    connection: str | None = None
    conn_el = ept_el.find("Connection")
    if conn_el is not None and conn_el.text:
        connection_guid = conn_el.text.strip()
        connection = _resolve_connection(connection_guid, connection_map)

    # Parameter assignments
    parameter_assignments: list[tuple[str, str]] = []
    for pa in ept_el.findall("ParameterAssignment"):
        param_name_el = pa.find("ParameterName")
        bind_var_el = pa.find("BindedVariableOrParameterName")
        param_name = (
            param_name_el.text.strip()
            if param_name_el is not None and param_name_el.text
            else ""
        )
        bind_var = (
            bind_var_el.text.strip()
            if bind_var_el is not None and bind_var_el.text
            else ""
        )
        if param_name or bind_var:
            parameter_assignments.append((param_name, bind_var))

    return ExecutePackagePayload(
        use_project_reference=use_project_reference,
        package_name=package_name,
        connection=connection,
        parameter_assignments=parameter_assignments,
    )


# ---------------------------------------------------------------------------
# SSAS Processing Task extraction
# ---------------------------------------------------------------------------


def _extract_as_processing(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> ASProcessingPayload | None:
    """Extract SSAS Processing Task payload from DTS:ObjectData/ASProcessingData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: DTSProcessingTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    asp_el = obj_data.find("ASProcessingData")
    if asp_el is None:
        # Try namespace variants
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "ProcessingData" in tag or "ASProcessing" in tag:
                asp_el = child
                break
    if asp_el is None:
        print(
            "Warning: DTSProcessingTask has no ASProcessingData element in ObjectData",
            file=sys.stderr,
        )
        return None

    # Connection — stored as ConnectionID, ConnectionName, or Connection child element
    connection_ref = asp_el.get("ConnectionID") or asp_el.get("ConnectionName") or ""
    if not connection_ref:
        conn_el = asp_el.find("Connection")
        if conn_el is not None and conn_el.text:
            connection_ref = conn_el.text.strip()
    connection = _resolve_connection(connection_ref, connection_map) if connection_ref else ""

    # Processing commands — stored as ProcessingCommands attribute or Batch child
    processing_commands = asp_el.get("ProcessingCommands") or ""
    if not processing_commands:
        # Search for Batch element — may have a namespace
        for child in asp_el:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Batch":
                processing_commands = ET.tostring(child, encoding="unicode")
                break

    return ASProcessingPayload(
        connection=connection,
        processing_commands=processing_commands,
    )


# ---------------------------------------------------------------------------
# FTP Task extraction
# ---------------------------------------------------------------------------


def _extract_ftp_task(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> FtpTaskPayload | None:
    """Extract FTP Task payload from DTS:ObjectData/FtpData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: FtpTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    ftp_el = obj_data.find("FtpData")
    if ftp_el is None:
        print(
            "Warning: FtpTask has no FtpData element in ObjectData",
            file=sys.stderr,
        )
        return None

    # Attribute names vary: spec uses Operation/Connection/LocalPath/RemotePath,
    # but real packages use TaskOperationType/ConnectionName/TaskLocalPath/TaskRemotePath
    operation = (
        ftp_el.get("Operation")
        or ftp_el.get("TaskOperationType")
        or ""
    )
    connection_guid = (
        ftp_el.get("Connection")
        or ftp_el.get("ConnectionName")
        or ""
    )
    connection = (
        _resolve_connection(connection_guid, connection_map)
        if connection_guid
        else ""
    )
    local_path = ftp_el.get("LocalPath") or ftp_el.get("TaskLocalPath") or ""
    remote_path = ftp_el.get("RemotePath") or ftp_el.get("TaskRemotePath") or ""
    is_transfer_ascii = safe_bool(
        ftp_el.get("IsTransferAscii") or ftp_el.get("TaskIsTransferASCII")
    )

    return FtpTaskPayload(
        operation=operation,
        connection=connection,
        local_path=local_path,
        remote_path=remote_path,
        is_transfer_ascii=is_transfer_ascii,
    )


# ---------------------------------------------------------------------------
# Transfer Task extraction (shared pattern for all 6 transfer task types)
# ---------------------------------------------------------------------------

_TRANSFER_TASK_TYPES: frozenset[str] = frozenset({
    "TransferDatabaseTask",
    "TransferJobsTask",
    "TransferLoginsTask",
    "TransferErrorMessagesTask",
    "TransferStoredProceduresTask",
    "TransferSqlServerObjectsTask",
})

# Map creation_name → expected ObjectData child element tag name
_TRANSFER_TASK_DATA_TAGS: dict[str, str] = {
    "TransferDatabaseTask": "TransferDatabasesTaskData",
    "TransferJobsTask": "TransferJobsTaskData",
    "TransferLoginsTask": "TransferLoginsTaskData",
    "TransferErrorMessagesTask": "TransferErrorMessagesTaskData",
    "TransferStoredProceduresTask": "TransferStoredProceduresTaskData",
    "TransferSqlServerObjectsTask": "TransferSqlServerObjectsTaskData",
}

# Attribute names that may hold source/destination connections
_SOURCE_CONN_ATTRS = ("SourceConnection", "SrcConn")
_DEST_CONN_ATTRS = ("DestinationConnection", "DestConn", "m_DestinationConnectionID")


def _extract_transfer_task(
    exe_el: ET.Element,
    format_version: int,
    creation_name: str,
    connection_map: dict[str, ConnectionManager] | None,
) -> TransferTaskPayload | None:
    """Extract Transfer task payload from DTS:ObjectData."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            f"Warning: {creation_name} has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    # Find the data element by expected tag name
    expected_tag = _TRANSFER_TASK_DATA_TAGS.get(creation_name, "")
    data_el = obj_data.find(expected_tag) if expected_tag else None
    if data_el is None:
        # Fallback: any child with "Transfer" and "TaskData" in tag
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "Transfer" in tag and "TaskData" in tag:
                data_el = child
                break
    if data_el is None:
        print(
            f"Warning: {creation_name} has no data element in ObjectData",
            file=sys.stderr,
        )
        return None

    # Source connection — try multiple attribute name patterns
    src_guid = ""
    for attr in _SOURCE_CONN_ATTRS:
        src_guid = data_el.get(attr) or ""
        if src_guid:
            break
    source_connection = (
        _resolve_connection(src_guid, connection_map) if src_guid else ""
    )

    # Destination connection — try multiple attribute name patterns
    dst_guid = ""
    for attr in _DEST_CONN_ATTRS:
        dst_guid = data_el.get(attr) or ""
        if dst_guid:
            break
    dest_connection = (
        _resolve_connection(dst_guid, connection_map) if dst_guid else ""
    )

    # Remaining attributes as properties (exclude connection attrs)
    connection_attr_names = set(_SOURCE_CONN_ATTRS + _DEST_CONN_ATTRS)
    properties: dict[str, str] = {}
    for attr_name, attr_value in sorted(data_el.attrib.items()):
        if attr_name not in connection_attr_names:
            properties[attr_name] = attr_value

    return TransferTaskPayload(
        task_type=creation_name,
        source_connection=source_connection,
        dest_connection=dest_connection,
        properties=properties,
    )


# ---------------------------------------------------------------------------
# Bulk Insert Task extraction
# ---------------------------------------------------------------------------


def _extract_bulk_insert(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str] | None:
    """Extract Bulk Insert Task payload as generic properties."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: BulkInsertTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    data_el = obj_data.find("BulkInsertTaskData")
    if data_el is None:
        data_el = obj_data.find(f"{_BULKINSERT_NS}BulkInsertTaskData")
    if data_el is None:
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "BulkInsert" in tag:
                data_el = child
                break
    if data_el is None:
        print(
            "Warning: BulkInsertTask has no data element in ObjectData",
            file=sys.stderr,
        )
        return None

    props: dict[str, str] = {}
    for raw_attr, attr_value in sorted(data_el.attrib.items()):
        # Strip namespace prefix from attribute names
        attr_name = raw_attr.split("}")[-1] if "}" in raw_attr else raw_attr
        # Skip xmlns declarations
        if attr_name.startswith("xmlns"):
            continue
        # Resolve connection GUIDs
        if "Connection" in attr_name and "{" in attr_value:
            attr_value = _resolve_connection(attr_value, connection_map)
        props[attr_name] = attr_value
    return props if props else None


# ---------------------------------------------------------------------------
# Data Profiling Task extraction
# ---------------------------------------------------------------------------


def _extract_data_profiling(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str] | None:
    """Extract Data Profiling Task payload as generic properties."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: DataProfilingTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    data_el = obj_data.find("DataProfilingTaskData")
    if data_el is None:
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "DataProfiling" in tag:
                data_el = child
                break
    if data_el is None:
        print(
            "Warning: DataProfilingTask has no data element in ObjectData",
            file=sys.stderr,
        )
        return None

    props: dict[str, str] = {}
    for raw_attr, attr_value in sorted(data_el.attrib.items()):
        # Strip namespace prefix from attribute names
        attr_name = raw_attr.split("}")[-1] if "}" in raw_attr else raw_attr
        if attr_name.startswith("xmlns"):
            continue
        if "Connection" in attr_name and "{" in attr_value:
            attr_value = _resolve_connection(attr_value, connection_map)
        props[attr_name] = attr_value

    # ProfileInput CDATA
    profile_input = data_el.find("ProfileInput")
    if profile_input is not None:
        props["ProfileInput"] = ET.tostring(profile_input, encoding="unicode")

    return props if props else None


# ---------------------------------------------------------------------------
# XML Task extraction
# ---------------------------------------------------------------------------


def _extract_xml_task(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str] | None:
    """Extract XML Task payload as generic properties."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: XMLTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    data_el = obj_data.find("XMLTaskData")
    if data_el is None:
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "XMLTask" in tag:
                data_el = child
                break
    if data_el is None:
        print(
            "Warning: XMLTask has no data element in ObjectData",
            file=sys.stderr,
        )
        return None

    props: dict[str, str] = {}
    for raw_attr, attr_value in sorted(data_el.attrib.items()):
        # Strip namespace prefix from attribute names
        attr_name = raw_attr.split("}")[-1] if "}" in raw_attr else raw_attr
        if attr_name.startswith("xmlns"):
            continue
        if "Connection" in attr_name and "{" in attr_value:
            attr_value = _resolve_connection(attr_value, connection_map)
        props[attr_name] = attr_value
    return props if props else None


# ---------------------------------------------------------------------------
# Web Service Task extraction
# ---------------------------------------------------------------------------


def _extract_web_service(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str] | None:
    """Extract Web Service Task payload as generic properties."""
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        print(
            "Warning: WebServiceTask has no DTS:ObjectData",
            file=sys.stderr,
        )
        return None

    data_el = obj_data.find("WebServiceTaskData")
    if data_el is None:
        data_el = obj_data.find(f"{_WSTASK_NS}WebServiceTaskData")
    if data_el is None:
        for child in obj_data:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if "WebService" in tag:
                data_el = child
                break
    if data_el is None:
        print(
            "Warning: WebServiceTask has no data element in ObjectData",
            file=sys.stderr,
        )
        return None

    props: dict[str, str] = {}
    for raw_attr, attr_value in sorted(data_el.attrib.items()):
        # Strip namespace prefix from attribute names
        attr_name = raw_attr.split("}")[-1] if "}" in raw_attr else raw_attr
        if attr_name.startswith("xmlns"):
            continue
        # Resolve connection GUIDs
        if "Connection" in attr_name and "{" in attr_value:
            attr_value = _resolve_connection(attr_value, connection_map)
        props[attr_name] = attr_value
    return props if props else None


# ---------------------------------------------------------------------------
# Unknown / fallback task extraction
# ---------------------------------------------------------------------------

# Task types with dedicated extraction or no payload (no unknown fallback)
_KNOWN_TASK_TYPES: frozenset[str] = frozenset({
    "Pipeline", "ExecuteSQLTask", "ForLoop", "ForEachLoop", "Sequence",
    "FileSystemTask", "SendMailTask", "ScriptTask", "ExpressionTask",
    "ExecutePackageTask", "DTSProcessingTask", "FtpTask",
    "TransferDatabaseTask", "TransferJobsTask", "TransferLoginsTask",
    "TransferErrorMessagesTask", "TransferStoredProceduresTask",
    "TransferSqlServerObjectsTask",
    "BulkInsertTask", "DataProfilingTask", "XMLTask", "WebServiceTask",
    "Package",
})


def _extract_unknown_payload(
    exe_el: ET.Element,
    creation_name: str,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict[str, str] | None:
    """Extract generic payload from ObjectData for unknown task types.

    Iterates all child elements and attributes under DTS:ObjectData to
    capture any available data, ensuring no task type is silently dropped.
    """
    obj_data = exe_el.find(f"{_NS}ObjectData")
    if obj_data is None:
        return None

    props: dict[str, str] = {}
    children = list(obj_data)
    for child in children:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        # Attributes of the child element
        for raw_attr, attr_value in sorted(child.attrib.items()):
            attr_name = raw_attr.split("}")[-1] if "}" in raw_attr else raw_attr
            if attr_name.startswith("xmlns"):
                continue
            key = f"{tag}.{attr_name}" if len(children) > 1 else attr_name
            props[key] = attr_value
        # Text content
        if child.text and child.text.strip():
            props[tag] = child.text.strip()

    if not props and creation_name:
        print(
            f"Warning: unknown task type {creation_name!r} has no extractable ObjectData",
            file=sys.stderr,
        )
    return props if props else None


# ---------------------------------------------------------------------------
# Precedence constraint extraction
# ---------------------------------------------------------------------------

_VALUE_MAP: dict[str, str] = {
    "0": "Success",
    "1": "Failure",
    "2": "Completion",
}

_EVALOP_MAP: dict[str, str] = {
    "1": "Constraint",
    "2": "Expression",
    "3": "ExpressionAndConstraint",
    "4": "ExpressionOrConstraint",
}


def _extract_precedence_constraints(
    parent: ET.Element,
    format_version: int,
) -> list[PrecedenceConstraint]:
    """Extract ``DTS:PrecedenceConstraint`` children into model objects.

    Format 8 stores constraints in a ``DTS:PrecedenceConstraints`` container
    with attribute-based properties; format 2/6 may have them as direct
    children with ``DTS:Property`` sub-elements.
    """
    constraints: list[PrecedenceConstraint] = []

    # Try container first (format 8), then direct children
    pc_container = parent.find(f"{_NS}PrecedenceConstraints")
    if pc_container is not None:
        pc_elements = list(pc_container.findall(f"{_NS}PrecedenceConstraint"))
    else:
        pc_elements = list(parent.findall(f"{_NS}PrecedenceConstraint"))

    for pc_el in pc_elements:
        from_task = get_property(pc_el, "From", format_version) or ""
        to_task = get_property(pc_el, "To", format_version) or ""

        value_raw = get_property(pc_el, "Value", format_version) or "0"
        value = _VALUE_MAP.get(value_raw)
        if value is None:
            print(
                f"Warning: unknown precedence constraint Value {value_raw!r}, "
                f"using raw value",
                file=sys.stderr,
            )
            value = value_raw

        eval_op_raw = get_property(pc_el, "EvalOp", format_version) or "1"
        eval_op = _EVALOP_MAP.get(eval_op_raw)
        if eval_op is None:
            print(
                f"Warning: unknown precedence constraint EvalOp {eval_op_raw!r}, "
                f"using raw value",
                file=sys.stderr,
            )
            eval_op = eval_op_raw

        expression = get_property(pc_el, "Expression", format_version) or None

        # LogicalAnd: "-1"/"True" → True (AND), "0"/"False" → False (OR)
        logical_and_raw = get_property(pc_el, "LogicalAnd", format_version)
        logical_and = logical_and_raw not in ("0", "False") if logical_and_raw else True

        constraints.append(PrecedenceConstraint(
            from_task=from_task,
            to_task=to_task,
            value=value,
            eval_op=eval_op,
            expression=expression,
            logical_and=logical_and,
        ))

    return constraints


# ---------------------------------------------------------------------------
# Executable map & constraint resolution
# ---------------------------------------------------------------------------


def build_executable_map(executables: list[Executable]) -> dict[str, str]:
    """Build a GUID/refId → name lookup for all executables (recursively).

    Keys are upper-cased DTSIDs and raw refId strings; values are task names.
    This enables resolution of raw From/To references in precedence
    constraints to human-readable names.
    """
    exe_map: dict[str, str] = {}
    for exe in executables:
        if exe.dtsid:
            exe_map[exe.dtsid.upper()] = exe.name
        if exe.ref_id:
            exe_map[exe.ref_id] = exe.name
        # Recurse into children
        if exe.children:
            exe_map.update(build_executable_map(exe.children))
    return exe_map


def resolve_constraints(
    constraints: list[PrecedenceConstraint],
    exe_map: dict[str, str],
    format_version: int,
) -> list[PrecedenceConstraint]:
    """Return new constraint objects with from_task/to_task resolved to names.

    * Format 8 (refId paths): look up in exe_map; fallback extracts last
      path segment (e.g. ``"Package\\Container\\TaskName"`` → ``"TaskName"``).
    * Format 2/6 (GUIDs): look up upper-cased GUID in exe_map; fallback
      emits ``[unresolved: {GUID}]``.
    """
    resolved: list[PrecedenceConstraint] = []
    for c in constraints:
        from_name = _resolve_ref(c.from_task, exe_map, format_version)
        to_name = _resolve_ref(c.to_task, exe_map, format_version)
        resolved.append(PrecedenceConstraint(
            from_task=from_name,
            to_task=to_name,
            value=c.value,
            eval_op=c.eval_op,
            expression=c.expression,
            logical_and=c.logical_and,
        ))
    return resolved


def _resolve_ref(
    raw: str,
    exe_map: dict[str, str],
    format_version: int,
) -> str:
    """Resolve a single From/To reference to a human-readable name."""
    if not raw:
        return raw

    # Direct lookup (handles refId paths for format 8)
    if raw in exe_map:
        return exe_map[raw]

    # Case-insensitive GUID lookup
    upper = raw.upper()
    if upper in exe_map:
        return exe_map[upper]

    # Fallback strategies differ by format
    if format_version >= 8:
        # Extract last path segment from refId (e.g. "Package\Task" → "Task")
        last_segment = raw.rsplit("\\", 1)[-1]
        if last_segment != raw:
            print(
                f"Warning: refId {raw!r} not found in executable map, "
                f"using last segment {last_segment!r}",
                file=sys.stderr,
            )
            return last_segment
        print(
            f"Warning: refId {raw!r} not found in executable map",
            file=sys.stderr,
        )
        return raw
    else:
        # Format 2/6: GUID not found
        print(
            f"Warning: GUID {raw!r} not found in executable map",
            file=sys.stderr,
        )
        return f"[unresolved: {raw}]"


# ---------------------------------------------------------------------------
# Logging options extraction
# ---------------------------------------------------------------------------


def _parse_event_filter(raw: str) -> list[str]:
    """Parse an EventFilter string into a list of event names.

    Format: ``count,id1,name1,id2,name2,...``
    Extracts event names at every other position starting at index 2.
    Returns an empty list for empty or malformed input.
    """
    if not raw or not raw.strip():
        return []
    parts = raw.split(",")
    if len(parts) < 3:
        return []
    # Validate count
    try:
        count = int(parts[0])
    except (ValueError, TypeError):
        print(
            f"Warning: EventFilter count is not a valid integer ({parts[0]!r}), "
            f"parsing remaining entries",
            file=sys.stderr,
        )
        count = -1  # parse what we can

    names: list[str] = []
    # Names are at indices 2, 4, 6, ... (every other starting at 2)
    for i in range(2, len(parts), 2):
        name = parts[i].strip()
        if name:
            names.append(name)

    if count >= 0 and len(names) != count:
        print(
            f"Warning: EventFilter declared {count} events but found {len(names)}",
            file=sys.stderr,
        )

    return names


def _parse_column_filter(
    filter_el: ET.Element,
) -> tuple[str, list[str]]:
    """Parse a ColumnFilter property element into (event_name, [included_columns]).

    Each child ``DTS:Property`` has ``DTS:Name`` = column name and text = ``-1``
    (include) or ``0`` (exclude).  Only included columns (text ``-1``) are returned.
    """
    event_name = filter_el.get(f"{_NS}EventName") or ""
    included: list[str] = []
    for col_prop in filter_el.findall(f"{_NS}Property"):
        col_name = col_prop.get(f"{_NS}Name") or ""
        col_value = col_prop.text or ""
        if col_name and col_value.strip() == "-1":
            included.append(col_name)
    return event_name, included


def _extract_logging_options(
    exe_el: ET.Element,
    format_version: int,
    log_provider_map: dict[str, LogProvider] | None,
) -> LoggingOptions | None:
    """Extract ``DTS:LoggingOptions`` from an executable element.

    Returns ``None`` when LoggingMode is 0 (UseParent) and no explicit
    configuration is present, indicating the executable inherits its
    parent's logging settings.

    Parameters
    ----------
    exe_el:
        The executable XML element.
    format_version:
        Package format version.
    log_provider_map:
        DTSID → LogProvider map for resolving selected log providers.
    """
    # Find LoggingOptions element — may be direct child or inside container
    log_opts_el = exe_el.find(f"{_NS}LoggingOptions")
    if log_opts_el is None:
        return None

    logging_mode = safe_int(
        get_property(log_opts_el, "LoggingMode", format_version),
        "LoggingMode",
    )
    filter_kind = safe_int(
        get_property(log_opts_el, "FilterKind", format_version),
        "FilterKind",
    )

    # EventFilter: comma-delimited string
    event_filter_raw = get_property(log_opts_el, "EventFilter", format_version) or ""
    event_filters = _parse_event_filter(event_filter_raw)

    # ColumnFilter: one or more DTS:Property[@DTS:Name='ColumnFilter'] children
    column_filters: dict[str, list[str]] = {}
    for prop in log_opts_el.findall(f"{_NS}Property"):
        if prop.get(f"{_NS}Name") == "ColumnFilter":
            event_name, included_cols = _parse_column_filter(prop)
            if event_name and included_cols:
                column_filters[event_name] = included_cols

    # SelectedLogProvider: resolve InstanceID via log_provider_map
    selected_log_providers: list[str] = []
    for slp_el in log_opts_el.findall(f"{_NS}SelectedLogProvider"):
        instance_id = slp_el.get(f"{_NS}InstanceID") or ""
        if instance_id and log_provider_map is not None:
            provider = log_provider_map.get(instance_id.upper())
            if provider is not None:
                selected_log_providers.append(provider.name)
            else:
                print(
                    f"Warning: SelectedLogProvider InstanceID {instance_id!r} "
                    f"not found in log provider map",
                    file=sys.stderr,
                )
                selected_log_providers.append(instance_id)
        elif instance_id:
            selected_log_providers.append(instance_id)

    # Suppress if UseParent with no explicit configuration
    has_config = bool(event_filters or column_filters or selected_log_providers)
    if logging_mode == 0 and not has_config:
        return None

    return LoggingOptions(
        logging_mode=logging_mode,
        filter_kind=filter_kind,
        event_filters=event_filters,
        column_filters=column_filters,
        selected_log_providers=selected_log_providers,
    )


# ---------------------------------------------------------------------------
# Event handler extraction
# ---------------------------------------------------------------------------

_MAX_EVENT_HANDLER_DEPTH = 10


def _extract_event_handlers(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
    log_provider_map: dict[str, LogProvider] | None = None,
    _depth: int = 0,
) -> list[EventHandler]:
    """Extract ``DTS:EventHandler`` children from an executable element.

    Event handlers are containers that can hold variables, logging options,
    child executables, and precedence constraints — just like regular
    executables.  Child executables are extracted recursively.

    Parameters
    ----------
    exe_el:
        The parent executable XML element.
    format_version:
        Package format version.
    connection_map:
        Connection Map for resolving component connections.
    log_provider_map:
        DTSID → LogProvider map for resolving selected log providers.
    _depth:
        Current recursion depth (capped at :data:`_MAX_EVENT_HANDLER_DEPTH`).
    """
    if _depth >= _MAX_EVENT_HANDLER_DEPTH:
        print(
            f"Warning: event handler recursion depth {_depth} exceeds limit "
            f"{_MAX_EVENT_HANDLER_DEPTH}, stopping recursion",
            file=sys.stderr,
        )
        return []

    handlers: list[EventHandler] = []

    # Format 8: nested inside DTS:EventHandlers container
    container = exe_el.find(f"{_NS}EventHandlers")
    if container is not None:
        handler_els = list(container.findall(f"{_NS}EventHandler"))
    else:
        # Format 2/3/6: direct children
        handler_els = list(exe_el.findall(f"{_NS}EventHandler"))

    for eh_el in handler_els:
        event_name = get_property(eh_el, "EventName", format_version) or ""
        # Event handler CreationName is the event name itself (OnError, etc.),
        # not a task type — skip normalize_creation_name to avoid spurious warnings
        creation_name = get_property(eh_el, "CreationName", format_version) or ""
        event_id = get_property(eh_el, "EventID", format_version) or ""
        disabled = safe_bool(get_property(eh_el, "Disabled", format_version))
        dtsid = get_property(eh_el, "DTSID", format_version) or ""

        # Variables within the event handler
        variables = extract_variables(eh_el, format_version, scope=event_name)

        # Logging options on the event handler
        logging = _extract_logging_options(eh_el, format_version, log_provider_map)

        # Child executables within the event handler (depth incremented)
        child_executables = _extract_executables(
            eh_el, format_version, connection_map, log_provider_map,
            _eh_depth=_depth + 1,
        )

        # Precedence constraints within the event handler
        precedence_constraints = _extract_precedence_constraints(
            eh_el, format_version,
        )

        handlers.append(EventHandler(
            event_name=event_name,
            creation_name=creation_name,
            event_id=event_id,
            disabled=disabled,
            dtsid=dtsid,
            executables=child_executables,
            variables=variables,
            logging=logging,
            precedence_constraints=precedence_constraints,
        ))

    return handlers


# ---------------------------------------------------------------------------
# Single executable extraction
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Single executable extraction
# ---------------------------------------------------------------------------


def _extract_core_props(
    exe_el: ET.Element,
    format_version: int,
) -> dict:
    """Extract common fields shared by all executable types."""
    name = get_property(exe_el, "ObjectName", format_version) or ""
    creation_name_raw = get_property(exe_el, "CreationName", format_version) or ""
    creation_name = normalize_creation_name(creation_name_raw)
    dtsid = get_property(exe_el, "DTSID", format_version) or ""
    description = get_property(exe_el, "Description", format_version) or ""
    disabled = safe_bool(get_property(exe_el, "Disabled", format_version))
    ref_id = exe_el.get(f"{_NS}refId") or ""
    transaction_option = get_property(exe_el, "TransactionOption", format_version) or ""
    isolation_level = get_property(exe_el, "ISOLevel", format_version) or ""
    max_error_count = safe_int(
        get_property(exe_el, "MaxErrorCount", format_version),
        "MaxErrorCount",
        1,
    )
    fail_package_on_failure = safe_bool(
        get_property(exe_el, "FailPackageOnFailure", format_version)
    )
    fail_parent_on_failure = safe_bool(
        get_property(exe_el, "FailParentOnFailure", format_version)
    )
    delay_validation = safe_bool(
        get_property(exe_el, "DelayValidation", format_version)
    )
    disable_event_handlers = safe_bool(
        get_property(exe_el, "DisableEventHandlers", format_version)
    )
    property_expressions = _extract_property_expressions(exe_el, format_version)

    return {
        "name": name,
        "creation_name": creation_name,
        "dtsid": dtsid,
        "description": description,
        "disabled": disabled,
        "ref_id": ref_id,
        "transaction_option": transaction_option,
        "isolation_level": isolation_level,
        "max_error_count": max_error_count,
        "fail_package_on_failure": fail_package_on_failure,
        "fail_parent_on_failure": fail_parent_on_failure,
        "delay_validation": delay_validation,
        "disable_event_handlers": disable_event_handlers,
        "property_expressions": property_expressions,
    }


def _extract_task_payload(
    exe_el: ET.Element,
    creation_name: str,
    name: str,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
) -> dict:
    """Dispatch to type-specific extractors and return payload fields."""
    data_flow: DataFlow | None = None
    execute_sql: ExecuteSqlPayload | None = None
    for_loop: ForLoopPayload | None = None
    for_each: ForEachEnumerator | None = None
    file_system: FileSystemPayload | None = None
    send_mail: SendMailPayload | None = None
    script_task: ScriptTaskPayload | None = None
    expression_task_expression: str | None = None
    execute_package: ExecutePackagePayload | None = None
    as_processing: ASProcessingPayload | None = None
    ftp_task: FtpTaskPayload | None = None
    transfer_task: TransferTaskPayload | None = None
    generic_payload: dict[str, str] | None = None

    if creation_name == "Pipeline":
        pipeline_el = _find_pipeline_element(exe_el)
        if pipeline_el is not None:
            try:
                df, _lineage_map, _id_map = extract_data_flow(
                    pipeline_el, format_version, connection_map
                )
                data_flow = df
            except (ValueError, KeyError, AttributeError) as exc:
                print(
                    f"Warning: failed to extract data flow for {name!r}: {exc}",
                    file=sys.stderr,
                )
        else:
            print(
                f"Warning: Pipeline executable {name!r} has no <pipeline> element",
                file=sys.stderr,
            )
    elif creation_name == "ExecuteSQLTask":
        execute_sql = _extract_execute_sql(exe_el, format_version, connection_map)
    elif creation_name == "ForLoop":
        for_loop = _extract_for_loop(exe_el, format_version)
    elif creation_name == "ForEachLoop":
        for_each = _extract_for_each(exe_el, format_version, connection_map)
    elif creation_name == "FileSystemTask":
        file_system = _extract_file_system(exe_el, format_version, connection_map)
    elif creation_name == "SendMailTask":
        send_mail = _extract_send_mail(exe_el, format_version, connection_map)
    elif creation_name == "ScriptTask":
        script_task = _extract_script_task(exe_el, format_version)
    elif creation_name == "ExpressionTask":
        expression_task_expression = _extract_expression_task(exe_el, format_version)
    elif creation_name == "ExecutePackageTask":
        execute_package = _extract_execute_package(exe_el, format_version, connection_map)
    elif creation_name == "DTSProcessingTask":
        as_processing = _extract_as_processing(exe_el, format_version, connection_map)
    elif creation_name == "FtpTask":
        ftp_task = _extract_ftp_task(exe_el, format_version, connection_map)
    elif creation_name in _TRANSFER_TASK_TYPES:
        transfer_task = _extract_transfer_task(
            exe_el, format_version, creation_name, connection_map
        )
    elif creation_name == "BulkInsertTask":
        generic_payload = _extract_bulk_insert(exe_el, format_version, connection_map)
    elif creation_name == "DataProfilingTask":
        generic_payload = _extract_data_profiling(exe_el, format_version, connection_map)
    elif creation_name == "XMLTask":
        generic_payload = _extract_xml_task(exe_el, format_version, connection_map)
    elif creation_name == "WebServiceTask":
        generic_payload = _extract_web_service(exe_el, format_version, connection_map)
    elif creation_name not in _KNOWN_TASK_TYPES:
        generic_payload = _extract_unknown_payload(
            exe_el, creation_name, connection_map
        )

    return {
        "data_flow": data_flow,
        "execute_sql": execute_sql,
        "for_loop": for_loop,
        "for_each": for_each,
        "file_system": file_system,
        "send_mail": send_mail,
        "script_task": script_task,
        "expression_task_expression": expression_task_expression,
        "execute_package": execute_package,
        "as_processing": as_processing,
        "ftp_task": ftp_task,
        "transfer_task": transfer_task,
        "generic_payload": generic_payload,
    }


def _extract_single_executable(
    exe_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
    log_provider_map: dict[str, LogProvider] | None = None,
    _eh_depth: int = 0,
) -> Executable:
    """Extract one DTS:Executable into an Executable model.

    Recursively processes child executables.  When the CreationName
    normalizes to ``"Pipeline"``, extracts the data flow.
    """
    core = _extract_core_props(exe_el, format_version)
    payload = _extract_task_payload(
        exe_el, core["creation_name"], core["name"],
        format_version, connection_map,
    )

    # Precedence constraints
    precedence_constraints = _extract_precedence_constraints(exe_el, format_version)

    # Container-scoped variables
    variables = extract_variables(exe_el, format_version, scope=core["name"])

    # Logging options
    logging = _extract_logging_options(exe_el, format_version, log_provider_map)

    # Event handlers
    event_handlers = _extract_event_handlers(
        exe_el, format_version, connection_map, log_provider_map,
        _depth=_eh_depth,
    )

    # Recurse into children
    children = _extract_executables(
        exe_el, format_version, connection_map, log_provider_map,
        _eh_depth=_eh_depth,
    )

    return Executable(
        **core,
        **payload,
        precedence_constraints=precedence_constraints,
        variables=variables,
        logging=logging,
        event_handlers=event_handlers,
        children=children,
    )


def _extract_executables(
    parent: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None,
    log_provider_map: dict[str, LogProvider] | None = None,
    _eh_depth: int = 0,
) -> list[Executable]:
    """Walk child DTS:Executable elements under *parent* and extract each."""
    child_elements = _find_child_executables(parent, format_version)
    executables: list[Executable] = []
    for exe_el in child_elements:
        executables.append(
            _extract_single_executable(
                exe_el, format_version, connection_map, log_provider_map,
                _eh_depth=_eh_depth,
            )
        )
    return executables


def extract_executables(
    root: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None = None,
    log_provider_map: dict[str, LogProvider] | None = None,
) -> list[Executable]:
    """Extract all top-level executables from a .dtsx root element.

    Parameters
    ----------
    root:
        The root ``DTS:Executable`` element of the package.
    format_version:
        Package format version (2, 3, 6, or 8).
    connection_map:
        Connection Map for resolving component connections inside data
        flows.  Keys are upper-cased DTSIDs and/or refId paths.
    log_provider_map:
        DTSID → LogProvider map for resolving selected log providers in
        logging options.

    Returns
    -------
    list[Executable]
        The top-level executables, each potentially containing nested
        children and/or a ``data_flow``.
    """
    return _extract_executables(root, format_version, connection_map, log_provider_map)


def extract_root_precedence_constraints(
    root: ET.Element,
    format_version: int,
) -> list[PrecedenceConstraint]:
    """Extract top-level precedence constraints from the package root element.

    These constraints link top-level executables within the package and are
    stored directly under the root ``DTS:Executable`` element.

    Parameters
    ----------
    root:
        The root ``DTS:Executable`` element of the package.
    format_version:
        Package format version (2, 3, 6, or 8).
    """
    return _extract_precedence_constraints(root, format_version)


def extract_root_event_handlers(
    root: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None = None,
    log_provider_map: dict[str, LogProvider] | None = None,
) -> list[EventHandler]:
    """Extract event handlers from the package root element.

    These are package-level event handlers (e.g., OnError at the package scope).

    Parameters
    ----------
    root:
        The root ``DTS:Executable`` element of the package.
    format_version:
        Package format version (2, 3, 6, or 8).
    connection_map:
        Connection Map for resolving component connections.
    log_provider_map:
        DTSID → LogProvider map for resolving selected log providers.
    """
    return _extract_event_handlers(
        root, format_version, connection_map, log_provider_map,
    )
