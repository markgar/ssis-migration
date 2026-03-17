"""Extract data flow pipeline components and build the Lineage Map."""

from __future__ import annotations

import html as html_mod
import re
import sys
import xml.etree.ElementTree as ET

from lookups import resolve_component_class, resolve_data_type
from models import (
    Component,
    ComponentConnection,
    ComponentInput,
    ComponentOutput,
    ConnectionManager,
    DataFlow,
    DataFlowPath,
    InputColumn,
    OleDbParameterMapping,
    OutputColumn,
    ReferenceColumn,
    ScriptComponentData,
)
from normalizers import decode_escapes
from xml_helpers import (
    REFID_BRACKET_RE as _REFID_BRACKET_RE,
    resolve_connection_ref,
    safe_id as _safe_id,
    safe_int as _safe_int,
)

# Type alias for the Lineage Map: lineageId → (column_name, data_type, component_name)
LineageMap = dict[int | str, tuple[str, str, str]]

# Type alias for the Output/Input ID Map: id → (component_name, output_or_input_name)
OutputInputIdMap = dict[int | str, tuple[str, str]]

# HTML entity pattern for detecting values that need decoding
_HTML_ENTITY_RE = re.compile(r"&(?:lt|gt|amp|quot|apos|#\d+|#x[0-9a-fA-F]+);")

# Regex for ParameterMapping entries:  "@name":Direction,{guid},Variable
_PARAM_MAPPING_RE = re.compile(
    r'"([^"]+)"'       # quoted parameter name
    r"\s*:\s*"         # colon separator
    r"(\w+)"           # direction (Input/Output)
    r"\s*,\s*"         # comma
    r"(\{[^}]+\})"    # data type GUID
    r"\s*,\s*"         # comma
    r"([^;]+)"         # variable name
)

# Alternate format: "@name:Direction",{guid}  (direction inside quotes)
_PARAM_MAPPING_EMBEDDED_RE = re.compile(
    r'"@([^:"]+):(\w+)"'  # quoted "@name:Direction"
    r"\s*,\s*"              # comma
    r"(\{[^}]+\})"         # data type GUID
)

# Bare format: "@name",{guid}  (no direction)
_PARAM_MAPPING_BARE_RE = re.compile(
    r'"([^"]+)"'       # quoted parameter name
    r"\s*,\s*"         # comma
    r"(\{[^}]+\})"    # data type GUID
)


def _parse_reference_metadata_xml(xml_text: str) -> list[ReferenceColumn]:
    """Parse ``ReferenceMetadataXml`` into a list of :class:`ReferenceColumn`.

    The *xml_text* should already be HTML-unescaped (done by
    ``_extract_element_properties``).  This function parses the resulting
    XML and extracts ``<referenceColumn>`` elements.
    """
    if not xml_text or not xml_text.strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        print(
            f"Warning: failed to parse ReferenceMetadataXml: {exc}",
            file=sys.stderr,
        )
        return []

    columns: list[ReferenceColumn] = []
    # Handle both root-level referenceColumns and nested
    for ref_col in root.iter("referenceColumn"):
        name = ref_col.get("name", "")
        raw_dt = ref_col.get("dataType", "")
        data_type = _resolve_column_data_type(raw_dt)
        length = _safe_int(ref_col.get("length"), "referenceColumn length")
        precision = _safe_int(ref_col.get("precision"), "referenceColumn precision")
        scale = _safe_int(ref_col.get("scale"), "referenceColumn scale")
        columns.append(
            ReferenceColumn(
                name=name,
                data_type=data_type,
                length=length,
                precision=precision,
                scale=scale,
            )
        )

    return columns


def _resolve_parameter_map(
    param_map: str,
    lineage_map: LineageMap,
) -> list[tuple[int, str]]:
    """Parse a ``ParameterMap`` string and resolve lineage IDs.

    Format: ``"#21;#42;"`` where each ``#N;`` is a lineageId.

    Returns a list of ``(ordinal, column_name)`` tuples in order.
    """
    if not param_map:
        return []

    results: list[tuple[int, str]] = []
    # Strip '#' and split on ';'
    parts = param_map.replace("#", "").split(";")
    for ordinal, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
        lineage_id = _safe_int(part, "ParameterMap lineageId", -1)
        if lineage_id < 0:
            continue
        if lineage_id in lineage_map:
            col_name, _dt, _comp = lineage_map[lineage_id]
        else:
            col_name = f"[lineageId={lineage_id}]"
            print(
                f"Warning: ParameterMap lineageId {lineage_id} not found "
                f"in Lineage Map",
                file=sys.stderr,
            )
        results.append((ordinal, col_name))

    return results


def _parse_oledb_parameter_mapping(mapping_str: str) -> list[OleDbParameterMapping]:
    """Parse an OLE DB Source ``ParameterMapping`` property.

    Format: semicolon-delimited entries like
    ``"@StartDate":Input,{guid},User::StartDate;``

    Returns a list of :class:`OleDbParameterMapping` objects.
    """
    if not mapping_str:
        return []

    results: list[OleDbParameterMapping] = []
    entries = mapping_str.split(";")
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        match = _PARAM_MAPPING_RE.match(entry)
        if match:
            param_name = match.group(1)
            direction = match.group(2)
            variable = match.group(4).strip()
            results.append(
                OleDbParameterMapping(
                    parameter_ordinal=param_name,
                    direction=direction,
                    variable_name=variable,
                )
            )
            continue
        match = _PARAM_MAPPING_EMBEDDED_RE.match(entry)
        if match:
            results.append(
                OleDbParameterMapping(
                    parameter_ordinal=f"@{match.group(1)}",
                    direction=match.group(2),
                    variable_name="",
                )
            )
            continue
        match = _PARAM_MAPPING_BARE_RE.match(entry)
        if match:
            results.append(
                OleDbParameterMapping(
                    parameter_ordinal=match.group(1),
                    direction="Input",
                    variable_name="",
                )
            )
            continue
        print(
            f"Warning: could not parse ParameterMapping entry {entry!r}",
            file=sys.stderr,
        )

    return results


def _parse_design_time_properties(xml_text: str) -> dict[str, str]:
    """Parse SCD ``DesignTimeProperties`` embedded XML.

    The *xml_text* should already be HTML-unescaped (done by
    ``_extract_element_properties``).  Returns a dict of property name → value.
    """
    if not xml_text or not xml_text.strip():
        return {}

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        print(
            f"Warning: failed to parse SCD DesignTimeProperties: {exc}",
            file=sys.stderr,
        )
        return {}

    result: dict[str, str] = {}
    for child in root:
        # Use local name (strip any namespace)
        tag = child.tag.rsplit("}", 1)[-1] if "}" in child.tag else child.tag
        result[tag] = (child.text or "").strip()

    return result


def _parse_array_property(prop_el: ET.Element) -> list[str]:
    """Parse an ``isArray="true"`` property element into a list of strings.

    Handles ``state="cdata"`` (CDATA-wrapped values), ``state="escaped"``
    (HTML-entity-encoded values), and ``state="default"`` (plain text).

    Returns an empty list when the element has no ``arrayElements`` child
    or no ``arrayElement`` children.
    """
    array_els_container = prop_el.find("arrayElements")
    if array_els_container is None:
        return []

    state = prop_el.get("state", "default")
    values: list[str] = []

    for ae in array_els_container.findall("arrayElement"):
        raw = ae.text or ""
        if state == "escaped" and raw:
            raw = html_mod.unescape(raw)
        values.append(raw)

    return values


def _extract_script_component(
    comp_el: ET.Element,
    comp: Component,
) -> None:
    """Extract script component data from a Script Component element.

    Populates ``comp.script_data`` with language, variables, and source files.
    Requires raw XML access because ``SourceCode`` is an array property
    whose content lives in ``arrayElement`` child nodes (not in prop text).
    """
    # ScriptLanguage from already-extracted flat properties
    script_language = comp.properties.get("ScriptLanguage", "")
    if not script_language:
        print(
            f"Warning: Script Component {comp.name!r} has no ScriptLanguage property",
            file=sys.stderr,
        )

    # Split comma-separated variable lists
    ro_raw = comp.properties.get("ReadOnlyVariables", "")
    rw_raw = comp.properties.get("ReadWriteVariables", "")
    read_only = [v.strip() for v in ro_raw.split(",") if v.strip()] if ro_raw else []
    read_write = [v.strip() for v in rw_raw.split(",") if v.strip()] if rw_raw else []

    # Parse SourceCode array property from raw XML
    source_files: dict[str, str] = {}
    props_el = comp_el.find("properties")
    if props_el is not None:
        for prop in props_el.findall("property"):
            if prop.get("name") == "SourceCode":
                values = _parse_array_property(prop)
                if len(values) % 2 != 0:
                    print(
                        f"Warning: Script Component {comp.name!r} SourceCode has "
                        f"odd element count ({len(values)}), ignoring last element",
                        file=sys.stderr,
                    )
                for i in range(0, len(values) - 1, 2):
                    filename = values[i].lstrip("\\")
                    content = values[i + 1]
                    if filename:
                        source_files[filename] = content
                break

    comp.script_data = ScriptComponentData(
        script_language=script_language,
        read_only_variables=read_only,
        read_write_variables=read_write,
        source_files=source_files,
    )


def _post_process_components(
    components: list[Component],
    comp_elements: list[ET.Element],
    lineage_map: LineageMap,
) -> None:
    """Post-process extracted components to populate type-specific fields.

    Handles:
    - Lookup: ReferenceMetadataXml double-parse, ParameterMap resolution
    - OLE DB Source: ParameterMapping parsing
    - Data Conversion: SourceInputColumnLineageID resolution
    - SCD: DesignTimeProperties double-parse
    - Script Component: language, variables, source code extraction
    """
    for comp_el, comp in zip(comp_elements, components):
        cn = comp.class_name

        if cn == "Lookup":
            # Double-parse ReferenceMetadataXml
            ref_xml = comp.properties.get("ReferenceMetadataXml", "")
            if ref_xml:
                comp.reference_columns = _parse_reference_metadata_xml(ref_xml)

        elif cn == "OLE DB Source":
            # Parse ParameterMapping
            param_mapping = comp.properties.get("ParameterMapping", "")
            if param_mapping:
                comp.parameter_mappings = _parse_oledb_parameter_mapping(
                    param_mapping
                )

        elif cn == "Data Conversion":
            # Resolve SourceInputColumnLineageID on output columns
            for out in comp.outputs:
                if out.is_error_output:
                    continue
                for col in out.columns:
                    raw_id = col.properties.get("SourceInputColumnLineageID", "")
                    if not raw_id:
                        continue
                    src_lid = _safe_id(raw_id, "SourceInputColumnLineageID")
                    if src_lid in lineage_map:
                        src_name, src_dtype, _src_comp = lineage_map[src_lid]
                        col.properties["_resolved_source_column"] = src_name
                        col.properties["_resolved_source_data_type"] = src_dtype
                    else:
                        col.properties["_resolved_source_column"] = (
                            f"[lineageId={src_lid}]"
                        )
                        print(
                            f"Warning: SourceInputColumnLineageID {src_lid} "
                            f"not found in Lineage Map",
                            file=sys.stderr,
                        )

        elif cn == "Slowly Changing Dimension":
            # Double-parse DesignTimeProperties embedded XML
            dtp_xml = comp.properties.get("DesignTimeProperties", "")
            if dtp_xml:
                dtp = _parse_design_time_properties(dtp_xml)
                for key, val in dtp.items():
                    comp.properties["_dtp_" + key] = val

        elif cn == "Script Component":
            _extract_script_component(comp_el, comp)


def _extract_element_properties(element: ET.Element) -> dict[str, str]:
    """Parse all ``<property>`` children from a ``<properties>`` container.

    For each ``<property>`` element:
    - Reads ``@name`` and text content
    - If ``@containsID == "true"`` or value contains HTML entities: HTML-unescapes
    - Stores ``{name: value}``

    Returns a dict of ``{property_name: decoded_value}``.
    """
    properties: dict[str, str] = {}
    props_el = element.find("properties")
    if props_el is None:
        return properties

    for prop in props_el.findall("property"):
        prop_name = prop.get("name", "")
        if not prop_name:
            continue
        value = (prop.text or "").strip()

        # Decode HTML entities when containsID is set or entities are present
        contains_id = prop.get("containsID", "").lower() == "true"
        if contains_id or _HTML_ENTITY_RE.search(value):
            value = html_mod.unescape(value)

        properties[prop_name] = value

    return properties


def _resolve_column_data_type(raw: str) -> str:
    """Resolve a column dataType attribute to a display string.

    Pipeline output columns encode the data type either as an integer code
    (resolvable via the SSIS data-type map) or as a string identifier
    (``"r4"``, ``"wstr"``, etc.).  We try the integer path first and fall
    back to the raw string.
    """
    if not raw:
        return ""
    try:
        code = int(raw)
        return resolve_data_type(code)
    except (ValueError, TypeError):
        return raw


def _extract_output_column(col_el: ET.Element) -> OutputColumn:
    """Extract a single ``<outputColumn>`` element into an OutputColumn model."""
    name = decode_escapes(col_el.get("name", ""))
    lineage_id = _safe_id(col_el.get("lineageId"), "outputColumn lineageId")
    data_type = _resolve_column_data_type(col_el.get("dataType", ""))
    length = _safe_int(col_el.get("length"), "outputColumn length")
    precision = _safe_int(col_el.get("precision"), "outputColumn precision")
    scale = _safe_int(col_el.get("scale"), "outputColumn scale")
    code_page = _safe_int(col_el.get("codePage"), "outputColumn codePage")
    sort_key_position = _safe_int(
        col_el.get("sortKeyPosition"), "outputColumn sortKeyPosition"
    )
    error_row_disposition = col_el.get("errorRowDisposition", "")
    truncation_row_disposition = col_el.get("truncationRowDisposition", "")
    external_metadata_column_id = _safe_id(
        col_el.get("externalMetadataColumnId"),
        "outputColumn externalMetadataColumnId",
    )

    # Per-column custom properties (all properties captured)
    properties = _extract_element_properties(col_el)

    return OutputColumn(
        name=name,
        lineage_id=lineage_id,
        data_type=data_type,
        length=length,
        precision=precision,
        scale=scale,
        code_page=code_page,
        sort_key_position=sort_key_position,
        error_row_disposition=error_row_disposition,
        truncation_row_disposition=truncation_row_disposition,
        external_metadata_column_id=external_metadata_column_id,
        properties=properties,
    )


def _get_component_property(component_el: ET.Element, name: str) -> str:
    """Read a named property from a component's ``<properties>`` children.

    HTML-decodes the value when ``containsID`` is set or HTML entities are
    detected, consistent with ``_extract_element_properties`` and the
    Constitution's "Decode on read" rule.
    """
    props_el = component_el.find("properties")
    if props_el is not None:
        for prop in props_el.findall("property"):
            if prop.get("name") == name:
                value = (prop.text or "").strip()
                contains_id = prop.get("containsID", "").lower() == "true"
                if contains_id or _HTML_ENTITY_RE.search(value):
                    value = html_mod.unescape(value)
                return value
    return ""


def _resolve_external_metadata(
    input_el: ET.Element,
    ext_meta_id: int | str,
) -> str:
    """Find the ``<externalMetadataColumn>`` matching *ext_meta_id* in *input_el*.

    For format ≤6, *ext_meta_id* is an integer matching the ``@id`` attribute.
    For format 8, *ext_meta_id* is a RefId string matching the ``@refId`` attribute.

    Returns the column ``@name`` if found, empty string otherwise.
    """
    if ext_meta_id == 0:
        return ""
    ext_cols_el = input_el.find("externalMetadataColumns")
    if ext_cols_el is None:
        print(
            f"Warning: externalMetadataColumnId {ext_meta_id} specified but "
            f"no <externalMetadataColumns> element found",
            file=sys.stderr,
        )
        return ""
    for emc in ext_cols_el.findall("externalMetadataColumn"):
        if isinstance(ext_meta_id, str):
            # Format 8: match on refId attribute
            if emc.get("refId", "") == ext_meta_id:
                return decode_escapes(emc.get("name", ""))
        else:
            # Format ≤6: match on integer id attribute
            emc_id = _safe_int(emc.get("id"), "externalMetadataColumn id")
            if emc_id == ext_meta_id:
                return decode_escapes(emc.get("name", ""))
    # Fallback: for format 8 RefId strings, extract the column name from the RefId path
    if isinstance(ext_meta_id, str):
        m = _REFID_BRACKET_RE.findall(ext_meta_id)
        if m:
            return m[-1]  # Last bracketed segment is the column name
    print(
        f"Warning: externalMetadataColumnId {ext_meta_id} not found in "
        f"<externalMetadataColumns>",
        file=sys.stderr,
    )
    return ""


def _resolve_connection_manager_id(
    conn_manager_id: str,
    connection_map: dict[str, ConnectionManager],
) -> str:
    """Resolve a ``connectionManagerID`` to a connection manager display name."""
    return resolve_connection_ref(conn_manager_id, connection_map) or ""


def _extract_component_connections(
    comp_el: ET.Element,
    connection_map: dict[str, ConnectionManager],
) -> list[ComponentConnection]:
    """Extract ``<connection>`` elements from a component.

    Resolves each ``connectionManagerID`` via the *connection_map*.
    """
    connections: list[ComponentConnection] = []
    conns_el = comp_el.find("connections")
    if conns_el is None:
        return connections

    for conn_el in conns_el.findall("connection"):
        conn_manager_id = conn_el.get("connectionManagerID", "")
        role_name = conn_el.get("name", "")
        resolved_name = _resolve_connection_manager_id(
            conn_manager_id, connection_map
        )
        connections.append(
            ComponentConnection(
                name=role_name,
                connection_manager_name=resolved_name,
            )
        )

    return connections


def _extract_paths(
    pipeline_el: ET.Element,
    id_map: OutputInputIdMap,
) -> list[DataFlowPath]:
    """Extract ``<path>`` elements from the pipeline and resolve endpoints.

    Each path's ``startId`` and ``endId`` are looked up in the
    *id_map* to produce ``(component_name, output_or_input_name)`` pairs.
    Unresolvable IDs produce a warning to stderr.
    """
    paths: list[DataFlowPath] = []
    paths_el = pipeline_el.find("paths")
    if paths_el is None:
        return paths

    for path_el in paths_el.findall("path"):
        path_name = path_el.get("name", "")
        start_id = _safe_id(path_el.get("startId"), "path startId")
        end_id = _safe_id(path_el.get("endId"), "path endId")

        if start_id in id_map:
            start_component, start_output = id_map[start_id]
        else:
            print(
                f"Warning: path {path_name!r} startId {start_id} not found "
                f"in Output/Input ID Map",
                file=sys.stderr,
            )
            start_component = f"[unresolved id={start_id}]"
            start_output = ""

        if end_id in id_map:
            end_component, end_input = id_map[end_id]
        else:
            print(
                f"Warning: path {path_name!r} endId {end_id} not found "
                f"in Output/Input ID Map",
                file=sys.stderr,
            )
            end_component = f"[unresolved id={end_id}]"
            end_input = ""

        paths.append(
            DataFlowPath(
                start_component=start_component,
                start_output=start_output,
                end_component=end_component,
                end_input=end_input,
            )
        )

    return paths


def _extract_input_columns(
    input_el: ET.Element,
    lineage_map: LineageMap,
) -> list[InputColumn]:
    """Extract ``<inputColumn>`` elements from an ``<input>`` element.

    Resolves column name and source component via *lineage_map*.
    Falls back to cached attributes when lineageId is not in the map
    (format-8 packages).
    """
    columns: list[InputColumn] = []
    cols_el = input_el.find("inputColumns")
    if cols_el is None:
        return columns

    for col_el in cols_el.findall("inputColumn"):
        lineage_id = _safe_id(col_el.get("lineageId"), "inputColumn lineageId")
        usage_type = col_el.get("usageType", "")
        ext_meta_id = _safe_id(
            col_el.get("externalMetadataColumnId"),
            "inputColumn externalMetadataColumnId",
        )

        # Resolve from Lineage Map
        if lineage_id in lineage_map:
            col_name, _data_type, source_component = lineage_map[lineage_id]
        else:
            # Format 8 fallback: read cached attributes
            cached_name = col_el.get("cachedName", "")
            if cached_name:
                col_name = decode_escapes(cached_name)
            else:
                col_name = f"[unresolved lineageId={lineage_id}]"
                print(
                    f"Warning: lineageId {lineage_id} not found in Lineage Map "
                    f"and no cachedName available",
                    file=sys.stderr,
                )
            source_component = ""

        # External metadata resolution (§2h)
        external_metadata_column = _resolve_external_metadata(input_el, ext_meta_id)

        # Per-input-column custom properties
        col_properties = _extract_element_properties(col_el)

        columns.append(
            InputColumn(
                lineage_id=lineage_id,
                name=col_name,
                source_component=source_component,
                usage_type=usage_type,
                external_metadata_column=external_metadata_column,
                properties=col_properties,
            )
        )

    return columns


def _extract_component(
    comp_el: ET.Element,
    connection_map: dict[str, ConnectionManager] | None = None,
) -> tuple[Component, LineageMap, OutputInputIdMap]:
    """Extract a single ``<component>`` and its Lineage Map / ID Map contributions.

    Input columns are *not* resolved here because the global Lineage Map
    is required.  Call :func:`_resolve_component_inputs` in a second pass.
    """
    comp_id = _safe_id(comp_el.get("id"), "component id")
    comp_name = decode_escapes(comp_el.get("name", ""))
    class_id = comp_el.get("componentClassID", "")
    description = comp_el.get("description", "")

    # ManagedComponentHost handling: read UserComponentTypeName property
    user_comp_type = _get_component_property(comp_el, "UserComponentTypeName")

    class_name = resolve_component_class(
        class_id,
        description=description,
        user_component_type_name=user_comp_type,
    )

    # Component-level properties (HTML-decoded at extraction time per Constitution)
    comp_properties = _extract_element_properties(comp_el)

    lineage_map: LineageMap = {}
    id_map: OutputInputIdMap = {}
    outputs: list[ComponentOutput] = []

    outputs_el = comp_el.find("outputs")
    if outputs_el is not None:
        for output_el in outputs_el.findall("output"):
            output_id = _safe_id(output_el.get("id"), "output id")
            output_name = output_el.get("name", "")
            is_error_out = output_el.get("isErrorOut", "false").lower() == "true"
            is_default_out = output_el.get("isDefaultOut", "false").lower() == "true"
            exclusion_group = _safe_int(
                output_el.get("exclusionGroup"), "output exclusionGroup"
            )
            sync_input_id = _safe_id(
                output_el.get("synchronousInputId"),
                "output synchronousInputId",
            )
            error_row_disposition = output_el.get("errorRowDisposition", "")
            truncation_row_disposition = output_el.get(
                "truncationRowDisposition", ""
            )

            columns: list[OutputColumn] = []
            cols_el = output_el.find("outputColumns")
            if cols_el is not None:
                for col_el in cols_el.findall("outputColumn"):
                    oc = _extract_output_column(col_el)
                    columns.append(oc)
                    lineage_map[oc.lineage_id] = (
                        oc.name,
                        oc.data_type,
                        comp_name,
                    )

            # Output-level properties (e.g., Conditional Split conditions)
            output_properties = _extract_element_properties(output_el)

            outputs.append(
                ComponentOutput(
                    id=output_id,
                    name=output_name,
                    is_error_output=is_error_out,
                    is_default_output=is_default_out,
                    exclusion_group=exclusion_group,
                    synchronous_input_id=sync_input_id,
                    error_row_disposition=error_row_disposition,
                    truncation_row_disposition=truncation_row_disposition,
                    columns=columns,
                    properties=output_properties,
                )
            )

            # Output/Input ID Map — outputs
            id_map[output_id] = (comp_name, output_name)
            # Format 8: also register by refId for path resolution
            output_ref_id = output_el.get("refId", "")
            if output_ref_id:
                id_map[output_ref_id] = (comp_name, output_name)

    # Collect input metadata for ID map (columns resolved in second pass)
    inputs: list[ComponentInput] = []
    inputs_el = comp_el.find("inputs")
    if inputs_el is not None:
        for input_el in inputs_el.findall("input"):
            input_id = _safe_id(input_el.get("id"), "input id")
            input_name = input_el.get("name", "")
            has_side_effects = (
                input_el.get("hasSideEffects", "false").lower() == "true"
            )
            error_row_disposition = input_el.get("errorRowDisposition", "")

            inputs.append(
                ComponentInput(
                    id=input_id,
                    name=input_name,
                    has_side_effects=has_side_effects,
                    error_row_disposition=error_row_disposition,
                    # columns filled in second pass
                )
            )

            # Output/Input ID Map — inputs
            id_map[input_id] = (comp_name, input_name)
            # Format 8: also register by refId for path resolution
            input_ref_id = input_el.get("refId", "")
            if input_ref_id:
                id_map[input_ref_id] = (comp_name, input_name)

    # Component connections (resolved via Connection Map)
    comp_connections: list[ComponentConnection] = []
    if connection_map is not None:
        comp_connections = _extract_component_connections(comp_el, connection_map)

    component = Component(
        id=comp_id,
        name=comp_name,
        class_id=class_id,
        class_name=class_name,
        outputs=outputs,
        inputs=inputs,
        connections=comp_connections,
        properties=comp_properties,
    )

    return component, lineage_map, id_map


def _resolve_component_inputs(
    comp_el: ET.Element,
    component: Component,
    lineage_map: LineageMap,
) -> None:
    """Second pass: resolve input columns using the global Lineage Map.

    Mutates *component* in-place, populating ``columns`` on each
    ``ComponentInput``.
    """
    inputs_el = comp_el.find("inputs")
    if inputs_el is None:
        return

    input_elements = inputs_el.findall("input")
    for input_el, comp_input in zip(input_elements, component.inputs):
        comp_input.columns = _extract_input_columns(input_el, lineage_map)


def extract_data_flow(
    pipeline_el: ET.Element,
    format_version: int,
    connection_map: dict[str, ConnectionManager] | None = None,
) -> tuple[DataFlow, LineageMap, OutputInputIdMap]:
    """Extract a data flow pipeline from the ``<pipeline>`` element.

    Parameters
    ----------
    pipeline_el:
        The ``<pipeline>`` XML element (found under ``DTS:ObjectData``).
    format_version:
        Package format version (2, 3, 6, or 8).  Not currently used
        within the pipeline extractor (the ``<pipeline>`` sub-elements
        share the same schema across versions), but retained for API
        consistency with other extractors.
    connection_map:
        Connection Map for resolving component connections.  Keys are
        upper-cased DTSIDs and/or refId paths.

    Returns
    -------
    tuple[DataFlow, LineageMap, OutputInputIdMap]
        The populated ``DataFlow`` model, the Lineage Map
        ``{lineageId → (column_name, data_type, component_name)}``,
        and the Output/Input ID Map
        ``{id → (component_name, output_or_input_name)}``.
    """
    # Pipeline-level properties
    raw_max_rows = pipeline_el.get("defaultBufferMaxRows")
    raw_buf_size = pipeline_el.get("defaultBufferSize")
    raw_threads = pipeline_el.get("engineThreads")

    default_buffer_max_rows: int | None = None
    if raw_max_rows is not None:
        default_buffer_max_rows = _safe_int(
            raw_max_rows, "pipeline defaultBufferMaxRows"
        )

    default_buffer_size: int | None = None
    if raw_buf_size is not None:
        default_buffer_size = _safe_int(
            raw_buf_size, "pipeline defaultBufferSize"
        )

    engine_threads: int | None = None
    if raw_threads is not None:
        engine_threads = _safe_int(raw_threads, "pipeline engineThreads")

    # --- Pass 1: extract components, build global Lineage Map & ID Map ---
    components: list[Component] = []
    comp_elements: list[ET.Element] = []
    lineage_map: LineageMap = {}
    id_map: OutputInputIdMap = {}

    comps_el = pipeline_el.find("components")
    if comps_el is not None:
        for comp_el in comps_el.findall("component"):
            component, comp_lineage, comp_id_map = _extract_component(
                comp_el, connection_map=connection_map
            )
            components.append(component)
            comp_elements.append(comp_el)
            lineage_map.update(comp_lineage)
            id_map.update(comp_id_map)

    # --- Pass 2: resolve input columns using the global Lineage Map ---
    for comp_el, component in zip(comp_elements, components):
        _resolve_component_inputs(comp_el, component, lineage_map)

    # --- Pass 2.5: type-specific post-processing (Lookup, OLE DB Source, Script) ---
    _post_process_components(components, comp_elements, lineage_map)

    # --- Pass 3: extract paths using the Output/Input ID Map ---
    paths = _extract_paths(pipeline_el, id_map)

    data_flow = DataFlow(
        components=components,
        paths=paths,
        default_buffer_max_rows=default_buffer_max_rows,
        default_buffer_size=default_buffer_size,
        engine_threads=engine_threads,
    )

    return data_flow, lineage_map, id_map
