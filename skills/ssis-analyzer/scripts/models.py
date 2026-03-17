"""Domain model — typed dataclasses for all extracted SSIS data."""

from __future__ import annotations

from dataclasses import dataclass, field


# §8.1
@dataclass
class PackageMetadata:
    name: str
    description: str = ""
    creator: str = ""
    creator_machine: str = ""
    creation_date: str = ""
    format_version: str = ""
    protection_level: int = 0
    max_concurrent: int = -1
    version: str = ""
    last_modified_version: str = ""
    enable_config: bool = False


# §8.2
@dataclass
class FlatFileColumn:
    name: str
    data_type: str
    delimiter: str
    width: int = 0
    max_width: int = 0
    precision: int = 0
    scale: int = 0
    text_qualified: bool = False


# §8.3
@dataclass
class ConnectionManager:
    name: str
    dtsid: str
    creation_name: str
    description: str = ""
    connection_string: str = ""
    delay_validation: bool = False
    property_expressions: dict[str, str] = field(default_factory=dict)
    flat_file_format: str | None = None
    flat_file_header: bool | None = None
    flat_file_columns: list[FlatFileColumn] | None = None
    is_project_level: bool = False
    file_usage_type: int | None = None
    ref_id: str = ""
    row_delimiter: str = ""
    text_qualifier: str = ""
    code_page: int = 0
    unicode: bool = False
    header_rows_to_skip: int = 0
    parsed_properties: dict[str, str] = field(default_factory=dict)


# §8.4
@dataclass
class Variable:
    namespace: str
    name: str
    data_type: int
    scope: str
    source: str
    value: str = ""
    expression: str | None = None
    data_sub_type: str | None = None
    evaluate_as_expression: bool = False
    read_only: bool = False
    description: str = ""
    dtsid: str = ""


# §8.5
@dataclass
class Configuration:
    name: str
    config_type: int
    config_string: str = ""
    config_value: str = ""
    enabled: bool = True
    dtsid: str = ""


# §8.6
@dataclass
class PackageParameter:
    name: str
    data_type: str
    default_value: str = ""
    required: bool = False
    sensitive: bool = False
    description: str = ""


# §8.7
@dataclass
class SqlParameterBinding:
    parameter_name: str
    variable_name: str
    direction: str
    data_type: str = ""
    size: int = 0


# §8.8
@dataclass
class SqlResultBinding:
    result_name: str
    variable_name: str


# §8.9
@dataclass
class ExecuteSqlPayload:
    sql_statement: str = ""
    sql_source_type: str = "DirectInput"
    connection: str = ""
    result_type: str = ""
    parameter_bindings: list[SqlParameterBinding] = field(default_factory=list)
    result_bindings: list[SqlResultBinding] = field(default_factory=list)
    is_stored_procedure: bool = False
    bypass_prepare: bool = False
    timeout: int = 0
    code_page: int = 0


# §8.10
@dataclass
class FileSystemPayload:
    operation: str
    source_connection: str
    dest_connection: str = ""
    overwrite: bool = False


# §8.11
@dataclass
class SendMailPayload:
    to: str = ""
    cc: str = ""
    bcc: str = ""
    subject: str = ""
    message_source: str = ""
    message_source_type: str = ""
    smtp_connection: str = ""


# §8.12
@dataclass
class ScriptTaskPayload:
    script_language: str = ""
    entry_point: str = ""
    read_only_variables: list[str] = field(default_factory=list)
    read_write_variables: list[str] = field(default_factory=list)
    source_code: dict[str, str] | None = None


# §8.13
@dataclass
class ForLoopPayload:
    init_expression: str = ""
    eval_expression: str = ""
    assign_expression: str = ""


# §8.14
@dataclass
class ExecutePackagePayload:
    use_project_reference: bool = False
    package_name: str = ""
    connection: str | None = None
    parameter_assignments: list[tuple[str, str]] = field(default_factory=list)


# §8.15
@dataclass
class ASProcessingPayload:
    connection: str
    processing_commands: str = ""


# §8.16
@dataclass
class FtpTaskPayload:
    operation: str
    connection: str
    local_path: str = ""
    remote_path: str = ""
    is_transfer_ascii: bool = False


# §8.17
@dataclass
class TransferTaskPayload:
    task_type: str
    source_connection: str
    dest_connection: str
    properties: dict[str, str] = field(default_factory=dict)


# §8.18
@dataclass
class VariableMapping:
    variable_name: str
    value_index: int


# §8.19
@dataclass
class ForEachEnumerator:
    enumerator_type: str
    properties: dict[str, str] = field(default_factory=dict)
    variable_mappings: list[VariableMapping] = field(default_factory=list)
    property_expressions: dict[str, str] = field(default_factory=dict)


# §8.20
@dataclass
class PrecedenceConstraint:
    from_task: str
    to_task: str
    value: str
    eval_op: str
    expression: str | None = None
    logical_and: bool = True


# §8.20b
@dataclass
class ParallelBranch:
    """Group of tasks that can execute concurrently.

    Tasks share the same predecessor with no mutual dependency.
    """
    tasks: list[str]
    shared_predecessor: str


# §8.21
@dataclass
class LoggingOptions:
    filter_kind: int
    logging_mode: int = 0
    event_filters: list[str] = field(default_factory=list)
    column_filters: dict[str, list[str]] = field(default_factory=dict)
    selected_log_providers: list[str] = field(default_factory=list)


# §8.22
@dataclass
class LogProvider:
    name: str
    dtsid: str
    creation_name: str
    config_string: str = ""
    description: str = ""
    delay_validation: bool = False


# §8.23 — forward-referenced via string annotation for recursive Executable
@dataclass
class EventHandler:
    event_name: str
    creation_name: str = ""
    event_id: str = ""
    disabled: bool = False
    dtsid: str = ""
    executables: list[Executable] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    logging: LoggingOptions | None = None
    precedence_constraints: list[PrecedenceConstraint] = field(default_factory=list)


# §8.24
@dataclass
class ReferenceColumn:
    name: str
    data_type: str
    length: int = 0
    precision: int = 0
    scale: int = 0


# §8.25
@dataclass
class OleDbParameterMapping:
    parameter_ordinal: str
    direction: str
    variable_name: str
    data_type: str = ""


# §8.26
@dataclass
class OutputColumn:
    name: str
    lineage_id: int | str
    data_type: str
    length: int = 0
    precision: int = 0
    scale: int = 0
    code_page: int = 0
    sort_key_position: int = 0
    error_row_disposition: str = ""
    truncation_row_disposition: str = ""
    external_metadata_column_id: int | str = 0
    properties: dict[str, str] = field(default_factory=dict)


# §8.27
@dataclass
class InputColumn:
    lineage_id: int | str
    name: str
    source_component: str = ""
    usage_type: str = ""
    external_metadata_column: str = ""
    properties: dict[str, str] = field(default_factory=dict)


# §8.28
@dataclass
class ComponentConnection:
    name: str
    connection_manager_name: str


# §8.29
@dataclass
class ComponentOutput:
    id: int | str
    name: str
    is_error_output: bool = False
    is_default_output: bool = False
    exclusion_group: int = 0
    synchronous_input_id: int | str = 0
    error_row_disposition: str = ""
    truncation_row_disposition: str = ""
    columns: list[OutputColumn] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)


# §8.30
@dataclass
class ComponentInput:
    id: int | str
    name: str
    has_side_effects: bool = False
    error_row_disposition: str = ""
    columns: list[InputColumn] = field(default_factory=list)


# §8.31a
@dataclass
class ScriptComponentData:
    script_language: str = ""
    read_only_variables: list[str] = field(default_factory=list)
    read_write_variables: list[str] = field(default_factory=list)
    source_files: dict[str, str] = field(default_factory=dict)


# §8.31
@dataclass
class Component:
    id: int | str
    name: str
    class_id: str
    class_name: str
    outputs: list[ComponentOutput] = field(default_factory=list)
    inputs: list[ComponentInput] = field(default_factory=list)
    connections: list[ComponentConnection] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    reference_columns: list[ReferenceColumn] = field(default_factory=list)
    parameter_mappings: list[OleDbParameterMapping] = field(default_factory=list)
    script_data: ScriptComponentData | None = None


# §8.32
@dataclass
class DataFlowPath:
    start_component: str
    start_output: str
    end_component: str
    end_input: str


# §8.33
@dataclass
class DataFlow:
    components: list[Component] = field(default_factory=list)
    paths: list[DataFlowPath] = field(default_factory=list)
    default_buffer_max_rows: int | None = None
    default_buffer_size: int | None = None
    engine_threads: int | None = None


# §8.34
@dataclass
class Executable:
    name: str
    creation_name: str
    dtsid: str = ""
    description: str = ""
    disabled: bool = False
    ref_id: str = ""
    transaction_option: str = ""
    isolation_level: str = ""
    max_error_count: int = 1
    fail_package_on_failure: bool = False
    fail_parent_on_failure: bool = False
    delay_validation: bool = False
    disable_event_handlers: bool = False
    property_expressions: dict[str, str] = field(default_factory=dict)
    children: list[Executable] = field(default_factory=list)
    precedence_constraints: list[PrecedenceConstraint] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    logging: LoggingOptions | None = None
    event_handlers: list[EventHandler] = field(default_factory=list)
    execute_sql: ExecuteSqlPayload | None = None
    file_system: FileSystemPayload | None = None
    send_mail: SendMailPayload | None = None
    script_task: ScriptTaskPayload | None = None
    for_loop: ForLoopPayload | None = None
    for_each: ForEachEnumerator | None = None
    execute_package: ExecutePackagePayload | None = None
    as_processing: ASProcessingPayload | None = None
    ftp_task: FtpTaskPayload | None = None
    transfer_task: TransferTaskPayload | None = None
    data_flow: DataFlow | None = None
    generic_payload: dict[str, str] | None = None
    expression_task_expression: str | None = None


# §8.35
@dataclass
class VariableReference:
    variable_name: str
    set_by: list[str] = field(default_factory=list)
    consumed_by: list[str] = field(default_factory=list)


# §8.36
@dataclass
class ParsedPackage:
    metadata: PackageMetadata
    format_version: int
    deployment_model: str = "Package"
    connections: list[ConnectionManager] = field(default_factory=list)
    connection_map: dict[str, ConnectionManager] = field(default_factory=dict)
    variables: list[Variable] = field(default_factory=list)
    parameters: list[PackageParameter] = field(default_factory=list)
    configurations: list[Configuration] = field(default_factory=list)
    log_providers: list[LogProvider] = field(default_factory=list)
    executables: list[Executable] = field(default_factory=list)
    root_precedence_constraints: list[PrecedenceConstraint] = field(default_factory=list)
    root_event_handlers: list[EventHandler] = field(default_factory=list)
    variable_references: list[VariableReference] = field(default_factory=list)
