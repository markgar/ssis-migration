"""SSIS component knowledge base.

Provides structured descriptions, behavioral notes, migration guidance, and
risk/gotcha information for every known SSIS task type, data-flow component,
and connection manager type.

Each entry is a plain dict with consistent keys so the MCP tool can return
it directly as formatted text.  The lookup function does fuzzy matching on
component names so agents don't need the exact canonical string.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import get_close_matches


@dataclass(frozen=True)
class ComponentKnowledge:
    """Knowledge entry for a single SSIS component or task type."""

    name: str
    category: str  # "Control Flow Task", "Data Flow Component", "Connection Manager"
    description: str
    behavior: str
    migration: dict[str, str] = field(default_factory=dict)
    risks: list[str] = field(default_factory=list)
    see_also: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Control Flow tasks
# ---------------------------------------------------------------------------

_CONTROL_FLOW: list[ComponentKnowledge] = [
    ComponentKnowledge(
        name="Execute SQL Task",
        category="Control Flow Task",
        description=(
            "Runs a SQL statement or stored procedure against a relational "
            "database connection.  Can return single-row results, full result "
            "sets, row counts, or XML.  Supports parameter binding and result-"
            "set mapping to variables."
        ),
        behavior=(
            "Opens a connection, executes one SQL batch (or parameterised "
            "statement), optionally maps output columns to SSIS variables, "
            "and closes.  Runs synchronously inside the control flow."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Lookup** activity (for queries that return data) or a "
                "**Stored Procedure** activity.  For arbitrary SQL, use a "
                "**Script** activity or an **Azure Function** activity."
            ),
            "SQL / Stored Procedure": (
                "The SQL statement can usually be executed directly.  Parameter "
                "bindings become stored-procedure parameters or script variables."
            ),
            "Python": (
                "Use `pyodbc`, `sqlalchemy`, or the appropriate DB driver to "
                "execute the SQL.  Map result sets to pandas DataFrames or "
                "plain dicts."
            ),
        },
        risks=[
            "Parameter binding syntax differs between OLE DB (? positional) "
            "and ADO.NET (@named) — verify the connection type.",
            "Return full result sets into Object variables — these become "
            "ADO RecordSets that need special iteration in ForEach loops.",
            "BypassPrepare property can affect plan caching on SQL Server.",
        ],
        see_also=["OLE DB Connection", "ADO.NET Connection"],
    ),
    ComponentKnowledge(
        name="Data Flow Task",
        category="Control Flow Task",
        description=(
            "A container that hosts an in-memory data pipeline (the "
            "\"pipeline\" or \"data flow\").  Sources extract rows, "
            "transforms reshape them, and destinations load them — all "
            "streaming in parallel."
        ),
        behavior=(
            "The SSIS engine creates an in-memory buffer pipeline.  Rows "
            "stream from sources through transforms to destinations in "
            "configurable buffer sizes.  Blocking transforms (Sort, "
            "Aggregate) must consume all input before producing output.  "
            "Semi-blocking transforms (Merge Join) require sorted inputs."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Mapping Data Flow** for complex transformations, or "
                "a **Copy Activity** for simple source-to-destination moves.  "
                "Mapping Data Flows have visual equivalents for most SSIS "
                "transforms."
            ),
            "SQL / Stored Procedure": (
                "Many data flows can be replaced by INSERT…SELECT, MERGE, or "
                "CTEs when source and destination are on the same server.  "
                "Cross-server flows need linked servers or staging tables."
            ),
            "Python": (
                "Use pandas (read_sql → transform → to_sql) for moderate "
                "volumes.  For large-scale, use PySpark or Polars for "
                "streaming transformations."
            ),
        },
        risks=[
            "Blocking transforms (Sort, Aggregate) buffer all rows in memory "
            "and can cause OOM at scale.",
            "Buffer sizes and MaxBufferRows affect throughput — defaults may "
            "not be optimal.",
            "Error output rows may be silently discarded if not connected.",
        ],
    ),
    ComponentKnowledge(
        name="ForEach Loop Container",
        category="Control Flow Task",
        description=(
            "Iterates over a collection (files in a folder, rows in a "
            "result set, items in a list, ADO enumerator, SMO objects, etc.) "
            "and executes its child tasks once per item.  Each iteration "
            "maps the current item to a variable."
        ),
        behavior=(
            "Loops sequentially by default.  Each iteration assigns the "
            "current value to one or more mapped variables, then runs all "
            "child tasks in their precedence-constraint order.  Loop "
            "variables are scoped to the container."
        ),
        migration={
            "ADF / Fabric": (
                "Use the **ForEach** activity.  ADF supports file, list, and "
                "lookup-output enumerators natively.  For ADO RecordSet "
                "iteration, use a Lookup activity output piped to ForEach."
            ),
            "SQL / Stored Procedure": (
                "Use a CURSOR or WHILE loop with a temp table.  For file "
                "iteration, use xp_cmdshell or PowerShell."
            ),
            "Python": (
                "Standard `for` loop.  `glob.glob()` or `pathlib.Path.glob()` "
                "for file enumeration; iterate DataFrames with `.iterrows()` "
                "or batch SQL with parameters."
            ),
        },
        risks=[
            "Sequential iteration can be very slow for large collections — "
            "consider parallel processing on the target platform.",
            "Variable mapping errors are common when the enumerator returns "
            "multi-column result sets.",
            "File enumerator path expressions may use UNC paths that won't "
            "exist on cloud platforms.",
        ],
        see_also=["For Loop Container", "Sequence Container"],
    ),
    ComponentKnowledge(
        name="For Loop Container",
        category="Control Flow Task",
        description=(
            "Executes child tasks repeatedly while a Boolean expression "
            "evaluates to True.  Has init-expression, eval-expression, and "
            "assign-expression properties (like a C-style for loop)."
        ),
        behavior=(
            "Evaluates the init expression once, then loops: check eval "
            "expression → if true, run children → execute assign expression "
            "→ repeat.  Children run in precedence-constraint order each "
            "iteration."
        ),
        migration={
            "ADF / Fabric": (
                "Use an **Until** activity (loops until a condition becomes "
                "true — invert the condition).  Set variables with **Set "
                "Variable** activity inside the loop."
            ),
            "SQL / Stored Procedure": "WHILE loop with local variables.",
            "Python": "`while` loop or `for i in range(...)` with variables.",
        },
        risks=[
            "Infinite loops if the eval expression never becomes False.",
            "No built-in parallel iteration.",
        ],
        see_also=["ForEach Loop Container"],
    ),
    ComponentKnowledge(
        name="Sequence Container",
        category="Control Flow Task",
        description=(
            "Groups tasks into a logical unit for organizational purposes, "
            "shared transactions, or scoped variable/event-handler isolation."
        ),
        behavior=(
            "Executes child tasks according to their precedence constraints.  "
            "If a transaction is set, all children share it.  Variables "
            "defined at the container scope are visible only to children."
        ),
        migration={
            "ADF / Fabric": (
                "There is no direct equivalent.  Group activities visually or "
                "use a nested pipeline (Execute Pipeline activity) for "
                "isolation."
            ),
            "SQL / Stored Procedure": (
                "A BEGIN…END block or explicit transaction scope (BEGIN TRAN "
                "/ COMMIT)."
            ),
            "Python": "A function or context manager for scoping.",
        },
        risks=[
            "Transaction isolation levels and timeout settings may silently "
            "differ from the default when migrated.",
        ],
    ),
    ComponentKnowledge(
        name="Execute Package Task",
        category="Control Flow Task",
        description=(
            "Runs another SSIS package (child package) from within the "
            "current package.  Can pass variable values to the child and "
            "receive them back."
        ),
        behavior=(
            "Loads and executes a child .dtsx package, optionally passing "
            "parent variables to child parameters/configurations.  Runs "
            "synchronously.  The child can be on the file system, in MSDB, "
            "or in the SSIS Catalog."
        ),
        migration={
            "ADF / Fabric": (
                "Use the **Execute Pipeline** activity to call a child "
                "pipeline, passing parameters explicitly."
            ),
            "SQL / Stored Procedure": (
                "Use EXEC to call a sub-procedure, passing parameters."
            ),
            "Python": "Call a function or import and run a sub-module.",
        },
        risks=[
            "Parent-to-child variable passing relies on configurations or "
            "project parameters — verify the binding method.",
            "Relative file paths for child packages may break on migration.",
            "In SSIS Catalog, project-reference vs. external-reference mode "
            "matters for deployment.",
        ],
    ),
    ComponentKnowledge(
        name="Execute Process Task",
        category="Control Flow Task",
        description=(
            "Launches an external executable (EXE, BAT, CMD, PowerShell "
            "script, etc.) and optionally captures stdout/stderr into "
            "variables."
        ),
        behavior=(
            "Spawns a child process with the specified command line, "
            "optionally setting working directory and environment.  Waits "
            "for completion (or times out).  Exit code can be captured."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Custom Activity** (Azure Batch), **Azure Function**, "
                "or **Web Activity** to execute external code.  No direct "
                "\"run EXE\" equivalent."
            ),
            "SQL / Stored Procedure": (
                "xp_cmdshell (if enabled) or sp_execute_external_script for "
                "R/Python.  Agent Jobs for scheduled executables."
            ),
            "Python": "`subprocess.run()` or `subprocess.Popen()`.",
        },
        risks=[
            "External executables may not exist on the target platform.",
            "UNC/local file paths and environment variables will differ.",
            "Security: xp_cmdshell is disabled by default on SQL Server.",
        ],
    ),
    ComponentKnowledge(
        name="Script Task",
        category="Control Flow Task",
        description=(
            "Executes custom .NET code (C# or VB.NET) inline within the "
            "control flow.  Has access to SSIS variables, connections, and "
            "events via the Dts object model."
        ),
        behavior=(
            "Compiles and runs a .NET class at runtime.  The entry point "
            "method receives the SSIS runtime context.  Can read/write "
            "variables, fire events, and interact with any .NET library."
        ),
        migration={
            "ADF / Fabric": (
                "Use an **Azure Function** activity, **Custom Activity** "
                "(Azure Batch), or **Notebook** activity.  Rewrite the C# "
                "logic in the target language."
            ),
            "SQL / Stored Procedure": (
                "If the logic is data-oriented, SQL CLR or T-SQL may work.  "
                "Complex .NET logic needs an external service."
            ),
            "Python": (
                "Rewrite in Python.  Most System.IO, System.Net, and "
                "string-manipulation logic translates directly.  .NET-specific "
                "libraries (e.g., COM interop) need alternatives."
            ),
        },
        risks=[
            "Hardest component to migrate automatically — requires manual "
            "code translation.",
            "May reference .NET assemblies not available on target platform.",
            "Variable access via Dts.Variables must be mapped to the new "
            "platform's variable/parameter system.",
            "Binary-serialised script projects embedded in the .dtsx are "
            "opaque to static analysis.",
        ],
    ),
    ComponentKnowledge(
        name="Send Mail Task",
        category="Control Flow Task",
        description=(
            "Sends an email via SMTP.  Supports To/CC/BCC, subject, body "
            "(plain or HTML), file attachments, and priority settings."
        ),
        behavior=(
            "Connects to an SMTP server (using an SMTP Connection Manager) "
            "and sends one email.  Body and addresses can be expressions."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Web Activity** to call an email API (SendGrid, "
                "Graph API), or a **Logic App** with an email connector."
            ),
            "SQL / Stored Procedure": "sp_send_dbmail (Database Mail).",
            "Python": "`smtplib.SMTP` or a service SDK (SendGrid, boto3 SES).",
        },
        risks=[
            "SMTP server may not be reachable from cloud environments.",
            "Attachment file paths are local/UNC.",
        ],
    ),
    ComponentKnowledge(
        name="File System Task",
        category="Control Flow Task",
        description=(
            "Performs file and directory operations: copy, move, delete, "
            "rename files or directories.  Source and destination can use "
            "File Connection Managers or variable expressions."
        ),
        behavior=(
            "Executes a single file-system operation synchronously.  The "
            "operation type (copy, move, delete, create directory, etc.) is "
            "set by the Operation property."
        ),
        migration={
            "ADF / Fabric": (
                "Use the **Copy Activity** (for file copy/move between "
                "supported stores) or a **Delete** activity.  For local "
                "file operations, use a **Self-Hosted Integration Runtime** "
                "with a Custom Activity."
            ),
            "SQL / Stored Procedure": (
                "xp_cmdshell or PowerShell via Agent Job.  No native T-SQL "
                "file operations."
            ),
            "Python": (
                "`shutil.copy()`, `shutil.move()`, `os.remove()`, "
                "`os.makedirs()`, or `pathlib.Path` methods."
            ),
        },
        risks=[
            "Local/UNC paths won't exist in cloud environments.",
            "Permissions and service accounts differ between on-prem and "
            "cloud.",
        ],
    ),
    ComponentKnowledge(
        name="FTP Task",
        category="Control Flow Task",
        description=(
            "Sends or receives files via FTP.  Supports upload, download, "
            "delete remote files, create/remove remote directories."
        ),
        behavior=(
            "Connects to an FTP server (via FTP Connection Manager), "
            "performs the specified operation (send/receive/delete), and "
            "disconnects.  Passive/active mode is configurable."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Copy Activity** with an FTP or SFTP linked service."
            ),
            "SQL / Stored Procedure": (
                "xp_cmdshell with a command-line FTP client, or CLR stored "
                "procedure."
            ),
            "Python": "`ftplib.FTP` or `paramiko` (for SFTP).",
        },
        risks=[
            "Plain FTP is unencrypted — migrate to SFTP or FTPS.",
            "Firewall rules differ in cloud environments.",
        ],
    ),
    ComponentKnowledge(
        name="Bulk Insert Task",
        category="Control Flow Task",
        description=(
            "Performs a high-speed bulk copy from a flat file directly into "
            "a SQL Server table using the T-SQL BULK INSERT statement."
        ),
        behavior=(
            "Issues a BULK INSERT command on the target SQL Server.  The "
            "flat file must be accessible to the SQL Server service account "
            "(not the SSIS machine).  Minimal logging is possible."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Copy Activity** with a Flat File source and SQL "
                "Server sink.  Set the write method to Bulk Insert or "
                "PolyBase for large volumes."
            ),
            "SQL / Stored Procedure": (
                "BULK INSERT or OPENROWSET(BULK...) directly in T-SQL."
            ),
            "Python": (
                "`pandas.read_csv()` → `to_sql(method='multi')` or use "
                "the `bcp` utility via subprocess."
            ),
        },
        risks=[
            "File must be reachable by SQL Server, not by the ETL agent.",
            "Format files (.fmt) may be required and must also migrate.",
        ],
    ),
    ComponentKnowledge(
        name="Expression Task",
        category="Control Flow Task",
        description=(
            "Evaluates a single SSIS expression and assigns the result to "
            "a variable.  Useful for string building, date arithmetic, or "
            "conditional variable assignment in the control flow."
        ),
        behavior=(
            "Evaluates one expression at runtime and stores the result in "
            "the target variable.  Runs synchronously, very lightweight."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Set Variable** activity with an ADF expression."
            ),
            "SQL / Stored Procedure": "SET @variable = <expression>;",
            "Python": "Simple variable assignment.",
        },
        risks=[
            "SSIS expression syntax differs from ADF expression syntax — "
            "functions like GETDATE(), REPLACE, SUBSTRING have different "
            "signatures.",
        ],
    ),
    ComponentKnowledge(
        name="Data Profiling Task",
        category="Control Flow Task",
        description=(
            "Computes data-quality profiles (column statistics, pattern "
            "distributions, null ratios, value distributions, functional "
            "dependencies) against a SQL Server data source and writes "
            "results to XML."
        ),
        behavior=(
            "Runs predefined profile queries against the source, aggregates "
            "results, and writes an XML profile output file."
        ),
        migration={
            "ADF / Fabric": (
                "No direct equivalent.  Use **Mapping Data Flow** with "
                "aggregate transforms, or external tools like Great "
                "Expectations or Azure Purview for data profiling."
            ),
            "SQL / Stored Procedure": (
                "Custom queries: COUNT, COUNT(DISTINCT), MIN, MAX, "
                "percentiles via PERCENTILE_CONT."
            ),
            "Python": (
                "`pandas.DataFrame.describe()`, `pandas-profiling` / "
                "`ydata-profiling`, or Great Expectations."
            ),
        },
        risks=["Output XML format is SSIS-specific — not portable."],
    ),
    ComponentKnowledge(
        name="XML Task",
        category="Control Flow Task",
        description=(
            "Performs XML operations: validation against XSD, XSLT "
            "transformations, XPath queries, merge, diff, and patch."
        ),
        behavior=(
            "Reads XML from a file or variable, applies the configured "
            "operation, and writes results to a file or variable."
        ),
        migration={
            "ADF / Fabric": (
                "No direct equivalent.  Use an Azure Function or Databricks "
                "notebook for XML processing."
            ),
            "SQL / Stored Procedure": (
                "SQL Server's XML data type methods: .query(), .value(), "
                ".modify(), plus OPENXML."
            ),
            "Python": "`lxml`, `xml.etree.ElementTree`, or `xmlschema`.",
        },
        risks=[
            "XSLT stylesheets and XSD schemas must also be migrated.",
            "XML operations on large documents can be memory-intensive.",
        ],
    ),
    ComponentKnowledge(
        name="Web Service Task",
        category="Control Flow Task",
        description=(
            "Calls a SOAP web-service method and stores the response in a "
            "variable or file.  Uses an HTTP Connection Manager and WSDL."
        ),
        behavior=(
            "Constructs a SOAP request from the WSDL definition, sends it "
            "over HTTP/HTTPS, and captures the XML response."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Web Activity** for REST/SOAP calls, or a "
                "**Logic App** for complex SOAP interactions."
            ),
            "SQL / Stored Procedure": (
                "sp_OACreate (COM automation) or CLR stored procedure with "
                "HttpClient.  Generally not recommended in T-SQL."
            ),
            "Python": "`requests` (REST) or `zeep` (SOAP client).",
        },
        risks=[
            "SOAP/WSDL-based — evaluate whether the service has a modern "
            "REST API.",
            "Authentication (Windows Auth, certificates) may not transfer.",
        ],
    ),
    ComponentKnowledge(
        name="Transfer Database Task",
        category="Control Flow Task",
        description=(
            "Transfers (copies or moves) an entire SQL Server database "
            "between server instances using detach/attach or SMO transfer."
        ),
        behavior=(
            "Connects to source and destination SQL Server instances.  "
            "Either detaches the database files and reattaches them on the "
            "destination, or uses SMO to script and recreate objects."
        ),
        migration={
            "ADF / Fabric": (
                "Use SQL Server backup/restore, Azure Database Migration "
                "Service, or dacpac deployment.  No single ADF activity."
            ),
            "SQL / Stored Procedure": (
                "BACKUP DATABASE + RESTORE DATABASE, or sp_detach_db / "
                "sp_attach_db."
            ),
            "Python": "Invoke sqlcmd or dbatools via subprocess.",
        },
        risks=[
            "Requires sysadmin on both servers.",
            "Detach mode causes source downtime.",
            "Cloud databases (Azure SQL) don't support detach/attach.",
        ],
        see_also=["Transfer Jobs Task", "Transfer Logins Task"],
    ),
    ComponentKnowledge(
        name="Transfer Jobs Task",
        category="Control Flow Task",
        description=(
            "Copies SQL Server Agent jobs between server instances."
        ),
        behavior=(
            "Uses SMO to read Agent job definitions from the source and "
            "recreate them on the destination.  Can overwrite or skip "
            "existing jobs."
        ),
        migration={
            "ADF / Fabric": (
                "No equivalent — Agent jobs don't exist in cloud PaaS.  "
                "Recreate as ADF triggers/pipelines, Logic Apps, or "
                "Azure Automation runbooks."
            ),
            "SQL / Stored Procedure": "sp_add_job / sp_add_jobstep scripting.",
            "Python": "Script via SMO or dbatools via subprocess.",
        },
        risks=[
            "Agent job schedules and notifications need manual recreation.",
            "Job steps may reference local paths or linked servers.",
        ],
    ),
    ComponentKnowledge(
        name="Transfer Logins Task",
        category="Control Flow Task",
        description=(
            "Copies SQL Server logins (with SIDs and passwords) between "
            "server instances."
        ),
        behavior=(
            "Reads login definitions from the source server and recreates "
            "them on the destination.  Can preserve SIDs for database-level "
            "user mapping."
        ),
        migration={
            "ADF / Fabric": (
                "No equivalent.  Use T-SQL scripts or dbatools "
                "(Copy-DbaLogin) for login migration."
            ),
            "SQL / Stored Procedure": (
                "sp_help_revlogin or CREATE LOGIN with SID and "
                "HASHED PASSWORD."
            ),
            "Python": "dbatools or custom SMO scripting.",
        },
        risks=[
            "Password hashes may not transfer to Azure SQL (AAD auth).",
            "Orphaned users if SIDs don't match.",
        ],
    ),
    ComponentKnowledge(
        name="Transfer Error Messages Task",
        category="Control Flow Task",
        description=(
            "Copies user-defined error messages (sp_addmessage entries) "
            "between SQL Server instances."
        ),
        behavior=(
            "Reads from sys.messages on the source and creates matching "
            "entries on the destination."
        ),
        migration={
            "ADF / Fabric": (
                "No equivalent.  Script the messages via sp_addmessage."
            ),
            "SQL / Stored Procedure": "sp_addmessage for each message.",
            "Python": "Execute sp_addmessage via pyodbc.",
        },
        risks=["Language-specific messages need matching language IDs."],
    ),
    ComponentKnowledge(
        name="Transfer Stored Procedures Task",
        category="Control Flow Task",
        description=(
            "Copies stored procedures from the master database between "
            "SQL Server instances."
        ),
        behavior=(
            "Scripts stored procedures from master on the source and "
            "executes the scripts on the destination."
        ),
        migration={
            "ADF / Fabric": (
                "No equivalent.  Use dacpac, sqlpackage, or manual scripting."
            ),
            "SQL / Stored Procedure": "Script with sys.sql_modules + EXEC.",
            "Python": "Extract via SMO or sys.sql_modules, execute on target.",
        },
        risks=[
            "Only transfers from master database.",
            "Dependencies on other objects must be resolved manually.",
        ],
    ),
    ComponentKnowledge(
        name="Transfer SQL Server Objects Task",
        category="Control Flow Task",
        description=(
            "Copies database objects (tables, views, stored procedures, "
            "functions, etc.) between SQL Server instances using SMO.  The "
            "most flexible of the Transfer tasks."
        ),
        behavior=(
            "Connects via SMO, scripts selected object types from the "
            "source database, and executes the scripts on the destination."
        ),
        migration={
            "ADF / Fabric": (
                "Use dacpac/sqlpackage for schema, and Copy Activity or "
                "pipelines for data.  No single equivalent."
            ),
            "SQL / Stored Procedure": (
                "Generate scripts via sys.sql_modules, INFORMATION_SCHEMA, "
                "or sqlpackage."
            ),
            "Python": "dbatools or sqlpackage via subprocess.",
        },
        risks=[
            "Object dependencies and ordering matter.",
            "Includes data copy option (CopyData) — can be very slow.",
            "Security and permissions are not always transferred.",
        ],
    ),
    ComponentKnowledge(
        name="Analysis Services Processing Task",
        category="Control Flow Task",
        description=(
            "Processes (refreshes) SQL Server Analysis Services objects — "
            "cubes, dimensions, measure groups, or mining models."
        ),
        behavior=(
            "Connects to an SSAS instance and issues XMLA ProcessFull/"
            "ProcessIncremental/etc. commands against specified objects."
        ),
        migration={
            "ADF / Fabric": (
                "For Azure Analysis Services, use a **Web Activity** calling "
                "the SSAS REST API, or a **Custom Activity** running "
                "Invoke-ASCmd.  For Power BI / Fabric, use the dataset "
                "refresh API."
            ),
            "SQL / Stored Procedure": (
                "XMLA commands via linked server or CLR, or PowerShell "
                "Invoke-ASCmd."
            ),
            "Python": (
                "`pyadomd` or the Azure Analysis Services REST API "
                "via `requests`."
            ),
        },
        risks=[
            "XMLA commands are SSAS-version-specific.",
            "Azure AS and Power BI have different refresh APIs than on-prem "
            "SSAS.",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Data Flow components
# ---------------------------------------------------------------------------

_DATA_FLOW: list[ComponentKnowledge] = [
    ComponentKnowledge(
        name="OLE DB Source",
        category="Data Flow Component",
        description=(
            "Reads data from a relational database via an OLE DB connection.  "
            "Can read from a table, view, or SQL query."
        ),
        behavior=(
            "Opens a connection, issues a SELECT (or reads the table/view "
            "directly), and streams rows into the data flow pipeline buffer.  "
            "Access mode can be table, SQL command, or SQL from variable."
        ),
        migration={
            "ADF / Fabric": (
                "**Copy Activity** source or **Mapping Data Flow** source "
                "with an OLE DB, SQL Server, or Azure SQL linked service."
            ),
            "SQL / Stored Procedure": "SELECT statement directly.",
            "Python": (
                "`pandas.read_sql()` with `pyodbc` or `sqlalchemy` connection."
            ),
        },
        risks=[
            "SQL Command may use vendor-specific syntax (T-SQL, PL/SQL).",
            "Variable-based SQL commands are dynamic and may need review.",
        ],
        see_also=["OLE DB Destination", "OLE DB Command"],
    ),
    ComponentKnowledge(
        name="OLE DB Destination",
        category="Data Flow Component",
        description=(
            "Writes data to a relational database via OLE DB.  Supports "
            "table or view direct insert, fast-load (bulk) mode, and "
            "table/view variable modes."
        ),
        behavior=(
            "Receives rows from upstream transforms and INSERTs them into "
            "the target table.  Fast-load mode uses bulk copy for "
            "performance (configurable batch size, row locking, etc.)."
        ),
        migration={
            "ADF / Fabric": (
                "**Copy Activity** sink or **Mapping Data Flow** sink.  "
                "Bulk insert mode maps to Copy Activity's bulk write method."
            ),
            "SQL / Stored Procedure": "INSERT INTO ... SELECT ... or BULK INSERT.",
            "Python": "`DataFrame.to_sql(method='multi')` or bcp utility.",
        },
        risks=[
            "Fast-load settings (batch size, table lock, check constraints) "
            "affect performance and concurrency.",
            "Error output handling for failed rows needs migration.",
        ],
        see_also=["OLE DB Source"],
    ),
    ComponentKnowledge(
        name="Flat File Source",
        category="Data Flow Component",
        description=(
            "Reads rows from a delimited or fixed-width text file via a "
            "Flat File Connection Manager."
        ),
        behavior=(
            "Parses the file according to the Flat File Connection Manager's "
            "column definitions (delimiter, text qualifier, widths).  "
            "Streams rows into the pipeline."
        ),
        migration={
            "ADF / Fabric": (
                "**Copy Activity** or **Mapping Data Flow** with a Delimited "
                "Text / Fixed Width linked service."
            ),
            "SQL / Stored Procedure": "BULK INSERT or OPENROWSET(BULK...).",
            "Python": (
                "`pandas.read_csv()` or `pandas.read_fwf()` for fixed-width."
            ),
        },
        risks=[
            "Column definitions (widths, delimiters, code pages) must match "
            "exactly.",
            "Text qualifier and escape-character handling varies between "
            "platforms.",
        ],
        see_also=["Flat File Destination"],
    ),
    ComponentKnowledge(
        name="Flat File Destination",
        category="Data Flow Component",
        description=(
            "Writes rows to a delimited or fixed-width text file."
        ),
        behavior=(
            "Receives rows and writes them to a flat file according to the "
            "Flat File Connection Manager's format (delimiters, text "
            "qualifiers, header row)."
        ),
        migration={
            "ADF / Fabric": (
                "**Copy Activity** sink with Delimited Text format."
            ),
            "SQL / Stored Procedure": "bcp OUT or SQLCMD with -o flag.",
            "Python": "`DataFrame.to_csv()`.",
        },
        risks=[
            "Output file path may be UNC or local — not available in cloud.",
            "Encoding (UTF-8 vs ANSI vs Unicode) must be preserved.",
        ],
    ),
    ComponentKnowledge(
        name="Excel Source",
        category="Data Flow Component",
        description=(
            "Reads data from a Microsoft Excel workbook (.xls or .xlsx) "
            "via the Excel Connection Manager."
        ),
        behavior=(
            "Opens the workbook via the ACE/Jet OLE DB provider, reads a "
            "sheet or named range, and streams rows.  Data types are "
            "inferred from the first few rows."
        ),
        migration={
            "ADF / Fabric": (
                "**Copy Activity** with an Excel linked service, or "
                "**Mapping Data Flow** with an Excel source."
            ),
            "SQL / Stored Procedure": "OPENROWSET with ACE provider.",
            "Python": "`pandas.read_excel()` with `openpyxl` engine.",
        },
        risks=[
            "ACE/Jet provider is 32-bit only on many systems.",
            "Data type inference from Excel is unreliable — mixed-type "
            "columns may truncate.",
            "Named ranges vs sheet references behave differently.",
        ],
    ),
    ComponentKnowledge(
        name="Lookup",
        category="Data Flow Component",
        description=(
            "Enriches rows by joining them against a reference dataset "
            "(a table or query result) on one or more key columns.  Returns "
            "matched columns from the reference to the pipeline.  Unmatched "
            "rows can be redirected."
        ),
        behavior=(
            "In Full Cache mode (default), loads the entire reference "
            "dataset into memory at startup, then does hash lookups per row.  "
            "Partial Cache uses an LRU cache with database round-trips for "
            "misses.  No Cache queries the database per row.  Match/no-match "
            "outputs split the flow."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Lookup transformation (supports "
                "broadcast and database lookup modes)."
            ),
            "SQL / Stored Procedure": (
                "LEFT JOIN or INNER JOIN in a SELECT statement."
            ),
            "Python": (
                "`pandas.merge()` — left, inner, or outer join on key columns."
            ),
        },
        risks=[
            "Full Cache loads entire reference table into memory — large "
            "reference tables can cause OOM.",
            "No-match handling (redirect vs. ignore vs. fail) must be "
            "replicated.",
            "Cache type affects performance dramatically — Full Cache is "
            "fast but memory-hungry.",
        ],
        see_also=["Merge Join"],
    ),
    ComponentKnowledge(
        name="Derived Column",
        category="Data Flow Component",
        description=(
            "Adds new columns or replaces existing columns using SSIS "
            "expressions.  Each expression computes a value from existing "
            "columns, variables, and functions."
        ),
        behavior=(
            "Evaluates one or more SSIS expressions per row.  Expressions "
            "can use string functions, math, date functions, type casts, "
            "conditionals, and NULL handling.  New columns get specified "
            "data types."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Derived Column transformation "
                "(similar concept, different expression syntax)."
            ),
            "SQL / Stored Procedure": (
                "Computed columns in SELECT: "
                "`SELECT col1, col1 + col2 AS new_col ...`"
            ),
            "Python": (
                "`df['new_col'] = df['col1'] + df['col2']` or "
                "`df.assign()`."
            ),
        },
        risks=[
            "SSIS expression syntax is unique — (DT_WSTR, 50), ternary ?, "
            "REPLACENULL, etc. must be translated.",
            "Data type promotion rules differ between SSIS and SQL/Python.",
        ],
    ),
    ComponentKnowledge(
        name="Conditional Split",
        category="Data Flow Component",
        description=(
            "Routes rows to different outputs based on Boolean SSIS "
            "expressions.  Like a CASE statement — each output has a "
            "condition; rows go to the first matching output, or the "
            "default output."
        ),
        behavior=(
            "Evaluates conditions top-to-bottom for each row.  The row "
            "goes to the first output whose condition is true.  If no "
            "condition matches, rows go to the default output.  Each "
            "output can connect to a different downstream path."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Conditional Split transformation "
                "(same concept)."
            ),
            "SQL / Stored Procedure": (
                "CASE expression in SELECT, or separate INSERT statements "
                "with WHERE clauses."
            ),
            "Python": (
                "`df[df['col'] > 0]` filtering, or `np.where()` / "
                "`np.select()` for multi-condition."
            ),
        },
        risks=[
            "Condition evaluation order matters — first match wins.",
            "Default output must be handled (unmatched rows).",
        ],
    ),
    ComponentKnowledge(
        name="Merge Join",
        category="Data Flow Component",
        description=(
            "Joins two sorted input streams on key columns, producing "
            "Inner, Left Outer, or Full Outer join output.  Similar to an "
            "SQL JOIN but operates on streaming data."
        ),
        behavior=(
            "**Both inputs MUST be sorted** on the join key columns before "
            "reaching the Merge Join.  Performs a sort-merge algorithm: "
            "walks both sorted streams in parallel, matching on keys.  "
            "This is a semi-blocking transform — it buffers one side while "
            "scanning the other."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Join transformation (handles "
                "sorting automatically)."
            ),
            "SQL / Stored Procedure": (
                "INNER JOIN / LEFT JOIN / FULL OUTER JOIN in SQL — the "
                "optimizer handles sort order."
            ),
            "Python": (
                "`pandas.merge(left, right, on='key', how='inner|left|outer')`."
            ),
        },
        risks=[
            "Inputs MUST be pre-sorted on the join keys — forgetting the "
            "Sort transform upstream causes runtime failure.",
            "Sort transform is fully blocking (buffers all rows in memory).",
            "Join key data types must match exactly between the two inputs.",
        ],
        see_also=["Merge", "Sort", "Lookup"],
    ),
    ComponentKnowledge(
        name="Merge",
        category="Data Flow Component",
        description=(
            "Combines two sorted input streams into a single sorted "
            "output.  Similar to UNION ALL but preserves sort order."
        ),
        behavior=(
            "Both inputs must be sorted on the same key.  Interleaves rows "
            "maintaining sort order.  Unlike Union All, only accepts "
            "exactly two inputs."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Union transformation (handles "
                "sorting internally if needed)."
            ),
            "SQL / Stored Procedure": (
                "UNION ALL with ORDER BY, or merge logic in a CTE."
            ),
            "Python": (
                "`pandas.concat([df1, df2]).sort_values('key')` or use "
                "`heapq.merge()` for streaming."
            ),
        },
        risks=[
            "Both inputs must be pre-sorted — same gotcha as Merge Join.",
            "Accepts exactly 2 inputs (use Union All for more).",
        ],
        see_also=["Merge Join", "Union All"],
    ),
    ComponentKnowledge(
        name="Union All",
        category="Data Flow Component",
        description=(
            "Combines multiple input streams into a single output.  "
            "Appends all rows from all inputs (no deduplication)."
        ),
        behavior=(
            "Accepts 2+ inputs.  Simply concatenates all rows from all "
            "inputs.  Column mapping determines which columns align.  "
            "Does NOT require sorted inputs.  Non-blocking."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Union transformation."
            ),
            "SQL / Stored Procedure": "UNION ALL.",
            "Python": "`pandas.concat([df1, df2, ...], ignore_index=True)`.",
        },
        risks=[
            "Column mapping can silently discard columns if not mapped.",
            "No deduplication — use Sort + remove duplicates if needed.",
        ],
        see_also=["Merge"],
    ),
    ComponentKnowledge(
        name="Multicast",
        category="Data Flow Component",
        description=(
            "Sends every input row to every output.  Creates identical "
            "copies of the data stream for branching the pipeline."
        ),
        behavior=(
            "Non-blocking pass-through.  Each output gets a reference to "
            "the same buffer (zero-copy in the engine).  Used when the "
            "same data needs to go to multiple destinations or transforms."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** has no explicit Multicast — instead, "
                "connect a single source to multiple downstream transforms "
                "directly (implicit fan-out)."
            ),
            "SQL / Stored Procedure": (
                "SELECT the same data multiple times, or INSERT into a "
                "staging table and read from it multiple times."
            ),
            "Python": "Assign the DataFrame to an alias: `df2 = df.copy()`.",
        },
        risks=[
            "In SSIS the copies share memory buffers; on other platforms "
            "this may mean reading the source multiple times.",
        ],
    ),
    ComponentKnowledge(
        name="Aggregate",
        category="Data Flow Component",
        description=(
            "Computes aggregate functions (SUM, AVG, COUNT, COUNT DISTINCT, "
            "MIN, MAX, GROUP BY) over the data stream.  Similar to SQL "
            "GROUP BY."
        ),
        behavior=(
            "**Fully blocking** — must consume all input rows before "
            "producing any output.  Loads all rows into memory, groups "
            "them, computes the aggregates, and then emits one row per "
            "group."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Aggregate transformation."
            ),
            "SQL / Stored Procedure": (
                "SELECT ... GROUP BY ... with SUM, AVG, COUNT, MIN, MAX."
            ),
            "Python": "`df.groupby('col').agg({'val': 'sum'})`.",
        },
        risks=[
            "Fully blocking — buffers all rows in memory.  Very large "
            "datasets can cause OOM.",
            "COUNT DISTINCT uses hash tables — memory scales with "
            "cardinality.",
        ],
        see_also=["Sort"],
    ),
    ComponentKnowledge(
        name="Sort",
        category="Data Flow Component",
        description=(
            "Sorts the data stream by one or more columns.  Can optionally "
            "remove duplicate rows."
        ),
        behavior=(
            "**Fully blocking** — buffers all incoming rows in memory, "
            "performs an in-memory sort, then outputs all rows in sorted "
            "order.  Remove-duplicates mode performs a distinct after sorting."
        ),
        migration={
            "ADF / Fabric": (
                "**Mapping Data Flow** Sort transformation.  For large data, "
                "consider sorting in the SQL source query instead."
            ),
            "SQL / Stored Procedure": "ORDER BY clause in the SELECT.",
            "Python": "`df.sort_values('col')` or `.drop_duplicates()`.",
        },
        risks=[
            "FULLY BLOCKING — the single most dangerous transform for "
            "memory consumption.  Avoid if possible by sorting in the "
            "source query (ORDER BY) instead.",
            "Remove-duplicates adds additional memory overhead.",
            "Often used only to satisfy Merge Join's sorted-input "
            "requirement — consider replacing the entire Sort + Merge Join "
            "pattern with a Lookup or SQL JOIN.",
        ],
        see_also=["Merge Join", "Aggregate"],
    ),
    ComponentKnowledge(
        name="Data Conversion",
        category="Data Flow Component",
        description=(
            "Converts columns from one SSIS data type to another.  "
            "Creates new output columns with the target data types."
        ),
        behavior=(
            "Non-blocking row-by-row conversion.  Each configured column "
            "gets a new alias in the output with the target data type.  "
            "Original columns remain available.  Conversion errors can be "
            "redirected."
        ),
        migration={
            "ADF / Fabric": (
                "In Mapping Data Flows, use type conversions in **Derived "
                "Column** expressions: `toString()`, `toInteger()`, etc."
            ),
            "SQL / Stored Procedure": "CAST() or CONVERT() in SQL.",
            "Python": (
                "`df['col'].astype(int)` or `pd.to_datetime()`, "
                "`pd.to_numeric()`."
            ),
        },
        risks=[
            "Truncation warnings/errors for string-to-shorter-string, "
            "numeric overflow, or date parsing failures.",
            "Code page conversions (ANSI ↔ Unicode) may lose characters.",
        ],
    ),
    ComponentKnowledge(
        name="Row Count",
        category="Data Flow Component",
        description=(
            "Counts the number of rows passing through the pipeline and "
            "stores the count in a variable."
        ),
        behavior=(
            "Non-blocking pass-through.  Increments an integer counter for "
            "each row and writes the total to the mapped variable.  Does "
            "not modify the data stream."
        ),
        migration={
            "ADF / Fabric": (
                "In ADF, pipeline activity output provides row counts "
                "automatically (e.g., `@activity('Copy1').output.rowsCopied`)."
            ),
            "SQL / Stored Procedure": "@@ROWCOUNT or COUNT(*).",
            "Python": "`len(df)` or `df.shape[0]`.",
        },
        risks=["Very simple — low migration risk."],
    ),
    ComponentKnowledge(
        name="OLE DB Command",
        category="Data Flow Component",
        description=(
            "Executes a parameterised SQL command (INSERT, UPDATE, DELETE, "
            "or stored procedure call) once per row in the data flow."
        ),
        behavior=(
            "For each input row, binds column values to parameter markers "
            "in the SQL command and executes it.  Runs row-by-row — "
            "extremely slow for high-volume data."
        ),
        migration={
            "ADF / Fabric": (
                "In Mapping Data Flows, use a **Sink** with ALTER ROW "
                "policies for upsert/delete, or a Stored Procedure sink.  "
                "Avoid row-by-row patterns."
            ),
            "SQL / Stored Procedure": (
                "Rewrite as a set-based UPDATE/DELETE/MERGE statement "
                "joining a staging table."
            ),
            "Python": (
                "`cursor.executemany()` for bulk operations, or better yet, "
                "use a set-based approach with a temp table."
            ),
        },
        risks=[
            "**Performance killer** — executes once PER ROW.  Must be "
            "converted to set-based logic for any serious data volume.",
            "Implicit transactions may cause lock escalation.",
        ],
        see_also=["OLE DB Destination"],
    ),
    ComponentKnowledge(
        name="Script Component",
        category="Data Flow Component",
        description=(
            "Custom .NET code (C# or VB.NET) running inside the data flow "
            "pipeline as a source, transformation, or destination.  Has "
            "access to input/output columns, variables, and connections."
        ),
        behavior=(
            "Runs compiled .NET code per row (transform/destination mode) "
            "or to produce rows (source mode).  The code has typed "
            "accessors for input/output columns defined in the component's "
            "metadata.  Can call external APIs, parse complex formats, or "
            "implement custom logic."
        ),
        migration={
            "ADF / Fabric": (
                "Rewrite in a **Databricks Notebook**, **Azure Function**, "
                "or **Mapping Data Flow** expressions if the logic is "
                "simple.  Complex scripts may need a Spark UDF."
            ),
            "SQL / Stored Procedure": (
                "CLR stored procedure for .NET logic, or rewrite in T-SQL "
                "if feasible."
            ),
            "Python": (
                "Rewrite the logic in Python.  `.ProcessInputRow()` "
                "becomes a `df.apply()` or vectorised operation."
            ),
        },
        risks=[
            "Hardest data-flow component to migrate — requires manual "
            "code translation.",
            "May reference .NET assemblies or COM objects not available on "
            "target.",
            "Binary-serialised project code embedded in .dtsx is opaque.",
        ],
        see_also=["Script Task"],
    ),
    ComponentKnowledge(
        name="Slowly Changing Dimension",
        category="Data Flow Component",
        description=(
            "Implements the Slowly Changing Dimension (SCD) pattern for "
            "data warehousing.  Classifies columns as Business Key, "
            "Fixed Attribute, Changing Attribute, or Historical Attribute "
            "and routes rows to appropriate update/insert/history paths."
        ),
        behavior=(
            "Compares incoming rows against a dimension table using the "
            "business key.  New rows go to the New output.  Changed rows "
            "are routed based on column SCD type: Changing Attributes get "
            "updated, Historical Attributes trigger new dimension records "
            "(Type 2), Fixed Attributes optionally fail."
        ),
        migration={
            "ADF / Fabric": (
                "Use **Mapping Data Flow** with ALTER ROW policies for "
                "upsert, or a custom SCD pattern with Lookup + Conditional "
                "Split + Derived Column.  No built-in SCD transform in ADF."
            ),
            "SQL / Stored Procedure": (
                "MERGE statement with WHEN MATCHED / WHEN NOT MATCHED, "
                "combined with history-row insertion for Type 2."
            ),
            "Python": (
                "Custom merge logic with pandas, or use a framework like "
                "dbt snapshots for SCD Type 2."
            ),
        },
        risks=[
            "Complex logic — the SCD wizard generates multiple downstream "
            "components (OLE DB Command for updates, OLE DB Destination "
            "for inserts).  All must be migrated together.",
            "Row-by-row OLE DB Command for updates is a performance "
            "bottleneck.",
            "Type 2 history requires careful handling of effective dates.",
        ],
    ),
    ComponentKnowledge(
        name="Balanced Data Distributor",
        category="Data Flow Component",
        description=(
            "Distributes rows evenly across multiple outputs for parallel "
            "processing.  Used to scale out CPU-bound transforms by "
            "running multiple parallel paths."
        ),
        behavior=(
            "Round-robins input rows across its outputs.  Each output can "
            "feed an independent processing path.  Typically reunited with "
            "a Union All downstream."
        ),
        migration={
            "ADF / Fabric": (
                "Mapping Data Flows handle parallelism automatically via "
                "Spark partitioning — no explicit distributor needed."
            ),
            "SQL / Stored Procedure": "Partitioned views or parallel queries.",
            "Python": (
                "Spark/Dask handles partitioning automatically.  "
                "`multiprocessing.Pool` for manual parallelism."
            ),
        },
        risks=[
            "Only beneficial when downstream transforms are CPU-bound.",
            "Must be recombined with Union All — creates complex topology.",
        ],
    ),
    ComponentKnowledge(
        name="Percentage Sampling",
        category="Data Flow Component",
        description=(
            "Randomly samples a specified percentage of rows from the "
            "data stream.  Has two outputs: selected rows and unselected "
            "rows."
        ),
        behavior=(
            "For each row, generates a random number and compares against "
            "the percentage threshold.  Non-blocking."
        ),
        migration={
            "ADF / Fabric": (
                "In Mapping Data Flows, use a Filter expression with "
                "`rand()` or TABLESAMPLE in the source query."
            ),
            "SQL / Stored Procedure": "TABLESAMPLE or WHERE RAND() < 0.N.",
            "Python": "`df.sample(frac=0.1)` for 10% sampling.",
        },
        risks=["Sample is random — not reproducible without a seed."],
    ),
    ComponentKnowledge(
        name="Row Sampling",
        category="Data Flow Component",
        description=(
            "Randomly samples a fixed number of rows from the data stream."
        ),
        behavior=(
            "Uses reservoir sampling to select exactly N rows from the "
            "stream.  Semi-blocking — must see all rows to guarantee the "
            "final sample."
        ),
        migration={
            "ADF / Fabric": (
                "TOP N in the source query, or filter with row_number() in "
                "a Mapping Data Flow."
            ),
            "SQL / Stored Procedure": "SELECT TOP N ... ORDER BY NEWID().",
            "Python": "`df.sample(n=100)`.",
        },
        risks=["Must process all rows before emitting — semi-blocking."],
    ),
]


# ---------------------------------------------------------------------------
# Connection Manager types
# ---------------------------------------------------------------------------

_CONNECTIONS: list[ComponentKnowledge] = [
    ComponentKnowledge(
        name="OLE DB Connection",
        category="Connection Manager",
        description=(
            "Connects to relational databases via OLE DB providers.  The "
            "most common SSIS connection type — used by Execute SQL Tasks, "
            "OLE DB Sources, Destinations, Lookups, and OLE DB Commands."
        ),
        behavior=(
            "Manages a pooled OLE DB connection.  Provider can be "
            "SQLOLEDB (legacy), MSOLEDBSQL (modern SQL Server), or "
            "third-party OLE DB drivers for Oracle, DB2, etc."
        ),
        migration={
            "ADF / Fabric": (
                "Create a **Linked Service** matching the database type "
                "(Azure SQL, SQL Server with SHIR, Oracle, etc.)."
            ),
            "SQL / Stored Procedure": (
                "Direct connection via the database engine — no wrapper "
                "needed."
            ),
            "Python": (
                "`pyodbc` connection string (prefer ODBC over OLE DB in "
                "Python)."
            ),
        },
        risks=[
            "OLE DB is Windows-only — Linux/cloud may need ODBC instead.",
            "Integrated Security (Windows Auth) needs credential mapping.",
        ],
    ),
    ComponentKnowledge(
        name="ADO.NET Connection",
        category="Connection Manager",
        description=(
            "Connects to databases via .NET data providers (SqlClient, "
            "OracleClient, ODBC, etc.).  Used by some tasks and the "
            "ADO.NET Source/Destination."
        ),
        behavior=(
            "Manages a .NET DbConnection.  Supports a wider range of "
            ".NET providers but is generally slower than OLE DB for bulk "
            "operations in SSIS."
        ),
        migration={
            "ADF / Fabric": (
                "Create a Linked Service for the target database type."
            ),
            "SQL / Stored Procedure": "Direct connection.",
            "Python": "`pyodbc`, `pymssql`, or `cx_Oracle` depending on target.",
        },
        risks=[
            ".NET provider names must map to equivalent drivers on the "
            "target platform.",
        ],
    ),
    ComponentKnowledge(
        name="Flat File Connection",
        category="Connection Manager",
        description=(
            "Defines the format of a delimited or fixed-width text file: "
            "column names, data types, delimiters, text qualifiers, "
            "code page, header rows, etc."
        ),
        behavior=(
            "Stores the file path and format metadata.  Used by Flat File "
            "Source and Flat File Destination.  Format can be Delimited, "
            "FixedWidth, or RaggedRight."
        ),
        migration={
            "ADF / Fabric": (
                "Create a **Delimited Text** or **Fixed Width** dataset "
                "definition.  Column metadata maps to the schema tab."
            ),
            "SQL / Stored Procedure": (
                "Format file (.fmt) for BULK INSERT, or OPENROWSET format."
            ),
            "Python": (
                "`pandas.read_csv()` parameters: sep, quotechar, encoding, "
                "header, names, dtype."
            ),
        },
        risks=[
            "File path (local/UNC) must change for cloud storage (Blob, "
            "ADLS, S3).",
            "Code page / encoding must be preserved exactly.",
            "Column order and delimiter changes break the mapping.",
        ],
    ),
    ComponentKnowledge(
        name="Excel Connection",
        category="Connection Manager",
        description=(
            "Connects to a Microsoft Excel workbook (.xls or .xlsx) via "
            "the ACE or Jet OLE DB provider."
        ),
        behavior=(
            "Opens the Excel file as a pseudo-database.  Sheet names "
            "appear as tables (with $ suffix).  Named ranges also appear "
            "as tables."
        ),
        migration={
            "ADF / Fabric": (
                "Use an **Excel** dataset/linked service in ADF (supports "
                ".xlsx only)."
            ),
            "SQL / Stored Procedure": (
                "OPENROWSET with ACE provider, or import via SSIS/bcp."
            ),
            "Python": (
                "`pandas.read_excel(engine='openpyxl')` for .xlsx, "
                "`xlrd` for .xls."
            ),
        },
        risks=[
            "ACE/Jet provider is 32-bit — causes issues on 64-bit SSIS.",
            "Data type inference from Excel is unreliable.",
            "Password-protected workbooks need special handling.",
        ],
    ),
    ComponentKnowledge(
        name="SMTP Connection",
        category="Connection Manager",
        description=(
            "Connects to an SMTP server for sending email.  Used by the "
            "Send Mail Task."
        ),
        behavior=(
            "Stores SMTP server name/IP, authentication settings, and "
            "SSL configuration."
        ),
        migration={
            "ADF / Fabric": (
                "Use an email API (SendGrid, Microsoft Graph, SES) via "
                "a Web Activity or Logic App."
            ),
            "SQL / Stored Procedure": "Database Mail profile (sp_send_dbmail).",
            "Python": "`smtplib.SMTP` or email service SDK.",
        },
        risks=[
            "SMTP server may be behind a firewall not accessible from cloud.",
            "Authentication credentials need secure storage.",
        ],
    ),
    ComponentKnowledge(
        name="FTP Connection",
        category="Connection Manager",
        description=(
            "Connects to an FTP server.  Used by the FTP Task."
        ),
        behavior=(
            "Stores server address, port, credentials, and passive/active "
            "mode setting."
        ),
        migration={
            "ADF / Fabric": (
                "Create an **FTP** or **SFTP** Linked Service."
            ),
            "SQL / Stored Procedure": "xp_cmdshell with ftp client.",
            "Python": "`ftplib.FTP` or `paramiko.SFTPClient`.",
        },
        risks=[
            "Plain FTP is unencrypted — prefer SFTP.",
            "Passive mode firewall rules differ in cloud.",
        ],
    ),
    ComponentKnowledge(
        name="HTTP Connection",
        category="Connection Manager",
        description=(
            "Connects to a web server/service via HTTP or HTTPS.  Used "
            "by the Web Service Task."
        ),
        behavior=(
            "Stores the base URL, authentication (anonymous, basic, NTLM, "
            "certificate), proxy settings, and timeout."
        ),
        migration={
            "ADF / Fabric": (
                "Use a **Web** or **REST** Linked Service."
            ),
            "SQL / Stored Procedure": (
                "CLR HttpClient or sp_OACreate (not recommended)."
            ),
            "Python": "`requests.Session()` with base URL and auth.",
        },
        risks=[
            "NTLM/Windows authentication may not work in cloud.",
            "Client certificates need secure storage and rotation.",
        ],
    ),
    ComponentKnowledge(
        name="ODBC Connection",
        category="Connection Manager",
        description=(
            "Connects to databases via ODBC drivers.  More portable than "
            "OLE DB but less commonly used in SSIS."
        ),
        behavior=(
            "Uses a system or file DSN, or a direct connection string.  "
            "Works with any database that has an ODBC driver."
        ),
        migration={
            "ADF / Fabric": (
                "Use the appropriate native Linked Service (SQL Server, "
                "Oracle, MySQL, etc.) rather than ODBC."
            ),
            "SQL / Stored Procedure": (
                "Linked server with an ODBC driver, or direct connection."
            ),
            "Python": "`pyodbc.connect()` with the same connection string.",
        },
        risks=[
            "ODBC DSN names are machine-specific — use connection strings "
            "instead.",
            "Driver versions may differ between environments.",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Master index — keyed by lowercase canonical name
# ---------------------------------------------------------------------------

_ALL_ENTRIES: list[ComponentKnowledge] = _CONTROL_FLOW + _DATA_FLOW + _CONNECTIONS

_INDEX: dict[str, ComponentKnowledge] = {}

# Also build an alias map for common alternative names
_ALIASES: dict[str, str] = {
    # Control flow aliases
    "execute sql": "execute sql task",
    "sql task": "execute sql task",
    "data flow": "data flow task",
    "pipeline": "data flow task",
    "foreach loop": "foreach loop container",
    "foreach": "foreach loop container",
    "foreachloop": "foreach loop container",
    "for loop": "for loop container",
    "forloop": "for loop container",
    "sequence": "sequence container",
    "execute package": "execute package task",
    "executepackagetask": "execute package task",
    "execute process": "execute process task",
    "executeprocess": "execute process task",
    "script": "script task",
    "scripttask": "script task",
    "send mail": "send mail task",
    "sendmailtask": "send mail task",
    "file system": "file system task",
    "filesystemtask": "file system task",
    "ftp": "ftp task",
    "ftptask": "ftp task",
    "bulk insert": "bulk insert task",
    "bulkinserttask": "bulk insert task",
    "expression": "expression task",
    "expressiontask": "expression task",
    "data profiling": "data profiling task",
    "dataprofilingtask": "data profiling task",
    "xml": "xml task",
    "xmltask": "xml task",
    "web service": "web service task",
    "webservicetask": "web service task",
    "transfer database": "transfer database task",
    "transferdatabasetask": "transfer database task",
    "transfer jobs": "transfer jobs task",
    "transferjobstask": "transfer jobs task",
    "transfer logins": "transfer logins task",
    "transferloginstask": "transfer logins task",
    "transfer error messages": "transfer error messages task",
    "transfererrormessagestask": "transfer error messages task",
    "transfer stored procedures": "transfer stored procedures task",
    "transferstoredprocedurestask": "transfer stored procedures task",
    "transfer sql server objects": "transfer sql server objects task",
    "transfersqlserverobjectstask": "transfer sql server objects task",
    "analysis services processing": "analysis services processing task",
    "dtsprocessingtask": "analysis services processing task",
    "ssas processing": "analysis services processing task",
    # Data flow aliases
    "oledb source": "ole db source",
    "ole db src": "ole db source",
    "oledb destination": "ole db destination",
    "ole db dest": "ole db destination",
    "oledbdestination": "ole db destination",
    "flat file src": "flat file source",
    "flat file dest": "flat file destination",
    "excel src": "excel source",
    "conditional split": "conditional split",
    "conditionalsplit": "conditional split",
    "derived column": "derived column",
    "derivedcolumn": "derived column",
    "merge join": "merge join",
    "mergejoin": "merge join",
    "union all": "union all",
    "unionall": "union all",
    "row count": "row count",
    "rowcount": "row count",
    "data conversion": "data conversion",
    "dataconversion": "data conversion",
    "ole db command": "ole db command",
    "oledb command": "ole db command",
    "oledbcommand": "ole db command",
    "script component": "script component",
    "scriptcomponent": "script component",
    "scd": "slowly changing dimension",
    "slowly changing dimension": "slowly changing dimension",
    "bdd": "balanced data distributor",
    "percentage sampling": "percentage sampling",
    "pctsampling": "percentage sampling",
    "row sampling": "row sampling",
    "rowsampling": "row sampling",
    # Connection aliases
    "oledb": "ole db connection",
    "oledb connection": "ole db connection",
    "ole db": "ole db connection",
    "ado.net": "ado.net connection",
    "ado.net connection": "ado.net connection",
    "adonet": "ado.net connection",
    "flat file": "flat file connection",
    "flatfile": "flat file connection",
    "excel": "excel connection",
    "smtp": "smtp connection",
    "http": "http connection",
    "odbc": "odbc connection",
    "ftp connection": "ftp connection",
}


def _build_index() -> None:
    """Build the lookup index from all entries."""
    for entry in _ALL_ENTRIES:
        _INDEX[entry.name.lower()] = entry


_build_index()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def lookup_component(name: str) -> ComponentKnowledge | None:
    """Look up a component by name with fuzzy matching.

    Tries in order:
      1. Exact match (case-insensitive)
      2. Alias match
      3. Substring match
      4. Fuzzy match (difflib)

    Returns None if no match is found.
    """
    key = name.strip().lower()

    # 1. Exact match
    if key in _INDEX:
        return _INDEX[key]

    # 2. Alias match
    alias_target = _ALIASES.get(key)
    if alias_target and alias_target in _INDEX:
        return _INDEX[alias_target]

    # 3. Substring match — find entries whose name contains the query
    for entry_key, entry in _INDEX.items():
        if key in entry_key:
            return entry

    # 4. Fuzzy match
    all_names = list(_INDEX.keys()) + list(_ALIASES.keys())
    matches = get_close_matches(key, all_names, n=1, cutoff=0.6)
    if matches:
        matched = matches[0]
        if matched in _INDEX:
            return _INDEX[matched]
        alias_target = _ALIASES.get(matched)
        if alias_target and alias_target in _INDEX:
            return _INDEX[alias_target]

    return None


def list_all_components() -> list[ComponentKnowledge]:
    """Return all knowledge base entries."""
    return list(_ALL_ENTRIES)


def list_categories() -> dict[str, list[str]]:
    """Return component names grouped by category."""
    cats: dict[str, list[str]] = {}
    for entry in _ALL_ENTRIES:
        cats.setdefault(entry.category, []).append(entry.name)
    return cats


def format_knowledge(entry: ComponentKnowledge) -> str:
    """Format a knowledge entry as readable Markdown text."""
    lines = [
        f"# {entry.name}",
        f"**Category:** {entry.category}",
        "",
        "## What It Does",
        entry.description,
        "",
        "## How It Works",
        entry.behavior,
        "",
    ]

    if entry.migration:
        lines.append("## Migration Guidance")
        for platform, guidance in entry.migration.items():
            lines.append(f"### {platform}")
            lines.append(guidance)
            lines.append("")

    if entry.risks:
        lines.append("## Risks & Gotchas")
        for risk in entry.risks:
            lines.append(f"- {risk}")
        lines.append("")

    if entry.see_also:
        lines.append("## See Also")
        lines.append(", ".join(entry.see_also))
        lines.append("")

    return "\n".join(lines)
