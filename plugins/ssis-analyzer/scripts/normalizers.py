"""Escape decoding and CreationName normalization."""

from __future__ import annotations

import re
import sys

# ---------------------------------------------------------------------------
# Escaped character decoder
# ---------------------------------------------------------------------------

_ESCAPE_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")


def decode_escapes(s: str) -> str:
    """Replace every ``_xHHHH_`` sequence with the corresponding Unicode character."""
    return _ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), s)


# ---------------------------------------------------------------------------
# CreationName normalization
# ---------------------------------------------------------------------------

_EXACT_MAP: dict[str, str] = {
    # ExecuteSQLTask
    "Microsoft.ExecuteSQLTask": "ExecuteSQLTask",
    # Pipeline
    "{5918251B-2970-45A4-AB5F-01C3C48C3BBD}": "Pipeline",
    "SSIS.Pipeline.2": "Pipeline",
    "Microsoft.Pipeline": "Pipeline",
    # SendMailTask
    "Microsoft.SendMailTask": "SendMailTask",
    # FileSystemTask
    "Microsoft.FileSystemTask": "FileSystemTask",
    # ExecuteProcess
    "Microsoft.ExecuteProcess": "ExecuteProcess",
    # ScriptTask
    "Microsoft.ScriptTask": "ScriptTask",
    # Container types
    "STOCK:FOREACHLOOP": "ForEachLoop",
    "STOCK:FORLOOP": "ForLoop",
    "STOCK:SEQUENCE": "Sequence",
    # Other tasks
    "Microsoft.ExecutePackageTask": "ExecutePackageTask",
    "Microsoft.ExpressionTask": "ExpressionTask",
    "Microsoft.DTSProcessingTask": "DTSProcessingTask",
    "Microsoft.FtpTask": "FtpTask",
    "Microsoft.BulkInsertTask": "BulkInsertTask",
    "Microsoft.DataProfilingTask": "DataProfilingTask",
    "Microsoft.XMLTask": "XMLTask",
    "Microsoft.WebServiceTask": "WebServiceTask",
    "Microsoft.TransferDatabaseTask": "TransferDatabaseTask",
    "Microsoft.TransferJobsTask": "TransferJobsTask",
    "Microsoft.TransferLoginsTask": "TransferLoginsTask",
    "Microsoft.TransferErrorMessagesTask": "TransferErrorMessagesTask",
    "Microsoft.TransferStoredProceduresTask": "TransferStoredProceduresTask",
    "Microsoft.TransferSqlServerObjectsTask": "TransferSqlServerObjectsTask",
    "Microsoft.Package": "Package",
}

# Substring patterns for assembly-qualified names (format 2/6).
# Checked in order; first match wins.
_SUBSTRING_MAP: list[tuple[str, str]] = [
    ("ExecuteSQLTask.ExecuteSQLTask", "ExecuteSQLTask"),
    ("ExecuteProcess.ExecuteProcess", "ExecuteProcess"),
    ("ScriptTask.ScriptTask", "ScriptTask"),
]


def normalize_creation_name(raw: str) -> str:
    """Normalize a raw CreationName to its canonical identifier.

    Resolution order:
      1. Exact match in the known map.
      2. Substring match for assembly-qualified names.
      3. Fall back to the raw value (with warning).
    """
    canonical = _EXACT_MAP.get(raw)
    if canonical is not None:
        return canonical

    for substring, name in _SUBSTRING_MAP:
        if substring in raw:
            return name

    if raw:
        print(
            f"Warning: unknown CreationName {raw!r}, using raw value",
            file=sys.stderr,
        )
    return raw
