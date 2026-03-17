"""Tests that load real .dtsx packages and verify control flow task extraction."""

from __future__ import annotations

from pathlib import Path
from typing import Generator

import pytest

from loader import load_package
from models import Executable, ParsedPackage

CONTROL_FLOW_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "ControlFlowTraining"
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def walk_executables(executables: list[Executable]) -> Generator[Executable, None, None]:
    """Recursively yield every Executable in the tree."""
    for exe in executables:
        yield exe
        yield from walk_executables(exe.children)


def find_by_creation_name(pkg: ParsedPackage, name: str) -> list[Executable]:
    """Return all executables whose creation_name matches *name*."""
    return [e for e in walk_executables(pkg.executables) if e.creation_name == name]


# ---------------------------------------------------------------------------
# ForLoop — ForLoopContainerDemo_2.dtsx
# ---------------------------------------------------------------------------

class TestForLoop:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "ForLoopContainerDemo_2.dtsx")

    def test_has_for_loop(self, pkg: ParsedPackage) -> None:
        loops = find_by_creation_name(pkg, "ForLoop")
        assert len(loops) >= 1

    def test_for_loop_payload(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForLoop")[0]
        assert loop.for_loop is not None

    def test_for_loop_expressions(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForLoop")[0]
        fl = loop.for_loop
        assert fl is not None
        # A for-loop container should have at least an eval expression
        assert fl.eval_expression != ""

    def test_for_loop_contains_execute_sql(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForLoop")[0]
        child_names = [c.creation_name for c in walk_executables(loop.children)]
        assert "ExecuteSQLTask" in child_names


# ---------------------------------------------------------------------------
# ForEachLoop — ForeachLoopFileEnumDemo.dtsx
# ---------------------------------------------------------------------------

class TestForEachLoop:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "ForeachLoopFileEnumDemo.dtsx")

    def test_has_for_each_loop(self, pkg: ParsedPackage) -> None:
        loops = find_by_creation_name(pkg, "ForEachLoop")
        assert len(loops) >= 1

    def test_for_each_payload(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForEachLoop")[0]
        assert loop.for_each is not None

    def test_for_each_enumerator_type(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForEachLoop")[0]
        assert loop.for_each is not None
        assert loop.for_each.enumerator_type != ""

    def test_for_each_contains_pipeline(self, pkg: ParsedPackage) -> None:
        loop = find_by_creation_name(pkg, "ForEachLoop")[0]
        child_names = [c.creation_name for c in walk_executables(loop.children)]
        assert "Pipeline" in child_names


# ---------------------------------------------------------------------------
# ScriptTask — ChildPackage.dtsx
# ---------------------------------------------------------------------------

class TestScriptTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "ChildPackage.dtsx")

    def test_has_script_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "ScriptTask")
        assert len(tasks) >= 1

    def test_script_task_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ScriptTask")[0]
        assert task.script_task is not None

    def test_script_task_has_source_code(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ScriptTask")[0]
        st = task.script_task
        assert st is not None
        # source_code is a dict mapping filename → code text
        assert st.source_code is not None
        assert len(st.source_code) > 0


# ---------------------------------------------------------------------------
# ExecutePackageTask — ExecutePackageParent.dtsx
# ---------------------------------------------------------------------------

class TestExecutePackageTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "ExecutePackageParent.dtsx")

    def test_has_execute_package_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "ExecutePackageTask")
        assert len(tasks) >= 1

    def test_execute_package_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ExecutePackageTask")[0]
        assert task.execute_package is not None

    def test_execute_package_has_package_name(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ExecutePackageTask")[0]
        ep = task.execute_package
        assert ep is not None
        # The connection or package_name should be populated
        assert ep.package_name != "" or ep.connection is not None


# ---------------------------------------------------------------------------
# FileSystemTask — FilesystemTaskDemo.dtsx
# ---------------------------------------------------------------------------

class TestFileSystemTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "FilesystemTaskDemo.dtsx")

    def test_has_file_system_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "FileSystemTask")
        assert len(tasks) >= 1

    def test_file_system_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "FileSystemTask")[0]
        assert task.file_system is not None

    def test_file_system_has_operation(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "FileSystemTask")[0]
        fs = task.file_system
        assert fs is not None
        assert fs.operation != ""

    def test_file_system_has_source_connection(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "FileSystemTask")[0]
        fs = task.file_system
        assert fs is not None
        assert fs.source_connection != ""


# ---------------------------------------------------------------------------
# BulkInsertTask — BulkInsertDemo.dtsx
# ---------------------------------------------------------------------------

class TestBulkInsertTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "BulkInsertDemo.dtsx")

    def test_has_bulk_insert_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "BulkInsertTask")
        assert len(tasks) >= 1

    def test_bulk_insert_generic_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "BulkInsertTask")[0]
        assert task.generic_payload is not None

    def test_bulk_insert_payload_has_keys(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "BulkInsertTask")[0]
        gp = task.generic_payload
        assert gp is not None
        assert len(gp) > 0


# ---------------------------------------------------------------------------
# TransferDatabaseTask — TransferDatabaseDemo.dtsx
# ---------------------------------------------------------------------------

class TestTransferDatabaseTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "TransferDatabaseDemo.dtsx")

    def test_has_transfer_database_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "TransferDatabaseTask")
        assert len(tasks) >= 1

    def test_transfer_task_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferDatabaseTask")[0]
        assert task.transfer_task is not None

    def test_transfer_task_has_connections(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferDatabaseTask")[0]
        tt = task.transfer_task
        assert tt is not None
        assert tt.source_connection != ""
        assert tt.dest_connection != ""

    def test_transfer_task_type(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferDatabaseTask")[0]
        assert task.transfer_task is not None
        assert task.transfer_task.task_type == "TransferDatabaseTask"


# ---------------------------------------------------------------------------
# WebServiceTask — WebServiceTaskDemp.dtsx
# ---------------------------------------------------------------------------

class TestWebServiceTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "WebServiceTaskDemp.dtsx")

    def test_has_web_service_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "WebServiceTask")
        assert len(tasks) >= 1

    def test_web_service_generic_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "WebServiceTask")[0]
        assert task.generic_payload is not None

    def test_web_service_payload_has_keys(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "WebServiceTask")[0]
        gp = task.generic_payload
        assert gp is not None
        assert len(gp) > 0

    def test_also_has_script_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "ScriptTask")
        assert len(tasks) >= 1


# ---------------------------------------------------------------------------
# XMLTask — XmlTaskDemo.dtsx
# ---------------------------------------------------------------------------

class TestXMLTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "XmlTaskDemo.dtsx")

    def test_has_xml_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "XMLTask")
        assert len(tasks) >= 1

    def test_xml_task_generic_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "XMLTask")[0]
        assert task.generic_payload is not None

    def test_xml_task_payload_has_keys(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "XMLTask")[0]
        gp = task.generic_payload
        assert gp is not None
        assert len(gp) > 0


# ---------------------------------------------------------------------------
# ExecuteProcess — ExecuteProcessTask.dtsx
# ---------------------------------------------------------------------------

class TestExecuteProcess:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "ExecuteProcessTask.dtsx")

    def test_has_execute_process(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "ExecuteProcess")
        assert len(tasks) >= 1

    def test_execute_process_generic_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ExecuteProcess")[0]
        # ExecuteProcess has no dedicated payload; falls back to generic
        assert task.generic_payload is not None

    def test_execute_process_payload_has_keys(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "ExecuteProcess")[0]
        gp = task.generic_payload
        assert gp is not None
        assert len(gp) > 0


# ---------------------------------------------------------------------------
# Sequence container — SequenceContainerDemo.dtsx
# ---------------------------------------------------------------------------

class TestSequenceContainer:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "SequenceContainerDemo.dtsx")

    def test_has_sequence_container(self, pkg: ParsedPackage) -> None:
        seqs = find_by_creation_name(pkg, "Sequence")
        assert len(seqs) >= 1

    def test_sequence_has_children(self, pkg: ParsedPackage) -> None:
        seq = find_by_creation_name(pkg, "Sequence")[0]
        assert len(seq.children) > 0

    def test_sequence_no_specific_payload(self, pkg: ParsedPackage) -> None:
        """Sequence is a pure container — no task-specific payload."""
        seq = find_by_creation_name(pkg, "Sequence")[0]
        assert seq.execute_sql is None
        assert seq.script_task is None
        assert seq.for_loop is None
        assert seq.for_each is None
        assert seq.data_flow is None

    def test_package_also_has_script_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "ScriptTask")
        assert len(tasks) >= 1


# ---------------------------------------------------------------------------
# Additional coverage — TransferJobsTask (TransferJobsTask.dtsx)
# ---------------------------------------------------------------------------

class TestTransferJobsTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "TransferJobsTask.dtsx")

    def test_has_transfer_jobs_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "TransferJobsTask")
        assert len(tasks) >= 1

    def test_transfer_task_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferJobsTask")[0]
        assert task.transfer_task is not None

    def test_transfer_jobs_task_type(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferJobsTask")[0]
        assert task.transfer_task is not None
        assert task.transfer_task.task_type == "TransferJobsTask"


# ---------------------------------------------------------------------------
# Additional coverage — TransferLoginsTask (TransferLoginsDemo.dtsx)
# ---------------------------------------------------------------------------

class TestTransferLoginsTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "TransferLoginsDemo.dtsx")

    def test_has_transfer_logins_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "TransferLoginsTask")
        assert len(tasks) >= 1

    def test_transfer_task_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferLoginsTask")[0]
        assert task.transfer_task is not None

    def test_transfer_logins_task_type(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "TransferLoginsTask")[0]
        assert task.transfer_task is not None
        assert task.transfer_task.task_type == "TransferLoginsTask"


# ---------------------------------------------------------------------------
# Additional coverage — DataProfilingTask (DataProfilingDemo.dtsx)
# ---------------------------------------------------------------------------

class TestDataProfilingTask:
    @pytest.fixture(scope="class")
    def pkg(self) -> ParsedPackage:
        return load_package(CONTROL_FLOW_DIR / "DataProfilingDemo.dtsx")

    def test_has_data_profiling_task(self, pkg: ParsedPackage) -> None:
        tasks = find_by_creation_name(pkg, "DataProfilingTask")
        assert len(tasks) >= 1

    def test_data_profiling_generic_payload(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "DataProfilingTask")[0]
        assert task.generic_payload is not None

    def test_data_profiling_payload_has_keys(self, pkg: ParsedPackage) -> None:
        task = find_by_creation_name(pkg, "DataProfilingTask")[0]
        gp = task.generic_payload
        assert gp is not None
        assert len(gp) > 0
