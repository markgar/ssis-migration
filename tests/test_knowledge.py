"""Tests for knowledge.py — component knowledge base lookup and formatting."""

from knowledge import (
    ComponentKnowledge,
    format_knowledge,
    list_categories,
    lookup_component,
)


# ---------------------------------------------------------------------------
# lookup_component
# ---------------------------------------------------------------------------

class TestLookupComponent:
    def test_exact_match(self):
        entry = lookup_component("OLE DB Source")
        assert entry is not None
        assert entry.name == "OLE DB Source"

    def test_case_insensitive(self):
        entry = lookup_component("ole db source")
        assert entry is not None
        assert entry.name == "OLE DB Source"

    def test_alias_match(self):
        entry = lookup_component("execute sql")
        assert entry is not None
        assert entry.name == "Execute SQL Task"

    def test_fuzzy_match(self):
        entry = lookup_component("Executee SQL Taskk")
        assert entry is not None
        assert "SQL" in entry.name

    def test_substring_match(self):
        entry = lookup_component("Conditional")
        assert entry is not None
        assert "Conditional Split" in entry.name

    def test_unknown_returns_none(self):
        result = lookup_component("zzz_completely_unknown_xyz_99")
        assert result is None

    def test_data_flow_task(self):
        entry = lookup_component("Data Flow Task")
        assert entry is not None
        assert entry.category == "Control Flow Task"

    def test_connection_manager(self):
        entry = lookup_component("OLE DB Connection")
        assert entry is not None
        assert entry.category == "Connection Manager"


# ---------------------------------------------------------------------------
# list_categories
# ---------------------------------------------------------------------------

class TestListCategories:
    def test_returns_non_empty_dict(self):
        cats = list_categories()
        assert isinstance(cats, dict)
        assert len(cats) > 0

    def test_expected_categories_present(self):
        cats = list_categories()
        assert "Control Flow Task" in cats
        assert "Data Flow Component" in cats
        assert "Connection Manager" in cats

    def test_categories_contain_names(self):
        cats = list_categories()
        assert "Execute SQL Task" in cats["Control Flow Task"]


# ---------------------------------------------------------------------------
# format_knowledge
# ---------------------------------------------------------------------------

class TestFormatKnowledge:
    def test_contains_expected_sections(self):
        entry = lookup_component("Execute SQL Task")
        assert entry is not None
        text = format_knowledge(entry)
        assert "# Execute SQL Task" in text
        assert "**Category:**" in text
        assert "## What It Does" in text
        assert "## How It Works" in text

    def test_migration_section_present(self):
        entry = lookup_component("Execute SQL Task")
        assert entry is not None
        text = format_knowledge(entry)
        assert "## Migration Guidance" in text

    def test_risks_section_present(self):
        entry = lookup_component("Execute SQL Task")
        assert entry is not None
        text = format_knowledge(entry)
        assert "## Risks & Gotchas" in text

    def test_minimal_entry(self):
        entry = ComponentKnowledge(
            name="Test",
            category="Test Category",
            description="A test component.",
            behavior="Does test things.",
        )
        text = format_knowledge(entry)
        assert "# Test" in text
        assert "**Category:** Test Category" in text
        assert "## What It Does" in text
        assert "## Migration Guidance" not in text  # no migration data
