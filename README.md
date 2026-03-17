# SSIS Analyzer Skill

An [Agent Skill](https://agentskills.io) that analyzes SQL Server Integration Services (SSIS) `.dtsx` package files. It extracts and presents control flow, data flows, connections, variables, parameters, SQL statements, script code, column lineage, execution order, and migration assessment information.

## Installation

### Copilot CLI / Claude Code

Register this repo as a plugin marketplace:

```
/plugin marketplace add <owner>/ssis-analyzer-skill
```

Then install the plugin:

```
/plugin install ssis-analyzer@ssis-analyzer-marketplace
```

### Manual

Copy the `skills/ssis-analyzer/` directory into your agent's skills location.

## What It Does

Given a `.dtsx` file, this skill enables an agent to:

- **Document** a package: metadata, connections, tasks, data flows, variables, parameters
- **Trace** column lineage through data flow pipelines
- **Extract** all SQL statements and C#/VB script code
- **Analyze** execution order with parallel branch detection
- **Search** across all named objects in a package
- **Assess** migration readiness for Azure Data Factory / Fabric

## Requirements

- Python 3.10+
- No external dependencies (pure Python standard library)

## Development

```bash
# Run tests
pip install pytest
pytest tests/ -q

# Run a command manually
python skills/ssis-analyzer/scripts/analyze.py <path-to.dtsx> overview
```

## License

MIT
