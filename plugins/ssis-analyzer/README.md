# SSIS Analyzer Plugin

Analyze SQL Server Integration Services (SSIS) `.dtsx` package files — extract
control flow, data flows, connections, variables, parameters, SQL statements,
script code, column lineage, execution order, and migration assessment.

## Installation

```bash
# Using Copilot CLI
copilot plugin install ssis-analyzer@ssis-migration
```

Or register the marketplace first:

```bash
copilot plugin marketplace add markgar/ssis-migration
copilot plugin install ssis-analyzer@ssis-migration
```

## What's Included

### Skills

| Skill | Description |
|-------|-------------|
| `ssis-analyzer` | Analyze `.dtsx` packages to document structure, trace column lineage, extract SQL and scripts, analyze execution order, search named objects, and assess migration readiness for Azure Data Factory / Fabric. |

### Available Commands

| Command | Description |
|---------|-------------|
| `overview` | Package metadata, format version, deployment model, and summary counts |
| `execution-order` | Topologically sorted execution order with parallel branch detection |
| `list-connections` | List all connection managers |
| `connection-detail` | Full details for a specific connection |
| `list-tasks` | Tree view of all tasks and containers |
| `task-detail` | Detailed task info including SQL, script code, loop config |
| `list-constraints` | Precedence constraints between tasks |
| `list-data-flows` | List all data flow tasks |
| `data-flow-detail` | Inspect components and paths in a data flow |
| `component-detail` | Full column specifications for a component |
| `column-lineage` | Trace column lineage through a data flow pipeline |
| `list-variables` | List all variables |
| `list-parameters` | List all parameters |
| `variable-refs` | Cross-reference where variables are set and consumed |
| `extract-sql` | Extract all SQL statements from tasks and components |
| `extract-scripts` | Extract all C#/VB scripts from Script Tasks and Components |
| `find` | Case-insensitive search across all named objects |
| `explain` | Look up what a component does and get migration guidance |
| `list-known-components` | Reference list of all supported SSIS component types |

## Requirements

- Python 3.10+
- No external dependencies (pure Python standard library)

## Source

This plugin is published from [markgar/ssis-migration](https://github.com/markgar/ssis-migration).

## License

MIT
