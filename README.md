# ssis-migration

A plugin repository for [GitHub Copilot](https://github.com/features/copilot) that provides SQL Server migration analysis capabilities.

## Plugins

| Plugin | Description |
|--------|-------------|
| [ssis-analyzer](plugins/ssis-analyzer/) | Analyze SSIS `.dtsx` packages — extract control flow, data flows, connections, variables, SQL, scripts, column lineage, and migration assessment. |
| [dacpac-analyzer](plugins/dacpac-analyzer/) | Analyze SQL Server `.dacpac` and `.bacpac` packages — extract tables, views, stored procedures, functions, constraints, indexes, schemas, and full database schema metadata. |
| [spec-writer](plugins/spec-writer/) | Generate ETL migration spec sets from SSIS/DACPAC artifacts — creates README, CONSTITUTION, and numbered implementation specs for migrating SQL Server ETL to PySpark on Fabric. |

## Installation

```bash
copilot plugin marketplace add markgar/ssis-migration
copilot plugin install ssis-analyzer@ssis-migration
copilot plugin install dacpac-analyzer@ssis-migration
copilot plugin install spec-writer@ssis-migration
```

## Repository Structure

```
├── plugins/
│   ├── ssis-analyzer/       # SSIS package analysis plugin
│   │   ├── .github/plugin/  # Plugin manifest
│   │   ├── skills/          # Skills bundled with the plugin
│   │   ├── scripts/         # Python analysis scripts
│   │   └── README.md        # Plugin documentation
│   ├── dacpac-analyzer/     # DACPAC/BACPAC schema analysis plugin
│   │   ├── .github/plugin/  # Plugin manifest
│   │   ├── skills/          # Skills bundled with the plugin
│   │   ├── scripts/         # Python analysis scripts
│   │   └── README.md        # Plugin documentation
│   └── spec-writer/         # ETL migration spec generation plugin
│       ├── .github/plugin/  # Plugin manifest
│       ├── skills/          # Skills bundled with the plugin
│       └── README.md        # Plugin documentation
```

## License

MIT
