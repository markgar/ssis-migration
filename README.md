# ssis-migration

A plugin repository for [GitHub Copilot](https://github.com/features/copilot) that provides SQL Server migration analysis capabilities.

## Plugins

| Plugin | Description |
|--------|-------------|
| [ssis-analyzer](plugins/ssis-analyzer/) | Analyze SSIS `.dtsx` packages — extract control flow, data flows, connections, variables, SQL, scripts, column lineage, and migration assessment. |
| [dacpac-analyzer](plugins/dacpac-analyzer/) | Analyze SQL Server `.dacpac` and `.bacpac` packages — extract tables, views, stored procedures, functions, constraints, indexes, schemas, and full database schema metadata. |

## Installation

```bash
copilot plugin marketplace add markgar/ssis-migration
copilot plugin install ssis-analyzer@ssis-migration
copilot plugin install dacpac-analyzer@ssis-migration
```

## Repository Structure

```
├── plugins/
│   ├── ssis-analyzer/       # SSIS package analysis plugin
│   │   ├── .github/plugin/  # Plugin manifest
│   │   ├── skills/          # Skills bundled with the plugin
│   │   ├── scripts/         # Python analysis scripts
│   │   └── README.md        # Plugin documentation
│   └── dacpac-analyzer/     # DACPAC/BACPAC schema analysis plugin
│       ├── .github/plugin/  # Plugin manifest
│       ├── skills/          # Skills bundled with the plugin
│       ├── scripts/         # Python analysis scripts
│       └── README.md        # Plugin documentation
├── tests/                   # Test suite
└── pyproject.toml           # Python project config
```

## Development

```bash
# Run tests
pip install pytest
pytest tests/ -q
```

## License

MIT
