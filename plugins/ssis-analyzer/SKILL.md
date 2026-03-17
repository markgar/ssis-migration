---
name: ssis-analyzer
description: Analyze SQL Server Integration Services (SSIS) .dtsx package files. Use when users need to understand, document, audit, or plan migration of SSIS packages. Extracts control flow, data flows, connections, variables, parameters, SQL statements, script code, column lineage, and execution order. Provides migration assessment for Azure Data Factory / Fabric.
compatibility: Requires Python 3.10+. Uses only Python standard library (no pip install needed). Works with SSIS package files (.dtsx) in XML format.
---

# SSIS Package Analyzer

Analyze SSIS `.dtsx` package files to understand their structure, document them, and assess migration readiness.

## How to Use

All analysis is done via the CLI script. The script path relative to this skill is `scripts/analyze.py`.

```bash
python scripts/analyze.py <path-to-dtsx-file> <command> [args...]
```

**IMPORTANT**: Always use the full path to `analyze.py` based on where this skill is installed. The script uses only Python stdlib — no dependencies to install.

## Available Commands

### Package Overview
```bash
python scripts/analyze.py package.dtsx overview
```
Returns package metadata, format version, deployment model, and summary counts.

### Execution Order
```bash
python scripts/analyze.py package.dtsx execution-order
```
Topologically sorted execution order with parallel branch detection.

### Connections
```bash
python scripts/analyze.py package.dtsx list-connections
python scripts/analyze.py package.dtsx connection-detail "MyConnection"
```
List all connection managers or get full details (connection string, properties, expressions, flat-file schema).

### Control Flow (Tasks)
```bash
python scripts/analyze.py package.dtsx list-tasks
python scripts/analyze.py package.dtsx task-detail "Load Customers"
python scripts/analyze.py package.dtsx list-constraints
```
Tree view of all tasks/containers, detailed task info (SQL, script code, loop config), and precedence constraints.

### Data Flow
```bash
python scripts/analyze.py package.dtsx list-data-flows
python scripts/analyze.py package.dtsx data-flow-detail "Load Data"
python scripts/analyze.py package.dtsx component-detail "Load Data" "OLE DB Source"
python scripts/analyze.py package.dtsx column-lineage "Load Data"
```
List data flows, inspect components and paths, get full column specifications, and trace column lineage.

### Variables & Parameters
```bash
python scripts/analyze.py package.dtsx list-variables
python scripts/analyze.py package.dtsx list-parameters
python scripts/analyze.py package.dtsx variable-refs
python scripts/analyze.py package.dtsx variable-refs "MyVariable"
```
List all variables/parameters and cross-reference where they are set and consumed.

### SQL & Script Extraction
```bash
python scripts/analyze.py package.dtsx extract-sql
python scripts/analyze.py package.dtsx extract-scripts
```
Extract all SQL statements from Execute SQL Tasks and data flow components. Extract all C#/VB scripts from Script Tasks and Script Components.

### Search
```bash
python scripts/analyze.py package.dtsx find "Customer"
```
Case-insensitive search across tasks, connections, variables, parameters, and data flow components.

### Knowledge Base
```bash
python scripts/analyze.py _ explain "OLE DB Source"
python scripts/analyze.py _ list-known-components
```
Look up what a component does, migration guidance, and risks. The first argument can be any value for these commands (the package is not loaded).

## Recommended Workflows

### Full Package Documentation
1. Run `overview` to get the big picture
2. Run `list-connections` to understand data sources and destinations
3. Run `list-tasks` to see the control flow structure
4. Run `execution-order` to understand the execution sequence
5. Run `list-data-flows` then `data-flow-detail` for each flow
6. Run `extract-sql` to collect all SQL
7. Run `list-variables` and `variable-refs` for variable usage

### Migration Assessment
1. Run `overview` to check format version and deployment model
2. Run `list-connections` to identify connection types
3. Run `list-tasks` to identify task types (check for Script Tasks, custom components)
4. For each Data Flow, run `data-flow-detail` to identify component types
5. Use `explain` on unfamiliar components to get migration guidance
6. Run `extract-scripts` to assess custom code complexity
7. Run `list-known-components` for reference on all supported component types

### Quick Summary
1. Run `overview` for counts and metadata
2. Run `execution-order` for the flow
3. Run `find` to locate specific objects

## SSIS Concepts

- **Package**: A `.dtsx` file — the unit of deployment and execution
- **Control Flow**: The sequence of tasks connected by precedence constraints
- **Data Flow**: A pipeline of sources, transforms, and destinations that moves data
- **Connection Manager**: Configuration for connecting to databases, files, FTP, etc.
- **Variable**: Named values scoped to package or container level
- **Parameter**: Externally configurable values (Project Deployment Model)
- **Precedence Constraint**: Links between tasks defining execution order and conditions
- **Component**: A source, transform, or destination within a Data Flow
- **Lineage**: The path a column takes through a Data Flow pipeline
