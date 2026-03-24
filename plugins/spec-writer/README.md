# Spec Writer Plugin

Generate ETL migration spec sets from SSIS (`.ispac`/`.dtsx`) and database
(`.bacpac`/`.dacpac`) source artifacts. Creates README, CONSTITUTION, and
numbered implementation specs for migrating SQL Server ETL to PySpark on Fabric.

## Installation

```bash
# Using Copilot CLI
copilot plugin install spec-writer@ssis-migration
```

Or register the marketplace first:

```bash
copilot plugin marketplace add markgar/ssis-migration
copilot plugin install spec-writer@ssis-migration
```

## What's Included

### Skills

| Skill | Description |
|-------|-------------|
| `spec-writer` | Orchestrates the `ssis-analyzer` and `dacpac-analyzer` skills to produce complete, implementation-ready specification documents for migrating SSIS ETL packages to PySpark on Microsoft Fabric. |

### Dependencies

This plugin depends on two other plugins in the same repository:

| Plugin | Purpose |
|--------|---------|
| `ssis-analyzer` | Extract control flow, data flows, connections, variables, SQL, scripts, column lineage from `.ispac`/`.dtsx` files |
| `dacpac-analyzer` | Extract table schemas, views, stored procedures, functions, constraints, indexes from `.bacpac`/`.dacpac` files |

## Output

The spec writer produces a directory of Markdown files:

| File | Purpose |
|------|---------|
| `README.md` | High-level overview of the migration, spec table, reference materials |
| `CONSTITUTION.md` | Project-level facts shared across all specs (stack, auth, naming conventions) |
| `01-<name>.md` | First spec — typically prerequisites and infrastructure |
| `02-<name>.md` | Second spec — and so on, in implementation order |

## Usage

Point the skill at your source artifacts and a target directory:

```
Write migration specs from my .ispac and .bacpac files into specs/daily-etl/
```

The skill will:
1. Analyze the SSIS package using `ssis-analyzer`
2. Analyze the database schemas using `dacpac-analyzer`
3. Plan the spec set based on execution order and dependencies
4. Write the CONSTITUTION, numbered specs, and README

## Constraints

- Produces specs only — no PySpark code
- Every schema, column, and procedure comes from the actual artifacts (never invented)
- Each dimension or fact table gets its own numbered spec file
- Known bugs in original procedures are documented explicitly
- Environment-specific values go in CONSTITUTION, not in numbered specs
