# Wide World Importers migration example

This example combines the official Microsoft Wide World Importers OLTP and data
warehouse databases with the SSIS project that performs the daily ETL load.
It provides a matched set of source artifacts for exercising the SSIS and
DACPAC/BACPAC analyzers in this repository.

## Contents

```text
wide-world-importers/
├── databases/
│   ├── WideWorldImporters-Full.bacpac
│   └── WideWorldImportersDW-Full.bacpac
├── ssis/
│   ├── Daily ETL.dtproj
│   ├── DailyETLMain.dtsx
│   ├── Project.params
│   ├── WWI-SSIS.database
│   ├── WWI_DW_Destination_DB.conmgr
│   ├── WWI_Source_DB.conmgr
│   └── wwi-ssis.sln
└── LICENSE.microsoft.txt
```

- `WideWorldImporters-Full.bacpac` is the transactional source database.
- `WideWorldImportersDW-Full.bacpac` is the destination data warehouse.
- `DailyETLMain.dtsx` loads dimensions and facts from `WideWorldImporters` into
  `WideWorldImportersDW`.

The BACPAC files are stored with Git LFS. Run `git lfs install` once on your
machine and `git lfs pull` after cloning to retrieve them.

## Analyze the artifacts

From the repository root:

```bash
python3 plugins/dacpac-analyzer/scripts/analyze.py \
  examples/wide-world-importers/databases/WideWorldImporters-Full.bacpac \
  overview

python3 plugins/dacpac-analyzer/scripts/analyze.py \
  examples/wide-world-importers/databases/WideWorldImportersDW-Full.bacpac \
  overview

python3 plugins/ssis-analyzer/scripts/analyze.py \
  examples/wide-world-importers/ssis/DailyETLMain.dtsx \
  overview
```

The analyzers require Python 3.10 or later.

## Run the original SSIS project

The project was built for SQL Server 2016. Its project-level OLE DB connection
managers use Windows integrated authentication and target a local SQL Server
instance (`Data Source=.`):

- `WWI_Source_DB` connects to `WideWorldImporters`.
- `WWI_DW_Destination_DB` connects to `WideWorldImportersDW`.

Import the BACPAC files into SQL Server, then update the connection managers if
your server, authentication method, or installed OLE DB provider differs. The
original project references the legacy `SQLNCLI11.1` provider.

## Source and license

These files come from Microsoft's
[SQL Server samples](https://github.com/microsoft/sql-server-samples):

- [Wide World Importers v1.0 database release](https://github.com/microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0)
- [Wide World Importers SSIS project](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/wide-world-importers/wwi-ssis)

They are redistributed under Microsoft's MIT license, included as
`LICENSE.microsoft.txt`.
