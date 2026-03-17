# GitHub Repositories with SSIS (.dtsx) Samples

*Compiled: March 3, 2026*

## Summary

GitHub code search found **~1,236 .dtsx files** across all public repositories. Most repos with SSIS packages are data warehouse / ETL student projects. The ecosystem is fragmented - there's no single "awesome-ssis" mega-repo. The best sources for diverse sample packages are listed below, ranked by usefulness.

---

## Tier 1: Most Useful for Diverse Samples

### 1. microsoft/sql-server-samples (Microsoft Official)
- **URL:** https://github.com/microsoft/sql-server-samples
- **Stars:** ~9,000+
- **Type:** Official Microsoft sample DB (WideWorldImporters)
- **.dtsx count:** 1 (DailyETLMain.dtsx - but it's large/complex, 882KB)
- **Notes:** The official WideWorldImporters SSIS sample. Contains the same `DailyETLMain.dtsx` we already have in the workspace under `Daily.ETL/`. Protected by Microsoft SAML SSO via GitHub API, but publicly browsable. This is the canonical SSIS sample from Microsoft.
- **Package types:** Complex daily ETL with multiple data flows, dimension/fact loading, connection managers

### 2. Diresage/SSISControlFlowTraining
- **URL:** https://github.com/Diresage/SSISControlFlowTraining
- **Stars:** 0 (but high quality training content)
- **.dtsx count:** **23 packages**
- **Package types:** Excellent diversity of **control flow tasks**:
  - `XmlTaskDemo.dtsx` - XML Task
  - `BulkInsertDemo.dtsx` - Bulk Insert Task
  - `ChildPackage.dtsx` - Execute Package Task (child)
  - `TransferJobsTask.dtsx` - Transfer Jobs Task
  - `DataProfilingDemo.dtsx` - Data Profiling Task
  - `LoopContainerDemo.dtsx` - For/Foreach Loop Container
  - `ForeachSMOEnumDemo.dtsx` - Foreach SMO Enumerator
  - `TransferLoginsDemo.dtsx` - Transfer Logins Task
  - `FilesystemTaskDemo.dtsx` - File System Task
  - `WebServiceTaskDemp.dtsx` - Web Service Task
  - + 13 more (MessageQueueDemo, SendMailDemo, WMIEventWatcher, etc.)
- **Why useful:** Best single source for **control flow task diversity**. Each package demonstrates a different SSIS task type.

### 3. MO3AZ-SAIF/Wise-Owl-28-SSIS-Exercises
- **URL:** https://github.com/MO3AZ-SAIF/Wise-Owl-28-SSIS-Exercises
- **Stars:** 0
- **.dtsx count:** **28 packages** (Ex_1.dtsx through Ex_28.dtsx)
- **Package types:** Progressive exercises covering a broad range of SSIS features - data flow transformations, lookups, conditional splits, derived columns, etc.
- **Why useful:** Systematic exercises that progressively increase in complexity. Good for testing parser against varied patterns.

### 4. Doanh-Chinh/Loading-a-DWH-with-SSIS
- **URL:** https://github.com/Doanh-Chinh/Loading-a-DWH-with-SSIS
- **Description:** "ETL process on AdventureWorks 2019 database using SSIS"
- **.dtsx count:** **16 packages** (8 unique, 8 in obj/Development)
- **Package types:** Classic dimension/fact loading:
  - `DimProduct.dtsx`, `DimCustomer.dtsx`, `DimEmployee.dtsx`
  - `DimPromotion.dtsx`, `DimGeography.dtsx`, `DimSalesTerritory.dtsx`
  - `Fact_InternetSales.dtsx`
- **Why useful:** Clean AdventureWorks DWH ETL with standard dimension/fact patterns, SCD, lookups.

### 5. arunava2024/SSISPackageRef
- **URL:** https://github.com/arunava2024/SSISPackageRef
- **Description:** "SSIS Package For References"
- **.dtsx count:** **24 packages**
- **Package types:** CRM/DW integration patterns:
  - `MasterPackage.dtsx` - Orchestrator/parent package
  - `Main.dtsx` - Main execution
  - `ContactGetDump.dtsx`, `AccountGetDump.dtsx`, `AgreementGetDump.dtsx` - Data extraction
  - `ProductUpdate.dtsx`, `UnitUpdate.dtsx` - Update patterns
  - `GetDW_Mapping.dtsx`, `GetOptionsetData.dtsx` - Metadata/mapping
- **Why useful:** Real-world CRM-to-DW integration patterns with parent/child package execution.

---

## Tier 2: Good Supporting Examples

### 6. rminder/MHS
- **URL:** https://github.com/rminder/MHS
- **.dtsx count:** **29 packages**
- **Package types:** Salesforce + Solomon (SL) ERP integration:
  - `SF/SF-Note.dtsx`, `SF/SF-Product2.dtsx`, `SF/SF-RecordType.dtsx`, `SF/SF-UserDelete.dtsx`, etc.
  - `SL/SL-Ledger.dtsx`, `SL/SL-Subaccount.dtsx`, `SL/SL-VendorClass.dtsx`
  - `WebTime/WebTime.dtsx`
- **Why useful:** Real enterprise integration between Salesforce and ERP, organized in subdirectories.

### 7. AlainPerez/SSIS-WideWorldImporters
- **URL:** https://github.com/AlainPerez/SSIS-WideWorldImporters
- **Stars:** 1
- **.dtsx count:** 1 (`DailyETLMain.dtsx`, 882KB)
- **Description:** Fork/variant of the WideWorldImporters SSIS sample with full Visual Studio solution structure (.sln, .dtproj, .conmgr files)
- **Why useful:** Complete SSIS project structure with connection managers, project params.

### 8. tawfikhammad/ETL-AdventureWorks-SSIS
- **URL:** https://github.com/tawfikhammad/ETL-AdventureWorks-SSIS
- **Stars:** 1
- **.dtsx count:** **5 packages**
- **Package types:** AdventureWorks Data Mart ETL:
  - `Dim Customer ETL.dtsx`, `Dim Date ETL.dtsx`, `Dim Product ETL.dtsx`
  - `Dim Territory ETL.dtsx`, `Fact Sales.dtsx`
- **Why useful:** Clean dimension/fact ETL with SQL scripts and design docs included.

### 9. Jabari-59/Proyecto-Ventas-ETL-SSIS-SSAS
- **URL:** https://github.com/Jabari-59/Proyecto-Ventas-ETL-SSIS-SSAS
- **Description:** "Data Warehouse for sales with SSIS ETL and SSAS tabular model"
- **.dtsx count:** **7 packages**
- **Package types:** Sales DWH with orchestrator:
  - `DimFecha.dtsx`, `DimDivisa.dtsx`, `DimTienda.dtsx`, `DimProducto.dtsx`, `DimPromocion.dtsx`
  - `ThVentas.dtsx` (fact table)
  - `Orquestador.dtsx` (orchestrator/master package)
- **Why useful:** Includes orchestrator pattern + SSAS integration.

### 10. saraadel6/BikeStore-Data-Warehouse-project
- **URL:** https://github.com/saraadel6/BikeStore-Data-Warehouse-project
- **.dtsx count:** ~10 packages
- **Package types:** BikeStore DWH - `Order_Items.dtsx`, dimension/fact tables
- **Why useful:** Different source database (BikeStore) for variety.

---

## Tier 3: Smaller/Niche but Interesting

| Repository | URL | .dtsx Count | Type |
|---|---|---|---|
| microsoft/SQLBDC-AppDeploy | https://github.com/microsoft/SQLBDC-AppDeploy | 1 | SQL Big Data Cluster app template (hello.dtsx) |
| DuaA-A/DWH-Project | https://github.com/DuaA-A/DWH-Project | ~5 | DWH from real OLTP source with staging |
| allan-kirui/SpotifyETL | https://github.com/allan-kirui/SpotifyETL | ~2 | Spotify data ETL |
| gennovas/FMT.ETLProcess | https://github.com/gennovas/FMT.ETLProcess | ~3 | Forecast/EDI (Forecast862.dtsx) |
| dinamaher12/DatawarehouseProject | https://github.com/dinamaher12/DatawarehouseProject | ~5 | DWH with language dimensions |
| FedericoAlmiron02/ADE | https://github.com/FedericoAlmiron02/ADE | ~3 | SCD (Slowly Changing Dimension) examples |
| eshohag/FileUpload_ETL_SSIS | https://github.com/eshohag/FileUpload_ETL_SSIS | ~2 | File loop + CSV upload pattern |
| NTgog/Business-Intelligence-MusicCompany | https://github.com/NTgog/Business-Intelligence-MusicCompany | ~3 | BI project with Update.dtsx |
| marcelmotta/IMSports-ETL | https://github.com/marcelmotta/IMSports-ETL | ~10 | AdventureWorks DWH (24 stars, highest for actual SSIS ETL) |
| freddie2025/WideWorldImporters | https://github.com/freddie2025/WideWorldImporters | ? | SQL Server sandbox (5 stars, includes SSIS/SSAS/SSRS/PowerBI) |
| TMS-BI21-onl/Yuliya.Adziareika | https://github.com/TMS-BI21-onl/Yuliya.Adziareika | ~5 | BI training school lesson packages |

---

## Related Tools (not .dtsx packages but SSIS-related)

| Repository | URL | Stars | Description |
|---|---|---|---|
| yorek/ssis-dashboard | https://github.com/yorek/ssis-dashboard | 260 | HTML5 SSIS monitoring dashboard |
| yorek/ssis-queries | https://github.com/yorek/ssis-queries | 99 | SSISDB monitoring queries |
| HadiFadl/GetDTSXInfos | https://github.com/HadiFadl/GetDTSXInfos | 6 | .dtsx metadata reader (VB.NET) |
| DAToolset/SSIS_Util | https://github.com/DAToolset/SSIS_Util | 1 | Extract connections/SQL from .dtsx (Python) |
| 7045kHz/dtsx | https://github.com/7045kHz/dtsx | 0 | DTSX parser in Go |
| kromerm/adflab | https://github.com/kromerm/adflab | 137 | ADF lab for SSIS lift-and-shift to Azure |

---

## Recommendations for Diverse Sample Packages

To get the best coverage of SSIS patterns, I recommend cloning these repos:

1. **Diresage/SSISControlFlowTraining** (23 packages) - Best for control flow task diversity
2. **MO3AZ-SAIF/Wise-Owl-28-SSIS-Exercises** (28 packages) - Best for progressive data flow complexity
3. **Doanh-Chinh/Loading-a-DWH-with-SSIS** (16 packages) - Best for standard DWH dimension/fact patterns
4. **arunava2024/SSISPackageRef** (24 packages) - Best for real-world CRM integration patterns
5. **rminder/MHS** (29 packages) - Best for enterprise Salesforce/ERP integration

Combined with the WideWorldImporters `DailyETLMain.dtsx` and the tutorial Lessons 1-5 already in your workspace, this gives a corpus of **~130+ diverse .dtsx files** covering:
- Control flow tasks (XML, Bulk Insert, File System, Web Service, Send Mail, WMI, etc.)
- Data flow transformations (Lookup, Derived Column, Conditional Split, SCD, etc.)
- Container patterns (Foreach Loop, For Loop, Sequence)
- Parent/child package execution (Execute Package Task)
- Connection managers (OLE DB, Flat File, ADO.NET)
- Dimension loading (SCD Type 1 & 2)
- Fact table loading
- Real-world ETL patterns (CRM, ERP, Salesforce)
- Error handling and logging
