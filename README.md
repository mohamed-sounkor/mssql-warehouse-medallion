# 🏗️ SQL Data Warehouse Project
This is my first project in my journey to learn Data Engineering

## 📌 Overview

This project is a **SQL Server-based Data Warehouse** that follows the **Medallion Architecture** — a layered ELT design consisting of **Bronze (raw)**, **Silver (clean)**, and **Gold (business-ready)** data layers. It demonstrates the full **ELT lifecycle** using T-SQL and stored procedures.

> This project is based on the original guided project by [Baraa Khatib Salkini](https://github.com/DataWithBaraa), available here: [Original Repo](https://github.com/DataWithBaraa/sql-data-warehouse-project).  
> I followed the same architectural pattern and added meaningful enhancements in the **Bronze** and **Silver** layers.

---

## 🧱 Architecture Layers

### 🔶 Bronze Layer — *Raw Data*

- **Object Type:** Tables
- **Load Strategy:**  
  - Batch Processing  
  - Full Load  
  - Truncate & Insert  
- **Transformations:** None
- **Data Model:** None (as-is)

#### 🔧 My Enhancement: Modular Loading Logic

Instead of a monolithic stored procedure with hardcoded file paths (as in the original), I used a **main/helper procedure pattern**:

- ✅ `load_bronze` — orchestrates loading for all tables  
- ✅ `bulk_load_csv(@table_name, @file_path)` — reusable loader

#### 💡 Why This Matters

| Feature | Single SP (Original) | Main/Helper SPs (My Version) |
|--------|------------------------|-------------------------------|
| **Maintainability** | ❌ Hard to update paths or add new tables | ✅ Easy to modify/configure |
| **Reusability** | ❌ Logic tightly coupled | ✅ Generic and reusable |
| **Readability** | ✅ All in one place | ❌ Slightly more code spread |
| **Flexibility** | ❌ Static behavior | ✅ Dynamic, param-driven |

**Trade-Off:** While slightly more verbose, my modular implementation supports better **scalability and flexibility** — a worthy trade for production-level pipelines.

---

### ⚪ Silver Layer — *Cleaned, Standardized Data*

- **Object Type:** Tables
- **Load Strategy:**  
  - Batch Processing  
  - Full Load  
  - Truncate & Insert
- **Transformations:**  
  - Data Cleansing  
  - Standardization  
  - Normalization  
  - Derived Columns  
  - Enrichment
- **Data Model:** None (as-is)

#### 🔧 My Enhancement: Quarantine Tables

To improve data quality, I introduced **quarantine tables**:

- ✅ Captures rows with validation errors (e.g., invalid dates, keys)
- ✅ Keeps the main tables clean
- ✅ Supports future data correction & auditability

This mirrors real-world data pipelines where traceability and governance are crucial — especially for compliance and analytics teams.

---

### 🟡 Gold Layer — *Business-Ready Data*

- **Object Type:** Views
- **Transformations:**  
  - Business Logic  
  - Joins/Integrations  
  - Aggregations  
- **Data Models:**  
  - Star Schema  
  - Flat Tables  
  - Aggregated Views

---

## 📊 Consumption Layer

The Gold layer data is used by:

- 📈 **Power BI**, **Tableau**, **Looker**
- 🔍 **Ad-Hoc SQL Queries**
- 🤖 **Machine Learning Models**

---

## 🚀 Technologies Used

- **SQL Server** (T-SQL Engine)
- **Flat Files (CSV)** as source systems
- **Stored Procedures** for ELT logic
- **Power BI / SQL** for visualization

---

## 🙋‍♂️ Credits

- **Instructor:** [Baraa Khatib Salkini](https://github.com/DataWithBaraa)
- **Original Project:** [SQL Data Warehouse Project](https://github.com/DataWithBaraa/sql-data-warehouse-project)

---

## 🛠️ Future Enhancements

- [ ] Metadata-driven ingestion (control/config tables)
- [ ] Incremental loads instead of full loads
- [ ] Automated data validation alerts
- [ ] Data lineage visualization

---
