# 🛒 E-Commerce Medallion Pipeline

An end-to-end data engineering project that takes deliberately messy,
incrementally-arriving e-commerce data and turns it into a trusted,
analysis-ready dataset — implemented twice, independently, on two
platforms: **Databricks** and **Microsoft Fabric**.

![Platform](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Platform](https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=flat&logo=microsoft&logoColor=white)
![Language](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Model](https://img.shields.io/badge/Star%20Schema-FFD700?style=flat)

---

## 🧩 The Problem

An e-commerce platform's data comes from several systems (Order
Management, Payment Gateway, Shipping Provider, HR, Product Catalog)
that were never designed to agree with each other:

- 🗂️ **The raw data is dirty**: inconsistent formatting (`"Male"` /
  `"male"` / `"M"` / `"1"`), mixed null tokens (`""` / `"NULL"` /
  `"N/A"` / `"-"`), numbers stored as text (`"$45.00"`), mixed date
  formats in the same column, duplicate records, orphan foreign keys,
  impossible values (a shipment delivered before it was shipped),
  inconsistent file delimiters, broken JSON.
- 🔀 **The systems don't reconcile**: ~20% of orders have no matching
  payment record, and ~20% of non-cancelled orders have no matching
  shipment record.

The goal isn't to hide these gaps — it's to **clean what can be
cleaned, flag what can't, and report confirmed numbers separately from
unconfirmed ones**, so Finance and Operations can trust what they see.

---

## 🏗️ Architecture

```
🐍  Python generator (dirty, incrementally-arriving data)
        │
        ▼
📁  Raw storage (CSV, one folder per table, timestamped files)
        │  ingestion (Auto Loader / batch read)
        ▼
🥉  Bronze  (raw, typed as string, unexpected columns captured)
        │  cleaning, type casting, standardization, deduplication
        ▼
🥈  Silver  (cleaned, CDC-merged for late-arriving order status updates)
        │  star schema modeling
        ▼
🥇  Gold   (5 dimensions + 3 fact tables)
        │
        ▼
📊  Power BI / dashboard reporting
```

> **Why three fact tables, not one?** `fact_order_details` is grained
> at one row per product per order. Payments and shipments happen once
> per *order*, not once per *product* — folding them into the
> product-level fact would duplicate a per-order amount across every
> product row and inflate any `SUM()` (a classic fan-out bug). They're
> kept as separate facts sharing the same dimensions instead.

---

## ✅ What this project demonstrates

- ⏱️ Incremental ingestion with per-table checkpointing (only new data
  is processed on each run)
- 🛡️ Schema-on-read safety nets for unexpected/malformed source columns
- 🚩 A documented, intentional policy for handling missing cross-system
  records (flag, don't guess)
- 🔄 CDC-style merge logic for late-arriving status updates
- ⭐ A star schema designed around measurement grain, not convenience
- ⏰ Scheduled, multi-step orchestration with dependency-based task
  ordering and failure handling
- ☁️ The same design implemented on two different cloud data platforms

---

## 📂 Repository structure

```
ecommerce-medallion-pipeline/
├── README.md
├── docs/
│   └── README.md                  → full write-ups (Notion, one per platform)
├── data_generator/
│   └── incremental_generator.py  → produces the dirty, incremental source data
├── data_model/
│   └── gold_star_schema_final.dbml  → paste into dbdiagram.io to view
└── notebooks/
    ├── databricks/
    │   ├── 01_Raw_to_Bronze.py
    │   ├── 02_bronze_to_silver_pipeline.py
    │   └── 03_silver_to_gold.py
    └── fabric/
        ├── 01_raw_to_bronze.py
        ├── 02_bronze_to_silver.py
        └── 03_silver_to_gold.py
```

---

## 📖 Full documentation

Step-by-step write-ups with screenshots for each platform are linked
from [`docs/README.md`](docs/links.md).

---

## 🛠️ Tech stack

| | |
|---|---|
| 🧱 **Databricks** | Auto Loader, Delta Lake, Unity Catalog Volumes, Databricks Jobs & Workflows, Databricks SQL |
| 🪟 **Microsoft Fabric** | Lakehouse, OneLake, PySpark notebooks, Data Pipelines, Power BI (DirectLake) |
| 🤝 **Both** | PySpark, star schema dimensional modeling |
