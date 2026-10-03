# 📊 Data Modeling & Star Schema

This section illustrates the data modeling journey of the project, showing how the complex raw data was transformed into an optimized Star Schema for Business Intelligence (BI) reporting.

## 🔄 The Transformation Journey

### 1. Raw Data Model (Before)
The initial complex structure and relationships of the raw data before cleansing and processing in the Bronze layer.
![Raw Schema](star%20schema%20raw.png)

---

### 2. Gold Star Schema (After)
The final Star Schema designed in the Gold layer, ready for direct integration with Power BI. 

We deliberately separated the metrics into **3 distinct Fact Tables** instead of a single massive table to prevent data fan-out issues and incorrect revenue aggregations. These are connected to **5 Dimension Tables** for streamlined analytical queries.
![Final Star Schema](star%20schema%20final.png)

## 🛠️ Interactive DBML Source
This architecture was designed programmatically. You can review the structural code in the [`gold_star_schema_final.dbml`](gold_star_schema_final.dbml) file.

> **💡 Pro Tip:** You can copy the contents of the DBML file and paste it directly into [dbdiagram.io](https://dbdiagram.io) to interact with and edit the schema layout.
