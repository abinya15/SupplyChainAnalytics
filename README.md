# 🏭 Supply Chain Analytics in Microsoft Fabric

An end-to-end data engineering and business intelligence solution built natively in Microsoft Fabric, designed to track supply chain quality, monitor supplier defect rates, and analyze the financial impact of manufacturing issues.

```mermaid
graph LR
    A[Raw Data] -->|Ingestion| B[(Bronze Lakehouse)]
    B -->|PySpark Clean| C[(Silver Lakehouse)]
    C -->|Star Schema| D[(Gold Lakehouse)]
    D -->|Direct Lake| E[Semantic Model]
    E -->|DAX & RLS| F[Power BI Dashboard]
    
    style B fill:#CD7F32,stroke:#333,stroke-width:2px,color:#fff
    style C fill:#C0C0C0,stroke:#333,stroke-width:2px,color:#000
    style D fill:#FFD700,stroke:#333,stroke-width:2px,color:#000
    style E fill:#00A4EF,stroke:#333,stroke-width:2px,color:#fff
    style F fill:#F2C811,stroke:#333,stroke-width:2px,color:#000
```

## 🏗️ Architecture & Tech Stack
* **Platform:** Microsoft Fabric
* **Data Engineering:** PySpark, Notebooks
* **Storage:** OneLake, Delta Parquet (Medallion Architecture)
* **Data Modeling:** Power BI Direct Lake Semantic Model, DAX
* **CI/CD:** Fabric Deployment Pipelines (Dev/Test/Prod), Azure DevOps Git Integration

## 🔄 CI/CD & Lifecycle Management
* Established a full deployment pipeline managing isolated **Development, Test, and Production** environments.
* Integrated with Azure DevOps (Git) for version control, enabling push-button code promotion and enterprise-grade release management.

## ⚙️ Data Pipeline (Medallion Architecture)
1. **Bronze Layer:** Raw supply chain data ingestion.
2. **Silver Layer:** Data cleaning, deduplication, and transformation using PySpark. Handled schema enforcement and timestamp formatting.
3. **Gold Layer:** Star schema optimization. Created `fact_defects` and dimensional tables (`dim_supplier`, `dim_region`, `dim_date`) saved as managed Delta tables.

## 📊 Semantic Model & Security
* Configured a **Direct Lake** semantic model to query OneLake files directly without data duplication or import lag.
* Implemented **Row-Level Security (RLS)** via DAX to restrict regional managers to viewing only defects within their assigned territories.

## 📈 Power BI Dashboard
*(Drag and drop your dashboard screenshot here)*

### Key Metrics Analyzed:
* Total Defect Count & Financial Cost Impact
* Supplier Quality Scorecards
* Regional Defect Distribution
