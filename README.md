# 📌 Data Engineering Assignment

## 👤 Student Details

* Full Name: Arun Bamal
* Roll No: A501132522015
* Branch: B.tech(AI&ML)
---

## 📌 Overview

This project implements an end-to-end **data engineering pipeline** using DuckDB on an e-commerce behavioral dataset.

The objective is to:

* Design an optimized analytical schema
* Build an ETL pipeline
* Perform analytical queries
* Benchmark performance
* Generate insights through visualizations

---

## 📂 Project Structure

```
data-engineering-assignment/
│
├── notebooks/
│   ├── 01_schema_design.ipynb
│   ├── 02_etl_pipeline.ipynb
│   ├── 03_benchmarks.ipynb
│   ├── 04_queries.ipynb
│   ├── 05_visualizations.ipynb
│
├── sql/
│   └── schema.sql
│
├── schema_diagram.png
├── README.md
└── .gitignore
```

---

## 📊 Dataset

The dataset is not included due to size limitations.

Download from:
https://drive.google.com/drive/folders/1MRcQPQScc-cq35Iw2ZHpMNzW--_hFOHx?usp=drive_link

Place the file here:

```
data/raw/2019-Oct.csv
```

---

## ⚙️ Environment Setup

### 1. Create Virtual Environment

```
python -m venv venv
source venv/Scripts/activate
```

### 2. Install Dependencies

```
pip install duckdb pandas matplotlib jupyter ipykernel
```

### 3. Run Notebooks

```
jupyter notebook
```

---

## 🧱 Schema Design

A star schema is used for analytical efficiency.

### Tables:

* fact_events → stores user interaction events
* dim_product → product details
* dim_category → category hierarchy
* dim_date → time-based attributes
* pipeline_log → ETL tracking

### Justification:

* Reduces redundancy
* Improves GROUP BY performance
* Enables efficient joins
* Indexes added for query optimization

📌 Schema Diagram:

![Schema Diagram](schema_diagram.png)

---

## 🔄 ETL Pipeline

### Extract

* Load CSV using DuckDB (`read_csv_auto`)

### Transform

* Handle missing values
* Split category into main/subcategory
* Convert timestamps

### Load

* Populate dimension tables
* Populate fact table
* Log execution in pipeline_log

---

## 📈 Analytical Queries

* Top categories by revenue
* Top brands by sales
* Conversion funnel (view → cart → purchase)
* User session analysis
* Monthly revenue trends
* Hourly activity patterns

---

## ⚡ Performance Benchmark

Performance comparison between querying raw CSV vs optimized DuckDB tables.

### Query Example: Revenue by Category

| Approach      | Execution Time (seconds) |
| ------------- | ------------------------ |
| Raw CSV       | 8.736719846725464 sec                 |
| DuckDB Tables | 1.40381227 sec                 |

### Additional Benchmarks

| Query               | Table Time |
| ------------------- | -------- |
| Revenue by Category | 1.4038 s   | 
| Top Users           | 0.3288 s   | 
| Hourly activity     | 0.0432 s   | 
|Event funnel         | 0.0377 s    |

### Observations:

* DuckDB tables are significantly faster than raw CSV
* Performance improves due to:

  * Columnar storage
  * Pre-processed schema
  * Indexed joins

---

## 📊 Visualizations

* Revenue by category
* Revenue by brand
* Monthly trends
* Hourly activity patterns

---

## 🧾 DDL (Schema SQL)

Schema definitions are available in:

```
sql/schema.sql
```

Includes:

* Table creation
* Constraints
* Indexes
* Design justifications

---

## ▶️ How to Run

1. Download dataset
2. Place in `data/raw/`
3. Run notebooks in order:

   * Schema Design
   * ETL Pipeline
   * Queries
   * Benchmarks
   * Visualizations

---

## ✅ Summary

This project demonstrates:

* End-to-end data pipeline development
* Efficient schema design using star schema
* Query optimization using DuckDB
* Performance improvement via ETL and indexing

Key insights:

* Electronics category generates highest revenue
* Peak activity occurs during evening hours
* Conversion rate from view to purchase is relatively low

---
