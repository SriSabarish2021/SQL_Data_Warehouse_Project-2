# 🏗️ Data Warehousing Project using PostgreSQL (Medallion Architecture)

## 📌 Project Overview

This project demonstrates a complete **Data Warehousing solution** built using PostgreSQL, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The goal of this project is to design an efficient **ETL pipeline** that extracts raw data, transforms it into a clean and structured format, and presents it in a business-ready layer for analysis.

---

## 🧱 Architecture: Medallion Approach

### 🥉 Bronze Layer (Raw Data)

* Data is extracted from **local files (CSV/External sources)**
* Stored in PostgreSQL without any transformation
* Acts as the **source of truth**
* Schema: `bronze`

**Key Features:**

* Raw ingestion
* No data modification
* Supports reprocessing

---

### 🥈 Silver Layer (Cleaned & Transformed Data)

* Data is **cleaned, validated, and transformed**
* Applied business logic and standardization

**Transformations include:**

* Data type conversions (TEXT → INTEGER, DATE)
* Handling NULL values
* Removing duplicates
* Data validation using regex
* Standardizing categorical values
* Joining multiple data sources

Schema: `silver`

---

### 🥇 Gold Layer (Business-Ready Data)

* Final layer for **analytics and reporting**
* Data is exposed using **views**
* Optimized for querying and business insights

Schema: `gold`

**Features:**

* Aggregated data
* Business-friendly column names
* Simplified structure for reporting tools

---

## ⚙️ ETL Pipeline

### 🔄 Extract

* Loaded data from local files into Bronze tables

### 🔄 Transform

* Cleaned and structured data in Silver layer
* Applied business rules and validations

### 🔄 Load

* Created views in Gold layer for analytics

---

## 🛠️ Technologies Used

* **Database:** PostgreSQL
* **Language:** SQL (PL/pgSQL for procedures)
* **Architecture:** Medallion (Bronze, Silver, Gold)

⭐ If you like this project, don’t forget to give it a star!
