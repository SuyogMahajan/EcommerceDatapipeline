# 🚀 Ecommerce ETL Pipeline (PySpark + Docker + MySQL)

An end-to-end **data engineering pipeline** built using PySpark and Docker, designed to process, transform, and analyze e-commerce datasets at scale.

---

## 🧠 Project Overview

This project simulates a real-world **data engineering workflow**:

- Ingest raw CSV datasets
- Perform data cleaning & transformations
- Build analytical datasets
- Store results in MySQL for querying
- Run everything on a distributed Spark cluster using Docker

---

## ⚙️ Tech Stack

- Python
- PySpark (Apache Spark)
- MySQL (JDBC Integration)
- Docker & Docker Compose
- Logging (Python logging module)

---

## 🏗️ Architecture

        CSV Files (Raw Data)
                 │
                 ▼
     PySpark (ETL Processing)
                 │
     ┌───────────┴───────────┐
     ▼                       ▼
    Feature Engineering     Aggregations
     │                       │
     └───────────┬───────────┘
                 ▼
           MySQL Database
                 │
                 ▼
        Analytics / Reporting

---

## 📂 Project Structure
    ├── docker-compose.yml
    ├── Dockerfile
    ├── scripts/
    │ └── etl_pipeline.py
    ├── data/
    │ ├── input/
    │ └── output/
    ├── logs/
    └── README.md


---

## 🔄 ETL Pipeline Steps

### 1. Data Ingestion
- Load multiple datasets:
  - Orders
  - Customers
  - Order Items
  - Payments
  - Reviews
  - Products

---

### 2. Data Cleaning
- Removed null values
- Dropped duplicates
- Selected required columns

---

### 3. Data Enrichment
- Joined multiple datasets
- Created a unified dataset for downstream processing

---

### 4. Feature Engineering
Generated new columns:
- `order_date`
- `shipping_time`
- `delivery_time`
- `approval_time`

⚠️ Note: Time intervals are converted to numeric values (minutes/seconds) for database compatibility.

---

### 5. Aggregations (Analytics Tables)

Generated multiple analytical datasets:

- `fact_sales_daily`
- `customer_analytics`
- `product_performance`
- `delivery_performance`
- `review_insights`
- `payment_analytics`
- `order_payment_breakdown`
- `product_category_performance`
- `seller_performance`
- `shipping_cost_analysis`
- `order_fulfillment_analysis`

---

### 6. Data Storage
- Stored final datasets in **MySQL**
- Used Spark JDBC connector

---

## 🐳 Docker Setup

### Services:

- Spark Master
- Spark Worker
- Spark History Server
- MySQL Database

---

## ▶️ How to Run

### Clone the Repository and  run via docker

    git clone <your-repo-url>
    cd <project-folder>

    docker compose up --build
    docker exec -it spark-master spark-submit /opt/spark/scripts/etl_pipeline.py

# Access UIs

* Spark Master UI → http://localhost:8080
* Spark History Server → http://localhost:18080
* MySQL → localhost:3306
* 🗄️ MySQL Configuration (Database: ecommerce , User: spark .Password: spark123)

# 📊 Output Tables
* fact_sales_daily
* customer_analytics
* product_performance
* delivery_performance
* review_insights
* payment_analytics
* order_payment_breakdown
* product_category_performance
* seller_performance
* shipping_cost_analysis
* order_fulfillment_analysis

# ️ ⚠️ Key Learnings
- Spark is lazy evaluated → actions trigger execution 
- Interval types must be converted before storing 
- Distributed systems require structured logging 
- Docker enables reproducible environments

# 🚀 Future Improvements
* Add Apache Airflow for orchestration
* Implement real-time pipeline (Kafka + Spark Streaming)
* Add dashboarding (Metabase / Superset)
* Optimize Spark jobs (partitioning, caching)