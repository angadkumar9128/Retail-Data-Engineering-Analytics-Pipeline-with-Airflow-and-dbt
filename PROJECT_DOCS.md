# 📂 Enterprise Retail Analytics Transformation Pipeline: Detailed Component Documentation

This document provides a comprehensive, file-by-file breakdown of the project and explains how the components work together to build an end-to-end retail analytics pipeline. It is intended to help new developers, data engineers, and architects understand the architecture, dataset design, transformation flow, and operational logic of the system.

---

## 1. 🏗️ Infrastructure & Environment (`airflow_dbt_project/`)

These files define the runtime infrastructure and the execution environment for the orchestration layer.

### `Dockerfile`

- **Purpose**: Defines the custom Airflow runtime image.
- **Key Logic**:
  - Extends the official `apache/airflow:3.2.0` image.
  - Installs `gcc` and required system dependencies for Python libraries.
  - Installs the Python dependencies listed in `requirements.txt`.
- **Interaction**: This image is built and deployed by `docker-compose.yaml`.

### `docker-compose.yaml`

- **Purpose**: Launches the full local Airflow environment.
- **Key Components**:
  - `postgres`: Metadata database for Airflow state and task history.
  - `redis`: Message broker used by Celery.
  - `airflow-apiserver`: Exposes the Airflow UI and API.
  - `airflow-scheduler`: Schedules and monitors DAG execution.
  - `airflow-worker`: Executes tasks.
  - `airflow-init`: Creates the initial database and admin credentials.
- **Critical Configuration**: Mounts local folders like `/dags` and `/walmart_project` so code changes are reflected immediately in the running containers.

### `requirements.txt`

- **Purpose**: Defines the exact Python package versions used in the project.
- **Core Dependencies**:
  - `apache-airflow`
  - `dbt-core`
  - `dbt-databricks`

### `config/airflow.cfg`

- **Purpose**: Primary Airflow configuration file.
- **Key Settings**: Contains DAG folder location and core runtime settings.

---

## 2. 🚀 Orchestration Layer (`airflow_dbt_project/dags/`)

This directory contains the DAG logic that orchestrates the full data lifecycle.

### `orchestrate.py`

- **Purpose**: Master DAG that coordinates ingestion, validation, transformation, and reporting.
- **Execution Flow**:
  1. **`ingest_cdc`**  
     Triggers a Databricks job via the `WorkspaceClient` SDK and polls until completion.
  2. **`clean_target`**  
     Deletes stale dbt build artifacts to avoid collisions.
  3. **`source_freshness`**  
     Runs `dbt source freshness` to verify Bronze data availability.
  4. **`silver_technical` -> `silver_technical_tests`**  
     Cleans and validates the technical layer.
  5. **`silver_business` -> `silver_business_tests`**  
     Builds the business layer and tests the model logic.
  6. **`gold_ephemeral`**  
     Produces intermediate Gold-layer views.
  7. **`gold_dimensions`**  
     Executes dbt snapshots for Slowly Changing Dimension tracking.
  8. **`gold_facts`**  
     Produces final curated fact tables for business reporting.
- **Interaction**: Connects the Databricks environment to the local dbt project using Airflow Bash operators and the Databricks API.

---

## 3. 🛠️ Transformation Layer (`airflow_dbt_project/walmart_project/`)

This is the dbt project where raw data is transformed into analytical models.

### `dbt_project.yml`

- **Purpose**: Core dbt project configuration.
- **Key Logic**:
  - Sets the project name and profile.
  - Controls materialization strategy.
  - Defines the Bronze/Silver/Gold model flow.

### `profiles.yml`

- **Purpose**: Stores database connection information.
- **Critical Parameters**:
  - `host`
  - `http_path`
  - `token`
  - `catalog`
  - `schema`

### `macros/custom_schema.sql`

- **Purpose**: Customizes how dbt names schemas in different environments.
- **Key Logic**: Ensures that custom schema names are respected without unwanted prefixing.

### `snapshots/`

- **Purpose**: Implements SCD Type 2 change tracking.
- **Function**: Uses timestamp-based logic to detect changes in dimension tables such as customer or product records and preserves historical versions.

### `tests/test_obt.sql`

- **Purpose**: Validates business logic in the final joined model.
- **Logic**: Ensures critical keys such as order ID and product ID are not null, preventing broken joins from reaching reporting.

---

## 4. 📊 Data Source & Setup (`walmart_dataset/`)

This section contains the seed dataset and schema used for local setup and pipeline testing.

### `ddl/walmart_schema.sql`

- **Purpose**: Defines the retail database schema used by the project.
- **Tables Created**:
  - `customers`
  - `stores`
  - `products`
  - `employees`
  - `orders`
  - `order_items`
- **Design Principle**: Every table includes `created_timestamp`, `updated_timestamp`, and `is_active` fields to support CDC-style ingestion and historical tracking.

### `load_data.py`

- **Purpose**: Loads CSV files into the raw database schema.
- **Logic**:
  - Reads CSV files from `walmart_dataset/data/`
  - Maps them to raw database tables
  - Uses PostgreSQL `COPY` for efficient ingestion

### `data/` (CSV files)

- **Purpose**: Provides the raw data source used by the pipeline.
- **Role**: Acts as the "Bronze" layer in the Medallion architecture.

---

## 5. 🧾 Walmart Dataset Overview and Working Model

The dataset in `walmart_dataset/data/` is a synthetic but realistic retail dataset designed to simulate day-to-day operations across a retail enterprise. It contains dimensions and facts that mimic a business environment where customers place orders in stores, products are purchased, employees are assigned to stores, and sales activity is tracked at order and line-item detail levels.

### 5.1 Dataset Tables and Purpose

#### `customers.csv`
- Stores customer master data.
- Includes attributes like:
  - `customer_id`
  - `first_name`
  - `last_name`
  - `email`
  - `phone`
  - `city`
  - `province`
  - `country`
  - timestamps and active flag

#### `stores.csv`
- Stores store location data.
- Includes store metadata such as:
  - `store_id`
  - `store_name`
  - `city`
  - `province`
  - `country`
  - timestamps and active flag

#### `products.csv`
- Stores the item catalog.
- Includes:
  - `product_id`
  - `product_name`
  - `category`
  - `brand`
  - `price`
  - timestamps and active flag

#### `employees.csv`
- Stores employee details associated with stores.
- Includes:
  - `employee_id`
  - `store_id`
  - `first_name`
  - `last_name`
  - `email`
  - `job_title`
  - `salary`
  - timestamps and active flag

#### `orders.csv`
- Stores order header information.
- Includes:
  - `order_id`
  - `customer_id`
  - `store_id`
  - `order_timestamp`
  - `payment_method`
  - `order_status`
  - `total_amount`
  - timestamps and active flag

#### `order_items.csv`
- Stores item-level detail for each order.
- Includes:
  - `order_item_id`
  - `order_id`
  - `product_id`
  - `quantity`
  - `unit_price`
  - `line_amount`
  - timestamps and active flag

### 5.2 Data Relationships

The dataset follows a standard retail relational model:

- `orders.customer_id -> customers.customer_id`
- `orders.store_id -> stores.store_id`
- `order_items.order_id -> orders.order_id`
- `order_items.product_id -> products.product_id`
- `employees.store_id -> stores.store_id`

This relationship model enables business queries such as:
- Which products are selling best by store?
- Which customers are buying most frequently?
- Which regions generate the highest revenue?
- What is the revenue contribution by store, product, or category?
- Which sales trends appear over time?

### 5.3 Working Process and Pipeline Flow

The dataset is loaded into the raw layer via `load_data.py`, which reads the CSV files and loads them into the Bronze schema using PostgreSQL `COPY`. Once the raw tables are populated, the data moves through the Medallion architecture:

1. **Bronze Layer**
   - Raw CSVs are ingested without transformation.
   - Preserves the original source fidelity.
   - Used as the starting historical reference.

2. **Silver Technical Layer**
   - Cleans and standardizes data.
   - Handles:
     - type conversion
     - null handling
     - column renaming
     - timestamp normalization
     - flag filtering
   - Ensures the raw data is ready for join-based analytics.

3. **Silver Business Layer**
   - Joins related tables to build a denormalized analytical view.
   - Produces the business-friendly OBT (`One Big Table`) model.
   - Adds context such as customer names, product names, order totals, and store details.

4. **Gold Layer**
   - Produces final analytical tables for BI and reporting.
   - Includes fact tables and dimension tables.
   - Uses snapshots to track historical changes and maintain SCD Type 2 logic.

### 5.4 Why This Dataset Matters

This dataset is valuable because it represents a realistic retail analytics environment. It supports:
- sales analytics
- demand planning
- customer behavior analysis
- store performance reporting
- operational monitoring
- trend forecasting
- reporting layer development

It is ideal for learning data modeling, dbt transformation logic, Airflow orchestration, and Medallion architecture in a retail context.

---

## 6. 🔄 Component Interdependency Map

```text
Raw CSVs
  -> load_data.py
  -> SQL Database (Bronze)
  -> Airflow orchestrate.py
  -> dbt Silver Technical
  -> dbt Silver Business
  -> dbt Gold Layer
  -> BI / Dashboards / Decision Making
```

This flow demonstrates the complete analytics lifecycle: source data is ingested, cleaned, modeled, validated, and finally used for business reporting and operational insight.

---

## 7. ✅ Summary

This project combines a realistic retail dataset with a modern data engineering stack to demonstrate a complete end-to-end analytics pipeline. The dataset is not just a set of CSV files—it represents a business model where customer behavior, store performance, product sales, and order details are connected through strong relational keys. By processing this data through Bronze, Silver, and Gold layers, the system transforms raw source records into usable business intelligence.
