# 📂 Enterprise Retail Analytics Transformation Pipeline: Detailed Component Documentation

This document provides a comprehensive, file-by-file breakdown of the project. It is designed to help new developers and architects understand the exact role of every file and how they interconnect within the system.

---

## 1. 🏗️ Infrastructure & Environment (`airflow_dbt_project/`)

These files define the runtime environment and the containerized infrastructure used to host the orchestration layer.

### `Dockerfile`

- **Purpose**: Defines the custom Airflow runtime environment.
- **Key Logic**:
  - Extends the official `apache/airflow:3.2.0` image.
  - Installs `gcc` and other system-level dependencies required for compiling certain Python libraries.
  - Installs Python dependencies listed in `requirements.txt`.
- **Interactions**: This image is built and deployed by `docker-compose.yaml`.

### `docker-compose.yaml`

- **Purpose**: Orchestrates the local deployment of the multi-service Airflow stack.
- **Key Components**:
  - `postgres`: The metadata database that stores DAG states, task history, and user accounts.
  - `redis`: The message broker used by the CeleryExecutor to distribute tasks to workers.
  - `airflow-apiserver`: Provides the REST API for the Airflow UI and external triggers.
  - `airflow-scheduler`: The heartbeat of Airflow; it monitors DAGs and schedules tasks.
  - `airflow-worker`: The execution engine that actually runs the Bash and Python commands.
  - `airflow-init`: A one-time setup container that migrates the database and creates the admin user.
- **Critical Config**: Maps local volumes (like `/dags` and `/walmart_project`) into the container so that code changes on your host machine are immediately reflected in the cluster.

### `requirements.txt`

- **Purpose**: Explicitly defines the Python library versions.
- **Critical Dependencies**:
  - `apache-airflow`: Core orchestration framework.
  - `dbt-core`: The transformation engine.
  - `dbt-databricks`: The adapter that allows dbt to communicate with Databricks SQL warehouses.

### `config/airflow.cfg`

- **Purpose**: The primary configuration file for the Airflow instance.
- **Key Settings**: Defines the `dags_folder` location and core execution settings.

---

## 2. 🚀 Orchestration Layer (`airflow_dbt_project/dags/`)

This directory contains the logic that automates the movement and transformation of data.

### `orchestrate.py`

- **Purpose**: The Master DAG that coordinates the entire data lifecycle.
- **Execution Logic (Linear Pipeline)**:
  1. **`ingest_cdc`**: Uses the `WorkspaceClient` (Databricks SDK) to trigger a specific Job ID. It includes a `while True` polling loop that checks the job status every 5 seconds until it succeeds.
  2. **`clean_target`**: A Bash operator that removes the `target/` and `logs/` directories in the dbt project to prevent artifact collisions.
  3. **`source_freshness`**: Runs `dbt source freshness` to ensure the Bronze layer has new data before spending compute resources on transformations.
  4. **`silver_technical` $\rightarrow$ `silver_technical_tests`**: Runs the cleaning models and validates them.
  5. **`silver_business` $\rightarrow$ `silver_business_tests`**: Runs the OBT (One Big Table) logic and validates it.
  6. **`gold_ephermeral`**: Processes intermediate views.
  7. **`gold_dimensions`**: Executes `dbt snapshot` to implement SCD Type 2 tracking.
  8. **`gold_facts`**: Builds the final reporting tables.
- **Interactions**: Bridges the gap between Databricks Cloud (via API) and the local dbt project (via Bash CLI).

---

## 3. 🛠️ Transformation Layer (`airflow_dbt_project/walmart_project/`)

This is the dbt project where business logic is converted into SQL.

### `dbt_project.yml`

- **Purpose**: Global settings for the dbt project.
- **Key Logic**:
  - Sets the project name and profile.
  - **Materialization Mapping**: Defines that `silver_t` and `silver_b` are `table` materializations, while `gold/ephemeral` models are `ephemeral` (views that don't exist in the DB but are used as intermediate steps).

### `profiles.yml`

- **Purpose**: Connectivity and authentication for the data warehouse.
- **Critical Variables**:
  - `host`, `http_path`, and `token` (Required for Databricks connectivity).
  - Defines the target `catalog` (walmart) and `schema`.

### `macros/custom_schema.sql`

- **Purpose**: Customizes how dbt handles schemas.
- **Key Logic**: Overrides the default behavior to ensure that when a model specifies a custom schema, dbt uses that exact name instead of prefixing it with the target schema.

### `snapshots/` (e.g., `dim_customers.yml`)

- **Purpose**: Implements Slowly Changing Dimensions (SCD Type 2).
- **Logic**: Uses the `timestamp` strategy. It tracks the `updated_timestamp` of records; if a change is detected, it "expires" the old record and creates a new one, preserving the history of changes over time.

### `tests/test_obt.sql`

- **Purpose**: High-level data integrity validation.
- **Logic**: Specifically queries the `obt_b` table to ensure that critical join keys (Order ID, Product ID, etc.) are not NULL, which would indicate a failure in the join logic of the Silver Business layer.

---

## 4. 📊 Data Source & Setup (`walmart_dataset/`)

These files provide the foundation for local testing and initial environment setup.

### `ddl/walmart_schema.sql`

- **Purpose**: Database initialization.
- **Logic**: Creates the base tables (`customers`, `stores`, `products`, `employees`, `orders`, `order_items`).
- **Key Design**: Every table includes `created_timestamp`, `updated_timestamp`, and `is_active` columns to support the CDC logic and dbt snapshots.

### `load_data.py`

- **Purpose**: Data ingestion utility.
- **Logic**: A Python script using `psycopg2` that reads CSVs from the `data/` folder and uses the SQL `COPY` command for high-performance loading into the raw schema.

### `data/` (CSV files)

- **Purpose**: The raw source of truth.
- **Role**: Acts as the "Bronze" data that the pipeline processes.

The Walmart dataset in `walmart_dataset/data/` is a synthetic but realistic retail dataset designed to simulate operational business activity across customers, stores, products, employees, orders, and line items. The dataset is structured as a classic retail star/snowflake-style model: `customers` stores shopper profile information, `stores` defines retail locations, `products` contains item catalog data, `employees` captures staff assigned to a store, `orders` represents a customer purchase event at a specific store, and `order_items` records the detailed product-level breakdown of each order. The core relationship chain is `orders.customer_id -> customers.customer_id`, `orders.store_id -> stores.store_id`, `order_items.order_id -> orders.order_id`, `order_items.product_id -> products.product_id`, and `employees.store_id -> stores.store_id`. In effect, the business question can be answered by joining the order header with its items, then enriching it with customer, product, and store metadata. The working process is straightforward: raw CSV files are loaded into the Bronze layer via `load_data.py`, then the pipeline normalizes and cleans them in the Silver Technical layer (type conversion, null handling, timestamp standardization, and logical filtering), followed by the Silver Business layer where these entities are joined into a denormalized One Big Table (`obt_b`) for analytics. The final Gold layer then builds reporting tables and dimensions, using `updated_timestamp` values and `is_active` flags to support change tracking, incremental processing, and historical analysis. This design makes the dataset ideal for learning Medallion Architecture, foreign-key-based modeling, dbt transformations, and end-to-end retail analytics workflows.

---

## 🔄 Component Interdependency Map

$\text{Raw CSVs} \xrightarrow{\text{load\_data.py}} \text{SQL Database (Bronze)} \xrightarrow{\text{orchestrate.py (Airflow)}} \text{dbt (Silver T)} \xrightarrow{\text{dbt (Silver B)}} \text{dbt (Gold)} \xrightarrow{\text{BI / Dashboards}} \text{Decision Making}$

