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

## 5. 🧾 Walmart Dataset — Expanded, Column-level and Business-Focused Documentation

Below is an in-depth reference for each CSV in `walmart_dataset/data/`. This section explains: why the dataset is included, where it comes from (source), how it satisfies business use cases, the role of each column, primary and foreign keys, how tables interact, what insights each table enables, and known limitations and data-quality concerns.

### 5.1 Why this dataset is included

- Purpose: The dataset is included as a realistic seed for local development, testing, CI, and demonstrations of the Medallion architecture. It replicates the common entities and relationships found in retail operations so transformations and tests can be developed without external systems.
- Origin: The data is synthetic (generated or sampled) to be privacy-safe but realistic in distribution and cardinality. It models Walmart-like retail events (orders, order items, product catalog, customers, stores, and employees) to exercise joins, aggregations, incremental loads, and SCD patterns.
- Business value: It allows developers and analysts to validate data ingestion, test dbt models and tests, simulate incremental ingestion and snapshots, and prototype dashboards and KPIs without needing production access.

### 5.2 How the dataset solves business needs (mapping to use cases)

- Retail performance monitoring: order and order_items provide the transactional detail required to compute revenue, average order value, units sold, and product-level performance.
- Inventory & demand planning: product-level sales over time by store enable replenishment decisions and demand forecasting.
- Customer and sales insights: customer-level attributes joined with orders enable RFM (recency, frequency, monetary) segmentation and cohort analysis.
- Operational reporting: employees + store information enables headcount and labor cost analysis tied to store sales.
- Forecasting and trend analysis: timestamped orders and order_items enable time-series modeling and seasonality detection.

### 5.3 Table-by-table column descriptions, keys and role

Note: the CSVs are loaded into `raw.*` tables by `load_data.py` and have the same column names as shown below.

#### customers.csv
- Primary key: `customer_id` (BIGINT) — unique identifier for a customer.
- Columns and purpose:
  - `customer_id` — PK, stable identifier used to join orders.
  - `first_name`, `last_name` — customer attributes for reporting and personalization.
  - `email`, `phone` — contact info (sensitive in production; synthetic here).
  - `city`, `province`, `country` — geography for regional analysis.
  - `created_timestamp` — when the customer record was created in source systems.
  - `updated_timestamp` — last update time for the record (used by snapshots/SCD logic).
  - `is_active` (CHAR(1), values: 'Y'|'N') — soft-delete/active flag. Use to filter out inactive customers in Silver.

Why columns matter: `customer_id` links to orders; timestamps let you detect profile changes and support SCD Type 2; contact and geography fields enable customer segmentation and regional KPIs.

Known limits: synthetic contact fields are not suitable for real communications; email/phone may not be normalized; missing or duplicated customer_ids should be checked with dbt tests.


#### stores.csv
- Primary key: `store_id` (BIGINT).
- Columns:
  - `store_id` — PK.
  - `store_name` — human-readable name.
  - `city`, `province`, `country` — location; used for geospatial rollups.
  - `created_timestamp`, `updated_timestamp` — lifecycle timestamps.
  - `is_active` — active flag to retire closed stores.

Role: Provides the location dimension for sales aggregation and store-level KPIs.

Limitations: store geographic granularity is city/province; no precise coordinates (latitude/longitude) available; enrich with geo data if needed.


#### products.csv
- Primary key: `product_id` (BIGINT).
- Columns:
  - `product_id` — PK.
  - `product_name` — product label for catalog reporting.
  - `category`, `brand` — categorical attributes for grouping and hierarchy.
  - `price` (NUMERIC(10,2)) — catalog price; may differ from unit_price on receipts due to discounts.
  - `created_timestamp`, `updated_timestamp`, `is_active` — lifecycle management.

Role: Offers product metadata for product-performance analytics, category-level reporting and enrichment of order items.

Limitations: only catalog price available; promotions/discounts are not modeled as separate tables (you must infer discounts by comparing `price` and `unit_price` on order_items).


#### employees.csv
- Primary key: `employee_id` (BIGINT).
- Columns:
  - `employee_id` — PK.
  - `store_id` — FK -> `stores.store_id` (nullable if corporate employee).
  - `first_name`, `last_name`, `email` — identity fields.
  - `job_title` — role used for labour reporting.
  - `salary` (NUMERIC(10,2)) — used for cost-of-labour calculations.
  - `created_timestamp`, `updated_timestamp`, `is_active` — lifecycle.

Role: Enables linking labour costs to store performance and building store-level efficiency metrics.

Limitations: No shift/time logs are present; linking salary to specific sales periods requires additional datasets (timesheets).


#### orders.csv
- Primary key: `order_id` (BIGINT).
- Columns:
  - `order_id` — PK.
  - `customer_id` — FK -> `customers.customer_id`.
  - `store_id` — FK -> `stores.store_id`.
  - `order_timestamp` — when the sale took place (used for time-series aggregation).
  - `payment_method` — e.g., Cash, Card.
  - `order_status` — e.g., Completed, Cancelled (useful to exclude refunds/cancels).
  - `total_amount` — header-level sum (should equal sum of line amounts in order_items for completed orders).
  - `created_timestamp`, `updated_timestamp`, `is_active` — lifecycle and soft-delete.

Role: Order header ties customer and store context to transactional activity.

Quality checks to run: verify `total_amount` ≈ SUM(order_items.line_amount) per order for active orders; check for orphaned orders (customer_id or store_id not found in dimension tables).


#### order_items.csv
- Primary key: `order_item_id` (BIGINT).
- Columns:
  - `order_item_id` — PK.
  - `order_id` — FK -> `orders.order_id`.
  - `product_id` — FK -> `products.product_id`.
  - `quantity` (INT) — units sold in the line.
  - `unit_price` (NUMERIC(10,2)) — price charged per unit on the receipt.
  - `line_amount` (NUMERIC(12,2)) — typically `quantity * unit_price` (may include per-line discounts).
  - `created_timestamp`, `updated_timestamp`, `is_active` — lifecycle.

Role: The most granular transactional table — used to compute units sold, revenue per product, margins (given cost data), and support basket analysis.

Quality checks: `line_amount` === `quantity * unit_price` or accept small rounding differences; ensure `order_id` exists in orders; check for negative quantities or zero/negative prices.


### 5.4 Special columns explained: `is_active`, `created_timestamp`, `updated_timestamp`

- `is_active` (CHAR(1)): a soft-delete flag used to indicate whether the source considers the row active. Typical values in this dataset are 'Y' (active) and 'N' (inactive). Use cases:
  - Filter out inactive rows in Silver and Gold models to avoid skewing metrics.
  - Support soft-delete semantics (retain history in Bronze, exclude in Silver unless historical view required).

- `created_timestamp`: the time the record was first created in the source system. Useful for auditing, onboarding timelines, and initial ingestion ordering.

- `updated_timestamp`: the time the record was last modified. This is the primary field used by dbt `snapshots` to detect changes and implement SCD Type 2. When `updated_timestamp` changes for a PK, dbt snapshot logic should expire the previous version and insert a new version.

Why two timestamps? Having both `created_timestamp` and `updated_timestamp` preserves both the initial creation time and the most recent change time. This separation is valuable for:
- accurate SCD tracking (change detection vs original creation date)
- determining latency between creation and updates
- debugging and forensic analysis of data changes

### 5.5 How tables interact and examples of join patterns

Core join patterns used in Silver Business (OBT) models:

- Order-level enrichment (header + customer + store):

  SELECT o.order_id, o.order_timestamp, o.total_amount, c.customer_id, c.first_name, c.last_name, s.store_id, s.store_name
  FROM raw.orders o
  LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
  LEFT JOIN raw.stores s ON o.store_id = s.store_id

- Line-item expansion (line + product metadata):

  SELECT oi.order_item_id, oi.order_id, oi.quantity, oi.unit_price, p.product_id, p.product_name, p.category
  FROM raw.order_items oi
  LEFT JOIN raw.products p ON oi.product_id = p.product_id

- Full OBT (order header + items + product + customer + store):

  SELECT oi.order_item_id, o.order_id, o.order_timestamp, c.customer_id, c.city, s.store_id, s.city as store_city, p.product_id, p.brand, oi.quantity, oi.unit_price, oi.line_amount
  FROM raw.order_items oi
  JOIN raw.orders o ON oi.order_id = o.order_id
  LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
  LEFT JOIN raw.products p ON oi.product_id = p.product_id
  LEFT JOIN raw.stores s ON o.store_id = s.store_id

Use these joins in Silver Business to denormalize into `obt_b` for faster analytics.

### 5.6 Insights and KPIs the dataset enables (and how to compute them)

High-value KPIs and queries that can be computed from these tables:

- Total revenue (time-window): SUM(order_items.line_amount) WHERE order_status = 'Completed' AND is_active = 'Y'
- Revenue by store: GROUP BY store_id / store_name on SUM(line_amount)
- Units sold by product: GROUP BY product_id SUM(quantity)
- Average order value (AOV): SUM(order_items.line_amount) / COUNT(DISTINCT orders.order_id)
- Repeat purchase rate: compute number of customers with >1 completed order in a window / total customers with at least one order
- RFM segmentation: use orders.order_timestamp and SUM(line_amount) per customer to compute recency, frequency, monetary
- Basket analysis: compute combinations of products by joining order_items by order_id and counting co-occurrence
- Conversion & cancellation rate: ratio of order_status = 'Cancelled' to total orders

Each KPI benefits from the Silver Business layer where denormalization reduces expensive joins at query time.

### 5.7 Known flaws, limitations, and recommended validations

Common dataset issues (present in many synthetic/seed datasets) and how to handle them:

- Missing referential integrity: CSVs may contain orphan FK values (e.g., order.customer_id not in customers). Add dbt tests: `unique`, `not_null`, and `relationships` to validate.
- Inconsistent formatting: email/phone may be non-normalized; standardize in Silver Technical.
- Price vs unit_price mismatch: catalog `price` may not equal `unit_price` due to promotions — record a rule or add a `discounts` table in the future.
- Incomplete timestamps: null `updated_timestamp` or timestamps in inconsistent timezones. Normalize timezone and guard with `coalesce(updated_timestamp, created_timestamp)` if needed.
- Soft-delete ambiguity: `is_active` values should be consistent; enforce allowed values ('Y','N') with dbt tests.
- No cost information: margins and profitability cannot be computed without `cost` or `cost_of_goods_sold` fields; consider adding `product_cost` to `products`.
- No promotions/pricing history: discounts are not modeled explicitly; use snapshots or add a `promotions` table for accurate price history.

Recommended dbt tests and checks:
- Generic tests: `not_null` on PKs, `unique` on PKs.
- Relationship tests: `orders.customer_id` references `customers.customer_id`; `order_items.order_id` references `orders.order_id`.
- Data-quality tests: `total_amount` ~ SUM(order_items.line_amount) within a tolerance; `line_amount` ≈ `quantity * unit_price`.
- Freshness tests: `dbt source freshness` on the Bronze layer to ensure timely ingestion.

### 5.8 Incremental ingestion and SCD patterns

- Bronze: keep raw CSVs appended as-is to preserve history.
- Silver Technical: apply incremental models to transform only new/changed rows using primary keys + `updated_timestamp` as the watermark.
- dbt Snapshots (Gold Dimensions): use `updated_timestamp` to capture SCD Type 2 history for customers, products, and stores.

Implementation notes:
- Use a `WHERE updated_timestamp > (select max(updated_timestamp) from model)` predicate for incremental dbt models.
- Store `ingestion_timestamp` in Bronze to track when a file was loaded.

### 5.9 Privacy and sensitivity considerations

- The sample data contains `email` and `phone` columns — treat them as sensitive in production. For public repositories, keep synthetic or anonymized values and avoid exposing real PII.
- Access control: ensure production datasets are protected and masked; consider column-level masking for `email`, `phone` and PII.

### 5.10 Suggested improvements and future additions

- Add `promotions` and `price_history` tables to model discounts and price changes explicitly.
- Add `store_hours` and `shift_logs` to link sales to labor scheduling.
- Add `product_cost` or `cost_center` to allow margin calculations.
- Add geolocation (latitude/longitude) for stores to support spatial analytics.
- Add `returns` and `refunds` tables if modeling reverse logistics and net revenue.
- Introduce referential integrity checks (FK constraints) if loading into a managed database (for test/dev only; do not add to Bronze if you want to preserve raw fidelity).

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

This project combines a realistic retail dataset with a modern data engineering stack to demonstrate a complete end-to-end analytics pipeline. The dataset is not just a set of CSV files—it represents a business model where customer behavior, store performance, product sales, and order details are connected through strong relational keys. By processing this data through Bronze, Silver, and Gold layers, the system transforms raw source records into reliable, testable, and analytics-ready business intelligence.

---

## Appendix A — Original "polish" version (commit 1e055692ac17d7b830b6119f18c195aea5d56e45)

Below is the full content of the earlier polished documentation that existed in commit 1e055692... (preserved here for history and comparison). This section reproduces that earlier document verbatim and is intentionally left read-only; do not edit this block unless you intend to preserve a new snapshot of the earlier version.

(START OF PRESERVED CONTENT)

# 📂 Enterprise Retail Analytics Transformation Pipeline: Detailed Component Documentation

This document provides a comprehensive, file-by-file breakdown of the project and explains how the components work together to build an end-to-end retail analytics pipeline. It is intended to help new developers, data engineers, and architects understand the architecture, dataset design, transformation flow, and operational logic of the system.

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

(END OF PRESERVED CONTENT)


---

## Change log

- Preserved earlier polished content from commit `1e055692ac17d7b830b6119f18c195aea5d56e45` in Appendix A.
- Appended Appendix A to ensure the earlier doc is retained alongside the expanded dataset documentation.

---

## Notes

If you'd like Appendix A kept in a separate file instead (for clarity), I can move it to `docs/PROJECT_DOCS_v1e055692.md` and keep `PROJECT_DOCS.md` focused only on the latest merged content. Reply with your preference and I'll update accordingly.
