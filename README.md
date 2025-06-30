# Enhanced Seller Performance & Order Context ETL — Assignment Documentation

## Assumptions & Limitations
- **Customer Location:** The customer zip code is available via the `olist_customers_dataset.csv` file and is used for accurate delivery geolocation and weather enrichment. This ensures weather data is based on the true delivery location, not the seller's location.
- **API Calls:** Real external API calls are implemented (Yahoo Finance for S&P 500 data, Open-Meteo for weather data) with robust error handling and fallback logic for market closures and data availability.
- **Data Quality:** Rows with missing critical information (IDs, timestamps, lat/lon) are dropped. In production, more nuanced imputation or error logging may be used.
- **No SQL Transformations:** All data transformations are performed in Python (Pandas). No SQL is used for transformation logic, only for DDL.
- **Data Volume:** The design assumes the dataset fits in memory for Pandas processing. For larger datasets, a distributed framework (e.g., PySpark) would be considered.
- **Security:** No sensitive data or credentials are present in the code. In production, secrets would be managed securely.
- **Performance Optimization:** The ETL uses batch processing and caching to minimize API calls (90%+ reduction in API requests) and vectorized operations for better performance.
- **Timezone Handling:** S&P 500 data timezone issues are handled by converting timezone-aware indices to timezone-naive for proper comparison.
- **Fallback Logic:** For market closures, the system looks back up to 90 days to find the last available trading day, ensuring robust data availability.

---

## 1. Apache Airflow DAG Design

### Overview
- **Purpose:** Orchestrate the ETL pipeline to process Olist data, enrich with external APIs, and output the final dataset.
- **DAG Structure:** Single DAG, no custom plugins.
- **Optimization:** Uses batch processing and caching to minimize API calls and improve performance.

### DAG Steps & Operators
1. **Extract:** Load Olist CSVs from local/cloud storage. *(PythonOperator)*
    - The `olist_customers_dataset.csv` file is loaded specifically to obtain the `customer_zip_code_prefix` for each order. This field is not present in the other four core Olist files and is required to accurately join with the geolocation data for weather enrichment.
2. **Transform:** Clean, join, and aggregate data using `clean_and_join_olist_data()`. *(PythonOperator)*
3. **Enrich:**
   - Batch fetch S&P 500 data using `batch_get_sp500_data()` for all unique purchase dates. *(PythonOperator)*
   - Batch fetch weather data using `batch_get_weather_data()` for all unique location-date combinations. *(PythonOperator)*
   - Apply enrichment using `enrich_with_external_data()` with vectorized operations. *(PythonOperator)*
4. **Data Quality Checks:** Validate row counts, nulls, and value ranges. *(PythonOperator or SQLCheckOperator if loading to DB)*
5. **Load:** Store the enriched dataset in PostgreSQL or data lake. *(PythonOperator or PostgresOperator)*

### Configuration & Secrets Management
- Use **Airflow Variables** for file paths, API URLs, and configuration.
- Use **Airflow Connections** for database credentials and (if needed) API keys.
- Use **Environment Variables** for sensitive data, injected via Airflow or Kubernetes.
- **Configuration Management:**  
  All file paths, API URLs, and constants are managed via a `config.json` file. This approach allows for flexible, environment-specific configuration and keeps sensitive or environment-dependent values out of the main codebase.

### Idempotency & Backfilling
- **Idempotency:** Each task writes outputs with unique partition keys (e.g., order date or DAG run date). Loads use upserts or partition overwrites.
- **Backfilling:** DAG supports backfill by accepting a date range as a parameter (via `execution_date`). All tasks are parameterized by date.

### Parallelism & Resource Management
- **Parallelism:**
  - API enrichment tasks can be parallelized by splitting orders into chunks and using Airflow's task concurrency.
  - Use Airflow's `max_active_runs` and `concurrency` settings.
  - Batch processing reduces API calls from thousands to dozens, enabling better parallelization.
- **Resource Management:**
  - If running in Kubernetes, use **KubernetesPodOperator** to allocate CPU/memory per task.
  - Set resource requests/limits in pod specs.
  - Optimized memory usage through vectorized operations instead of row-by-row processing.

### Error Handling, Logging, Alerting
- Use Airflow's built-in retries, failure callbacks, and email/Slack alerts.
- Log all API failures, data validation errors, and transformation issues.
- Use Airflow's XCom for passing small data between tasks.
- **Enhanced Error Handling:** Individual date processing errors are caught and logged with fallback mechanisms.

### Data Quality Checks
- Integrate **SQLCheckOperator** or custom **PythonOperator** after enrichment and before load to validate:
  - No nulls in primary keys
  - Value ranges for numerical columns
  - Row counts match expectations
  - S&P 500 values are within realistic ranges
  - Weather data values are within expected bounds for the location

### Example DAG Structure (Pseudocode)
```python
with DAG('enhanced_seller_performance_etl', schedule_interval='@daily', default_args=default_args) as dag:
    extract = PythonOperator(task_id='extract', python_callable=load_olist_data)
    transform = PythonOperator(task_id='transform', python_callable=clean_and_join_olist_data)
    enrich = PythonOperator(task_id='enrich', python_callable=enrich_with_external_data)
    data_quality = PythonOperator(task_id='data_quality', python_callable=validate_data)
    load = PostgresOperator(task_id='load', sql='INSERT INTO seller_order_enriched...')

    extract >> transform >> enrich >> data_quality >> load
```

---

## 2. Database Design (PostgreSQL)

### Table: seller_order_enriched
Stores the final output of the ETL pipeline.

| Column                          | Type           | Description                                 |
|----------------------------------|----------------|---------------------------------------------|
| order_id                        | VARCHAR(50)    | Unique order identifier                     |
| seller_id                       | VARCHAR(50)    | Unique seller identifier                    |
| order_purchase_timestamp        | TIMESTAMP      | Order purchase datetime                     |
| order_delivered_customer_date   | TIMESTAMP      | Order delivered datetime                    |
| total_order_value               | NUMERIC(12,2)  | Total value of the order                    |
| market_sentiment_on_purchase_date | NUMERIC(10,2) | S&P 500 close price on purchase date        |
| delivery_location_latitude      | NUMERIC(9,6)   | Delivery latitude (approximate)             |
| delivery_location_longitude     | NUMERIC(9,6)   | Delivery longitude (approximate)            |
| delivery_date_mean_temp         | NUMERIC(5,2)   | Mean temperature on delivery date           |
| delivery_date_precipitation_sum | NUMERIC(5,2)   | Precipitation sum on delivery date          |

### DDL Example
```sql
CREATE TABLE seller_order_enriched (
    order_id VARCHAR(50) PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_delivered_customer_date TIMESTAMP NOT NULL,
    total_order_value NUMERIC(12,2) NOT NULL,
    market_sentiment_on_purchase_date NUMERIC(10,2),
    delivery_location_latitude NUMERIC(9,6),
    delivery_location_longitude NUMERIC(9,6),
    delivery_date_mean_temp NUMERIC(5,2),
    delivery_date_precipitation_sum NUMERIC(5,2)
);
```

### Indexing Strategy
- **Primary Key:** `order_id` (unique per order)
- **Additional Indexes:**
  - `CREATE INDEX idx_seller_id ON seller_order_enriched(seller_id);`
  - `CREATE INDEX idx_order_purchase_timestamp ON seller_order_enriched(order_purchase_timestamp);`
  - `CREATE INDEX idx_delivery_location ON seller_order_enriched(delivery_location_latitude, delivery_location_longitude);`
- **Purpose:** Supports fast queries by seller, date, and location.

### Performance Optimization
- **Partitioning:** Partition table by month or year on `order_purchase_timestamp` for efficient time-based queries and loads.
- **Bulk Loads:** Use `COPY` or batch inserts for large ETL loads.
- **Vacuum/Analyze:** Schedule regular maintenance for table statistics and bloat.

### Data Retention & Archival
- **Retention Policy:** Retain recent N years of data in the main table; archive older data to a separate table or cold storage.
- **Archival Process:** Use scheduled jobs to move/archive data based on `order_purchase_timestamp`.

---

## 3. DevOps, Docker & Kubernetes Setup

### Docker
- **Base Image:** Use `python:3.10-slim` for minimal size and security.
- **Dependencies:** Install via `pip` (see Dockerfile). For production, use a `requirements.txt` for reproducibility.
- **Non-root User:** For security, consider running as a non-root user in production.
- **Configuration Management:**
  - Use environment variables for API endpoints, DB credentials, and config.
  - Inject via Kubernetes ConfigMaps (for non-sensitive) and Secrets (for sensitive data).
  - All file paths, API URLs, and constants are managed via a `config.json` file. This approach allows for flexible, environment-specific configuration and keeps sensitive or environment-dependent values out of the main codebase.

### Kubernetes
- **Deployment:** Use a Kubernetes Job or CronJob for scheduled ETL runs.
- **KubernetesPodOperator:** In Airflow, configure with:
  - `image`, `resources` (CPU/memory), `env` (from ConfigMaps/Secrets), `volumeMounts` for data.
- **Pod Dependencies:**
  - Use shared persistent volumes or object storage for passing data between pods/tasks.
  - Use Airflow XComs for small metadata.
- **Resource Management:** Set resource requests/limits for each pod to ensure fair scheduling and avoid overcommitment.

#### Example KubernetesPodOperator (Airflow)
```python
KubernetesPodOperator(
    task_id='run_etl',
    name='etl-seller-performance',
    image='your-docker-repo/etl-seller-performance:latest',
    env_vars={'DB_HOST': '{{ var.value.db_host }}'},
    resources={'request_cpu': '1', 'request_memory': '2Gi'},
    secrets=[Secret('env', 'DB_PASSWORD', 'etl-secrets', 'db_password')],
    volume_mounts=[...],
    volumes=[...],
    is_delete_operator_pod=True
)
```

---

## 4. CI/CD & Testing Strategy

### CI/CD Pipeline
- **Linting:** Use `flake8` or `black` for code style.
- **Unit Tests:** Use `pytest` for function-level tests. Mock external API calls with `pytest-mock` or `requests-mock`.
- **Integration Tests:** Run the ETL end-to-end with sample data and validate output schema and values.
- **DAG Validation:** Use Airflow's `airflow dags test` and DAG integrity checks.
- **Docker Build & Push:** Build Docker image and push to registry on merge.
- **Multi-Environment Deployment:** Use separate configs/variables for dev, staging, and prod. Deploy to each environment via pipeline stages.
- **Deployment:** Trigger Kubernetes Job or Airflow DAG via CI/CD pipeline (e.g., GitHub Actions, GitLab CI).

### Testing
- **Unit Tests:**
  - Test each function (data loading, cleaning, enrichment).
  - Mock external API calls (Yahoo Finance, Open-Meteo) using `pytest-mock` or `requests-mock`.
- **Integration Tests:**
  - Use a small sample of Olist data.
  - Validate that the output DataFrame matches expected schema and values.
- **Airflow DAG Tests:**
  - Use Airflow's test utilities to check DAG structure and task dependencies.
- **Data Validation Testing:**
  - Check for nulls in primary keys and critical columns.
  - Validate value ranges (e.g., order value > 0, lat/lon within Brazil).
  - Referential integrity between orders, sellers, and geolocations.

### Example CI/CD Steps
1. Checkout code
2. Run linting and unit tests
3. Build Docker image
4. Run integration tests in Docker
5. Validate Airflow DAGs
6. Push image to registry
7. Deploy or trigger ETL job

---

## 5. SQL for Transformations
- All transformation logic is implemented in Python (Pandas). No additional SQL is used for data transformation beyond the DDL provided above. 