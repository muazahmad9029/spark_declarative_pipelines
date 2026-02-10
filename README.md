## Transformations: Bronze–Silver–Gold Data Lakehouse

This repository contains a simple lakehouse-style transformation project that implements **bronze, silver, and gold layers** on top of Delta tables using **PySpark data pipelines** (via `pyspark.pipelines` decorators).

The code is organized around a typical medallion architecture:

- **Bronze layer (`bronze/ingestion.py`)**: streaming ingestion of raw CSV files from Azure Data Lake Storage (ADLS) into bronze Delta tables using **Auto Loader (cloudFiles)**.
- **Silver layer (`silver/transformations.py`)**: cleaning, standardization, type casting, data quality expectations, and derived fields for the main entities.
- **Gold layer (`gold/dimension_modelling.py`)**: star-schema style **dimensions and facts**, joining the cleaned silver tables to build analytics-ready models.

---

### Project Structure

- **`bronze/ingestion.py`**
  - Generic `read_csv_autoloader(source_path, schema_location)` helper that:
    - Uses `spark.readStream.format("cloudFiles")` to read CSVs from ADLS.
    - Enables schema inference and schema evolution (`addNewColumns`).
    - Tracks schema metadata in a dedicated schema location.
    - Adds an `_ingest_ts` ingestion timestamp column.
  - Bronze streaming tables (all decorated with `@dp.table`):
    - `client_demo.bronze.beneficiary_satisfaction`
    - `client_demo.bronze.performance_measurements`
    - `client_demo.bronze.training_programs`
    - `client_demo.bronze.performance_indicators`
    - `client_demo.bronze.public_entities`

- **`silver/transformations.py`**
  - Contains **cleaned silver tables** with expectations via `@dp.expect` / `@dp.expect_or_drop` and `pyspark.sql.functions`:
    - `client_demo.silver.clean_public_entities`
      - Standardizes and trims text attributes, normalizes types, enforces non-null `entity_id`.
    - `client_demo.silver.clean_performance_indicators`
      - Standardizes indicator attributes, numeric target/baseline values, and enforces non-null `indicator_id`.
    - `client_demo.silver.clean_performance_measurements`
      - Cleans measurement values, derives `year`, `month`, `yyyymm`, recalculates variance percentage, and flags `on_target_flag`.
    - `client_demo.silver.clean_beneficiary_satisfaction`
      - Cleans survey responses, clamps scores to 0–10, normalizes boolean recommendation responses, and derives time fields.
    - `client_demo.silver.clean_training_programs`
      - Cleans training data, derives duration, completion rate, total cost, and time attributes.

- **`gold/dimension_modelling.py`**
  - **Dimensions**
    - `client_demo.gold.dim_public_entities`
      - Latest version per `entity_id` based on `_ingest_timestamp`.
    - `client_demo.gold.dim_performance_indicators`
      - Latest version per `indicator_id` based on `_ingest_timestamp`.
  - **Facts**
    - `client_demo.gold.fact_performance_measurements`
      - Joins clean measurements with the public-entity and indicator dimensions.
    - `client_demo.gold.fact_beneficiary_satisfaction`
      - Joins clean satisfaction surveys with the public-entity dimension.
    - `client_demo.gold.fact_training_programs`
      - Joins clean training programs with the public-entity dimension.

All tables are tagged with table properties indicating **quality** (`bronze` / `silver` / `gold`) and **layer**, and enable Delta optimizations like **Change Data Feed** and auto-optimization.

---

### Prerequisites

- A Databricks environment that supports:
  - `pyspark` and the `pyspark.pipelines` package (or Databricks Workflows/Delta Live Tables–style decorators, depending on your environment).
  - Delta Lake with Change Data Feed enabled.
  - Auto Loader (`cloudFiles`) for streaming ingestion.
- Access to **Azure Data Lake Storage (ADLS)** with the raw CSV files and schema locations configured.

You will need to replace the placeholder ADLS paths in `bronze/ingestion.py`:

```python
BASE = "adls_path_for_csv_files"
SCHEMA_BASE = "adls_path_for_schema_files"
```

with your actual container/folder paths.

---

### How to Run

The exact way you run these pipelines depends on your orchestration environment (e.g. Databricks Jobs / Delta Live Tables / custom PySpark jobs), but the logical flow is:

1. **Configure ADLS paths**
   - Edit `bronze/ingestion.py` and set `BASE` and `SCHEMA_BASE` to valid ADLS locations.
   - Ensure your Spark cluster/service has the necessary credentials and mounts to access those paths.

2. **Deploy the project code**
   - Package or upload this repository so that your Spark environment can import:
     - `bronze.ingestion`
     - `silver.transformations`
     - `gold.dimension_modelling`

3. **Create / refresh the Bronze tables**
   - Execute the functions in `bronze/ingestion.py` (or configure them as streaming/Auto Loader pipelines) to populate:
     - `client_demo.bronze.beneficiary_satisfaction`
     - `client_demo.bronze.performance_measurements`
     - `client_demo.bronze.training_programs`
     - `client_demo.bronze.performance_indicators`
     - `client_demo.bronze.public_entities`

4. **Run the Silver transformations**
   - Execute the silver-layer functions in `silver/transformations.py` to build the cleaned tables:
     - `client_demo.silver.clean_public_entities`
     - `client_demo.silver.clean_performance_indicators`
     - `client_demo.silver.clean_performance_measurements`
     - `client_demo.silver.clean_beneficiary_satisfaction`
     - `client_demo.silver.clean_training_programs`

5. **Run the Gold modelling**
   - Execute the gold-layer functions in `gold/dimension_modelling.py` to build the dimensions and facts:
     - `client_demo.gold.dim_public_entities`
     - `client_demo.gold.dim_performance_indicators`
     - `client_demo.gold.fact_performance_measurements`
     - `client_demo.gold.fact_beneficiary_satisfaction`
     - `client_demo.gold.fact_training_programs`

In a Databricks / DLT-like setup, these functions would typically be registered as pipeline tables, and the framework will handle graph execution order based on table references.

---

### Data Quality & Modelling Highlights

- **Data quality**
  - `@dp.expect_or_drop` used to enforce non-null keys (e.g. `entity_id`, `indicator_id`, `measurement_id`, `survey_id`, `training_id`).
  - Additional `@dp.expect` used for critical fields such as dates.
  - Type casting and trimming for consistent string and numeric fields.
- **Derived metrics**
  - Time attributes: `year`, `month`, `yyyymm` for both measurements and surveys/training.
  - Recomputed variance percentage and on-target flag in measurements.
  - Clamped scores (0–10) for survey satisfaction.
  - Duration, completion rate, and total cost for training programs.
- **Star schema**
  - Slowly-changing-like logic using ingest timestamp and windowing to get the latest version per `entity_id` / `indicator_id`.
  - Facts conformed to shared dimensions for consistent analytics across KPIs, satisfaction, and training.

---
