# BRONZE LAYER: Raw Data Ingestion from Azure Data Lake Storage
# ============================================================================
# Purpose: Ingest CSV files from ADLS using Auto Loader (cloudFiles)
# Target Schema: client.bronze
# Pattern: Streaming ingestion with automatic schema inference and evolution
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import input_file_name, current_timestamp

# Azure Data Lake Storage paths
BASE = "adls_path_for_csv_files"
SCHEMA_BASE = "adls_path_for_schema_files"

def read_csv_autoloader(source_path: str, schema_location: str):
    """
    Generic Auto Loader function for CSV ingestion.
    
    Features:
    - Automatic schema inference and evolution
    - Incremental processing of new files
    - Schema tracking in dedicated location
    
    Args:
        source_path: ADLS path containing CSV files
        schema_location: Path to store inferred schema metadata
    
    Returns:
        Streaming DataFrame with inferred schema
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source_path)
        .withColumn("_ingest_ts", current_timestamp())
    )

# -----------------------------------------------------------------------------
# Bronze Table 1: Beneficiary Satisfaction Surveys
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.bronze.beneficiary_satisfaction",
    comment="Bronze ingestion of beneficiary_satisfaction CSVs via Auto Loader.",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_beneficiary_satisfaction():
    """Raw beneficiary satisfaction survey responses."""
    return read_csv_autoloader(
        f"{BASE}/beneficiary_satisfaction",
        f"{SCHEMA_BASE}/beneficiary_satisfaction"
    )

# -----------------------------------------------------------------------------
# Bronze Table 2: Performance Measurements
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.bronze.performance_measurements",
    comment="Bronze ingestion of performance_measurements CSVs via Auto Loader.",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_performance_measurements():
    """Raw performance measurement records from entities."""
    return read_csv_autoloader(
        f"{BASE}/performance_measurements",
        f"{SCHEMA_BASE}/performance_measurements"
    )

# -----------------------------------------------------------------------------
# Bronze Table 3: Training Programs
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.bronze.training_programs",
    comment="Bronze ingestion of training_programs CSVs via Auto Loader.",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_training_programs():
    """Raw training program records and participant data."""
    return read_csv_autoloader(
        f"{BASE}/training_programs",
        f"{SCHEMA_BASE}/training_programs"
    )

# -----------------------------------------------------------------------------
# Bronze Table 4: Performance Indicators (Reference Data)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.bronze.performance_indicators",
    comment="Bronze ingestion of performance_indicators CSVs via Auto Loader.",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_performance_indicators():
    """Raw performance indicator definitions and targets."""
    return read_csv_autoloader(
        f"{BASE}/performance_indicators",
        f"{SCHEMA_BASE}/performance_indicators"
    )

# -----------------------------------------------------------------------------
# Bronze Table 5: Public Entities (Reference Data)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.bronze.public_entities",
    comment="Bronze ingestion of public_entities CSVs via Auto Loader.",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_public_entities():
    """Raw public entity master data and organizational information."""
    return read_csv_autoloader(
        f"{BASE}/public_entities",
        f"{SCHEMA_BASE}/public_entities"
    )