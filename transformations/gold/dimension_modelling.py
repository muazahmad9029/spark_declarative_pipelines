# =============================================================================
# GOLD LAYER: Star Schema (Dimensions + Facts) + KPI Aggregations
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# GOLD DIM 1: Public Entities
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.gold.dim_public_entities",
    comment="Dimension: Public entities (sourced from silver_clean_public_entities).",
    table_properties={
        "quality":"gold",
        "layer":"gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_dim_public_entities():
    s = spark.table("client_demo.silver.clean_public_entities")

    w = Window.partitionBy("entity_id").orderBy(F.col("_ingest_timestamp").desc())

    return (
        s.withColumn("rn", F.row_number().over(w))
         .filter(F.col("rn") == 1)
         .drop("rn", "_ingest_timestamp")
    )
# -----------------------------------------------------------------------------
# GOLD DIM 2: Performance Indicators
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.gold.dim_performance_indicators",
    comment="Dimension: Performance indicators (sourced from silver_clean_performance_indicators).",
    table_properties={
        "quality":"gold",
        "layer":"gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_dim_performance_indicators():
    s = spark.table("client_demo.silver.clean_performance_indicators")

    w = Window.partitionBy("indicator_id").orderBy(F.col("_ingest_timestamp").desc())

    return (
        s.withColumn("rn", F.row_number().over(w))
         .filter(F.col("rn") == 1)
         .drop("rn", "_ingest_timestamp")
    )

# -----------------------------------------------------------------------------
# GOLD FACT 1: Performance Measurements (conformed + enriched with dims)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.gold.fact_performance_measurements",
    comment="Fact: Performance measurements conformed to dims + KPI fields.",
    table_properties={
        "quality":"gold",
        "layer":"gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_fact_performance_measurements():
    m = spark.table("client_demo.silver.clean_performance_measurements")
    ind = spark.table("client_demo.gold.dim_performance_indicators")
    ent = spark.table("client_demo.gold.dim_public_entities")

    return (
        m.join(ind, on="indicator_id", how="left")
         .join(ent, on="entity_id", how="left")
    )


# -----------------------------------------------------------------------------
# GOLD FACT 2: Beneficiary Satisfaction (conformed with entity dim)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.gold.fact_beneficiary_satisfaction",
    comment="Fact: Beneficiary satisfaction surveys conformed with entity attributes.",
    table_properties={
        "quality":"gold",
        "layer":"gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_fact_beneficiary_satisfaction():
    s = spark.table("client_demo.silver.clean_beneficiary_satisfaction")
    ent = spark.table("client_demo.gold.dim_public_entities")
    return s.join(ent, on="entity_id", how="left")


# -----------------------------------------------------------------------------
# GOLD FACT 3: Training Programs (conformed with entity dim)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.gold.fact_training_programs",
    comment="Fact: Training programs conformed with entity attributes.",
    table_properties={
        "quality":"gold",
        "layer":"gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_fact_training_programs():
    t = spark.table("client_demo.silver.clean_training_programs")
    ent = spark.table("client_demo.gold.dim_public_entities")
    return t.join(ent, on="entity_id", how="left")

