# =============================================================================
# SILVER LAYER: Cleaned + Standardized + Data Quality 
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Silver Clean 1: Public Entities (cleaned)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.silver.clean_public_entities",
    comment="Cleaned public_entities (standardization + dedupe + DQ).",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("entity_id_not_null", "entity_id IS NOT NULL")
def silver_clean_public_entities():
        df = spark.table("client_demo.bronze.public_entities")
        cleaned = (
            df.select(
                F.col("entity_id").cast("string").alias("entity_id"),
                F.trim(F.col("entity_name_en")).alias("entity_name_en"),
                F.trim(F.col("sector")).alias("sector"),
                F.trim(F.col("category")).alias("entity_category"),
                F.col("establishment_date").alias("establishment_date"),
                F.trim(F.col("status")).alias("entity_status"),
                F.trim(F.col("region")).alias("region"),
                F.lower(F.trim(F.col("contact_email"))).alias("contact_email"),
                F.col("contact_phone").cast("string").alias("contact_phone"),
                F.col("_ingest_ts") .alias("_ingest_timestamp")
            )
        )

        return cleaned


    # -----------------------------------------------------------------------------
    # Silver Clean 2: Performance Indicators
    # -----------------------------------------------------------------------------
@dp.table(
        name="client_demo.silver.clean_performance_indicators",
        comment="Cleaned performance_indicators (standardization + dedupe + DQ).",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
    )
@dp.expect_or_drop("indicator_id_not_null", "indicator_id IS NOT NULL")
def silver_clean_performance_indicators():
        df = spark.table("client_demo.bronze.performance_indicators")

        cleaned = (
            df.select(
                F.col("indicator_id").cast("string").alias("indicator_id"),
                F.trim(F.col("indicator_code")).alias("indicator_code"),
                F.trim(F.col("indicator_name_en")).alias("indicator_name_en"),
                F.trim(F.col("category")).alias("indicator_category"),
                F.trim(F.col("subcategory")).alias("indicator_subcategory"),
                F.trim(F.col("unit")).alias("unit"),
                F.trim(F.col("measurement_frequency")).alias("measurement_frequency"),
                F.col("target_value_2024").cast("double").alias("target_value_2024"),
                F.col("baseline_value_2023").cast("double").alias("baseline_value_2023"),
                F.trim(F.col("status")).alias("indicator_status"),
                F.col("entity_id").cast("string").alias("owner_entity_id"),
                F.trim(F.col("vision_2030_program")).alias("vision_2030_program"),
                F.col("_ingest_ts") .alias("_ingest_timestamp")
            )
        )

        return cleaned


# -----------------------------------------------------------------------------
# Silver Clean 3: Performance Measurements 
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.silver.clean_performance_measurements",
    comment="Cleaned performance_measurements with derived time fields + variance calc (no joins).",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("measurement_id_not_null", "measurement_id IS NOT NULL")
@dp.expect("measurement_date_not_null", "measurement_date IS NOT NULL")
def silver_clean_performance_measurements():
    m = spark.table("client_demo.bronze.performance_measurements")

    cleaned = (
        m.select(
            F.col("measurement_id").cast("string").alias("measurement_id"),
            F.col("indicator_id").cast("string").alias("indicator_id"),
            F.col("entity_id").cast("string").alias("entity_id"),
            F.col("measurement_date").alias("measurement_date"),
            F.trim(F.col("measurement_period")).alias("measurement_period"),
            F.col("actual_value").cast("double").alias("actual_value"),
            F.col("target_value").cast("double").alias("target_value"),
            F.col("baseline_value").cast("double").alias("baseline_value"),
            F.col("variance_percentage").cast("double").alias("variance_percentage"),
            F.trim(F.col("status")).alias("status"),
            F.trim(F.col("performance_level")).alias("performance_level"),
            F.trim(F.col("trend")).alias("trend"),
            F.trim(F.col("data_source")).alias("data_source"),
            F.col("verified_date").alias("verified_date"),
            F.trim(F.col("verified_by")).alias("verified_by"),
            F.trim(F.col("notes")).alias("notes")
        )
        .withColumn("year", F.year("measurement_date"))
        .withColumn("month", F.month("measurement_date"))
        .withColumn("yyyymm", F.date_format("measurement_date", "yyyy-MM"))
        .withColumn(
            "variance_pct_calc",
            F.when(
                (F.col("target_value").isNotNull()) & (F.col("target_value") != 0) & (F.col("actual_value").isNotNull()),
                (F.col("actual_value") - F.col("target_value")) / F.col("target_value") * 100
            ).otherwise(F.col("variance_percentage"))
        )
        .withColumn(
            "on_target_flag",
            F.when(
                (F.col("actual_value").isNotNull()) & (F.col("target_value").isNotNull()),
                F.col("actual_value") >= F.col("target_value")
            ).otherwise(F.lit(None))
        )
    )

    return cleaned


# -----------------------------------------------------------------------------
# Silver Clean 4: Beneficiary Satisfaction (cleaned only)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.silver.clean_beneficiary_satisfaction",
    comment="Cleaned beneficiary satisfaction surveys with DQ + time fields (no entity join).",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("survey_id_not_null", "survey_id IS NOT NULL")
@dp.expect("survey_date_not_null", "survey_date IS NOT NULL")
def silver_clean_beneficiary_satisfaction():
    s = spark.table("client_demo.bronze.beneficiary_satisfaction")

    recommend = F.lower(F.trim(F.col("would_recommend")))
    recommend_flag = (
        F.when(recommend.isin("yes", "y", "true", "1"), F.lit(True))
         .when(recommend.isin("no", "n", "false", "0"), F.lit(False))
         .otherwise(F.lit(None))
    )

    def clamp(colname):
        c = F.col(colname).cast("double")
        return F.when(c.isNull(), None).otherwise(F.greatest(F.lit(0.0), F.least(F.lit(10.0), c)))

    cleaned = (
        s.select(
            F.col("survey_id").cast("string").alias("survey_id"),
            F.col("entity_id").cast("string").alias("entity_id"),
            F.col("survey_date").alias("survey_date"),
            F.col("beneficiary_id").cast("string").alias("beneficiary_id"),
            F.trim(F.col("beneficiary_type")).alias("beneficiary_type"),
            F.trim(F.col("service_category")).alias("service_category"),
            clamp("overall_satisfaction_score").alias("overall_satisfaction_score"),
            clamp("service_quality_score").alias("service_quality_score"),
            clamp("ease_of_access_score").alias("ease_of_access_score"),
            clamp("response_time_score").alias("response_time_score"),
            clamp("communication_score").alias("communication_score"),
            clamp("digital_experience_score").alias("digital_experience_score"),
            recommend_flag.alias("would_recommend_flag"),
            F.trim(F.col("additional_comments")).alias("additional_comments"),
            F.trim(F.col("response_channel")).alias("response_channel"),
            F.trim(F.col("survey_type")).alias("survey_type")
        )
        .withColumn("year", F.year("survey_date"))
        .withColumn("month", F.month("survey_date"))
        .withColumn("yyyymm", F.date_format("survey_date", "yyyy-MM"))
    )

    return cleaned


# -----------------------------------------------------------------------------
# Silver Clean 5: Training Programs (cleaned only)
# -----------------------------------------------------------------------------
@dp.table(
    name="client_demo.silver.clean_training_programs",
    comment="Cleaned training programs with derived metrics (no entity join).",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("training_id_not_null", "training_id IS NOT NULL")
@dp.expect("start_date_not_null", "start_date IS NOT NULL")
def silver_clean_training_programs():
    t = spark.table("client_demo.bronze.training_programs")

    cleaned = (
        t.select(
            F.col("training_id").cast("string").alias("training_id"),
            F.col("entity_id").cast("string").alias("entity_id"),
            F.trim(F.col("training_name_en")).alias("training_name_en"),
            F.trim(F.col("training_type")).alias("training_type"),
            F.trim(F.col("delivery_method")).alias("delivery_method"),
            F.col("start_date").alias("start_date"),
            F.col("end_date").alias("end_date"),
            F.col("duration_hours").cast("double").alias("duration_hours"),
            F.trim(F.col("instructor_name")).alias("instructor_name"),
            F.trim(F.col("trainer_organization")).alias("trainer_organization"),
            F.col("number_of_participants").cast("double").alias("participants"),
            F.col("number_completed").cast("double").alias("completed"),
            F.col("cost_per_participant").cast("double").alias("cost_per_participant"),
            F.col("overall_rating").cast("double").alias("overall_rating"),
            F.trim(F.col("category")).alias("training_category"),
            F.trim(F.col("topic_area")).alias("topic_area"),
            F.trim(F.col("status")).alias("training_status"),
            F.trim(F.col("location")).alias("location")
        )
        .withColumn("training_days", F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1))
        .withColumn(
            "completion_rate",
            F.when(F.col("participants") > 0, F.col("completed") / F.col("participants")).otherwise(F.lit(None))
        )
        .withColumn(
            "total_training_cost",
            F.when(
                (F.col("participants").isNotNull()) & (F.col("cost_per_participant").isNotNull()),
                F.col("participants") * F.col("cost_per_participant")
            ).otherwise(F.lit(None))
        )
        .withColumn("year", F.year("start_date"))
        .withColumn("month", F.month("start_date"))
        .withColumn("yyyymm", F.date_format("start_date", "yyyy-MM"))
    )

    return cleaned
