# ============================================================
# ETL REAL ESTATE: SILVER → GOLD FACTS & DIMENSIONS (DW Schema)
# Purpose: Build dimensional model for analytics warehouse
# Spark 3.5.1 + Iceberg + Nessie + MinIO
# ============================================================

from __future__ import annotations

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException
from src.common.spark import build_spark

# ============================================================
# SPARK SESSION + ICEBERG CATALOG
# ============================================================
spark = build_spark("Lakehouse-Silver-To-Gold-DW-Schema")

# ============================================================
# MEMORY & PERFORMANCE OPTIMIZATION
# ============================================================
# Increase shuffle partitions for better memory distribution
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Enable AQE (Adaptive Query Execution) for smart optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Increase broadcast join threshold (avoid OOM on small dims)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Memory optimization for cross joins
spark.conf.set("spark.sql.crossJoin.enabled", "true")  # Allow cross joins explicitly

print("✓ Memory optimization settings applied:")
print(f"  • Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  • Adaptive Query Execution: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  • Broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

# ============================================================
# CẤU HÌNH BẢNG
# ============================================================
SILVER_TABLE = os.getenv("SILVER_TABLE", "lakehouse.silver.chotot_cleaned")
GOLD_NAMESPACE = "lakehouse.gold"

DIM_TIME = f"{GOLD_NAMESPACE}.dim_time"
DIM_LOCATION = f"{GOLD_NAMESPACE}.dim_location"
DIM_PROPERTY = f"{GOLD_NAMESPACE}.dim_property"
DIM_SELLER = f"{GOLD_NAMESPACE}.dim_seller"
DIM_METRO_STATION = f"{GOLD_NAMESPACE}.dim_metro_station"

# Fact tables - Core
FACT_PROPERTIES = f"{GOLD_NAMESPACE}.fact_properties"

# Fact tables - Aggregations (Conformed to use dimension keys)
FACT_PRICE_AREA_TIME = f"{GOLD_NAMESPACE}.fact_price_area_time"
FACT_SELLER_TIME = f"{GOLD_NAMESPACE}.fact_seller_time"
FACT_METRO_ANALYSIS = f"{GOLD_NAMESPACE}.fact_metro_analysis"
FACT_SEARCH_TRENDS_MONTHLY = f"{GOLD_NAMESPACE}.fact_search_trends_monthly"

# Dimension tables - Search
DIM_SEARCH_CATEGORY = f"{GOLD_NAMESPACE}.dim_search_category"

# Source tables - Silver Layer
METRO_STATIONS = "lakehouse.silver.metro_stations"
GGTREND_SILVER = os.getenv(
    "GGTREND_SILVER_TABLE",
    "lakehouse.silver.ggtrend_daily"
)

# ============================================================
# HELPER FUNCTIONS
# ============================================================


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def read_silver() -> Optional[DataFrame]:
    print(f"=== GOLD DW: Reading Silver table: {SILVER_TABLE} ===")

    # Compatibility: Bronze→Silver writes to lakehouse.silver.chotot_cleaned by default.
    # If the user overrides SILVER_TABLE, respect it. Otherwise, fall back to the
    # old table name only when the new default table does not exist.
    candidate_tables = [SILVER_TABLE]
    legacy_table = "lakehouse.silver.cleaned"
    if SILVER_TABLE != legacy_table:
        candidate_tables.append(legacy_table)

    for table_name in candidate_tables:
        try:
            df = spark.table(table_name)
            count = df.count()
            print(f"✓ Silver table read: {table_name} ({count:,} rows)")
            return df
        except AnalysisException as e:
            print(f"⚠️  Cannot read Silver table {table_name}: {e}")

    print("❌ GOLD DW: No readable Silver table found")
    return None



# ============================================================
# DIMENSION TABLES
# ============================================================

def build_dim_time():
    """
    Tạo calendar dimension: 2024-2027
    """
    print("\n=== BUILD DIM_TIME ===")
    
    start_date = F.to_date(F.lit("2024-01-01"))
    end_date = F.to_date(F.lit("2027-12-31"))
    
    df = (
        spark
        .range(1)
        .select(F.sequence(start_date, end_date).alias("d"))
        .select(F.explode("d").alias("d"))
    )
    
    df_time = df.select(
        F.date_format(F.col("d"), "yyyyMMdd").cast("int").alias("time_key"),
        F.col("d").alias("date"),
        F.year(F.col("d")).alias("year"),
        F.month(F.col("d")).alias("month"),
        F.date_format(F.col("d"), "MMMM").alias("month_name"),
        F.quarter(F.col("d")).alias("quarter"),
        F.weekofyear(F.col("d")).alias("week"),
        F.dayofweek(F.col("d")).alias("day_of_week"),
        F.date_format(F.col("d"), "EEEE").alias("day_name"),
        F.dayofmonth(F.col("d")).alias("day_of_month"),
        F.dayofweek(F.col("d")).isin([1, 7]).cast("int").alias("is_weekend"),
        # Seasonality
        F.when(
            F.month(F.col("d")).isin([12, 1, 2]),
            "Winter"
        ).when(
            F.month(F.col("d")).isin([3, 4, 5]),
            "Spring"
        ).when(
            F.month(F.col("d")).isin([6, 7, 8]),
            "Summer"
        ).otherwise("Fall").alias("season"),
    )
    
    count = df_time.count()
    print(f"  ✓ Created {count} time records (2024-2027)")
    
    (
        df_time.writeTo(DIM_TIME)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("primaryKey", "time_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_TIME} created (PK: time_key)")


def build_dim_location(df_silver: DataFrame):
    print("\n=== BUILD DIM_LOCATION ===")
    df_loc = (
        df_silver
        .select(
            "latitude",
            "longitude",
            "area_name",
            "ward_name",
            "street_name",
            "area_name_encoded",
            "ward_name_encoded",
        )
        .filter(F.col("area_name").isNotNull())
        .distinct()
        .withColumn(
            "location_key",
            F.row_number().over(
                Window.partitionBy().orderBy(
                    "area_name", "ward_name", "street_name", "latitude", "longitude"
                )
            )
        )
    )
    
    # Categorize location_tier từ area statistics
    df_price_by_area = df_silver.filter(
        (F.col("price").isNotNull()) & (F.col("area_name").isNotNull())
    ).groupBy("area_name").agg(
        F.avg("price").alias("avg_price")
    )
    
    overall_median = df_silver.filter(F.col("price").isNotNull()).agg(
        F.percentile_approx("price", 0.5)
    ).collect()[0][0]
    
    df_price_by_area = df_price_by_area.withColumn(
        "location_tier",
        F.when(
            F.col("avg_price") > F.lit(overall_median) * 1.5,
            "Premium"
        ).when(
            F.col("avg_price") < F.lit(overall_median) * 0.7,
            "Developing"
        ).otherwise("Standard")
    )
    
    df_loc = df_loc.join(
        df_price_by_area.select("area_name", "location_tier"),
        on="area_name",
        how="left"
    )
    
    df_loc = df_loc.fillna("Unknown", ["location_tier"])
    
    count = df_loc.count()
    print(f"  ✓ Created {count} location records")
    print(f"    Columns: latitude, longitude, area_name, ward_name, street_name, area_name_encoded, ward_name_encoded, location_tier")
    
    tier_dist = df_loc.groupBy("location_tier").count()
    print(f"  Location tier distribution:")
    tier_dist.show()
    
    (
        df_loc.writeTo(DIM_LOCATION)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("primaryKey", "location_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_LOCATION} created (PK: location_key)")


def build_dim_property(df_silver: DataFrame):
    """
    Tạo dimension property: descriptive attributes của căn nhà từ Silver.
    Chỉ sử dụng cột thực tế có trong Silver:
    - category_name, is_land, is_apt, is_house (loại hình)
    - rooms, has_rooms (phòng)
    """
    print("\n=== BUILD DIM_PROPERTY ===")
    
    df_prop = (
        df_silver
        .select(
            "category_name",
            "is_land",
            "is_apt",
            "is_house",
            "rooms",
            "has_rooms",
        )
        .filter(F.col("category_name").isNotNull())
        .distinct()
        .withColumn(
            "property_key",
            F.row_number().over(
                Window.partitionBy().orderBy(
                    "category_name", "is_land", "is_apt", "is_house", 
                    "rooms", "has_rooms"
                )
            )
        )
    )
    
    # Tạo rooms_category cho phân tích
    df_prop = df_prop.withColumn(
        "rooms_category",
        F.when(F.col("has_rooms") == 0, "No_Rooms_Land")
        .when((F.col("rooms").isNull()) | (F.col("rooms") == 0), "Unknown_Rooms")
        .when((F.col("rooms") > 0) & (F.col("rooms") < 1), "Studio")
        .when((F.col("rooms") >= 1) & (F.col("rooms") < 2), "1BR")
        .when((F.col("rooms") >= 2) & (F.col("rooms") < 3), "2BR")
        .when((F.col("rooms") >= 3) & (F.col("rooms") < 4), "3BR")
        .when((F.col("rooms") >= 4) & (F.col("rooms") < 5), "4BR")
        .when(F.col("rooms") >= 5, "5BR_Plus")
        .otherwise("Unknown")
    )
    
    count = df_prop.count()
    print(f"  ✓ Created {count} property records")
    print(f"    Columns: category_name, is_land, is_apt, is_house, rooms, has_rooms, rooms_category")
    
    rooms_dist = df_prop.groupBy("rooms_category").count().orderBy("rooms_category")
    print(f"  Rooms distribution:")
    rooms_dist.show()
    
    (
        df_prop.writeTo(DIM_PROPERTY)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("primaryKey", "property_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_PROPERTY} created (PK: property_key)")


def build_dim_seller(df_silver: DataFrame):
    """
    Tạo dimension seller từ Silver.
    Chỉ sử dụng cột thực tế có trong Silver:
    - account_id, account_name
    - is_company_ad, is_personal_ad (seller type flags)
    """
    print("\n=== BUILD DIM_SELLER ===")
    
    df_seller = (
        df_silver
        .select(
            "account_id",
            "account_name",
            "is_company_ad",
            "is_personal_ad"
        )
        .filter(F.col("account_id").isNotNull())
        .distinct()
        .withColumn(
            "seller_key",
            F.row_number().over(Window.partitionBy().orderBy("account_id"))
        )
    )
    
    # Categorize seller tier dựa trên listing_count
    seller_volume = df_silver.filter(
        F.col("account_id").isNotNull()
    ).groupBy("account_id").agg(
        F.count("*").alias("listing_count")
    )
    
    # Top 75% percentile = High, 50-75% = Medium, <50% = Low
    percentile_75 = seller_volume.agg(
        F.percentile_approx("listing_count", 0.75)
    ).collect()[0][0]
    
    percentile_50 = seller_volume.agg(
        F.percentile_approx("listing_count", 0.5)
    ).collect()[0][0]
    
    seller_volume = seller_volume.withColumn(
        "seller_tier",
        F.when(
            F.col("listing_count") >= F.lit(percentile_75),
            "High-Volume"
        ).when(
            F.col("listing_count") >= F.lit(percentile_50),
            "Medium-Volume"
        ).otherwise("Low-Volume")
    )
    
    df_seller = df_seller.join(
        seller_volume.select("account_id", "seller_tier"),
        on="account_id",
        how="left"
    )
    
    df_seller = df_seller.fillna("Unknown", ["seller_tier"])
    
    count = df_seller.count()
    print(f"  ✓ Created {count} seller records")
    print(f"    Columns: account_id, account_name, is_company_ad, is_personal_ad, seller_tier")
    
    seller_dist = df_seller.groupBy("is_company_ad").count()
    print(f"  Seller type distribution:")
    seller_dist.show()
    
    (
        df_seller.writeTo(DIM_SELLER)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("primaryKey", "seller_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {GOLD_NAMESPACE}.dim_seller created (PK: seller_key)")


def build_dim_legal_status(df_silver: DataFrame):
    """
    ⚠️ DEPRECATED: Legal attributes are now stored directly in fact_properties
    This reduces dimension fragmentation and improves query performance.
    """
    print("\n  ⏭️  dim_legal_status deprecated - attributes stored in fact_properties")
    pass


def build_dim_quality(df_silver: DataFrame):
    """
    ⚠️ DEPRECATED: Quality attributes are now stored directly in fact_properties
    This reduces dimension fragmentation and improves query performance.
    """
    print("\n  ⏭️  dim_quality deprecated - attributes stored in fact_properties")
    pass


def build_dim_furnishing(df_silver: DataFrame):
    """
    ⚠️ DEPRECATED: Furnishing is a simple flag - now stored directly in fact_properties
    This reduces dimension fragmentation and improves query performance.
    """
    print("\n  ⏭️  dim_furnishing deprecated - attribute stored in fact_properties")
    pass


def build_dim_metro_station():
    """
    🚇 Tạo dimension metro stations từ silver.metro_stations
    Chứa tất cả unique metro stations
    """
    print("\n=== BUILD DIM_METRO_STATION ===")
    
    try:
        df_metro = spark.table(METRO_STATIONS)
    except AnalysisException as e:
        print(f"  ⚠️  Metro table not found: {e}")
        print("  → Skipping dim_metro_station creation")
        return
    
    df_metro_dim = (
        df_metro
        .select(
            "station_id",
            "station_code",
            "station_name",
            "station_type",
            "station_status",
            "area_name_encoded",
            "ward_name_encoded",
            "area_name",
            "ward_name",
            "latitude",
            "longitude"
        )
        .filter(F.col("station_id").isNotNull())
        .distinct()
        .withColumn(
            "station_key",
            F.row_number().over(
                Window.partitionBy().orderBy("station_id")
            )
        )
    )
    
    count = df_metro_dim.count()
    print(f"  ✓ Created {count} metro station records")
    print(f"    Columns: station_id, station_code, station_name, station_type, station_status, area_name, ward_name, latitude, longitude")
    
    station_types = df_metro_dim.groupBy("station_type").count()
    print(f"  Station types distribution:")
    station_types.show()
    
    (
        df_metro_dim.writeTo(DIM_METRO_STATION)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("primaryKey", "station_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_METRO_STATION} created (PK: station_key)")


# ============================================================
# FACT TABLE
# ============================================================

def build_fact_properties(df_silver: DataFrame):
    """
    Tạo fact table: fact_properties (Feature Store)
    
    Chứa tất cả engineered FEATURES cho analytics & ML:
    - Price Features: price_per_m2, price_z_score, price_percentile_in_area
    - Quality Features: quality_score, legal_risk_level, property_quality_tier
    - Engagement Features: image_richness_score, has_images, has_multiple_images, listing_age_bucket
    - Location Features: location_tier, area_popularity, market_saturation_score
    - Seller Features: seller_type, seller_credibility_score
    - Temporal Features: month_of_year, is_peak_season, day_of_week, is_weekend
    - Dimension Keys: für joining to dimensions
    """
    print("\n=== BUILD FACT_PROPERTIES (Feature Store) ===")
    
    df_fact = df_silver
    
    # ============================================================
    # PRICE FEATURES
    # ============================================================
    
    # price_per_m2
    if "price_per_m2" not in df_fact.columns:
        df_fact = df_fact.withColumn(
            "price_per_m2",
            F.when(
                (F.col("size").isNotNull()) & (F.col("size") > 0) & (F.col("price").isNotNull()),
                F.col("price") / F.col("size")
            ).otherwise(None)
        )
    
    # For z-score & percentile, we need area-level statistics
    # price_z_score: how many std devs from area mean?
    area_price_stats = (
        df_fact
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .groupBy("area_name", "category_name")
        .agg(
            F.avg("price").alias("area_price_avg"),
            F.stddev("price").alias("area_price_std"),
            F.percentile_approx("price", 0.5).alias("area_price_median"),
            F.min("price").alias("area_price_min"),
            F.max("price").alias("area_price_max"),
            F.count("*").alias("area_listing_count"),
        )
    )
    
    df_fact = df_fact.join(
        area_price_stats,
        on=["area_name", "category_name"],
        how="left"
    )
    
    # price_z_score
    df_fact = df_fact.withColumn(
        "price_z_score",
        F.when(
            (F.col("price").isNotNull()) & (F.col("area_price_std").isNotNull()) & (F.col("area_price_std") > 0),
            F.round((F.col("price") - F.col("area_price_avg")) / F.col("area_price_std"), 2)
        ).otherwise(None)
    )
    
    # price_percentile_in_area: what % of listings in area are cheaper?
    window_percentile = Window.partitionBy("area_name", "category_name").orderBy(F.col("price"))
    df_fact = df_fact.withColumn(
        "price_percentile_in_area",
        F.when(
            F.col("price").isNotNull(),
            F.round(F.percent_rank().over(window_percentile) * 100, 1)
        ).otherwise(None)
    )
    
    # ============================================================
    # QUALITY FEATURES
    # ============================================================
    
    # quality_score: already calculated
    if "quality_score" not in df_fact.columns:
        quality_score = F.lit(0).cast("double")
        
        if "fully_legal" in df_fact.columns:
            quality_score = quality_score + F.when(F.col("fully_legal") == 1, 35).otherwise(0)
        if "semi_legal" in df_fact.columns:
            quality_score = quality_score + F.when(F.col("semi_legal") == 1, 15).otherwise(0)
        if "good_quality" in df_fact.columns:
            quality_score = quality_score + F.when(F.col("good_quality") == 1, 30).otherwise(0)
        if "number_of_images" in df_fact.columns:
            quality_score = quality_score + F.when(F.col("number_of_images") >= 5, 15).when(
                F.col("number_of_images") >= 1, 5
            ).otherwise(0)
        if "is_furnishing" in df_fact.columns:
            quality_score = quality_score + F.when(F.col("is_furnishing") == 1, 5).otherwise(0)
        
        df_fact = df_fact.withColumn("quality_score", F.least(quality_score, F.lit(100)))
    
    # legal_risk_level: categorical
    df_fact = df_fact.withColumn(
        "legal_risk_level",
        F.when(F.col("fully_legal") == 1, "Low")
        .when(F.col("semi_legal") == 1, "Medium")
        .when(F.col("risk_legal") == 1, "High")
        .otherwise("Unknown")
    )
    
    # property_quality_tier: categorical
    df_fact = df_fact.withColumn(
        "property_quality_tier",
        F.when(F.col("good_quality") == 1, "Good")
        .when(F.col("low_quality") == 1, "Neutral")
        .when(F.col("risk_quality") == 1, "Risk")
        .otherwise("Unknown")
    )
    
    # ============================================================
    # ENGAGEMENT FEATURES
    # ============================================================
    
    # image_richness_score: 0-10
    if "image_richness_score" not in df_fact.columns:
        df_fact = df_fact.withColumn(
            "image_richness_score",
            F.when(F.col("number_of_images").isNull(), 0)
            .when(F.col("number_of_images") == 0, 0)
            .when(F.col("number_of_images") < 1, 1)
            .when(F.col("number_of_images") < 3, 3)
            .when(F.col("number_of_images") < 5, 5)
            .when(F.col("number_of_images") < 10, 7)
            .otherwise(10)
        )
    
    # has_images: binary
    if "has_images" not in df_fact.columns:
        df_fact = df_fact.withColumn(
            "has_images",
            F.when(F.col("number_of_images") > 0, 1).otherwise(0)
        )
    
    # has_multiple_images: 3+ images (professional standard)
    df_fact = df_fact.withColumn(
        "has_multiple_images",
        F.when(F.col("number_of_images") >= 3, 1).otherwise(0)
    )
    
    # listing_age_days
    if "listing_age_days" not in df_fact.columns:
        df_fact = df_fact.withColumn(
            "listing_age_days",
            F.datediff(F.col("snapshot_date"), F.col("crawled_at_ts"))
        )
    
    # listing_age_bucket: categorize
    df_fact = df_fact.withColumn(
        "listing_age_bucket",
        F.when(F.col("listing_age_days") <= 7, "New (0-7d)")
        .when(F.col("listing_age_days") <= 30, "Active (8-30d)")
        .when(F.col("listing_age_days") <= 90, "Aging (31-90d)")
        .otherwise("Old (90d+)")
    )
    
    # ============================================================
    # LOCATION FEATURES
    # ============================================================
    
    # location_tier: from area statistics (calculated with area_price_stats already)
    # Calculate global percentiles for location classification
    area_price_avg_median = df_fact.agg(
        F.percentile_approx("area_price_avg", 0.5)
    ).collect()[0][0]
    
    if area_price_avg_median is None:
        area_price_avg_median = 0
    
    df_fact = df_fact.withColumn(
        "location_tier",
        F.when(
            F.col("area_price_avg").isNotNull(),
            F.when(
                F.col("area_price_avg") > F.lit(area_price_avg_median) * 1.5,
                "Premium"
            ).when(
                F.col("area_price_avg") < F.lit(area_price_avg_median) * 0.7,
                "Developing"
            ).otherwise("Standard")
        ).otherwise("Unknown")
    )
    
    # area_popularity: listing density (listings per area)
    df_fact = df_fact.withColumn(
        "area_popularity",
        F.col("area_listing_count")
    )
    
    # market_saturation_score: 0-100 based on competition & price variance
    df_fact = df_fact.withColumn(
        "market_saturation_score",
        F.when(
            (F.col("area_listing_count").isNotNull()) &
            (F.col("area_price_std").isNotNull()) &
            (F.col("area_price_avg").isNotNull()) &
            (F.col("area_price_avg") > 0),
            F.round(
                F.least(
                    F.col("area_listing_count") / 100 * 50 +
                    (F.col("area_price_std") / F.col("area_price_avg")) * 50,
                    F.lit(100.0)
                ),
                1
            )
        ).otherwise(None)
    )
    
    # ============================================================
    # SELLER FEATURES
    # ============================================================
    
    # seller_type: derived from is_company_ad
    df_fact = df_fact.withColumn(
        "seller_type",
        F.when(F.col("is_company_ad") == 1, "Company")
        .when(F.col("is_personal_ad") == 1, "Personal")
        .otherwise("Unknown")
    )
    
    # seller_credibility_score: 0-100 composite
    seller_cred = F.lit(0).cast("double")
    
    seller_cred = seller_cred + F.when(F.col("is_company_ad") == 1, 30).otherwise(0)  # Companies more credible
    if "good_quality" in df_fact.columns:
        seller_cred = seller_cred + F.when(F.col("good_quality") == 1, 25).otherwise(0)
    if "fully_legal" in df_fact.columns:
        seller_cred = seller_cred + F.when(F.col("fully_legal") == 1, 25).otherwise(0)
    if "number_of_images" in df_fact.columns:
        seller_cred = seller_cred + F.when(F.col("number_of_images") >= 5, 20).when(
            F.col("number_of_images") >= 1, 10
        ).otherwise(0)
    
    df_fact = df_fact.withColumn("seller_credibility_score", F.least(seller_cred, F.lit(100)))
    
    # ============================================================
    # TEMPORAL FEATURES
    # ============================================================
    
    # month_of_year: 1-12
    df_fact = df_fact.withColumn(
        "month_of_year",
        F.month(F.col("snapshot_date"))
    )
    
    # day_of_week: 1=Sun ... 7=Sat
    df_fact = df_fact.withColumn(
        "day_of_week",
        F.dayofweek(F.col("snapshot_date"))
    )
    
    # is_weekend
    df_fact = df_fact.withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("snapshot_date")).isin([1, 7]), 1).otherwise(0)
    )
    
    # is_peak_season: summer (Jun-Aug) + end_year (Nov-Dec)
    df_fact = df_fact.withColumn(
        "is_peak_season",
        F.when(F.month(F.col("snapshot_date")).isin([6, 7, 8, 11, 12]), 1).otherwise(0)
    )
    
    # ============================================================
    # CALCULATE TIME KEYS FOR JOINS
    # ============================================================
    
    # 2. Create time_key
    df_fact = df_fact.withColumn(
        "time_key",
        F.date_format(F.col("snapshot_date"), "yyyyMMdd").cast("int")
    )
    
    # 3. Join với dimensions để lấy keys
    
    # Join dim_location
    df_dim_loc = spark.table(DIM_LOCATION).select(
        "location_key",
        "area_name",
        "ward_name",
        "street_name",
        "latitude",
        "longitude"
    )

    df_fact = df_fact.join(
        df_dim_loc,
        on=["area_name", "ward_name", "street_name", "latitude", "longitude"],
        how="left"
    )
    
    # Join dim_property
    df_dim_prop = spark.table(DIM_PROPERTY).select(
        "property_key", "category_name", "is_land", "is_apt", "is_house", 
        "rooms", "has_rooms"
    )
    df_fact = df_fact.join(
        df_dim_prop,
        on=["category_name", "is_land", "is_apt", "is_house", "rooms", "has_rooms"],
        how="left"
    )
    
    # Join dim_seller
    df_dim_seller = spark.table(DIM_SELLER).select(
        "seller_key", "account_id"
    )
    df_fact = df_fact.join(df_dim_seller, on="account_id", how="left")
    
    # 4. Select final columns (FK + measures + flags + timestamps + engineered features)
    fact_cols = [
        # ===== DIMENSION KEYS =====
        "location_key", "time_key", "property_key", "seller_key",
        
        # ===== IDENTIFIERS =====
        "ad_id", "list_id", "account_id",
        
        # ===== PRICE FEATURES =====
        "price", "price_per_m2", "price_z_score", "price_percentile_in_area", "size",
        
        # ===== QUALITY FEATURES =====
        "quality_score", "legal_risk_level", "property_quality_tier",
        
        # ===== ENGAGEMENT FEATURES =====
        "number_of_images", "image_richness_score", "has_images",
        "has_multiple_images", "listing_age_days", "listing_age_bucket",
        
        # ===== LOCATION FEATURES =====
        "location_tier", "area_popularity", "market_saturation_score",
        
        # ===== SELLER FEATURES =====
        "seller_type", "seller_credibility_score",
        
        # ===== TEMPORAL FEATURES =====
        "month_of_year", "day_of_week", "is_weekend", "is_peak_season",
        
        # ===== FLAGS & ATTRIBUTES =====
        "rooms", "is_company_ad",
        "is_furnishing", "is_not_furnishing", "fully_legal", "semi_legal", "risk_legal",
        "good_quality", "low_quality", "risk_quality",
        "is_land", "is_apt", "is_house", "category_name",
        
        # ===== TIMESTAMPS =====
        "snapshot_date", "crawled_at_ts", "_ingest_ts",
    ]
    
    fact_cols = [c for c in fact_cols if c in df_fact.columns]
    
    df_fact = df_fact.select(*fact_cols)
    
    count = df_fact.count()
    print(f"  ✓ Created fact table (Feature Store): {count:,} records")
    print(f"  ✓ Total feature columns: {len(df_fact.columns)}")
    print(f"""
    ✓ Engineered Features Included:
      • Price:      price_per_m2, price_z_score, price_percentile_in_area
      • Quality:    quality_score, legal_risk_level, property_quality_tier
      • Engagement: image_richness_score, has_multiple_images, listing_age_bucket
      • Location:   location_tier, area_popularity, market_saturation_score
      • Seller:     seller_type, seller_credibility_score
      • Temporal:   month_of_year, is_peak_season, is_weekend
    """)
    
    (
        df_fact.writeTo(FACT_PROPERTIES)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("fk_location_key", "dim_location.location_key")
        .tableProperty("fk_property_key", "dim_property.property_key")
        .tableProperty("fk_seller_key", "dim_seller.seller_key")
        .tableProperty("fk_time_key", "dim_time.time_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {FACT_PROPERTIES} created")


# ============================================================
# AGGREGATED FACT TABLES (Use Case-Specific)
# ============================================================

def build_fact_price_area_time(df_silver: DataFrame):
    """
    💰 PRICE ANALYTICS: Conformed to star schema
    Joins with: dim_time, dim_location, dim_property
    Dimensions: location_key, property_key, time_key
    FK: Aggregated from fact_properties (can join back)
    """
    print("\n=== BUILD fact_price_area_time ===")
    
    # Get dimension tables for FK lookup
    df_dim_loc = spark.table(DIM_LOCATION).select(
        "location_key", "area_name"
    )
    df_dim_prop = spark.table(DIM_PROPERTY).select(
        "property_key", "category_name"
    )
    
    # Filter valid price data
    df = (
        df_silver
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("year_month", F.date_format(F.col("snapshot_date"), "yyyy-MM-01"))
        .withColumn(
            "time_key",
            F.date_format(F.col("snapshot_date"), "yyyyMM01").cast("int")
        )
    )
    
    # Aggregate by location + property category + month
    df_agg = (
        df
        .groupBy("area_name", "category_name", "time_key", "year_month")
        .agg(
            F.round(F.avg("price"), 0).alias("price_avg"),
            F.round(F.percentile_approx("price", 0.5), 0).alias("price_median"),
            F.round(F.min("price"), 0).alias("price_min"),
            F.round(F.max("price"), 0).alias("price_max"),
            F.count("*").alias("listing_count"),
            F.stddev("price").alias("price_std"),
        )
        .drop("year_month")
    )
    
    # Join with dimensions to get keys
    df_agg = df_agg.join(df_dim_loc, on="area_name", how="left")
    df_agg = df_agg.join(df_dim_prop, on="category_name", how="left")
    
    # Select final columns with dimension keys
    df_agg = df_agg.select(
        "location_key",
        "property_key", 
        "time_key",
        "area_name",
        "category_name",
        "price_avg",
        "price_median",
        "price_min",
        "price_max",
        "listing_count",
        "price_std"
    )
    
    count = df_agg.count()
    print(f"  ✓ {count:,} records (area + category + month)")
    print(f"  ✓ FK: location_key, property_key, time_key")
    df_agg.writeTo(FACT_PRICE_AREA_TIME).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    print(f"  ✓ {FACT_PRICE_AREA_TIME} created")


def build_fact_seller_time(df_silver: DataFrame):
    """
    👤 SELLER PERFORMANCE: Conformed to star schema
    Joins with: dim_time, dim_seller
    Dimensions: seller_key, time_key
    FK: Aggregated from fact_properties (can join back)
    """
    print("\n=== BUILD fact_seller_time ===")
    
    # Get dimension table for FK lookup
    df_dim_seller = spark.table(DIM_SELLER).select(
        "seller_key", "account_id"
    )
    
    df = (
        df_silver
        .filter(F.col("account_id").isNotNull())
        .withColumn("year_month", F.date_format(F.col("snapshot_date"), "yyyy-MM-01"))
        .withColumn(
            "time_key",
            F.date_format(F.col("snapshot_date"), "yyyyMM01").cast("int")
        )
        .withColumn("seller_type", F.when(F.col("is_company_ad") == 1, "Company").otherwise("Personal"))
    )
    
    # Aggregate by seller + month
    df_agg = (
        df
        .groupBy("account_id", "account_name", "seller_type", "time_key", "year_month")
        .agg(
            F.count("*").alias("listing_count"),
            F.round(F.avg("price"), 0).alias("price_avg"),
            F.round(F.percentile_approx("price", 0.5), 0).alias("price_median"),
            F.round(F.avg("number_of_images"), 1).alias("images_avg"),
            F.sum(F.when(F.col("fully_legal") == 1, 1).otherwise(0)).alias("legal_listings_count"),
            F.sum(F.when(F.col("good_quality") == 1, 1).otherwise(0)).alias("quality_listings_count"),
        )
        .drop("year_month")
    )
    
    # Join with seller dimension to get seller_key
    df_agg = df_agg.join(df_dim_seller, on="account_id", how="left")
    
    # Select final columns with dimension keys
    df_agg = df_agg.select(
        "seller_key",
        "time_key",
        "account_id",
        "account_name",
        "seller_type",
        "listing_count",
        "price_avg",
        "price_median",
        "images_avg",
        "legal_listings_count",
        "quality_listings_count"
    )
    
    count = df_agg.count()
    print(f"  ✓ {count:,} records (seller + month)")
    print(f"  ✓ FK: seller_key, time_key")
    df_agg.writeTo(FACT_SELLER_TIME).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    print(f"  ✓ {FACT_SELLER_TIME} created")


# ============================================================
# DEPRECATED: Trend functions removed - simplify to core star schema
# ============================================================
# The core star schema focuses on: fact_properties, fact_price_area_time, 
# and fact_seller_time all using shared dimensions
# ============================================================


def build_fact_properties_metro_analysis(df_silver: DataFrame):
    """
    🚇 METRO PROXIMITY ANALYSIS: Join properties with nearest metro stations (via dimension)
    
    Features:
    - Calculate distance (m) from property to nearest metro station
    - Categorize: Near_Metro (<500m), Close_To_Metro (500m-1.5km), Far_From_Metro (>1.5km)
    - Foreign Key: station_id (join to dim_metro_station for details)
    - Analyze price premium by metro proximity
    """
    print("\n=== BUILD fact_properties_metro_analysis ===")
    
    # Read dim_metro_station
    try:
        df_dim_metro = spark.table(DIM_METRO_STATION)
        metro_count = df_dim_metro.count()
        print(f"  ✓ Metro dimensions loaded: {metro_count} stations")
    except AnalysisException as e:
        print(f"  ⚠️  Metro dimension not found: {e}")
        print("  → Skipping metro analysis")
        return
    
    # Prepare real estate data with coordinates
    df_props = (
        df_silver
        .filter(
            F.col("latitude").isNotNull() & 
            F.col("longitude").isNotNull() & 
            F.col("price").isNotNull() &
            (F.col("price") > 0)
        )
        .select(
            "ad_id", "list_id", "area_name_encoded", "ward_name_encoded",
            "area_name", "ward_name", "latitude", "longitude",
            "price", "size", "category_name", "rooms",
            "is_land", "is_apt", "is_house", "has_rooms",
            "snapshot_date", "is_company_ad"
        )
        .repartition(100)  # Distribute for cross join
        .cache()  # Cache for cross join performance
    )
    
    prop_count = df_props.count()
    print(f"  ✓ Properties to process: {prop_count:,}")
    
    # Prepare metro dimension with coordinates
    df_metro_prep = (
        df_dim_metro
        .select(
            "station_id", "station_key",
            "area_name_encoded", "ward_name_encoded",
            F.col("latitude").alias("metro_latitude"),
            F.col("longitude").alias("metro_longitude")
        )
    )
    
    # Cross join properties with metro stations in same area+ward
    # This ensures we only join properties with metros in their vicinity
    df_joined = (
        df_props
        .join(
            df_metro_prep,
            on=["area_name_encoded", "ward_name_encoded"],
            how="left"
        )
    )
    
    # Calculate distance using Euclidean formula (simplified for lat/lon)
    # distance_m ≈ sqrt((lat_diff*111km)^2 + (lon_diff*111*cos(lat))^2)
    df_joined = df_joined.withColumn(
        "distance_to_metro_m",
        F.when(
            F.col("station_id").isNotNull(),
            F.round(
                F.sqrt(
                    F.pow((F.col("latitude") - F.col("metro_latitude")) * 111000, 2) +
                    F.pow(
                        (F.col("longitude") - F.col("metro_longitude")) * 111000 * 
                        F.cos(F.col("latitude") * F.lit(3.14159 / 180)),
                        2
                    )
                ),
                0
            )
        ).otherwise(None)
    )
    
    # Find nearest metro station per property
    window_nearest = Window.partitionBy("ad_id").orderBy(
        F.col("distance_to_metro_m").asc_nulls_last()
    )
    
    df_nearest = (
        df_joined
        .withColumn("rn", F.row_number().over(window_nearest))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    
    # Categorize proximity
    df_nearest = df_nearest.withColumn(
        "metro_proximity_category",
        F.when(
            F.col("distance_to_metro_m").isNull(),
            "No_Metro_In_Ward"
        ).when(
            F.col("distance_to_metro_m") < 500,
            "Near_Metro"
        ).when(
            F.col("distance_to_metro_m") < 1500,
            "Close_To_Metro"
        ).otherwise("Far_From_Metro")
    )
    
    # Calculate price per m2 if size available
    df_nearest = df_nearest.withColumn(
        "price_per_m2",
        F.when(
            (F.col("size").isNotNull()) & (F.col("size") > 0),
            F.round(F.col("price") / F.col("size"), 0)
        ).otherwise(None)
    )
    
    # ============================================================
    # ADD DIMENSION KEYS FOR STAR SCHEMA CONNECTIONS
    # ============================================================
    
    # Add time_key for joining with dim_time
    df_nearest = df_nearest.withColumn(
        "time_key",
        F.date_format(F.col("snapshot_date"), "yyyyMMdd").cast("int")
    )
    
    # Join with dim_location to get location_key (based on area, ward, coordinates)
    df_dim_loc = spark.table(DIM_LOCATION).select(
        "location_key", "area_name", "ward_name", "latitude", "longitude"
    )
    df_nearest = df_nearest.join(
        df_dim_loc,
        on=["area_name", "ward_name", "latitude", "longitude"],
        how="left"
    )
    
    # Join with dim_property to get property_key (ALL attributes for proper match)
    df_dim_prop = spark.table(DIM_PROPERTY).select(
        "property_key", "category_name", "is_land", "is_apt", "is_house", "rooms", "has_rooms"
    )
    df_nearest = df_nearest.join(
        df_dim_prop,
        on=["category_name", "is_land", "is_apt", "is_house", "rooms", "has_rooms"],
        how="left"
    )
    
    # Select final columns for fact table (with all dimension keys)
    fact_metro_cols = [
        # DIMENSION KEYS (for joining to shared dimensions)
        "location_key", "property_key", "time_key", "station_key",
        # Identifiers
        "ad_id", "list_id",
        # Location
        "area_name", "ward_name", "latitude", "longitude",
        # Property attributes
        "price", "price_per_m2", "size", "category_name", "rooms",
        "is_company_ad",
        # Metro Foreign Key & Distance
        "station_id",
        "distance_to_metro_m", "metro_proximity_category",
        # Temporal
        "snapshot_date"
    ]
    
    df_fact_metro = df_nearest.select(*fact_metro_cols)
    
    count = df_fact_metro.count()
    print(f"  ✓ Created {count:,} property-metro records")
    
    # Show distribution by proximity
    dist_by_proximity = (
        df_fact_metro
        .groupBy("metro_proximity_category")
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("price"), 0).alias("avg_price"),
            F.round(F.avg("price_per_m2"), 0).alias("avg_price_per_m2"),
            F.round(F.avg("distance_to_metro_m"), 0).alias("avg_distance_m")
        )
        .orderBy("avg_distance_m")
    )
    
    print(f"\n  📊 Metro Proximity Distribution:")
    dist_by_proximity.show(truncate=False)
    
    # Calculate price premium near metro
    avg_price_all = (
        df_fact_metro
        .agg(F.avg("price")).collect()[0][0]
    )
    
    if avg_price_all is not None and avg_price_all != 0:
        price_premiums = (
            df_fact_metro
            .groupBy("metro_proximity_category")
            .agg(F.round(F.avg("price"), 0).alias("avg_price"))
            .withColumn(
                "price_premium_pct",
                F.round(
                    ((F.col("avg_price") - F.lit(avg_price_all)) / F.lit(avg_price_all)) * 100,
                    1
                )
            )
            .orderBy(F.col("price_premium_pct").desc())
        )
        
        print(f"\n  💰 Price Premium by Metro Proximity:")
        price_premiums.show(truncate=False)
    
    # Write to gold table
    (
        df_fact_metro.writeTo(FACT_METRO_ANALYSIS)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("fk_location_key", "dim_location.location_key")
        .tableProperty("fk_property_key", "dim_property.property_key")
        .tableProperty("fk_time_key", "dim_time.time_key")
        .tableProperty("fk_station_key", "dim_metro_station.station_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {FACT_METRO_ANALYSIS} created")
    
    # Clean up cached data to free memory
    df_props.unpersist()
    print("  ✓ Memory freed from cross join cache")


def build_dim_search_category():
    """
    🔍 SEARCH CATEGORY DIMENSION: Map search categories from GGTREND data
    
    Categories:
    - Căn hộ/Chung cư (Apartments)
    - Nhà ở (Houses)
    - Đất (Land)
    - Bất động sản (All categories combined)
    """
    print("\n=== BUILD dim_search_category ===")
    
    categories = [
        (1, "Căn hộ/Chung cư", "Căn hộ, Chung cư"),
        (2, "Nhà ở", "Nhà, Nhà ở, Nhà riêng"),
        (3, "Đất", "Đất, Đất nền"),
        (4, "Bất động sản", "Tất cả loại hình")
    ]
    
    df_dim = spark.createDataFrame(
        categories,
        ["category_key", "category_name", "category_description"]
    )
    
    count = df_dim.count()
    print(f"  ✓ Created {count} search categories")
    
    # Write to Gold
    (
        df_dim.writeTo(DIM_SEARCH_CATEGORY)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("primaryKey", "category_key")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_SEARCH_CATEGORY} created (PK: category_key)")
    return df_dim


def build_fact_search_trends_monthly():
    """
    📊 SEARCH TRENDS FACT TABLE: Monthly search volume by property type

    Star Schema Structure:
    - Grain: (year_month, category_name) - 1 row per month × property type
    - Foreign Keys: time_key (DIM_TIME), category_key (DIM_SEARCH_CATEGORY)
    - Measures: search_volume_sum, search_volume_avg, search_volume_min, search_volume_max

    Source: silver.ggtrend_daily (Vietnam-scope Google Trends, unpivoted)
    """
    print("\n=== BUILD fact_search_trends_monthly ===")

    try:
        df_ggtrend = spark.table(GGTREND_SILVER)
        df_dim_time = spark.table(DIM_TIME)
        df_dim_search = spark.table(DIM_SEARCH_CATEGORY)

        trend_count = df_ggtrend.count()
        print(f"  ✓ GGTREND data loaded: {trend_count:,} records")
    except AnalysisException as e:
        print(f"  ⚠️  Error loading tables: {e}")
        return

    # ============================================================
    # Normalize Google Trends keyword columns.
    # Current Silver uses Vietnam-scope generic names:
    #   bat_dong_san, mua_nha, can_ho, chung_cu, dat_nen
    #
    # Legacy *_tp_hcm names are still accepted only as backward-compatible input.
    # The analytics logic remains unchanged.
    # ============================================================
    legacy_to_current = {
        "bat_dong_san_tp_hcm": "bat_dong_san",
        "mua_nha_tp_hcm": "mua_nha",
        "can_ho_tp_hcm": "can_ho",
        "chung_cu_tp_hcm": "chung_cu",
        "dat_nen_tp_hcm": "dat_nen",
    }

    for legacy_col, current_col in legacy_to_current.items():
        if current_col not in df_ggtrend.columns and legacy_col in df_ggtrend.columns:
            df_ggtrend = df_ggtrend.withColumnRenamed(legacy_col, current_col)
            print(f"  Using legacy input column {legacy_col} as {current_col}")

    required_cols = [
        "date",
        "chung_cu",
        "can_ho",
        "mua_nha",
        "dat_nen",
        "bat_dong_san",
    ]

    missing_cols = [c for c in required_cols if c not in df_ggtrend.columns]
    if missing_cols:
        print(f"  ⚠️  Missing required Google Trends columns: {missing_cols}")
        print(f"  Available columns: {df_ggtrend.columns}")
        print("  → Skipping fact_search_trends_monthly creation")
        return

    # ============================================================
    # UNPIVOT GGTREND: Convert category columns to (date, category, volume) format
    # ============================================================
    df_trends = (
        df_ggtrend
        .withColumn("date", F.to_date(F.col("date")))
        # Prepare columns: combine related categories
        .withColumn("apts_volume", F.col("chung_cu") + F.col("can_ho"))
        .withColumn("house_volume", F.col("mua_nha"))
        .withColumn("land_volume", F.col("dat_nen"))
        .withColumn("total_volume", F.col("bat_dong_san"))
        # Unpivot (must select all columns used in stack expression)
        .select(
            "date",
            "apts_volume", "house_volume", "land_volume", "total_volume",
            F.expr("""stack(4,
                'Căn hộ/Chung cư', apts_volume,
                'Nhà ở', house_volume,
                'Đất', land_volume,
                'Bất động sản', total_volume
            )""").alias("category_name", "search_volume")
        )
        .withColumn("year_month", F.trunc(F.col("date"), "month"))
        .groupBy("year_month", "category_name")
        .agg(
            F.sum("search_volume").alias("search_volume_sum"),
            F.round(F.avg("search_volume"), 0).alias("search_volume_avg"),
            F.min("search_volume").alias("search_volume_min"),
            F.max("search_volume").alias("search_volume_max")
        )
    )

    # ============================================================
    # JOIN WITH DIMENSIONS
    # ============================================================

    # 1. Join with DIM_TIME (get first date of each month via window function)
    df_dim_time_with_month = (
        df_dim_time
        .withColumn("year_month", F.trunc(F.col("date"), "month"))
        .withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("year_month").orderBy("date"))
        )
        .filter(F.col("rn") == 1)  # Keep only first day of month
        .select("year_month", "time_key")
    )

    df_fact = (
        df_trends
        .join(df_dim_time_with_month, on="year_month", how="left")
    )

    # 2. Join with DIM_SEARCH_CATEGORY
    df_dim_search_join = df_dim_search.select("category_key", "category_name")

    df_fact = (
        df_fact
        .join(df_dim_search_join, on="category_name", how="left")
    )

    # ============================================================
    # SELECT AND WRITE
    # ============================================================

    df_fact_final = (
        df_fact
        .select(
            "time_key", "category_key",
            "year_month", "category_name",
            "search_volume_sum", "search_volume_avg", "search_volume_min", "search_volume_max"
        )
        .orderBy("year_month", "category_name")
    )

    count = df_fact_final.count()
    print(f"  ✓ Created {count:,} monthly search trend records")

    if count > 0:
        print("\n  📊 Sample Monthly Search Trends:")
        df_fact_final.show(10, truncate=False)

    # Write to Gold
    (
        df_fact_final.writeTo(FACT_SEARCH_TRENDS_MONTHLY)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("fk_time_key", "dim_time.time_key")
        .tableProperty("fk_category_key", "dim_search_category.category_key")
        .createOrReplace()
    )

    print(f"  ✓ {FACT_SEARCH_TRENDS_MONTHLY} created")


# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def transform_to_gold_dw():
    """
    Main ETL function: orchestrate creation of conformed star schema
    
    SCHEMA DESIGN:
    - Core Dimensions (shared): dim_time, dim_location, dim_property, dim_seller, dim_metro_station
    - Atomic Fact Table: fact_properties (all properties with engineered features)
    - Conformed Aggregate Fact Tables: 
      * fact_price_area_time (prices by location + category + month)
      * fact_seller_time (seller performance by month)
    """
    print("\n" + "="*60)
    print("GOLD DW: Building Conformed Star Schema")
    print("="*60)
    
    # Create namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    print(f"✓ Namespace ready: {GOLD_NAMESPACE}")
    
    # Read Silver
    df_silver = read_silver()
    if df_silver is None or df_silver.rdd.isEmpty():
        print("❌ GOLD DW: No data from Silver table")
        return
    
    # Build shared dimensions
    print("\n📊 BUILDING SHARED DIMENSIONS...")
    build_dim_time()
    build_dim_location(df_silver)
    build_dim_property(df_silver)
    build_dim_seller(df_silver)
    build_dim_metro_station()  # For metro analysis
    
    # Build core atomic fact table
    print("\n💾 BUILDING ATOMIC FACT TABLE...")
    build_fact_properties(df_silver)
    
    # Build conformed aggregate fact tables (all use shared dimension keys)
    print("\n📈 BUILDING CONFORMED AGGREGATE FACT TABLES...")
    build_fact_price_area_time(df_silver)
    build_fact_seller_time(df_silver)
    build_fact_properties_metro_analysis(df_silver)
    
    # Build search trends dimension and fact table
    print("\n🔍 BUILDING SEARCH TRENDS ANALYTICS...")
    build_dim_search_category()
    build_fact_search_trends_monthly()
    
    print("\n" + "="*60)
    print("✓ GOLD DW: Star Schema Complete!")
    print("="*60)
    print(f"""
    ✓ SHARED DIMENSIONS (5 core tables):
      • {DIM_TIME}: Calendar (2024-2027) with temporal attributes
      • {DIM_LOCATION}: Geo dimensions (area, ward, street, location_tier)
      • {DIM_PROPERTY}: Property attributes (category, rooms, type flags)
      • {DIM_SELLER}: Seller info (account, seller_tier)
      • {DIM_METRO_STATION}: Metro stations with coordinates & attributes
    
    ✓ ATOMIC FACT TABLE:
      • {FACT_PROPERTIES}: Complete feature store with all properties
        └─ Dimensions: location_key, time_key, property_key, seller_key
        └─ Features: 30+ engineered features (price, quality, engagement, location, seller, temporal)
    
    ✓ CONFORMED AGGREGATE FACT TABLES (All connected via shared dimension keys):
      
      • {FACT_PRICE_AREA_TIME}: Price analytics
        ├─ Dimensions: location_key, property_key, time_key
        ├─ Granularity: Area + Category + Month
        ├─ Measures: avg/median/min/max price, listing_count, price_std
        └─ Joins to: fact_properties (via location_key, property_key, time_key)
      
      • {FACT_SELLER_TIME}: Seller performance
        ├─ Dimensions: seller_key, time_key
        ├─ Granularity: Seller + Month
        ├─ Measures: listing_count, price_avg/median, images, legal/quality counts
        └─ Joins to: fact_properties (via seller_key, time_key)
      
      • {FACT_METRO_ANALYSIS}: Metro proximity analysis
        ├─ Dimensions: location_key, station_key, time_key
        ├─ Granularity: Property + Nearest Metro Station
        ├─ Measures: distance_to_metro_m, price_per_m2, metro_proximity_category
        └─ Joins to: fact_properties (via location_key, time_key)
    
    ✓ SEARCH TRENDS ANALYTICS:
      
      • {DIM_SEARCH_CATEGORY}: Property type categories
        ├─ Categories: 4 types (Căn hộ/Chung cư, Nhà ở, Đất, Bất động sản)
        └─ Attributes: category_key, category_name, category_description
      
      • {FACT_SEARCH_TRENDS_MONTHLY}: Monthly search volume by property type
        ├─ Dimensions: time_key, category_key
        ├─ Granularity: Month + Property Type
        ├─ Measures: search_volume_sum, search_volume_avg, search_volume_min/max
        └─ Analytics: 1 tháng × 1 loại hình = bao nhiêu lượt tìm kiếm
    
    ✓ STAR SCHEMA CONNECTIONS:
      
      Core dimensions connected to all facts:
      
         dim_time ◄──────┐
                         │
      dim_location ◄─────┤──► fact_properties
                         │      ↑
      dim_property ◄─────┤      │ (aggregations from)
                         │      ↓
         dim_seller ◄─────┤ ┌─────────────────────────────────────────┐
                         │ │                                         │
      dim_metro_station ◄┤ │  ┌──────────────┐  ┌──────────────┐   │
                         │ │  │              │  │              │   │
                         └─┤──┤ fact_price   ├──┤ fact_seller  ├───┤ fact_metro
                           │  │ _area_time   │  │ _time        │   │ _analysis
                           │  └──────────────┘  └──────────────┘   │
                           │                                         │
                           └─────────────────────────────────────────┘
    
    ✓ KEY IMPROVEMENTS:
      ✓ All fact tables have dimension keys (location_key, property_key, seller_key, time_key)
      ✓ Fact tables can join with each other via shared dimensions
      ✓ Fact tables are aggregations of fact_properties with common granularities
      ✓ True Star Schema: Conformed dimensions + Multiple aggregation levels
      ✓ Easy to traverse: Any fact can join to any other via shared keys
      ✓ BI-friendly: All relationships are clear and unambiguous
    """)


# ============================================================
# MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold star schema from the Silver table produced by etl_bronze_to_silver.py"
    )
    parser.add_argument(
        "--silver-table",
        default=os.getenv("SILVER_TABLE", SILVER_TABLE),
        help="Input Silver table. Default matches etl_bronze_to_silver.py: lakehouse.silver.chotot_cleaned",
    )
    parser.add_argument(
        "--gold-namespace",
        default=os.getenv("GOLD_NAMESPACE", GOLD_NAMESPACE),
        help="Output Gold namespace. Default: lakehouse.gold",
    )
    parser.add_argument(
        "--ggtrend-silver-table",
        default=os.getenv("GGTREND_SILVER_TABLE", GGTREND_SILVER),
        help="Input Silver Google Trends table. Default: lakehouse.silver.ggtrend_daily",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Keep existing business logic intact; only align runtime table names.
    SILVER_TABLE = args.silver_table
    GOLD_NAMESPACE = args.gold_namespace
    GGTREND_SILVER = args.ggtrend_silver_table

    DIM_TIME = f"{GOLD_NAMESPACE}.dim_time"
    DIM_LOCATION = f"{GOLD_NAMESPACE}.dim_location"
    DIM_PROPERTY = f"{GOLD_NAMESPACE}.dim_property"
    DIM_SELLER = f"{GOLD_NAMESPACE}.dim_seller"
    DIM_METRO_STATION = f"{GOLD_NAMESPACE}.dim_metro_station"

    FACT_PROPERTIES = f"{GOLD_NAMESPACE}.fact_properties"
    FACT_PRICE_AREA_TIME = f"{GOLD_NAMESPACE}.fact_price_area_time"
    FACT_SELLER_TIME = f"{GOLD_NAMESPACE}.fact_seller_time"
    FACT_METRO_ANALYSIS = f"{GOLD_NAMESPACE}.fact_metro_analysis"
    FACT_SEARCH_TRENDS_MONTHLY = f"{GOLD_NAMESPACE}.fact_search_trends_monthly"

    DIM_SEARCH_CATEGORY = f"{GOLD_NAMESPACE}.dim_search_category"

    try:
        transform_to_gold_dw()
    finally:
        spark.stop()
