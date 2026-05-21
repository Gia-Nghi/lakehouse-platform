# ============================================================
# ETL REAL ESTATE: SILVER → GOLD FACTS & DIMENSIONS (DW Schema)
# Purpose: Build dimensional model for analytics warehouse
# Spark 3.5.1 + Iceberg + Nessie + MinIO
# ============================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException

# ============================================================
# SPARK SESSION + ICEBERG CATALOG
# ============================================================
def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Lakehouse-Silver-To-Gold-DW-Schema")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "admin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


spark = build_spark()
spark.sql("SET spark.sql.session.timeZone = UTC")

# ============================================================
# CẤU HÌNH BẢNG
# ============================================================
SILVER_TABLE = "lakehouse.silver.cleaned"
GOLD_NAMESPACE = "lakehouse.gold"

DIM_TIME = f"{GOLD_NAMESPACE}.dim_time"
DIM_LOCATION = f"{GOLD_NAMESPACE}.dim_location"
DIM_PROPERTY = f"{GOLD_NAMESPACE}.dim_property"
DIM_SELLER = f"{GOLD_NAMESPACE}.dim_seller"
DIM_LEGAL_STATUS = f"{GOLD_NAMESPACE}.dim_legal_status"
DIM_QUALITY = f"{GOLD_NAMESPACE}.dim_quality"
DIM_FURNISHING = f"{GOLD_NAMESPACE}.dim_furnishing"
DIM_METRO_STATION = f"{GOLD_NAMESPACE}.dim_metro_station"

# Fact tables - Atomic
FACT_PROPERTIES = f"{GOLD_NAMESPACE}.fact_properties"

# Fact tables - Aggregations (Use case-specific)
FACT_PRICE = f"{GOLD_NAMESPACE}.fact_price_by_area_time"
FACT_QUALITY = f"{GOLD_NAMESPACE}.fact_quality_by_area_time"
FACT_SELLER = f"{GOLD_NAMESPACE}.fact_seller_by_month"
FACT_ENGAGEMENT = f"{GOLD_NAMESPACE}.fact_engagement_by_area_time"
FACT_DAILY = f"{GOLD_NAMESPACE}.fact_daily_market"
FACT_METRO_ANALYSIS = f"{GOLD_NAMESPACE}.fact_properties_metro_analysis"
FACT_TREND_CATEGORY = f"{GOLD_NAMESPACE}.fact_trend_category_daily"

# Source tables - Silver Layer
METRO_STATIONS = "lakehouse.silver.metro_stations"
GGTREND_SILVER = "lakehouse.silver.ggtrend_daily"

# ============================================================
# HELPER FUNCTIONS
# ============================================================


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def read_silver() -> DataFrame:
    print(f"=== GOLD DW: Reading Silver table: {SILVER_TABLE} ===")
    
    try:
        df = spark.table(SILVER_TABLE)
        count = df.count()
        print(f"✓ Silver table read: {count:,} rows")
        return df
    except AnalysisException as e:
        print(f"❌ Error reading Silver table: {e}")
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
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_TIME} created")


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
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_LOCATION} created")


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
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_PROPERTY} created")


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
        .createOrReplace()
    )
    
    print(f"  ✓ {GOLD_NAMESPACE}.dim_seller created")


def build_dim_legal_status(df_silver: DataFrame):
    """
    Tạo dimension tình trạng pháp lý
    """
    print("\n=== BUILD DIM_LEGAL_STATUS ===")
    
    df_legal = (
        df_silver
        .select("fully_legal", "semi_legal", "risk_legal")
        .filter(
            (F.col("fully_legal").isNotNull()) |
            (F.col("semi_legal").isNotNull()) |
            (F.col("risk_legal").isNotNull())
        )
        .distinct()
        .withColumn(
            "legal_status_key",
            F.row_number().over(Window.partitionBy().orderBy("fully_legal", "semi_legal", "risk_legal"))
        )
    )
    
    # Categorize
    df_legal = df_legal.withColumn(
        "legal_category",
        F.when(F.col("fully_legal") == 1, "Fully_Legal")
        .when(F.col("semi_legal") == 1, "Semi_Legal")
        .when(F.col("risk_legal") == 1, "Risk_Legal")
        .otherwise("Unknown")
    )
    
    count = df_legal.count()
    print(f"  ✓ Created {count} legal status records")
    
    (
        df_legal.writeTo(DIM_LEGAL_STATUS)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_LEGAL_STATUS} created")


def build_dim_quality(df_silver: DataFrame):
    """
    Tạo dimension chất lượng
    """
    print("\n=== BUILD DIM_QUALITY ===")
    
    df_quality = (
        df_silver
        .select("good_quality", "low_quality", "risk_quality")
        .filter(
            (F.col("good_quality").isNotNull()) |
            (F.col("low_quality").isNotNull()) |
            (F.col("risk_quality").isNotNull())
        )
        .distinct()
        .withColumn(
            "quality_key",
            F.row_number().over(Window.partitionBy().orderBy("good_quality", "low_quality", "risk_quality"))
        )
    )
    
    # Categorize
    df_quality = df_quality.withColumn(
        "quality_tier",
        F.when(F.col("good_quality") == 1, "Good")
        .when(F.col("low_quality") == 1, "Neutral")
        .when(F.col("risk_quality") == 1, "Risk")
        .otherwise("Unknown")
    )
    
    count = df_quality.count()
    print(f"  ✓ Created {count} quality records")
    
    (
        df_quality.writeTo(DIM_QUALITY)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_QUALITY} created")


def build_dim_furnishing(df_silver: DataFrame):
    print("\n=== BUILD DIM_FURNISHING ===")

    df_furnish = (
        df_silver
        .select("is_furnishing", "is_not_furnishing")
        .distinct()
        .withColumn(
            "furnishing_key",
            F.row_number().over(
                Window.partitionBy().orderBy("is_furnishing", "is_not_furnishing")
            )
        )
        .withColumn(
            "furnishing_category",
            F.when(F.col("is_furnishing") == 1, "Furnished")
             .when(F.col("is_not_furnishing") == 1, "Not_Furnished")
             .otherwise("Unknown")
        )
    )

    schema = StructType([
        StructField("is_furnishing", IntegerType(), True),
        StructField("is_not_furnishing", IntegerType(), True),
        StructField("furnishing_key", IntegerType(), True),
        StructField("furnishing_category", StringType(), True)
    ])

    df_null_record = spark.createDataFrame(
        [(None, None, 999, "Unknown")],
        schema=schema
    )

    df_furnish = df_furnish.union(df_null_record).dropDuplicates(
        ["is_furnishing", "is_not_furnishing"]
    )

    df_furnish.writeTo(DIM_FURNISHING).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    
    print(f"  ✓ {DIM_FURNISHING} created")


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
        .createOrReplace()
    )
    
    print(f"  ✓ {DIM_METRO_STATION} created")


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
    
    # Join dim_legal_status
    df_dim_legal = spark.table(DIM_LEGAL_STATUS).select(
        "legal_status_key", "fully_legal", "semi_legal", "risk_legal"
    )
    df_fact = df_fact.join(
        df_dim_legal,
        on=["fully_legal", "semi_legal", "risk_legal"],
        how="left"
    )
    
    # Join dim_quality
    df_dim_quality = spark.table(DIM_QUALITY).select(
        "quality_key", "good_quality", "low_quality", "risk_quality"
    )
    df_fact = df_fact.join(
        df_dim_quality,
        on=["good_quality", "low_quality", "risk_quality"],
        how="left"
    )
    
    # Join dim_furnishing
    # Need to match on: is_furnishing, is_not_furnishing, param_furnishing_sell
    # Using coalesce with sentinel value to handle NULLs in the join
    df_dim_furnish = (
        spark.table(DIM_FURNISHING)
        .select(
            "furnishing_key",
            F.coalesce(F.col("is_furnishing"), F.lit(-1)).alias("is_furnishing_join"),
            F.coalesce(F.col("is_not_furnishing"), F.lit(-1)).alias("is_not_furnishing_join"),
        )
    )

    df_fact = (
        df_fact
        .withColumn("is_furnishing_join", F.coalesce(F.col("is_furnishing"), F.lit(-1)))
        .withColumn("is_not_furnishing_join", F.coalesce(F.col("is_not_furnishing"), F.lit(-1)))
    )
    
    df_fact = df_fact.join(
        df_dim_furnish,
        on=[
            "is_furnishing_join", 
            "is_not_furnishing_join", 
        ],
        how="left"
    ).drop("is_furnishing_join", "is_not_furnishing_join")
    
    # 4. Select final columns (FK + measures + flags + timestamps + engineered features)
    fact_cols = [
        # ===== DIMENSION KEYS =====
        "location_key", "time_key", "property_key", "seller_key",
        "legal_status_key", "quality_key", "furnishing_key",
        
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
        .createOrReplace()
    )
    
    print(f"  ✓ {FACT_PROPERTIES} created")


# ============================================================
# AGGREGATED FACT TABLES (Use Case-Specific)
# ============================================================

def build_fact_price_by_area_time(df_silver: DataFrame):
    print("\n=== BUILD fact_price_by_area_time ===")
    
    df = (
        df_silver
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("year_month", F.date_format(F.col("snapshot_date"), "yyyy-MM"))
    )
    
    df = (
        df
        .groupBy("area_name", "category_name", "year_month")
        .agg(
            F.round(F.avg("price"), 0).alias("price_avg"),
            F.round(F.percentile_approx("price", 0.5), 0).alias("price_median"),
            F.round(F.min("price"), 0).alias("price_min"),
            F.round(F.max("price"), 0).alias("price_max"),
            F.count("*").alias("listing_count"),
            F.stddev("price").alias("price_std_raw"),
        )
        .withColumn(
            "price_std",
            F.when(F.col("listing_count") >= 2, F.round(F.col("price_std_raw"), 0))
            .otherwise(F.lit(0))
        )
        .drop("price_std_raw")
    )
    
    count = df.count()
    print(f"  ✓ {count:,} records")
    df.writeTo(FACT_PRICE).using("iceberg").tableProperty("format-version", "2").createOrReplace()


def build_fact_quality_by_area_time(df_silver: DataFrame):
    """🏆 Quality analytics: area + month"""
    print("\n=== BUILD fact_quality_by_area_time ===")
    
    df = (
        df_silver
        .withColumn("year_month", F.date_format(F.col("snapshot_date"), "yyyy-MM"))
    )
    
    df = (
        df
        .groupBy("area_name", "year_month")
        .agg(
            F.round(
                F.sum(F.when(F.col("fully_legal") == 1, 1).otherwise(0)) / F.count("*") * 100,
                1
            ).alias("legal_fully_pct"),
            F.round(
                F.sum(F.when(F.col("good_quality") == 1, 1).otherwise(0)) / F.count("*") * 100,
                1
            ).alias("good_quality_pct"),
            F.round(F.avg("number_of_images"), 1).alias("images_avg"),
            F.count("*").alias("listing_count"),
        )
    )
    
    count = df.count()
    print(f"  ✓ {count:,} records")
    df.writeTo(FACT_QUALITY).using("iceberg").tableProperty("format-version", "2").createOrReplace()


def build_fact_seller_by_month(df_silver: DataFrame):
    """👤 Seller analytics: seller + month"""
    print("\n=== BUILD fact_seller_by_month ===")
    
    df = (
        df_silver
        .withColumn("year_month", F.date_format(F.col("snapshot_date"), "yyyy-MM"))
        .withColumn("seller_type", F.when(F.col("is_company_ad") == 1, "Company").otherwise("Personal"))
    )
    
    df = (
        df
        .groupBy("account_id", "account_name", "seller_type", "year_month")
        .agg(
            F.count("*").alias("listing_count"),
            F.round(F.avg("price"), 0).alias("price_avg"),
            F.round(F.avg("number_of_images"), 1).alias("images_avg"),
            F.round(
                F.sum(F.when(F.col("fully_legal") == 1, 1).otherwise(0)) / F.count("*") * 100,
                1
            ).alias("legal_fully_pct"),
        )
    )
    
    count = df.count()
    print(f"  ✓ {count:,} records")
    df.writeTo(FACT_SELLER).using("iceberg").tableProperty("format-version", "2").createOrReplace()


def build_fact_engagement_by_area_time(df_silver: DataFrame):
    """📱 Engagement analytics: area + category + week"""
    print("\n=== BUILD fact_engagement_by_area_time ===")
    
    df = (
        df_silver
        .withColumn("year_week", F.concat(
            F.year(F.col("snapshot_date")),
            F.lit("-"),
            F.lpad(F.weekofyear(F.col("snapshot_date")), 2, "0")
        ))
    )
    
    df = (
        df
        .groupBy("area_name", "category_name", "year_week")
        .agg(
            F.count("*").alias("listing_count"),
            F.round(F.avg("number_of_images"), 1).alias("images_avg"),
            F.round(
                F.sum(F.when(F.col("number_of_images") >= 3, 1).otherwise(0)) / F.count("*") * 100,
                1
            ).alias("rich_images_pct"),
            F.approx_count_distinct("account_id").alias("unique_sellers"),
        )
    )
    
    count = df.count()
    print(f"  ✓ {count:,} records")
    df.writeTo(FACT_ENGAGEMENT).using("iceberg").tableProperty("format-version", "2").createOrReplace()


def build_fact_daily_market(df_silver: DataFrame):
    """⏰ Daily market trends"""
    print("\n=== BUILD fact_daily_market ===")
    
    df = (
        df_silver
        .withColumn("date", F.to_date(F.col("snapshot_date")))
        .withColumn("month_name", F.date_format(F.col("snapshot_date"), "MMMM"))
        .withColumn("is_peak_season", F.when(F.month(F.col("snapshot_date")).isin([6, 7, 8, 11, 12]), 1).otherwise(0))
    )
    
    df = (
        df
        .groupBy("date", "month_name", "is_peak_season")
        .agg(
            F.count("*").alias("listing_count_total"),
            F.sum(F.when(F.col("is_company_ad") == 1, 1).otherwise(0)).alias("listing_count_company"),
            F.round(F.avg("price"), 0).alias("price_avg"),
            F.round(F.avg("number_of_images"), 1).alias("images_avg"),
            F.approx_count_distinct("account_id").alias("active_sellers"),
        )
    )
    
    count = df.count()
    print(f"  ✓ {count:,} records")
    df.writeTo(FACT_DAILY).using("iceberg").tableProperty("format-version", "2").createOrReplace()


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
            "snapshot_date", "is_company_ad"
        )
    )
    
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
    
    # Select final columns for fact table (OPTIMIZED: station_id as FK, not station details)
    fact_metro_cols = [
        # Identifiers
        "ad_id", "list_id",
        # Location
        "area_name", "ward_name", "latitude", "longitude",
        # Property attributes
        "price", "price_per_m2", "size", "category_name", "rooms",
        "is_company_ad",
        # Metro Foreign Key & Distance
        "station_id", "station_key",
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
        .createOrReplace()
    )
    
    print(f"  ✓ {FACT_METRO_ANALYSIS} created")


# ============================================================
# GGTREND ANALYSIS: Search Interest by Property Category
# ============================================================
def build_fact_trend_category_daily():
    print("\n=== BUILD fact_trend_category_daily ===")

    try:
        df_ggtrend = spark.table(GGTREND_SILVER)
        ggtrend_count = df_ggtrend.count()
        print(f"  ✓ Ggtrend data loaded: {ggtrend_count} rows")
    except AnalysisException as e:
        print(f"  ⚠️  Ggtrend table not found: {e}")
        print("  → Skipping ggtrend analysis")
        return

    # 1) Map keyword -> category_group thống nhất
    keyword_mapping = {
        "bat_dong_san_tp_hcm": "Văn phòng, Mặt bằng kinh doanh",
        "chung_cu_tp_hcm": "Căn hộ/Chung cư",
        "can_ho_tp_hcm": "Căn hộ/Chung cư",
        "mua_nha_tp_hcm": "Nhà ở",
        "dat_nen_tp_hcm": "Đất",
    }

    # 2) Unpivot ggtrend WIDE -> LONG
    dfs_long = []
    for col_name, category_group in keyword_mapping.items():
        df_long = (
            df_ggtrend
            .filter(F.col("ispartial") == False)
            .select(
                F.to_date(F.col("date")).alias("date"),
                F.lit(category_group).alias("category_group"),
                F.col(col_name).alias("search_volume")
            )
            .filter(F.col("search_volume").isNotNull())
        )
        dfs_long.append(df_long)

    df_ggtrend_long = dfs_long[0]
    for df in dfs_long[1:]:
        df_ggtrend_long = df_ggtrend_long.union(df)

    # 3) Gộp lại để tránh 2 dòng cùng ngày cho Căn hộ/Chung cư
    df_ggtrend_long = (
        df_ggtrend_long
        .groupBy("date", "category_group")
        .agg(F.sum("search_volume").alias("search_volume"))
    )

    print(f"  ✓ Ggtrend unpivoted to LONG format: {df_ggtrend_long.count()} records")

    # 4) Read silver
    df_silver = read_silver()
    if df_silver is None:
        print("  ❌ Silver table not available")
        return

    # 5) Chuẩn hóa category_name của listing về cùng category_group
    df_daily_props = (
        df_silver
        .filter(
            (F.col("category_name").isNotNull()) &
            (F.col("price").isNotNull()) &
            (F.col("price") > 0) &
            (F.col("snapshot_date").isNotNull())
        )
        .withColumn(
            "category_group",
            F.when(
                F.col("category_name").isin(
                    "Căn hộ", "Chung cư", "Căn hộ/Chung cư"
                ),
                "Căn hộ/Chung cư"
            ).when(
                F.col("category_name").isin(
                    "Nhà", "Nhà ở", "Nhà riêng"
                ),
                "Nhà ở"
            ).when(
                F.col("category_name").isin(
                    "Đất", "Đất nền"
                ),
                "Đất"
            ).when(
                F.col("category_name").isin(
                    "Văn phòng", "Mặt bằng kinh doanh", "Văn phòng, Mặt bằng kinh doanh"
                ),
                "Văn phòng, Mặt bằng kinh doanh"
            ).otherwise(None)
        )
        .filter(F.col("category_group").isNotNull())
        .groupBy(
            F.to_date(F.col("snapshot_date")).alias("date"),
            F.col("category_group")
        )
        .agg(
            F.count("ad_id").alias("listing_count"),
            F.round(F.avg("price"), 0).alias("avg_price"),
            F.round(F.percentile_approx("price", 0.5), 0).alias("median_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.stddev("price").alias("price_stddev")
        )
    )

    print(f"  ✓ Daily property stats computed: {df_daily_props.count()} date-category combinations")

    # Debug nhanh
    print("\n  Distinct category_group in ggtrend:")
    df_ggtrend_long.select("category_group").distinct().orderBy("category_group").show(truncate=False)

    print("\n  Distinct category_group in daily props:")
    df_daily_props.select("category_group").distinct().orderBy("category_group").show(truncate=False)

    # 6) Join
    df_fact_trend = (
        df_ggtrend_long
        .join(
            df_daily_props,
            on=["date", "category_group"],
            how="left"
        )
        .select(
            "date",
            "category_group",
            "search_volume",
            "listing_count",
            "avg_price",
            "median_price",
            "min_price",
            "max_price",
            "price_stddev"
        )
        .orderBy("date", "category_group")
    )

    count = df_fact_trend.count()
    print(f"  ✓ Created {count:,} trend-category records")

    print("\n  📊 Sample Trend Data (non-zero search volume):")
    df_fact_trend.filter(F.col("search_volume") > 0).show(10, truncate=False)

    print("\n  📈 Search Volume vs Price Correlation:")
    trend_summary = (
        df_fact_trend
        .groupBy("category_group")
        .agg(
            F.max("search_volume").alias("max_search_volume"),
            F.round(F.avg("search_volume"), 2).alias("avg_search_volume"),
            F.count("*").alias("days_tracked"),
            F.round(F.avg("avg_price"), 0).alias("avg_price_overall"),
            F.round(F.max("listing_count"), 0).alias("max_listings_in_day"),
            F.round(F.avg("listing_count"), 0).alias("avg_daily_listings")
        )
        .orderBy("category_group")
    )

    trend_summary.show(truncate=False)

    (
        df_fact_trend.writeTo(FACT_TREND_CATEGORY)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"  ✓ {FACT_TREND_CATEGORY} created")


# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def transform_to_gold_dw():
    """
    Main ETL function: orchestrate creation of facts + dims
    """
    print("\n" + "="*60)
    print("GOLD DW: Building Facts & Dimensions (Star Schema)")
    print("="*60)
    
    # Create namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    print(f"✓ Namespace ready: {GOLD_NAMESPACE}")
    
    # Read Silver
    df_silver = read_silver()
    if df_silver is None or df_silver.rdd.isEmpty():
        print("❌ GOLD DW: No data from Silver table")
        return
    
    # Build dimensions
    build_dim_time()
    build_dim_location(df_silver)
    build_dim_property(df_silver)
    build_dim_seller(df_silver)
    build_dim_legal_status(df_silver)
    build_dim_quality(df_silver)
    build_dim_furnishing(df_silver)
    build_dim_metro_station()  # Must be before metro analysis fact table
    
    # Build fact tables
    build_fact_properties(df_silver)
    
    # Build aggregated fact tables (use case-specific)
    build_fact_price_by_area_time(df_silver)
    build_fact_quality_by_area_time(df_silver)
    build_fact_seller_by_month(df_silver)
    build_fact_engagement_by_area_time(df_silver)
    build_fact_daily_market(df_silver)
    
    # Build metro proximity analysis (requires dim_metro_station)
    build_fact_properties_metro_analysis(df_silver)
    
    # Build ggtrend analysis (search interest by category)
    build_fact_trend_category_daily()
    
    print("\n" + "="*60)
    print("✓ GOLD DW: All Facts & Dimensions created successfully")
    print("="*60)
    print(f"""
    ✓ Synchronized Dimensions (matching Silver exactly):
      - {DIM_TIME}: Time attributes for analytics
      - {DIM_LOCATION}: Geo attributes + area_name, ward_name, street_name (+ encoded variants)
      - {DIM_PROPERTY}: category_name, is_land, is_apt, is_house, rooms, has_rooms + rooms_category
      - {DIM_SELLER}: account_id, account_name, is_company_ad, is_personal_ad + seller_tier
      - {DIM_LEGAL_STATUS}: fully_legal, semi_legal, risk_legal + legal_category
      - {DIM_QUALITY}: good_quality, low_quality, risk_quality + quality_tier
      - {DIM_FURNISHING}: is_furnishing, is_not_furnishing, param_furnishing_sell + furnishing_category
      - {DIM_METRO_STATION}: station_id, station_code, station_name, station_type, area_name, ward_name
    
    ✓ Atomic Fact Table:
      - {FACT_PROPERTIES} (all properties with full features)
    
    ✓ 6 Aggregated Fact Tables (Use Case-Specific):
      - {FACT_PRICE}: Price by area + category + month
      - {FACT_QUALITY}: Quality by area + month
      - {FACT_SELLER}: Seller performance by month
      - {FACT_ENGAGEMENT}: Engagement by area + category + week
      - {FACT_DAILY}: Daily market trends
      - {FACT_METRO_ANALYSIS}: Properties with metro proximity (FK: station_id → dim_metro_station)
    
    ✓ 1 Trend Analysis Fact Table:
      - {FACT_TREND_CATEGORY}: Daily search volume by category + property stats (search interest → price correlation)
    
    ✓ Ready for ANALYTICS, BI, & ML queries!
    """)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    transform_to_gold_dw()
    spark.stop()
