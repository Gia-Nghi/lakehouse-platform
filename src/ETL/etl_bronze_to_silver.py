from __future__ import annotations

import argparse
import os
import sys
import re
import json
import unicodedata
import time
from typing import Optional, Sequence, Any, Dict, List
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, DateType, TimestampType
from pyspark.sql.utils import AnalysisException
# ============================================================
# SPARK SESSION + ICEBERG CATALOG
# ============================================================
def build_spark() -> SparkSession:
    """
    Build Spark session from environment variables so this Bronze→Silver job
    uses the same Iceberg/Nessie/MinIO runtime configuration as the
    Silver→Gold job.
    """
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
    nessie_ref = os.getenv("NESSIE_REF", "main")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")

    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    return (
        SparkSession.builder.appName("Lakehouse-Chotot-Bronze-To-Silver")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", nessie_uri)
        .config("spark.sql.catalog.lakehouse.ref", nessie_ref)
        .config("spark.sql.catalog.lakehouse.warehouse", iceberg_warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )

spark = build_spark()
#Timezone xử lý SQL
spark.sql("SET spark.sql.session.timeZone = UTC")
# ============================================================
# CẤU HÌNH ĐƯỜNG DẪN + TÊN BẢNG
# ============================================================
# Defaults are aligned with etl_silver_to_gold_facts_and_dims.py.
# The transformation logic below is unchanged; only runtime names/paths are
# configurable so Airflow/Spark can run the Bronze→Silver→Gold chain end-to-end.
PROCESS_DATE = os.getenv("PROCESS_DATE")

if PROCESS_DATE:
    RAW_JSON_PATH = os.getenv(
        "BRONZE_PATH",
        f"s3a://lakehouse/bronze/market_listings/chotot/date={PROCESS_DATE}/"
    )
else:
    # fallback: đọc toàn bộ prefix trong bucket bronze
    RAW_JSON_PATH = os.getenv(
        "BRONZE_PATH",
        "s3a://lakehouse/bronze/market_listings/chotot/"
    )

SILVER_TABLE = os.getenv(
    "SILVER_TABLE",
    "lakehouse.silver.chotot_cleaned"
)
INGEST_LOG_TABLE = os.getenv(
    "INGEST_LOG_TABLE",
    "lakehouse.meta.bronze_to_silver_files"
)

# Global mapping for area_name and ward_name (shared across nhatot and metro)
AREA_NAME_MAPPING = {}
WARD_NAME_MAPPING = {}
# ============================================================
# COLUMNS TO SELECT FOR PROCESSING (INPUT từ Bronze)
# ============================================================
COLUMNS_TO_SELECT = [
    'snapshot_date','crawled_at_unix','ad_id','list_id','subject','body','account_id','account_name','price','size','width','length',
    'rooms','category_name','region_name','area_name','ward_name','street_name','latitude','longitude','number_of_images','company_ad','param_furnishing_sell',
    'param_property_legal_document','param_price_m2','param_pty_characteristics',
]
# ============================================================
# BRONZE LAYER (READ-ONLY, AUTO-FLATTEN list/detail)
# ============================================================
def read_bronze():
    print(f"=== BRONZE: Reading raw JSON data from: {RAW_JSON_PATH} ===")
    try:
        df_raw = (
            spark.read
            .option("multiLine", False)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(RAW_JSON_PATH)
        )
    except AnalysisException:
        # Không có file nào khớp path → không làm gì
        print(f"!!! Không tìm thấy dữ liệu bronze ở path: {RAW_JSON_PATH} → dừng job.")
        return None
    df = (
        df_raw
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name())
    )
    
    return df

def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        # Catch all exceptions: AnalysisException, NotFoundException, etc.
        return False
# ============================================================
# PARAMS FLATTENING HELPERS (Python-based for flatten_record)
# ============================================================
def standardize_colname(col: str) -> str:
    col = str(col).strip().lower()
    col = unicodedata.normalize("NFD", col)
    col = "".join(c for c in col if unicodedata.category(c) != "Mn")  # Remove combining marks
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "_", col)  # Keep only alphanumeric and underscore
    col = re.sub(r"_+", "_", col).strip("_")  # Collapse multiple underscores
    return col

def extract_snapshot_date_from_path(source_path: str) -> Optional[str]:
    if not source_path or not isinstance(source_path, str):
        return None

    match = re.search(r'date=(\d{4}-\d{2}-\d{2})', source_path)
    if match:
        return match.group(1)
    return None

def flatten_bronze_with_params(df: DataFrame) -> DataFrame:
    print("=== Flattening nested Bronze JSON structures ===")
    
    # ========== STEP 0: EXTRACT snapshot_date FROM FOLDER PATH ==========
    print("\n  >>> EXTRACTING snapshot_date FROM FOLDER PATH <<<")
    if "snapshot_date" not in df.columns:
        # Create UDF for path extraction
        extract_date_udf = F.udf(extract_snapshot_date_from_path, StringType())
        
        if "_source_path" in df.columns:
            df = df.withColumn("snapshot_date", extract_date_udf(F.col("_source_path")))
            
            # Check how many were successfully extracted
            snapshot_count = df.filter(F.col("snapshot_date").isNotNull()).count()
            total_count = df.count()
            
            print(f"    ✓ Extracted snapshot_date from _source_path")
            print(f"      Extracted: {snapshot_count:,} / {total_count:,} rows ({snapshot_count/total_count*100:.1f}%)")
            
            # Show sample values
            sample_dates = df.select("_source_path", "snapshot_date").distinct().limit(3).collect()
            for row in sample_dates:
                print(f"      Sample: {row['_source_path']} → {row['snapshot_date']}")
        else:
            print(f"    ⚠ _source_path column not found, cannot extract snapshot_date")
            df = df.withColumn("snapshot_date", F.lit(None).cast("date"))
    else:
        print(f"    ✓ snapshot_date already exists in dataframe")
    
    # ========== STEP 0b: ENSURE crawled_at_unix FROM ROOT LEVEL ==========
    print("\n  >>> ENSURING crawled_at_unix FROM ROOT LEVEL <<<")
    if "crawled_at_unix" not in df.columns:
        # Try to use crawled_at field if it exists at root level
        if "crawled_at" in df.columns:
            df = df.withColumn("crawled_at_unix", F.col("crawled_at"))
            print(f"    ✓ Copied root.crawled_at → crawled_at_unix")
        else:
            print(f"    ⚠ Neither crawled_at_unix nor crawled_at found at root level")
    else:
        print(f"    ✓ crawled_at_unix already exists (from root level)")
    
    # ========== STEP 1: FLATTEN 'list' STRUCT COMPLETELY ==========
    print("\n  >>> FLATTENING 'list' STRUCT <<<")
    if "list" in df.columns:
        list_schema = df.schema["list"].dataType
        if hasattr(list_schema, 'fields'):
            list_fields = [f.name for f in list_schema.fields]
            print(f"    Found {len(list_fields)} fields in list struct: {sorted(list_fields)}")
            
            for field in list_fields:
                if field not in df.columns:
                    try:
                        df = df.withColumn(field, F.col(f"list.{field}"))
                        print(f"      ✓ Extracted list.{field}")
                    except Exception as e:
                        print(f"      ⚠ Could not extract list.{field}: {e}")
    else:
        print(f"    ⚠ 'list' struct not found in Bronze data")
    
    # ========== STEP 2: FLATTEN 'detail' STRUCT COMPLETELY ==========
    print("\n  >>> FLATTENING 'detail' STRUCT <<<")
    if "detail" in df.columns:
        detail_schema = df.schema["detail"].dataType
        if hasattr(detail_schema, 'fields'):
            detail_fields = [f.name for f in detail_schema.fields]
            print(f"    Found {len(detail_fields)} fields in detail struct: {sorted(detail_fields)}")
            
            for field in detail_fields:
                # Skip nested structs/arrays that need special handling
                if field in {'ad', 'parameters', 'ad_params'}:
                    print(f"      ⊘ Skipping nested struct: detail.{field} (will handle separately)")
                    continue
                
                if field not in df.columns:
                    try:
                        df = df.withColumn(field, F.col(f"detail.{field}"))
                        print(f"      ✓ Extracted detail.{field}")
                    except Exception as e:
                        print(f"      ⚠ Could not extract detail.{field}: {e}")
    else:
        print(f"    ⚠ 'detail' struct not found in Bronze data")
    
    # ========== STEP 3: FLATTEN 'detail.ad' NESTED STRUCT ==========
    print("\n  >>> FLATTENING 'detail.ad' NESTED STRUCT <<<")
    if "detail" in df.columns:
        try:
            detail_schema = df.schema["detail"].dataType
            if hasattr(detail_schema, 'fields'):
                detail_field_names = [f.name for f in detail_schema.fields]
                if "ad" in detail_field_names:
                    # Get ad schema
                    ad_schema = None
                    for f in detail_schema.fields:
                        if f.name == "ad":
                            ad_schema = f.dataType
                            break
                    
                    if ad_schema and hasattr(ad_schema, 'fields'):
                        ad_fields = [f.name for f in ad_schema.fields]
                        print(f"    Found {len(ad_fields)} fields in detail.ad: {sorted(ad_fields)}")
                        
                        for field in ad_fields:
                            if field == 'params':
                                print(f"      ⊘ Skipping detail.ad.params (will handle separately)")
                                continue
                            
                            col_name = f"ad_{field}"
                            if col_name not in df.columns and field not in df.columns:
                                try:
                                    df = df.withColumn(col_name, F.col(f"detail.ad.{field}"))
                                    print(f"      ✓ Extracted detail.ad.{field} → {col_name}")
                                except Exception as e:
                                    print(f"      ⚠ Could not extract detail.ad.{field}: {e}")
        except Exception as e:
            print(f"    ⚠ Error processing detail.ad: {e}")
    
    # ========== STEP 4: EXTRACT PARAMS FROM 3 SOURCES ==========
    print("\n  >>> EXTRACTING PARAMS (detail.ad_params, detail.parameters, detail.ad.params) <<<")
    if "detail" in df.columns:
        try:
            detail_json = F.to_json(F.col("detail"))
            
            param_mappings = [
                ('param_furnishing_sell', '$.ad_params.furnishing_sell.value'),
                ('param_property_legal_document', '$.ad_params.property_legal_document.value'),
                ('param_price_m2', '$.ad_params.price_m2.value'),
                ('param_pty_characteristics', '$.ad_params.pty_characteristics.value'),
            ]
            
            for col_name, json_path in param_mappings:
                if col_name not in df.columns:
                    extracted = F.get_json_object(detail_json, json_path)
                    df = df.withColumn(col_name, extracted.cast("string"))
                    print(f"      ✓ Extracted {col_name} from {json_path}")
            
            print(f"  ✓ Extracted params from detail.ad_params")
        except Exception as e:
            print(f"  ⚠ Error extracting params: {e}")
    
    # Ensure all required param columns exist
    param_cols = [
        'param_furnishing_sell',
        'param_property_legal_document', 
        'param_price_m2',
        'param_pty_characteristics'
    ]
    for col_name in param_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("string"))
    
    print(f"\n✓ Flattening complete: {len(df.columns)} total columns")
    print(f"  Current columns: {sorted(df.columns)}")
    return df

def dedup_by_list_id(df: DataFrame) -> DataFrame:
    if "list_id" not in df.columns:
        print("=== DEDUP LIST_ID: list_id column not found, skipping ===")
        return df
    
    rows_before = df.count()
    
    # Determine which timestamp column to use
    order_cols = []
    if "list_time" in df.columns:
        order_cols.append(F.col("list_time").desc_nulls_last())
    if "_ingest_ts" in df.columns:
        order_cols.append(F.col("_ingest_ts").desc_nulls_last())
    
    if not order_cols:
        print("=== DEDUP LIST_ID: No timestamp columns found, cannot order properly ===")
        return df
    
    # Create window: partition by list_id, order by timestamp(s)
    w = Window.partitionBy("list_id").orderBy(*order_cols)
    
    # Keep only row_number = 1 (most recent)
    df_dedup = (
        df
        .withColumn("_rn_list_id", F.row_number().over(w))
        .filter(F.col("_rn_list_id") == 1)
        .drop("_rn_list_id")
    )
    
    rows_after = df_dedup.count()
    removed = rows_before - rows_after
    
    print(f"=== DEDUP LIST_ID: before={rows_before:,}, after={rows_after:,}, removed={removed:,} ===")
    return df_dedup

def filter_null_addresses(df: DataFrame) -> DataFrame:
    # Cột địa chỉ bắt buộc
    cols_null_filter = ["region_name", "area_name", "street_name", "ward_name"]
    
    # Kiểm tra cột tồn tại
    available_cols = [c for c in cols_null_filter if c in df.columns]
    
    if not available_cols:
        print("=== FILTER NULL ADDRESSES: Không có cột địa chỉ, bỏ qua ===")
        return df
    
    rows_before = df.count()
    
    # Tạo điều kiện: loại bỏ khi ANY cột địa chỉ là NULL
    null_condition = F.lit(False)
    for col in available_cols:
        null_condition = null_condition | F.col(col).isNull()
    
    df_filtered = df.filter(~null_condition)
    rows_after = df_filtered.count()
    removed = rows_before - rows_after
    
    print(f"=== FILTER NULL ADDRESSES: checked cols={available_cols}, before={rows_before:,}, after={rows_after:,}, removed={removed:,} ===")
    return df_filtered

def fill_null_number_of_images(df: DataFrame) -> DataFrame:
    if "number_of_images" not in df.columns:
        print("=== FILL NULL IMAGES: number_of_images column not found, skipping ===")
        return df
    
    # Tính median (dùng percentile_approx cho performance)
    median_result = df.agg(
        F.expr("percentile_approx(number_of_images, 0.5, 10000)").alias("median")
    ).collect()
    median_images = median_result[0]["median"] if median_result else None
    
    if median_images is None:
        print("=== FILL NULL IMAGES: Cannot calculate median (all NULL?), skipping ===")
        return df
    
    # Count NULL before fill
    null_before = df.filter(F.col("number_of_images").isNull()).count()
    # Fill NULL với median
    df_filled = df.withColumn(
        "number_of_images",
        F.coalesce(F.col("number_of_images"), F.lit(float(median_images)))
    )
    null_after = df_filled.filter(F.col("number_of_images").isNull()).count()
    print(f"=== FILL NULL IMAGES: median={median_images:.2f}, before={null_before:,} NULLs, after={null_after:,} NULLs ===")
    return df_filled

def handle_rooms_null(df: DataFrame) -> DataFrame:
    if "category_name" not in df.columns or "rooms" not in df.columns:
        print("=== HANDLE ROOMS NULL: Missing category_name or rooms, skipping ===")
        return df
    
    # Định nghĩa loại hình
    land_types = ['Đất', 'Văn phòng, Mặt bằng kinh doanh']
    apt_types = ['Căn hộ/Chung cư']
    house_types = ['Nhà ở']
    
    # Bước 1: Tạo 3 cột indicator loại hình
    df = df.withColumn(
        "is_land",
        F.when(F.col("category_name").isin(land_types), 1).otherwise(0).cast("int")
)
    df = df.withColumn(
        "is_apt",
        F.when(F.col("category_name").isin(apt_types), 1).otherwise(0).cast("int")
    )
    df = df.withColumn(
        "is_house",
        F.when(F.col("category_name").isin(house_types), 1).otherwise(0).cast("int")
    )
    
    # Bước 2: Tạo has_rooms dựa trên logic loại hình
    # has_rooms = 1 nếu là apt hoặc house (nên có rooms)
    # has_rooms = 0 nếu là land hoặc khác
    df = df.withColumn(
        "has_rooms",
        F.when((F.col("is_apt") == 1) | (F.col("is_house") == 1), 1).otherwise(0).cast("int")
    )
    
    # Count NULL before fill
    rooms_null_before = df.filter(F.col("rooms").isNull()).count()
    
    # Bước 3: Fill NULL trong rooms = 0
    df = df.withColumn(
        "rooms",
        F.coalesce(F.col("rooms"), F.lit(0.0))
    )
    rooms_null_after = df.filter(F.col("rooms").isNull()).count()
    
    # Tính statistics
    is_land_count = df.filter(F.col("is_land") == 1).count()
    is_apt_count = df.filter(F.col("is_apt") == 1).count()
    is_house_count = df.filter(F.col("is_house") == 1).count()
    
    print(f"=== HANDLE ROOMS NULL ===")
    print(f"    is_land=1: {is_land_count:,}")
    print(f"    is_apt=1: {is_apt_count:,}")
    print(f"    is_house=1: {is_house_count:,}")
    print(f"    rooms NULL: before={rooms_null_before:,}, after={rooms_null_after:,}, filled={(rooms_null_before - rooms_null_after):,}")
    return df

def flag_furnishing_sell(df: DataFrame) -> DataFrame:
    if "param_furnishing_sell" not in df.columns:
        print("=== FLAG FURNISHING: param_furnishing_sell column not found, skipping ===")
        return df
    
    # Định nghĩa các loại nội thất
    not_furnishing_types = ['Bàn giao thô', 'Hoàn thiện cơ bản']
    furnishing_types = ['Nội thất cao cấp', 'Nội thất đầy đủ']
    
    # Khởi tạo: mặc định là không nội thất (0)
    df = df.withColumn("is_furnishing", F.lit(0).cast("int"))
    df = df.withColumn("is_not_furnishing", F.lit(0).cast("int"))
    
    # Bước 1: Gắn cờ "có nội thất" → chỉ cho furnishing_types
    df = df.withColumn(
        "is_furnishing",
        F.when(F.col("param_furnishing_sell").isin(furnishing_types), 1).otherwise(0).cast("int")
    )
    
    # Bước 2: Gắn cờ "không nội thất" → cho not_furnishing_types HOẶC NULL/unknown
    # is_not_furnishing = 1 khi is_furnishing = 0 (tất cả trường hợp không phải có nội thất)
    df = df.withColumn(
        "is_not_furnishing",
        F.when(F.col("is_furnishing") == 0, 1).otherwise(0).cast("int")
    )
    
    # Tính statistics
    not_furnishing_count = df.filter(F.col("is_not_furnishing") == 1).count()
    furnishing_count = df.filter(F.col("is_furnishing") == 1).count()
    
    # Chi tiết loại nội thất
    not_furnishing_explicit = df.filter(F.col("param_furnishing_sell").isin(not_furnishing_types)).count()
    furnishing_explicit = df.filter(F.col("param_furnishing_sell").isin(furnishing_types)).count()
    null_unknown = not_furnishing_count - not_furnishing_explicit
    
    print(f"=== FLAG FURNISHING ===")
    print(f"    is_furnishing=1: {furnishing_count:,} (Nội thất cao cấp, Nội thất đầy đủ)")
    print(f"    is_not_furnishing=1: {not_furnishing_count:,}")
    print(f"      ├─ Bàn giao thô, Hoàn thiện cơ bản: {not_furnishing_explicit:,}")
    print(f"      └─ NULL / Không xác định: {null_unknown:,}")
    return df

def flag_property_quality(df: DataFrame) -> DataFrame:
    if "param_pty_characteristics" not in df.columns:
        print("=== FLAG PROPERTY QUALITY: param_pty_characteristics column not found, skipping ===")
        return df
    
    # Định nghĩa các loại chất lượng
    good_types = ['Hẻm xe hơi', 'Nhà nở hậu', 'Nở hậu', 'Mặt tiền', 'Thổ cư toàn bộ']
    risk_types = ['Đất chưa chuyển thổ', 'Nhà chưa hoàn công', 'Nhà dính quy hoạch / lộ giới', 
                  'Chưa có thổ cư', 'Không có thổ cư', 'Thổ cư 1 phần']
    
    # Khởi tạo: mặc định tất cả = 0
    df = df.withColumn("good_quality", F.lit(0).cast("int"))
    df = df.withColumn("risk_quality", F.lit(0).cast("int"))
    df = df.withColumn("low_quality", F.lit(0).cast("int"))
    
    # Chuyển column thành lowercase để search case-insensitive
    df = df.withColumn(
        "pty_text_lower",
        F.lower(F.coalesce(F.col("param_pty_characteristics").cast("string"), F.lit("")))
    )
    
    # Tạo boolean masks dựa trên pty_text_lower (giờ đã có trong dataframe)
    # good_mask: chứa good_types nhưng KHÔNG chứa risk_types
    good_mask = F.lit(False)
    for good_term in good_types:
        good_mask = good_mask | F.col("pty_text_lower").contains(good_term.lower())
    
    risk_mask = F.lit(False)
    for risk_term in risk_types:
        risk_mask = risk_mask | F.col("pty_text_lower").contains(risk_term.lower())
    
    # Bước 1: Gắn cờ risk_quality = 1 nếu chứa từ rủi ro
    df = df.withColumn(
        "risk_quality",
        F.when(risk_mask, 1).otherwise(0).cast("int")
    )
    
    # Bước 2: Gắn cờ good_quality = 1 nếu chứa từ tốt AND KHÔNG chứa từ rủi ro
    df = df.withColumn(
        "good_quality",
        F.when(good_mask & ~risk_mask, 1).otherwise(0).cast("int")
    )
    
    # Bước 3: Gắn cờ low_quality = 1 nếu không phải good cũng không phải risk
    df = df.withColumn(
        "low_quality",
        F.when((F.col("good_quality") == 0) & (F.col("risk_quality") == 0), 1).otherwise(0).cast("int")
    )
    
    # Drop temp column
    df = df.drop("pty_text_lower")
    
    # Tính statistics
    good_quality_count = df.filter(F.col("good_quality") == 1).count()
    risk_quality_count = df.filter(F.col("risk_quality") == 1).count()
    low_quality_count = df.filter(F.col("low_quality") == 1).count()
    
    print(f"=== FLAG PROPERTY QUALITY ===")
    print(f"    good_quality=1: {good_quality_count:,} (Hẻm xe hơi, Mặt tiền, Thổ cư toàn bộ...)")
    print(f"    risk_quality=1: {risk_quality_count:,} (Chưa hoàn công, Chưa thổ cư...)")
    print(f"    low_quality=1: {low_quality_count:,} (không xác định hoặc NULL)")
    return df

def flag_advertiser_type(df: DataFrame) -> DataFrame:
    if "company_ad" not in df.columns:
        print("=== FLAG ADVERTISER TYPE: company_ad column not found, skipping ===")
        return df
    
    # Khởi tạo 2 cột flag = 0
    df = df.withColumn("is_company_ad", F.lit(0).cast("int"))
    df = df.withColumn("is_personal_ad", F.lit(0).cast("int"))
    
    # Bước 1: is_company_ad = 1 khi company_ad == 1 (môi giới)
    df = df.withColumn(
        "is_company_ad",
        F.when(F.col("company_ad") == 1, 1).otherwise(0).cast("int")
    )
    
    # Bước 2: is_personal_ad = 1 khi company_ad == 0 (chính chủ)
    df = df.withColumn(
        "is_personal_ad",
        F.when(F.col("company_ad") == 0, 1).otherwise(0).cast("int")
    )
    
    # Tính statistics
    company_ad_count = df.filter(F.col("is_company_ad") == 1).count()
    personal_ad_count = df.filter(F.col("is_personal_ad") == 1).count()
    unknown_ad_count = df.filter(
        (F.col("is_company_ad") == 0) & (F.col("is_personal_ad") == 0)
    ).count()
    
    print(f"=== FLAG ADVERTISER TYPE ===")
    print(f"    is_company_ad=1: {company_ad_count:,} (Môi giới)")
    print(f"    is_personal_ad=1: {personal_ad_count:,} (Chính chủ)")
    print(f"    Không xác định: {unknown_ad_count:,}")
    return df

def flag_legal_document(df: DataFrame) -> DataFrame:
    if "param_property_legal_document" not in df.columns:
        print("=== FLAG LEGAL DOCUMENT: param_property_legal_document column not found, skipping ===")
        return df
    
    # Định nghĩa các loại pháp lý
    fully_legal_types = ['Đã có sổ', 'Sổ hồng riêng']
    semi_legal_types = ['Đang chờ sổ', 'Sổ chung / Công chứng vi bằng', 'Hợp đồng mua bán', 'Hợp đồng đặt cọc']
    risk_legal_types = ['Không có sổ', 'Giấy tờ viết tay', 'Giấy tờ khác', '1']
    
    # Khởi tạo 3 cột flag = 0
    df = df.withColumn("fully_legal", F.lit(0).cast("int"))
    df = df.withColumn("semi_legal", F.lit(0).cast("int"))
    df = df.withColumn("risk_legal", F.lit(0).cast("int"))
    
    # Bước 1: fully_legal = 1 khi param_property_legal_document in fully_legal_types
    df = df.withColumn(
        "fully_legal",
        F.when(F.col("param_property_legal_document").isin(fully_legal_types), 1).otherwise(0).cast("int")
    )
    
    # Bước 2: semi_legal = 1 khi param_property_legal_document in semi_legal_types
    df = df.withColumn(
        "semi_legal",
        F.when(F.col("param_property_legal_document").isin(semi_legal_types), 1).otherwise(0).cast("int")
    )
    
    # Bước 3: risk_legal = 1 khi in risk_legal_types OR NULL
    df = df.withColumn(
        "risk_legal",
        F.when(
            F.col("param_property_legal_document").isin(risk_legal_types) | F.col("param_property_legal_document").isNull(),
            1
        ).otherwise(0).cast("int")
    )
    
    # Tính statistics
    fully_legal_count = df.filter(F.col("fully_legal") == 1).count()
    semi_legal_count = df.filter(F.col("semi_legal") == 1).count()
    risk_legal_count = df.filter(F.col("risk_legal") == 1).count()
    
    # Chi tiết risk_legal
    risk_explicit = df.filter(F.col("param_property_legal_document").isin(risk_legal_types)).count()
    risk_null = risk_legal_count - risk_explicit
    
    print(f"=== FLAG LEGAL DOCUMENT ===")
    print(f"    fully_legal=1: {fully_legal_count:,} (Đã có sổ, Sổ hồng riêng)")
    print(f"    semi_legal=1: {semi_legal_count:,} (Đang chờ sổ, Hợp đồng...)")
    print(f"    risk_legal=1: {risk_legal_count:,}")
    print(f"      ├─ Không có sổ, Giấy tờ khác: {risk_explicit:,}")
    print(f"      └─ NULL: {risk_null:,}")
    return df

def round_size_column(df: DataFrame) -> DataFrame:
    if "size" not in df.columns:
        print("=== ROUND SIZE: size column not found, skipping ===")
        return df
    
    # Count NULL before rounding
    size_null_count = df.filter(F.col("size").isNull()).count()
    
    # Round size to 2 decimal places
    df = df.withColumn(
        "size",
        F.round(F.col("size"), 2).cast("float")
    )
    
    # Count after (should be same as before + any conversions)
    size_valid_count = df.filter(F.col("size").isNotNull()).count()
    
    print(f"=== ROUND SIZE ===")
    print(f"    Làm tròn size tới 2 chữ số thập phân")
    print(f"    size NULL: {size_null_count:,}")
    print(f"    size valid: {size_valid_count:,}")
    return df

def clean_ward_name(df: DataFrame) -> DataFrame:
    print("\n>>> Cleaning ward_name: Remove '(Quận...cũ)' pattern")
    
    if "ward_name" not in df.columns:
        print("⚠ ward_name column not found")
        return df
    
    # Count records with pattern before
    ward_before = df.filter(
        F.col("ward_name").rlike(r'\(Quận.*?cũ\)')
    ).count()
    
    # Remove pattern using regex and trim
    df = df.withColumn(
        "ward_name",
        F.trim(
            F.regexp_replace(
                F.col("ward_name"),
                r'\s*\(Quận.*?cũ\)',
                ""
            )
        )
    )
    
    # Count records with pattern after
    ward_after = df.filter(
        F.col("ward_name").rlike(r'\(Quận.*?cũ\)')
    ).count()
    
    print(f"  Pattern '(Quận...cũ)' found before: {ward_before:,} records")
    print(f"  Pattern '(Quận...cũ)' found after:  {ward_after:,} records")
    print(f"  ✓ {ward_before - ward_after:,} records cleaned")
    return df

def clean_param_price_m2(df: DataFrame) -> DataFrame:
    print("\n>>> Cleaning param_price_m2: Extract numeric values")
    
    if "param_price_m2" not in df.columns:
        print("⚠ param_price_m2 column not found")
        return df
    
    # Count records with NULL before
    null_before = df.filter(F.col("param_price_m2").isNull()).count()
    
    # Use Python UDF for flexible numeric extraction
    def extract_numeric(value):
        """Extract first numeric sequence, handle decimal/thousands separator"""
        if value is None or value == "":
            return None
        try:
            value_str = str(value).strip()
            # Extract first numeric sequence (digits, comma, decimal point)
            import re
            match = re.search(r'[\d,\.]+', value_str)
            if match:
                num_str = match.group(0)
                # Replace Vietnamese comma with period for decimal
                num_str = num_str.replace(',', '.')
                return float(num_str)
            return None
        except (ValueError, AttributeError):
            return None
    
    extract_numeric_udf = F.udf(extract_numeric, DoubleType())
    
    # Apply UDF to extract numeric values
    df = df.withColumn("param_price_m2", extract_numeric_udf(F.col("param_price_m2")))
    
    # Count records with NULL after
    null_after = df.filter(F.col("param_price_m2").isNull()).count()
    print(f"  Extracted numeric values from param_price_m2")
    print(f"  NULL before: {null_before:,} records")
    print(f"  NULL after:  {null_after:,} records")
    print(f"  Extracted: {null_before - null_after:,} records")
    return df

def encode_category_name(df: DataFrame) -> DataFrame:
    print("\n>>> One-Hot Encoding category_name")
    
    if "category_name" not in df.columns:
        print("⚠ category_name column not found")
        return df
    
    # Get unique categories (excluding NULL)
    unique_cats = df.filter(F.col("category_name").isNotNull()).select(
        F.col("category_name")
    ).distinct().collect()
    
    categories = [row[0] for row in unique_cats]
    categories = sorted(categories)  # Sort for consistent column order
    
    print(f"  Found {len(categories)} unique categories: {categories}")
    
    # Create one-hot encoded columns for each category
    for cat in categories:
        col_name = f"cat_{standardize_colname(cat)}"
        df = df.withColumn(
            col_name,
            F.when(F.col("category_name") == cat, 1).otherwise(0).cast("int")
        )
    
    cat_cols = [c for c in df.columns if c.startswith("cat_")]
    print(f"  ✓ Created {len(cat_cols)} one-hot columns: {cat_cols}")
    print(f"  ✓ Kept original column: category_name")
    return df

def build_area_name_mapping(df: DataFrame) -> Dict[str, int]:
    """Build area_name to code mapping from nhatot data"""
    print("\n>>> Building area_name mapping from nhatot data")
    
    if "area_name" not in df.columns:
        print("⚠ area_name column not found")
        return {}
    
    # Get unique non-NULL values and create mapping
    unique_vals = (
        df.filter(F.col("area_name").isNotNull())
        .select(F.col("area_name").alias("value"))
        .distinct()
        .rdd.map(lambda r: r[0])
        .collect()
    )
    
    # Sort for consistent encoding
    unique_vals = sorted(unique_vals)
    
    # Create mapping dictionary
    mapping_dict = {val: idx for idx, val in enumerate(unique_vals)}
    
    print(f"  ✓ Created mapping for {len(unique_vals)} unique area names")
    print(f"  Areas: {unique_vals}")
    
    return mapping_dict

def build_ward_name_mapping(df: DataFrame) -> Dict[str, int]:
    """Build ward_name to code mapping from nhatot data"""
    print("\n>>> Building ward_name mapping from nhatot data")
    
    if "ward_name" not in df.columns:
        print("⚠ ward_name column not found")
        return {}
    
    # Get unique non-NULL values and create mapping
    unique_vals = (
        df.filter(F.col("ward_name").isNotNull())
        .select(F.col("ward_name").alias("value"))
        .distinct()
        .rdd.map(lambda r: r[0])
        .collect()
    )
    
    # Sort for consistent encoding
    unique_vals = sorted(unique_vals)
    
    # Create mapping dictionary
    mapping_dict = {val: idx for idx, val in enumerate(unique_vals)}
    
    print(f"  ✓ Created mapping for {len(unique_vals)} unique ward names")
    print(f"  Ward names: {unique_vals[:10]}{'...' if len(unique_vals) > 10 else ''}")
    
    return mapping_dict

def encode_area_with_shared_mapping(df: DataFrame, area_mapping: Dict[str, int]) -> DataFrame:
    """Encode area_name using shared mapping"""
    print("\n>>> Encoding area_name with shared mapping")
    
    if "area_name" not in df.columns:
        print("⚠ area_name column not found")
        return df
    
    if not area_mapping:
        print("⚠ Shared area_name mapping is empty")
        return df
    
    # Convert to Spark broadcast for efficient lookup
    mapping_broadcast = spark.sparkContext.broadcast(area_mapping)
    
    # Define UDF to encode values
    def encode_value(value):
        if value is None:
            return -1
        return mapping_broadcast.value.get(value, -1)
    
    encode_udf = F.udf(encode_value, IntegerType())
    
    # Apply encoding
    df = df.withColumn("area_name_encoded", encode_udf(F.col("area_name")))
    
    # Count how many were successfully mapped
    mapped_count = df.filter(F.col("area_name_encoded") >= 0).count()
    unmapped_count = df.filter(F.col("area_name_encoded") == -1).count()
    
    print(f"  ✓ area_name → area_name_encoded using shared mapping")
    print(f"    Mapped: {mapped_count:,} rows")
    if unmapped_count > 0:
        print(f"    Unmapped/NULL: {unmapped_count:,} rows (encoded as -1)")
    
    return df

def encode_ward_with_shared_mapping(df: DataFrame, ward_mapping: Dict[str, int]) -> DataFrame:
    """Encode ward_name using shared mapping"""
    print("\n>>> Encoding ward_name with shared mapping")
    
    if "ward_name" not in df.columns:
        print("⚠ ward_name column not found")
        return df
    
    if not ward_mapping:
        print("⚠ Shared ward_name mapping is empty")
        return df
    
    # Convert to Spark broadcast for efficient lookup
    mapping_broadcast = spark.sparkContext.broadcast(ward_mapping)
    
    # Define UDF to encode values
    def encode_value(value):
        if value is None:
            return -1
        return mapping_broadcast.value.get(value, -1)
    
    encode_udf = F.udf(encode_value, IntegerType())
    
    # Apply encoding
    df = df.withColumn("ward_name_encoded", encode_udf(F.col("ward_name")))
    
    # Count how many were successfully mapped
    mapped_count = df.filter(F.col("ward_name_encoded") >= 0).count()
    unmapped_count = df.filter(F.col("ward_name_encoded") == -1).count()
    
    print(f"  ✓ ward_name → ward_name_encoded using shared mapping")
    print(f"    Mapped: {mapped_count:,} rows")
    if unmapped_count > 0:
        print(f"    Unmapped/NULL: {unmapped_count:,} rows (encoded as -1)")
    
    return df

def detect_and_remove_outliers_by_area(df: DataFrame) -> DataFrame:
    print("\n>>> Detecting and removing outliers by area (IQR method)")
    
    if "param_price_m2" not in df.columns or "area_name" not in df.columns:
        print("⚠ param_price_m2 or area_name column not found")
        return df
    
    rows_before = df.count()
    
    # Filter out NULL values for outlier analysis
    df_valid = df.filter(
        (F.col("param_price_m2").isNotNull()) & 
        (F.col("area_name").isNotNull())
    )
    
    # Calculate Q1, Q3, IQR for each area using percentile_approx
    quantile_stats = df_valid.groupBy("area_name").agg(
        F.count("*").alias("count"),
        F.percentile_approx("param_price_m2", 0.25).alias("Q1"),
        F.percentile_approx("param_price_m2", 0.75).alias("Q3"),
        F.percentile_approx("param_price_m2", 0.5).alias("median_price"),
        F.min("param_price_m2").alias("min_price"),
        F.max("param_price_m2").alias("max_price"),
        F.avg("param_price_m2").alias("mean_price")
    )
    
    # Calculate IQR and bounds
    quantile_stats = quantile_stats.withColumn(
        "IQR", F.col("Q3") - F.col("Q1")
    ).withColumn(
        "lower_bound", F.col("Q1") - 1.5 * F.col("IQR")
    ).withColumn(
        "upper_bound", F.col("Q3") + 1.5 * F.col("IQR")
    )
    
    print("  Area outlier statistics (IQR method):")
    quantile_stats.show(truncate=False)
    
    # Join bounds back to main dataframe
    df_with_bounds = df.join(quantile_stats, on="area_name", how="left")
    
    # Identify outliers: values outside bounds (or in areas with < 4 items)
    df_with_bounds = df_with_bounds.withColumn(
        "is_outlier",
        F.when(
            (F.col("lower_bound").isNotNull()) & (F.col("upper_bound").isNotNull()),
            ((F.col("param_price_m2").isNotNull()) & (
                (F.col("param_price_m2") < F.col("lower_bound")) | 
                (F.col("param_price_m2") > F.col("upper_bound"))
            ))
        ).otherwise(False)
    )
    
    # Count outliers
    outlier_count = df_with_bounds.filter(F.col("is_outlier") == True).count()
    
    # Remove outliers
    df = df_with_bounds.filter(F.col("is_outlier") == False)
    
    # Drop temporary columns
    temp_cols = ["Q1", "Q3", "IQR", "lower_bound", "upper_bound", "min_price", "max_price", "mean_price", "median_price", "count", "is_outlier"]
    df = df.drop(*temp_cols)
    
    rows_after = df.count()
    print(f"  ✓ Rows before: {rows_before:,}")
    print(f"  ✓ Outliers removed: {outlier_count:,}")
    print(f"  ✓ Rows after: {rows_after:,}")
    return df

def validate_and_remove_size_mismatch(df: DataFrame) -> DataFrame:
    print("\n>>> Validating size consistency (width × length vs size)")
    
    if "width" not in df.columns or "length" not in df.columns or "size" not in df.columns:
        print("⚠ width, length, or size column not found")
        return df
    
    rows_before = df.count()
    
    # Step 1: Create size_cal = width * length (only when both exist)
    df = df.withColumn(
        "size_cal",
        F.when(
            (F.col("width").isNotNull()) & (F.col("length").isNotNull()),
            F.col("width") * F.col("length")
        ).otherwise(None)
    )
    
    size_cal_count = df.filter(F.col("size_cal").isNotNull()).count()
    print(f"  size_cal created for {size_cal_count:,} rows (width × length)")
    
    # Step 2: Calculate deviation ratio
    # Condition: size_cal exists AND size exists AND size > 0
    df = df.withColumn(
        "deviation_ratio",
        F.when(
            (F.col("size_cal").isNotNull()) & 
            (F.col("size").isNotNull()) & 
            (F.col("size") > 0),
            F.abs(F.col("size_cal") - F.col("size")) / F.col("size")
        ).otherwise(None)
    )
    
    valid_deviation_count = df.filter(F.col("deviation_ratio").isNotNull()).count()
    print(f"  Deviation ratio calculated for {valid_deviation_count:,} rows")
    
    # Step 3: Identify rows with deviation > 10%
    df = df.withColumn(
        "is_size_mismatch",
        F.when(
            (F.col("deviation_ratio").isNotNull()) & 
            (F.col("deviation_ratio") > 0.10),
            True
        ).otherwise(False)
    )
    
    mismatch_count = df.filter(F.col("is_size_mismatch") == True).count()
    
    # Step 4: Remove mismatched rows
    df = df.filter(F.col("is_size_mismatch") == False)
    
    # Drop temporary columns
    df = df.drop("is_size_mismatch", "deviation_ratio", "size_cal")
    
    rows_after = df.count()
    removal_pct = (mismatch_count / rows_before * 100) if rows_before > 0 else 0
    
    print(f"  ✓ Rows before: {rows_before:,}")
    print(f"  ✓ Rows removed (deviation > 10%): {mismatch_count:,}")
    print(f"  ✓ Rows after: {rows_after:,}")
    print(f"  ✓ Removal %: {removal_pct:.2f}%")
    
    return df

def dedup_by_geolocation(df: DataFrame) -> DataFrame:
    print("\n>>> Deduplicating by geolocation (coordinates + price + size)")
    
    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("⚠ latitude or longitude column not found")
        return df
    
    if "price" not in df.columns or "size" not in df.columns:
        print("⚠ price or size column not found")
        return df
    
    # Use crawled_at_ts or list_time for ordering (not timestamp)
    order_col = None
    if "crawled_at_ts" in df.columns:
        order_col = "crawled_at_ts"
    elif "list_time" in df.columns:
        order_col = "list_time"
    elif "_ingest_ts" in df.columns:
        order_col = "_ingest_ts"
    
    if order_col is None:
        print("⚠ No timestamp column found for dedup ordering")
        return df
    
    rows_before = df.count()
    
    # Step 1: Round coordinates (~1.1m precision at 5 decimals)
    df = df.withColumn("lat_rounded", F.round(F.col("latitude"), 5))
    df = df.withColumn("lon_rounded", F.round(F.col("longitude"), 5))
    
    # Step 2: Find potential duplicates
    dup_cols = ["lat_rounded", "lon_rounded", "price", "size"]
    dup_stats = df.groupBy(dup_cols).agg(
        F.count("*").alias("count"),
        F.collect_list(F.col("list_id")).alias("list_ids"),
        F.collect_list(F.col("ad_id")).alias("ad_ids"),
        F.collect_list(F.col("account_id")).alias("account_ids"),
        F.collect_list(F.col("category_name")).alias("categories")
    ).filter(F.col("count") > 1)
    
    potential_dup_count = dup_stats.count()
    
    if potential_dup_count > 0:
        affected_records = dup_stats.agg(F.sum("count")).collect()[0][0]
        print(f"  📍 Found {potential_dup_count} duplicate groups (same coords + price + size)")
        print(f"  📍 Affected records: {affected_records} listings")
        
        # Show sample duplicates
        print(f"  Sample duplicates (first 5):")
        dup_samples = dup_stats.limit(5).collect()
        for row in dup_samples:
            print(f"    Coords ({row['lat_rounded']}, {row['lon_rounded']}) | Price {row['price']:,} | Size {row['size']}m² | Count: {row['count']}")
    
    # Step 3: Deduplicate using window function
    # Partition by geo+price+size, order by timestamp DESC (newest first), keep row_number=1
    w = Window.partitionBy(dup_cols).orderBy(F.col(order_col).desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w))
    
    # Keep only first row per group (latest timestamp)
    df = df.filter(F.col("rn") == 1).drop("rn")
    
    rows_after = df.count()
    removed = rows_before - rows_after
    removal_pct = (removed / rows_before * 100) if rows_before > 0 else 0
    
    print(f"  ✓ Rows before: {rows_before:,}")
    print(f"  ✓ Rows removed (geo duplicates): {removed:,}")
    print(f"  ✓ Rows after: {rows_after:,}")
    print(f"  ✓ Removal %: {removal_pct:.2f}%")
    return df

# ============================================================
# GOOGLE TRENDS PROCESSING
# ============================================================
GGTREND_PATH = os.getenv(
    "GGTREND_PATH",
    "s3a://lakehouse/bronze/user_interest/google_trends/"
)
GGTREND_SILVER_TABLE = os.getenv(
    "GGTREND_SILVER_TABLE",
    "lakehouse.silver.ggtrend_daily"
)

# Google Trends is collected at Vietnam scope (geo=VN), not HCMC-only.
# Keep these as metadata columns in Silver so downstream tables are self-describing.
GGTREND_GEO = os.getenv("GGTREND_GEO", "VN")
GGTREND_TARGET_REGION = os.getenv("GGTREND_TARGET_REGION", "Vietnam")
def read_ggtrend_json() -> Optional[DataFrame]:
    """Read Google Trends JSONL files from partitioned Bronze path."""
    base_path = GGTREND_PATH.rstrip("/")

    if base_path.endswith(".jsonl") or "*" in base_path:
        input_path = base_path
    elif PROCESS_DATE:
        input_path = f"{base_path}/date={PROCESS_DATE}/*.jsonl"
    else:
        input_path = f"{base_path}/date=*/*.jsonl"

    print(f"=== GGTREND: Reading JSONL from: {input_path} ===")

    try:
        df_raw = (
            spark.read
            .format("json")
            .option("multiLine", False)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(input_path)
            .withColumn("_source_path", F.input_file_name())
        )

        raw_count = df_raw.count()
        print(f"✓ GGTREND: Read {raw_count:,} JSONL records")
        print("  Raw schema:")
        df_raw.printSchema()

        if raw_count == 0:
            print("⚠ GGTREND: No records found")
            return None

        return df_raw

    except AnalysisException as e:
        print(f"!!! Cannot read Google Trends JSONL from: {input_path}")
        print(f"  Error: {e}")
        return None

def normalize_column_name(col_name: str) -> str:
    """Normalize column name: decode URL encoding, remove accents, convert to lowercase"""
    # Decode x20 (URL encoding for space) to underscore
    col_name = col_name.replace("_x20", "_").replace("x20", "_")
    
    # Remove accents (diacritics)
    col_name = ''.join(
        c for c in unicodedata.normalize('NFD', col_name)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Convert to lowercase
    col_name = col_name.lower()
    
    # Remove any remaining special characters except underscore
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)
    
    # Remove duplicate underscores
    col_name = re.sub(r'_+', '_', col_name)
    
    # Remove leading/trailing underscores
    col_name = col_name.strip('_')
    
    return col_name


def process_ggtrend_data(df: DataFrame) -> DataFrame:
    """Process and standardize Google Trends data collected at Vietnam scope."""
    print("=== GGTREND: Processing Google Trends data ===")

    if df is None or df.count() == 0:
        print(f"  ⚠ No data to process")
        return df

    # Rename columns to clean up URL encoding and accents.
    # Example: isPartial -> ispartial
    print(f"  Original columns: {df.columns}")
    col_mapping = {}
    for col_name in df.columns:
        # Preserve Spark/internal metadata columns such as _source_path and _corrupt_record.
        if col_name.startswith("_"):
            continue

        new_col_name = normalize_column_name(col_name)
        if new_col_name != col_name:
            col_mapping[col_name] = new_col_name
            print(f"    {col_name} → {new_col_name}")

    for old_name, new_name in col_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)

    # ============================================================
    # Current Google Trends JSONL schema:
    # {
    #   "load_type": "initial",
    #   "date": "2025-05-30",
    #   "keyword_group": "hcm_real_estate",
    #   "values": {
    #       "bất động sản": 22,
    #       "mua nhà": 39,
    #       "căn hộ": 32,
    #       "chung cư": 85,
    #       "đất nền": 6
    #   },
    #   "isPartial": false,
    #   "timeframe_start": "2025-05-30",
    #   "timeframe_end": "2025-06-29",
    #   "collected_at_ms": 1779709952091
    # }
    #
    # IMPORTANT:
    # - The collection geo is VN / Vietnam, not TP.HCM.
    # - Therefore Silver uses generic keyword columns, not *_tp_hcm.
    # ============================================================
    if "values" in df.columns:
        print("  Detected nested Google Trends `values` column → flattening to Vietnam-scope keyword columns")

        df = (
            df
            .withColumn("bat_dong_san", F.col("values").getField("bất động sản").cast(DoubleType()))
            .withColumn("mua_nha", F.col("values").getField("mua nhà").cast(DoubleType()))
            .withColumn("can_ho", F.col("values").getField("căn hộ").cast(DoubleType()))
            .withColumn("chung_cu", F.col("values").getField("chung cư").cast(DoubleType()))
            .withColumn("dat_nen", F.col("values").getField("đất nền").cast(DoubleType()))
            .drop("values")
        )

    # Backward-compatible renames for older/alternate flattened files.
    # These are normalized names that may appear if the collector writes columns directly.
    ggtrend_col_mapping = {
        "batongsan": "bat_dong_san",
        "bat_dong_san": "bat_dong_san",
        "muanha": "mua_nha",
        "mua_nha": "mua_nha",
        "canho": "can_ho",
        "can_ho": "can_ho",
        "chungcu": "chung_cu",
        "chung_cu": "chung_cu",
        "datnen": "dat_nen",
        "dat_nen": "dat_nen",

        # Legacy names from the earlier HCMC-specific naming.
        "batongsantphcm": "bat_dong_san",
        "bat_dong_san_tp_hcm": "bat_dong_san",
        "chungcutphcm": "chung_cu",
        "chung_cu_tp_hcm": "chung_cu",
        "canhotphcm": "can_ho",
        "can_ho_tp_hcm": "can_ho",
        "muanhatphcm": "mua_nha",
        "mua_nha_tp_hcm": "mua_nha",
        "atnentphcm": "dat_nen",
        "dat_nen_tp_hcm": "dat_nen",
    }

    for old_col, new_col in ggtrend_col_mapping.items():
        if old_col in df.columns and old_col != new_col:
            if new_col in df.columns:
                df = df.drop(old_col)
                print(f"    Dropped legacy duplicate column: {old_col}")
            else:
                df = df.withColumnRenamed(old_col, new_col)
                print(f"    {old_col} → {new_col}")

    # Add collection scope metadata if it is not already present in the JSONL records.
    if "geo" not in df.columns:
        df = df.withColumn("geo", F.lit(GGTREND_GEO))
    if "target_region" not in df.columns:
        df = df.withColumn("target_region", F.lit(GGTREND_TARGET_REGION))

    if "collected_at_ms" in df.columns:
        df = df.withColumn(
            "collected_at_ts",
            F.to_timestamp(
                F.from_unixtime((F.col("collected_at_ms") / F.lit(1000.0)).cast("double"))
            )
        )

    # Filter: Keep only rows where isPartial = False
    if "ispartial" in [c.lower() for c in df.columns]:
        ispartial_col = next((c for c in df.columns if c.lower() == "ispartial"), None)
        if ispartial_col:
            before_count = df.count()
            df = df.filter(
                (F.col(ispartial_col) == False) |
                (F.col(ispartial_col).cast("string") == "false")
            )
            after_count = df.count()
            print(
                f"  Filtered isPartial: {before_count:,} → {after_count:,} records "
                f"(removed {before_count - after_count:,} partial entries)"
            )

    # Convert expected dates explicitly. Keep `date` as DateType for monthly aggregation.
    for date_col in ["date", "timeframe_start", "timeframe_end"]:
        if date_col in df.columns:
            df = df.withColumn(date_col, F.to_date(F.col(date_col)))

    # Cast keyword columns to numeric if they exist.
    keyword_cols = ["bat_dong_san", "mua_nha", "can_ho", "chung_cu", "dat_nen"]
    for col_name in keyword_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    print(f"  New columns: {df.columns}")
    print(f"  Schema:")
    df.printSchema()

    print(f"  Sample data (first 2 rows):")
    df.limit(2).show(truncate=False)

    print(f"✓ GGTREND: Processing complete, {df.count():,} records")
    return df

def load_ggtrend_to_silver(df: DataFrame) -> None:
    """Load processed Google Trends data to Silver layer"""
    print(f"=== GGTREND: Loading to Silver table: {GGTREND_SILVER_TABLE} ===")
    
    # Add metadata columns
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    if "_source_path" not in df.columns:
        df = df.withColumn("_source_path", F.lit(GGTREND_PATH))
    
    # Select columns for Silver layer (keep all available columns)
    # Only exclude internal columns
    exclude_cols = ["_corrupt_record"]
    ggtrend_silver_cols = [c for c in df.columns if c not in exclude_cols]
    
    df_silver = df.select(*ggtrend_silver_cols)
    
    print(f"  Output columns ({len(ggtrend_silver_cols)}): {sorted(ggtrend_silver_cols)}")
    
    if table_exists(GGTREND_SILVER_TABLE):
        print(f"  Table exists. Appending new data...")
        try:
            df_silver.writeTo(GGTREND_SILVER_TABLE).append()
            print(f"  ✓ Appended {df_silver.count():,} Google Trends records")
        except Exception as e:
            print(f"❌ Error appending to Google Trends table: {e}")
            raise
    else:
        print(f"  Creating new table: {GGTREND_SILVER_TABLE}")
        try:
            (
                df_silver.writeTo(GGTREND_SILVER_TABLE)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )
            print(f"  ✓ Created {GGTREND_SILVER_TABLE} with {df_silver.count():,} Google Trends records")
        except Exception as e:
            print(f"❌ Error creating Google Trends table: {e}")
            raise

# ============================================================
# METRO STATIONS PROCESSING
# ============================================================
METRO_CSV_PATH = os.getenv("METRO_PATH", "s3a://lakehouse/bronze/metro/metro_stations.csv")
METRO_SILVER_TABLE = os.getenv("METRO_SILVER_TABLE", "lakehouse.silver.metro_stations")

def read_metro_csv() -> Optional[DataFrame]:
    print(f"=== METRO: Reading CSV from: {METRO_CSV_PATH} ===")
    try:
        df_metro = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .load(METRO_CSV_PATH)
        )
        print(f"✓ METRO: Read {df_metro.count():,} metro stations")
        print(f"  Columns: {df_metro.columns}")
        return df_metro
    except AnalysisException as e:
        print(f"!!! Cannot read metro CSV from: {METRO_CSV_PATH}")
        print(f"  Error: {e}")
        return None

def process_metro_data(df: DataFrame) -> DataFrame:
    print("=== METRO: Processing metro stations data ===")
    
    # Add ingestion timestamp and source info
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    
    # Cast latitude/longitude to double if they're strings
    if df.schema["lat"].dataType == StringType():
        df = df.withColumn("lat", F.col("lat").cast("double"))
    if df.schema["lng"].dataType == StringType():
        df = df.withColumn("lng", F.col("lng").cast("double"))
    
    # Rename columns to standard naming
    df = df.withColumnRenamed("lat", "latitude")
    df = df.withColumnRenamed("lng", "longitude")
    
    # Standardize boolean columns: convert TRUE/FALSE strings to integers (1/0)
    boolean_cols = [
        'amenity_atm', 'amenity_elevator', 'amenity_info_desk', 'amenity_parking',
        'amenity_shops', 'amenity_ticket_machine', 'amenity_toilet', 'amenity_vending',
        'amenity_wc', 'amenity_wifi'
    ]
    
    for col in boolean_cols:
        if col in df.columns:
            # Cast to string first to handle both string and boolean types
            df = df.withColumn(
                col,
                F.when(F.col(col).cast("string").isin('TRUE', 'true', 'True', '1', 'true'), 1)
                 .when(F.col(col).cast("string").isin('FALSE', 'false', 'False', '0'), 0)
                 .otherwise(None)
                 .cast("int")
            )
    
    print(f"✓ METRO: Processed {df.count():,} metro stations")
    return df

def encode_metro_with_shared_mappings(df: DataFrame, area_mapping: Dict[str, int], ward_mapping: Dict[str, int]) -> DataFrame:
    print("\n>>> Encoding metro area_name and ward_name with shared mappings")
    
    # Encode area_name
    if "area_name" in df.columns and area_mapping:
        mapping_broadcast = spark.sparkContext.broadcast(area_mapping)
        
        def encode_value(value):
            if value is None:
                return -1
            return mapping_broadcast.value.get(value, -1)
        
        encode_udf = F.udf(encode_value, IntegerType())
        df = df.withColumn("area_name_encoded", encode_udf(F.col("area_name")))
        
        mapped_count = df.filter(F.col("area_name_encoded") >= 0).count()
        unmapped_count = df.filter(F.col("area_name_encoded") == -1).count()
        print(f"  ✓ area_name encoded: {mapped_count:,} mapped, {unmapped_count:,} unmapped")
    elif "area_name" in df.columns:
        print(f"  ⚠ area_name not in shared mapping, skipping")
    
    # Encode ward_name
    if "ward_name" in df.columns and ward_mapping:
        mapping_broadcast = spark.sparkContext.broadcast(ward_mapping)
        
        def encode_value(value):
            if value is None:
                return -1
            return mapping_broadcast.value.get(value, -1)
        
        encode_udf = F.udf(encode_value, IntegerType())
        df = df.withColumn("ward_name_encoded", encode_udf(F.col("ward_name")))
        
        mapped_count = df.filter(F.col("ward_name_encoded") >= 0).count()
        unmapped_count = df.filter(F.col("ward_name_encoded") == -1).count()
        print(f"  ✓ ward_name encoded: {mapped_count:,} mapped, {unmapped_count:,} unmapped")
    elif "ward_name" in df.columns:
        print(f"  ⚠ ward_name not in shared mapping, skipping")
    
    return df

def load_metro_to_silver(df: DataFrame) -> None:
    print(f"=== METRO: Loading to Silver table: {METRO_SILVER_TABLE} ===")
    
    # Select columns for Silver layer
    metro_silver_cols = [
        'station_id', 'station_code', 'station_name', 'slug', 'station_type',
        'station_status', 'latitude', 'longitude', 'address', 'area_name', 'area_name_encoded', 'ward_name', 'ward_name_encoded',
        'amenity_atm', 'amenity_elevator', 'amenity_parking', 'amenity_wifi', '_ingest_ts'
    ]
    
    # Filter to only columns that exist
    metro_silver_cols = [c for c in metro_silver_cols if c in df.columns]
    df_silver = (
        df
        .select(*metro_silver_cols)
        .dropDuplicates(["station_id"])   # chống trùng khi file có lặp
    )
    
    print(f"  Output columns ({len(metro_silver_cols)}): {sorted(metro_silver_cols)}")
    
    try:
        (
            df_silver.writeTo(METRO_SILVER_TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

        print(f"  ✓ Replaced {METRO_SILVER_TABLE} with {df_silver.count():,} metro stations")

    except Exception as e:
        print(f"❌ Error writing metro table: {e}")
        raise

# ============================================================
# SILVER TRANSFORMATIONS
# ============================================================
def transform_to_silver():
    print("=== SILVER: Cleaning and standardizing data ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.meta")

    df = read_bronze()
    if df is None:
        print("❌ BRONZE: No data found. Stopping.")
        return
    
    print(f"✓ BRONZE: Read {df.count():,} rows")
    
    if df.rdd.isEmpty():
        print("=== Không có dữ liệu Bronze cần xử lý. Kết thúc job. ===")
        return
    
    print(f"✓ PROCESSING: {df.count():,} rows")

    # >>> FLATTEN PARAMS: Extract parameters từ detail.ad_params, detail.parameters, detail.ad.params
    df = flatten_bronze_with_params(df)
    print(f"✓ FLATTEN PARAMS: {df.count():,} rows")
    # ====================== TIER 1: DEDUP Bronze Input ======================
    # Deduplicate by list_id BEFORE processing - keep newest version from Bronze
    df_before_tier1 = df.count()
    df = dedup_by_list_id(df)
    print(f"✓ TIER 1 DEDUP: {df_before_tier1:,} → {df.count():,} rows")
    # ====================== TIER 2: DEDUP Fingerprint + Geolocation ======================
    print("=== TIER 2: Dedup fingerprint + geolocation (during transform) ===")
    fp_cols = ["subject", "body", "price_string", "latitude", "longitude", "size", "account_id"]
    available_fp_cols = [c for c in fp_cols if c in df.columns]
    
    if len(available_fp_cols) > 0:
        print(f"  Creating fingerprint from: {available_fp_cols}")
        df = df.withColumn(
            "content_fp",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.col(c) for c in available_fp_cols]
                ),
                256
            )
        )

        # 2) DEDUP theo fingerprint – bản tin mới nhất thắng
        order_cols = []
        if "crawled_at_ts" in df.columns:
            order_cols.append(F.col("crawled_at_ts").desc_nulls_last())
        elif "list_time" in df.columns:
            order_cols.append(F.col("list_time").desc_nulls_last())
        if "_ingest_ts" in df.columns:
            order_cols.append(F.col("_ingest_ts").desc_nulls_last())
        
        if not order_cols:
            print("  ⚠ No timestamp columns for dedup ordering, skipping fingerprint dedup")
        else:
            df_before_fp = df.count()
            w = Window.partitionBy("content_fp").orderBy(*order_cols)

            df = (
                df
                .withColumn("_rn", F.row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn")
            )
            print(f"  Fingerprint dedup: {df_before_fp:,} → {df.count():,} rows")
    else:
        print("  ⚠ Fingerprint columns not available, skipping content dedup")
        df = df.withColumn("content_fp", F.lit(None).cast("string"))
    print(f"✓ TIER 2 DEDUP: {df.count():,} rows")

    # Loại bỏ các tin không có địa chỉ đầy đủ
    df = filter_null_addresses(df)
    print(f"✓ FILTER addresses: {df.count():,} rows")

    # Điền median vào các vị trí NULL
    df = fill_null_number_of_images(df)
    print(f"✓ FILL images: {df.count():,} rows")

    # Xử lý NULL trong rooms dựa vào loại hình + tạo indicator
    df = handle_rooms_null(df)

    # Gắn cờ phân loại nội thất: is_not_furnishing, is_furnishing
    df = flag_furnishing_sell(df)

    # Gắn cờ phân loại chất lượng: good_quality, risk_quality, low_quality
    df = flag_property_quality(df)

    # Gắn cờ phân loại người đăng: is_company_ad, is_personal_ad
    df = flag_advertiser_type(df)

    # Gắn cờ phân loại pháp lý: fully_legal, semi_legal, risk_legal
    df = flag_legal_document(df)

    print(f"✓ FLAGS & TRANSFORMATIONS: {df.count():,} rows")

    # Xóa pattern "(Quận...cũ)" từ cột ward_name
    df = clean_ward_name(df)

    # Trích xuất giá trị số từ param_price_m2, bỏ đơn vị
    df = clean_param_price_m2(df)

    # Tạo cột binary cho mỗi giá trị category (giữ lại cột gốc)
    df = encode_category_name(df)

    # ===== BUILD SHARED AREA_NAME MAPPING FROM NHATOT DATA =====
    global AREA_NAME_MAPPING, WARD_NAME_MAPPING
    AREA_NAME_MAPPING = build_area_name_mapping(df)
    
    # Mã hóa area_name dùng shared mapping
    df = encode_area_with_shared_mapping(df, AREA_NAME_MAPPING)
    
    # ===== BUILD SHARED WARD_NAME MAPPING FROM NHATOT DATA =====
    WARD_NAME_MAPPING = build_ward_name_mapping(df)
    
    # Mã hóa ward_name dùng shared mapping
    df = encode_ward_with_shared_mapping(df, WARD_NAME_MAPPING)

    # Xóa giá ngoại lệ theo IQR method từng khu vực
    df = detect_and_remove_outliers_by_area(df)

    # Xóa dòng lệch > 10%: abs(width*length - size) / size > 0.10
    df = validate_and_remove_size_mismatch(df)

    # Làm tròn size tới 2 chữ số thập phân
    df = round_size_column(df)

    # Xóa trùng lặp theo: tọa độ + giá + diện tích (giữ record mới nhất)
    df = dedup_by_geolocation(df)
    print(f"✓ FINAL ROWS FOR SILVER: {df.count():,}")
    
    # Convert crawled_at_unix (milliseconds) to timestamp format
    if "crawled_at_unix" in df.columns:
        df = df.withColumn(
            "crawled_at_ts",
            F.to_timestamp(F.from_unixtime((F.col("crawled_at_unix") / F.lit(1000.0)).cast("double")))
        )
        print(f"  ✓ Converted crawled_at_unix (ms) → crawled_at_ts (timestamp)")
    else:
        print(f"  ⚠ crawled_at_unix not found → cannot create crawled_at_ts")
        df = df.withColumn("crawled_at_ts", F.lit(None).cast("timestamp"))
    # ================== CÁC CỘT GHI VÀO SILVER ==================
    # Select ONLY required columns (explicit list)
    silver_cols = [
        'snapshot_date','crawled_at_ts','ad_id','list_id','account_id','account_name','price','size','rooms','category_name','area_name','ward_name','street_name',
        'latitude','longitude','number_of_images','param_furnishing_sell','param_property_legal_document','param_price_m2','param_pty_characteristics',
        'is_land','is_apt','is_house','has_rooms','is_not_furnishing','is_furnishing','good_quality','risk_quality','low_quality','is_company_ad','is_personal_ad',
        'fully_legal','semi_legal','risk_legal','_ingest_ts','area_name_encoded','ward_name_encoded','content_fp',
    ]
    # Add one-hot encoded category columns dynamically (cat_*)
    cat_cols = [c for c in df.columns if c.startswith('cat_')]
    silver_cols.extend(cat_cols)
    # Filter to only columns that exist in dataframe
    silver_cols = [c for c in silver_cols if c in df.columns]
    print(f"=== SILVER OUTPUT COLUMNS ({len(silver_cols)}) ===")
    print(f"    Columns: {sorted(silver_cols)}")
    df_silver = df.select(*silver_cols)
    print(f"✓ SILVER DATAFRAME: {df_silver.count():,} rows, {len(df_silver.columns)} columns")
    # Remove NullType columns (they are all NULL anyway, and Iceberg doesn't support NullType)
    null_type_cols = [
        col_name for col_name in df_silver.columns 
        if str(df_silver.schema[col_name].dataType) == "NullType()"
    ]
    if null_type_cols:
        print(f"  Dropping {len(null_type_cols)} NullType columns (all NULL values): {null_type_cols}")
        df_silver = df_silver.drop(*null_type_cols)
        print(f"  ✓ Remaining columns: {len(df_silver.columns)}")
    # ================== TIER 3: Smart Merge on Append ==================
    # Nếu Silver tồn tại: Kiểm tra fingerprint bước đầu tiên
    if table_exists(SILVER_TABLE):
        print(f"=== TIER 3: Smart merge (fingerprint dedup + timestamp compare) ===")
        
        df_silver_before = df_silver.count()
        
        # Lấy fingerprint từ Silver đã tồn tại
        df_existing_fp = spark.table(SILVER_TABLE).select("content_fp").distinct()
        
        # Lọc: chỉ giữ những fingerprint CHỨ từng xuất hiện (hoàn toàn mới)
        df_new = df_silver.join(df_existing_fp, on="content_fp", how="left_anti")
        rows_new = df_new.count()
        rows_dup = df_silver_before - rows_new
        
        print(f"  New fingerprints (never seen): {rows_new:,}")
        print(f"  Duplicate fingerprints (exist): {rows_dup:,}")
        
        if rows_new > 0:
            print(f"=== Ghi append {rows_new:,} records vào bảng Silver: {SILVER_TABLE} ===")
            try:
                df_new.writeTo(SILVER_TABLE).append()
                print(f"  ✓ Successfully appended {rows_new:,} new records")
            except Exception as e:
                print(f"❌ Error appending to Silver table: {e}")
                raise
        else:
            print(f"  No new records to append (all are duplicates)")
        
        if rows_dup > 0:
            print(f"  Note: {rows_dup:,} duplicate fingerprints skipped (keep existing Silver data)")
        
        print(f"=== Silver table append completed ===")
    else:
        print(f"=== TIER 3: Create new Silver table (first load) ===")
        print(f"=== Tạo mới bảng Silver: {SILVER_TABLE} ===")
        # Wait a moment for S3 operations to settle
        time.sleep(1)
        print(f"  Schema validation before write:")
        print(f"  Total columns: {len(df_silver.columns)}")
        
        # Validate schema - only print first 10 columns for brevity
        for col_name in df_silver.columns[:10]:
            col_type = df_silver.schema[col_name].dataType
            print(f"    {col_name}: {col_type}")
        if len(df_silver.columns) > 10:
            print(f"    ... and {len(df_silver.columns) - 10} more columns")
        
        try:
            # First attempt: try createOrReplace (handles stale Nessie state)
            print(f"  Attempting table creation with createOrReplace...")
            (
                df_silver.writeTo(SILVER_TABLE)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .createOrReplace()
            )
            print(f"  ✓ Created/Replaced {SILVER_TABLE} successfully")
        except Exception as e:
            print(f"❌ Error writing Silver table: {e}")
            raise
    print("✓ Silver layer transformation completed successfully")

    # ===== PROCESS METRO STATIONS =====
    print("\n" + "="*60)
    print("PROCESSING METRO STATIONS DATA")
    print("="*60)
    df_metro = read_metro_csv()
    if df_metro is not None:
        df_metro = process_metro_data(df_metro)
        # Use shared area_name and ward_name mappings from nhatot data
        df_metro = encode_metro_with_shared_mappings(df_metro, AREA_NAME_MAPPING, WARD_NAME_MAPPING)
        load_metro_to_silver(df_metro)
        print("✓ Metro stations processing completed successfully")
    else:
        print("⚠ Metro stations data not available, skipping metro processing")

    # ===== PROCESS GOOGLE TRENDS DATA =====
    print("\n" + "="*60)
    print("PROCESSING GOOGLE TRENDS DATA")
    print("="*60)
    df_ggtrend = read_ggtrend_json()
    if df_ggtrend is not None:
        df_ggtrend = process_ggtrend_data(df_ggtrend)
        load_ggtrend_to_silver(df_ggtrend)
        print("✓ Google Trends processing completed successfully")
    else:
        print("⚠ Google Trends data not available, skipping GGTREND processing")

# ============================================================
# RUNTIME ARGUMENTS
# ============================================================

def build_raw_json_path(process_date: Optional[str], bronze_path: Optional[str]) -> str:
    """
    Resolve Bronze input path without changing the ETL transformation logic.
    Priority:
    1) explicit --bronze-path / BRONZE_PATH
    2) date-partition path when PROCESS_DATE is provided
    3) full Chotot Bronze prefix
    """
    if bronze_path:
        return bronze_path

    if process_date:
        return f"s3a://lakehouse/bronze/market_listings/chotot/date={process_date}/"

    return "s3a://lakehouse/bronze/market_listings/chotot/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Chotot Bronze data to Silver tables for the Gold star-schema job."
    )
    parser.add_argument(
        "process_date_positional",
        nargs="?",
        default=None,
        help="Optional process date in YYYY-MM-DD format. Kept for backward compatibility.",
    )
    parser.add_argument(
        "--process-date",
        default=os.getenv("PROCESS_DATE"),
        help="Optional process date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--bronze-path",
        default=os.getenv("BRONZE_PATH"),
        help="Input Bronze JSON path. Overrides --process-date when provided.",
    )
    parser.add_argument(
        "--silver-table",
        default=os.getenv("SILVER_TABLE", SILVER_TABLE),
        help="Output cleaned Silver table. Default: lakehouse.silver.chotot_cleaned",
    )
    parser.add_argument(
        "--metro-path",
        default=os.getenv("METRO_PATH", METRO_CSV_PATH),
        help="Input Metro CSV path.",
    )
    parser.add_argument(
        "--metro-table",
        default=os.getenv("METRO_SILVER_TABLE", METRO_SILVER_TABLE),
        help="Output Metro Silver table. Default: lakehouse.silver.metro_stations",
    )
    parser.add_argument(
        "--ggtrend-path",
        default=os.getenv("GGTREND_PATH", GGTREND_PATH),
        help="Input Google Trends Bronze path.",
    )
    parser.add_argument(
        "--ggtrend-table",
        default=os.getenv("GGTREND_SILVER_TABLE", GGTREND_SILVER_TABLE),
        help="Output Google Trends Silver table. Default: lakehouse.silver.ggtrend_daily",
    )
    parser.add_argument(
        "--ggtrend-geo",
        default=os.getenv("GGTREND_GEO", GGTREND_GEO),
        help="Google Trends geo scope metadata. Default: VN",
    )
    parser.add_argument(
        "--ggtrend-target-region",
        default=os.getenv("GGTREND_TARGET_REGION", GGTREND_TARGET_REGION),
        help="Google Trends target region metadata. Default: Vietnam",
    )
    parser.add_argument(
        "--ingest-log-table",
        default=os.getenv("INGEST_LOG_TABLE", INGEST_LOG_TABLE),
        help="Metadata ingest log table.",
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"⚠ Ignoring unknown arguments from spark-submit/Airflow: {unknown}")
    return args


def apply_runtime_args(args: argparse.Namespace) -> None:
    """
    Update only runtime configuration variables. Business transformations,
    selected columns, deduplication, encoding, and write logic stay unchanged.
    """
    global PROCESS_DATE, RAW_JSON_PATH, SILVER_TABLE, INGEST_LOG_TABLE
    global METRO_CSV_PATH, METRO_SILVER_TABLE, GGTREND_PATH, GGTREND_SILVER_TABLE
    global GGTREND_GEO, GGTREND_TARGET_REGION

    PROCESS_DATE = args.process_date or args.process_date_positional
    RAW_JSON_PATH = build_raw_json_path(PROCESS_DATE, args.bronze_path)
    SILVER_TABLE = args.silver_table
    INGEST_LOG_TABLE = args.ingest_log_table

    METRO_CSV_PATH = args.metro_path
    METRO_SILVER_TABLE = args.metro_table

    GGTREND_PATH = args.ggtrend_path
    GGTREND_SILVER_TABLE = args.ggtrend_table
    GGTREND_GEO = args.ggtrend_geo
    GGTREND_TARGET_REGION = args.ggtrend_target_region

    print("=== BRONZE→SILVER RUNTIME CONFIG ===")
    print(f"PROCESS_DATE={PROCESS_DATE}")
    print(f"RAW_JSON_PATH={RAW_JSON_PATH}")
    print(f"SILVER_TABLE={SILVER_TABLE}")
    print(f"METRO_CSV_PATH={METRO_CSV_PATH}")
    print(f"METRO_SILVER_TABLE={METRO_SILVER_TABLE}")
    print(f"GGTREND_PATH={GGTREND_PATH}")
    print(f"GGTREND_SILVER_TABLE={GGTREND_SILVER_TABLE}")
    print(f"GGTREND_GEO={GGTREND_GEO}")
    print(f"GGTREND_TARGET_REGION={GGTREND_TARGET_REGION}")
    print(f"INGEST_LOG_TABLE={INGEST_LOG_TABLE}")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    args = parse_args()
    apply_runtime_args(args)

    try:
        transform_to_silver()
    finally:
        spark.stop()
