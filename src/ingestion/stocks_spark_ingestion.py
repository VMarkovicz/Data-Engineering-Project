from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month, concat_ws,
      weekofyear, date_format, row_number, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
import json
import os


spark = SparkSession.builder \
    .appName("Cryptostock_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


raw_data_path = "/opt/raw_datasets/cryptostocks"


print("="*80)
print("CRYPTOSTOCK SPARK PROCESSING")
print("="*80)


# ============================================================================
# BRONZE LAYER
# ============================================================================
print("\n[BRONZE] Reading JSON files...")


json_files = [f for f in os.listdir(raw_data_path) if f.endswith('.json')]


if not json_files:
    print("⚠️  No JSON files found")
    spark.stop()
    exit(0)


all_data = []


for json_file in json_files:
    filepath = os.path.join(raw_data_path, json_file)
    print(f"Processing {json_file}...")
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    # Extract ticker from new structure
    symbol = data.get("symbol", "UNKNOWN")
    bars = data.get("bars", [])
    
    for bar in bars:
        all_data.append({
            "symbol": symbol,
            "timestamp": bar.get("t"),
            "open": float(bar.get("o", 0)),
            "high": float(bar.get("h", 0)),
            "low": float(bar.get("l", 0)),
            "close": float(bar.get("c", 0)),
            "volume": int(bar.get("v", 0)),
            "vwap": float(bar.get("vw", 0)),
            "trade_count": int(bar.get("n", 0)),
            "source_file": json_file
        })


if not all_data:
    print("⚠️  No data extracted")
    spark.stop()
    exit(0)


schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("trade_count", LongType(), True),
    StructField("source_file", StringType(), True)
])


bronze_df = spark.createDataFrame(all_data, schema) \
    .withColumn("ingestion_timestamp", current_timestamp())


print(f"✅ [BRONZE] {bronze_df.count()} records ingested")


bronze_df.write.format("delta").mode("overwrite").save("/datalake/bronze/cryptostock_stocks")


# ============================================================================
# SILVER LAYER
# ============================================================================
print("\n[SILVER] Cleaning and transforming...")


silver_df = spark.read.format("delta").load("/datalake/bronze/cryptostock_stocks") \
    .dropDuplicates(["symbol", "timestamp"]) \
    .filter(col("close").isNotNull()) \
    .filter(col("volume") > 0) \
    .withColumn("date", to_date(col("timestamp"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("processed_timestamp", current_timestamp())


print(f"✅ [SILVER] {silver_df.count()} cleaned records")


silver_df.write.format("delta").mode("overwrite").partitionBy("year").save("/datalake/silver/cryptostock_stocks")


# ============================================================================
# GOLD LAYER - DIMENSIONAL MODEL
# ============================================================================
print("\n[GOLD] Creating dimensional model...")


# Read silver data
silver_data = spark.read.format("delta").load("/datalake/silver/cryptostock_stocks")


# ============================================================================
# dim_time
# ============================================================================
print("\n[GOLD] Creating dim_time...")


dim_time = silver_data.select("date").distinct() \
    .withColumn("date_key", col("date")) \
    .withColumn("day_name", date_format(col("date"), "EEEE")) \
    .withColumn("week_number", weekofyear(col("date"))) \
    .withColumn("month_number", month(col("date"))) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("year", year(col("date"))) \
    .select(
        "date_key",
        "day_name",
        "week_number",
        "month_number",
        "month_name",
        "year"
    ).distinct()


print(f"✅ [GOLD] dim_time: {dim_time.count()} dates")
dim_time.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_time")


# ============================================================================
# dim_asset
# ============================================================================
print("\n[GOLD] Creating dim_asset...")


dim_asset = silver_data.select("symbol").distinct() \
    .withColumnRenamed("symbol", "asset_key") \
    .withColumn("type", lit("STOCK")) \
    .select("asset_key", "type")


print(f"✅ [GOLD] dim_asset: {dim_asset.count()} assets")
dim_asset.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_asset")


# ============================================================================
# dim_stock_price
# ============================================================================
print("\n[GOLD] Creating dim_stock_price...")


# Create unique key combining symbol and date
window_spec = Window.orderBy("symbol", "date")


# Keep symbol and date for joining later
dim_stock_price_full = silver_data.select(
    col("symbol"),
    col("date"),
    col("open")
).distinct() \
    .withColumn("stock_price_key", 
                row_number().over(window_spec).cast("string"))


dim_stock_price = dim_stock_price_full.select("stock_price_key", "open")


print(f"✅ [GOLD] dim_stock_price: {dim_stock_price.count()} stock prices")
dim_stock_price.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_stock_price")


# ============================================================================
# dim_cryptostock_value
# ============================================================================
print("\n[GOLD] Creating dim_stock_price...")

# Create unique key that includes symbol and date
dim_stock_price_full = silver_data.select(
    col("symbol"),
    col("date"),
    col("open")
).distinct() \
    .withColumn("stock_price_key", 
                concat_ws("_", col("symbol"), col("date").cast("string")))

dim_stock_price = dim_stock_price_full.select("stock_price_key", "open")

print(f"✅ [GOLD] dim_stock_price: {dim_stock_price.count()} stock prices")
dim_stock_price.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_stock_price")


# ============================================================================
# dim_cryptostock_value - FIXED
# ============================================================================
print("\n[GOLD] Creating dim_cryptostock_value...")

dim_cryptostock_value_full = silver_data.select(
    col("symbol"),
    col("date"),
    col("high"),
    col("low"),
    col("volume")
).distinct() \
    .withColumn("stock_price_key", 
                concat_ws("_", col("symbol"), col("date").cast("string"))) \
    .join(dim_stock_price_full.select("symbol", "date", "stock_price_key"), 
          ["symbol", "date", "stock_price_key"], "inner") \
    .withColumn("cryptostock_value_key", 
                concat_ws("_", col("symbol"), col("date").cast("string"))) \
    .withColumn("crypto_price_key", lit(None).cast("string"))

dim_cryptostock_value = dim_cryptostock_value_full.select(
    "cryptostock_value_key",
    "stock_price_key",
    "crypto_price_key",
    "high",
    "low",
    "volume"
)

print(f"✅ [GOLD] dim_cryptostock_value: {dim_cryptostock_value.count()} records")
dim_cryptostock_value.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_cryptostock_value")
# ============================================================================
# dim_value
# ============================================================================
print("\n[GOLD] Creating dim_value...")


# Extract close prices as values
dim_value_full = silver_data.select(
    col("close").alias("value")
).distinct() \
    .withColumn("value_key", monotonically_increasing_id()) \
    .withColumn("unit", lit("USD"))


dim_value = dim_value_full.select("value_key", "value", "unit")


print(f"✅ [GOLD] dim_value: {dim_value.count()} unique values")
dim_value.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_value")


# ============================================================================
# fact_value
# ============================================================================
print("\n[GOLD] Creating fact_value...")


# Build the fact table with date_key reference to dim_time
fact_value = silver_data.select(
    col("symbol").alias("asset_key"),
    col("date").alias("date_key"),
    col("close").alias("value")
)


# Join with dim_value to get value_key
fact_value = fact_value.join(
    dim_value_full.select("value", "value_key"),
    ["value"],
    "inner"
)


# Join with dim_cryptostock_value to get cryptostock_value_key
fact_value = fact_value.join(
    dim_cryptostock_value_full.select("symbol", "date", "cryptostock_value_key") \
        .withColumnRenamed("symbol", "asset_key") \
        .withColumnRenamed("date", "date_key"),
    ["asset_key", "date_key"],
    "inner"
)


# Final selection with all foreign keys
fact_value = fact_value.select(
    "value_key",
    "asset_key",
    "date_key",
    col("cryptostock_value_key").cast("string"),
    lit(None).cast("string").alias("socioeconomical_indicator_key"),
    lit(None).cast("string").alias("realstate_indicator_key")
).distinct()


# Extract year for partitioning
fact_value = fact_value.withColumn("year", year(col("date_key")))


print(f"✅ [GOLD] fact_value: {fact_value.count()} fact records")
fact_value.write.format("parquet").mode("overwrite").partitionBy("year").save("/datalake/gold/fact_value")


# ============================================================================
# dim_crypto_price (empty placeholder for future crypto data)
# ============================================================================
print("\n[GOLD] Creating dim_crypto_price placeholder...")


crypto_schema = StructType([
    StructField("crypto_price_key", StringType(), False),
    StructField("mid", DoubleType(), True),
    StructField("last", DoubleType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True)
])


dim_crypto_price = spark.createDataFrame([], crypto_schema)


print(f"✅ [GOLD] dim_crypto_price: {dim_crypto_price.count()} records (placeholder)")
dim_crypto_price.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_crypto_price")


print("\n" + "="*80)
print("✅ PIPELINE COMPLETED SUCCESSFULLY")
print("="*80)
print("\nGold Layer Tables Created:")
print("  - dim_time")
print("  - dim_asset")
print("  - dim_stock_price")
print("  - dim_crypto_price (empty)")
print("  - dim_cryptostock_value")
print("  - dim_value")
print("  - fact_value")
print("="*80)


spark.stop()
