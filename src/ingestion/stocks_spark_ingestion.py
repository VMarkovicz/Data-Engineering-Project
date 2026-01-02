from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month,
    weekofyear, date_format, row_number, to_timestamp, explode, input_file_name, substring
)
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
import os


spark = SparkSession.builder \
    .appName("Cryptostock_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


raw_data_path = "/opt/src/raw_datasets/cryptostocks"

wanted_assets = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'MSFT', 'NVDA', 'AAPL', 'AMZN', 'GOOGL', 'META', 'AVGO', 'BRK.B', 'TSLA', 'LLY', 'V', 'JNJ', 'XOM', 'WMT', 'JPM', 'BCH/USD']

print("="*80)
print("CRYPTOSTOCK SPARK PROCESSING - JSON FILES")
print("="*80)


# ============================================================================
# BRONZE LAYER - FIXED (Handle ISO 8601 timestamps)
# ============================================================================
print("\n[BRONZE] Reading JSON files with Spark...")

json_path = f"{raw_data_path}/*.json"

# Read raw JSON with multiline option
raw_df = spark.read.option("multiline", "true").json(json_path)

print("Raw JSON schema:")
raw_df.printSchema()
print(f"Raw files read: {raw_df.count()} JSON objects")

# Transform to desired structure
bronze_df = raw_df \
    .select(
        col("symbol"),
        col("type"),
        explode(col("bars")).alias("bar"),
        input_file_name().alias("source_file")
    ) \
    .select(
        col("symbol").cast("string"),
        # FIX: Extract just the date part (YYYY-MM-DD)
        substring(col("bar.t"), 1, 10).alias("date"),
        col("bar.h").cast("double").alias("high"),
        col("bar.l").cast("double").alias("low"),
        col("bar.c").cast("double").alias("last"),
        col("bar.v").cast("long").alias("volume"),
        col("type").cast("string"),
        col("source_file").cast("string")
    ) \
    .withColumn("ingestion_timestamp", current_timestamp())

print(f"✅ [BRONZE] {bronze_df.count()} records ingested")

print("\n[DEBUG] Sample bronze data:")
bronze_df.show(5, truncate=False)

bronze_df.write.format("delta").mode("append").save("/datalake/bronze/cryptostock_stocks")


# ============================================================================
# SILVER LAYER
# ============================================================================
print("\n[SILVER] Cleaning and transforming...")

silver_df = spark.read.format("delta").load("/datalake/bronze/cryptostock_stocks") \
    .dropDuplicates(["symbol", "date"]) \
    .filter(col("last").isNotNull()) \
    .filter(col("volume") > 0) \
    .withColumn("date_parsed", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date_parsed"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .filter(col("symbol").isin(wanted_assets))

print(f"✅ [SILVER] {silver_df.count()} cleaned records")

print("\n[DEBUG] Sample silver data:")
silver_df.select("symbol", "date", "date_parsed").show(5, truncate=False)

silver_df.write.format("delta").mode("overwrite").partitionBy("year").save("/datalake/silver/cryptostock_stocks")


# ============================================================================
# GOLD LAYER - DIMENSIONAL MODEL
# ============================================================================
print("\n[GOLD] Creating dimensional model...")

silver_data = spark.read.format("delta").load("/datalake/silver/cryptostock_stocks")


# ============================================================================
# dim_time
# ============================================================================
print("\n[GOLD] Creating dim_time...")

dim_time = silver_data.select("date_parsed").distinct() \
    .filter(col("date_parsed").isNotNull()) \
    .withColumn("date_key", to_timestamp(col("date_parsed"))) \
    .withColumn("day_name", date_format(col("date_parsed"), "EEEE")) \
    .withColumn("week_number", weekofyear(col("date_parsed"))) \
    .withColumn("month_number", month(col("date_parsed"))) \
    .withColumn("month_name", date_format(col("date_parsed"), "MMMM")) \
    .withColumn("year", year(col("date_parsed"))) \
    .select(
        "date_key",
        "day_name",
        "week_number",
        "month_number",
        "month_name",
        "year"
    ).distinct()

print(f"✅ [GOLD] dim_time: {dim_time.count()} dates")
dim_time.show(5, truncate=False)

# FIX: Changed from 'append' to 'overwrite'
dim_time.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_time")


# ============================================================================
# dim_asset
# ============================================================================
print("\n[GOLD] Creating dim_asset...")

dim_asset = silver_data.select("symbol", "type").distinct() \
    .withColumnRenamed("symbol", "asset_key") \
    .select("asset_key", "type")

print(f"✅ [GOLD] dim_asset: {dim_asset.count()} assets")

# FIX: Changed from 'append' to 'overwrite'
dim_asset.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_asset")


# ============================================================================
# dim_cryptostock_value
# ============================================================================
print("\n[GOLD] Creating dim_cryptostock_value...")

dim_cryptostock_value_base = silver_data.select(
    col("symbol"),
    col("date_parsed"),
    col("high"),
    col("low"),
    col("volume"),
    col("last")
).distinct()

window_spec = Window.orderBy("symbol", "date_parsed", "high", "low", "last", "volume")

dim_cryptostock_value = dim_cryptostock_value_base \
    .withColumn("cryptostock_value_key", row_number().over(window_spec)) \
    .withColumn("close", col("last")) \
    .select(
        col("cryptostock_value_key").cast("integer"),
        "symbol",
        "date_parsed",
        "high",
        "low",
        col("volume").cast(DoubleType()).alias("volume"),
        "close"
    )

print(f"✅ [GOLD] dim_cryptostock_value: {dim_cryptostock_value.count()} records")
dim_cryptostock_value.show(5, truncate=False)

# FIX: Changed from 'append' to 'overwrite'
dim_cryptostock_value.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_cryptostock_value")


# ============================================================================
# fact_value
# ============================================================================
print("\n[GOLD] Creating fact_value...")

fact_value_base = silver_data.select(
    col("symbol"),
    col("date_parsed"),
    col("high"),
    col("low"),
    col("last").alias("close"),
    col("volume").cast(DoubleType()).alias("volume")
).distinct()

print(f"fact_value_base records: {fact_value_base.count()}")

fact_value = fact_value_base.join(
    dim_cryptostock_value.select(
        col("cryptostock_value_key"),
        col("symbol"),
        col("date_parsed"),
        col("high"),
        col("low"),
        col("close"),
        col("volume")
    ),
    ["symbol", "date_parsed", "high", "low", "close", "volume"],
    "inner"
)

print(f"✅ [GOLD] After join: {fact_value.count()} records")

if fact_value.count() == 0:
    print("⚠️  WARNING: Join returned 0 records!")

fact_value = fact_value.withColumn("date_key", to_timestamp(col("date_parsed")))

fact_value = fact_value.select(
    lit(None).cast(IntegerType()).alias("value_key"),
    col("symbol").alias("asset_key"),
    "date_key",
    col("cryptostock_value_key").cast("string"),
    lit(None).cast(StringType()).alias("socioeconomical_indicator_key"),
    lit(None).cast(StringType()).alias("realstate_indicator_key"),
    lit(None).cast(StringType()).alias("country_key")
).distinct()

fact_value = fact_value.withColumn("year", year(col("date_key")))

print(f"✅ [GOLD] fact_value: {fact_value.count()} fact records")
fact_value.show(5, truncate=False)

if fact_value.count() > 0:
    # FIX: Changed from 'append' to 'overwrite'
    fact_value.write.format("parquet").mode("overwrite").partitionBy("year").save("/datalake/gold/fact_value")
    print("✅ fact_value written successfully")
else:
    print("⚠️  ERROR: fact_value has 0 records!")


print("\n" + "="*80)
print("✅ PIPELINE COMPLETED SUCCESSFULLY")
print("="*80)

spark.stop()