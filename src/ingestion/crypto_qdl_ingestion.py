from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month, concat_ws,
    weekofyear, date_format, to_timestamp, monotonically_increasing_id, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
import os


spark = SparkSession.builder \
    .appName("Cryptostock_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


raw_data_path = "/opt/src/raw_datasets/QDL_BITFINEX_bd501c887fbc1cc545f11778912a9118.csv"

wanted_cryptos = {
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD",
}


print("="*80)
print("CRYPTOSTOCK SPARK PROCESSING")
print("="*80)


# ============================================================================
# BRONZE LAYER
# ============================================================================
print("\n[BRONZE] Reading CSV files...")

bronze_crypto_data = spark.read.csv(raw_data_path, header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("QDL_BITFINEX_bd501c887fbc1cc545f11778912a9118.csv"))

# Select only the required fields
all_data = []

for row in bronze_crypto_data.collect():
    all_data.append({
        "symbol": wanted_cryptos.get(row["code"], row["code"]),
        "date": str(row["date"]),  # ← FIX: Force to string
        "high": float(row["high"]),
        "low": float(row["low"]),
        "last": float(row["last"]),
        "volume": int(row["volume"]),
        "type": "CRYPTO",
        "source_file": row["source_file"],
    })

if not all_data:
    print("⚠️  No data extracted")
    spark.stop()
    exit(0)

schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("date", StringType(), False),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("last", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("type", StringType(), True),
    StructField("source_file", StringType(), True)
])

bronze_df = spark.createDataFrame(all_data, schema) \
    .withColumn("ingestion_timestamp", current_timestamp())

print(f"✅ [BRONZE] {bronze_df.count()} records ingested")

bronze_df.write.format("delta").mode("append").save("/datalake/bronze/cryptostock_stocks")


# ============================================================================
# SILVER LAYER - WITH DIAGNOSTICS
# ============================================================================
print("\n[SILVER] Cleaning and transforming...")

silver_df = spark.read.format("delta").load("/datalake/bronze/cryptostock_stocks") \
    .dropDuplicates(["symbol", "date"]) \
    .filter(col("last").isNotNull()) \
    .filter(col("volume") > 0) \
    .filter(col("symbol").isin(list(wanted_cryptos.values())))

# DIAGNOSTIC: Check date column BEFORE parsing
print("\n[DIAGNOSTIC] Checking date column:")
silver_df.select("date").printSchema()
print("Sample dates:")
silver_df.select("date").show(5, truncate=False)

# Try parsing with explicit format
silver_df = silver_df \
    .withColumn("date_parsed", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date_parsed"))) \
    .withColumn("processed_timestamp", current_timestamp())

# DIAGNOSTIC: Check if parsing worked
print("\n[DIAGNOSTIC] After date parsing:")
silver_df.select("date", "date_parsed", "year").show(5, truncate=False)
print(f"NULL date_parsed count: {silver_df.filter(col('date_parsed').isNull()).count()}")
print(f"NOT NULL date_parsed count: {silver_df.filter(col('date_parsed').isNotNull()).count()}")

print(f"✅ [SILVER] {silver_df.count()} cleaned records")

silver_df.write.format("delta").mode("overwrite").partitionBy("year").save("/datalake/silver/cryptostock_stocks")


# ============================================================================
# GOLD LAYER - DIMENSIONAL MODEL
# ============================================================================
print("\n[GOLD] Creating dimensional model...")

silver_data = spark.read.format("delta").load("/datalake/silver/cryptostock_stocks")

print(f"[DIAGNOSTIC] Silver data loaded: {silver_data.count()} records")
silver_data.select("date_parsed").show(5, truncate=False)


# ============================================================================
# dim_time
# ============================================================================
print("\n[GOLD] Creating dim_time...")

dim_time = silver_data.select("date_parsed").distinct() \
    .filter(col("date_parsed").isNotNull())

print(f"[DIAGNOSTIC] After distinct and filter: {dim_time.count()} records")
dim_time.show(5, truncate=False)

dim_time = dim_time \
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

dim_time.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_time")


# ============================================================================
# dim_asset
# ============================================================================
print("\n[GOLD] Creating dim_asset...")

dim_asset = silver_data.select("symbol", "type").distinct() \
    .withColumnRenamed("symbol", "asset_key") \
    .select("asset_key", "type")

print(f"✅ [GOLD] dim_asset: {dim_asset.count()} assets")
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
dim_cryptostock_value.show(3, truncate=False)

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
fact_value_base.show(3, truncate=False)

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
    print("fact_value_base schema:")
    fact_value_base.printSchema()
    print("dim_cryptostock_value schema:")
    dim_cryptostock_value.printSchema()
    spark.stop()
    exit(1)

fact_value = fact_value.withColumn("date_key", to_timestamp(col("date_parsed")))

fact_value = fact_value.select(
    lit(None).cast(StringType()).alias("value_key"),
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
    fact_value.write.format("parquet").mode("overwrite").partitionBy("year").save("/datalake/gold/fact_value")
    print("✅ fact_value written successfully")
else:
    print("⚠️  ERROR: fact_value has 0 records!")

print("\n" + "="*80)
print("✅ PIPELINE COMPLETED")
print("="*80)

spark.stop()