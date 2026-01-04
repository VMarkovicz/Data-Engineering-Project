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

print("\n" + "="*80)
print("✅ PIPELINE COMPLETED")
print("="*80)

spark.stop()