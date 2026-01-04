from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, year, explode, input_file_name, substring
)
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

print("\n[BRONZE] Reading JSON files with Spark...")

json_path = f"{raw_data_path}/*.json"

raw_df = spark.read.option("multiline", "true").json(json_path)

print("Raw JSON schema:")
raw_df.printSchema()
print(f"Raw files read: {raw_df.count()} JSON objects")

bronze_df = raw_df \
    .select(
        col("symbol"),
        col("type"),
        explode(col("bars")).alias("bar"),
        input_file_name().alias("source_file")
    ) \
    .select(
        col("symbol").cast("string"),
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

print("\n" + "="*80)
print("✅ PIPELINE COMPLETED SUCCESSFULLY")
print("="*80)

spark.stop()