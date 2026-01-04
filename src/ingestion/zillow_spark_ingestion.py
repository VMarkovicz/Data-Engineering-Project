from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder \
    .appName("FinancialDataWarehouse_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

raw_data_path = "/opt/src/raw_datasets" 

print("Starting Bronze Layer: Raw Data Ingestion...")

bronze_zillow_data = spark.read.csv(f"{raw_data_path}/ZILLOW_DATA_962c837a6ccefddddf190101e0bafdaf.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_DATA.csv"))

bronze_zillow_indicators = spark.read.csv(f"{raw_data_path}/ZILLOW_INDICATORS_e93833a53d6c88463446a364cda611cc.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_INDICATORS.csv"))

bronze_zillow_regions = spark.read.csv(f"{raw_data_path}/ZILLOW_REGIONS_1a51d107db038a83ac171d604cb48d5b.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_REGIONS.csv"))

bronze_zillow_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/bronze/zillow_data")

bronze_zillow_indicators.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/bronze/zillow_indicators")

bronze_zillow_regions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/bronze/zillow_regions")

print("Bronze Layer completed: Raw data stored successfully")

print("Starting Silver Layer: Data Cleaning and Transformation...")

bronze_zillow_data = spark.read.format("delta").load("/datalake/bronze/zillow_data")
bronze_zillow_indicators = spark.read.format("delta").load("/datalake/bronze/zillow_indicators")
bronze_zillow_regions = spark.read.format("delta").load("/datalake/bronze/zillow_regions")

silver_zillow_data = bronze_zillow_data \
    .filter(col("region_id").isNotNull()) \
    .filter(col("date").isNotNull()) \
    .filter(col("value").isNotNull()) \
    .filter(col("indicator_id").isin(['SAAW', 'SRAW', 'NRAW', 'IRAW', 'CRAW', 'LRAW', 'RSNA', 'RSSA', 'ZSFH', 'ZATT', 'ZABT', 'ZCON'])) \
    .filter(col("date") >= lit("2014-01-01")) \
    .withColumn("value", col("value").cast("decimal(30,10)")) \

silver_zillow_indicators = bronze_zillow_indicators \
    .filter(col("indicator").isNotNull())

silver_zillow_indicators_clean = silver_zillow_indicators.drop(
    "source_file",
    'category'
)

silver_zillow_regions = bronze_zillow_regions \
    .dropDuplicates(["region_id"]) \
    .filter(col("region").isNotNull()) \
    .filter(col("region_type").isNotNull())

silver_zillow_regions_clean = silver_zillow_regions.drop(
    "source_file",
)

silver_enriched_data = silver_zillow_data.join(
    silver_zillow_indicators_clean,
    on="indicator_id",
    how="left"
).select(
    col("indicator_id").alias("indicator_id"),
    col("region_id").alias("region_id"),
    col("date").alias("date"),
    col("value").alias("value"),
    col("indicator").alias("indicator"),
)

silver_enriched_data = silver_enriched_data.join(
    silver_zillow_regions_clean,
    on="region_id",
    how="left"
).select(
    col("indicator_id").alias("indicator_id"),
    col("region_id").alias("region_id"),
    col("date").alias("date"),
    col("value").alias("value"),
    col("indicator").alias("indicator"),
    col("region_type").alias("region_type"),
    col("region").alias("region"),
)

silver_enriched_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/silver/zillow_enriched_data")

print("Silver Layer completed: Data cleaned and enriched")

print("\n=== Medallion Architecture Summary ===")
print(f"Bronze: Raw data ingested from CSV files")
print(f"Silver: {silver_enriched_data.count()} cleaned and enriched records")

spark.stop()