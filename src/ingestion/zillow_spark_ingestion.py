from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp, lit, row_number, concat_ws, md5
from pyspark.sql.window import Window

# Initialize Spark Session - Delta JARs already loaded from /opt/spark/jars/
spark = SparkSession.builder \
    .appName("FinancialDataWarehouse_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

raw_data_path = "/opt/src/raw_datasets"  # Use Docker mounted path instead of Windows path

print("Starting Bronze Layer: Raw Data Ingestion...")

# Read raw CSV files exactly as they are - USE FORWARD SLASHES
bronze_zillow_data = spark.read.csv(f"{raw_data_path}/ZILLOW_DATA_962c837a6ccefddddf190101e0bafdaf/ZILLOW_DATA_962c837a6ccefddddf190101e0bafdaf.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_DATA.csv"))

bronze_zillow_indicators = spark.read.csv(f"{raw_data_path}/ZILLOW_INDICATORS_e93833a53d6c88463446a364cda611cc/ZILLOW_INDICATORS_e93833a53d6c88463446a364cda611cc.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_INDICATORS.csv"))

bronze_zillow_regions = spark.read.csv(f"{raw_data_path}/ZILLOW_REGIONS_1a51d107db038a83ac171d604cb48d5b/ZILLOW_REGIONS_1a51d107db038a83ac171d604cb48d5b.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_REGIONS.csv"))

# Write to Bronze layer (preserve raw data as Delta tables)
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

# Read from Bronze layer
bronze_zillow_data = spark.read.format("delta").load("/datalake/bronze/zillow_data")
bronze_zillow_indicators = spark.read.format("delta").load("/datalake/bronze/zillow_indicators")
bronze_zillow_regions = spark.read.format("delta").load("/datalake/bronze/zillow_regions")



# Data Quality Checks & Cleaning
silver_zillow_data = bronze_zillow_data \
    .dropDuplicates(["indicator_id"]) \
    .filter(col("region_id").isNotNull()) \
    .filter(col("date").isNotNull()) \
    .filter(col("value").isNotNull()) \
    .withColumn("value", col("value").cast("decimal(30,10)")) \
    .withColumn("processed_timestamp", current_timestamp())

silver_zillow_indicators = bronze_zillow_indicators \
    .dropDuplicates(["indicator_id"]) \
    .filter(col("indicator").isNotNull())

silver_zillow_indicators_clean = silver_zillow_indicators.drop(
    "ingestion_timestamp", 
    "source_file",
    "processed_timestamp",
    'category'
)



silver_zillow_regions = bronze_zillow_regions \
    .dropDuplicates(["region_id"]) \
    .filter(col("region").isNotNull()) \
    .filter(col("region_type").isNotNull())

silver_zillow_regions_clean = silver_zillow_regions.drop(
    "ingestion_timestamp",
    "source_file",
    "processed_timestamp"
)


# Enrich data by joining metadata with data
silver_enriched_data = silver_zillow_data.join(
    silver_zillow_indicators_clean,
    on="indicator_id",
    how="left"
).select(
    col("indicator_id"),
    col("region_id"),
    col("date"),
    col("value"),
    col("indicator"),
    col("ingestion_timestamp"),
    col("processed_timestamp")
).dropDuplicates(["indicator_id", "region_id", "date"])

silver_enriched_data = silver_enriched_data.join(
    silver_zillow_regions_clean,
    on="region_id",
    how="left"
).select(
    col("indicator_id"),
    col("region_id"),
    col("date"),
    col("value"),
    col("indicator"),
    col("region_type"),
    col("region"),
    col("ingestion_timestamp"),
    col("processed_timestamp")
).dropDuplicates(["indicator_id", "region_id", "date"])


# Write to Silver layer
silver_enriched_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("indicator") \
    .save("/datalake/silver/zillow_enriched_data")

print("Silver Layer completed: Data cleaned and enriched")

