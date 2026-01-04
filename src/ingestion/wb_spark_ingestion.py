from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp, lit, row_number, concat_ws, md5, make_date, weekofyear, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize Spark Session - Delta JARs already loaded from /opt/spark/jars/
spark = SparkSession.builder \
    .appName("FinancialDataWarehouse_Medallion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

raw_data_path = "/opt/src/raw_datasets"

print("Starting Bronze Layer: Raw Data Ingestion...")

# Read raw CSV files exactly as they are
bronze_wb_data = spark.read.csv(f"{raw_data_path}/WB_DATA_usa_2014_onwards.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("WB_DATA_usa_2014_onwards.csv"))

bronze_wb_metadata = spark.read.csv(f"{raw_data_path}/WB_METADATA_usa_2014_onwards.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("WB_METADATA_usa_2014_onwards.csv"))
# Write to Bronze layer (preserve raw data as Delta tables)
bronze_wb_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/bronze/wb_data")

bronze_wb_metadata.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/bronze/wb_metadata")

print("Bronze Layer completed: Raw data stored successfully")


print("Starting Silver Layer: Data Cleaning and Transformation...")

# Read from Bronze layer
bronze_wb_data_df = spark.read.format("delta").load("/datalake/bronze/wb_data")
bronze_wb_metadata_df = spark.read.format("delta").load("/datalake/bronze/wb_metadata")

# Data Quality Checks & Cleaning
silver_wb_data = bronze_wb_data_df \
    .dropDuplicates(["series_id", "country_code", "year"]) \
    .filter(col("value").isNotNull()) \
    .filter(col("year").isNotNull()) \
    .filter(col("series_id").isNotNull()) \
    .withColumn("value", col("value").cast("decimal(30,10)")) \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("processed_timestamp", current_timestamp())

silver_wb_metadata = bronze_wb_metadata_df \
    .dropDuplicates(["series_id"]) \
    .filter(col("series_id").isNotNull()) \
    .filter(col("name").isNotNull())

silver_wb_metadata_clean = silver_wb_metadata.drop(
    "ingestion_timestamp", 
    "source_file",
    "processed_timestamp"
)

# Enrich data by joining metadata with data
silver_enriched_data = silver_wb_data.join(
    silver_wb_metadata_clean,
    on="series_id",
    how="left"
).select(
    col("series_id"),
    col("country_code"),
    col("country_name"),
    col("year"),
    col("value"),
    col("name").alias("indicator_name"),
    col("description").alias("indicator_description"),
    col("ingestion_timestamp"),
    col("processed_timestamp")
).dropDuplicates(["series_id", "country_code", "year"])

# Write to Silver layer
silver_enriched_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year") \
    .save("/datalake/silver/wb_enriched_data")

silver_wb_metadata.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/datalake/silver/wb_metadata")

print("Silver Layer completed: Data cleaned and enriched")

# ====================================================================
# SUMMARY
# ====================================================================
print("\n=== Medallion Architecture Summary ===")
print(f"Bronze: Raw data ingested from CSV files")
print(f"Silver: {silver_enriched_data.count()} cleaned and enriched records")

spark.stop()
