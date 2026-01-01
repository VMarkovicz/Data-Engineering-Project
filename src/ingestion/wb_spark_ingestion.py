from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp, lit, row_number, concat_ws, md5, make_date, dayofweek, weekofyear, month, date_format
from pyspark.sql.window import Window

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

print("Starting Gold Layer: Creating Dimension Tables...")

# Unit mapping dictionary (keep your existing dict)
series_unit_dict = {
    'NY.ADJ.NNAT.GN.ZS': 'PERCENTAGE',
    'NY.GNP.PCAP.PP.KD': 'USD_CONSTANT',
    'NY.GDP.MKTP.PP.CD': 'USD_CURRENT',
    'CM.MKT.TRAD.CD': 'USD_CURRENT',
    'FP.CPI.TOTL': 'INDEX',
    'GC.REV.XGRT.GD.ZS': 'PERCENTAGE',
    'TM.VAL.MRCH.CD.WT': 'USD_CURRENT',
    'GC.DOD.TOTL.CN': 'LCU_CURRENT',
    'GC.XPN.TOTL.GD.ZS': 'PERCENTAGE',
    'FB.AST.NPER.ZS': 'PERCENTAGE',
    'NY.ADJ.NNTY.KD.ZG': 'PERCENTAGE',
    'PA.NUS.ATLS': 'LCU_CURRENT',
    'NY.GDP.DEFL.KD.ZG': 'PERCENTAGE_GROWTH',
    'NE.GDI.STKB.CD': 'USD_CURRENT',
    'GC.DOD.TOTL.GD.ZS': 'PERCENTAGE',
    'NY.GDP.MKTP.PP.KD': 'USD_CONSTANT',
    'BN.CAB.XOKA.GD.ZS': 'PERCENTAGE',
    'NE.EXP.GNFS.KD': 'USD_CONSTANT',
    'NE.RSB.GNFS.CD': 'USD_CURRENT',
    'NE.EXP.GNFS.CD': 'USD_CURRENT',
    'CM.MKT.TRAD.GD.ZS': 'PERCENTAGE',
    'FS.AST.PRVT.GD.ZS': 'PERCENTAGE',
    'DT.DOD.DECT.CD': 'USD_CURRENT',
    'FM.LBL.BMNY.CN': 'LCU_CURRENT',
    'FM.LBL.BMNY.ZG': 'PERCENTAGE',
    'NE.IMP.GNFS.CD': 'USD_CURRENT',
    'CM.MKT.LCAP.CD': 'USD_CURRENT',
    'NE.CON.PRVT.KD.ZG': 'PERCENTAGE',
    'NE.GDI.TOTL.CD': 'USD_CURRENT',
    'CM.MKT.TRNR': 'PERCENTAGE',
    'NE.CON.PRVT.CD': 'USD_CURRENT',
    'PX.REX.REER': 'INDEX',
    'NY.GNP.MKTP.CD': 'USD_CURRENT',
    'NY.GDP.MKTP.KD': 'USD_CONSTANT',
    'NV.AGR.TOTL.ZS': 'PERCENTAGE',
    'NY.GDP.MKTP.KD.ZG': 'PERCENTAGE',
    'NV.SRV.TOTL.ZS': 'PERCENTAGE',
    'FR.INR.RINR': 'PERCENTAGE',
    'NY.GDP.PCAP.KD': 'USD_CONSTANT',
    'NE.IMP.GNFS.KD.ZG': 'PERCENTAGE',
    'PA.NUS.PRVT.PP': 'INTERNATIONAL_DOLLAR_CONSTANT',
    'NV.IND.TOTL.ZS': 'PERCENTAGE',
    'NV.IND.MANF.ZS': 'PERCENTAGE',
    'NE.GDI.TOTL.KD.ZG': 'PERCENTAGE',
    'NE.GDI.FTOT.ZS': 'PERCENTAGE',
    'NE.GDI.TOTL.KD': 'USD_CONSTANT',
    'NY.GNP.PCAP.CD': 'USD_CURRENT',
    'NY.GNS.ICTR.ZS': 'PERCENTAGE',
    'NY.GNP.PCAP.PP.CD': 'USD_CURRENT',
    'NY.GDP.DEFL.KD.ZG.AD': 'PERCENTAGE',
    'TX.VAL.TECH.CD': 'USD_CURRENT',
    'NY.GNP.MKTP.PP.CD': 'USD_CURRENT',
    'FP.CPI.TOTL.ZG': 'PERCENTAGE',
    'NE.CON.GOVT.ZS': 'PERCENTAGE',
    'FI.RES.XGLD.CD': 'USD_CURRENT',
    'NE.CON.TOTL.ZS': 'PERCENTAGE',
    'IC.CRD.INFO.XQ': 'INDEX',
    'SL.TLF.TOTL.IN': 'COUNT',
    'BN.GSR.FCTY.CD': 'USD_CURRENT',
    'TM.TAX.MRCH.WM.AR.ZS': 'PERCENTAGE',
    'NY.GDP.PCAP.CD': 'USD_CURRENT',
    'NE.CON.GOVT.CD': 'USD_CURRENT',
    'PA.NUS.PPPC.RF': 'RATIO',
    'BM.GSR.GNFS.CD': 'USD_CURRENT',
    'NY.GDP.PCAP.KD.ZG': 'PERCENTAGE',
    'NE.GDI.TOTL.ZS': 'PERCENTAGE',
    'FI.RES.TOTL.MO': 'COUNT',
    'NY.GNS.ICTR.CD': 'USD_CURRENT',
    'NE.IMP.GNFS.KD': 'USD_CONSTANT',
    'NE.IMP.GNFS.ZS': 'PERCENTAGE',
    'NY.GDP.PCAP.PP.KD': 'USD_CONSTANT',
    'NY.GDP.MKTP.CD': 'USD_CURRENT',
    'BM.KLT.DINV.CD.WD': 'USD_CURRENT',
    'NY.GDP.PCAP.PP.CD': 'USD_CURRENT',
    'BX.KLT.DINV.WD.GD.ZS': 'PERCENTAGE',
    'BX.TRF.PWKR.CD.DT': 'USD_CURRENT',
    'NE.EXP.GNFS.KD.ZG': 'PERCENTAGE',
    'BN.CAB.XOKA.CD': 'USD_CURRENT',
    'FD.RES.LIQU.AS.ZS': 'PERCENTAGE',
    'CM.MKT.LCAP.GD.ZS': 'PERCENTAGE',
    'FR.INR.LNDP': 'PERCENTAGE',
    'NE.CON.TOTL.CD': 'USD_CURRENT',
    'PA.NUS.FCRF': 'LCU_CURRENT',
    'BN.RES.INCL.CD': 'USD_CURRENT',
    'FR.INR.LEND': 'PERCENTAGE',
    'CM.MKT.LDOM.NO': 'COUNT',
    'CM.MKT.INDX.ZG': 'PERCENTAGE_GROWTH',
    'GC.NLD.TOTL.GD.ZS': 'PERCENTAGE',
    'BX.GSR.GNFS.CD': 'USD_CURRENT',
    'NE.EXP.GNFS.ZS': 'PERCENTAGE',
    'GC.REV.XGRT.CN': 'LCU_CURRENT',
    'NE.RSB.GNFS.ZS': 'PERCENTAGE',
    'DT.TDS.DECT.EX.ZS': 'PERCENTAGE',
    'FS.AST.DOMS.GD.ZS': 'PERCENTAGE',
    'PA.NUS.PPP': 'INTERNATIONAL_DOLLAR_CONSTANT',
    'NE.CON.PRVT.ZS': 'PERCENTAGE',
    'BX.KLT.DINV.CD.WD': 'USD_CURRENT',
    'SL.UEM.TOTL.ZS': 'PERCENTAGE',
    'NY.ADJ.NNTY.PC.CD': 'USD_CURRENT',
    'FI.RES.TOTL.CD': 'USD_CURRENT',
    'IC.LGL.CRED.XQ': 'INDEX',
    'NY.GDP.DEFL.ZS': 'PERCENTAGE',
    'TX.VAL.TECH.MF.ZS': 'PERCENTAGE',
    'TX.VAL.MRCH.CD.WT': 'USD_CURRENT',
    'TG.VAL.TOTL.GD.ZS': 'PERCENTAGE',
    'FB.CBK.BRCH.P5': 'OTHER',
    'FB.BNK.CAPA.ZS': 'PERCENTAGE',
    'FM.LBL.BMNY.GD.ZS': 'PERCENTAGE',
    'GC.XPN.TOTL.CN': 'LCU_CURRENT',
    'GC.TAX.TOTL.CN': 'LCU_CURRENT',
    'GC.TAX.TOTL.GD.ZS': 'PERCENTAGE'
}
# Read from Silver layer
silver_data_df = spark.read.format("delta").load("/datalake/silver/wb_enriched_data")
silver_metadata_df = spark.read.format("delta").load("/datalake/silver/wb_metadata")

# FIXED: Create dim_country - deduplicate by country_code only
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("country_code").orderBy("country_name")

dim_country = silver_data_df.select(
    col("country_name"),
    col("country_code")
).distinct() \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .withColumn("country_key", monotonically_increasing_id()) \
    .select(
        "country_key",
        "country_name",
        "country_code"
    )

print(f"dim_country created with {dim_country.count()} unique countries")

# Create dim_socioeconomical_indicator
dim_socio_indicator = silver_metadata_df.select(
    col("series_id").alias("socioeconomical_indicator_key"),
    col("name"),
    col("description")
).distinct()

dim_time_year = (
    silver_data_df 
    .withColumn("month_number", lit(12))
    .withColumn("day_number", lit(31))
    .withColumn("date_key", make_date(col("year"), col("month_number"), col("day_number")))
)

dim_time = (
    dim_time_year
    .withColumn("day_name",  date_format(col("date_key"), "EEEE"))
    .withColumn("week_number", weekofyear(col("date_key")))
    .withColumn("month_name", date_format(col("date_key"), "MMMM"))
    .select(
        "date_key",    
        "day_name",
        "week_number",
        "month_number",
        "month_name",
        "year",
    )
    .distinct()
)

# Create unit mapping DataFrame
unit_mapping = spark.createDataFrame(
    [(k, v) for k, v in series_unit_dict.items()],
    ["series_id", "unit"]
)

# Create enriched dataset
enriched_data = silver_data_df.alias("silver") \
    .join(dim_country.alias("country"), 
          col("silver.country_code") == col("country.country_code"),
          "inner") \
    .join(unit_mapping.alias("units"),
          col("silver.series_id") == col("units.series_id"),
          "left") \
    .join(dim_time.alias("time"),
          (col("silver.year") == col("time.year")), 
            "inner") \
    .withColumn("value_key", 
                md5(concat_ws("||", 
                             col("silver.value").cast("string"),
                             col("silver.series_id"),
                             col("silver.country_code"),
                             col("silver.year").cast("string")))) \
    .select(
        col("value_key"),
        col("silver.value").alias("value"),
        col("units.unit").alias("unit"),
        col("time.date_key").alias("date_key"),
        col("silver.series_id").alias("socioeconomical_indicator_key"),
        col("country.country_key").alias("country_key")
    )

# Fill null units with UNKNOWN
enriched_data = enriched_data.na.fill({"unit": "UNKNOWN"})

# Cache since we use it twice
enriched_data.cache()

# Create dim_value from enriched data
dim_value = enriched_data.select(
    "value_key",
    "value",
    "unit"
).distinct()

# Create fact_value - deduplication by business key
fact_value = enriched_data.select(
    "value_key",
    lit(None).cast("string").alias("asset_key"),
    "date_key",
    "socioeconomical_indicator_key",
    lit(None).cast("string").alias("realstate_indicator_key"),
    lit(None).cast("string").alias("cryptostock_value_key"),
    "country_key"
).dropDuplicates([
    "socioeconomical_indicator_key",
    "country_key",
    "date_key"
])

# Write to Gold layer
print("Writing Gold layer tables...")
dim_country.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_country")
dim_socio_indicator.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_socioeconomical_indicator")
dim_time.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_time")
dim_value.write.format("parquet").mode("overwrite").save("/datalake/gold/dim_value")
fact_value.write.format("parquet").mode("overwrite").save("/datalake/gold/fact_value_wb")

# Unpersist cache
enriched_data.unpersist()

print("Gold Layer completed: Dimension tables ready for DuckDB")

# ====================================================================
# SUMMARY
# ====================================================================
print("\n=== Medallion Architecture Summary ===")
print(f"Bronze: Raw data ingested from CSV files")
print(f"Silver: {silver_enriched_data.count()} cleaned and enriched records")
print(f"Gold: {dim_country.count()} countries, {dim_time.count()} years")
print(f"Gold: {fact_value.count()} fact records ready for analytics")

spark.stop()
