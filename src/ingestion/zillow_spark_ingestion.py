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
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

raw_data_path = "/opt/src/raw_datasets"  # Use Docker mounted path instead of Windows path

print("Starting Bronze Layer: Raw Data Ingestion...")

# Read raw CSV files exactly as they are - USE FORWARD SLASHES
bronze_zillow_data = spark.read.csv(f"{raw_data_path}/ZILLOW_DATA_962c837a6ccefddddf190101e0bafdaf.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_DATA.csv"))

bronze_zillow_indicators = spark.read.csv(f"{raw_data_path}/ZILLOW_INDICATORS_e93833a53d6c88463446a364cda611cc.csv", header=True, inferSchema=True) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ZILLOW_INDICATORS.csv"))

bronze_zillow_regions = spark.read.csv(f"{raw_data_path}/ZILLOW_REGIONS_1a51d107db038a83ac171d604cb48d5b.csv", header=True, inferSchema=True) \
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
    # .dropDuplicates(["indicator_id"]) \
silver_zillow_data = bronze_zillow_data \
    .filter(col("region_id").isNotNull()) \
    .filter(col("date").isNotNull()) \
    .filter(col("value").isNotNull()) \
    .filter(col("indicator_id").isin(['SAAW', 'SRAW', 'NRAW', 'IRAW', 'CRAW', 'LRAW', 'RSNA', 'RSSA', 'ZSFH', 'ZATT', 'ZABT', 'ZCON'])) \
    .filter(col("date") >= lit("2014-01-01")) \
    .withColumn("value", col("value").cast("decimal(30,10)")) \


    # .dropDuplicates(["indicator_id"]) \
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

# Enrich data by joining metadata with data
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


# Write to Silver layer
    # .partitionBy("indicator") \
silver_enriched_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/silver/zillow_enriched_data")

print("Silver Layer completed: Data cleaned and enriched")


print("Starting Gold Layer: Creating Dimension Tables...")

us_states_to_code = {
    'California': 'CA',
    'Connecticut': 'CT',
    'Texas': 'TX',
    'Utah': 'UT',
    'Montana': 'MT',
    'Florida': 'FL',
    'New York': 'NY',
    'Illinois': 'IL',
    'Ohio': 'OH',
    'North Carolina': 'NC',
    'Michigan': 'MI',
    'New Jersey': 'NJ',
    'Virginia': 'VA',
    'Indiana': 'IN',
    'Missouri': 'MO',
    'Colorado': 'CO',
    'Alabama': 'AL',
    'Louisiana': 'LA',
    'Kentucky': 'KY',
    'Oklahoma': 'OK',
    'North Dakota': 'ND',
    'Alaska': 'AK',
    'District of Columbia': 'DC',
    'South Carolina': 'SC',
    'Hawaii': 'HI',
    'New Hampshire': 'NH',
    'Pennsylvania': 'PA',
    'Washington': 'WA',
    'Arizona': 'AZ',
    'Massachusetts': 'MA',
    'Iowa': 'IA',
    'Arkansas': 'AR',
    'Mississippi': 'MS',
    'Kansas': 'KS',
    'New Mexico': 'NM',
    'Nebraska': 'NE',
    'Tennessee': 'TN',
    'Wyoming': 'WY',
    'Georgia': 'GA',
    'Delaware': 'DE',
    'Minnesota': 'MN',
    'Oregon': 'OR',
    'Maryland': 'MD',
    'Wisconsin': 'WI',
    'Idaho': 'ID',
    'Nevada': 'NV',
    'Maine': 'ME',
    'West Virginia': 'WV',
    'Vermont': 'VT',
    'Rhode Island': 'RI',
    'South Dakota': 'SD'
}

us_code_to_states = {
    'CA': 'California',
    'CT': 'Connecticut',
    'TX': 'Texas',
    'UT': 'Utah',
    'MT': 'Montana',
    'FL': 'Florida',
    'NY': 'New York',
    'IL': 'Illinois',
    'OH': 'Ohio',
    'NC': 'North Carolina',
    'MI': 'Michigan',
    'NJ': 'New Jersey',
    'VA': 'Virginia',
    'IN': 'Indiana',
    'MO': 'Missouri',
    'CO': 'Colorado',
    'AL': 'Alabama',
    'LA': 'Louisiana',
    'KY': 'Kentucky',
    'OK': 'Oklahoma',
    'ND': 'North Dakota',
    'AK': 'Alaska',
    'DC': 'District of Columbia',
    'SC': 'South Carolina',
    'HI': 'Hawaii',
    'NH': 'New Hampshire',
    'PA': 'Pennsylvania',
    'WA': 'Washington',
    'AZ': 'Arizona',
    'MA': 'Massachusetts',
    'IA': 'Iowa',
    'AR': 'Arkansas',
    'MS': 'Mississippi',
    'KS': 'Kansas',
    'NM': 'New Mexico',
    'NE': 'Nebraska',
    'TN': 'Tennessee',
    'WY': 'Wyoming',
    'GA': 'Georgia',
    'DE': 'Delaware',
    'MN': 'Minnesota',
    'OR': 'Oregon',
    'MD': 'Maryland',
    'WI': 'Wisconsin',
    'ID': 'Idaho',
    'NV': 'Nevada',
    'ME': 'Maine',
    'WV': 'West Virginia',
    'VT': 'Vermont',
    'RI': 'Rhode Island',
    'SD': 'South Dakota'
}

get_indicator_description = {
    'CRAW':'Percentage of listings with a price reduction (RAW, ALL HOMES, WEEKLY)',
    'IRAW':'Number of properties listed for sale (RAW, ALL HOMES, WEEKLY)',
    'LRAW':'Average list price (RAW, ALL HOMES, WEEKLY)',
    'NRAW':'Average days to properties enter pending status (RAW, ALL HOMES, WEEKLY)',
    'RSNA':'ZOOM rent index, rental trends (SMOOTHED, ALL HOMES + MULTI-FAMILY)',
    'RSSA':'ZOOM rent index, rental trends (SMOOTHED + SEASONALLY ADJUSTED, ALL HOMES + MULTI-FAMILY)',
    'SAAW':'Average sale price (SMOOTHED + SEASONALLY ADJUSTED, ALL HOMES, WEEKLY)',
    'SRAW':'Average sale price (RAW, ALL HOMES, WEEKLY)',
    'ZABT':'Typical home values - Segment: Bottom Tier',
    'ZATT':'Typical home values - Segment: Top Tier',
    'ZCON':'Typical home values - Segment: Condo/Co-op',
    'ZSFH':'Typical home values - Segment: Single Family Homes',
}

get_unit = {

    'CRAW': "PERCENTAGE",
    'IRAW': "UNITS",
    'LRAW': "USD_CURRENT",
    'NRAW': "DAYS",
    'RSNA': "USD_CURRENT",
    'RSSA': "USD_CURRENT",
    'SAAW': "USD_CURRENT",
    'SRAW': "USD_CURRENT",
    'ZABT': "USD_CURRENT",
    'ZATT': "USD_CURRENT",
    'ZCON': "USD_CURRENT",
    'ZSFH': "USD_CURRENT",
}

# Read from Silver layer
silver_data_df = spark.read.format("delta").load("/datalake/silver/zillow_enriched_data")

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf, year, weekofyear, month, date_format

# Define UDFs for your processing functions
@udf(returnType=StringType())
def extract_state_code(region, region_type):
    """Extract state code from region based on region_type"""
    if region_type == 'state':
        return us_states_to_code.get(region)
    elif region_type == 'metro':
        if ',' in region:
            return region.rsplit(',', 1)[1].strip()
        else:
            parts = region.split(';')
            return parts[1] if len(parts) > 1 else None
    elif region_type in ['county', 'city', 'zip', 'neigh']:
        parts = region.split(';')
        return parts[1] if len(parts) > 1 else None
    return None

@udf(returnType=StringType())
def extract_state_name(state_code):
    """Convert state code to state name"""
    return us_code_to_states.get(state_code.strip()) if state_code else None

@udf(returnType=StringType())
def extract_metro_name(region, region_type):
    """Extract metro name from region"""
    if region_type == 'metro':
        if ',' in region:
            return region.rsplit(',', 1)[0].strip()
        return region
    elif region_type in ['county', 'city', 'zip']:
        parts = region.split(';')
        metro_idx = 2
        if len(parts) > metro_idx:
            metro = parts[metro_idx]
            if ',' in metro:
                metro = metro.rsplit(',', 1)[0]
            return metro if metro != 'nan' else None
    elif region_type == 'neigh':
        parts = region.split(';')
        if len(parts) > 2:
            metro = parts[2]
            if ',' in metro:
                metro = metro.rsplit(',', 1)[0]
            return metro if metro != 'nan' else None
    return None

@udf(returnType=StringType())
def extract_county_name(region, region_type):
    """Extract county name from region"""
    if region_type == 'county':
        return region.split(';')[0]
    elif region_type in ['city', 'zip']:
        parts = region.split(';')
        return parts[3] if len(parts) > 3 else None
    elif region_type == 'neigh':
        parts = region.split(';')
        return parts[4] if len(parts) > 4 else None
    return None

@udf(returnType=StringType())
def extract_city_name(region, region_type):
    """Extract city name from region"""
    if region_type == 'city':
        return region.split(';')[0]
    elif region_type == 'zip':
        parts = region.split(';')
        return parts[3] if len(parts) > 3 else None
    elif region_type == 'neigh':
        parts = region.split(';')
        return parts[3] if len(parts) > 3 else None
    return None

@udf(returnType=StringType())
def extract_zip_key(region, region_type):
    """Extract ZIP code from region"""
    if region_type == 'zip':
        zip_key = region.split(';')[0]
        return zip_key if zip_key != 'nan' else '00000'
    return None

@udf(returnType=StringType())
def extract_neighborhood_name(region, region_type):
    """Extract neighborhood name from region"""
    if region_type == 'neigh':
        return region.split(';')[0]
    return None

@udf(returnType=StringType())
def get_indicator_desc(indicator_id):
    """Get indicator description"""
    return get_indicator_description.get(indicator_id)

@udf(returnType=StringType())
def get_value_unit(indicator_id):
    """Get unit for indicator"""
    return get_unit.get(indicator_id)

# Apply transformations using native PySpark operations
enriched_gold_data = silver_data_df \
    .withColumn("country_name", lit("United States")) \
    .withColumn("country_code", lit("USA")) \
    .withColumn("state_code", extract_state_code(col("region"), col("region_type"))) \
    .withColumn("state_name", extract_state_name(col("state_code"))) \
    .withColumn("metro_name", extract_metro_name(col("region"), col("region_type"))) \
    .withColumn("county_name", extract_county_name(col("region"), col("region_type"))) \
    .withColumn("city_name", extract_city_name(col("region"), col("region_type"))) \
    .withColumn("zip_key", extract_zip_key(col("region"), col("region_type"))) \
    .withColumn("neighborhood_name", extract_neighborhood_name(col("region"), col("region_type"))) \
    .withColumn("region_type", col("region_type")) \
    .withColumn("region_id", col("region_id")) \
    .withColumn("indicator_id", col("indicator_id")) \
    .withColumn("indicator_name", col("indicator")) \
    .withColumn("indicator_description", get_indicator_desc(col("indicator_id"))) \
    .withColumn("date", col("date").cast("date")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("day_name", date_format(col("date"), "EEEE")) \
    .withColumn("week_number", weekofyear(col("date"))) \
    .withColumn("month_number", month(col("date"))) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("value", col("value")) \
    .withColumn("unit", get_value_unit(col("indicator_id"))) \
    .withColumn(
        "country_key",
        F.hash("country_code") \
    ).withColumn(
        "state_key",
        F.hash("state_code") \
    ).withColumn(
        "metro_key",
        F.hash("metro_name") \
    ).withColumn(
        "county_key",
        F.hash("county_name") \
    ).withColumn(
        "city_key",
        F.hash("city_name") \
    ).withColumn(
        "neighborhood_key",
        F.hash("neighborhood_name") \
    )


enriched_gold_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/datalake/enrgold/enriched_gold_data")


dim_country = (
    enriched_gold_data.select("country_key", "country_code", "country_name")
    .where(col("country_code").isNotNull())
    .distinct()
    .select("country_key", "country_name", "country_code")
)
dim_country.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_country")

# dim_state
dim_state = (
    enriched_gold_data.select("state_key", "country_key", "state_code", "state_name")
    .where(col("state_code").isNotNull())
    .where(col("state_name").isNotNull())
    .distinct()
    .select("state_key", "state_name", "state_code", "country_key")
)
dim_state.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_state")

# dim_metro
dim_metro = (
    enriched_gold_data.select("metro_key", "state_key", "metro_name")
    .where(col("metro_name").isNotNull())
    .distinct()
    .select("metro_key", "metro_name", "state_key")
)
dim_metro.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_metro")

# dim_county
dim_county = (
    enriched_gold_data.select("county_key", "metro_key", "county_name")
    .where(col("county_name").isNotNull())
    .distinct()
    .select("county_key", "county_name", "metro_key")
)
dim_county.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_county")

# dim_city
dim_city = (
    enriched_gold_data.select("city_key", "city_name", "county_key")
    .where(col("city_name").isNotNull())
    .distinct()
    .select("city_key", "city_name", "county_key")
)
dim_city.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_city")

# dim_zip
dim_zip = (
    enriched_gold_data.select("zip_key", "city_key")
    .where(col("zip_key").isNotNull())
    .distinct()
    .select(col("zip_key"), col("city_key"))
)
dim_zip.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_zip")

# dim_neighborhood
dim_neighborhood = (
    enriched_gold_data.select("neighborhood_key", "neighborhood_name", "city_key")
    .where(col("neighborhood_name").isNotNull())
    .distinct()
    .select("neighborhood_key", "neighborhood_name", "city_key")
)
dim_neighborhood.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_neighborhood")

dim_region = (
    enriched_gold_data
        .select(
            "region_id",
            "region_type",
            "country_key",
            "state_key",
            "metro_key",
            "county_key",
            "city_key",
            "zip_key",
            "neighborhood_key",
        )
        .where(F.col("region_id").isNotNull())
        .where(F.col("region_type").isNotNull())
        .dropDuplicates(["region_id"])
        .select(
            F.col("region_id").alias("region_key"),
            "region_type",
            "country_key",
            "state_key",
            "metro_key",
            "county_key",
            "city_key",
            "zip_key",
            "neighborhood_key",
        )
)

dim_region.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_region")

# dim_realestate_indicator
dim_realestate_indicator = (
    enriched_gold_data.select(
        col("indicator_id").alias("realestate_indicator_key"), "indicator_name", "indicator_description", col("region_id").alias("region_key")
    )
    .where(col("realestate_indicator_key").isNotNull())
    .where(col("indicator_name").isNotNull())
    .where(col("indicator_description").isNotNull())
    .where(col("region_key").isNotNull())
    .distinct()
    .select(
        col("realestate_indicator_key"),
        col("region_key"),
        "indicator_name",
        "indicator_description",
    )
)
dim_realestate_indicator.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_realestate_indicator")

# dim_time
dim_time = (
    enriched_gold_data.select(
        col("date").alias("date_key"),
        "day_name",
        "week_number",
        "month_number",
        "month_name",
        "year",
    )
    .where(col("date_key").isNotNull())
    .where(col("day_name").isNotNull())
    .where(col("week_number").isNotNull())
    .where(col("month_number").isNotNull())
    .where(col("month_name").isNotNull())
    .where(col("year").isNotNull())
    .distinct()
)
dim_time.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_time")


# (Optional) If you want dim_value, you can keep it, but it is expensive.
# Here it is simplified; you may remove it and put value/unit in the fact.
dim_value = (
    enriched_gold_data.select("value", "unit")
    .where(col("value").isNotNull())
    .where(col("unit").isNotNull())
    .distinct()
    .withColumn("value_key", monotonically_increasing_id())
    .select("value_key", "value", "unit")
)
dim_value.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/dim_value")

# ====================================================
# GOLD – Fact
# ====================================================


fact_value = (
    enriched_gold_data.alias("e")
    # Join com dim_value para obter value_key
    .join(
        dim_value.alias("v"),
        (col("e.value") == col("v.value"))
        & (col("e.unit") == col("v.unit")),
        "inner",
    )
    .join(
        dim_realestate_indicator.alias("ri"),
        (col("e.indicator_id") == col("ri.realestate_indicator_key"))
        & (col("e.region_id") == col("ri.region_key")),
        "inner",
    )
    .join(
        dim_time.alias("d"),
        col("e.date") == col("d.date_key"),
        "inner",
    )
    .join(                       
        dim_region.alias("r"),
        col("e.region_id").cast(IntegerType()) == col("r.region_key").cast(IntegerType()),
        "inner",
    )
    .select(
        col("v.value_key").alias("value_key"),
        lit(None).cast(StringType()).alias("socioeconomical_indicator_key"),
        col("ri.realestate_indicator_key").alias(
            "realestate_indicator_key"
        ),
        lit(None).cast(StringType()).alias("cryptostock_value_key"),
        col("r.country_key").alias("country_key"),
        col("d.date_key").alias("date_key"),
        lit(None).cast(StringType()).alias("asset_key"),
    )
)
fact_value.write.format("delta").mode("overwrite").save("/datalake/gold_zillow/fact_value")

print("Gold Layer completed: Dimension and fact tables created")

spark.stop()