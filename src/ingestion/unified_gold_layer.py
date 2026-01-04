# gold_unified_transformation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, year, month, weekofyear, date_format,
    md5, concat_ws, coalesce, row_number, when
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pyspark.sql.functions import udf

spark = SparkSession.builder \
    .appName("Unified_Gold_Layer") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("="*80)
print("UNIFIED GOLD LAYER TRANSFORMATION")
print("="*80)

# ============================================================================
# STEP 1: Read ALL Silver Layers
# ============================================================================
print("\n[STEP 1] Reading all Silver layers...")

wb_silver = spark.read.format("delta").load("/datalake/silver/wb_enriched_data")
wb_metadata = spark.read.format("delta").load("/datalake/silver/wb_metadata")
print(f"✓ World Bank: {wb_silver.count()} records")

zillow_silver = spark.read.format("delta").load("/datalake/silver/zillow_enriched_data")
print(f"✓ Zillow: {zillow_silver.count()} records")

cryptostock_silver = spark.read.format("delta").load("/datalake/silver/cryptostock_stocks")
print(f"✓ Crypto/Stocks: {cryptostock_silver.count()} records")

# ============================================================================
# STEP 2: Build dim_time from ALL sources
# ============================================================================
print("\n[STEP 2] Creating unified dim_time...")

# World Bank: year-only data, convert to Jan 1st of that year
wb_dates = wb_silver.select(
    to_date(concat_ws("-", col("year").cast("string"), lit("01"), lit("01")), "yyyy-MM-dd").alias("date_parsed")
).distinct()

# Zillow: full dates
zillow_dates = zillow_silver.select(
    to_date(col("date"), "yyyy-MM-dd").alias("date_parsed")
).distinct()

# Crypto/Stocks: full dates
crypto_dates = cryptostock_silver.select(
    col("date_parsed")
).distinct()

# Union all dates
all_dates = wb_dates.union(zillow_dates).union(crypto_dates) \
    .distinct() \
    .filter(col("date_parsed").isNotNull())

# Create complete dim_time
dim_time = all_dates \
    .withColumn("date_key", col("date_parsed")) \
    .withColumn("day_name", date_format(col("date_parsed"), "EEEE")) \
    .withColumn("week_number", weekofyear(col("date_parsed"))) \
    .withColumn("month_number", month(col("date_parsed"))) \
    .withColumn("month_name", date_format(col("date_parsed"), "MMMM")) \
    .withColumn("year", year(col("date_parsed"))) \
    .select("date_key", "day_name", "week_number", "month_number", "month_name", "year") \
    .distinct() \
    .orderBy("date_key")

print(f"✓ dim_time: {dim_time.count()} unique dates")
dim_time.write.format("delta").mode("overwrite").save("/datalake/gold/dim_time")

# Reload and cache for fact table joins
dim_time = spark.read.format("delta").load("/datalake/gold/dim_time").cache()
print("✓ dim_time cached for reuse")

# ============================================================================
# STEP 3: Build dim_country from ALL sources
# ============================================================================
print("\n[STEP 3] Creating unified dim_country...")

wb_countries = wb_silver.select(
    col("country_code"),
    col("country_name")
).distinct()

usa_country = spark.createDataFrame([
    ("USA", "United States")
], ["country_code", "country_name"])

all_countries = wb_countries.union(usa_country).distinct()

window_spec = Window.orderBy("country_code")
dim_country = all_countries \
    .withColumn("country_key",
        F.hash("country_code")) \
    .select("country_key", "country_name", "country_code")

print(f"✓ dim_country: {dim_country.count()} countries")
dim_country.write.format("delta").mode("overwrite").save("/datalake/gold/dim_country")

# Reload and cache for reuse
dim_country = spark.read.format("delta").load("/datalake/gold/dim_country").cache()
print("✓ dim_country cached for reuse")

# ============================================================================
# STEP 4: Build Zillow geographic dimensions
# ============================================================================
print("\n[STEP 4] Creating Zillow geographic dimensions...")

# State/region mappings
us_states_to_code = {
    'California': 'CA', 'Texas': 'TX', 'Florida': 'FL', 'New York': 'NY',
    'Illinois': 'IL', 'Pennsylvania': 'PA', 'Ohio': 'OH', 'Georgia': 'GA',
    'North Carolina': 'NC', 'Michigan': 'MI', 'New Jersey': 'NJ', 'Virginia': 'VA',
    'Washington': 'WA', 'Arizona': 'AZ', 'Massachusetts': 'MA', 'Tennessee': 'TN',
    'Indiana': 'IN', 'Missouri': 'MO', 'Maryland': 'MD', 'Wisconsin': 'WI',
    'Colorado': 'CO', 'Minnesota': 'MN', 'South Carolina': 'SC', 'Alabama': 'AL',
    'Louisiana': 'LA', 'Kentucky': 'KY', 'Oregon': 'OR', 'Oklahoma': 'OK',
    'Connecticut': 'CT', 'Utah': 'UT', 'Iowa': 'IA', 'Nevada': 'NV',
    'Arkansas': 'AR', 'Mississippi': 'MS', 'Kansas': 'KS', 'New Mexico': 'NM',
    'Nebraska': 'NE', 'Idaho': 'ID', 'West Virginia': 'WV', 'Hawaii': 'HI',
    'New Hampshire': 'NH', 'Maine': 'ME', 'Montana': 'MT', 'Rhode Island': 'RI',
    'Delaware': 'DE', 'South Dakota': 'SD', 'North Dakota': 'ND', 'Alaska': 'AK',
    'District of Columbia': 'DC', 'Vermont': 'VT', 'Wyoming': 'WY'
}

us_code_to_states = {v: k for k, v in us_states_to_code.items()}

# Region parsing UDFs
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

# Zillow values
zillow_indicator_units = {
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

@udf(returnType=StringType())
def get_zillow_unit(indicator_id):
    return zillow_indicator_units.get(indicator_id, "USD_CURRENT")


# Apply transformations
enriched_zillow = zillow_silver \
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
    .withColumn("value", col("value")) \
    .withColumn("unit", get_zillow_unit(col("indicator_id"))) \
    .withColumn("indicator_id", col("indicator_id")) \
    .withColumn("date", col("date").cast("date")) \
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

# dim_state
dim_state = (
    enriched_zillow.select("state_key", "country_key", "state_code", "state_name")
    .where(col("state_code").isNotNull())
    .where(col("state_name").isNotNull())
    .distinct()
    .select("state_key", "state_name", "state_code", "country_key")
)

print(f"✓ dim_state: {dim_state.count()} states")
dim_state.write.format("delta").mode("overwrite").save("/datalake/gold/dim_state")

# dim_metro
dim_metro = (
    enriched_zillow.select("metro_key", "state_key", "metro_name")
    .where(col("metro_name").isNotNull())
    .distinct()
    .select("metro_key", "metro_name", "state_key")
)

print(f"✓ dim_metro: {dim_metro.count()} metros")
dim_metro.write.format("delta").mode("overwrite").save("/datalake/gold/dim_metro")

# dim_county
dim_county = (
    enriched_zillow.select("county_key", "metro_key", "county_name")
    .where(col("county_name").isNotNull())
    .distinct()
    .select("county_key", "county_name", "metro_key")
)

print(f"✓ dim_county: {dim_county.count()} counties")
dim_county.write.format("delta").mode("overwrite").save("/datalake/gold/dim_county")

# dim_city
dim_city = (
    enriched_zillow.select("city_key", "city_name", "county_key")
    .where(col("city_name").isNotNull())
    .distinct()
    .select("city_key", "city_name", "county_key")
)
print(f"✓ dim_city: {dim_city.count()} cities")
dim_city.write.format("delta").mode("overwrite").save("/datalake/gold/dim_city")

# dim_zip - FIXED: consistent naming
dim_zip = (
    enriched_zillow.select("zip_key", "city_key")
    .where(col("zip_key").isNotNull())
    .distinct()
    .select(col("zip_key"), col("city_key"))
)
print(f"✓ dim_zip: {dim_zip.count()} zip codes")
dim_zip.write.format("delta").mode("overwrite").save("/datalake/gold/dim_zip")

# dim_neighborhood
dim_neighborhood = (
    enriched_zillow.select("neighborhood_key", "neighborhood_name", "city_key")
    .where(col("neighborhood_name").isNotNull())
    .distinct()
    .select("neighborhood_key", "neighborhood_name", "city_key")
)

print(f"✓ dim_neighborhood: {dim_neighborhood.count()} neighborhoods")
dim_neighborhood.write.format("delta").mode("overwrite").save("/datalake/gold/dim_neighborhood")

# dim_region - FIXED: use zip_key consistently
dim_region = (
    enriched_zillow
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

print(f"✓ dim_region: {dim_region.count()} regions")
dim_region.write.format("delta").mode("overwrite").save("/datalake/gold/dim_region")

# ============================================================================
# STEP 5: Build dim_value (unified)
# ============================================================================
print("\n[STEP 5] Creating unified dim_value...")

# World Bank unit mapping
series_unit_dict = {
    'NY.ADJ.NNAT.GN.ZS': 'PERCENTAGE', 'NY.GNP.PCAP.PP.KD': 'USD_CONSTANT',
    'NY.GDP.MKTP.PP.CD': 'USD_CURRENT', 'CM.MKT.TRAD.CD': 'USD_CURRENT',
    'FP.CPI.TOTL': 'INDEX', 'GC.REV.XGRT.GD.ZS': 'PERCENTAGE',
    'TM.VAL.MRCH.CD.WT': 'USD_CURRENT', 'GC.DOD.TOTL.CN': 'LCU_CURRENT',
    'GC.XPN.TOTL.GD.ZS': 'PERCENTAGE', 'FB.AST.NPER.ZS': 'PERCENTAGE',
    'NY.ADJ.NNTY.KD.ZG': 'PERCENTAGE', 'PA.NUS.ATLS': 'LCU_CURRENT',
    'NY.GDP.DEFL.KD.ZG': 'PERCENTAGE_GROWTH', 'NE.GDI.STKB.CD': 'USD_CURRENT',
    'GC.DOD.TOTL.GD.ZS': 'PERCENTAGE', 'NY.GDP.MKTP.PP.KD': 'USD_CONSTANT',
    'BN.CAB.XOKA.GD.ZS': 'PERCENTAGE', 'NE.EXP.GNFS.KD': 'USD_CONSTANT',
    'NE.RSB.GNFS.CD': 'USD_CURRENT', 'NE.EXP.GNFS.CD': 'USD_CURRENT',
    'CM.MKT.TRAD.GD.ZS': 'PERCENTAGE', 'FS.AST.PRVT.GD.ZS': 'PERCENTAGE',
    'DT.DOD.DECT.CD': 'USD_CURRENT', 'FM.LBL.BMNY.CN': 'LCU_CURRENT',
    'FM.LBL.BMNY.ZG': 'PERCENTAGE', 'NE.IMP.GNFS.CD': 'USD_CURRENT',
    'CM.MKT.LCAP.CD': 'USD_CURRENT', 'NE.CON.PRVT.KD.ZG': 'PERCENTAGE',
    'NE.GDI.TOTL.CD': 'USD_CURRENT', 'CM.MKT.TRNR': 'PERCENTAGE',
    'NE.CON.PRVT.CD': 'USD_CURRENT', 'PX.REX.REER': 'INDEX',
    'NY.GNP.MKTP.CD': 'USD_CURRENT', 'NY.GDP.MKTP.KD': 'USD_CONSTANT',
    'NV.AGR.TOTL.ZS': 'PERCENTAGE', 'NY.GDP.MKTP.KD.ZG': 'PERCENTAGE',
    'NV.SRV.TOTL.ZS': 'PERCENTAGE', 'FR.INR.RINR': 'PERCENTAGE',
    'NY.GDP.PCAP.KD': 'USD_CONSTANT', 'NE.IMP.GNFS.KD.ZG': 'PERCENTAGE',
    'PA.NUS.PRVT.PP': 'INTERNATIONAL_DOLLAR_CONSTANT', 'NV.IND.TOTL.ZS': 'PERCENTAGE',
    'NV.IND.MANF.ZS': 'PERCENTAGE', 'NE.GDI.TOTL.KD.ZG': 'PERCENTAGE',
    'NE.GDI.FTOT.ZS': 'PERCENTAGE', 'NE.GDI.TOTL.KD': 'USD_CONSTANT',
    'NY.GNP.PCAP.CD': 'USD_CURRENT', 'NY.GNS.ICTR.ZS': 'PERCENTAGE',
    'NY.GNP.PCAP.PP.CD': 'USD_CURRENT', 'NY.GDP.DEFL.KD.ZG.AD': 'PERCENTAGE',
    'TX.VAL.TECH.CD': 'USD_CURRENT', 'NY.GNP.MKTP.PP.CD': 'USD_CURRENT',
    'FP.CPI.TOTL.ZG': 'PERCENTAGE', 'NE.CON.GOVT.ZS': 'PERCENTAGE',
    'FI.RES.XGLD.CD': 'USD_CURRENT', 'NE.CON.TOTL.ZS': 'PERCENTAGE',
    'IC.CRD.INFO.XQ': 'INDEX', 'SL.TLF.TOTL.IN': 'COUNT',
    'BN.GSR.FCTY.CD': 'USD_CURRENT', 'TM.TAX.MRCH.WM.AR.ZS': 'PERCENTAGE',
    'NY.GDP.PCAP.CD': 'USD_CURRENT', 'NE.CON.GOVT.CD': 'USD_CURRENT',
    'PA.NUS.PPPC.RF': 'RATIO', 'BM.GSR.GNFS.CD': 'USD_CURRENT',
    'NY.GDP.PCAP.KD.ZG': 'PERCENTAGE', 'NE.GDI.TOTL.ZS': 'PERCENTAGE',
    'FI.RES.TOTL.MO': 'COUNT', 'NY.GNS.ICTR.CD': 'USD_CURRENT',
    'NE.IMP.GNFS.KD': 'USD_CONSTANT', 'NE.IMP.GNFS.ZS': 'PERCENTAGE',
    'NY.GDP.PCAP.PP.KD': 'USD_CONSTANT', 'NY.GDP.MKTP.CD': 'USD_CURRENT',
    'BM.KLT.DINV.CD.WD': 'USD_CURRENT', 'NY.GDP.PCAP.PP.CD': 'USD_CURRENT',
    'BX.KLT.DINV.WD.GD.ZS': 'PERCENTAGE', 'BX.TRF.PWKR.CD.DT': 'USD_CURRENT',
    'NE.EXP.GNFS.KD.ZG': 'PERCENTAGE', 'BN.CAB.XOKA.CD': 'USD_CURRENT',
    'FD.RES.LIQU.AS.ZS': 'PERCENTAGE', 'CM.MKT.LCAP.GD.ZS': 'PERCENTAGE',
    'FR.INR.LNDP': 'PERCENTAGE', 'NE.CON.TOTL.CD': 'USD_CURRENT',
    'PA.NUS.FCRF': 'LCU_CURRENT', 'BN.RES.INCL.CD': 'USD_CURRENT',
    'FR.INR.LEND': 'PERCENTAGE', 'CM.MKT.LDOM.NO': 'COUNT',
    'CM.MKT.INDX.ZG': 'PERCENTAGE_GROWTH', 'GC.NLD.TOTL.GD.ZS': 'PERCENTAGE',
    'BX.GSR.GNFS.CD': 'USD_CURRENT', 'NE.EXP.GNFS.ZS': 'PERCENTAGE',
    'GC.REV.XGRT.CN': 'LCU_CURRENT', 'NE.RSB.GNFS.ZS': 'PERCENTAGE',
    'DT.TDS.DECT.EX.ZS': 'PERCENTAGE', 'FS.AST.DOMS.GD.ZS': 'PERCENTAGE',
    'PA.NUS.PPP': 'INTERNATIONAL_DOLLAR_CONSTANT', 'NE.CON.PRVT.ZS': 'PERCENTAGE',
    'BX.KLT.DINV.CD.WD': 'USD_CURRENT', 'SL.UEM.TOTL.ZS': 'PERCENTAGE',
    'NY.ADJ.NNTY.PC.CD': 'USD_CURRENT', 'FI.RES.TOTL.CD': 'USD_CURRENT',
    'IC.LGL.CRED.XQ': 'INDEX', 'NY.GDP.DEFL.ZS': 'PERCENTAGE',
    'TX.VAL.TECH.MF.ZS': 'PERCENTAGE', 'TX.VAL.MRCH.CD.WT': 'USD_CURRENT',
    'TG.VAL.TOTL.GD.ZS': 'PERCENTAGE', 'FB.CBK.BRCH.P5': 'OTHER',
    'FB.BNK.CAPA.ZS': 'PERCENTAGE', 'FM.LBL.BMNY.GD.ZS': 'PERCENTAGE',
    'GC.XPN.TOTL.CN': 'LCU_CURRENT', 'GC.TAX.TOTL.CN': 'LCU_CURRENT',
    'GC.TAX.TOTL.GD.ZS': 'PERCENTAGE'
}

# FIXED: Proper unit mapping for WB
unit_mapping_df = spark.createDataFrame(
    [(k, v) for k, v in series_unit_dict.items()],
    ["series_id", "unit"]
)

wb_values = wb_silver.select(col("value"), col("series_id")) \
    .join(unit_mapping_df, "series_id", "left") \
    .select("value", coalesce(col("unit"), lit("UNKNOWN")).alias("unit")) \
    .distinct()



zillow_values = zillow_silver.select(col("value"), col("indicator_id")) \
    .withColumn("unit", get_zillow_unit(col("indicator_id"))) \
    .select("value", "unit") \
    .distinct()

# Union all values
all_values = wb_values.union(zillow_values).distinct()

# Create unique keys with hash + row_number for determinism
dim_value = all_values \
    .withColumn("value_hash", md5(concat_ws("||", col("value").cast("string"), col("unit")))) \
    .withColumn("value_key", row_number().over(Window.orderBy("value_hash"))) \
    .select("value_key", "value", "unit")

print(f"✓ dim_value: {dim_value.count()} unique values")
dim_value.write.format("delta").mode("overwrite").save("/datalake/gold/dim_value")

# Reload and cache
dim_value = spark.read.format("delta").load("/datalake/gold/dim_value").cache()
print("✓ dim_value cached for reuse")

# ============================================================================
# STEP 6: Build source-specific dimensions
# ============================================================================
print("\n[STEP 6] Creating source-specific dimensions...")

# dim_socioeconomical_indicator
dim_socio_indicator = wb_metadata.select(
    col("series_id").alias("socioeconomical_indicator_key"),
    col("name"),
    col("description")
).distinct()

print(f"✓ dim_socioeconomical_indicator: {dim_socio_indicator.count()}")
dim_socio_indicator.write.format("delta").mode("overwrite") \
    .save("/datalake/gold/dim_socioeconomical_indicator")

# dim_realestate_indicator
zillow_indicator_descriptions = {
    'CRAW': 'Percentage of listings with a price reduction (RAW, ALL HOMES, WEEKLY)',
    'IRAW': 'Number of properties listed for sale (RAW, ALL HOMES, WEEKLY)',
    'LRAW': 'Average list price (RAW, ALL HOMES, WEEKLY)',
    'NRAW': 'Average days to properties enter pending status (RAW, ALL HOMES, WEEKLY)',
    'RSNA': 'ZOOM rent index, rental trends (SMOOTHED, ALL HOMES + MULTI-FAMILY)',
    'RSSA': 'ZOOM rent index, rental trends (SMOOTHED + SEASONALLY ADJUSTED)',
    'SAAW': 'Average sale price (SMOOTHED + SEASONALLY ADJUSTED, ALL HOMES, WEEKLY)',
    'SRAW': 'Average sale price (RAW, ALL HOMES, WEEKLY)',
    'ZABT': 'Typical home values - Segment: Bottom Tier',
    'ZATT': 'Typical home values - Segment: Top Tier',
    'ZCON': 'Typical home values - Segment: Condo/Co-op',
    'ZSFH': 'Typical home values - Segment: Single Family Homes'
}

@udf(returnType=StringType())
def get_zillow_description(indicator_id):
    return zillow_indicator_descriptions.get(indicator_id, "")

dim_realestate_indicator = enriched_zillow.select(
    col("indicator_id").alias("realestate_indicator_key"),
    col("region_id").cast(IntegerType()).alias("region_key"),
    col("indicator").alias("indicator_name")
).distinct() \
    .withColumn("indicator_description", get_zillow_description(col("realestate_indicator_key"))) \
    .select("realestate_indicator_key", "region_key", "indicator_name", "indicator_description")

print(f"✓ dim_realestate_indicator: {dim_realestate_indicator.count()}")
dim_realestate_indicator.write.format("delta").mode("overwrite") \
    .save("/datalake/gold/dim_realestate_indicator")

# dim_asset
dim_asset = cryptostock_silver.select("symbol", "type").distinct() \
    .withColumnRenamed("symbol", "asset_key") \
    .select("asset_key", "type")

print(f"✓ dim_asset: {dim_asset.count()}")
dim_asset.write.format("delta").mode("overwrite").save("/datalake/gold/dim_asset")

# FIXED: dim_cryptostock_value - keep symbol and date for joins
window_crypto = Window.orderBy("symbol", "date_parsed", "high", "low", "close", "volume")
dim_cryptostock_value = cryptostock_silver.select(
    col("symbol"),
    col("date_parsed"),
    col("high"),
    col("low"),
    col("volume").cast(DecimalType(30, 10)),
    col("last").alias("close")
).distinct() \
    .withColumn("cryptostock_value_key", row_number().over(window_crypto)) \
    .select("cryptostock_value_key", "symbol", "date_parsed", "high", "low", "volume", "close")

print(f"✓ dim_cryptostock_value: {dim_cryptostock_value.count()}")
dim_cryptostock_value.write.format("delta").mode("overwrite") \
    .save("/datalake/gold/dim_cryptostock_value")

# ============================================================================
# STEP 7: Build unified fact_value
# ============================================================================
print("\n[STEP 7] Creating unified fact_value...")

# FIXED: World Bank facts - resolve ambiguous 'unit' column
wb_with_unit = wb_silver.alias("wb") \
    .join(unit_mapping_df.alias("um"), col("wb.series_id") == col("um.series_id"), "left") \
    .select(
        col("wb.value"),
        col("wb.series_id"),
        col("wb.country_code"),
        col("wb.year"),
        coalesce(col("um.unit"), lit("UNKNOWN")).alias("wb_unit")
    )

wb_facts = wb_with_unit.alias("wb") \
    .join(dim_value.alias("v"), 
        (col("wb.value") == col("v.value")) & (col("wb.wb_unit") == col("v.unit")), 
        "inner") \
    .join(dim_country.alias("c"), col("wb.country_code") == col("c.country_code"), "inner") \
    .join(dim_time.alias("t"), 
        to_date(concat_ws("-", col("wb.year").cast("string"), lit("01"), lit("01"))) == col("t.date_key"), 
        "inner") \
    .select(
        col("v.value_key").cast(IntegerType()),
        col("wb.series_id").alias("socioeconomical_indicator_key"),
        lit(None).cast(StringType()).alias("realestate_indicator_key"),
        lit(None).cast(StringType()).alias("cryptostock_value_key"),
        col("c.country_key"),
        col("t.date_key"),
        lit(None).cast(StringType()).alias("asset_key")
    ).distinct()

print(f"✓ World Bank facts: {wb_facts.count()}")

# Zillow facts - same pattern
zillow_with_unit = enriched_zillow.alias("z") \
    .select(
        col("z.value"),
        col("z.indicator_id"),
        col("z.region_id"),
        col("z.date"),
        get_zillow_unit(col("z.indicator_id")).alias("zillow_unit")
    )

zillow_facts = (
    enriched_zillow.alias("e")
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
).distinct()
# zillow_with_unit.alias("z") \
#     .join(dim_value.alias("v"), 
#         (col("z.value") == col("v.value")) & (col("z.zillow_unit") == col("v.unit")),
#         "inner") \
#     .join(dim_region.alias("r"), 
#         col("z.region_id").cast(IntegerType()) == col("r.region_key"), 
#         "inner") \
#     .join(dim_time.alias("t"), 
#         to_date(col("z.date")) == col("t.date_key"), 
#         "inner") \
#     .join(dim_realstate_indicator.alias("ri"),
#         (col("z.indicator_id") == col("ri.realestate_indicator_key")) &
#         (col("z.region_id").cast(IntegerType()) == col("ri.region_key")),
#         "inner") \
#     .select(
#         col("v.value_key").cast(IntegerType()),
#         lit(None).cast(StringType()).alias("socioeconomical_indicator_key"),
#         col("ri.realestate_indicator_key"),
#         lit(None).cast(StringType()).alias("cryptostock_value_key"),
#         col("r.country_key"),
#         col("t.date_key"),
#         lit(None).cast(StringType()).alias("asset_key")
#     ).distinct()

print(f"✓ Zillow facts: {zillow_facts.count()}")

# Crypto facts (no changes needed)
crypto_facts = cryptostock_silver.alias("cs") \
    .join(dim_time.alias("t"), col("cs.date_parsed") == col("t.date_key"), "inner") \
    .join(dim_asset.alias("a"), col("cs.symbol") == col("a.asset_key"), "inner") \
    .join(dim_cryptostock_value.alias("cv"),
        (col("cs.symbol") == col("cv.symbol")) &
        (col("cs.date_parsed") == col("cv.date_parsed")) &
        (col("cs.high") == col("cv.high")) &
        (col("cs.low") == col("cv.low")) &
        (col("cs.volume") == col("cv.volume")) &
        (col("cs.last") == col("cv.close")),
        "inner") \
    .select(
        lit(None).cast(IntegerType()).alias("value_key"),
        lit(None).cast(StringType()).alias("socioeconomical_indicator_key"),
        lit(None).cast(StringType()).alias("realestate_indicator_key"),
        col("cv.cryptostock_value_key").cast(StringType()),
        lit(None).cast(IntegerType()).alias("country_key"),
        col("t.date_key"),
        col("a.asset_key")
    ).distinct()

print(f"✓ Crypto/Stock facts: {crypto_facts.count()}")

# Union all facts
fact_value = wb_facts.union(zillow_facts).union(crypto_facts)

# Add year column before partitionBy
fact_value = fact_value.withColumn("year", year(col("date_key")))

print(f"✓ fact_value total: {fact_value.count()} records")
fact_value.write.format("delta").mode("overwrite").partitionBy("year") \
    .save("/datalake/gold/fact_value")

# Unpersist cached dataframes
dim_time.unpersist()
dim_country.unpersist()
dim_value.unpersist()

print("\n" + "="*80)
print("✅ UNIFIED GOLD LAYER COMPLETED")
print("="*80)

spark.stop()
