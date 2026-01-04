# load_gold_to_duckdb_star_schema.py
import duckdb
from pathlib import Path

print("=" * 80)
print("LOADING STAR SCHEMA GOLD LAYER INTO DUCKDB")
print("=" * 80)

GOLD_PATH = "./datalake/gold"
DUCKDB_PATH = "./datalake/analytics/warehouse_star.duckdb"

Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DUCKDB_PATH)
print(f"\n✓ Connected to DuckDB at: {DUCKDB_PATH}")

print("\n[STEP 1] Installing Delta extension...")
con.execute("INSTALL delta;")
con.execute("LOAD delta;")
print("✓ Delta extension loaded")

print("\n[STEP 2] Loading dimension tables into DuckDB...")

def recreate(table_name: str, ddl: str):
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"  Loading {table_name}...", end=" ")
        con.execute(ddl)
        cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✓ {cnt:,} rows")
    except Exception as e:
        print(f"✗ Error: {e}")

# dim_cryptostock_value
recreate(
    "dim_cryptostock_value",
    f"""
    CREATE TABLE dim_cryptostock_value AS
    SELECT
        cryptostock_value_key::INTEGER AS cryptostock_value_key,
        high       ::DOUBLE  AS high,
        low        ::DOUBLE  AS low,
        volume     ::DOUBLE  AS volume,
        close      ::DOUBLE  AS close
    FROM delta_scan('{GOLD_PATH}/dim_cryptostock_value')
    """
)

# dim_asset
recreate(
    "dim_asset",
    f"""
    CREATE TABLE dim_asset AS
    SELECT
        asset_key::VARCHAR AS asset_key,
        type     ::VARCHAR AS type
    FROM delta_scan('{GOLD_PATH}/dim_asset')
    """
)

# dim_socioeconomical_indicator
recreate(
    "dim_socioeconomical_indicator",
    f"""
    CREATE TABLE dim_socioeconomical_indicator AS
    SELECT
        socioeconomical_indicator_key::VARCHAR AS socioeconomical_indicator_key,
        name        ::VARCHAR AS name,
        description ::VARCHAR AS description
    FROM delta_scan('{GOLD_PATH}/dim_socioeconomical_indicator')
    """
)

# dim_country
recreate(
    "dim_country",
    f"""
    CREATE TABLE dim_country AS
    SELECT
        country_key  AS country_key,
        country_name AS country_name,
        country_code AS country_code
    FROM delta_scan('{GOLD_PATH}/dim_country')
    """
)

# dim_region
recreate(
    "dim_region",
    f"""
    CREATE TABLE dim_region AS
    SELECT
        region_key AS region_key,
        country_key AS country_key,
        region_type AS region_type,
        zip_key AS zip_key,
        neighborhood_name AS neighborhood_name,
        city_name AS city_name,
        county_name AS county_name,
        metro_name AS metro_name,
        state_name AS state_name,
        state_code AS state_code
    FROM delta_scan('{GOLD_PATH}/dim_region')
    """
)

# dim_realestate_indicator 
recreate(
    "dim_realestate_indicator",
    f"""
    CREATE TABLE dim_realestate_indicator AS
    SELECT
        realestate_indicator_key AS realestate_indicator_key,
        indicator_name,
        indicator_description
    FROM delta_scan('{GOLD_PATH}/dim_realestate_indicator')
    """
)

# dim_value
recreate(
    "dim_value",
    f"""
    CREATE TABLE dim_value AS
    SELECT
        value_key,
        value,
        unit
    FROM delta_scan('{GOLD_PATH}/dim_value')
    """
)

# dim_time
recreate(
    "dim_time",
    f"""
    CREATE TABLE dim_time AS
    SELECT
        date_key,
        day_name,
        week_number,
        month_number,
        month_name,
        year
    FROM delta_scan('{GOLD_PATH}/dim_time')
    """
)

print("\n[STEP 3] Loading fact_value...")

# fact_value 
try:
    con.execute("DROP TABLE IF EXISTS fact_value")
    con.execute(
        f"""
        CREATE TABLE fact_value AS
        SELECT
            value_key,
            socioeconomical_indicator_key,
            realestate_indicator_key AS realestate_indicator_key,
            region_key,
            cryptostock_value_key,
            country_key,
            date_key,
            asset_key
        FROM delta_scan('{GOLD_PATH}/fact_value')
        """
    )
    cnt = con.execute("SELECT COUNT(*) FROM fact_value").fetchone()[0]
    print(f"  ✓ fact_value loaded with {cnt:,} rows")
except Exception as e:
    print(f"  ✗ Error loading fact_value: {e}")

print("\n[STEP 4] Listing tables and row counts:")

tables = con.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
""").fetchall()

for (t,) in tables:
    cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  - {t}: {cnt:,} rows")

con.close()
print("\n✓ DuckDB star-schema database created at:", DUCKDB_PATH)
