from typing import Dict, Any, List
from datetime import datetime
import psycopg2
from pgvector.psycopg2 import register_vector
from openai import OpenAI
from dotenv import load_dotenv
import os
from threading import Lock
import time

EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 100
MAX_WORKERS = 5
DB_BATCH_SIZE = 50

DB_SETTINGS = {
    "dbname": "rag_db",
    "user": "rag_user",
    "password": "rag_password",
    "host": "localhost",
    "port": 5432,
}

load_dotenv()
client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

progress_lock = Lock()
total_inserted = 0


def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts
    )

    embeddings = [item.embedding for item in response.data]
    return embeddings


def chunked_list(lst: List, chunk_size: int):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def get_connection():
    conn = psycopg2.connect(**DB_SETTINGS)
    register_vector(conn)
    return conn


def batch_insert_chunks(documents: List[Dict[str, Any]]) -> int:
    if not documents:
        return 0

    conn = get_connection()
    inserted_count = 0

    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO rag.doc_chunks (
                    text, embedding, year_key, region_key,
                    socio_indicator_key, realstate_indicator_key, asset_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

            data_tuples = [
                (
                    doc['text'], doc['embedding'], doc.get('year_key'),
                    doc.get('region_key'), doc.get('socio_indicator_key'),
                    doc.get('realstate_indicator_key'), doc.get('asset_key')
                )
                for doc in documents
            ]

            cur.executemany(insert_query, data_tuples)
            inserted_count = len(data_tuples)

        conn.commit()

        global total_inserted
        with progress_lock:
            total_inserted += inserted_count
            if total_inserted % 500 == 0:
                print(f"✓ Progress: {total_inserted} documents inserted")

        return inserted_count

    except Exception as e:
        conn.rollback()
        print(f"✗ Error inserting batch: {e}")
        return 0
    finally:
        conn.close()


class FinancialDocumentBuilder:
    @staticmethod
    def build_cryptostock_document(record: Dict[str, Any]) -> Dict[str, Any]:
        code = record['code']
        date = record['date']
        high = record['high']
        low = record['low']
        last = record['last']
        volume = record['volume']

        asset_type = "cryptocurrency" if "/" in code else "stock"
        asset_name = code.replace("/", " to ")
        price_range = high - low
        price_volatility = (price_range / last) * 100 if last > 0 else 0

        date_obj = datetime.strptime(str(date)[:10], '%Y-%m-%d')
        year = date_obj.year
        month = date_obj.strftime('%B')
        day = date_obj.day

        text = (
            f"On {month} {day}, {year}, {asset_name} ({asset_type}) traded with the following data: "
            f"The opening price was around {last:.2f} USD. "
            f"Throughout the day, the price reached a high of {high:.2f} USD and a low of {low:.2f} USD, "
            f"resulting in a daily price range of {price_range:.2f} USD ({price_volatility:.2f}% volatility). "
            f"The closing price was {last:.2f} USD. "
            f"Trading volume for the day was {volume:,.0f} units. "
            f"Market: {code}. Date: {date}."
        )

        return {
            "text": text,
            "year_key": year,
            "region_key": None,
            "socio_indicator_key": None,
            "realstate_indicator_key": None,
            "asset_key": code,
        }

    @staticmethod
    def build_socioeconomic_document(record: Dict[str, Any]) -> Dict[str, Any]:
        series_id = record['series_id']
        indicator_name = record['indicator_name']
        indicator_description = record['indicator_description']
        country_code = record['country_code']
        country_name = record['country_name']
        year = record['year']
        value = record['value']
        unit = record['unit']

        if value >= 1_000_000_000_000:
            formatted_value = f"{value / 1_000_000_000_000:.2f} trillion"
        elif value >= 1_000_000_000:
            formatted_value = f"{value / 1_000_000_000:.2f} billion"
        elif value >= 1_000_000:
            formatted_value = f"{value / 1_000_000:.2f} million"
        else:
            formatted_value = f"{value:,.2f}"

        unit_description = {
            'USD_CURRENT': 'current US dollars',
            'USD_CONSTANT': 'constant US dollars',
            'PERCENTAGE': 'percent',
            'INDEX': 'index value',
            'COUNT': 'count',
            'LCU_CURRENT': 'local currency units',
            'RATIO': 'ratio',
            'UNKNOWN': 'units'
        }.get(unit, 'units')

        text = (
            f"In {year}, {country_name} ({country_code}) reported the following economic indicator: "
            f"{indicator_name}. "
            f"{indicator_description}. "
            f"The recorded value was {formatted_value} {unit_description}. "
            f"This data point represents the country's economic performance for that year. "
            f"Indicator code: {series_id}. Country: {country_name}. Year: {year}."
        )

        return {
            "text": text,
            "year_key": year,
            "region_key": None,
            "socio_indicator_key": series_id,
            "realstate_indicator_key": None,
            "asset_key": None,
        }


def load_cryptostock_data_to_rag_optimized():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING CRYPTO/STOCK DATA (OPTIMIZED)")
    print("=" * 80)

    query = """
        SELECT 
            da.asset_key as code,
            CAST(dt.date_key AS VARCHAR) as date,
            dcsv.high,
            dcsv.low,
            dcsv.close as last,
            dcsv.volume,
            dt.year
        FROM fact_value fv
        JOIN dim_asset da ON fv.asset_key = da.asset_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
        WHERE fv.cryptostock_value_key IS NOT NULL
        ORDER BY dt.date_key DESC;
    """

    print("Fetching data from DuckDB...")
    start_time = time.time()

    rows = duckdb_conn.execute(query).fetchall()
    columns = ['code', 'date', 'high', 'low', 'last', 'volume', 'year']

    print(f"✓ Fetched {len(rows)} records in {time.time() - start_time:.2f}s")

    print("Building documents...")
    builder = FinancialDocumentBuilder()
    documents = []

    for row in rows:
        record = dict(zip(columns, row))
        doc = builder.build_cryptostock_document(record)
        documents.append(doc)

    print(f"✓ Built {len(documents)} documents")

    print(f"\nGenerating embeddings in batches of {BATCH_SIZE}...")
    start_time = time.time()

    for i, doc_batch in enumerate(chunked_list(documents, BATCH_SIZE)):
        texts = [doc['text'] for doc in doc_batch]

        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rate = ((i + 1) * BATCH_SIZE) / elapsed
            print(f"  Batch {i+1}: {rate:.1f} docs/sec")

    total_time = time.time() - start_time
    print(f"\n✅ Loaded {len(documents)} documents in {total_time:.2f}s ({len(documents)/total_time:.1f} docs/sec)")

    duckdb_conn.close()


def load_socioeconomic_data_to_rag_optimized():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING SOCIOECONOMIC DATA (OPTIMIZED)")
    print("=" * 80)

    query = """
        SELECT 
            dsi.socioeconomical_indicator_key as series_id,
            dsi.name as indicator_name,
            dsi.description as indicator_description,
            dc.country_code,
            dc.country_name,
            dt.year,
            dv.value,
            dv.unit
        FROM fact_value fv
        JOIN dim_socioeconomical_indicator dsi 
            ON fv.socioeconomical_indicator_key = dsi.socioeconomical_indicator_key
        JOIN dim_country dc ON fv.country_key = dc.country_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_value dv ON fv.value_key = dv.value_key
        WHERE fv.socioeconomical_indicator_key IS NOT NULL
        ORDER BY dc.country_code, dsi.socioeconomical_indicator_key, dt.year;
    """

    print("Fetching data from DuckDB...")
    start_time = time.time()

    rows = duckdb_conn.execute(query).fetchall()
    columns = ['series_id', 'indicator_name', 'indicator_description', 'country_code', 'country_name', 'year', 'value', 'unit']

    print(f"✓ Fetched {len(rows)} records in {time.time() - start_time:.2f}s")

    print("Building documents...")
    builder = FinancialDocumentBuilder()
    documents = []

    for row in rows:
        record = dict(zip(columns, row))
        doc = builder.build_socioeconomic_document(record)
        documents.append(doc)

    print(f"✓ Built {len(documents)} documents")

    print(f"\nGenerating embeddings in batches of {BATCH_SIZE}...")
    start_time = time.time()

    for i, doc_batch in enumerate(chunked_list(documents, BATCH_SIZE)):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rate = ((i + 1) * BATCH_SIZE) / elapsed
            print(f"  Batch {i+1}: {rate:.1f} docs/sec")

    total_time = time.time() - start_time
    print(f"\n✅ Loaded {len(documents)} documents in {total_time:.2f}s ({len(documents)/total_time:.1f} docs/sec)")

    duckdb_conn.close()


def load_socioeconomic_multi_year_trends():
    import duckdb
    from itertools import groupby

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING SOCIOECONOMIC MULTI-YEAR TRENDS")
    print("=" * 80)

    query = """
        SELECT 
            dsi.socioeconomical_indicator_key as series_id,
            dsi.name as indicator_name,
            dsi.description as indicator_description,
            dc.country_code,
            dc.country_name,
            dt.year,
            dv.value,
            dv.unit
        FROM fact_value fv
        JOIN dim_socioeconomical_indicator dsi 
            ON fv.socioeconomical_indicator_key = dsi.socioeconomical_indicator_key
        JOIN dim_country dc ON fv.country_key = dc.country_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_value dv ON fv.value_key = dv.value_key
        WHERE fv.socioeconomical_indicator_key IS NOT NULL
        ORDER BY dc.country_code, dsi.socioeconomical_indicator_key, dt.year;
    """

    print("Fetching socioeconomic data...")
    rows = duckdb_conn.execute(query).fetchall()
    columns = ['series_id', 'indicator_name', 'indicator_description', 'country_code', 'country_name', 'year', 'value', 'unit']

    records = [dict(zip(columns, row)) for row in rows]
    print(f"✓ Fetched {len(records)} records")

    grouped = groupby(
        records,
        key=lambda x: (x['country_code'], x['country_name'], x['series_id'], x['indicator_name'], x['unit'])
    )

    documents = []
    for (country_code, country_name, series_id, indicator_name, unit), group_records in grouped:
        years_data = list(group_records)

        if len(years_data) < 3:
            continue

        start_year = years_data[0]['year']
        end_year = years_data[-1]['year']
        start_value = years_data[0]['value']
        end_value = years_data[-1]['value']

        value_change = end_value - start_value
        value_change_pct = (value_change / start_value * 100) if start_value != 0 else 0

        def format_value(val):
            if val >= 1_000_000_000_000:
                return f"{val / 1_000_000_000_000:.2f} trillion"
            elif val >= 1_000_000_000:
                return f"{val / 1_000_000_000:.2f} billion"
            elif val >= 1_000_000:
                return f"{val / 1_000_000:.2f} million"
            else:
                return f"{val:,.2f}"

        start_fmt = format_value(start_value)
        end_fmt = format_value(end_value)

        unit_description = {
            'USD_CURRENT': 'current US dollars',
            'USD_CONSTANT': 'constant US dollars',
            'PERCENTAGE': 'percent',
            'INDEX': 'index value',
            'COUNT': 'count',
            'LCU_CURRENT': 'local currency units',
            'RATIO': 'ratio',
            'UNKNOWN': 'units'
        }.get(unit, 'units')

        yearly_summary = ", ".join([
            f"{y['year']}: {format_value(y['value'])}"
            for y in years_data
        ])

        text = (
            f"Multi-year trend analysis for {country_name} ({country_code}) - {indicator_name} from {start_year} to {end_year}: "
            f"This indicator is described as: {years_data[0]['indicator_description']}. "
            f"Over {len(years_data)} years, this indicator {'increased' if value_change > 0 else 'decreased'} "
            f"from {start_fmt} in {start_year} to {end_fmt} in {end_year}, "
            f"representing a {'growth' if value_change > 0 else 'decline'} of {abs(value_change_pct):.2f}%. "
            f"Year-by-year values: {yearly_summary}. "
            f"Unit: {unit_description}. "
            f"Indicator code: {series_id}. "
            f"This multi-year document provides comprehensive insight into {country_name}'s {indicator_name} trajectory from {start_year} through {end_year}."
        )

        documents.append({
            "text": text,
            "year_key": end_year,
            "region_key": None,
            "socio_indicator_key": series_id,
            "realstate_indicator_key": None,
            "asset_key": None,
        })

    print(f"✓ Built {len(documents)} multi-year trend documents")

    print("Processing multi-year trends with embeddings...")
    start_time = time.time()

    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} socioeconomic multi-year trends in {total_time:.2f}s")

    duckdb_conn.close()


def load_socioeconomic_yearly_comparison_documents():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING SOCIOECONOMIC YEAR-OVER-YEAR COMPARISONS")
    print("=" * 80)

    query = """
        WITH yearly_stats AS (
            SELECT 
                dsi.socioeconomical_indicator_key as series_id,
                dsi.name as indicator_name,
                dsi.description as indicator_description,
                dc.country_code,
                dc.country_name,
                dt.year,
                dv.value,
                dv.unit
            FROM fact_value fv
            JOIN dim_socioeconomical_indicator dsi 
                ON fv.socioeconomical_indicator_key = dsi.socioeconomical_indicator_key
            JOIN dim_country dc ON fv.country_key = dc.country_key
            JOIN dim_time dt ON fv.date_key = dt.date_key
            JOIN dim_value dv ON fv.value_key = dv.value_key
            WHERE fv.socioeconomical_indicator_key IS NOT NULL
        )
        SELECT 
            curr.series_id,
            curr.indicator_name,
            curr.indicator_description,
            curr.country_code,
            curr.country_name,
            curr.year as current_year,
            curr.value as curr_value,
            curr.unit,
            prev.year as prev_year,
            prev.value as prev_value
        FROM yearly_stats curr
        LEFT JOIN yearly_stats prev 
            ON curr.country_code = prev.country_code
            AND curr.series_id = prev.series_id
            AND curr.year = prev.year + 1
        WHERE prev.year IS NOT NULL
        ORDER BY curr.country_code, curr.series_id, curr.year;
    """

    print("Fetching year-over-year comparison data...")
    rows = duckdb_conn.execute(query).fetchall()
    print(f"✓ Fetched {len(rows)} year-over-year comparisons")

    documents = []
    for row in rows:
        (series_id, indicator_name, indicator_description, country_code, country_name,
         curr_year, curr_value, unit, prev_year, prev_value) = row

        value_change = curr_value - prev_value
        value_change_pct = (value_change / prev_value * 100) if prev_value != 0 else 0

        def format_value(val):
            if val >= 1_000_000_000_000:
                return f"{val / 1_000_000_000_000:.2f} trillion"
            elif val >= 1_000_000_000:
                return f"{val / 1_000_000_000:.2f} billion"
            elif val >= 1_000_000:
                return f"{val / 1_000_000:.2f} million"
            else:
                return f"{val:,.2f}"

        unit_description = {
            'USD_CURRENT': 'current US dollars',
            'USD_CONSTANT': 'constant US dollars',
            'PERCENTAGE': 'percent',
            'INDEX': 'index value',
            'COUNT': 'count',
            'LCU_CURRENT': 'local currency units',
            'RATIO': 'ratio',
            'UNKNOWN': 'units'
        }.get(unit, 'units')

        text = (
            f"Year-over-year comparison for {country_name} ({country_code}) - {indicator_name} between {prev_year} and {curr_year}: "
            f"This indicator is described as: {indicator_description}. "
            f"The value {'increased' if value_change > 0 else 'decreased'} from {format_value(prev_value)} in {prev_year} "
            f"to {format_value(curr_value)} in {curr_year}, a change of {abs(value_change_pct):.2f}%. "
            f"Absolute change: {'+' if value_change > 0 else ''}{format_value(abs(value_change))}. "
            f"Unit: {unit_description}. "
            f"Indicator code: {series_id}. "
            f"This comparison reveals the {'growth' if value_change > 0 else 'decline'} trajectory of {indicator_name} between these consecutive years."
        )

        documents.append({
            "text": text,
            "year_key": curr_year,
            "region_key": None,
            "socio_indicator_key": series_id,
            "realstate_indicator_key": None,
            "asset_key": None,
        })

    print(f"✓ Built {len(documents)} year-over-year comparisons")

    print("Processing year-over-year comparisons with embeddings...")
    start_time = time.time()

    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} socioeconomic year-over-year comparisons in {total_time:.2f}s")

    duckdb_conn.close()


def load_monthly_aggregates_optimized():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING MONTHLY AGGREGATES (OPTIMIZED)")
    print("=" * 80)

    monthly_query = """
        SELECT 
            da.asset_key as code,
            dt.year,
            dt.month_number,
            AVG(dcsv.close) as avg_price,
            MIN(dcsv.low) as min_price,
            MAX(dcsv.high) as max_price,
            AVG(dcsv.volume) as avg_volume,
            COUNT(*) as trading_days
        FROM fact_value fv
        JOIN dim_asset da ON fv.asset_key = da.asset_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
        WHERE fv.cryptostock_value_key IS NOT NULL
        GROUP BY da.asset_key, dt.year, dt.month_number
        ORDER BY dt.year DESC, dt.month_number DESC;
    """

    rows = duckdb_conn.execute(monthly_query).fetchall()
    print(f"✓ Fetched {len(rows)} monthly aggregates")

    documents = []
    for row in rows:
        code, year, month, avg_price, min_price, max_price, avg_volume, trading_days = row

        text = (
            f"In {year}, month {month}, {code} had an average closing price of {avg_price:.2f} USD. "
            f"The month's price ranged from a low of {min_price:.2f} USD to a high of {max_price:.2f} USD. "
            f"Average daily trading volume was {avg_volume:,.0f} units across {trading_days} trading days. "
            f"This monthly summary provides insight into the asset's performance trends."
        )

        documents.append({
            "text": text,
            "year_key": year,
            "region_key": None,
            "socio_indicator_key": None,
            "realstate_indicator_key": None,
            "asset_key": code,
        })

    start_time = time.time()
    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} monthly aggregates in {total_time:.2f}s")

    duckdb_conn.close()


def init_db():
    conn = psycopg2.connect(**DB_SETTINGS)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()
            register_vector(conn)

            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS rag;
                
                CREATE TABLE IF NOT EXISTS rag.doc_chunks (
                    id                  bigserial PRIMARY KEY,
                    text                text NOT NULL,
                    embedding           vector(1536) NOT NULL,
                    year_key            integer,
                    region_key          integer,
                    socio_indicator_key varchar,
                    realstate_indicator_key varchar,
                    asset_key           varchar,
                    created_at          timestamptz DEFAULT now()
                );
                
                CREATE INDEX IF NOT EXISTS doc_chunks_embedding_idx 
                ON rag.doc_chunks USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
                
                CREATE INDEX IF NOT EXISTS doc_chunks_year_idx ON rag.doc_chunks(year_key);
                CREATE INDEX IF NOT EXISTS doc_chunks_asset_idx ON rag.doc_chunks(asset_key);
                CREATE INDEX IF NOT EXISTS doc_chunks_socio_idx ON rag.doc_chunks(socio_indicator_key);
            """)
            conn.commit()
        print("✅ Database initialized with indexes")
    finally:
        conn.close()


def load_yearly_aggregates_optimized():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING YEARLY AGGREGATES (OPTIMIZED)")
    print("=" * 80)

    yearly_query = """
        SELECT 
            da.asset_key as code,
            dt.year,
            MIN(dcsv.close) as year_open,
            MAX(dcsv.close) as year_close,
            MIN(dcsv.low) as year_low,
            MAX(dcsv.high) as year_high,
            AVG(dcsv.close) as avg_price,
            COALESCE(STDDEV(dcsv.close), 0.0) as price_stddev,
            SUM(dcsv.volume) as total_volume,
            AVG(dcsv.volume) as avg_volume,
            COUNT(*) as trading_days,
            MIN(dt.date_key) as first_date,
            MAX(dt.date_key) as last_date
        FROM fact_value fv
        JOIN dim_asset da ON fv.asset_key = da.asset_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
        WHERE fv.cryptostock_value_key IS NOT NULL
        GROUP BY da.asset_key, dt.year
        HAVING COUNT(*) >= 2
        ORDER BY dt.year DESC, da.asset_key;
    """

    print("Fetching yearly aggregates from DuckDB...")
    rows = duckdb_conn.execute(yearly_query).fetchall()
    print(f"✓ Fetched {len(rows)} yearly aggregates")

    documents = []
    for row in rows:
        (code, year, year_open, year_close, year_low, year_high,
         avg_price, price_stddev, total_volume, avg_volume, trading_days,
         first_date, last_date) = row

        year_open = year_open or 0
        year_close = year_close or 0
        year_low = year_low or 0
        year_high = year_high or 0
        avg_price = avg_price or 0
        price_stddev = price_stddev or 0
        total_volume = total_volume or 0
        avg_volume = avg_volume or 0

        price_change = year_close - year_open if year_open else 0
        price_change_pct = (price_change / year_open * 100) if year_open and year_open > 0 else 0
        price_range = year_high - year_low

        asset_type = "cryptocurrency" if "/" in code else "stock"
        asset_name = code.replace("/", " to ")

        volatility_ratio = (price_stddev / avg_price) if avg_price > 0 else 0
        volatility_desc = 'high' if volatility_ratio > 0.3 else 'moderate' if volatility_ratio > 0.1 else 'low'

        text = (
            f"Annual performance summary for {asset_name} ({code}) in {year}: "
            f"This {asset_type} had {trading_days} trading days from {first_date} to {last_date}. "
            f"The year started with a price around {year_open:.2f} USD and ended at {year_close:.2f} USD, "
            f"representing a {'gain' if price_change > 0 else 'loss'} of {abs(price_change_pct):.2f}% "
            f"({'+' if price_change > 0 else '-'}{abs(price_change):.2f} USD). "
            f"Throughout {year}, the price reached a high of {year_high:.2f} USD and a low of {year_low:.2f} USD, "
            f"with an annual range of {price_range:.2f} USD. "
            f"The average price for the year was {avg_price:.2f} USD with a standard deviation of {price_stddev:.2f} USD, "
            f"indicating {volatility_desc} volatility. "
            f"Total trading volume for {year} was {total_volume:,.0f} units, averaging {avg_volume:,.0f} units per day. "
            f"This yearly summary provides comprehensive insight into {code}'s annual performance and market behavior in {year}."
        )

        documents.append({
            "text": text,
            "year_key": year,
            "region_key": None,
            "socio_indicator_key": None,
            "realstate_indicator_key": None,
            "asset_key": code,
        })

    print("Processing yearly aggregates with embeddings...")
    start_time = time.time()

    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} yearly aggregates in {total_time:.2f}s")

    duckdb_conn.close()


def load_yearly_comparison_documents():
    import duckdb

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING YEAR-OVER-YEAR COMPARISON DOCUMENTS")
    print("=" * 80)

    query = """
        WITH yearly_stats AS (
            SELECT 
                da.asset_key as code,
                dt.year,
                AVG(dcsv.close) as avg_price,
                MIN(dcsv.low) as min_price,
                MAX(dcsv.high) as max_price,
                AVG(dcsv.volume) as avg_volume
            FROM fact_value fv
            JOIN dim_asset da ON fv.asset_key = da.asset_key
            JOIN dim_time dt ON fv.date_key = dt.date_key
            JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
            WHERE fv.cryptostock_value_key IS NOT NULL
            GROUP BY da.asset_key, dt.year
        )
        SELECT 
            curr.code,
            curr.year as current_year,
            curr.avg_price as curr_avg_price,
            curr.min_price as curr_min_price,
            curr.max_price as curr_max_price,
            curr.avg_volume as curr_avg_volume,
            prev.year as prev_year,
            prev.avg_price as prev_avg_price,
            prev.min_price as prev_min_price,
            prev.max_price as prev_max_price,
            prev.avg_volume as prev_avg_volume
        FROM yearly_stats curr
        LEFT JOIN yearly_stats prev 
            ON curr.code = prev.code 
            AND curr.year = prev.year + 1
        WHERE prev.year IS NOT NULL
        ORDER BY curr.code, curr.year;
    """

    print("Fetching year-over-year comparison data...")
    rows = duckdb_conn.execute(query).fetchall()
    print(f"✓ Fetched {len(rows)} year-over-year comparisons")

    documents = []
    for row in rows:
        (code, curr_year, curr_avg, curr_min, curr_max, curr_vol,
         prev_year, prev_avg, prev_min, prev_max, prev_vol) = row

        price_change_pct = ((curr_avg - prev_avg) / prev_avg * 100) if prev_avg > 0 else 0
        vol_change_pct = ((curr_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
        high_change_pct = ((curr_max - prev_max) / prev_max * 100) if prev_max > 0 else 0
        low_change_pct = ((curr_min - prev_min) / prev_min * 100) if prev_min > 0 else 0

        asset_name = code.replace("/", " to ")

        text = (
            f"Year-over-year comparison for {asset_name} ({code}) between {prev_year} and {curr_year}: "
            f"The average price {'increased' if price_change_pct > 0 else 'decreased'} from {prev_avg:.2f} USD in {prev_year} "
            f"to {curr_avg:.2f} USD in {curr_year}, a change of {abs(price_change_pct):.2f}%. "
            f"The annual high moved from {prev_max:.2f} USD to {curr_max:.2f} USD ({'+' if high_change_pct > 0 else ''}{high_change_pct:.2f}%), "
            f"while the annual low shifted from {prev_min:.2f} USD to {curr_min:.2f} USD ({'+' if low_change_pct > 0 else ''}{low_change_pct:.2f}%). "
            f"Trading volume {'increased' if vol_change_pct > 0 else 'decreased'} by {abs(vol_change_pct):.2f}%, "
            f"from an average of {prev_vol:,.0f} units per day in {prev_year} to {curr_vol:,.0f} units in {curr_year}. "
            f"This comparison reveals the {'growth' if price_change_pct > 0 else 'decline'} trajectory of {code} between these consecutive years."
        )

        documents.append({
            "text": text,
            "year_key": curr_year,
            "region_key": None,
            "socio_indicator_key": None,
            "realstate_indicator_key": None,
            "asset_key": code,
        })

    print("Processing year-over-year comparisons with embeddings...")
    start_time = time.time()

    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} year-over-year comparisons in {total_time:.2f}s")

    duckdb_conn.close()


def load_multi_year_trend_documents():
    import duckdb
    from collections import defaultdict

    duckdb_conn = duckdb.connect('./datalake/analytics/warehouse.duckdb')

    print("\n" + "=" * 80)
    print("LOADING MULTI-YEAR TREND DOCUMENTS")
    print("=" * 80)

    query = """
        SELECT 
            da.asset_key as code,
            MIN(dt.year) as start_year,
            MAX(dt.year) as end_year,
            COUNT(DISTINCT dt.year) as num_years,
            MIN(dcsv.close) as all_time_low,
            MAX(dcsv.high) as all_time_high,
            AVG(dcsv.close) as overall_avg_price,
            SUM(dcsv.volume) as total_volume
        FROM fact_value fv
        JOIN dim_asset da ON fv.asset_key = da.asset_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
        WHERE fv.cryptostock_value_key IS NOT NULL
        GROUP BY da.asset_key
        HAVING COUNT(DISTINCT dt.year) >= 3
        ORDER BY da.asset_key;
    """

    print("Fetching multi-year trend data...")
    rows = duckdb_conn.execute(query).fetchall()
    print(f"✓ Fetched {len(rows)} multi-year trends")

    yearly_query = """
        SELECT 
            da.asset_key as code,
            dt.year,
            AVG(dcsv.close) as avg_price
        FROM fact_value fv
        JOIN dim_asset da ON fv.asset_key = da.asset_key
        JOIN dim_time dt ON fv.date_key = dt.date_key
        JOIN dim_cryptostock_value dcsv ON CAST(fv.cryptostock_value_key AS INTEGER) = dcsv.cryptostock_value_key
        WHERE fv.cryptostock_value_key IS NOT NULL
        GROUP BY da.asset_key, dt.year
        ORDER BY da.asset_key, dt.year;
    """

    yearly_data = duckdb_conn.execute(yearly_query).fetchall()

    asset_yearly = defaultdict(list)
    for code, year, avg_price in yearly_data:
        asset_yearly[code].append((year, avg_price))

    documents = []
    for row in rows:
        code, start_year, end_year, num_years, atl, ath, overall_avg, total_vol = row

        asset_name = code.replace("/", " to ")
        asset_type = "cryptocurrency" if "/" in code else "stock"

        yearly_prices = asset_yearly[code]
        first_year_price = yearly_prices[0][1] if yearly_prices else 0
        last_year_price = yearly_prices[-1][1] if yearly_prices else 0

        overall_change_pct = ((last_year_price - first_year_price) / first_year_price * 100) if first_year_price > 0 else 0

        yearly_summary = ", ".join([f"{year}: ${price:.2f}" for year, price in yearly_prices])

        text = (
            f"Multi-year trend analysis for {asset_name} ({code}) from {start_year} to {end_year}: "
            f"Over {num_years} years, this {asset_type} showed {'growth' if overall_change_pct > 0 else 'decline'} "
            f"of {abs(overall_change_pct):.2f}%, moving from an average of {first_year_price:.2f} USD in {start_year} "
            f"to {last_year_price:.2f} USD in {end_year}. "
            f"During this period, {code} reached an all-time high of {ath:.2f} USD and an all-time low of {atl:.2f} USD. "
            f"The overall average price across all years was {overall_avg:.2f} USD. "
            f"Total cumulative trading volume over this period was {total_vol:,.0f} units. "
            f"Year-by-year average prices: {yearly_summary}. "
            f"This long-term trend document provides comprehensive insight into {code}'s performance trajectory from {start_year} through {end_year}."
        )

        documents.append({
            "text": text,
            "year_key": end_year,
            "region_key": None,
            "socio_indicator_key": None,
            "realstate_indicator_key": None,
            "asset_key": code,
        })

    print("Processing multi-year trends with embeddings...")
    start_time = time.time()

    for doc_batch in chunked_list(documents, BATCH_SIZE):
        texts = [doc['text'] for doc in doc_batch]
        embeddings = get_embeddings_batch(texts)

        for doc, embedding in zip(doc_batch, embeddings):
            doc['embedding'] = embedding

        batch_insert_chunks(doc_batch)

    total_time = time.time() - start_time
    print(f"✅ Loaded {len(documents)} multi-year trends in {total_time:.2f}s")

    duckdb_conn.close()


if __name__ == "__main__":
    total_inserted = 0

    overall_start = time.time()

    print("=" * 80)
    print("OPTIMIZED RAG DATA LOADER")
    print("=" * 80)

    init_db()

    load_cryptostock_data_to_rag_optimized()
    load_socioeconomic_data_to_rag_optimized()
    load_socioeconomic_multi_year_trends()
    load_socioeconomic_yearly_comparison_documents()
    load_monthly_aggregates_optimized()
    load_yearly_aggregates_optimized()
    load_yearly_comparison_documents()
    load_multi_year_trend_documents()

    overall_time = time.time() - overall_start

    print("\n" + "=" * 80)
    print("✅ ALL DATA LOADED!")
    print(f"   Total documents: {total_inserted}")
    print(f"   Total time: {overall_time:.2f}s")
    print(f"   Average rate: {total_inserted/overall_time:.1f} docs/sec")
    print("=" * 80)

    print("\n📊 Document Granularity Summary:")
    print("   CRYPTO/STOCK:")
    print("     ✓ Daily records")
    print("     ✓ Monthly aggregates")
    print("     ✓ Yearly aggregates")
    print("     ✓ Year-over-year comparisons")
    print("     ✓ Multi-year trends")
    print("   SOCIOECONOMIC:")
    print("     ✓ Individual year records")
    print("     ✓ Year-over-year comparisons")
    print("     ✓ Multi-year trends")
