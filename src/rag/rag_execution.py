import datetime
import psycopg2
from pgvector.psycopg2 import register_vector
from openai import OpenAI
from typing import List, Dict, Any, Tuple

class EnhancedRAGRetrieval:
    
    def __init__(self, db_settings: Dict, client: OpenAI):
        self.db_settings = db_settings
        self.client = client
        self.embedding_model = "text-embedding-3-small"
        self.chat_model = "gpt-4o-mini-2024-07-18"
        
        self.entity_mappings = {
            'bitcoin': 'BTC/USD',
            'btc': 'BTC/USD',
            'ethereum': 'ETH/USD',
            'eth': 'ETH/USD',
            'apple': 'AAPL',
            'aapl': 'AAPL',
            'tesla': 'TSLA',
            'tsla': 'TSLA',
            'microsoft': 'MSFT',
            'msft': 'MSFT',
            'johnson & johnson': 'JNJ',
            'johnson and johnson': 'JNJ',
            'j&j': 'JNJ',
            'jnj': 'JNJ',
            'amazon': 'AMZN',
            'amzn': 'AMZN',
            'google': 'GOOGL',
            'googl': 'GOOGL',
            'alphabet': 'GOOGL',
            'meta': 'META',
            'facebook': 'META',
            'nvidia': 'NVDA',
            'nvda': 'NVDA',
            'usa': 'USA',
            'united states': 'USA',
            'us': 'USA',
            'america': 'USA',
        }
    
    def get_available_tickers(self) -> List[str]:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT asset_key 
                    FROM rag.doc_chunks 
                    WHERE asset_key IS NOT NULL
                    ORDER BY asset_key;
                """)
                rows = cur.fetchall()
                return [row[0] for row in rows]
        finally:
            conn.close()
    
    def fuzzy_match_ticker(self, company_name: str) -> str:
        conn = self.get_connection()
        
        search_query = f"{company_name} stock price"
        embedding = self.get_embedding(search_query)
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT asset_key,
                           1 - (embedding <=> %s::vector) AS similarity
                    FROM rag.doc_chunks
                    WHERE asset_key IS NOT NULL
                    ORDER BY embedding <=> %s::vector
                    LIMIT 1;
                """, (embedding, embedding))
                row = cur.fetchone()
                
                if row and row[1] > 0.7:
                    print(f"    🔍 Fuzzy matched '{company_name}' to '{row[0]}' (similarity: {row[1]:.3f})")
                    return row[0]
        finally:
            conn.close()
        
        return None
    
    def normalize_entities(self, entities: List[str]) -> List[str]:
        normalized = []
        
        for entity in entities:
            entity_lower = entity.lower().strip()
            
            mapped = self.entity_mappings.get(entity_lower)
            
            if mapped:
                normalized.append(mapped)
                print(f"    ✓ Mapped '{entity}' → '{mapped}'")
            else:
                fuzzy_match = self.fuzzy_match_ticker(entity)
                if fuzzy_match:
                    normalized.append(fuzzy_match)
                    self.entity_mappings[entity_lower] = fuzzy_match
                else:
                    print(f"    ⚠️  No mapping found for '{entity}', keeping as-is")
                    normalized.append(entity)
        
        return normalized
    
    def decompose_query(self, query: str) -> Dict[str, Any]:
        system_prompt = """You are a query analyzer for a financial data system. 
        Analyze the user's question and break it into distinct sub-questions.
        Also identify what types of data are needed:
        - socioeconomic: World Bank indicators (GDP, inflation, unemployment, etc.)
        - cryptostock: Cryptocurrency and stock prices (BTC, ETH, AAPL, etc.)
        - realstate: Real estate indicators (home prices, rent, etc.)
        
        IMPORTANT: For stocks, ALWAYS extract the TICKER SYMBOL if mentioned. So if you see a company name, convert it to its stock ticker.
        Examples:
        - "Johnson & Johnson" or "J&J" → extract as "JNJ"
        - "Apple" → extract as "AAPL"
        - "Bitcoin" → extract as "BTC"

        For time references:
        - "pandemic" or "COVID" → 2020-2021
        - "before pandemic" or "pre-pandemic" → 2017-2019
        - "3 years before pandemic" → 2017
        - "last 5 years" → last 5 calendar years from current year
        
        Return a JSON with:
        {
            "sub_queries": ["sub-query 1", "sub-query 2", ...],
            "data_types": ["socioeconomic", "cryptostock"],
            "time_range": {"start": 2014, "end": 2020},
            "entities": {
                "crypto_assets": ["Bitcoin", "BTC"],
                "stock_assets": ["JNJ", "Johnson & Johnson"],
                "countries": ["USA"],
                "indicators": ["GDP"]
            }
        }
        """
        
        response = self.client.chat.completions.create(
            model=self.chat_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Analyze this query: {query}"}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )
        
        import json
        return json.loads(response.choices[0].message.content)
    
    def get_embedding(self, text: str) -> list[float]:
        res = self.client.embeddings.create(
            model=self.embedding_model,
            input=text
        )
        return res.data[0].embedding
    
    def search_by_indicator(
        self,
        query_embedding: list[float],
        indicator_codes: List[str],
        country_codes: List[str] = None,
        start_year: int = None,
        end_year: int = None,
        top_k: int = 5
    ) -> List[Tuple]:
        conn = self.get_connection()
        
        where_clauses = []
        params = []
        
        if indicator_codes:
            indicator_conditions = " OR ".join([f"socio_indicator_key = %s" for _ in indicator_codes])
            where_clauses.append(f"({indicator_conditions})")
            params.extend(indicator_codes)
        
        if country_codes:
            country_conditions = " OR ".join([f"text ILIKE %s" for _ in country_codes])
            where_clauses.append(f"({country_conditions})")
            params.extend([f"%{code}%" for code in country_codes])
        
        if start_year and end_year:
            where_clauses.append(f"year_key BETWEEN %s AND %s")
            params.extend([start_year, end_year])
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        try:
            with conn.cursor() as cur:
                query_sql = f"""
                    SELECT
                        id,
                        text,
                        year_key,
                        region_key,
                        socio_indicator_key,
                        realstate_indicator_key,
                        asset_key,
                        1 - (embedding <=> %s::vector) AS similarity
                    FROM rag.doc_chunks
                    WHERE {where_sql}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s;
                """
                
                all_params = [query_embedding, *params, query_embedding, top_k]
                
                cur.execute(query_sql, all_params)
                rows = cur.fetchall()
        finally:
            conn.close()
        
        return rows

    def search_by_category(
        self, 
        query_embedding: list[float], 
        category_filter: Dict[str, Any],
        top_k: int = 5
    ) -> List[Tuple]:
        conn = self.get_connection()
        
        where_clauses = []
        for key, value in category_filter.items():
            if value is None:
                where_clauses.append(f"{key}")
            else:
                where_clauses.append(f"{key} = '{value}'")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        id,
                        text,
                        year_key,
                        region_key,
                        socio_indicator_key,
                        realstate_indicator_key,
                        asset_key,
                        1 - (embedding <=> %s::vector) AS similarity
                    FROM rag.doc_chunks
                    WHERE {where_sql}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s;
                """, (query_embedding, query_embedding, top_k))
                rows = cur.fetchall()
        finally:
            conn.close()
        
        return rows
    
    def search_with_time_filter(
        self,
        query_embedding: list[float],
        start_year: int,
        end_year: int,
        category_filter: Dict[str, Any] = None,
        top_k: int = 5
    ) -> List[Tuple]:
        conn = self.get_connection()
        
        where_clauses = [f"year_key BETWEEN {start_year} AND {end_year}"]
        
        if category_filter:
            for key, value in category_filter.items():
                if value is None:
                    where_clauses.append(f"{key}")
                else:
                    where_clauses.append(f"{key} = '{value}'")
        
        where_sql = " AND ".join(where_clauses)
        
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        id,
                        text,
                        year_key,
                        region_key,
                        socio_indicator_key,
                        realstate_indicator_key,
                        asset_key,
                        1 - (embedding <=> %s::vector) AS similarity
                    FROM rag.doc_chunks
                    WHERE {where_sql}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s;
                """, (query_embedding, query_embedding, top_k))
                rows = cur.fetchall()
        finally:
            conn.close()
        
        return rows
    
    def search_by_entity(
        self,
        query_embedding: list[float],
        entity_codes: List[str],
        category: str,
        start_year: int = None,
        end_year: int = None,
        top_k: int = 5
    ) -> List[Tuple]:
        conn = self.get_connection()
        
        where_clauses = []
        
        if category == "cryptostock":
            entity_conditions = " OR ".join([f"asset_key = '{code}'" for code in entity_codes])
            where_clauses.append(f"({entity_conditions})")
        
        if start_year and end_year:
            where_clauses.append(f"year_key BETWEEN {start_year} AND {end_year}")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        id,
                        text,
                        year_key,
                        region_key,
                        socio_indicator_key,
                        realstate_indicator_key,
                        asset_key,
                        1 - (embedding <=> %s::vector) AS similarity
                    FROM rag.doc_chunks
                    WHERE {where_sql}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s;
                """, (query_embedding, query_embedding, top_k))
                rows = cur.fetchall()
        finally:
            conn.close()
        
        return rows
    
    def balanced_retrieval(
        self,
        query: str,
        total_k: int = 30
    ) -> Dict[str, List[Tuple]]:
        analysis = self.decompose_query(query)
        
        print(f"\n📊 Query Analysis:")
        print(f"  Sub-queries: {analysis['sub_queries']}")
        print(f"  Data types needed: {analysis['data_types']}")
        print(f"  Time range: {analysis.get('time_range', 'Not specified')}")
        print(f"  Entities: {analysis.get('entities', {})}")
        
        data_types = analysis['data_types']
        num_types = len(data_types) if len(data_types) > 0 else 1
        docs_per_type = max(10, total_k // num_types)
        
        results = {
            "socioeconomic": [],
            "cryptostock": [],
            "realstate": [],
            "sub_queries": analysis['sub_queries'],
            "analysis": analysis
        }
        
        main_embedding = self.get_embedding(query)
        sub_embeddings = [self.get_embedding(sq) for sq in analysis['sub_queries']]
        
        time_range = analysis.get('time_range', {})
        start_year = time_range.get('start')
        end_year = time_range.get('end')
        
        entities = analysis.get('entities', {})
        
        if "socioeconomic" in data_types:
            print(f"\n🔍 Retrieving {docs_per_type} socioeconomic documents...")
            
            indicators = entities.get('indicators', [])
            indicator_codes = self.get_indicator_codes_from_keywords(indicators)
            countries = entities.get('countries', [])
            
            print(f"  📊 Detected indicators: {indicators}")
            print(f"  🔑 Mapped to codes: {indicator_codes}")
            
            all_socio_results = []
            
            if indicator_codes:
                print(f"  🎯 Using indicator-specific search...")
                indicator_results = self.search_by_indicator(
                    main_embedding,
                    indicator_codes,
                    countries,
                    start_year,
                    end_year,
                    docs_per_type // 2
                )
                all_socio_results.extend(indicator_results)
                print(f"    Found {len(indicator_results)} docs via indicator search")
            
            if start_year and end_year:
                main_results = self.search_with_time_filter(
                    main_embedding,
                    start_year,
                    end_year,
                    {"socio_indicator_key IS NOT NULL": None},
                    docs_per_type // 2
                )
            else:
                main_results = self.search_by_category(
                    main_embedding,
                    {"socio_indicator_key IS NOT NULL": None},
                    docs_per_type // 2
                )
            all_socio_results.extend(main_results)
            
            sub_results = []
            for sub_emb in sub_embeddings[:1]:
                if start_year and end_year:
                    sub_res = self.search_with_time_filter(
                        sub_emb,
                        start_year,
                        end_year,
                        {"socio_indicator_key IS NOT NULL": None},
                        docs_per_type // 4
                    )
                else:
                    sub_res = self.search_by_category(
                        sub_emb,
                        {"socio_indicator_key IS NOT NULL": None},
                        docs_per_type // 4
                    )
                sub_results.extend(sub_res)
            all_socio_results.extend(sub_results)
            
            seen_ids = set()
            unique_results = []
            for r in all_socio_results:
                if r[0] not in seen_ids:
                    seen_ids.add(r[0])
                    unique_results.append(r)
            
            results['socioeconomic'] = unique_results[:docs_per_type]
            print(f"  ✓ Found {len(results['socioeconomic'])} documents total")
        
        if "cryptostock" in data_types:
            print(f"\n🔍 Retrieving {docs_per_type} crypto/stock documents...")
            
            crypto_entities = entities.get('crypto_assets', [])
            stock_entities = entities.get('stock_assets', [])
            all_assets = crypto_entities + stock_entities
            
            print(f"  📋 Raw entities: {all_assets}")
            normalized_assets = self.normalize_entities(all_assets)
            
            print(f"  🎯 Normalized assets: {normalized_assets}")
            
            all_crypto_results = []
            
            if normalized_assets:
                for asset_code in normalized_assets:
                    print(f"    Searching for {asset_code}...")
                    entity_results = self.search_by_entity(
                        main_embedding,
                        [asset_code],
                        "cryptostock",
                        start_year,
                        end_year,
                        docs_per_type // len(normalized_assets) if len(normalized_assets) > 0 else docs_per_type
                    )
                    all_crypto_results.extend(entity_results)
                    print(f"      Found {len(entity_results)} docs for {asset_code}")
            
            if len(all_crypto_results) == 0:
                print(f"    ⚠️  No results from entity search, falling back to semantic search...")
                for sub_emb in sub_embeddings:
                    if start_year and end_year:
                        sub_res = self.search_with_time_filter(
                            sub_emb,
                            start_year,
                            end_year,
                            {"asset_key IS NOT NULL": None},
                            docs_per_type // len(sub_embeddings) if len(sub_embeddings) > 0 else docs_per_type
                        )
                    else:
                        sub_res = self.search_by_category(
                            sub_emb,
                            {"asset_key IS NOT NULL": None},
                            docs_per_type // len(sub_embeddings) if len(sub_embeddings) > 0 else docs_per_type
                        )
                    all_crypto_results.extend(sub_res)
                print(f"      Fallback found {len(all_crypto_results)} docs")
            else:
                for sub_emb in sub_embeddings:
                    if start_year and end_year:
                        sub_res = self.search_with_time_filter(
                            sub_emb,
                            start_year,
                            end_year,
                            {"asset_key IS NOT NULL": None},
                            5
                        )
                    else:
                        sub_res = self.search_by_category(
                            sub_emb,
                            {"asset_key IS NOT NULL": None},
                            5
                        )
                    all_crypto_results.extend(sub_res)
            
            seen_ids = set()
            unique_crypto = []
            for r in all_crypto_results:
                if r[0] not in seen_ids:
                    seen_ids.add(r[0])
                    unique_crypto.append(r)
            
            results['cryptostock'] = unique_crypto[:docs_per_type]
            print(f"  ✓ Found {len(results['cryptostock'])} documents total")
        
        if "realstate" in data_types:
            print(f"\n🔍 Retrieving {docs_per_type} real estate documents...")
            
            if start_year and end_year:
                results['realstate'] = self.search_with_time_filter(
                    main_embedding,
                    start_year,
                    end_year,
                    {"realstate_indicator_key IS NOT NULL": None},
                    docs_per_type
                )
            else:
                results['realstate'] = self.search_by_category(
                    main_embedding,
                    {"realstate_indicator_key IS NOT NULL": None},
                    docs_per_type
                )
            
            print(f"  ✓ Found {len(results['realstate'])} documents")
        
        return results
    
    def rerank_results(
        self,
        query: str,
        categorized_results: Dict[str, List[Tuple]],
        top_k: int = 20
    ) -> List[Tuple]:
        all_docs = []
        
        for category, docs in categorized_results.items():
            if category in ['sub_queries', 'analysis']:
                continue
            for doc in docs:
                all_docs.append({
                    'category': category,
                    'doc': doc,
                    'id': doc[0],
                    'text': doc[1],
                    'similarity': doc[7]
                })
        
        all_docs.sort(key=lambda x: x['similarity'], reverse=True)
        
        return [d['doc'] for d in all_docs[:top_k]]
    
    def get_connection(self):
        conn = psycopg2.connect(**self.db_settings)
        register_vector(conn)
        return conn
    
    def get_indicator_codes_from_keywords(self, keywords: List[str]) -> List[str]:
        indicator_mapping = {
            'unemployment': ['SL.UEM.TOTL.ZS'],
            'labor force': ['SL.TLF.TOTL.IN', 'SL.UEM.TOTL.ZS'],
            
            'gdp': ['NY.GDP.MKTP.CD', 'NY.GDP.MKTP.KD', 'NY.GDP.MKTP.KD.ZG', 'NY.GDP.MKTP.PP.CD', 
                    'NY.GDP.MKTP.PP.KD', 'NY.GDP.PCAP.CD', 'NY.GDP.PCAP.KD', 'NY.GDP.PCAP.KD.ZG',
                    'NY.GDP.PCAP.PP.CD', 'NY.GDP.PCAP.PP.KD'],
            
            'inflation': ['FP.CPI.TOTL.ZG', 'NY.GDP.DEFL.KD.ZG', 'NY.GDP.DEFL.KD.ZG.AD', 'FP.CPI.TOTL'],
            
            'exports': ['NE.EXP.GNFS.CD', 'NE.EXP.GNFS.KD', 'NE.EXP.GNFS.KD.ZG', 'NE.EXP.GNFS.ZS', 
                        'TX.VAL.TECH.CD', 'BX.GSR.GNFS.CD'],
            
            'imports': ['NE.IMP.GNFS.CD', 'NE.IMP.GNFS.KD', 'NE.IMP.GNFS.KD.ZG', 'NE.IMP.GNFS.ZS',
                        'TM.VAL.MRCH.CD.WT', 'BM.GSR.GNFS.CD'],
            
            'trade': ['NE.RSB.GNFS.CD', 'NE.RSB.GNFS.ZS', 'TG.VAL.TOTL.GD.ZS', 'TM.TAX.MRCH.WM.AR.ZS'],
            
            'consumption': ['NE.CON.TOTL.ZS', 'NE.CON.TOTL.CD', 'NE.CON.PRVT.CD', 'NE.CON.PRVT.ZS',
                            'NE.CON.PRVT.KD.ZG', 'NE.CON.GOVT.CD', 'NE.CON.GOVT.ZS'],
            
            'investment': ['NE.GDI.TOTL.CD', 'NE.GDI.TOTL.KD', 'NE.GDI.TOTL.KD.ZG', 'NE.GDI.TOTL.ZS',
                        'NE.GDI.FTOT.ZS', 'BX.KLT.DINV.CD.WD', 'BX.KLT.DINV.WD.GD.ZS'],
            
            'debt': ['GC.DOD.TOTL.CN', 'GC.DOD.TOTL.GD.ZS', 'DT.DOD.DECT.CD', 'DT.TDS.DECT.EX.ZS'],
            
            'credit': ['FS.AST.PRVT.GD.ZS', 'FS.AST.DOMS.GD.ZS', 'FB.AST.NPER.ZS', 'IC.CRD.INFO.XQ'],
            
            'interest rate': ['FR.INR.RINR', 'FR.INR.LEND', 'FR.INR.LNDP'],
            
            'savings': ['NY.GNS.ICTR.ZS', 'NY.GNS.ICTR.CD', 'NY.ADJ.NNAT.GN.ZS'],
            
            'revenue': ['GC.REV.XGRT.GD.ZS', 'GC.REV.XGRT.CN', 'GC.TAX.TOTL.CN', 'GC.TAX.TOTL.GD.ZS'],
            
            'reserves': ['FI.RES.XGLD.CD', 'FI.RES.TOTL.CD', 'FI.RES.TOTL.MO', 'BN.RES.INCL.CD'],
            
            'population': ['NY.GDP.PCAP.CD', 'NY.GDP.PCAP.KD', 'NY.GDP.PCAP.PP.CD', 'NY.GDP.PCAP.PP.KD'],
            
            'agriculture': ['NV.AGR.TOTL.ZS'],
            
            'industry': ['NV.IND.TOTL.ZS', 'NV.IND.MANF.ZS'],
            
            'services': ['NV.SRV.TOTL.ZS'],
        }
        
        codes = []
        for keyword in keywords:
            keyword_lower = keyword.lower().strip()
            if keyword_lower in indicator_mapping:
                codes.extend(indicator_mapping[keyword_lower])
        
        return codes

    def answer_question_with_balanced_context(
        self,
        query: str,
        total_k: int = 40,
        final_k: int = 20
    ) -> str:
        categorized_results = self.balanced_retrieval(query, total_k)
        
        final_docs = self.rerank_results(query, categorized_results, final_k)
        
        context_parts = []
        for i, doc in enumerate(final_docs, start=1):
            id_, text, year_key, region_key, socio_key, realstate_key, asset_key, similarity = doc
            
            category = "Unknown"
            if socio_key:
                category = "Socioeconomic"
            elif asset_key:
                category = f"Crypto/Stock ({asset_key})"
            elif realstate_key:
                category = "Real Estate"
            
            context_parts.append(
                f"Document {i} [{category}] (year={year_key}, similarity={similarity:.3f}):\n{text}\n"
            )
        
        context = "\n".join(context_parts)
        
        print(f"\n📄 Final Context Composition:")
        socio_count = sum(1 for d in final_docs if d[4] is not None)
        crypto_count = sum(1 for d in final_docs if d[6] is not None)
        realstate_count = sum(1 for d in final_docs if d[5] is not None)
        
        print(f"  - Socioeconomic: {socio_count} docs")
        print(f"  - Crypto/Stock: {crypto_count} docs")
        print(f"  - Real Estate: {realstate_count} docs")
        print(f"  - Total: {len(final_docs)} docs\n")

        print("\n🔍 Debug: Retrieved indicator codes:")

        indicator_codes = {}
        for doc in final_docs:
            if doc[4]:
                indicator_codes[doc[4]] = indicator_codes.get(doc[4], 0) + 1

        for code, count in indicator_codes.items():
            print(f"  - {code}: {count} docs")
        
        system_prompt = """You are an expert financial analyst that answers questions using the provided documents.

IMPORTANT INSTRUCTIONS:
- Analyze ALL provided documents carefully
- The documents are organized by category (Socioeconomic, Crypto/Stock, Real Estate)
- If the question asks about multiple topics, make sure to address ALL of them
- Use specific numbers and dates from the documents
- If comparing trends, explicitly describe both trends
- If documents are missing for part of the question, acknowledge that limitation
- Be analytical and draw insights from the data

Format your answer clearly with sections if discussing multiple topics."""
        
        user_prompt = f"""Question: {query}

Documents:
{context}

Answer the question comprehensively using ALL relevant documents. If the question has multiple parts, address each part clearly."""
        
        response = self.client.chat.completions.create(
            model=self.chat_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()

def main_enhanced():
    from dotenv import load_dotenv
    import os
    from openai import OpenAI
    
    load_dotenv()
    
    DB_SETTINGS = {
        "dbname": "rag_db",
        "user": "rag_user",
        "password": "rag_password",
        "host": "localhost",
        "port": 5432,
    }
    client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))
    
    rag = EnhancedRAGRetrieval(DB_SETTINGS, client)
    
    print("="*80)
    print("ENHANCED RAG SYSTEM WITH BALANCED RETRIEVAL")
    print("="*80)
    
    test_queries = [
        "What was USA's GDP trend from 2014-2020 and how did Bitcoin behave in the same years?",
        "Compare Apple stock performance to Ethereum in 2020",
        "How did US unemployment rates correlate with home prices in California from 2015-2020?",
    ]
    
    print("\nType your question (or 'test' to run example queries, 'quit' to exit):\n")
    
    while True:
        query = input("\n📝 Question: ").strip()
        
        if not query or query.lower() in {'q', 'quit', 'exit'}:
            break
        
        if query.lower() == 'test':
            for test_q in test_queries:
                print(f"\n{'='*80}")
                print(f"TEST QUERY: {test_q}")
                print('='*80)
                
                answer = rag.answer_question_with_balanced_context(
                    test_q,
                    total_k=200,
                    final_k=150
                )
                
                print(f"\n💡 ANSWER:\n{answer}\n")
                input("Press Enter to continue to next test...")
            continue
        
        answer = rag.answer_question_with_balanced_context(
            query,
            total_k=200, 
            final_k=150 
        )
        
        print(f"\n💡 ANSWER:\n{answer}\n")


if __name__ == "__main__":
    main_enhanced()
