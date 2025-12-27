import psycopg2
from pgvector.psycopg2 import register_vector
from openai import OpenAI
from dotenv import load_dotenv
import os

# ----- CONFIG -----

EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini-2024-07-18"

DB_SETTINGS = {
    "dbname": "rag_db",
    "user": "rag_user",
    "password": "rag_password",
    "host": "localhost", 
    "port": 5432,
}

load_dotenv()

client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))


# ----- DB UTILS -----

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
            """)
            conn.commit()
        print("DB initialized.")
    finally:
        conn.close()


def get_connection():
    conn = psycopg2.connect(**DB_SETTINGS)
    register_vector(conn)
    return conn


# ----- EMBEDDINGS -----

def get_embedding(text: str) -> list[float]:
    res = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text,
    )
    return res.data[0].embedding


# ----- INSERT DOC -----

def insert_chunk(
    text: str,
    year_key: int | None = None,
    region_key: int | None = None,
    socio_indicator_key: str | None = None,
    realstate_indicator_key: str | None = None,
    asset_key: str | None = None,
) -> int:
    embedding = get_embedding(text)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rag.doc_chunks (
                    text,
                    embedding,
                    year_key,
                    region_key,
                    socio_indicator_key,
                    realstate_indicator_key,
                    asset_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    text,
                    embedding,
                    year_key,
                    region_key,
                    socio_indicator_key,
                    realstate_indicator_key,
                    asset_key,
                ),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        print(f"Inserted chunk id: {new_id}")
        return new_id
    finally:
        conn.close()


# ----- SEARCH -----

def search_similar_chunks(query_text: str, top_k: int = 5):
    query_embedding = get_embedding(query_text)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET enable_indexscan = off;")
            cur.execute("SET enable_bitmapscan = off;")

            cur.execute(
                """
                SELECT
                    id,
                    text,
                    year_key,
                    region_key,
                    1 - (embedding <=> %s::vector) AS similarity
                FROM rag.doc_chunks
                ORDER BY embedding <=> %s::vector
                LIMIT %s;
                """,
                (query_embedding, query_embedding, top_k),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return rows


# ----- LLM ANSWER -----

def answer_question_with_llm(query: str, retrieved_rows):
    """
    retrieved_rows: list of tuples (id, text, year_key, region_key, similarity)
    Uses GPT-4o mini to answer using these rows as context.
    """
    # Build a context string
    context_parts = []
    for i, (id_, text, year_key, region_key, similarity) in enumerate(retrieved_rows, start=1):
        context_parts.append(
            f"Document {i} (id={id_}, year={year_key}, region={region_key}, sim={similarity:.3f}): {text}"
        )
    context = "\n".join(context_parts)

    system_prompt = (
        "You are a data assistant that answers questions using the provided documents only. "
        "If the answer is not clearly supported by the documents, say you are not sure. "
        "Be concise and factual."
        "Before answering, analyse the question carefully and check if the documents contain relevant information."
        "The ones that are more relevant are listed first."
        "If they do not contain relevant information, discard them in your answer and just use the ones that are relevant."
    )

    user_prompt = (
        f"Question: {query}\n\n"
        f"Documents:\n{context}\n\n"
        "Answer the question using only these documents."
    )

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )

    return response.choices[0].message.content.strip()


# ----- SAMPLE DATA -----

sample_chunks = [
    {
        "text": "In 2019, the median house price in region X was 220,000 EUR, with moderate growth over the previous five years.",
        "year_key": 2019,
        "region_key": 1,
        "socio_indicator_key": "income_median",
        "realstate_indicator_key": "median_house_price",
        "asset_key": "real_estate",
    },
    {
        "text": "In 2020, apartment rents in region X averaged 900 EUR per month, remaining stable compared to 2019.",
        "year_key": 2020,
        "region_key": 1,
        "socio_indicator_key": "rent_index",
        "realstate_indicator_key": "median_rent_price",
        "asset_key": "real_estate",
    },
    {
        "text": "In 2021, the median house price in region X increased to 270,000 EUR, driven by low interest rates and limited supply.",
        "year_key": 2021,
        "region_key": 1,
        "socio_indicator_key": "interest_rate",
        "realstate_indicator_key": "median_house_price",
        "asset_key": "real_estate",
    },
    {
        "text": "In 2020, the median house price in region Y was 180,000 EUR, significantly lower than in region X.",
        "year_key": 2020,
        "region_key": 2,
        "socio_indicator_key": "income_median",
        "realstate_indicator_key": "median_house_price",
        "asset_key": "real_estate",
    },
    {
        "text": "In 2022, commercial property prices in region X fell slightly, while residential properties continued to rise.",
        "year_key": 2022,
        "region_key": 1,
        "socio_indicator_key": "employment_rate",
        "realstate_indicator_key": "commercial_price_index",
        "asset_key": "commercial_real_estate",
    },
]


# ----- MAIN -----

def main():
    init_db()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rag.doc_chunks;")
            count = cur.fetchone()[0]
    finally:
        conn.close()

    if count == 0:
        print("Seeding example chunks...")
        example_text = (
            "In 2020, the median house price in region X was 250,000 EUR, "
            "showing strong growth compared to previous years."
        )
        insert_chunk(
            example_text,
            year_key=2020,
            region_key=1,
            socio_indicator_key="income_median",
            realstate_indicator_key="median_house_price",
            asset_key="real_estate",
        )

        for chunk in sample_chunks:
            insert_chunk(
                chunk["text"],
                year_key=chunk["year_key"],
                region_key=chunk["region_key"],
                socio_indicator_key=chunk["socio_indicator_key"],
                realstate_indicator_key=chunk["realstate_indicator_key"],
                asset_key=chunk["asset_key"],
            )
    else:
        print(f"Table already has {count} rows, skipping seeding.")

    print("\nType a search query (or 'quit' to exit).")
    while True:
        query = input("\nSearch: ").strip()
        if not query or query.lower() in {"q", "quit", "exit"}:
            break

        results = search_similar_chunks(query, top_k=5)
        if not results:
            print("No results.")
            continue

        print(f"\nTop {len(results)} retrieved chunks:\n")
        for i, (id_, text, year_key, region_key, similarity) in enumerate(results, start=1):
            print(f"[{i}] id={id_}, sim={similarity:.3f}, year={year_key}, region={region_key}")
            print(f"    {text}\n")

        answer = answer_question_with_llm(query, results)
        print("LLM answer:")
        print(answer)


if __name__ == "__main__":
    main()
