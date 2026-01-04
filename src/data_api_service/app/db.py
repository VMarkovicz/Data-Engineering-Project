import os
import duckdb

def get_duckdb_connection():
    path = os.getenv("DUCKDB_PATH", ":memory:")
    return duckdb.connect(path, read_only=True)
