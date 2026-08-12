"""
Export dbt marts tables from the local DuckDB file into PostgreSQL,
so Metabase can query them via the official, stable Postgres driver
(bypassing the unstable community DuckDB plugin for Metabase).

Run this after `dbt run` to refresh the marts-db with the latest data.
"""
import os

import duckdb
import pandas as pd
from sqlalchemy import create_engine

DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "dev.duckdb")

MARTS_DB_USER = os.environ.get("MARTS_DB_USER", "marts")
MARTS_DB_PASSWORD = os.environ.get("MARTS_DB_PASSWORD", "marts")
MARTS_DB_NAME = os.environ.get("MARTS_DB_NAME", "marts")
MARTS_DB_HOST = os.environ.get("MARTS_DB_HOST", "localhost")
MARTS_DB_PORT = os.environ.get("MARTS_DB_PORT", "5434")

MART_TABLES = [
    "mart_traffic_by_hour",
    "mart_latency_percentiles",
    "mart_error_rate",
    "mart_traffic_by_upstream",
]


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    engine = create_engine(
        f"postgresql+psycopg2://{MARTS_DB_USER}:{MARTS_DB_PASSWORD}"
        f"@{MARTS_DB_HOST}:{MARTS_DB_PORT}/{MARTS_DB_NAME}"
    )

    for table in MART_TABLES:
        df = con.execute(f"SELECT * FROM main.{table}").fetchdf()
        df.to_sql(table, engine, if_exists="replace", index=False)
        print(f"Exported {len(df)} rows to Postgres table '{table}'")

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
