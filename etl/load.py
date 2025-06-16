"""load.py
Load phase of the YouTube ETL pipeline.
--------------------------------------
Takes one or more processed CSV paths (produced by `transform.py`) and ingests
records into a relational database table.  By default it targets PostgreSQL via
SQLAlchemy, but *any* DB supported by SQLAlchemy (SQLite, MySQL, etc.) will work.

Usage examples:
    # CLI (local test)
    python load.py --inputs data/processed/*.csv --db_url postgresql://user:pw@localhost:5432/yt

    # In Airflow
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=run_load,
        params={"db_url": "postgresql://user:pw@db/yt"},
        dag=dag,
    )
    # Upstream task pushes processed_paths via XCom.

Environment variables:
    DB_URL  – fallback database URL if --db_url is not passed.
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

TABLE_NAME = "youtube_videos"
CHUNK_SIZE = 10_000  # rows per chunk during load

# --------------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------------

def get_engine(db_url: str) -> Engine:
    """Return a SQLAlchemy engine."""
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        # quick connection test
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as exc:
        logger.error("Could not connect to database: %s", exc)
        raise


def ensure_table(engine: Engine):
    """Create the destination table if it does not exist (simple schema)."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        video_id       TEXT PRIMARY KEY,
        title          TEXT,
        channel_title  TEXT,
        published_at   TIMESTAMP,
        description    TEXT,
        query_tag      TEXT,
        loaded_at      TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Ensured table '%s' exists", TABLE_NAME)


def load_csv(path: Path, engine: Engine, if_exists: str = "append"):
    """Load a single CSV file into the target table."""
    logger.info("Loading %s", path)
    df = pd.read_csv(path, encoding="utf-8-sig")
    # Convert date strings to datetime for proper DB insertion
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    # Upsert logic: simple approach using ON CONFLICT for Postgres
    # For engines that don't support ON CONFLICT, fall back to append+deduplicate later.
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            temp_table = "_tmp_youtube_load"
            df.to_sql(temp_table, conn, index=False, if_exists="replace", chunksize=CHUNK_SIZE)
            merge_sql = text(f"""
                INSERT INTO {TABLE_NAME} (video_id, title, channel_title, published_at, description, query_tag)
                SELECT video_id, title, channel_title, published_at, description, query_tag
                FROM {temp_table}
                ON CONFLICT (video_id) DO UPDATE
                SET title = EXCLUDED.title,
                    channel_title = EXCLUDED.channel_title,
                    published_at = EXCLUDED.published_at,
                    description = EXCLUDED.description,
                    query_tag = EXCLUDED.query_tag;
            """)
            conn.execute(merge_sql)
            conn.execute(text(f"DROP TABLE {temp_table}"))
        else:
            df.to_sql(TABLE_NAME, conn, index=False, if_exists=if_exists, chunksize=CHUNK_SIZE)

    logger.info("Loaded %d rows from %s", len(df), path.name)

# --------------------------------------------------------------------------------------
# Main callable for Airflow
# --------------------------------------------------------------------------------------

def run_load(**context):
    """Airflow task wrapper.

    Expects XCom `processed_paths` (list of CSVs) from upstream transform task.
    Additional param: `db_url` can be supplied via DAG's params, else env var DB_URL.
    """
    params = context.get("params", {})
    db_url = params.get("db_url") or os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Database URL not provided. Set DB_URL env var or pass via params['db_url'].")

    processed_paths: List[str] = context["ti"].xcom_pull(key="processed_paths", task_ids=params.get("upstream_task_id"))
    if not processed_paths:
        raise ValueError("No processed CSV paths received from XCom.")

    engine = get_engine(db_url)
    ensure_table(engine)

    for csv_path in processed_paths:
        load_csv(Path(csv_path), engine)

# --------------------------------------------------------------------------------------
# CLI entry-point
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load processed CSVs into a SQL database")
    parser.add_argument("--inputs", nargs="+", required=True, help="Processed CSV paths to load")
    parser.add_argument("--db_url", required=False, help="SQLAlchemy DB URL; fallback to env DB_URL")
    args = parser.parse_args()

    db_url = args.db_url or os.getenv("DB_URL")
    if not db_url:
        logger.error("Database URL must be provided via --db_url or env DB_URL")
        exit(1)

    engine = get_engine(db_url)
    ensure_table(engine)

    for csv_path in args.inputs:
        load_csv(Path(csv_path), engine)

    logger.info("Finished loading all files.")
