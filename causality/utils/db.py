import os

import psycopg


def get_db_connection() -> psycopg.Connection:
    conn_str = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")

    # Extract connection parameters from SQLAlchemy URL
    if conn_str.startswith("postgresql://"):
        conn_str = conn_str.replace("postgresql://", "postgres://")
    elif conn_str.startswith("postgresql+psycopg2://"):
        conn_str = conn_str.replace("postgresql+psycopg2://", "postgres://")

    return psycopg.connect(conn_str)
