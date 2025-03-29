from datetime import datetime, timedelta

import pycountry
from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_countries():
    """Load countries from pycountry into database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for country in pycountry.countries:
                query = """
                    INSERT INTO locations (name, code, admin_level, map)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (code) DO UPDATE SET
                        name = %s,
                        admin_level = %s,
                        map = %s
                """

                cur.execute(
                    query,
                    (
                        country.name,
                        country.alpha_2,
                        0,  # Countries are top level
                        None,
                        country.name,
                        0,
                        None,
                    ),
                )

            conn.commit()


with DAG(
    "load_countries_dag",
    default_args=default_args,
    description="Load countries from pycountry into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 23),
    tags=["foundation"],
) as dag:
    fetch_and_load_task = PythonOperator(
        task_id="load_countries",
        python_callable=load_countries,
    )
