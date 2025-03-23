import os
from datetime import timedelta

import pycountry
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_countries():
    conn_str = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    engine = create_engine(conn_str)

    with engine.connect() as connection:
        for country in pycountry.countries:
            query = text(
                """
                    INSERT INTO locations (name, code, admin_level, map)
                    VALUES (:name, :code, :admin_level, :map)
                    ON CONFLICT (code) DO UPDATE SET
                        name = :name,
                        admin_level = :admin_level,
                        map = :map
                """
            )

            connection.execute(
                query,
                {
                    "name": country.name,
                    "code": country.alpha_2,
                    "admin_level": 0,  # Countries are top level
                    "map": None,
                },
            )

        connection.commit()


with DAG(
    "countries_dag",
    default_args=default_args,
    description="Load countries from pycountry into database",
    schedule_interval="@once",
) as dag:
    fetch_and_load_task = PythonOperator(
        task_id="load_countries",
        python_callable=load_countries,
    )
