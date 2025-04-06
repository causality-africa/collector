from datetime import datetime, timedelta

import pycountry
from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection
from causality.utils.errors import send_error_to_sentry

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_error_to_sentry,
}

COMMON_NAMES = {
    "BN": "Brunei",
    "BO": "Bolivia",
    "CD": "Democratic Republic of Congo",
    "FM": "Micronesia",
    "GB": "United Kingdom",
    "IR": "Iran",
    "KP": "North Korea",
    "KR": "South Korea",
    "LA": "Laos",
    "MD": "Moldova",
    "NL": "Netherlands",
    "PS": "Palestine",
    "RU": "Russia",
    "SY": "Syria",
    "TW": "Taiwan",
    "TZ": "Tanzania",
    "VE": "Venezuela",
    "VN": "Vietnam",
}


def load_countries() -> None:
    """Load countries from pycountry into database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for country in pycountry.countries:
                name = COMMON_NAMES.get(country.alpha_2, country.name)

                query = """
                    INSERT INTO locations (name, code, admin_level, map)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        admin_level = EXCLUDED.admin_level,
                        map = EXCLUDED.map
                """

                cur.execute(query, (name, country.alpha_2, 0, None))

            conn.commit()


with DAG(
    "load_countries_dag",
    default_args=default_args,
    description="Load countries from pycountry into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 23),
    tags=["foundation"],
) as dag:
    load_data_task = PythonOperator(
        task_id="load_countries",
        python_callable=load_countries,
    )
