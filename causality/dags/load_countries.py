from datetime import datetime, timedelta

import pycountry
from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection
from causality.utils.geo import get_or_create_geo_entity
from causality.utils.errors import send_error_to_sentry

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 0,
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
    with get_db_connection() as conn, conn.cursor() as cursor:
        for country in pycountry.countries:
            get_or_create_geo_entity(
                country.alpha_2,
                COMMON_NAMES.get(country.alpha_2, country.name),
                "country",
                cursor,
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

    load_data_task = PythonOperator(
        task_id="load_countries",
        python_callable=load_countries,
    )
