from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection
from causality.utils.geo import get_geo_entity, get_or_create_geo_entity
from causality.utils.errors import send_error_to_sentry

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_error_to_sentry,
}

REGIONS = {
    "AFR": {
        "name": "Africa",
        "type": "continent",
        "countries": [
            {"code": "DZ", "since": "0001-01-01"},
            {"code": "AO", "since": "0001-01-01"},
            {"code": "BJ", "since": "0001-01-01"},
            {"code": "BW", "since": "0001-01-01"},
            {"code": "BF", "since": "0001-01-01"},
            {"code": "BI", "since": "0001-01-01"},
            {"code": "CV", "since": "0001-01-01"},
            {"code": "CM", "since": "0001-01-01"},
            {"code": "CF", "since": "0001-01-01"},
            {"code": "TD", "since": "0001-01-01"},
            {"code": "KM", "since": "0001-01-01"},
            {"code": "CG", "since": "0001-01-01"},
            {"code": "CD", "since": "0001-01-01"},
            {"code": "DJ", "since": "0001-01-01"},
            {"code": "EG", "since": "0001-01-01"},
            {"code": "GQ", "since": "0001-01-01"},
            {"code": "ER", "since": "0001-01-01"},
            {"code": "SZ", "since": "0001-01-01"},
            {"code": "ET", "since": "0001-01-01"},
            {"code": "GA", "since": "0001-01-01"},
            {"code": "GM", "since": "0001-01-01"},
            {"code": "GH", "since": "0001-01-01"},
            {"code": "GN", "since": "0001-01-01"},
            {"code": "GW", "since": "0001-01-01"},
            {"code": "CI", "since": "0001-01-01"},
            {"code": "KE", "since": "0001-01-01"},
            {"code": "LS", "since": "0001-01-01"},
            {"code": "LR", "since": "0001-01-01"},
            {"code": "LY", "since": "0001-01-01"},
            {"code": "MG", "since": "0001-01-01"},
            {"code": "MW", "since": "0001-01-01"},
            {"code": "ML", "since": "0001-01-01"},
            {"code": "MR", "since": "0001-01-01"},
            {"code": "MU", "since": "0001-01-01"},
            {"code": "MA", "since": "0001-01-01"},
            {"code": "MZ", "since": "0001-01-01"},
            {"code": "NA", "since": "0001-01-01"},
            {"code": "NE", "since": "0001-01-01"},
            {"code": "NG", "since": "0001-01-01"},
            {"code": "RW", "since": "0001-01-01"},
            {"code": "ST", "since": "0001-01-01"},
            {"code": "SN", "since": "0001-01-01"},
            {"code": "SC", "since": "0001-01-01"},
            {"code": "SL", "since": "0001-01-01"},
            {"code": "SO", "since": "0001-01-01"},
            {"code": "ZA", "since": "0001-01-01"},
            {"code": "SS", "since": "0001-01-01"},
            {"code": "SD", "since": "0001-01-01"},
            {"code": "TZ", "since": "0001-01-01"},
            {"code": "TG", "since": "0001-01-01"},
            {"code": "TN", "since": "0001-01-01"},
            {"code": "UG", "since": "0001-01-01"},
            {"code": "EH", "since": "0001-01-01"},
            {"code": "ZM", "since": "0001-01-01"},
            {"code": "ZW", "since": "0001-01-01"},
        ],
    },
    "AFR-NA": {
        "name": "North Africa",
        "type": "region",
        "countries": [
            {"code": "DZ", "since": "0001-01-01"},
            {"code": "EG", "since": "0001-01-01"},
            {"code": "LY", "since": "0001-01-01"},
            {"code": "MA", "since": "0001-01-01"},
            {"code": "TN", "since": "0001-01-01"},
            {"code": "SD", "since": "0001-01-01"},
            {"code": "EH", "since": "0001-01-01"},
        ],
    },
    "AFR-WA": {
        "name": "West Africa",
        "type": "region",
        "countries": [
            {"code": "BJ", "since": "0001-01-01"},
            {"code": "BF", "since": "0001-01-01"},
            {"code": "CV", "since": "0001-01-01"},
            {"code": "CI", "since": "0001-01-01"},
            {"code": "GM", "since": "0001-01-01"},
            {"code": "GH", "since": "0001-01-01"},
            {"code": "GN", "since": "0001-01-01"},
            {"code": "GW", "since": "0001-01-01"},
            {"code": "LR", "since": "0001-01-01"},
            {"code": "ML", "since": "0001-01-01"},
            {"code": "MR", "since": "0001-01-01"},
            {"code": "NE", "since": "0001-01-01"},
            {"code": "NG", "since": "0001-01-01"},
            {"code": "SN", "since": "0001-01-01"},
            {"code": "SL", "since": "0001-01-01"},
            {"code": "TG", "since": "0001-01-01"},
        ],
    },
    "AFR-CA": {
        "name": "Central Africa",
        "type": "region",
        "countries": [
            {"code": "CM", "since": "0001-01-01"},
            {"code": "CF", "since": "0001-01-01"},
            {"code": "TD", "since": "0001-01-01"},
            {"code": "CG", "since": "0001-01-01"},
            {"code": "CD", "since": "0001-01-01"},
            {"code": "GQ", "since": "0001-01-01"},
            {"code": "GA", "since": "0001-01-01"},
            {"code": "ST", "since": "0001-01-01"},
        ],
    },
    "AFR-EA": {
        "name": "East Africa",
        "type": "region",
        "countries": [
            {"code": "BI", "since": "0001-01-01"},
            {"code": "DJ", "since": "0001-01-01"},
            {"code": "ER", "since": "0001-01-01"},
            {"code": "ET", "since": "0001-01-01"},
            {"code": "KE", "since": "0001-01-01"},
            {"code": "RW", "since": "0001-01-01"},
            {"code": "SO", "since": "0001-01-01"},
            {"code": "SS", "since": "0001-01-01"},
            {"code": "TZ", "since": "0001-01-01"},
            {"code": "UG", "since": "0001-01-01"},
        ],
    },
    "AFR-SA": {
        "name": "Southern Africa",
        "type": "region",
        "countries": [
            {"code": "AO", "since": "0001-01-01"},
            {"code": "BW", "since": "0001-01-01"},
            {"code": "KM", "since": "0001-01-01"},
            {"code": "LS", "since": "0001-01-01"},
            {"code": "MG", "since": "0001-01-01"},
            {"code": "MW", "since": "0001-01-01"},
            {"code": "MU", "since": "0001-01-01"},
            {"code": "MZ", "since": "0001-01-01"},
            {"code": "NA", "since": "0001-01-01"},
            {"code": "SC", "since": "0001-01-01"},
            {"code": "ZA", "since": "0001-01-01"},
            {"code": "SZ", "since": "0001-01-01"},
            {"code": "ZM", "since": "0001-01-01"},
            {"code": "ZW", "since": "0001-01-01"},
        ],
    },
    "AFR-SSA": {
        "name": "Sub-Saharan Africa",
        "type": "region",
        "countries": [
            {"code": "AO", "since": "0001-01-01"},
            {"code": "BJ", "since": "0001-01-01"},
            {"code": "BW", "since": "0001-01-01"},
            {"code": "BF", "since": "0001-01-01"},
            {"code": "BI", "since": "0001-01-01"},
            {"code": "CV", "since": "0001-01-01"},
            {"code": "CM", "since": "0001-01-01"},
            {"code": "CF", "since": "0001-01-01"},
            {"code": "TD", "since": "0001-01-01"},
            {"code": "KM", "since": "0001-01-01"},
            {"code": "CG", "since": "0001-01-01"},
            {"code": "CD", "since": "0001-01-01"},
            {"code": "DJ", "since": "0001-01-01"},
            {"code": "GQ", "since": "0001-01-01"},
            {"code": "ER", "since": "0001-01-01"},
            {"code": "SZ", "since": "0001-01-01"},
            {"code": "ET", "since": "0001-01-01"},
            {"code": "GA", "since": "0001-01-01"},
            {"code": "GM", "since": "0001-01-01"},
            {"code": "GH", "since": "0001-01-01"},
            {"code": "GN", "since": "0001-01-01"},
            {"code": "GW", "since": "0001-01-01"},
            {"code": "CI", "since": "0001-01-01"},
            {"code": "KE", "since": "0001-01-01"},
            {"code": "LS", "since": "0001-01-01"},
            {"code": "LR", "since": "0001-01-01"},
            {"code": "MG", "since": "0001-01-01"},
            {"code": "MW", "since": "0001-01-01"},
            {"code": "ML", "since": "0001-01-01"},
            {"code": "MR", "since": "0001-01-01"},
            {"code": "MU", "since": "0001-01-01"},
            {"code": "MZ", "since": "0001-01-01"},
            {"code": "NA", "since": "0001-01-01"},
            {"code": "NE", "since": "0001-01-01"},
            {"code": "NG", "since": "0001-01-01"},
            {"code": "RW", "since": "0001-01-01"},
            {"code": "ST", "since": "0001-01-01"},
            {"code": "SN", "since": "0001-01-01"},
            {"code": "SC", "since": "0001-01-01"},
            {"code": "SL", "since": "0001-01-01"},
            {"code": "SO", "since": "0001-01-01"},
            {"code": "ZA", "since": "0001-01-01"},
            {"code": "SS", "since": "0001-01-01"},
            {"code": "TZ", "since": "0001-01-01"},
            {"code": "TG", "since": "0001-01-01"},
            {"code": "UG", "since": "0001-01-01"},
            {"code": "ZM", "since": "0001-01-01"},
            {"code": "ZW", "since": "0001-01-01"},
        ],
    },
    "AFR-ISL": {
        "name": "African Islands",
        "type": "region",
        "countries": [
            {"code": "CV", "since": "0001-01-01"},
            {"code": "KM", "since": "0001-01-01"},
            {"code": "MG", "since": "0001-01-01"},
            {"code": "MU", "since": "0001-01-01"},
            {"code": "ST", "since": "0001-01-01"},
            {"code": "SC", "since": "0001-01-01"},
        ],
    },
    "AFR-LLC": {
        "name": "African Landlocked Countries",
        "type": "region",
        "countries": [
            {"code": "BF", "since": "0001-01-01"},
            {"code": "BI", "since": "0001-01-01"},
            {"code": "CF", "since": "0001-01-01"},
            {"code": "TD", "since": "0001-01-01"},
            {"code": "ET", "since": "0001-01-01"},
            {"code": "LS", "since": "0001-01-01"},
            {"code": "MW", "since": "0001-01-01"},
            {"code": "ML", "since": "0001-01-01"},
            {"code": "NE", "since": "0001-01-01"},
            {"code": "RW", "since": "0001-01-01"},
            {"code": "SS", "since": "0001-01-01"},
            {"code": "SZ", "since": "0001-01-01"},
            {"code": "UG", "since": "0001-01-01"},
            {"code": "ZM", "since": "0001-01-01"},
            {"code": "ZW", "since": "0001-01-01"},
        ],
    },
    "AFR-SHL": {
        "name": "Sahel",
        "type": "region",
        "countries": [
            {"code": "BF", "since": "0001-01-01"},
            {"code": "TD", "since": "0001-01-01"},
            {"code": "ML", "since": "0001-01-01"},
            {"code": "MR", "since": "0001-01-01"},
            {"code": "NE", "since": "0001-01-01"},
            {"code": "SD", "since": "0001-01-01"},
            {"code": "SN", "since": "0001-01-01"},
        ],
    },
    "EAC": {
        "name": "East African Community",
        "type": "bloc",
        "countries": [
            {"code": "KE", "since": "2000-07-01"},
            {"code": "TZ", "since": "2000-07-01"},
            {"code": "UG", "since": "2000-07-01"},
            {"code": "RW", "since": "2007-07-01"},
            {"code": "BI", "since": "2007-07-01"},
            {"code": "SS", "since": "2016-09-05"},
            {"code": "CD", "since": "2022-07-11"},
            {"code": "SO", "since": "2024-05-04"},
        ],
    },
}


def load_regions() -> None:
    """Load regions into database."""
    with get_db_connection() as conn, conn.cursor() as cursor:
        for code, data in REGIONS.items():
            region = get_or_create_geo_entity(code, data["name"], data["type"], cursor)

            for member in data["countries"]:
                country = get_geo_entity(member["code"], cursor)

                if not country:
                    continue

                query = """
                INSERT INTO geo_relationships (parent_id, child_id, since, until)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (parent_id, child_id) DO UPDATE SET
                    since = EXCLUDED.since,
                    until = EXCLUDED.until
                """

                cursor.execute(
                    query,
                    (region.id, country.id, member["since"], member.get("until", None)),
                )

            conn.commit()


with DAG(
    "load_regions_dag",
    default_args=default_args,
    description="Load regions into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 23),
    tags=["foundation"],
) as dag:

    load_data_task = PythonOperator(
        task_id="load_regions",
        python_callable=load_regions,
    )
