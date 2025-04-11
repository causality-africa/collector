from datetime import date, datetime, timedelta

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

HISTORICAL_START_DATE = date(1900, 1, 1)

REGIONS = {
    "AF": {
        "name": "Africa",
        "description": "African continent",
        "countries": [
            "DZ",
            "AO",
            "BJ",
            "BW",
            "BF",
            "BI",
            "CV",
            "CM",
            "CF",
            "TD",
            "KM",
            "CG",
            "CD",
            "DJ",
            "EG",
            "GQ",
            "ER",
            "SZ",
            "ET",
            "GA",
            "GM",
            "GH",
            "GN",
            "GW",
            "CI",
            "KE",
            "LS",
            "LR",
            "LY",
            "MG",
            "MW",
            "ML",
            "MR",
            "MU",
            "MA",
            "MZ",
            "NA",
            "NE",
            "NG",
            "RW",
            "ST",
            "SN",
            "SC",
            "SL",
            "SO",
            "ZA",
            "SS",
            "SD",
            "TZ",
            "TG",
            "TN",
            "UG",
            "EH",
            "ZM",
            "ZW",
        ],
    },
    "AF-NORTH": {
        "name": "North Africa",
        "description": "North African region",
        "countries": ["DZ", "EG", "LY", "MA", "TN", "SD", "EH"],
    },
    "AF-WEST": {
        "name": "West Africa",
        "description": "West African region",
        "countries": [
            "BJ",
            "BF",
            "CV",
            "CI",
            "GM",
            "GH",
            "GN",
            "GW",
            "LR",
            "ML",
            "MR",
            "NE",
            "NG",
            "SN",
            "SL",
            "TG",
        ],
    },
    "AF-CENTRAL": {
        "name": "Central Africa",
        "description": "Central African region",
        "countries": ["CM", "CF", "TD", "CG", "CD", "GQ", "GA", "ST"],
    },
    "AF-EAST": {
        "name": "East Africa",
        "description": "East African region",
        "countries": ["BI", "DJ", "ER", "ET", "KE", "RW", "SO", "SS", "TZ", "UG"],
    },
    "AF-SOUTH": {
        "name": "Southern Africa",
        "description": "Southern African region",
        "countries": [
            "AO",
            "BW",
            "KM",
            "LS",
            "MG",
            "MW",
            "MU",
            "MZ",
            "NA",
            "SC",
            "ZA",
            "SZ",
            "ZM",
            "ZW",
        ],
    },
    "AF-SSA": {
        "name": "Sub-Saharan Africa",
        "description": "All African countries south of the Sahara desert",
        "countries": [
            "AO",
            "BJ",
            "BW",
            "BF",
            "BI",
            "CV",
            "CM",
            "CF",
            "TD",
            "KM",
            "CG",
            "CD",
            "DJ",
            "GQ",
            "ER",
            "SZ",
            "ET",
            "GA",
            "GM",
            "GH",
            "GN",
            "GW",
            "CI",
            "KE",
            "LS",
            "LR",
            "MG",
            "MW",
            "ML",
            "MR",
            "MU",
            "MZ",
            "NA",
            "NE",
            "NG",
            "RW",
            "ST",
            "SN",
            "SC",
            "SL",
            "SO",
            "ZA",
            "SS",
            "TZ",
            "TG",
            "UG",
            "ZM",
            "ZW",
        ],
    },
    "AF-ISL": {
        "name": "African Islands",
        "description": "Island nations in Africa",
        "countries": ["CV", "KM", "MG", "MU", "ST", "SC"],
    },
    "AF-LOCKED": {
        "name": "African Landlocked Countries",
        "description": "Countries in Africa without coastline",
        "countries": [
            "BF",
            "BI",
            "CF",
            "TD",
            "ET",
            "LS",
            "MW",
            "ML",
            "NE",
            "RW",
            "SS",
            "SZ",
            "UG",
            "ZM",
            "ZW",
        ],
    },
    "AF-SAHEL": {
        "name": "Sahel Region",
        "description": "Countries in Africa's Sahel region",
        "countries": ["BF", "TD", "ML", "MR", "NE", "SD", "SN"],
    },
}


def create_regions() -> dict:
    """Create all African regions and return a mapping of codes to IDs."""
    region_ids = {}

    with get_db_connection() as conn, conn.cursor() as cur:
        for code, region_data in REGIONS.items():
            query = """
                    INSERT INTO regions (name, code, description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description
                    RETURNING id
                """
            cur.execute(query, (region_data["name"], code, region_data["description"]))
            region_ids[code] = cur.fetchone()[0]

        conn.commit()

    return region_ids


def link_countries_to_regions(region_ids: dict) -> None:
    """Link countries to their respective regions."""
    with get_db_connection() as conn, conn.cursor() as cur:
        for region_code, region_data in REGIONS.items():
            region_id = region_ids[region_code]

            for country_code in region_data["countries"]:
                # Get location id for the country
                cur.execute("SELECT id FROM locations WHERE code = %s", (country_code,))
                result = cur.fetchone()

                if not result:
                    continue

                location_id = result[0]
                query = """
                        INSERT INTO location_in_region (location_id, region_id, join_date)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (location_id, region_id) DO UPDATE SET
                            join_date = EXCLUDED.join_date
                    """
                cur.execute(query, (location_id, region_id, HISTORICAL_START_DATE))

        conn.commit()


def load_geographical_regions() -> None:
    """Load geographical regions into database."""
    region_ids = create_regions()
    link_countries_to_regions(region_ids)


with DAG(
    "load_geographical_regions_dag",
    default_args=default_args,
    description="Load geographical regions into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 23),
    tags=["foundation"],
) as dag:
    load_data_task = PythonOperator(
        task_id="load_geographical_regions",
        python_callable=load_geographical_regions,
    )
