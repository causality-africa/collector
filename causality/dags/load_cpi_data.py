import logging
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection
from causality.utils.errors import send_error_to_sentry
from causality.utils.geo import iso3_to_iso2
from causality.utils.storage import download_from_backblaze

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_error_to_sentry,
}

YEAR_START = 2012
YEAR_END = 2024


def load_cpi_data():
    """Load CPI data from Excel file into database."""
    cpi_file = download_from_backblaze(
        "causality-africa",
        "governance/CPI2024-Results-and-trends.xlsx",
        ".xlsx",
    )
    df = pd.read_excel(cpi_file, "CPI Timeseries 2012 - 2024")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Create CPI indicator
            cur.execute(
            """
                INSERT INTO indicators (name, code, category, description, unit, data_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    description = EXCLUDED.description,
                    unit = EXCLUDED.unit
                RETURNING id
            """,
                (
                    "Corruption Perceptions Index",
                    "cpi",
                    "rule-of-law",
                    "Perceived levels of public sector corruption in countries worldwide",
                    "score",
                    "numeric",
                ),
            )
            indicator_id = cur.fetchone()[0]

            # Create Transparency International source
            cur.execute(
            """
                INSERT INTO data_sources (name, url, description, date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    description = EXCLUDED.description
                    date = EXCLUDED.date
                RETURNING id
            """,
                (
                    "Transparency International",
                    "https://www.transparency.org/en/cpi/2024",
                    "Corruption Perceptions Index",
                    "2024-01-01",
                ),
            )
            source_id = cur.fetchone()[0]

            for _, row in df.iterrows():
                iso3 = row["ISO3"]
                iso2 = iso3_to_iso2(iso3)

                if not iso2:
                    logging.warning(
                        f"Could not find ISO2 code for {row['Country / Territory']} ({iso3})"
                    )
                    continue

                # Get location_id from code
                cur.execute("SELECT id FROM locations WHERE code = %s", (iso2,))
                result = cur.fetchone()

                if not result:
                    continue

                location_id = result[0]

                # Insert data points for each available year
                for year in range(YEAR_START, YEAR_END+1):
                    score_col = f"CPI score {year}"

                    if score_col in row and pd.notna(row[score_col]):
                        score = float(row[score_col])
                        date_str = f"{year}-01-01"

                        cur.execute(
                        """
                            INSERT INTO data_points (
                                entity_type, entity_id, indicator_id, source_id,
                                date, numeric_value, text_value
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (entity_type, entity_id, indicator_id, source_id, date)
                            DO UPDATE SET numeric_value = EXCLUDED.numeric_value
                        """,
                            (
                                "location",
                                location_id,
                                indicator_id,
                                source_id,
                                date_str,
                                score,
                                None,
                            ),
                        )

            conn.commit()


with DAG(
    "load_cpi_data_dag",
    default_args=default_args,
    description="Load Corruption Perceptions Index data into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 23),
    tags=["governance"],
) as dag:

    load_task = PythonOperator(
        task_id="load_cpi_data",
        python_callable=load_cpi_data,
    )
