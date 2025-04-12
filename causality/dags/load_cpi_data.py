import logging
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.data_points import upsert_data_point
from causality.utils.db import get_db_connection
from causality.utils.errors import send_error_to_sentry
from causality.utils.geo import get_geo_entity, iso3_to_iso2
from causality.utils.indicators import get_or_create_indicator
from causality.utils.sources import get_or_create_data_source
from causality.utils.storage import download_from_backblaze

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_error_to_sentry,
}

YEAR_START = 2012
YEAR_END = 2024


def load_cpi_data():
    """Load CPI data from Excel file into database."""
    cpi_file = download_from_backblaze(
        "governance/CPI2024-Results-and-trends.xlsx",
        ".xlsx",
    )
    df = pd.read_excel(cpi_file, "CPI Timeseries 2012 - 2024")

    with get_db_connection() as conn, conn.cursor() as cursor:
        # Create Transparency International source
        source = get_or_create_data_source(
            "Transparency International",
            "https://www.transparency.org/en/cpi/2024",
            "Corruption Perceptions Index",
            "2024-01-01",
            cursor,
        )

        # Create CPI indicator
        indicator = get_or_create_indicator(
            "cpi",
            "Corruption Perceptions Index",
            "rule-of-law",
            "Perceived levels of public sector corruption in countries worldwide",
            "score",
            "numeric",
            cursor,
        )

        for _, row in df.iterrows():
            iso3 = row["ISO3"]
            iso2 = iso3_to_iso2(iso3)

            if not iso2:
                logging.warning(
                    f"Could not find ISO2 code for {row['Country / Territory']} ({iso3})"
                )
                continue

            country = get_geo_entity(iso2, cursor)
            if not country:
                continue

            # Insert data points for each available year
            for year in range(YEAR_START, YEAR_END + 1):
                score_col = f"CPI score {year}"

                if score_col in row and pd.notna(row[score_col]):
                    upsert_data_point(
                        country.id,
                        indicator.id,
                        source.id,
                        f"{year}-01-01",
                        Decimal(row[score_col]),
                        None,
                        cursor,
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
