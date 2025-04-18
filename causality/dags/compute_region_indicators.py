import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.data_points import upsert_data_point
from causality.utils.db import get_db_connection
from causality.utils.errors import send_error_to_sentry
from causality.utils.indicators import get_indicator
from causality.utils.sources import get_derived_data_source

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_error_to_sentry,
}

avg_indicators = ["cpi"]
sum_indicators = [
    "total-population",
    "male-population",
    "female-population",
    "population-natural-change",
    "population-change",
    "births",
    "births-15-19",
    "deaths",
    "male-deaths",
    "female-deaths",
    "infant-deaths",
    "births-surviving-1",
    "under5-deaths",
    "net-migrants",
]

derived_indicators = {
    "birth-rate": {
        "dependencies": ["births", "total-population"],
        "calculate": lambda values: (
            values["births"] / values["total-population"] * 1000
            if values["total-population"]
            else None
        ),
    },
    "death-rate": {
        "dependencies": ["deaths", "total-population"],
        "calculate": lambda values: (
            values["deaths"] / values["total-population"] * 1000
            if values["total-population"]
            else None
        ),
    },
}


def fetch_countries_in_region(region_id, year, cursor):
    """Fetch countries that were members of a region during a given year."""
    query = """
    SELECT child_id
    FROM geo_relationships
    WHERE parent_id = %s
    AND since <= %s
    AND (until IS NULL OR until > %s)
    """

    cursor.execute(query, (region_id, f"{year}-12-31", f"{year}-01-01"))
    return [row[0] for row in cursor.fetchall()]


def fetch_indicator_data(indicator_id, country_ids, year, cursor):
    """Fetch indicator data for multiple countries."""
    if not country_ids:
        return {}

    placeholders = ",".join(["%s"] * len(country_ids))
    query = f"""
    SELECT geo_entity_id, numeric_value
    FROM data_points
    WHERE indicator_id = %s
    AND geo_entity_id IN ({placeholders})
    AND date BETWEEN %s AND %s
    """

    params = [indicator_id] + country_ids + [f"{year}-01-01", f"{year}-12-31"]
    cursor.execute(query, params)
    return {row[0]: row[1] for row in cursor.fetchall()}


def calculate_average(values):
    """Calculate average of values."""
    if not values:
        return None
    return sum(values) / len(values)


def calculate_sum(values):
    """Calculate sum of values."""
    if not values:
        return None
    return sum(values)


def insert_data_points(data_points, cursor):
    """Insert multiple data points into database."""
    for dp in data_points:
        upsert_data_point(
            dp["geo_entity_id"],
            dp["indicator_id"],
            dp["source_id"],
            dp["date"],
            dp["numeric_value"],
            dp["text_value"],
            cursor,
        )


def calculate_base_indicators(**kwargs):
    """Calculate base indicators for all regions."""
    results = []
    with get_db_connection() as conn, conn.cursor() as cursor:
        source = get_derived_data_source(cursor)

        cursor.execute(
            "SELECT id FROM geo_entities WHERE type='region' OR type='bloc' OR type='continent'"
        )
        regions = cursor.fetchall()

        all_indicators = avg_indicators + sum_indicators
        for indicator_code in all_indicators:
            indicator = get_indicator(indicator_code, cursor)
            if not indicator:
                logging.warning(f"Indicator {indicator_code} not found.")
                continue

            # Find years with data for this specific indicator
            cursor.execute(
                """
                SELECT DISTINCT EXTRACT(YEAR FROM date) as year
                FROM data_points
                WHERE indicator_id = %s
                ORDER BY year
                """,
                (indicator.id,),
            )

            years = [int(row[0]) for row in cursor.fetchall()]
            if not years:
                logging.warning(f"No data years found for indicator {indicator_code}")
                continue

            logging.info(
                f"Processing indicator {indicator_code} for years {min(years)} to {max(years)}"
            )

            # Determine calculation method
            calculation_method = (
                calculate_average if indicator_code in avg_indicators else calculate_sum
            )

            # Process each year for this indicator
            for year in years:
                year_results = []
                for region_id in [r[0] for r in regions]:
                    location_ids = fetch_countries_in_region(region_id, year, cursor)
                    if not location_ids:
                        continue

                    location_data = fetch_indicator_data(
                        indicator.id, location_ids, year, cursor
                    )
                    if not location_data:
                        continue

                    # Calculate value using appropriate method
                    calculated_value = calculation_method(list(location_data.values()))

                    year_results.append(
                        {
                            "geo_entity_id": region_id,
                            "indicator_id": indicator.id,
                            "date": f"{year}-12-31",
                            "numeric_value": calculated_value,
                            "text_value": None,
                            "source_id": source.id,
                        }
                    )

                results.extend(year_results)

                if year_results:
                    logging.info(
                        f"Processed {len(year_results)} regions for {indicator_code} in {year}"
                    )

            # Commit after each indicator
            if results:
                insert_data_points(results, cursor)
                conn.commit()
                logging.info(
                    f"Inserted {len(results)} data points for indicator {indicator_code}"
                )
                results = []


def calculate_derived_indicators(**kwargs):
    """Calculate derived indicators for all regions."""
    results = []
    with get_db_connection() as conn, conn.cursor() as cursor:
        source = get_derived_data_source(cursor)

        cursor.execute(
            "SELECT id FROM geo_entities WHERE type='region' OR type='bloc' OR type='continent'"
        )
        regions = cursor.fetchall()

        for derived_code, config in derived_indicators.items():
            derived_indicator = get_indicator(derived_code, cursor)
            if not derived_indicator:
                logging.warning(f"Derived indicator {derived_code} not found")
                continue

            # Get dependency indicator IDs
            dependency_ids = {}
            missing_dependency_defs = False

            for dep_code in config["dependencies"]:
                dep_indicator = get_indicator(dep_code, cursor)
                if not dep_indicator:
                    logging.warning(f"Dependency indicator {dep_code} not found")
                    missing_dependency_defs = True
                    break
                dependency_ids[dep_code] = dep_indicator.id

            if missing_dependency_defs:
                continue

            # Find years where ALL dependencies have data
            dep_id_list = list(dependency_ids.values())
            placeholders = ",".join(["%s"] * len(dep_id_list))

            cursor.execute(
                f"""
                SELECT EXTRACT(YEAR FROM date) as year, COUNT(DISTINCT indicator_id)
                FROM data_points
                WHERE indicator_id IN ({placeholders})
                GROUP BY year
                HAVING COUNT(DISTINCT indicator_id) = %s
                ORDER BY year
                """,
                dep_id_list + [len(dep_id_list)],
            )

            years = [int(row[0]) for row in cursor.fetchall()]

            if not years:
                logging.warning(
                    f"No common years found for all dependencies of {derived_code}"
                )
                continue

            logging.info(
                f"Processing derived indicator {derived_code} for years {min(years)} to {max(years)}"
            )

            # Process each year for this derived indicator
            for year in years:
                for region_id in [r[0] for r in regions]:
                    # Fetch values for all dependencies for this region/year
                    dependency_values = {}
                    missing_data = False

                    for dep_code, dep_id in dependency_ids.items():
                        # Get the value for this specific year
                        query = """
                        SELECT numeric_value
                        FROM data_points
                        WHERE geo_entity_id = %s
                        AND indicator_id = %s
                        AND date BETWEEN %s AND %s
                        ORDER BY date DESC
                        LIMIT 1
                        """

                        cursor.execute(
                            query,
                            (region_id, dep_id, f"{year}-01-01", f"{year}-12-31"),
                        )
                        value = cursor.fetchone()
                        if not value:
                            missing_data = True
                            break

                        dependency_values[dep_code] = value[0]

                    if missing_data:
                        continue

                    # Calculate derived value
                    try:
                        derived_value = config["calculate"](dependency_values)

                        results.append(
                            {
                                "geo_entity_id": region_id,
                                "indicator_id": derived_indicator.id,
                                "date": f"{year}-12-31",
                                "numeric_value": derived_value,
                                "text_value": None,
                                "source_id": source.id,
                            }
                        )
                    except Exception as e:
                        logging.error(
                            f"Error calculating {derived_code} for region {region_id} in year {year}: {str(e)}"
                        )

            # Commit after each indicator
            if results:
                insert_data_points(results, cursor)
                conn.commit()
                logging.info(
                    f"Inserted {len(results)} derived data points for indicator {derived_code}"
                )
                results = []


with DAG(
    "compute_region_indicators_dag",
    default_args=default_args,
    description="Calculate region indicators",
    schedule_interval="@weekly",
    start_date=datetime(2025, 4, 11),
    catchup=False,
    tags=["foundation"],
) as dag:

    t1 = PythonOperator(
        task_id="calculate_base_indicators",
        python_callable=calculate_base_indicators,
    )

    t2 = PythonOperator(
        task_id="calculate_derived_indicators",
        python_callable=calculate_derived_indicators,
    )

    t1 >> t2
