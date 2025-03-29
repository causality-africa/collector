from decimal import Decimal
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from causality.utils.db import get_db_connection
from causality.utils.storage import download_from_backblaze

default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

INDICATOR_MAPPING = {
    "TPopulation1July": {
        "name": "Total Population",
        "code": "total-population",
        "category": "population",
        "description": "Total population as of 1 July",
        "unit": "persons",
    },
    "TPopulationMale1July": {
        "name": "Male Population",
        "code": "male-population",
        "category": "population",
        "description": "Male population as of 1 July",
        "unit": "persons",
    },
    "TPopulationFemale1July": {
        "name": "Female Population",
        "code": "female-population",
        "category": "population",
        "description": "Female population as of 1 July",
        "unit": "persons",
    },
    "PopDensity": {
        "name": "Population Density",
        "code": "population-density",
        "category": "population",
        "description": "Population density as of 1 July",
        "unit": "persons per square km",
    },
    "PopSexRatio": {
        "name": "Population Sex Ratio",
        "code": "sex-ratio",
        "category": "population",
        "description": "Population sex ratio as of 1 July",
        "unit": "males per 100 females",
    },
    "MedianAgePop": {
        "name": "Median Age",
        "code": "median-age",
        "category": "population",
        "description": "Median age as of 1 July",
        "unit": "years",
    },
    "NatChange": {
        "name": "Natural Change",
        "code": "population-natural-change",
        "category": "population",
        "description": "Births minus Deaths",
        "unit": "persons",
    },
    "NatChangeRT": {
        "name": "Rate of Natural Change",
        "code": "population-natural-change-rate",
        "category": "population",
        "description": "Rate of natural population change",
        "unit": "per 1000 population",
    },
    "PopChange": {
        "name": "Population Change",
        "code": "population-change",
        "category": "population",
        "description": "Total change in population",
        "unit": "persons",
    },
    "PopGrowthRate": {
        "name": "Population Growth Rate",
        "code": "population-growth-rate",
        "category": "population",
        "description": "Annual population growth rate",
        "unit": "percentage",
    },
    "DoublingTime": {
        "name": "Population Annual Doubling Time",
        "code": "population-doubling-time",
        "category": "population",
        "description": "Years required for population to double at current growth rate",
        "unit": "years",
    },
    "Births": {
        "name": "Births",
        "code": "births",
        "category": "population",
        "description": "Total annual births",
        "unit": "persons",
    },
    "Births1519": {
        "name": "Births by Women Aged 15-19",
        "code": "births-15-19",
        "category": "population",
        "description": "Annual births to women aged 15 to 19",
        "unit": "persons",
    },
    "CBR": {
        "name": "Crude Birth Rate",
        "code": "birth-rate",
        "category": "population",
        "description": "Annual births per 1000 population",
        "unit": "births per 1000 population",
    },
    "TFR": {
        "name": "Total Fertility Rate",
        "code": "fertility-rate",
        "category": "population",
        "description": "Average number of children a woman would have during reproductive age",
        "unit": "live births per woman",
    },
    "NRR": {
        "name": "Net Reproduction Rate",
        "code": "net-reproduction-rate",
        "category": "population",
        "description": "Average number of daughters a woman would have who survive to reproductive age",
        "unit": "surviving daughters per woman",
    },
    "MAC": {
        "name": "Mean Age at Childbearing",
        "code": "mean-childbearing-age",
        "category": "population",
        "description": "Average age of mothers at birth of their children",
        "unit": "years",
    },
    "SRB": {
        "name": "Sex Ratio at Birth",
        "code": "birth-sex-ratio",
        "category": "population",
        "description": "Ratio of male to female births",
        "unit": "males per 100 female births",
    },
    "Deaths": {
        "name": "Total Deaths",
        "code": "deaths",
        "category": "population",
        "description": "Total annual deaths",
        "unit": "persons",
    },
    "DeathsMale": {
        "name": "Male Deaths",
        "code": "male-deaths",
        "category": "population",
        "description": "Annual male deaths",
        "unit": "persons",
    },
    "DeathsFemale": {
        "name": "Female Deaths",
        "code": "female-deaths",
        "category": "population",
        "description": "Annual female deaths",
        "unit": "persons",
    },
    "CDR": {
        "name": "Crude Death Rate",
        "code": "death-rate",
        "category": "population",
        "description": "Annual deaths per 1000 population",
        "unit": "deaths per 1000 population",
    },
    "LEx": {
        "name": "Life Expectancy at Birth",
        "code": "life-expectancy",
        "category": "population",
        "description": "Average number of years a newborn is expected to live",
        "unit": "years",
    },
    "LExMale": {
        "name": "Male Life Expectancy at Birth",
        "code": "male-life-expectancy",
        "category": "population",
        "description": "Average number of years a newborn male is expected to live",
        "unit": "years",
    },
    "LExFemale": {
        "name": "Female Life Expectancy at Birth",
        "code": "female-life-expectancy",
        "category": "population",
        "description": "Average number of years a newborn female is expected to live",
        "unit": "years",
    },
    "LE15": {
        "name": "Life Expectancy at Age 15",
        "code": "life-expectancy-15",
        "category": "population",
        "description": "Average number of additional years a 15-year-old is expected to live",
        "unit": "years",
    },
    "LE15Male": {
        "name": "Male Life Expectancy at Age 15",
        "code": "male-life-expectancy-15",
        "category": "population",
        "description": "Average number of additional years a 15-year-old male is expected to live",
        "unit": "years",
    },
    "LE15Female": {
        "name": "Female Life Expectancy at Age 15",
        "code": "female-life-expectancy-15",
        "category": "population",
        "description": "Average number of additional years a 15-year-old female is expected to live",
        "unit": "years",
    },
    "LE65": {
        "name": "Life Expectancy at Age 65",
        "code": "life-expectancy-65",
        "category": "population",
        "description": "Average number of additional years a 65-year-old is expected to live",
        "unit": "years",
    },
    "LE65Male": {
        "name": "Male Life Expectancy at Age 65",
        "code": "male-life-expectancy-65",
        "category": "population",
        "description": "Average number of additional years a 65-year-old male is expected to live",
        "unit": "years",
    },
    "LE65Female": {
        "name": "Female Life Expectancy at Age 65",
        "code": "female-life-expectancy-65",
        "category": "population",
        "description": "Average number of additional years a 65-year-old female is expected to live",
        "unit": "years",
    },
    "LE80": {
        "name": "Life Expectancy at Age 80",
        "code": "life-expectancy-80",
        "category": "population",
        "description": "Average number of additional years an 80-year-old is expected to live",
        "unit": "years",
    },
    "LE80Male": {
        "name": "Male Life Expectancy at Age 80",
        "code": "male-life-expectancy-80",
        "category": "population",
        "description": "Average number of additional years an 80-year-old male is expected to live",
        "unit": "years",
    },
    "LE80Female": {
        "name": "Female Life Expectancy at Age 80",
        "code": "female-life-expectancy-80",
        "category": "population",
        "description": "Average number of additional years an 80-year-old female is expected to live",
        "unit": "years",
    },
    "InfantDeaths": {
        "name": "Infant Deaths",
        "code": "infant-deaths",
        "category": "population",
        "description": "Annual deaths of infants under age 1",
        "unit": "persons",
    },
    "IMR": {
        "name": "Infant Mortality Rate",
        "code": "infant-mortality-rate",
        "category": "population",
        "description": "Deaths of infants under age 1 per 1000 live births",
        "unit": "deaths per 1000 live births",
    },
    "LBsurvivingAge1": {
        "name": "Live Births Surviving to Age 1",
        "code": "births-surviving-1",
        "category": "population",
        "description": "Annual births who survive to age 1",
        "unit": "persons",
    },
    "Under5Deaths": {
        "name": "Deaths Under Age 5",
        "code": "under5-deaths",
        "category": "population",
        "description": "Annual deaths of children under age 5",
        "unit": "persons",
    },
    "Q5": {
        "name": "Under-five Mortality Rate",
        "code": "under5-mortality-rate",
        "category": "population",
        "description": "Deaths of children under age 5 per 1000 live births",
        "unit": "deaths per 1000 live births",
    },
    "Q0040": {
        "name": "Mortality Before Age 40",
        "code": "mortality-before-40",
        "category": "population",
        "description": "Probability of dying before age 40 per 1000 live births",
        "unit": "deaths per 1000 live births",
    },
    "Q0040Male": {
        "name": "Male Mortality Before Age 40",
        "code": "male-mortality-before-40",
        "category": "population",
        "description": "Probability of male dying before age 40 per 1000 male live births",
        "unit": "deaths per 1000 male live births",
    },
    "Q0040Female": {
        "name": "Female Mortality Before Age 40",
        "code": "female-mortality-before-40",
        "category": "population",
        "description": "Probability of female dying before age 40 per 1000 female live births",
        "unit": "deaths per 1000 female live births",
    },
    "Q0060": {
        "name": "Mortality Before Age 60",
        "code": "mortality-before-60",
        "category": "population",
        "description": "Probability of dying before age 60 per 1000 live births",
        "unit": "deaths per 1000 live births",
    },
    "Q0060Male": {
        "name": "Male Mortality Before Age 60",
        "code": "male-mortality-before-60",
        "category": "population",
        "description": "Probability of male dying before age 60 per 1000 male live births",
        "unit": "deaths per 1000 male live births",
    },
    "Q0060Female": {
        "name": "Female Mortality Before Age 60",
        "code": "female-mortality-before-60",
        "category": "population",
        "description": "Probability of female dying before age 60 per 1000 female live births",
        "unit": "deaths per 1000 female live births",
    },
    "Q1550": {
        "name": "Mortality Between Age 15 and 50",
        "code": "mortality-15-50",
        "category": "population",
        "description": "Probability of dying between ages 15 and 50 per 1000 alive at age 15",
        "unit": "deaths per 1000 alive at age 15",
    },
    "Q1550Male": {
        "name": "Male Mortality Between Age 15 and 50",
        "code": "male-mortality-15-50",
        "category": "population",
        "description": "Probability of male dying between ages 15 and 50 per 1000 males alive at age 15",
        "unit": "deaths per 1000 males alive at age 15",
    },
    "Q1550Female": {
        "name": "Female Mortality Between Age 15 and 50",
        "code": "female-mortality-15-50",
        "category": "population",
        "description": "Probability of female dying between ages 15 and 50 per 1000 females alive at age 15",
        "unit": "deaths per 1000 females alive at age 15",
    },
    "Q1560": {
        "name": "Mortality Between Age 15 and 60",
        "code": "mortality-15-60",
        "category": "population",
        "description": "Probability of dying between ages 15 and 60 per 1000 alive at age 15",
        "unit": "deaths per 1000 alive at age 15",
    },
    "Q1560Male": {
        "name": "Male Mortality Between Age 15 and 60",
        "code": "male-mortality-15-60",
        "category": "population",
        "description": "Probability of male dying between ages 15 and 60 per 1000 males alive at age 15",
        "unit": "deaths per 1000 males alive at age 15",
    },
    "Q1560Female": {
        "name": "Female Mortality Between Age 15 and 60",
        "code": "female-mortality-15-60",
        "category": "population",
        "description": "Probability of female dying between ages 15 and 60 per 1000 females alive at age 15",
        "unit": "deaths per 1000 females alive at age 15",
    },
    "NetMigrations": {
        "name": "Net Number of Migrants",
        "code": "net-migrants",
        "category": "population",
        "description": "Net number of migrants (immigrants minus emigrants)",
        "unit": "persons",
    },
    "CNMR": {
        "name": "Net Migration Rate",
        "code": "net-migration-rate",
        "category": "population",
        "description": "Net migration per 1000 population",
        "unit": "per 1000 population",
    },
}

IN_THOUSANDS = {
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
}


def load_wpp_indicators():
    """Create indicators for WPP data."""
    with get_db_connection() as conn:
        # Create data source for WPP
        with conn.cursor() as cur:
            source_query = """
                INSERT INTO data_sources (name, url, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    description = EXCLUDED.description
                RETURNING id
            """

            cur.execute(
                source_query,
                (
                    "UN World Population Prospects",
                    "https://population.un.org/wpp/",
                    "United Nations population estimates and projections",
                ),
            )
            source_id = cur.fetchone()[0]

            # Create indicators
            for metadata in INDICATOR_MAPPING.values():
                indicator_query = """
                    INSERT INTO indicators (name, code, category, description, unit, data_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        description = EXCLUDED.description,
                        unit = EXCLUDED.unit,
                        data_type = EXCLUDED.data_type
                    RETURNING id
                """

                cur.execute(
                    indicator_query,
                    (
                        metadata["name"],
                        metadata["code"],
                        metadata["category"],
                        metadata["description"],
                        metadata["unit"],
                        "numeric",
                    ),
                )

            conn.commit()

    return source_id


def load_wpp_data(**context):
    """Load WPP data into the database."""
    # Get indicator mapping and source ID from previous task
    ti = context["ti"]
    source_id = ti.xcom_pull(task_ids="load_wpp_indicators")

    # Load the WPP data
    wpp_file = download_from_backblaze(
        "causality-africa",
        "demographics/WPP2024_Demographic_Indicators_Medium.csv",
    )

    wpp_data = pd.read_csv(wpp_file, delimiter=",")

    with get_db_connection() as conn:
        # Process each row in the WPP data
        with conn.cursor() as cur:
            for i, row in wpp_data.iterrows():
                if not pd.notna(row["ISO2_code"]):
                    continue

                # Get location ID for this country
                loc_query = "SELECT id FROM locations WHERE code = %s"
                cur.execute(loc_query, (row["ISO2_code"],))
                loc_record = cur.fetchone()

                if loc_record:
                    location_id = loc_record[0]

                    # Process each indicator
                    year = int(row["Time"])
                    date_str = f"{year}-07-01"  # July 1st for mid-year data

                    for wpp_col, metadata in INDICATOR_MAPPING.items():
                        if pd.notna(row.get(wpp_col)):
                            # Get indicator ID
                            indicator = metadata["code"]
                            ind_query = "SELECT id FROM indicators WHERE code = %s"
                            cur.execute(ind_query, (indicator,))
                            indicator_id = cur.fetchone()[0]

                            # Insert the data point
                            data_query = """
                                INSERT INTO data_points (
                                    entity_type, entity_id, indicator_id, source_id,
                                    date, numeric_value, text_value
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (entity_type, entity_id, indicator_id, source_id, date)
                                DO UPDATE SET numeric_value = EXCLUDED.numeric_value
                            """

                            value = Decimal(row[wpp_col])
                            if indicator in IN_THOUSANDS:
                                value *= 1_000

                            cur.execute(
                                data_query,
                                (
                                    "location",
                                    location_id,
                                    indicator_id,
                                    source_id,
                                    date_str,
                                    value,
                                    None,
                                ),
                            )

                # Commit after processing a batch of records
                if i % 100 == 0:
                    conn.commit()

            conn.commit()


with DAG(
    "load_wpp_data_dag",
    default_args=default_args,
    description="Load WPP demographic data into database",
    schedule_interval="@once",
    start_date=datetime(2025, 3, 29),
    tags=["demographics"],
) as dag:
    create_indicators_task = PythonOperator(
        task_id="load_wpp_indicators",
        python_callable=load_wpp_indicators,
    )

    load_data_task = PythonOperator(
        task_id="load_wpp_data",
        python_callable=load_wpp_data,
        provide_context=True,
    )

    create_indicators_task >> load_data_task
