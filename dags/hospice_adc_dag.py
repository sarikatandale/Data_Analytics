from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_TABLE = "ADC_HOSPICE_ANALYTICS.RAW.PATIENT_CENSUS"
SOURCE_FILE = "/opt/airflow/data/patient_census.csv"

default_args = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="patient_census_load",
    description="Generate and load hospice patient census into Snowflake",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["hospice", "adc"],
):

    logger = LoggingMixin().log

    # Validating that CSV file exists
    @task
    def extract_patient_data():
        file_path = Path(SOURCE_FILE)

        if not file_path.exists():
            raise AirflowFailException(
                f"Source file not found: {file_path}"
            )

        logger.info("Found source file: %s", file_path)
        return str(file_path)
    
    # Loading data to Snowflake
    @task
    def load_to_snowflake(file_path: str):
        logger.info("Loading patient census data from %s", file_path)
    
        df = pd.read_csv(file_path)
        df["load_date"] = pd.Timestamp.utcnow()

        # Convert datetime columns to string and replace missing with None
        for col in ["admit_date", "discharge_date", "load_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')  # convert invalid to NaT
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")  # format
                df[col] = df[col].where(df[col].notna(), None)       # NaT -> None

        # Convert to list of tuples
        records = list(df.itertuples(index=False, name=None))

        insert_sql = f"""
            INSERT INTO {TARGET_TABLE}
            (patient_id, hospice_id, admit_date, discharge_date, load_date)
            VALUES (%s, %s, %s, %s, %s)
        """

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, records)

        logger.info("Successfully loaded %d records into Snowflake", len(records))



    # DAG Flow
    file_path = extract_patient_data()
    load_to_snowflake(file_path)
    
