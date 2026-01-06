from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@task
def load_to_snowflake(records):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    sql = """
    INSERT INTO ADC_HOSPICE_ANALYTICS.RAW.PATIENT_CENSUS
    (patient_id, hospice_id, admit_date, discharge_date, load_date)
    VALUES (%s, %s, %s, %s, %s)
    """

    # ✅ Convert list of dicts → list of tuples
    rows = [
        (
            r["patient_id"],
            r["hospice_id"],
            r["admit_date"],
            r["discharge_date"],
            r["load_date"],
        )
        for r in records
    ]

    hook.run(sql, parameters=rows)
