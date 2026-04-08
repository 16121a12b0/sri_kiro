import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def fetch_openflow_jobs(status_filter: list[str] | None = None) -> pd.DataFrame:
    """
    Query OpenFlow job execution history from Snowflake.
    Adjust the table/column names to match your actual OpenFlow metadata schema.
    """
    where_clause = ""
    if status_filter:
        statuses = ", ".join(f"'{s}'" for s in status_filter)
        where_clause = f"WHERE STATUS IN ({statuses})"

    query = f"""
        SELECT
            JOB_ID,
            JOB_NAME,
            STATUS,
            START_TIME,
            END_TIME,
            DURATION_SECONDS,
            ERROR_MESSAGE
        FROM OPENFLOW_JOB_HISTORY
        {where_clause}
        ORDER BY START_TIME DESC
        LIMIT 500
    """

    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    return df


def fetch_job_summary() -> pd.DataFrame:
    """Returns a count of jobs grouped by status."""
    query = """
        SELECT STATUS, COUNT(*) AS JOB_COUNT
        FROM OPENFLOW_JOB_HISTORY
        GROUP BY STATUS
        ORDER BY JOB_COUNT DESC
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df
