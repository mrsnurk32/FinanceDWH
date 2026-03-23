#Airflow imports
from airflow.decorators import task
from airflow.models.dag import DAG

#Utils
from datetime import datetime
from datetime import timedelta
from datetime import date

from utils.utils.fetch_last_securtity_records import fetch_last_security_records
from utils.clickhouse.clickhouse import get_clickhouse_client
from utils.rest_api.fetch_devidends import fetch_moex_dividends
import logging
import time

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_dividends",
    default_args=default_args,
    description="Fetch security dividends from MOEX once a day",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    @task(retries=0)
    def get_securities():
        client = get_clickhouse_client()
        securities = fetch_last_security_records(client)

        # convert dict -> list of dicts for mapping
        return [
            {"security": sec, "last_record_dt": dt}
            for sec, dt in securities.items()
        ]

    @task(
        retries=5
        , retry_delay=timedelta(minutes=2)
        , map_index_template="fetch_dividends_task_{{ security }}")
    def fetch_dividends_task(security: str, last_record_dt:datetime):
        """
        Fetch candle data for a single security.
        Replace the pass statement with API call logic.
        """
        time.sleep(.5)
        client = get_clickhouse_client()
        logger.info("fetch_dividends_task: client initiated", extra={"security": security, 'last_record': last_record_dt})

        rows, columns = fetch_moex_dividends(security)
        if len(rows) == 0:
            return None
        
        client.insert(
            'dividends',
            rows,
            column_names=columns
        )
        logger.info(f'{security} - data saved')

    securities = get_securities()
    mapped = fetch_dividends_task.expand_kwargs(securities)

    securities >> mapped