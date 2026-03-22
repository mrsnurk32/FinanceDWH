#Airflow imports

from airflow.decorators import task
from airflow.models.dag import DAG

#Datetime imports
from datetime import datetime
from datetime import timedelta
from datetime import date

#OS imports
import os

#Utils
import requests
import pandas as pd

from utils.utils.generate_ranges import generate_fetch_ranges
from utils.utils.fetch_last_securtity_records import fetch_last_security_records
from utils.clickhouse.clickhouse import get_clickhouse_client
from utils.rest_api.fetch_moex_candles import fetch_moex_candles
from utils.session import get_http_session
import logging


logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2021, 1, 1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_candles",
    default_args=default_args,
    description="Fetch security candles from MOEX once a day",
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
        retries=0
        , map_index_template="fetch_candles_task_{{ security }}")
    def fetch_candles_task(security: str, last_record_dt:datetime):
        """
        Fetch candle data for a single security.
        Replace the pass statement with API call logic.
        """
        client = get_clickhouse_client()
        session = get_http_session()
        logger.info("fetch_candles_task: client initiated", extra={"security": security, 'last_record': last_record_dt})

        if last_record_dt:
            start_interval = max(DEFAULT_START_DATE, last_record_dt.date())
        else:
            start_interval = DEFAULT_START_DATE

        ranges = generate_fetch_ranges(start_interval, date.today())
        logger.info(f'Ranges generated: {len(ranges)}', extra={"security": security, "start_interval": start_interval})
        if len(ranges) == 0:
            logger.info("Up to date", extra={"security": security, "start_interval": start_interval})
            return 

        all_rows = []

        for interval in ranges:
            data, columns = fetch_moex_candles(session, security, interval["start_date"], interval["end_date"])
            logger.info('fetch_candles_task: fetched data', extra={"security": security, "interval": interval})

            all_rows.extend(data)
        
        client.insert(
            'market_data',
            all_rows,
            column_names=columns
        )
        logger.info(f'{security} - data saved')


    securities = get_securities()

    mapped = fetch_candles_task.expand_kwargs(securities)

    securities >> mapped








    # Dynamically create tasks for each security
    # for security, last_record_datetime in securities.items():
    #     logger.info(f'Security {security} before start')
    #     task_instance = fetch_candles_task.override(
    #         task_id=f"fetch_candles_{security}",
    #         pool="moex-fetch-candels-pool"
    #     )(security,client, last_record_datetime)


