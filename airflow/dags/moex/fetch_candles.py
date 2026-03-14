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

from plugins.utils.generate_ranges import generate_fetch_ranges
from plugins.utils.fetch_last_securtity_records import fetch_last_security_records, get_last_record_for_secid
from plugins.rest_api.fetch_moex_candles import fetch_moex_candles
import logging


import clickhouse_connect


logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2021, 1, 1)

# List of securities
securities = ['SBER', 'GAZP', 'LKOH']  # example, can add 30 symbols

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
) as dag:

    @task(retries=0)
    def fetch_candles_task(security: str,client, last_record_dt:datetime):
        """
        Fetch candle data for a single security.
        Replace the pass statement with API call logic.
        """
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
            data, columns = fetch_moex_candles(security, interval["start_date"], interval["end_date"])
            logger.info('fetch_candles_task: fetched data', extra={"security": security, "interval": interval})

            all_rows.extend(data)
        
        client.insert(
            'market_data',
            all_rows,
            column_names=columns
        )
        logger.info(f'{security} - data saved')


    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST"),
        port=8123,
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database="source"
    )

    last_records = fetch_last_security_records(client)

    # Dynamically create tasks for each security
    for security in securities:
        logger.info(f'Security {security} before start')
        last_record_dt = get_last_record_for_secid(security, last_records)
        task_instance = fetch_candles_task.override(
            task_id=f"fetch_candles_{security}",
            pool="moex-fetch-candels-pool"
        )(security,client, last_record_dt)


