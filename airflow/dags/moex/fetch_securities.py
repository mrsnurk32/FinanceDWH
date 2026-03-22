#Airflow imports

from airflow.sdk import task
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
from utils.rest_api.fetch_securities import fetch_moex_securities
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
    "fetch_securities",
    default_args=default_args,
    description="Fetch security data from MOEX once a day",
    schedule=None,
    catchup=False,
) as dag:

    @task(retries=0)
    def fetch_securities(client):
        logger.info("fetch_securities_task: client initiated", extra={})
        rows, columns = fetch_moex_securities()
        
        logger.info("fetch_securities_task: data fetched", 
                    extra={
                        'rows': len(rows), 
                        'columns': len(columns)
                    })

        client.insert(
            'securities',
            rows,
            column_names=columns
        )


    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST"),
        port=8123,
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database="source"
    )


    task_instance = fetch_securities.override(
            task_id=f"fetch_securities",
            pool="moex-fetch-candels-pool"
        )(client)
        