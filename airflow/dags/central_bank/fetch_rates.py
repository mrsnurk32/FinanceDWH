#Airflow imports

from airflow.decorators import task
from airflow.models.dag import DAG

#Datetime imports
from datetime import datetime
from datetime import timedelta
from datetime import date

#Utils
from utils.utils.fetch_last_rates_dt import fetch_last_rates_dt
from utils.clickhouse.clickhouse import get_clickhouse_client
import logging
from utils.rest_api.fetch_central_bank_rates import fetch_data


logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2021, 1, 1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_central_bank_rates",
    default_args=default_args,
    description="Fetch central bank rates from MOEX once a day",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:


    def fetch_last_bank_record():
        client = get_clickhouse_client()
        return fetch_last_rates_dt(client)

    @task(retries=0)
    def fetch_bank_rates_task(last_record_dt:datetime):
        """
        Fetch candle data for a single security.
        Replace the pass statement with API call logic.
        """
        client = get_clickhouse_client()
        # logger.info("fetch_candles_task: client initiated", extra={"security": security, 'last_record': last_record_dt})

        if last_record_dt:
            start_interval = max(DEFAULT_START_DATE, last_record_dt.date())
        else:
            start_interval = DEFAULT_START_DATE

        end_interval = datetime.today()

        start_interval = start_interval.strftime('%Y')
        end_interval = end_interval.strftime('%Y')
        all_rows , columns = fetch_data(start_interval, end_interval)
        
        client.insert(
            'centralbank_rates_ru',
            all_rows,
            column_names=columns
        )

    last_record_dt = fetch_last_bank_record()
    fetch_bank_rates_task(last_record_dt)