import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder

log = logging.getLogger(__name__)

# Импортируем здесь, чтобы не было циклических импортов
try:
    from cdm.settlement_loader import SettlementLoader
    from cdm.courier_ledger_loader import CourierLedgerLoader
except ImportError:
    # Альтернативный импорт, если структура проекта отличается
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from settlement_loader import SettlementLoader
    from cdm.courier_ledger_loader import CourierLedgerLoader


@dag(
    schedule_interval='0 0 * * *',  # Ежедневно в полночь
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['settlement', 'report', 'cdm', 'loading', 'finance'],
    is_paused_upon_creation=True,
    default_args={
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5)
    }
)
def from_dds_to_cdm_dag ():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="settlement_load_task")
    def load_settlement_task():
        rest_loader = SettlementLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="courier_ledger_load_task")
    def load_courier_ledger_task():
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    load_task = load_settlement_task()
    load_courier_ledger = load_courier_ledger_task()

    load_task >> load_courier_ledger


# Запускаем DAG
from_dds_to_cdm_dag = from_dds_to_cdm_dag ()