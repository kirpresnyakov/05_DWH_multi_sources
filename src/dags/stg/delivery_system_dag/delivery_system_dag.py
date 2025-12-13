from airflow.decorators import dag, task
import logging
from lib import ConnectionBuilder
import pendulum
from stg.delivery_system_dag.couriers_loader import CouriersLader
from stg.delivery_system_dag.deliveries_loader import DeliveriesLader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='4/15 * * * *',
    start_date=pendulum.datetime(2023, 1, 22, tz="UTC"),
    catchup=False,
    tags=['project5', 'stg', 'origin'],
    is_paused_upon_creation=True
)

def stg_delivery_system_dag():

    # Создаю подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def  loader_task_couriers(task_id="laden_couriers_id"):
        url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
        couriers_lader = CouriersLader(url, dwh_pg_connect, log)
        couriers_lader.laden_couriers()  # Вызываю функцию, которая перельет данные.

    @task()
    def loader_task_deliveries(task_id="load_deliveries_id"):
        url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'
        couriers_lader = DeliveriesLader(url, dwh_pg_connect, log)
        couriers_lader.laden_deliveries()  # Вызываю функцию, которая перельет данные.

    # Инициализирую объявленные таски.
    res_load_couriers_aufgabe = loader_task_couriers()
    res_load_deliveries_aufgabe = loader_task_deliveries()

    [res_load_couriers_aufgabe, res_load_deliveries_aufgabe]

project_5_dag = stg_delivery_system_dag()