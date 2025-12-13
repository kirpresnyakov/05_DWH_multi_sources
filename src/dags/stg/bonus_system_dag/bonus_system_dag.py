import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_dag.ranks_loader import RankLoader
from stg.bonus_system_dag.users_loader import UserLoader
from stg.bonus_system_dag.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды - False.
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги для фильтрации.
    is_paused_upon_creation=True  # Остановлен при создании.
)
def stg_bonus_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные по ranks.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные по users.
    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)    
        user_loader.load_users() # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные по events.
    @task(task_id="events_load")
    def load_events():
        # Создаем экземпляр класса EventLoader
        events_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        events_loader.load_events()  # Вызываем функцию для загрузки данных 

    # Инициализируем объявленные таски
    ranks_task = load_ranks()
    users_task = load_users()
    events_task = load_events()

    [ranks_task, users_task, events_task]
       
stg_bonus_system_dag = stg_bonus_system_dag()