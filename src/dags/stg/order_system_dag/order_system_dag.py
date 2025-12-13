import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.order_system_dag.pg_saver_rest import PgSaverRest
from stg.order_system_dag.pg_saver_ord import PgSaverOrd
from stg.order_system_dag.pg_saver_user import PgSaverUser
from stg.order_system_dag.restaurant_loader import RestaurantLoader
from stg.order_system_dag.restaurant_reader import RestaurantReader
from stg.order_system_dag.order_loader import OrderLoader
from stg.order_system_dag.order_reader import OrderReader
from stg.order_system_dag.user_loader import UserLoader
from stg.order_system_dag.user_reader import UserReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin', 'order_system'],
    is_paused_upon_creation=True,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': pendulum.duration(seconds=30),
    }
)
def stg_order_system_dag():
    # Создаем подключение к базе dwh
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="load_restaurants")
    def load_restaurants():
        """Загрузка данных о ресторанах из MongoDB"""
        pg_saver = PgSaverRest()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()
        log.info("Restaurants data loaded successfully")

    @task(task_id="load_orders")
    def load_orders():
        """Загрузка данных о заказах из MongoDB"""
        pg_saver_ord = PgSaverOrd()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = OrderReader(mongo_connect)
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver_ord, log)
        loader.run_copy()
        log.info("Orders data loaded successfully")

    @task(task_id="load_users")
    def load_users():
        """Загрузка данных о пользователях из MongoDB"""
        pg_saver_user = PgSaverUser()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver_user, log)
        loader.run_copy()
        log.info("Users data loaded successfully")
    

    # Инициализируем задачи
    restaurants_task = load_restaurants()
    orders_task = load_orders()
    users_task = load_users()
    
    # Определяем последовательность выполнения
    # Вариант А: Параллельное выполнение
    [restaurants_task, orders_task, users_task]
    
    # Вариант Б: Сначала рестораны, потом заказы (если есть зависимость)
    # restaurants_task >> orders_task

stg_order_system_dag = stg_order_system_dag()