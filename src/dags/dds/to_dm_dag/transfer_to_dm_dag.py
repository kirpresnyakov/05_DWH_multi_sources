import logging

import pendulum
from airflow.decorators import dag, task
from dds.to_dm_dag.dm_user_loader import UsersLoader
from dds.to_dm_dag.dm_restaurant_loader import RestaurantsLoader
from dds.to_dm_dag.timestamp_loader import TimestampsLoader
from dds.to_dm_dag.order_loader import OrdersLoader
from dds.to_dm_dag.product_loader import ProductLoader
from dds.to_dm_dag.product_loader import ProductLoader
from dds.to_dm_dag.product_sales_loader import ProductSalesLoader
from dds.to_dm_dag.couriers_loader import CouriersLoader
from dds.to_dm_dag.deliveries_loader import DeliveriesLoader
from dds.to_dm_dag.courier_deliveries_loader import CourierDeliveriesLoader
from dds.to_dm_dag.dds_settings_repository import DdsEtlSettingsRepository
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага
    catchup=False,  # Не запускать даг за предыдущие периоды
    tags=['sprint5', 'dds', 'users', 'stg_to_dds'],  # Теги для фильтрации
    is_paused_upon_creation=True  # Остановлен при создании
)

# DAG для загрузки данных пользователей из STG слоя в DDS слой. 
# Извлекает данные из stg.ordersystem_users и загружает в dds.dm_users
def transfer_to_dds_dag():
    
    # Создаем подключение к DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    
    @task(task_id="users_load") # Таск, для загрузки данных пользователей
    def load_users():
        """
        Таск для загрузки данных пользователей из STG в DDS
        """
        log.info("Начало загрузки данных пользователей из STG в DDS")
        
        try:
            # Создаем экземпляр класса UsersLoader, передавая подключение
            users_loader = UsersLoader(dwh_pg_connect, log)
            
            # Вызываем метод загрузки
            users_loader.execute_loading()
            
            log.info("Загрузка данных пользователей успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке данных пользователей: {e}")
            raise
    
    
    @task(task_id="restaurants_load") # Таск, для загрузки данных ресторанов из STG в DDS
    def load_restaurants():
        log.info("Начало загрузки данных ресторанов из STG в DDS")
        
        try:
            # Создаем экземпляр класса RestaurantsLoader, передавая подключение
            restaurants_loader = RestaurantsLoader(dwh_pg_connect, log)
            
            # Вызываем метод загрузки
            restaurants_loader.execute_loading()
            
            log.info("Загрузка данных ресторанов успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке данных ресторанов: {e}")
            raise

    @task(task_id="timestamps_load") #Таск для загрузки временных меток
    def load_timestamps_task():
        log.info("Начало загрузки временных меток в DDS")
        
        try:
        # Создаем экземпляр класса TimestampsLoader
            timestamp_loader = TimestampsLoader(
                pg_origin=dwh_pg_connect,  # Для чтения из STG
                pg_dest=dwh_pg_connect,    # Для записи в DDS (можно тот же коннект)
                log=log                     # Логгер
            )
        
            # Выполнение загрузки
            timestamp_loader.load_data()
            
            log.info("Загрузка временных меток успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке временных меток: {e}")
            raise
    
    @task(task_id="load_orders") # Загрузка заказов из STG в DDS
    def load_orders():
        log.info("Начало загрузки заказов")
        
        try:
            # Создаем подключения к источнику и целевому хранилищу
            dwh_connection = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
            
            # Создаем подключения для источника и приемника
            pg_origin = dwh_connection
            pg_dest = dwh_connection
            
            # Создаем лоадер заказов
            orders_loader = OrdersLoader(
                pg_origin=pg_origin,
                pg_dest=pg_dest,
                log=log
            )
            
            # Выполняем загрузку
            orders_loader.load_data()
            
            log.info("Загрузка заказов успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке заказов: {e}")
            log.exception(e)
            raise
            
    @task(task_id ="load_product") # Загрузка продуктов из STG в DDS
    def load_products():
        log.info("Начало загрузки продуктов")
        
        try:
            # Создаем подключение
            dwh = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
            
            # Создаем репозиторий настроек
            settings_repository = DdsEtlSettingsRepository()
            
            # Создаем лоадер продуктов
            product_loader = ProductLoader(
                pg=dwh,
                settings_repository=settings_repository
            )
            
            # Выполняем загрузку
            product_loader.load_products()
            
            log.info("Загрузка продуктов успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке продуктов: {e}")
            log.exception(e)
            raise

    @task(task_id="load_product_sales") # Загрузка фактов продаж продуктов из STG в DDS
    def load_product_sales():
        log.info("Начало загрузки фактов продаж продуктов")
        
        try:
            # Создаем подключение к DWH
            dwh_connection = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
            
            # Создаем подключения для источника (STG) и приемника (DDS)
            pg_origin = dwh_connection
            pg_dest = dwh_connection
            
            # Создаем лоадер фактов продаж
            product_sales_loader = ProductSalesLoader(
                pg_origin=pg_origin,
                pg_dest=pg_dest,
                log=log
            )
            
            # Выполняем загрузку
            log.info("Запуск процесса загрузки фактов продаж...")
            product_sales_loader.load_data()
            
            log.info("Загрузка фактов продаж успешно завершена")
            
        except Exception as e:
            log.error(f"Ошибка при загрузке фактов продаж: {e}")
            log.exception(e)
            raise
    
    @task(task_id="couriers_load_task") #  Загрузка курьеров из STG в DDS
    def load_couriers_task():
        log.info("Начало загрузки курьеров")
        couriers_loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        couriers_loader.load_data()
        log.info("Загрузка курьеров завершена")

    @task(task_id="deliveries_load_task") # Загрузка доставок из STG в DDS
    def load_deliveries_task(): 
        log.info("Начало загрузки доставок")
        deliveries_loader = DeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        deliveries_loader.load_data()
        log.info("Загрузка доставок завершена")

    @task(task_id="couriers_deliveries_load_task") # Загрузка связей курьеров и доставок из STG в DDS
    def load_courier_deliveries_task():
        log.info("Начало загрузки связей курьеров и доставок")
        courier_deliveries_loader = CourierDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        courier_deliveries_loader.load_data()
        log.info("Загрузка связей курьеров и доставок завершена")

    # Инициализируем таски
    users_task = load_users()
    restaurants_task = load_restaurants()
    timestamps_task = load_timestamps_task()
    order_task = load_orders()
    product_task = load_products()
    product_sales = load_product_sales()
    couriers_task = load_couriers_task()
    deliveries_task = load_deliveries_task()
    courier_deliveries_task = load_courier_deliveries_task()
    

    [users_task, restaurants_task, timestamps_task]>>order_task >>product_task>>product_sales >> [deliveries_task, couriers_task] >> courier_deliveries_task
    
# Инициализируем DAG
dds_users_dag = transfer_to_dds_dag()
