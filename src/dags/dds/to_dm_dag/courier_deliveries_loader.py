import logging
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel, Field
from lib import PgConnect
import psycopg

logger = logging.getLogger(__name__)


class CourierDeliveryObj(BaseModel):
    id: int = Field(alias="id")
    delivery_id: str = Field(alias="delivery_id")
    courier_id: Optional[str] = Field(alias="courier_id", default=None)  # courier_id из dds.dm_couriers
    dds_courier_id: Optional[str] = Field(alias="dds_courier_id", default=None)  # ID курьера из DDS
    rate: int = Field(alias="rate")
    delivery_sum: float = Field(alias="delivery_sum")
    tip_sum: float = Field(alias="tip_sum")


class StgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, last_loaded: int, batch_limit: int) -> List[CourierDeliveryObj]:
        """Получаем данные из STG с JOIN к DDS таблицам для получения ключей"""
        with self._db.client() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT 
                        t.id,
                        t.delivery_id,
                        t.courier_id,
                        dc.id as dds_courier_id,  -- ID из dds.dm_couriers
                        t.rate,
                        t.delivery_sum,
                        t.tip_sum
                    FROM stg.deliverysystem_deliveries t
                    LEFT JOIN dds.dm_couriers dc ON dc.courier_id = t.courier_id
                    WHERE t.id > %(last_loaded)s 
                    ORDER BY t.id ASC 
                    LIMIT %(batch_limit)s;
                    """,
                    {
                        "last_loaded": last_loaded,
                        "batch_limit": batch_limit
                    }
                )
                
                objs = []
                rows = cur.fetchall()
                logger.info(f"Fetched {len(rows)} rows from stg.deliverysystem_deliveries")
                
                for row in rows:
                    try:
                        # Создаем объект Pydantic
                        obj = CourierDeliveryObj(**row)
                        objs.append(obj)
                    except Exception as e:
                        logger.error(f"Error creating object from row {row}: {e}")
                        continue
                        
                return objs


class DdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_last_loaded_id(self) -> int:
        """Получаем последний загруженный ID из таблицы настроек"""
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT workflow_settings->>'last_loaded_id' 
                    FROM dds.srv_wf_settings 
                    WHERE workflow_key = 'courier_deliveries_stg_to_dds_workflow';
                    """
                )
                result = cur.fetchone()
                if result and result[0]:
                    return int(result[0])
                return -1

    def save_courier_delivery(self, delivery_obj: CourierDeliveryObj) -> None:
        """Сохраняем доставку в фактовую таблицу"""
        with self._db.client() as conn:
            with conn.cursor() as cur:
                # Сначала получаем delivery_id из dds.dm_deliveries
                cur.execute(
                    """
                    SELECT id FROM dds.dm_deliveries 
                    WHERE delivery_id = %(delivery_id)s;
                    """,
                    {"delivery_id": delivery_obj.delivery_id}
                )
                delivery_result = cur.fetchone()
                
                if not delivery_result:
                    logger.warning(f"No delivery_id found in dds.dm_deliveries for {delivery_obj.delivery_id}")
                    return
                    
                dds_delivery_id = delivery_result[0]
                
                # Вставляем в фактовую таблицу
                cur.execute(
                    """
                    INSERT INTO dds.fct_courier_deliveries(
                        courier_id, 
                        delivery_id, 
                        rate, 
                        delivery_sum, 
                        tip_sum
                    )
                    VALUES (
                        %(courier_id)s,
                        %(delivery_id)s,
                        %(rate)s,
                        %(delivery_sum)s,
                        %(tip_sum)s
                    )
                    ON CONFLICT (courier_id, delivery_id) DO UPDATE SET
                        rate = EXCLUDED.rate,
                        delivery_sum = EXCLUDED.delivery_sum,
                        tip_sum = EXCLUDED.tip_sum;
                    """,
                    {
                        "courier_id": delivery_obj.dds_courier_id,
                        "delivery_id": dds_delivery_id,
                        "rate": delivery_obj.rate,
                        "delivery_sum": delivery_obj.delivery_sum,
                        "tip_sum": delivery_obj.tip_sum
                    }
                )
                conn.commit()

    def update_last_loaded(self, last_id: int) -> None:
        """Обновляем последний загруженный ID"""
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.srv_wf_settings (
                        workflow_key, 
                        workflow_settings
                    )
                    VALUES (
                        'courier_deliveries_stg_to_dds_workflow',
                        jsonb_build_object('last_loaded_id', %(last_id)s)
                    )
                    ON CONFLICT (workflow_key) DO UPDATE SET
                        workflow_settings = jsonb_build_object('last_loaded_id', %(last_id)s);
                    """,
                    {"last_id": last_id}
                )
                conn.commit()


class CourierDeliveriesLoader:
    WF_KEY = "courier_deliveries_stg_to_dds_workflow"
    BATCH_LIMIT = 2000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: logging.Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.log = log
        self.stg = StgRepository(pg_origin)
        self.dds = DdsRepository(pg_dest)

    def load_data(self) -> None:
        # Получаем последний загруженный ID
        last_loaded = self.dds.get_last_loaded_id()
        self.log.info(f"Starting courier deliveries load from id: {last_loaded}")

        # Загружаем данные пачками
        while True:
            load_queue = self.stg.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to process")
            
            if not load_queue:
                self.log.info("No more records to load")
                break

            processed_count = 0
            skipped_count = 0
            
            for delivery_obj in load_queue:
                # Пропускаем записи без курьера в DDS
                if not delivery_obj.dds_courier_id:
                    self.log.warning(f"Skipping delivery_id={delivery_obj.delivery_id}, no courier in DDS")
                    skipped_count += 1
                    continue
                    
                try:
                    # Загружаем в DDS
                    self.dds.save_courier_delivery(delivery_obj)
                    processed_count += 1
                    
                    # Обновляем последний обработанный ID
                    last_loaded = delivery_obj.id
                    
                except Exception as e:
                    self.log.error(f"Error loading delivery_id={delivery_obj.delivery_id}: {e}")
                    # Можно продолжить обработку следующих записей
                    continue

            # Сохраняем прогресс
            self.dds.update_last_loaded(last_loaded)
            
            self.log.info(f"Batch processed: {processed_count} loaded, {skipped_count} skipped")
            
            # Если загрузили меньше чем BATCH_LIMIT, значит это последняя пачка
            if len(load_queue) < self.BATCH_LIMIT:
                break

        self.log.info(f"Courier deliveries load completed. Last loaded id: {last_loaded}")