import json
from datetime import datetime
from typing import Dict, List, Optional
import logging

from lib import PgConnect


class RestaurantsLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "stg_to_dds_restaurants_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    
    # Инициализация загрузчика ресторанов из STG в DDS
    def __init__(self, pg_connect: PgConnect, logger: Optional[logging.Logger] = None) -> None:
        self.pg_connect = pg_connect #объект PgConnect для подключения к базе данных
        self.log = logger or logging.getLogger(__name__) # логгер для записи логов
        
    # Преобразование строки JSON в словарь Python
    def str2json(self, json_str: str) -> Dict: 
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            self.log.error(f"Ошибка декодирования JSON: {e}")
            return {}
            
   # Получение настроек workflow из таблицы dds.srv_wf_settings
    def _get_wf_setting(self, cursor) -> Dict: 
        query = """
            SELECT workflow_key, workflow_settings
            FROM dds.srv_wf_settings
            WHERE workflow_key = %s
        """
        
        try:
            cursor.execute(query, (self.WF_KEY,))
            result = cursor.fetchone()
            
            if result:
                workflow_settings = result[1]  # workflow_settings
                if isinstance(workflow_settings, str):
                    workflow_settings = json.loads(workflow_settings)
                return {
                    'workflow_key': result[0],
                    'workflow_settings': workflow_settings
                }
            else:
                # Начальные настройки, если запись не найдена
                return {
                    'workflow_key': self.WF_KEY,
                    'workflow_settings': {
                        self.LAST_LOADED_ID_KEY: 0,
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                }
                
        except Exception as e:
            self.log.error(f"Ошибка при получении настроек workflow: {e}")
            raise
            
    # Сохранение настроек workflow в таблицу dds.srv_wf_settings
    def _save_wf_setting(self, cursor, last_loaded_id: int, last_loaded_ts: datetime) -> None:
        workflow_settings = {
            self.LAST_LOADED_ID_KEY: last_loaded_id,
            self.LAST_LOADED_TS_KEY: last_loaded_ts.isoformat() if isinstance(last_loaded_ts, datetime) else last_loaded_ts
        }
        
        query = """
            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            VALUES (%s, %s)
            ON CONFLICT (workflow_key) DO UPDATE SET
                workflow_settings = EXCLUDED.workflow_settings
        """
        
        try:
            cursor.execute(query, (self.WF_KEY, json.dumps(workflow_settings)))
            self.log.info(f"Сохранены настройки workflow: {workflow_settings}")
        except Exception as e:
            self.log.error(f"Ошибка при сохранении настроек workflow: {e}")
            raise
            
    # Извлечение данных из STG слоя с учетом последней загрузки
    def extract_from_stg(self, cursor, last_loaded_ts: datetime, limit: int = None) -> List[Dict]:
        query = """
            SELECT id, object_id, object_value, update_ts
            FROM stg.ordersystem_restaurants
            WHERE update_ts > %s
            ORDER BY update_ts, id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            cursor.execute(query, (last_loaded_ts,))
            rows = cursor.fetchall()
            
            restaurants_data = []
            for row in rows:
                json_data = self.str2json(row[2])
                
                if json_data:
                    restaurant_data = {
                        'id': row[0],
                        'object_id': row[1],
                        'json_data': json_data,
                        'update_ts': row[3]
                    }
                    restaurants_data.append(restaurant_data)
            
            self.log.info(f"Извлечено {len(restaurants_data)} записей из stg.ordersystem_restaurants начиная с {last_loaded_ts}")
            return restaurants_data
            
        except Exception as e:
            self.log.error(f"Ошибка при извлечении данных из STG: {e}")
            return []
            
    # Преобразование данных из STG формата в DDS формат
    def transform_data(self, stg_data: List[Dict]) -> List[Dict]:
        transformed_data = [] # Извлекаем информацию о ресторане из поля restaurant в JSON
        
        for item in stg_data:
            json_data = item['json_data']
            
            try:
                restaurant_info = json_data.get('restaurant') # Извлекаем данные о ресторане из поля restaurant
                if not restaurant_info:
                    self.log.warning(f"Пропущено: нет данных о ресторане для id {item['id']}")
                    continue
                
                restaurant_id = restaurant_info.get('id')
                restaurant_name = restaurant_info.get('name')
                
                if not restaurant_id:
                    self.log.warning(f"Пропущено: нет restaurant_id для id {item['id']}")
                    continue
                    
                if not restaurant_name:  # Если имя не указано, используем ID как имя
                    restaurant_name = f"Ресторан {restaurant_id}"
                    self.log.warning(f"Для ресторана {restaurant_id} имя не указано, используем значение по умолчанию")
                
                transformed_data.append({
                    'stg_id': item['id'],
                    'restaurant_id': str(restaurant_id),
                    'restaurant_name': str(restaurant_name),
                    'active_from': item['update_ts'],
                    'active_to': datetime(2099, 12, 31, 0, 0, 0),
                    'update_ts': item['update_ts']
                })
                    
            except Exception as e:
                self.log.error(f"Ошибка преобразования данных для id {item['id']}: {e}")
                continue
        
        self.log.info(f"Преобразовано {len(transformed_data)} записей")
        return transformed_data
        
    # Загрузка данных в DDS слой для таблицы dm_restaurants
    def load_to_dds(self, cursor, transformed_data: List[Dict]) -> None:
        """
        Используем подход SCD 2 для хранения истории изменений.
        При поступлении новой версии ресторана:
        1. Закрываем предыдущую версию (устанавливаем active_to)
        2. Вставляем новую версию с новым active_from
        """
        if not transformed_data:
            self.log.warning("Нет данных для загрузки")
            return
        
        # 1. Закрываем предыдущие версии ресторанов
        close_previous_query = """
            UPDATE dds.dm_restaurants 
            SET active_to = %s - interval '1 second'
            WHERE restaurant_id = %s 
            AND active_to = '2099-12-31 00:00:00'::timestamp
            AND active_from < %s
        """
        
        # 2. Вставляем новые версии ресторанов
        insert_query = """
            INSERT INTO dds.dm_restaurants 
                (restaurant_id, restaurant_name, active_from, active_to)
            VALUES 
                (%s, %s, %s, %s)
        """
        
        try:
            updated_count = 0
            inserted_count = 0
            
            for item in transformed_data:
                restaurant_id = item['restaurant_id']
                restaurant_name = item['restaurant_name']
                active_from = item['active_from']
                active_to = item['active_to']
                
                # Закрываем предыдущую активную запись, если она существует
                cursor.execute(close_previous_query, (active_from, restaurant_id, active_from))
                if cursor.rowcount > 0:
                    updated_count += 1
                
                # Вставляем новую запись
                cursor.execute(insert_query, (restaurant_id, restaurant_name, active_from, active_to))
                if cursor.rowcount > 0:
                    inserted_count += 1
            
            self.log.info(f"Успешно обработано записей в dds.dm_restaurants: "
                         f"закрыто предыдущих версий {updated_count}, "
                         f"вставлено новых версий {inserted_count}")
            
        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в DDS: {e}")
            raise
            
    # Основной метод выполнения загрузки с отслеживанием прогресса. Возвращает количество обработанных записей
    def run_copy(self, cursor) -> int:
        self.log.info("=" * 60)
        self.log.info("Начало загрузки данных ресторанов из STG в DDS")
        self.log.info("=" * 60)
        
        try:
            # 1. Получаем настройки workflow
            wf_setting = self._get_wf_setting(cursor)
            last_loaded_ts_str = wf_setting['workflow_settings'][self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Начинаем загрузку с контрольной точки: {last_loaded_ts}")
            
            # 2. Извлекаем данные из STG
            load_queue = self.extract_from_stg(cursor, last_loaded_ts, self._SESSION_LIMIT)
            
            if not load_queue:
                self.log.info("Нет новых данных для загрузки")
                return 0
            
            self.log.info(f"Найдено {len(load_queue)} документов для синхронизации")
            
            # 3. Преобразуем данные
            transformed_data = self.transform_data(load_queue)
            
            if not transformed_data:
                self.log.info("Нет данных для загрузки после преобразования")
                return 0
            
            # 4. Загружаем данные в DDS
            self.load_to_dds(cursor, transformed_data)
            
            # 5. Обновляем контрольную точку
            max_id = max([item['stg_id'] for item in transformed_data])
            max_update_ts = max([item['update_ts'] for item in transformed_data])
            
            self._save_wf_setting(cursor, max_id, max_update_ts)
            
            self.log.info(f"Загрузка завершена. Обработано записей: {len(transformed_data)}")
            self.log.info(f"Последний ID: {max_id}, последняя дата: {max_update_ts}")
            
            return len(transformed_data)
            
        except Exception as e:
            self.log.error(f"Ошибка при выполнении загрузки: {e}")
            raise
            
    # Основной метод загрузки данных с использованием контекстного менеджера
    def execute_loading(self) -> None:
        
        try:
            self.log.info("Начало загрузки ресторанов в DDS")
            
            # Используем connection() как контекстный менеджер
            with self.pg_connect.connection() as conn:
                with conn.cursor() as cursor:
                    # Выполняем загрузку
                    processed_count = self.run_copy(cursor)
                    
            self.log.info(f"Данные ресторанов успешно загружены в DDS. Обработано записей: {processed_count}")
                
        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных: {e}")
            raise
