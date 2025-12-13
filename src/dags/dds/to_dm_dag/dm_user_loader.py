import json
from datetime import datetime
from typing import Dict, List, Optional
import logging

from lib import PgConnect


class UsersLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_stg_to_dds_users_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_connect: PgConnect, logger: Optional[logging.Logger] = None) -> None:
        """
        Инициализация загрузчика пользователей из STG в DDS
        
        :param pg_connect: объект PgConnect для подключения к базе данных
        :param logger: логгер для записи логов
        """
        self.pg_connect = pg_connect
        self.log = logger or logging.getLogger(__name__)

    def str2json(self, json_str: str) -> Dict:
        """
        Преобразование строки JSON в словарь Python
        """
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            self.log.error(f"Ошибка декодирования JSON: {e}")
            return {}

    def _get_wf_setting(self, cursor) -> Dict:
        """
        Получение настроек workflow из таблицы dds.srv_wf_settings
        """
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

    def _save_wf_setting(self, cursor, last_loaded_id: int, last_loaded_ts: datetime) -> None:
        """
        Сохранение настроек workflow в таблицу dds.srv_wf_settings
        """
        workflow_settings = {
            self.LAST_LOADED_ID_KEY: last_loaded_id,
            self.LAST_LOADED_TS_KEY: last_loaded_ts.isoformat() if isinstance(last_loaded_ts, datetime) else last_loaded_ts
        }
        
        # Убрали updated_at из запроса
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

    def extract_from_stg(self, cursor, last_loaded_ts: datetime, limit: int = None) -> List[Dict]:
        """
        Извлечение данных из STG слоя с учетом последней загрузки
        """
        query = """
            SELECT id, object_id, object_value, update_ts
            FROM stg.ordersystem_users
            WHERE update_ts > %s
            ORDER BY update_ts, id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            cursor.execute(query, (last_loaded_ts,))
            rows = cursor.fetchall()
            
            users_data = []
            for row in rows:
                json_data = self.str2json(row[2])
                
                if json_data:
                    user_data = {
                        'id': row[0],
                        'object_id': row[1],
                        'json_data': json_data,
                        'update_ts': row[3]
                    }
                    users_data.append(user_data)
            
            self.log.info(f"Извлечено {len(users_data)} записей из stg.ordersystem_users начиная с {last_loaded_ts}")
            return users_data
            
        except Exception as e:
            self.log.error(f"Ошибка при извлечении данных из STG: {e}")
            return []

    def transform_data(self, stg_data: List[Dict]) -> List[Dict]:
        """
        Преобразование данных из STG формата в DDS формат
        """
        transformed_data = []
        
        for item in stg_data:
            json_data = item['json_data']
            
            try:
                user_id = json_data.get('_id')
                user_login = json_data.get('login')
                user_name = json_data.get('name')
                
                if not all([user_id, user_login, user_name]):
                    self.log.warning(f"Пропущено: неполные данные для id {item['id']}")
                    continue
                
                transformed_data.append({
                    'stg_id': item['id'],
                    'user_id': str(user_id),
                    'user_name': str(user_name),
                    'user_login': str(user_login),
                    'update_ts': item['update_ts']
                })
                    
            except Exception as e:
                self.log.error(f"Ошибка преобразования данных для id {item['id']}: {e}")
                continue
        
        self.log.info(f"Преобразовано {len(transformed_data)} записей")
        return transformed_data

    def load_to_dds(self, cursor, transformed_data: List[Dict]) -> None:
        """
        Загрузка данных в DDS слой (без ON CONFLICT - используем MERGE через INSERT/UPDATE)
        """
        if not transformed_data:
            self.log.warning("Нет данных для загрузки")
            return
        
        # Используем подход с проверкой существования записи
        # Это безопаснее, чем ON CONFLICT, если нет ограничения UNIQUE
        upsert_query = """
            WITH new_data AS (
                SELECT %s AS user_id, %s AS user_name, %s AS user_login
            ),
            existing AS (
                SELECT user_id FROM dds.dm_users WHERE user_id = %s
            )
            INSERT INTO dds.dm_users (user_id, user_name, user_login)
            SELECT user_id, user_name, user_login 
            FROM new_data
            WHERE NOT EXISTS (SELECT 1 FROM existing)
        """
        
        update_query = """
            UPDATE dds.dm_users 
            SET user_name = %s, user_login = %s
            WHERE user_id = %s
        """
        
        try:
            inserted_count = 0
            updated_count = 0
            
            for item in transformed_data:
                user_id = item['user_id']
                user_name = item['user_name']
                user_login = item['user_login']
                
                # Сначала пытаемся вставить новую запись
                cursor.execute(upsert_query, (user_id, user_name, user_login, user_id))
                if cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    # Если запись уже существует, обновляем ее
                    cursor.execute(update_query, (user_name, user_login, user_id))
                    updated_count += 1
            
            self.log.info(f"Успешно обработано записей в dds.dm_users: "
                         f"вставлено {inserted_count}, обновлено {updated_count}")
            
        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в DDS: {e}")
            raise

    def run_copy(self, cursor) -> int:
        """
        Основной метод выполнения загрузки с отслеживанием прогресса
        Возвращает количество обработанных записей
        """
        self.log.info("=" * 60)
        self.log.info("Начало загрузки данных пользователей из STG в DDS")
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

    def execute_loading(self) -> None:
        """
        Основной метод загрузки данных с использованием контекстного менеджера
        """
        try:
            self.log.info("Начало загрузки пользователей в DDS")
            
            # Используем connection() как контекстный менеджер
            with self.pg_connect.connection() as conn:
                with conn.cursor() as cursor:
                    # Выполняем загрузку
                    processed_count = self.run_copy(cursor)
                    
            self.log.info(f"Данные пользователей успешно загружены в DDS. Обработано записей: {processed_count}")
                
        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных: {e}")
            raise