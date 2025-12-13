from datetime import datetime
from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from stg.order_system_dag.pg_saver_user import PgSaverUser
from stg.order_system_dag.user_reader import UserReader
from lib import PgConnect
from lib.dict_util import json2str


class UserLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: UserReader, pg_dest: PgConnect, pg_saver: PgSaverUser, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # Открываем транзакцию
        with self.pg_dest.connection() as conn:
            # Прочитываем состояние загрузки
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load from last checkpoint: {last_loaded_ts}")

            # Загружаем данные из коллекции users
            load_queue = self.collection_loader.get_users(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from users collection.")
            
            if not load_queue:
                self.log.info("No new data to load. Quitting.")
                return 0

            # Сохраняем данные в PostgreSQL
            i = 0
            for user in load_queue:
                self.pg_saver.save_object(
                    conn, 
                    str(user["_id"]), 
                    user["update_ts"], 
                    user
                )

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} documents of {len(load_queue)} while syncing users.")

            # Обновляем checkpoint (последнюю дату загрузки)
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                [user["update_ts"] for user in load_queue]
            )
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)