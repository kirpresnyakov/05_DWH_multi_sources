from datetime import datetime, timedelta
from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from stg.order_system_dag.pg_saver_ord import PgSaverOrd
from stg.order_system_dag.order_reader import OrderReader
from lib import PgConnect
from lib.dict_util import json2str


class OrderLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000
    _DEFAULT_LOAD_PERIOD_DAYS = 8  # Дней по умолчанию для загрузки

    WF_KEY = "example_ordersystem_orders_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: OrderReader, pg_dest: PgConnect, pg_saver: PgSaverOrd, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # Открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            
            if not wf_setting:
                # Если настройки нет, устанавливаем дату за 8 дней назад как начальную точку
                default_load_period = datetime.utcnow() - timedelta(days=self._DEFAULT_LOAD_PERIOD_DAYS)
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: default_load_period.isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]  # ИСПРАВЛЕНО: LAST_LOADED_TS_KEY вместо LAST_LOAD_TS_KEY
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            
            # Рассчитываем минимальную дату для фильтрации (8 дней назад)
            min_load_ts = datetime.utcnow() - timedelta(days=self._DEFAULT_LOAD_PERIOD_DAYS)
            
            # Используем максимальную дату из last_loaded_ts и min_load_ts
            # Это гарантирует, что мы всегда загружаем данные как минимум за последние 8 дней
            effective_load_ts = max(last_loaded_ts, min_load_ts)
            
            self.log.info(f"Starting to load from checkpoint: {last_loaded_ts}")
            self.log.info(f"Effective load timestamp (considering {self._DEFAULT_LOAD_PERIOD_DAYS} days filter): {effective_load_ts}")

            load_queue = self.collection_loader.get_orders(effective_load_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from orders collection.")
            
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} documents of {len(load_queue)} while syncing orders.")

            # Сохраняем новую контрольную точку только если есть обработанные документы
            if load_queue:
                max_update_ts = max([t["update_ts"] for t in load_queue])
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_update_ts.isoformat() if isinstance(max_update_ts, datetime) else max_update_ts
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
                self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
            else:
                self.log.info("No new documents to process.")

            return len(load_queue)