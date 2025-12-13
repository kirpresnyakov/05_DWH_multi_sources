from logging import Logger
from typing import List
from datetime import datetime

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, event_threshold: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                """, {
                    "threshold": event_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class EventDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class EventLoader:
    WF_KEY = "example_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            # Загружаем состояние предыдущего запуска
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            # Забираем ВСЕ записи с id больше сохраненного
            load_queue = self.origin.list_events(last_loaded)
            self.log.info(f"Found {len(load_queue)} events to load.")
            
            if not load_queue:
                self.log.info("No new events to load.")
                return

            # Сохраняем данные в целевую таблицу
            for event in load_queue:
                self.stg.insert_event(conn, event)

            # Обновляем состояние последнего загруженного id
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            
            # Сохраняем состояние в stg.srv_wf_settings
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Events load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")