# dds/order_repositories.py
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
from psycopg import Connection
from psycopg.rows import class_row


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class OrderRawRepository:
    def load_raw_orders(self, conn: Connection, last_loaded_id: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                SELECT id, object_id, object_value, update_ts
                FROM stg.ordersystem_orders
                WHERE id > %(last_loaded_id)s
                ORDER BY id ASC;
                """,
                {"last_loaded_id": last_loaded_id}
            )
            return cur.fetchall()