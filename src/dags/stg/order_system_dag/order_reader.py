from typing import List, Dict, Any
from datetime import datetime

from lib import MongoConnect

class OrderReader:
    def __init__(self, mc: MongoConnect, default_load_days: int = 8) -> None:
        self.dbs = mc.client()
        self.default_load_days = default_load_days

    def get_orders(self, load_threshold: datetime = None, limit: int = None) -> List[Dict]:
        # Если порог загрузки не указан, используем дефолтный период
        if load_threshold is None:
            load_threshold = datetime.utcnow() - timedelta(days=self.default_load_days)
        
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection("orders").find(filter=filter, sort=sort, limit=limit))
        return docs