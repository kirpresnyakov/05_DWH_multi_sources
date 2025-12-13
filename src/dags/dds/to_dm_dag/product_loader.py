import json
from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.to_dm_dag.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.to_dm_dag.order_repositories import OrderJsonObj, OrderRawRepository


class ProductDdsObj(BaseModel):
    id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: Decimal
    active_from: datetime
    active_to: datetime


class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str  # ID из системы-источника


class ProductDdsRepository:
    def insert_dds_product(self, conn: Connection, product: ProductDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(
                        restaurant_id, 
                        product_id, 
                        product_name, 
                        product_price, 
                        active_from, 
                        active_to
                    )
                    VALUES (
                        %(restaurant_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        %(product_price)s,
                        %(active_from)s,
                        %(active_to)s
                    );
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )

    def get_product(self, conn: Connection, restaurant_id: int, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id, product_id, product_name, product_price, active_from, active_to
                    FROM dds.dm_products
                    WHERE restaurant_id = %(restaurant_id)s 
                    AND product_id = %(product_id)s
                    AND active_to = '2099-12-31 00:00:00';
                """,
                {"restaurant_id": restaurant_id, "product_id": product_id},
            )
            obj = cur.fetchone()
        return obj

    def close_product_version(self, conn: Connection, product_id: int, close_date: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_products
                    SET active_to = %(close_date)s
                    WHERE id = %(product_id)s;
                """,
                {"product_id": product_id, "close_date": close_date},
            )


class RestaurantDdsRepository:
    def get_restaurant(self, conn: Connection, restaurant_source_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_source_id)s;
                """,
                {"restaurant_source_id": restaurant_source_id},
            )
            obj = cur.fetchone()
        return obj


class ProductLoader:
    WF_KEY = "products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw_orders = OrderRawRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.settings_repository = settings_repository

    def parse_order_products(self, order_raw: OrderJsonObj) -> Dict[str, Any]:
        """
        Парсит JSON заказа и извлекает информацию о продуктах.
        Возвращает словарь с данными:
        - restaurant_source_id: ID ресторана из системы-источника
        - update_ts: время обновления заказа
        - products: список продуктов
        """
        try:
            order_json = json.loads(order_raw.object_value)
            update_ts = datetime.strptime(order_json['update_ts'], "%Y-%m-%d %H:%M:%S")
            restaurant_source_id = order_json['restaurant']['id']
            
            products = []
            if 'order_items' in order_json:
                for item in order_json['order_items']:
                    product_data = {
                        'product_id': item['id'],
                        'product_name': item['name'],
                        'product_price': Decimal(str(item['price'])),
                        'active_from': update_ts,
                        'active_to': datetime(2099, 12, 31, 0, 0, 0)
                    }
                    products.append(product_data)
            
            return {
                'restaurant_source_id': restaurant_source_id,
                'update_ts': update_ts,
                'products': products
            }
        except (KeyError, json.JSONDecodeError) as e:
            raise ValueError(f"Error parsing order {order_raw.id}: {e}")

    def load_products(self):
        # Создаем новое соединение
        with self.dwh.connection() as conn:
            try:
                # Получаем настройки workflow
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
                if not wf_setting:
                    wf_setting = EtlSetting(
                        id=0, 
                        workflow_key=self.WF_KEY, 
                        workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                    )

                last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

                # Загружаем новые заказы
                load_queue = self.raw_orders.load_raw_orders(conn, last_loaded_id)
                
                for order in load_queue:
                    try:
                        # Парсим данные из заказа
                        parsed_data = self.parse_order_products(order)
                        
                        # Получаем restaurant_id
                        restaurant = self.dds_restaurants.get_restaurant(
                            conn, 
                            parsed_data['restaurant_source_id']
                        )
                        
                        if not restaurant:
                            # Если ресторан не найден, пропускаем этот заказ
                            print(f"Restaurant not found: {parsed_data['restaurant_source_id']}")
                            continue
                        
                        # Обрабатываем каждый продукт в заказе
                        for product_data in parsed_data['products']:
                            try:
                                # Проверяем, существует ли уже такой продукт
                                existing_product = self.dds_products.get_product(
                                    conn, 
                                    restaurant.id, 
                                    product_data['product_id']
                                )
                                
                                if existing_product:
                                    # Проверяем, изменились ли данные
                                    if (existing_product.product_name != product_data['product_name'] or 
                                        existing_product.product_price != product_data['product_price']):
                                        
                                        # Закрываем старую версию
                                        self.dds_products.close_product_version(
                                            conn, 
                                            existing_product.id, 
                                            parsed_data['update_ts']
                                        )
                                        
                                        # Создаем новую версию продукта
                                        new_product = ProductDdsObj(
                                            id=0,
                                            restaurant_id=restaurant.id,
                                            product_id=product_data['product_id'],
                                            product_name=product_data['product_name'],
                                            product_price=product_data['product_price'],
                                            active_from=product_data['active_from'],
                                            active_to=product_data['active_to']
                                        )
                                        self.dds_products.insert_dds_product(conn, new_product)
                                else:
                                    # Создаем новый продукт
                                    new_product = ProductDdsObj(
                                        id=0,
                                        restaurant_id=restaurant.id,
                                        product_id=product_data['product_id'],
                                        product_name=product_data['product_name'],
                                        product_price=product_data['product_price'],
                                        active_from=product_data['active_from'],
                                        active_to=product_data['active_to']
                                    )
                                    self.dds_products.insert_dds_product(conn, new_product)
                            except Exception as e:
                                # Логируем ошибку, но продолжаем обработку следующих продуктов
                                print(f"Error processing product {product_data.get('product_id', 'unknown')} in order {order.id}: {e}")
                                conn.rollback()
                                continue
                        
                        # Обновляем last_loaded_id в настройках
                        wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order.id
                        self.settings_repository.save_setting(
                            conn, 
                            self.WF_KEY, 
                            json.dumps(wf_setting.workflow_settings)
                        )
                    
                    except Exception as e:
                        # Логируем ошибку, откатываем транзакцию и продолжаем обработку следующих заказов
                        print(f"Error processing order {order.id}: {e}")
                        conn.rollback()
                        # Начинаем новую транзакцию для следующего заказа
                        conn.autocommit = False
                        continue
                    
                    # Коммитим после каждого успешного заказа
                    conn.commit()
                    # Начинаем новую транзакцию для следующего заказа
                    conn.autocommit = False
                
                print(f"Successfully processed {len(list(load_queue))} orders")
                
            except Exception as e:
                # Если произошла критическая ошибка, откатываем транзакцию
                conn.rollback()
                raise 