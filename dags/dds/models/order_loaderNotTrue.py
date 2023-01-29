import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
#from dds.models.user_loader import (UserDdsRepository, UserJsonObj)
#from dds.models.restaurant_loader import (RestaurantDdsRepository, RestaurantJsonObj, RestaurantRawRepository)
#from dds.models.timestamp_loader import (TimestampDdsRepository, TimestampJsonObj)


class OrderDdsObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    user_id: int
    restaurant_id: int
    timestamp_id: int

class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class OrderRawRepository:
    def load_raw_orders(self, conn: Connection, last_loaded_record_id: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

class OrderDdsRepository:
    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders
                    (
                        order_key,
                        order_status,
                        user_id,
                        restaurant_id,
                        timestamp_id
                    )
                    VALUES (
                        %(order_key)s,
                        %(order_status)s,
                        %(user_id)s,
                        %(restaurant_id)s,
                        %(timestamp_id)s
                        );
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id
                },
            )

    def get_order(self, conn: Connection, order_key: str) -> Optional[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_key, order_status, user_id, restaurant_id, timestamp_id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            obj = cur.fetchone()
        return obj

    def list_orders(self, conn: Connection) -> List[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_key, order_status, user_id, restaurant_id, timestamp_id
                    FROM dds.dm_orders;
                """
            )
            obj = cur.fetchall()
        return obj
        
    def load_clear_orders(self, conn: Connection, last_loaded_record_id: int) -> List[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    select 
                                oo.id as id,
                                oo.object_id as order_key,
                                REPLACE ((oo.object_value::JSON -> 'final_status')::text, '"', '')::text as order_status,
                                du.id as user_id,
                                dr.id as restaurant_id,
                                dt.id  as timestamp_id 
                    from 		stg.ordersystem_orders oo 
                    left join	dds.dm_users du on du.user_id = REPLACE ((oo.object_value::JSON -> 'user' -> 'id')::text, '"', '')
                    left join	dds.dm_restaurants dr on dr.restaurant_id = REPLACE ((oo.object_value::JSON -> 'restaurant' -> 'id')::text, '"', '')
                    left join 	dds.dm_timestamps dt on dt.ts::timestamp  = REPLACE ((oo.object_value::JSON -> 'update_ts')::text, '"', '')::timestamp
                    WHERE oo.id > %(last_loaded_record_id)s
                """,
                {"last_loaded_record_id": last_loaded_record_id},
                
            )
            objs = cur.fetchall()
        return objs

class OrderLoader:
    WF_KEY = "order_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        #self.raw = OrderRawRepository()
        self.dds_orders = OrderDdsRepository()
        #self.dds_restaurants = RestaurantDdsRepository()
        #self.dds_users = UserDdsRepository()
        self.settings_repository = settings_repository


    def load_orders(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.dds_orders.load_clear_orders(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)

            orders_to_load = load_queue
            
            for o in orders_to_load:
                existing = self.dds_orders.get_order(conn, o.order_key)
                if not existing:
                    self.dds_orders.insert_order(conn, o)
                    
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = o.id
                self.settings_repository.save_setting(conn, wf_setting)