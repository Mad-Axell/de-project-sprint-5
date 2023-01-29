from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class RestaurantDdsObj(BaseModel): 
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: str
    active_to: str


class RestaurantRawRepository:
    def load_raw_restaurants(self, conn: Connection, last_loaded_record_id: int) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s
                    AND object_value::json->'menu' IS NOT NULL;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from::varchar,
                        active_to::varchar
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj