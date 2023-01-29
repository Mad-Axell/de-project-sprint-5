from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CourierStgRepository:
    def load_raw_couriers(self, conn: Connection, last_loaded_record_id: int) -> List[CourierJsonObj]:
        with conn.cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.deliverysystem_couriers
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def insert_courier(self, conn: Connection, Courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s);
                """,
                {
                    "courier_id": Courier.courier_id,
                    "courier_name": Courier.courier_name
                },
            )

    def get_Courier(self, conn: Connection, courier_id: str) -> Optional[CourierDdsObj]:
        with conn.cursor(row_factory=class_row(CourierDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        courier_id,
                        courier_name
                    FROM dds.dm_couriers
                    WHERE courier_id = %(courier_id)s;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj