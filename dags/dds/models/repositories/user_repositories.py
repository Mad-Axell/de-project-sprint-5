from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class UserDdsObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UserStgRepository:
    def load_raw_users(self, conn: Connection, last_loaded_record_id: int) -> List[UserJsonObj]:
        with conn.cursor(row_factory=class_row(UserJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class UserDdsRepository:
    def insert_user(self, conn: Connection, user: UserDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s);
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )

    def get_user(self, conn: Connection, user_id: str) -> Optional[UserDdsObj]:
        with conn.cursor(row_factory=class_row(UserDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        user_id,
                        user_name,
                        user_login
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {"user_id": user_id},
            )
            obj = cur.fetchone()
        return obj