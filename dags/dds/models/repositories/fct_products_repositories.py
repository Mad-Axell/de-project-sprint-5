import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel

from datetime import datetime
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class EventJsonObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

class EventRawRepository:
    def load_raw_event(self, conn: Connection, event_type: str, last_loaded_record_id: int) -> List[EventJsonObj]:
        with conn.cursor(row_factory=class_row(EventJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = %(event_type)s AND id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {
                    "event_type": event_type,
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class FctProductDdsObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class ProductPaymentJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [ProductPaymentJsonObj(it) for it in d["product_payments"]]


class FctProductDdsRepository:
    def insert_facts(self, conn: Connection, facts: List[FctProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for fact in facts:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            order_id,
                            product_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant
                        )
                        VALUES (
                            %(order_id)s,
                            %(product_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s
                        )
                        ON CONFLICT (order_id, product_id) DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant
                        ;
                    """,
                    {
                        "product_id": fact.product_id,
                        "order_id": fact.order_id,
                        "count": fact.count,
                        "price": fact.price,
                        "total_sum": fact.total_sum,
                        "bonus_payment": fact.bonus_payment,
                        "bonus_grant": fact.bonus_grant
                    },
                )