from typing import List, Optional
from datetime import datetime

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductDdsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int

class ProductDdsRepository:
    def insert_dds_products(self, conn: Connection, products: List[ProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to,
                            restaurant_id)
                        VALUES (
                            %(product_id)s,
                            %(product_name)s,
                            %(product_price)s,
                            %(active_from)s,
                            %(active_to)s,
                            %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )

    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj

    def list_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj