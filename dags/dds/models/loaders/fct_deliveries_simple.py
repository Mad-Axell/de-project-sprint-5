from lib import PgConnect


class FctDeliveriesLoad:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_fct_deliveries(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
insert into de.dds.fct_deliveries (delivery_id, courier_id, order_id, order_ts, delivery_ts, address, rate, tip_sum, total_sum)
with
JS_ON as 
(
select 
            replace(replace(object_value, '"', ''), '''','"')::JSON as obj
from 		stg.deliverysystem_deliveries 
)
select 
            obj->>'delivery_id' as delivery_id,
            dc.id as courier_id,
            do2.id as order_id,
            TO_TIMESTAMP(obj->>'order_ts', 'YYYY-MM-DD HH24:MI:SS') as order_ts,
            TO_TIMESTAMP(obj->>'delivery_ts', 'YYYY-MM-DD HH24:MI:SS') as delivery_ts,
            obj->>'address' as address,
            CAST(obj->>'rate' AS integer) as rate,
            CAST(obj->>'tip_sum' AS DECIMAL ) as tip_sum,
            CAST(obj->>'sum' AS DECIMAL ) as total_sum
from 		JS_ON
left join 	de.dds.dm_couriers dc ON obj->>'courier_id' = dc.courier_id
left join 	de.dds.dm_orders do2 on obj->>'order_id' = do2.order_key
where 		dc.id is not null;
"""
                )
