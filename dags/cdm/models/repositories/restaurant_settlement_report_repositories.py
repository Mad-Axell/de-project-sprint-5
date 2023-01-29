from lib.pg_connect import PgConnect

class RestaurantSettlementReportRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_settlement_by_days(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
"""
WITH 
Report AS
(
    select 
                dr.restaurant_id as restaurant_id,
                dr.restaurant_name  as restaurant_name,
                dt.date as settlement_date,
                COUNT(distinct(fct.order_id)) as orders_count,
                SUM(fct.total_sum) as orders_total_sum,
                SUM(fct.bonus_payment) as orders_bonus_payment_sum,
                SUM(fct.bonus_grant) as orders_bonus_granted_sum,
                SUM(fct.total_sum)*0.25 as order_processing_fee,
                SUM(fct.total_sum)*0.75 - SUM(fct.bonus_payment) AS restaurant_reward_sum
    from 		de.dds.fct_product_sales fct
    left join 	de.dds.dm_orders do2 on do2.id = fct.order_id
    left join 	de.dds.dm_restaurants dr on dr.id = do2.restaurant_id
    left join 	de.dds.dm_timestamps dt on dt.id = do2.timestamp_id
    group by 	dr.restaurant_id, dr.restaurant_name, settlement_date
    order by 	restaurant_name, settlement_date asc, orders_count desc
)
INSERT INTO cdm.dm_restaurant_settlement_report
            (
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                order_processing_fee,
                restaurant_reward_sum
            )
SELECT 
            restaurant_id,
            restaurant_name,
            settlement_date,
            orders_count,
            orders_total_sum,
            orders_bonus_payment_sum,
            orders_bonus_granted_sum,
            order_processing_fee, 
            restaurant_reward_sum
FROM        Report
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
    """
                )
                conn.commit()