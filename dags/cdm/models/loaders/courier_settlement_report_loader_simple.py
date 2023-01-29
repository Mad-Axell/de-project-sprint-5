from lib import PgConnect


class CourierSettlementReportLoaderSimple:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_report(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
insert into de.cdm.dm_courier_settlement_report (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with 
tmp_report as 
(
SELECT
			dc.courier_id,
			dc."name" as courier_name,
			extract('YEAR' from fct.order_ts) as settlement_year,
			extract('MONTH' from fct.order_ts) as settlement_month,
			count(fct.order_id)  as orders_count,
			sum(fct.total_sum) as orders_total_sum,
			avg(fct.rate) as rate_avg,
			sum(fct.total_sum)*0.25 as order_processing_fee,
			case 
			    when avg(fct.rate) < 4 then sum(fct.total_sum) * 0.05
			    when avg(fct.rate) <= 4 or avg(fct.rate) < 4.5 then sum(fct.total_sum) * 0.07
			    when avg(fct.rate) <= 4.5 or avg(fct.rate) < 4.9 then sum(fct.total_sum) * 0.08
			    when avg(fct.rate) >= 4.9 then sum(fct.total_sum) * 0.1
			end as tmp_courier_order_sum,
			sum(fct.tip_sum) as courier_tips_sum
FROM 		de.dds.fct_deliveries fct
left join 	de.dds.dm_couriers dc on dc.id = fct.courier_id 
group by 	1, 2, 3, 4
)
select 
			courier_id,
			courier_name,
			settlement_year,
			settlement_month,
			orders_count,
			orders_total_sum,
			rate_avg,
			order_processing_fee,
			case 
		        when rate_avg < 4 and tmp_courier_order_sum < 100 then 100
		        when (rate_avg <= 4 or rate_avg < 4.5) and tmp_courier_order_sum < 150 then 150
		        when (rate_avg <= 4.5 or rate_avg < 4.9) and tmp_courier_order_sum < 175 then 175
		        when rate_avg >= 4.9 and tmp_courier_order_sum < 200 then 200 
		        else tmp_courier_order_sum
		    end as courier_order_sum,
			courier_tips_sum,
			tmp_courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
from 		tmp_report
"""
                )
