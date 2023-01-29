import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


from dds.models.repositories.fct_products_repositories import EventJsonObj, EventRawRepository, FctProductDdsObj, ProductPaymentJsonObj, BonusPaymentJsonObj, FctProductDdsRepository
from dds.models.repositories.order_repositories import OrderDdsRepository
from dds.models.repositories.product_repositories import ProductDdsObj, ProductDdsRepository

log = logging.getLogger(__name__)


class FctProductsLoader:
    PAYMENT_EVENT = "bonus_transaction"
    WF_KEY = "fact_product_events_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_event_id"

    _LOG_THRESHOLD = 100000

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw_events = EventRawRepository()
        self.dds_orders = OrderDdsRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_facts = FctProductDdsRepository()
        self.settings_repository = settings_repository

    def parse_order_products(self,
                             order_raw: BonusPaymentJsonObj,
                             order_id: int,
                             products: Dict[str, ProductDdsObj]
                             ) -> Tuple[bool, List[FctProductDdsObj]]:

        res = []

        for p_json in order_raw.product_payments:
            if p_json.product_id not in products:
                return (False, [])

            t = FctProductDdsObj(id=0,
                                 order_id=order_id,
                                 product_id=products[p_json.product_id].id,
                                 count=p_json.quantity,
                                 price=p_json.price,
                                 total_sum=p_json.product_cost,
                                 bonus_grant=p_json.bonus_grant,
                                 bonus_payment=p_json.bonus_payment
                                 )
            res.append(t)

        return (True, res)

    def load_product_facts(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            log.info(f"Starting load from: {last_loaded_id}")

            load_queue = self.raw_events.load_raw_event(conn, self.PAYMENT_EVENT, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            log.info(f"Found {len(load_queue)} events to load.")

            products = self.dds_products.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p

            proc_cnt = 0
            for payment_raw in load_queue:
                payment_obj = BonusPaymentJsonObj(json.loads(payment_raw.event_value))
                order = self.dds_orders.get_order(conn, payment_obj.order_id)
                if not order:
                    log.info(f"Not found order {payment_obj.order_id}. Finishing.")
                    break

                (success, facts_to_load) = self.parse_order_products(payment_obj, order.id, prod_dict)
                if not success:
                    log.info(f"Could not parse object for order {order.id}. Finishing.")
                    break

                self.dds_facts.insert_facts(conn, facts_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = payment_raw.id
                self.settings_repository.save_setting(conn, wf_setting)

                proc_cnt += 1
                if proc_cnt % self._LOG_THRESHOLD == 0:
                    log.info(f"Processing events {proc_cnt} out of {len(load_queue)}.")

            log.info(f"Processed {proc_cnt} events out of {len(load_queue)}.")
