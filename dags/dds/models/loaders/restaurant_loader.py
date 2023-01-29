import json
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.models.repositories.restaurant_repositories import RestaurantJsonObj, RestaurantDdsObj, RestaurantRawRepository, RestaurantDdsRepository

class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_restaurant_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repository = settings_repository

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            restaurant_json = json.loads(r.object_value)
            t = RestaurantDdsObj(id=r.id,
                           restaurant_id=restaurant_json['_id'],
                           restaurant_name=restaurant_json['name'],
                           active_from=restaurant_json['update_ts'],
                           active_to= "2099-12-31 00:00:00.000"
                           )

            res.append(t)
        return res

    def load_restaurants(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            restaurants_to_load = self.parse_restaurants(load_queue)
            for r in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, r.restaurant_id)
                if not existing:
                    self.dds.insert_restaurant(conn, r)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = r.id
                self.settings_repository.save_setting(conn, wf_setting)
