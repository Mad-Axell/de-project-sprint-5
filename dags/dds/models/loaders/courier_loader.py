import json
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.models.repositories.courier_repositories import CourierJsonObj, CourierDdsObj, CourierStgRepository, CourierDdsRepository

class CourierLoader:
    WF_KEY = "couriers_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_courier_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = CourierStgRepository()
        self.dds = CourierDdsRepository()
        self.settings_repository = settings_repository

    def parse_couriers(self, raws: List[CourierJsonObj]) -> List[CourierDdsObj]:
        res = []
        for r in raws:
            courier_json = json.loads(r.object_value)
            t = CourierDdsObj( id=r.id,
                               courier_id=courier_json['_id'],
                               courier_name=courier_json['name'],
                           )

            res.append(t)
        return res

    def load_couriers(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_couriers(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            courier_to_load = self.parse_couriers(load_queue)
            for c in courier_to_load:
                existing = self.dds.get_courier(conn, c.courier_id)
                if not existing:
                    self.dds.insert_courier(conn, c)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = c.id
                self.settings_repository.save_setting(conn, wf_setting)
