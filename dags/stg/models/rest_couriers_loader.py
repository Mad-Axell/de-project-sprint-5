from logging import Logger
from typing import List, Dict
from utility.config_const import ConfigConst
import json
import requests

from lib import PgConnect, ApiConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository

from datetime import datetime
from typing import Any

class CourierObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: str

class CourierOriginRepository:
    DEFAULT_OFFSET = "0"

    def __init__(self,
                sort_field: str = "id",
                sort_direction: str = "asc") -> None:
        self.sort_field = sort_field
        self.sort_direction = sort_direction
    
    def load_couriers(self, sort_field: str, sort_direction: str, offset: int = 0, limit: int = 1000000) -> None:
        offset = int(offset or self.DEFAULT_OFFSET)
        
        api_str =   "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=%s&sort_direction=%s&limit=%d&offset=%d" % (sort_field, sort_direction, limit, offset)
        Session = requests.Session()
        content = Session.get(api_str, headers = ConfigConst.REST_HEADERS).json()
        
        #content = self.hook.run(self.endpoint, self._get_request_params(offset)).content
        #batch = json.loads(content)
        #result = list(map(lambda obj: (obj[self.id_field], datetime.now(), json.dumps(obj)), batch))
        docs = list(content)
        return docs

class CourierDestRepository:
    def insert_courier(self, conn: Connection, id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "id": id,
                    "val": str_val
                }
            )
            
            
class REST_CourierLoader:
    _LOG_THRESHOLD = 2
    WF_KEY = "couriers_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # инкрементальная загрузка


    def __init__(self, collection_loader: CourierOriginRepository, pg_dest: PgConnect, pg_saver: CourierDestRepository, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        
        
    def load_courier(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            self.log.info(f"Continuing from {last_loaded_id} event id.")

            load_queue = self.collection_loader.load_couriers(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from Couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.insert_courier(conn, str(d["_id"]), str(d))
                i+=1

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = len(load_queue)+last_loaded_id
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings))
            return len(load_queue)

