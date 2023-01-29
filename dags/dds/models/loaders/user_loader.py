import json
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.models.repositories.user_repositories import UserJsonObj, UserDdsObj, UserStgRepository, UserDdsRepository

class UserLoader:
    WF_KEY = "users_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_user_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = UserStgRepository()
        self.dds = UserDdsRepository()
        self.settings_repository = settings_repository

    def parse_users(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            user_json = json.loads(r.object_value)
            t = UserDdsObj(id=r.id,
                           user_id=user_json['_id'],
                           user_name=user_json['name'],
                           user_login=user_json['login'],
                           )

            res.append(t)
        return res

    def load_users(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_users(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            users_to_load = self.parse_users(load_queue)
            for u in users_to_load:
                existing = self.dds.get_user(conn, u.user_id)
                if not existing:
                    self.dds.insert_user(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting)
