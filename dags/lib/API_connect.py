import requests
import json
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from airflow.providers.http.hooks.http import HttpHook

class ApiConnect:
    DEFAULT_OFFSET = "0"

    def __init__(self, 
                endpoint: str, 
                id_field: str = "_id", 
                sort_field: str = "_id",
                limit: int = 100,
                offset: int= 0) -> None:
        self.endpoint = endpoint
        self.id_field = id_field
        self.sort_field = sort_field
        self.limit = limit
        self.offset = offset

    def load_settings(self) -> None:
        raise NotImplementedError("Method load_settings not allowed for ApiConnect")

    def save_batch(self, batch: List, offset: str) -> None:
        raise NotImplementedError("Method save_batch not allowed for ApiConnect")

    def get_last_loaded_key(self) -> str:
        raise NotImplementedError("Method get_last_loaded_key not allowed for ApiConnect")

    def get_batch(self, offset: Optional[str], limit: int) -> Tuple[List, Optional[str]]:
        offset = int(offset or self.DEFAULT_OFFSET)
        
        api_str = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/" + self.endpoint + "?" + "sort_field=" + sort_field + "&sort_direction=" + sort_direction + "&limit=" + limit + "&offset=" + limit
        Session = requests.Session()
        content = Session.get(api_str).json()
        #content = self.hook.run(self.endpoint, self._get_request_params(offset)).content
        batch = json.loads(content)
        #result = list(map(lambda obj: (obj[self.id_field], datetime.now(), json.dumps(obj)), batch))
        return batch
