
import requests
import json
import os
from dcm.tasks.services import DcmService


class BaseCheckHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__CHECK")

class CheckHandler(BaseImportHandler):
    check_type = ["date_comparison", "string_comparison" ,"look_in", "search", "pycode_check", "duplicate"]

    def run(self, params):
        response = requests.post(url=f"{self.base_url}check/",json=params)
        response_body =  json.loads(response.text)
        return  {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import',
            "result_id":response_body["result_id"]
        }