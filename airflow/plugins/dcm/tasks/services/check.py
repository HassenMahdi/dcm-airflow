
import requests
import json
import os
from dcm.tasks.services import DcmService


class BaseCheckHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__CHECK")

class CheckHandler(BaseCheckHandler):
    # check_type = ["type_check", "date_comparison", "string_comparison" ,"look_in", "search", "pycode_check", "duplicate"]
    check_type = [
        "duplicate_check",
        "string_comparison",
        "column_comparison",
        "pycode_check",
        "type_check",
    ]

    def run(self, params):
        response = requests.post(url=f"{self.base_url}/simple",json=params)
        response_body =  json.loads(response.text)
        
        result_id = response_body.get("result_id", None)
        
        if result_id:
            self.task_instance.xcom_push("result_id",result_id,self.execution_date)
        
            results_req = requests.get(url=f"{self.base_url}/simple/{self.result_id}")
            results_body =  json.loads(results_req.text)
            if results_body["totals"]["ERROR"] > 0:
                raise Exception('FAILED_EXCEPTION')
        
        return  {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import',
            "result_id": result_id
        }