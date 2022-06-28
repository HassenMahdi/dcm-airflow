
import requests
import json
import os
from dcm.tasks.services import DcmService


class BaseCheckHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__CLEANSING")

class CheckHandler(BaseCheckHandler):
    # check_type = ["type_check", "date_comparison", "string_comparison" ,"look_in", "search", "pycode_check", "duplicate"]
    check_type = [
        "duplicate_check",
        "string_comparison",
        "column_comparison",
        "pycode_check",
        "type_check",
        "empty_check",
        "format_check"
    ]
    
    def get_check_list(self, params):
        if self.task_type == "empty_check":
            return [{"column": c, "type": "empty_check"} for c in params["columns"]]
        if self.task_type == "format_check":
            return [{"column": f["column"], "format": f["format"], "type": "format_check"} for f in params["formats"]]
        else:
            return [{**params}]

    def run(self, params):
        body = {
            "file_id": self.input['file_id'], 
            'sheet_id': self.input['sheet_id'],
            "modifications" : [],
            "checks" : self.get_check_list(params)
        }
        
        response = requests.post(url=f"{self.base_url}/simple",json=body)
        response_body =  json.loads(response.text)
        
        result_id = response_body.get("result_id", None)
        
        output = {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import',
        }
        output["result_id"] = result_id
        self.task_instance.xcom_push("OUTPUT", output,self.execution_date)

        if result_id:
            results_req = requests.get(url=f"{self.base_url}/simple/{result_id}")
            results_body =  json.loads(results_req.text)
            if results_body["totals"]["ERROR"] > 0:
                raise Exception('FAILED_EXCEPTION')
            else:
                return output
        else:
            return output
        
        

        