import os

import requests
from dcm.tasks.services import DcmService

import time


class BaseUploadHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__UPLOAD")

    def start(self):
        if self.run_as_preview:
            response = {'status':"success"}
            self.task_instance.xcom_push("OUTPUT",response,self.execution_date)
            return response
        else:
            return super().start()
            

class UploadConnectorHandler(BaseUploadHandler):
    upload_connector_types = ["BLOB_STORAGE_UPLOAD_CONNECTOR", "SQL_UPLOAD_CONNECTOR", "POSTGRES_UPLOAD_CONNECTOR"]
    def run(self, params):
        # FORM PAYLOAD
        file_id = self.input['file_id']
        sheet_id = self.input['sheet_id']
        payload = {
            **params,
            "file_id": file_id,
            "sheet_id": sheet_id
        }
        run_uplaod = requests.post(url=f"{self.base_url}connector", json=payload)
        if run_uplaod.status_code == 200:
            return {'status':'success'}
        else:
            raise Exception('Upload Failed')


class UploadCollectionConnectorHandler(BaseUploadHandler):
    cleansing_url = os.getenv("DCM_SERVICE__CLEANSING")

    def run(self, params):
        file_id = self.input['file_id']
        sheet_id = self.input['sheet_id']
        mapping_id = params["mapping_id"]
        domain_id = params["domain_id"]

        cleansing_payload = {
            "job_id":None,
            "file_id":file_id,
            "worksheet_id":sheet_id,
            "mapping_id":mapping_id,
            "domain_id":domain_id,
            "is_transformed":False,
            "modifications":{},
            "task_id":self.task_id,
            "execution_date": self.execution_date.timestamp() 
        }

        run_cleansing = requests.post(url=f"{self.cleansing_url}", json=cleansing_payload)
        cleansing_job_id = run_cleansing.json()['job_id']

        self.task_instance.xcom_push("CLEANSING_JOB_ID",cleansing_job_id,self.execution_date)

        job_status = requests.get(url=f"{self.cleansing_url}/metadata/{cleansing_job_id}")
        
        if job_status.json()["totalErrors"] == 0:
            self.task_instance.xcom_push("CLEANSING_PASSED",True,self.execution_date)
            upload_paylod = {
                "tags"  : params.get("tags", []),
                "domain_id"  : domain_id,
                "sheet_id"  : sheet_id,
                "file_id"  : file_id,
                "cleansing_job_id"  :cleansing_job_id,
                "mapping_id"  : mapping_id,
                "user_id"  : None,
                "task_id":self.task_id,
                "execution_date": self.execution_date.timestamp() 
            }
            print(upload_paylod)
            start_uplaod = requests.post(url=f"{self.base_url}flow", json=upload_paylod)
            flow_id = start_uplaod.json()
            
            while True:
                time.sleep(5)
                uplaod_status = requests.get(url=f"{self.base_url}flow/{flow_id}/status/")
                status = uplaod_status.json()['upload_status']
                if status == 'DONE':  
                    return {    
                                'status':'success',
                                "sheet_id"  : sheet_id,
                                "file_id"  : file_id
                            }
                elif status == 'ERROR' or uplaod_status.status_code == 500:
                    raise Exception('Upload Failed')

        else:
                self.task_instance.xcom_push("CLEANSING_PASSED",False,self.execution_date)
                raise Exception('Cleansing Failed')