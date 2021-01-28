from dcm.tasks.dcm import DcmService
import requests
import os
import json

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
            raise Exception('Upload has Failed')