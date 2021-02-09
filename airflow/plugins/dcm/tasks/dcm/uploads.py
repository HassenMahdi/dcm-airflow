import os

import requests
from airflow.plugins.dcm.tasks.dcm import DcmService


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


class UploadCollectionConnectorHandler(BaseUploadHandler):

    upload_collection_connector_type = "COLLECTION_UPLOAD"
    cleansing_url = os.getenv("DCM_SERVICE_CLEANSING")

    def run(self, params):
        # FORM PAYLOAD
        file_id = self.input['file_id']
        sheet_id = self.input['sheet_id']
        mapping_id = self.input["mapping_id"]
        """
        WRONG PAYLOAD FOR CLEANSING
        USE CORRECT ONE
        {"job_id":null,"file_id":"96bbf4bb9cda841457b659ee803b08cfe4b78b30d45644467090b6b1d14bb440","worksheet_id":"563848FB498B4078842DB94D69254B1BA49E8473F028700ADA11A5EEFBD38025","mapping_id":"f640ff67a6a7ac528e42bc8bc10141878e6b48f7e9d4868567274a710b9bac79","domain_id":"devDomain_devCollection","is_transformed":false,"modifications":{}}
        """
        payload = {
            **params,
            "file_id": file_id,
            "sheet_id": sheet_id,
            "mapping_id": mapping_id
        }
        run_cleansing = requests.post(url=f"{self.cleansing_url}", json=payload)
        job_status = requests.get(url=f"{self.cleansing_url}/metadata/{run_cleansing.json()['job_id']}")
        if job_status.json()["totalErrors"] == 0:
            """
            UPLAOD NOT WITH CONNECTOR
            USE EXISTING UPLOAD SERVICE
            USE POOLING EACH 2000ms
            START JOB REQUEST : POST https://dcm-upload.azurewebsites.net/upload/flow
            payload : {"id":null,"tags":["tag1","tag2"],"domain_id":"1CAA0B59809F4BCB806F7DBC53F23888","sheet_id":"0288FC7A2165A2773E374834FCBE8B7FBBCEDF27980067E6DCE4B9C9FE554673","file_id":"3a6cce0e9b360c18f2213b55bd8df648b2b2b77df9ab7be7a611c4902729c456","cleansing_job_id":"F7767EBA28A44E3E89F7A589B5B74253","transformation_id":null,"user_id":"d4ce2bac-ea05-4404-a617-ed3cdfa6c770","mapping_id":"2c211ab7323353f893736ae308bda4cbfd71591348a2ae583016dfe910900124"}
            response : "2A3E77CB69C94409BC1E6A21FCA8E809" < flow_id just a sting
                                                                                            flow_id
                                                                                               v
            GET RESULT REQUEST : GET https://dcm-upload.azurewebsites.net/upload/flow/2A3E77CB69C94409BC1E6A21FCA8E809/status/
            """
            run_upload = requests.post(url=f"{self.base_url}connector", json=payload)
            if run_upload.status_code == 200:
                return {'status':'success'}
            else:
                raise Exception('Upload has Failed')
        else:
            return {"status": "cleansing error"}