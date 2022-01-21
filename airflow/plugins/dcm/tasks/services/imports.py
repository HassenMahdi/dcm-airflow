
import requests
import json
import os
from dcm.tasks.services import DcmService


class BaseImportHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__IMPORT")

class ImportConnectorHandler(BaseImportHandler):
    import_connector_types = ["MONGODB_IMPORT_CONNECTOR", "BLOB_STORAGE_IMPORT_CONNECTOR", "SQL_IMPORT_CONNECTOR", "POSTGRES_IMPORT_CONNECTOR", "COLLECTION_IMPORT"]
    def run(self, params):
        response = requests.post(url=f"{self.base_url}connectors/",json=params)
        response_body =  json.loads(response.text)
        return  {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import'
        }

class ImportManualHandler(BaseImportHandler):
    def run(self, params):
        return  {
            "sheet_id":params['sheet_id'],
            "file_id":params['file_id'],
            "folder":'import'
        }
