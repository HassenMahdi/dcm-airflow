
import requests
import json
import os
from dcm_plugin.tasks.dcm import DcmService


class BaseImportHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__IMPORT")

class ImportConnectorHandler(BaseImportHandler):
    import_connector_types = ["BLOB_STORAGE_IMPORT_CONNECTOR", "SQL_IMPORT_CONNECTOR"]
    def run(self, params):
        response = requests.post(url=f"{self.base_url}connectors/import",json=params)
        response_body =  json.loads(response.text)
        return  {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import'
        }
