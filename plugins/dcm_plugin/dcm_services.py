import requests
import os
import json

def dcm_hook_factory(**context):
    
    task_type = context['task_type']
    
    handler = None
    if task_type == 'AZURE_STORAGE_ACCOUNT':
        handler = ImportAzureBlob
    elif task_type in TransformationHandler.transformation_types:
        handler = TransformationHandler
    elif task_type == 'concat':
        handler = ConcatHandler
    elif task_type == 'join':
        handler = JoinHandler
    elif task_type in BaseUploadHandler.uploader_types:
        handler = BaseUploadHandler
    else:
        raise f'NO HANDLER FOUND FOR {task_type} TYPE'

    handler(context).start()

    return


class DcmService:
    base_url = None
    
    @property
    def input(self, input_type='INPUT'):
        upstream = self.upstreams[input_type]
        return self.task_instance.xcom_pull(task_ids=upstream["upstream_task_id"], key=upstream["upstream_input_type"])

    def get_input(self, input_type):
        upstream = self.upstreams[input_type]
        return self.task_instance.xcom_pull(task_ids=upstream["upstream_task_id"], key=upstream["upstream_input_type"])
    

    def __init__(self, context) -> None:
        self.context = context
        self.execution_date = self.context['execution_date']
        self.parameters = self.context['task_parameters']
        self.task_instance = self.context['task_instance']
        self.task_id = self.context['task_instance'].task_id
        self.key = self.parameters['key']
        
        # GENERATE LIST OF [{upstream_task_id, upstream_input_type, ti_input_type}]
        self.upstreams = self.get_upstreams()

    def start(self):
        print(self.upstreams)
        task_parameters = self.context['task_parameters']
        result = self.run(task_parameters)

        # SHOULD ADD ABILITY TO HAVE MULTIPLE OUTPUTS
        self.task_instance.xcom_push("OUTPUT",result,self.execution_date)

        return result

    def run(self, params):
        # THIS IS THE INPUT FROM PREVIOUS TASK
        self.input
        # SHOULD ALWAYS RETURN THIS FORMAT
        # REPRESENTS DATASET
        return {
            "sheet_id":None,
            "file_id":None,
            "folder":None
        }

    def get_upstreams(self):
        upstream_map = {}
        links = self.context['dag_metadata']['dependincies']
        tasks = self.context['dag_metadata']['tasks']
        for l in links:
            if l['to']==self.key:
                upstream_map[l['from']]=dict(
                    upstream_task_id = None,
                    upstream_input_type = l['fromPort'],
                    ti_input_type = l['toPort'],
                )
        for t in tasks:
            if t['key'] in upstream_map.keys():
                upstream_map[t['key']]['upstream_task_id'] = t['key']

        return { v['ti_input_type']: v for k,v in upstream_map.items() }


# DATASOURCES
class ImportAzureBlob(DcmService):
    base_url = os.getenv("DCM_SERVICE__IMPORT")
    
    def run(self, params):
        payload = {
            "conn_string": params['conn_string'],
            "container": params['container'],
            "blob": params['blob'],
        }
        response = requests.post(url=f"{self.base_url}connectors/azure/import",json=payload)
        response_body =  json.loads(response.text)
        return  {
            "sheet_id":response_body['sheet_id'],
            "file_id":response_body['file_id'],
            "folder":'import'
        }

class BaseTransformationHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__TRANSFORM")
    def construct_output(self, transformed_file_id, transformation_id):
        return {
            "sheet_id": transformed_file_id.split('/')[-1],
            "file_id": transformed_file_id.split('/')[-2],
            "folder":'import',
            "transformation_id":transformation_id
        }

    def transform(self, file_id,sheet_id,transformation_id):
        run_transf = requests.get(url=f"{self.base_url}{file_id}/{sheet_id}/{transformation_id}")
        return json.loads(run_transf.text)['transformed_file_id'].replace("\\", "/")

    def save_transform(self, node):
        save_transf = requests.post(url=f"{self.base_url}",json={
            "name":f"{self.task_id}_{self.execution_date}",
            "created_on":str(self.execution_date),
            "modified_on":str(self.execution_date),
            "nodes":[node],
            "domain_id": None,
        })
        transformation_id = json.loads(save_transf.text)['id']
        return transformation_id
# TRANSFOMERES
class TransformationHandler(BaseTransformationHandler):
    transformation_types = ["filter", "find-replace", "merge", "replace", "delete-rows", "default-value", "split", "calculator", "format-date", "groupby", "pycode"]
    
    def run(self, params):
        # SAVE TRANSFORMATION
        transformation_id = self.save_transform(self.parameters)
        transformed_file_id = self.transform(self.input['file_id'],self.input['sheet_id'], transformation_id)
        return self.construct_output(transformed_file_id, transformation_id)

# Concat
class ConcatHandler(BaseTransformationHandler):
    def run(self, params):
        # SAVE TRANSFORMATION
        concat_data = self.get_input('CONCAT')
        node = {
                "type": 'concat',
                "concat_file_id": concat_data['file_id'],
                "concat_sheet_id": concat_data['sheet_id'],
            }
        transformation_id = self.save_transform(node)
        transformed_file_id = self.transform(self.input['file_id'],self.input['sheet_id'], transformation_id)
        return self.construct_output(transformed_file_id, transformation_id)


class JoinHandler(BaseTransformationHandler):
    def run(self, params):
        # SAVE TRANSFORMATION
        concat_data = self.get_input('JOIN')
        node = {
                "type": 'join',
                "join_file_id": concat_data['file_id'],
                "join_sheet_id": concat_data['sheet_id'],
            }
        transformation_id = self.save_transform(node)
        transformed_file_id = self.transform(self.input['file_id'],self.input['sheet_id'], transformation_id)
        return self.construct_output(transformed_file_id, transformation_id)


class BaseUploadHandler(DcmService):
    base_url = os.getenv("DCM_SERVICE__UPLOAD")
    uploader_types = ['UPLOAD_AZURE_STORAGE_ACCOUNT']

    def run(self, params):
        # FORM PAYLOAD
        file_id = self.input['file_id']
        sheet_id = self.input['sheet_id']
        payload = {
            **params,
            "file_id": file_id,
            "sheet_id": sheet_id
        }
        run_uplaod = requests.post(url=f"{self.base_url}", json=payload)
        if run_uplaod.status_code == 200:
            return {'status':'success'}
        else:
            raise Exception('Upload has Failed')
        
        # return self.construct_output(transformed_file_id, transformation_id)