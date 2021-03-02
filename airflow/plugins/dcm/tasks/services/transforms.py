from dcm.tasks.services import DcmService
import requests
import os
import json
import time

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
        print(run_transf.text)
        while True:
                time.sleep(2)
                uplaod_status = requests.get(url=f"{self.base_url}transformation/{run_transf.text}/status")
                print(f"{self.base_url}transformation/{run_transf.text}/status")
                status = uplaod_status.json()['job_status']
                if status == 'DONE': 
                    return uplaod_status.json()['transformed_file_id'].replace("\\", "/")
                elif status == 'ERROR':
                    raise Exception('Transforming Failed')

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

class PipelineHandler(BaseTransformationHandler):
     def run(self, params):
        # SAVE TRANSFORMATION
        transformation_id = self.parameters['transformation_id']
        transformed_file_id = self.transform(self.input['file_id'],self.input['sheet_id'], transformation_id)
        return self.construct_output(transformed_file_id, transformation_id)

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
                **params,
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
        join_data = self.get_input('JOIN')
        node = {
                **params,
                "type": 'join',
                "join_file_id": join_data['file_id'],
                "join_sheet_id": join_data['sheet_id'],
            }
        transformation_id = self.save_transform(node)
        transformed_file_id = self.transform(self.input['file_id'],self.input['sheet_id'], transformation_id)
        return self.construct_output(transformed_file_id, transformation_id)

