import requests
import os
import json

class DcmService:
    base_url = None
    
    @property
    def input(self, input_type='INPUT'):
        upstream = self.upstreams.get(input_type, None)
        if upstream:
            return self.task_instance.xcom_pull(task_ids=upstream["upstream_task_id"], key=upstream["upstream_input_type"])
        return None

    def get_input(self, input_type):
        upstream = self.upstreams[input_type]
        if upstream:
            return self.task_instance.xcom_pull(task_ids=upstream["upstream_task_id"], key=upstream["upstream_input_type"])
        return None
    

    def __init__(self, context) -> None:
        self.context = context
        self.dag_run = self.context["dag_run"]
        self.config = self.dag_run.conf or {}
        
        self.execution_date = self.context['execution_date']

        # TASK PARAMS
        self.parameters = self.context['task_parameters']
        self.run_as_preview = self.config.get('preview', False)
        # CLEAN UP PARAMS
        self.parameters.pop('__gohashid', None)
        self.parameters.update({
            "preview": self.run_as_preview
        })

        self.task_instance = self.context['task_instance']
        self.task_id = self.context['task_instance'].task_id
        self.key = self.parameters['key']
        
        # GENERATE LIST OF [{upstream_task_id, upstream_input_type, ti_input_type}]
        self.upstreams = self.get_upstreams()

    def start(self):
        # REGITER THE INPUT
        self.task_instance.xcom_push("INPUT",self.input,self.execution_date)

        result = self.run(self.parameters)
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