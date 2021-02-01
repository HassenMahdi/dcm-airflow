# This is the class you derive to create a plugin
from flask import Blueprint
from flask_admin import BaseView, expose

from flask_appbuilder import BaseView as AppBuilderBaseView

from airflow.www.app import csrf

from airflow import DAG
from airflow.utils import dates

from flask import request

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils import timezone
from dcm.utils.dag import unpause_dag, pause_dag, stop_all_dags_runs

import json 

# from airflow.utils.types import DagRunType
import uuid

imports_string = """
import codecs
import logging
from datetime import timedelta
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# IMPORT PLUGINS FOR DCM MODULES
from airflow.macros import dcm_hook_factory
"""

logging_string = """
logging.basicConfig(format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
        """

def camelCase(st):
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]

    
def generate_id():
    return uuid.uuid4().hex.upper()


# Creating a flask admin BaseView
class Dags(BaseView):
    @expose('/', methods=['GET'])
    def get(self):
        # dag_id = "ganareted"
        # create_dag(dag_id)
        return {
            "test": None
        }

    @csrf.exempt
    @expose('/', methods=['POST'])
    def post(self):
        paylaod = request.json
        print(request.json)

        dag_id = paylaod['id']
        schedule_interval = None
        default_args = {}

        tasks = paylaod['nodes']
        dependincies = paylaod['links']

        dag_definition = (
        f"dag = DAG("
            f"dag_id='{dag_id}',"
            f"default_args={default_args},"
            f"schedule_interval={schedule_interval},"
            f"start_date=days_ago(2),"
            f"is_paused_upon_creation=False,"
        f")"
        )

        with open(f"/opt/airflow/dags/{dag_id}.py", "a") as dag_file:
            # Clear File
            dag_file.truncate(0)
            dag_file.write(imports_string)
            dag_file.write(logging_string)
            dag_file.write('\n')

            # CREATE DAG
            dag_file.write(dag_definition)
            dag_file.write('\n')

            tasks_meta = [{
                "key": t["key"],
                "label": t["label"],
                "type": t["type"],
            } for t in tasks]

            #  CRAETE DAG TASKS
            for task in tasks:
                task_id = task["key"]
                task_label = task["label"]
                task_type = task["type"]

                op_kwargs = "{" + (
                    f"'task_parameters':{json.dumps(task)},"
                    f"'task_type':'{task_type}',"
                    f"'task_label':'{task_label}',"
                    f"'dag_metadata':"+"{"+f"'tasks':{json.dumps(tasks_meta)}, 'dependincies': {json.dumps(dependincies)} "+"}"+f","
                ) + "}"

                task_definition = (
                    f"task_{task_id} = PythonOperator("
                    f"    task_id='{task_id}',"
                    f"    python_callable=dcm_hook_factory,"
                    f"    provide_context=True,"
                    f"    op_kwargs={op_kwargs},"
                    f"    dag=dag,"
                    f")"
                )
                dag_file.write(task_definition)
                dag_file.write('\n')

            # ESTABLISH DEPENDENCIES
            for dependency in dependincies:
                dep_from_var = f"task_{dependency['from']}"
                dep_to_var = f"task_{dependency['to']}"
                dependency_definition = f"{dep_from_var}.set_downstream({dep_to_var})"
                dag_file.write(dependency_definition)
                dag_file.write('\n')

            # END
            dag_file.close()

        return {
            "status": 'success',
            "message": "Dag Created",
            "dag_id": dag_id
        }

    # Trigger Dag Maunally By API 
    @csrf.exempt
    @expose('/trigger', methods=['POST'])
    def trigger_post(self):
        payload = request.json
        trigger_dag_id = payload['dag_id']
        conf = payload['conf'] or {}
        preview = conf.get('preview', False)
        execution_date = timezone.utcnow()


        stop_all_dags_runs(trigger_dag_id)
        if preview:
            pause_dag(trigger_dag_id)
            run_id = "preview_" + generate_id()
        else:
            unpause_dag(trigger_dag_id)
            run_id = "manual_" + generate_id()

        trigger_dag(
            dag_id=trigger_dag_id,
            run_id=run_id,
            conf=conf,
            execution_date=execution_date,
            replace_microseconds=False,
        )

        return {
            'run_id': run_id,
            'execution_date': str(execution_date)
        }
        


generator = Dags(category="Generator", name="Dag Generator API")

