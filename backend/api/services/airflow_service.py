import json

from flask import current_app
from pip._vendor import requests

from api.services.pipeline_service import get_pipeline


def publish_to_airflow(pipe_id):
    pipeline = get_pipeline(pipe_id)
    airflow_uri = current_app.config["AIRFLOW_ENDPOINT"]
    dag_definition = dict(
        id=pipeline['pipeline_id'],
        nodes=pipeline['nodes'],
        links=pipeline['links']
    )
    response = requests.post(url=f"{airflow_uri}dags/", json=dag_definition)

    if response.status_code == 200:
        return {"status": "success", "message": "Dag Created From Pipeline"}
    else:
        raise Exception('Failed to publish Pipeline')


def run_dag_in_airflow(pipeline_id, run_params={}):
    airflow_uri = current_app.config["AIRFLOW_ENDPOINT"]

    payload = {
        "dag_id": pipeline_id,
        "conf": run_params
    }
    response = requests.post(url=f"{airflow_uri}dags/trigger", json=payload)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception('Failed to Trigger Pipeline')
