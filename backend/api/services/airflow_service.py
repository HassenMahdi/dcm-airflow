import json

from flask import current_app
from pip._vendor import requests


def publish_to_airflow(pipeline):
    airflow_uri = current_app.config["AIRFLOW_ENDPOINT"]

    response = requests.post(url=f"{airflow_uri}dags/", json=pipeline)

    if response.status_code == 200:
        return
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
        return response.text
    else:
        raise Exception('Failed to Trigger Pipeline')
