from flask import current_app
from pip._vendor import requests


def publish_to_airflow(pipeline):
    airflow_uri = current_app.config["AIRFLOW_ENDPOINT"]

    response = requests.post(url=f"{airflow_uri}dags/", json=pipeline)

    if response.status_code == 200:
        return
    else:
        raise Exception('Failed to publish Pipeline')
