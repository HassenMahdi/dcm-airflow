import json

from flask import current_app
import requests

from api.db.airflow import db
from api.models.dag_model import DagModel
from api.models.dag_run import DagRun
from api.models.task_instance import TaskInstance
from api.services.pipeline_service import get_pipeline
from api.utils.utils import get_start_date


def publish_to_airflow(pipe_id):
    pipeline = get_pipeline(pipe_id)
    airflow_uri = current_app.config["AIRFLOW_ENDPOINT"]
    dag_definition = dict(
        id=pipeline['pipeline_id'],
        nodes=pipeline['nodes'],
        links=pipeline['links'],
        schedule_interval=pipeline.get("scheduler", None),
        start_date=get_start_date(pipeline.get("start_date"))
    )
    response = requests.post(url=f"{airflow_uri}dags/", json=dag_definition, headers={
                             'Content-type': 'application/json', 'Accept': 'text/plain'})

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


def un_pause_dag(dag_id):
    DagModel.change_pause_state(dag_id, False)


def pause_dag(dag_id):
    DagModel.change_pause_state(dag_id, True)


def pulsate_run(run_id):
    run = DagRun.get_by_id(run_id)
    DagRun.change_state(run_id, 'running')
    un_pause_dag(run.dag_id)


def retry_failed_run(run_id, tasks=None, states=["failed", "upstream_failed"]):
    session = db.session
    run = session.query(DagRun).filter_by(run_id=run_id).first()
    tasks = session.query(TaskInstance).filter(TaskInstance.state.in_(
        states)).filter_by(dag_id=run.dag_id, execution_date=run.execution_date).all()
    for t in tasks:
        t.state = None
    run.state = 'running'
    # SHOULD UNPAUSE DAG
    session.commit()
