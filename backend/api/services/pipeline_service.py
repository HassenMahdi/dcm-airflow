#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import current_app

from api.documents.dataflow_document import DataFlowDocument
from api.services.airflow_service import publish_to_airflow, run_dag_in_airflow
from api.utils.utils import generate_id

dataflow_document = DataFlowDocument()

def save_pipeline(template):

    if not template.get("pipeline_id"):
        template["pipeline_id"] = generate_id()

    dataflow_document.save_pipeline(template)

    return template["pipeline_id"]


def publish_pipeline(pipeline_id):
    pipeline = dataflow_document.get_pipeline(pipeline_id)

    try:
        publish_to_airflow(pipeline)
    except Exception as e:
        return {'status': 'fail', "message": repr(e)}

    return {'status': 'success', "message": "Pipeline published successfully"}


def run_pipeline(pipeline_id):
    pipeline = dataflow_document.get_pipeline(pipeline_id)

    try:
        run_data = run_dag_in_airflow(pipeline)
    except Exception as e:
        return {'status': 'fail', "message": repr(e)}

    return {'status': 'success', "message": "Pipeline published successfully", **run_data}


