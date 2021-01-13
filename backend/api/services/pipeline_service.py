#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import current_app

from api.documents.dataflow_document import DataFlowDocument
from api.utils.utils import generate_id

dataflow_document = DataFlowDocument()

def save_pipeline(template):

    if not template.get("pipeline_id"):
        template["pipeline_id"] = generate_id()

    dataflow_document.save_pipeline(template)

    return template["pipeline_id"]

def get_pipeline(pipe_id):
    return dataflow_document.get_pipeline(pipe_id)

