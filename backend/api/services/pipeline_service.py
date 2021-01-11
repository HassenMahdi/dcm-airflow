#!/usr/bin/python
# -*- coding: utf-8 -*-
from api.documents.dataflow_document import DataFlowDocument
from api.utils.utils import generate_id


def save_pipeline(template):

    if not template.get("pipe_id"):
        template["pipeline_id"] = generate_id()

    dataflow_document = DataFlowDocument()
    dataflow_document.save_pipeline(template)

    return template["pipeline_id"]