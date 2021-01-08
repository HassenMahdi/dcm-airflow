#!/usr/bin/python
# -*- coding: utf-8 -*-
import traceback

from flask import jsonify
from flask_restx import Namespace, Resource

from api.documents.dataflow_document import DataFlowDocument


pipeline_namespace = Namespace('monitor')


@pipeline_namespace.route('/list')
class ListingPiplines(Resource):
    @pipeline_namespace.doc("Returns the data check results metadata")
    def get(self):
        try:
            dataflow_document = DataFlowDocument()
            pipes = dataflow_document.list_pipelines()
            return jsonify(pipes)
        except Exception:
            traceback.print_exc()