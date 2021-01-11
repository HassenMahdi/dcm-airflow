#!/usr/bin/python
# -*- coding: utf-8 -*-
import traceback

from flask import jsonify, request
from flask_restx import Namespace, Resource

from api.documents.dataflow_document import DataFlowDocument
from api.services.pipeline_service import save_pipeline


dataflow_namespace = Namespace("dataflow")


@dataflow_namespace.route("/list-pipelines", methods=["GET"])
@dataflow_namespace.route("/save", methods=["POST"])
@dataflow_namespace.route("<pipe_id>/", methods=["PUT"])
class ListingPipelines(Resource):

    @dataflow_namespace.doc("Returns all pipelines' metadata")
    def get(self):
        try:
            dataflow_document = DataFlowDocument()
            pipes = dataflow_document.list_pipelines()
            return jsonify(pipes)

        except Exception:
            traceback.print_exc()

    @dataflow_namespace.doc("Saves or update a pipeline")
    def post(self):
        try:
            template = request.json
            pipe_id = save_pipeline(template)

            return jsonify(pipe_id)

        except Exception:
            traceback.print_exc()