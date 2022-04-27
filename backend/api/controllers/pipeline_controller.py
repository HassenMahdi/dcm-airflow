#!/usr/bin/python
# -*- coding: utf-8 -*-
import traceback

from flask import jsonify, request
from flask_restx import Namespace, Resource

from api.documents.dataflow_document import DataFlowDocument
from api.services.airflow_service import publish_to_airflow
from api.services.pipeline_service import save_pipeline

api = Namespace("dataflow")

@api.route("/list-pipelines", methods=["GET"])
@api.route("/save", methods=["POST"])
@api.route("/delete/<pipe_id>/", methods=["DELETE"])
class Pipelines(Resource):

    @api.doc("Returns all pipelines' metadata")
    def get(self):
        dataflow_document = DataFlowDocument()
        pipes = dataflow_document.list_pipelines()
        return jsonify(pipes)

    @api.doc("Saves or update a pipeline")
    def post(self):
        try:
            template = request.json
            pipe_id = save_pipeline(template)

            return jsonify(pipe_id)

        except Exception:
            traceback.print_exc()

    @api.doc("Deletes a pipeline")
    def delete(self, pipe_id):
        try:
            dataflow_document = DataFlowDocument()
            pipes = dataflow_document.delete_pipeline(pipe_id)

            return jsonify(True)

        except Exception:
            traceback.print_exc()


@api.route("/<pipe_id>/list-nodes")
class Nodes(Resource):

    @api.doc("Lists all nodes and links in a pipeline")
    def get(self, pipe_id):
        try:
            dataflow_document = DataFlowDocument()
            pipeline = dataflow_document.get_pipeline(pipe_id)

            return jsonify(pipeline)

        except Exception:
            traceback.print_exc()


@api.route("/<pipe_id>/publish")
class PublishNodes(Resource):

    @api.doc("Publish Pipeline")
    def post(self, pipe_id):
        return publish_to_airflow(pipe_id)