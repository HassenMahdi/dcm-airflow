#!/usr/bin/python
# -*- coding: utf-8 -*-
import traceback

from flask import jsonify, request
from flask_restx import Namespace, Resource

from api.documents.dataflow_document import DataFlowDocument
from api.services.pipeline_service import save_pipeline, publish_pipeline, run_pipeline

dataflow_namespace = Namespace("dataflow")

@dataflow_namespace.route("/list-pipelines", methods=["GET"])
@dataflow_namespace.route("/save", methods=["POST"])
@dataflow_namespace.route("/delete/<pipe_id>/", methods=["DELETE"])
class Pipelines(Resource):

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

    @dataflow_namespace.doc("Deletes a pipeline")
    def delete(self, pipe_id):
        try:
            dataflow_document = DataFlowDocument()
            pipes = dataflow_document.delete_pipeline(pipe_id)

            return jsonify(True)

        except Exception:
            traceback.print_exc()


@dataflow_namespace.route("/<pipe_id>/list-nodes")
class Nodes(Resource):

    @dataflow_namespace.doc("Lists all nodes and links in a pipeline")
    def get(self, pipe_id):
        try:
            dataflow_document = DataFlowDocument()
            pipeline = dataflow_document.get_pipeline(pipe_id)

            return jsonify(pipeline)

        except Exception:
            traceback.print_exc()


@dataflow_namespace.route("/<pipe_id>/publish")
class PublishNodes(Resource):

    @dataflow_namespace.doc("Publish Pipeline")
    def post(self, pipe_id):
        return publish_pipeline(pipe_id)


@dataflow_namespace.route("/<pipe_id>/run")
class PublishNodes(Resource):

    @dataflow_namespace.doc("Publish Pipeline")
    def post(self, pipe_id):
        return run_pipeline(pipe_id)