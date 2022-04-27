#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import request
from flask_restx import Namespace, Resource

from api.services.airflow_service import run_dag_in_airflow, un_pause_dag, pause_dag, pulsate_run, \
    retry_failed_run

api = Namespace("run")


@api.route("/dag/<pipe_id>")
class PublishNodes(Resource):

    @api.doc("Publish Pipeline")
    def post(self, pipe_id):
        run_params = request.json
        return run_dag_in_airflow(pipe_id, run_params)


@api.route("/dag/<pipe_id>/unpause")
class UnPauseDag(Resource):
    @api.doc("Publish Pipeline")
    def post(self, pipe_id):
        run_params = request.json
        return un_pause_dag(pipe_id)


@api.route("/dag/<pipe_id>/pause")
class PauseDag(Resource):
    @api.doc("Publish Pipeline")
    def post(self, pipe_id):
        run_params = request.json
        return pause_dag(pipe_id)


@api.route("/<run_id>/pulsate")
class PauseDag(Resource):
    @api.doc("Pulsate Run")
    def post(self, run_id):
        run_params = request.json
        return pulsate_run(run_id)


@api.route("/<run_id>/retry")
class RetryDag(Resource):
    @api.doc("Retry Failed Tasks For Run")
    def post(self, run_id):
        run_params = request.json
        return retry_failed_run(run_id, run_params.get('tasks', None))
