#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import request, jsonify
from flask_restplus import Resource, fields, Namespace

from api.services.runs_service import get_runs, get_run_details
from api.services.tasks_service import get_task_log

api = Namespace('monitor')

dag_instance = api.model('dag_task', {
        "task_id": fields.String,
        "start_date": fields.DateTime,
        "end_date": fields.DateTime,
        "state": fields.String,
    })

dag_run = api.model('dag_run', {
        "id": fields.String,
        "dag_id": fields.String,
        "execution_date": fields.DateTime,
        "run_id": fields.String,
        "start_date": fields.DateTime,
        "end_date": fields.DateTime,
        "tasks": fields.List(fields.Nested(dag_instance))
    })

@api.route('/dag/<dag_id>/run')
class MonitorRuns(Resource):

    @api.marshal_list_with(dag_run)
    def get(self, dag_id):
        return get_runs(dag_id)


@api.route('/run/<run_id>')
class MonitorRun(Resource):

    @api.marshal_with(dag_run)
    def get(self, run_id):
        return get_run_details(run_id)


@api.route('/log/<dag_id>/<task_id>/<execution_date>')
class Logs(Resource):
    def get(self, dag_id,task_id, execution_date):
        return get_task_log(dag_id,task_id, execution_date)

