#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import request, jsonify
from flask_restplus import Resource, fields, Namespace

from api.services.runs_service import get_runs

api = Namespace('monitor')


@api.route('run')
class MonitorRuns(Resource):
    def get(self):
        return get_runs(None)


@api.route('task')
class MonitorRuns(Resource):
    def get(self):
        pass

