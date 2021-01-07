#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import request, jsonify
from flask_restplus import Resource, fields, Namespace

api = Namespace('monitor')


@api.route('run')
class MonitorRuns(Resource):
    def get(self):
        pass


@api.route('task')
class MonitorRuns(Resource):
    def get(self):
        pass

