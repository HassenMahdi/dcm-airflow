#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask_restx import Resource, fields, Namespace
from flask import request


from api.services.tasks_service import update_task

api = Namespace('task')

task_instance = api.model('dag_task', {
        "status": fields.String,
        "output": fields.Raw,
    })

@api.route('/<run_id>/<task_id>/<execution_date>')
class TaskResource(Resource):

    @api.expect(task_instance)
    def put(self, dag_id, task_id, execution_date):
        body = request.json
        
        status = body['status']
        output = body['output']
        
        update_task(dag_id, task_id, execution_date, status, output)
    
        return {"status":"success", "message": "task updated"}, 200

