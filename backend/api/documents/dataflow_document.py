#!/usr/bin/python
# -*- coding: utf-8 -*-

from api.db.airflow import mongo


class DataFlowDocument:

    def list_pipelines(self):
        """Fetches all pipelines' metadata"""

        dataflow = mongo.db.airflow_pipelines

        pipes = dataflow.find()
        return [{"name": pipe["name"], "id": pipe["pipeId"], "created_at": pipe["createdAt"]} for pipe in pipes]
