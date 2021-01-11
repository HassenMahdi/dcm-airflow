#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime

from api.db.airflow import mongo


class DataFlowDocument:

    def list_pipelines(self, pipe_id=None):
        """Fetches all pipelines' metadata, if pipe_id, fetches a pipeline nodes and links"""

        dataflow = mongo.db.airflow_pipelines
        if pipe_id:
            pipe = self.get_pipeline(pipe_id)
            return {{"name": pipe["name"], "id": pipe["pipeline_id"], "created_at": pipe["created_at"]}}

        pipes = dataflow.find()
        return [{"name": pipe["name"], "id": pipe["pipeline_id"], "created_at": pipe["created_at"]} for pipe in pipes]

    def get_pipeline(self, pipe_id):
        """Fetches a pipeline document based on pipe_id"""

        dataflow = mongo.db.airflow_pipelines

        return dataflow.find_one({"pipeline_id": pipe_id})

    def save_pipeline(self, template):
        """Saves or updates a pipeline document"""

        dataflow = mongo.db.airflow_pipelines

        exist_pipeline = self.get_pipeline(template["pipeline_id"])
        if exist_pipeline:
            dataflow.update_one(
                    {'pipeline_id': template["pipeline_id"]},
                    {'$set': {
                        "nodes": template["nodes"],
                        "links": template["links"]
                    }
                    }, upsert=False
                )

        else:
            template["created_at"] = datetime.now()
            dataflow.insert_one(template)

    def delete_pipeline(self, pipe_id):
        """Deletes a pipeline document based on pipe_id"""

        dataflow = mongo.db.airflow_pipelines

        dataflow.delete_one({"pipeline_id": pipe_id})
