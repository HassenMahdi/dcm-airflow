#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime

from api.db.airflow import mongo


class DataFlowDocument:
    __TABLE__ = 'dataflow'

    def list_pipelines(self, uid, pipe_id=None):
        """Fetches all pipelines' metadata, if pipe_id, fetches a pipeline nodes and links"""

        dataflow = mongo.db[self.__TABLE__]
        if pipe_id:
            pipe = self.get_pipeline(pipe_id)
            return {"name": pipe["name"], "id": pipe["pipeline_id"], "created_on": pipe["created_on"],
                    "description": pipe["description"], "modified_on": pipe.get("modified_on")}

        pipes = dataflow.find({"uid": uid})
        return [{"name": pipe["name"], "id": pipe["pipeline_id"], "created_on": pipe["created_on"],
                 "description": pipe["description"], "modified_on": pipe.get("modified_on")} for pipe in pipes]

    def get_pipeline(self, pipe_id):
        """Fetches a pipeline document based on pipe_id"""

        dataflow = mongo.db[self.__TABLE__]

        return dataflow.find_one({"pipeline_id": pipe_id}, {"_id": 0})

    def save_pipeline(self, template):
        """Saves or updates a pipeline document"""

        dataflow = mongo.db[self.__TABLE__]

        exist_pipeline = self.get_pipeline(template["pipeline_id"])
        if exist_pipeline:
            dataflow.update_one(
                {'pipeline_id': template["pipeline_id"]},
                {'$set': {
                    "nodes": template["nodes"],
                    "links": template["links"],
                    "name": template["name"],
                    "description": template["description"],
                    "scheduler": template["scheduler"],
                    "modified_on": datetime.now()
                }
                }, upsert=False
            )

        else:
            template["created_on"] = datetime.now()
            dataflow.insert_one(template)

    def delete_pipeline(self, pipe_id):
        """Deletes a pipeline document based on pipe_id"""

        dataflow = mongo.db[self.__TABLE__]

        dataflow.delete_one({"pipeline_id": pipe_id})
