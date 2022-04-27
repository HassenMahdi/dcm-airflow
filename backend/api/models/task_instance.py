#!/usr/bin/python
# -*- coding: utf-8 -*-
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import relationship

from api.db.airflow import db

# class TaskFail(db.Model):
#     __tablename__ = 'task_fail'
#
#     task_id = db.Column(db.String , primary_key=True)
#     dag_id = db.Column(db.String, primary_key=True)
#     execution_date = db.Column(db.Date , primary_key=True)
from api.models.xcom import XCom
from api.utils.utils import is_container


class TaskInstance(db.Model):
    __tablename__ = 'task_instance'

    # __table_args__ = (
    #     ForeignKeyConstraint(
    #         ['task_id', 'dag_id', 'execution_date'],
    #         ['task_fail.task_id', 'task_fail.dag_id', 'task_fail.execution_date']
    #     ),
    # )

    task_id = db.Column(db.String, primary_key=True)
    dag_id = db.Column(db.String, primary_key=True)
    execution_date = db.Column(db.Date, primary_key=True)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    state = db.Column(db.String)

    # task_fail = relationship("TaskFail", lazy=False)

    @property
    def output(self):
        return self.get_xcom("OUTPUT")

    @property
    def input(self):
        return self.get_xcom("INPUT")

    @property
    def cleansing_passed(self):
        return self.get_xcom("CLEANSING_PASSED")

    @property
    def cleansing_job_id(self):
        return self.get_xcom("CLEANSING_JOB_ID")
    
    @property
    def result_id(self):
        return self.get_xcom("result_id")

    def get_xcom(self, key):

        dag_id = self.dag_id
        task_ids = self.task_id

        query = XCom.get_many(
            execution_date=self.execution_date,
            key=key,
            dag_ids=dag_id,
            task_ids=task_ids,
            include_prior_dates=False,
        )

        # Since we're only fetching the values field, and not the
        # whole class, the @recreate annotation does not kick in.
        # Therefore we need to deserialize the fields by ourselves.
        if is_container(task_ids):
            vals_kv = {
                result.task_id: XCom.deserialize_value(result)
                for result in query.with_entities(XCom.task_id, XCom.value)
            }

            values_ordered_by_id = [vals_kv.get(task_id) for task_id in task_ids]
            return values_ordered_by_id
        else:
            xcom = query.with_entities(XCom.value).first()
            if xcom:
                return XCom.deserialize_value(xcom)

                
