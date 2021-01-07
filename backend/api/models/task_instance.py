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
    #
    # @property
    # def task_status(self):
    #     if self.task_fail:
    #         return "FAILED"
    #     elif self.end_date and self.start_date:
    #         return "SUCCESS"
    #     elif not self.end_date and not self.start_date:
    #         return "PENDING"
    #     else:
    #         return "RUNNING"
