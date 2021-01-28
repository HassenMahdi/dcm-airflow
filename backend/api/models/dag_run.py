#!/usr/bin/python
# -*- coding: utf-8 -*-

from api.db.airflow import db


class DagRun(db.Model):
    __tablename__ = 'dag_run'

    id = db.Column(db.Integer, primary_key=True)
    dag_id = db.Column(db.String)
    execution_date = db.Column(db.Date)
    run_id = db.Column(db.Integer)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    state = db.Column(db.String)
    conf = db.Column(db.PickleType)

    tasks = None