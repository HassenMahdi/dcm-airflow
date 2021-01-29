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
    paused = False

    @staticmethod
    def get_by_id(run_id):
        return DagRun.query.filter_by(run_id=run_id).first()

    @staticmethod
    def change_state(run_id, state, session=None):
        session = session or db.session
        try:
            qry = session.query(DagRun).filter(DagRun.run_id == run_id)
            d = qry.first()
            d.state = state
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()