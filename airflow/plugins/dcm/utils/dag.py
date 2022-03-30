import airflow.settings
from airflow.models import DagModel, DagRun
from sqlalchemy import and_

def stop_all_dags_runs(dag_id):
    session = airflow.settings.Session()
    try:
        qry = session.query(DagRun).filter(and_(DagRun.dag_id == dag_id, DagRun.state == 'running'))
        runs = qry.all()
        for run in runs:
            run.set_state('failed')
        session.commit()
    except:
        session.rollback()
    finally:
        session.close()

def unpause_dag(dag_id):
    change_dag_pause_state(dag_id, False)

def pause_dag(dag_id):
    change_dag_pause_state(dag_id, True)

def change_dag_pause_state(dag_id, pause_state):
    session = airflow.settings.Session()
    try:
        qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        d = qry.first()
        d.is_paused = pause_state
        session.commit()
    except:
        session.rollback()
    finally:
        session.close()


