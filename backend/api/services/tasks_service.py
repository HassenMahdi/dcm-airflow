from flask import current_app, send_file

from api.db.airflow import db
from api.models.task_instance import TaskInstance
import os


def get_run_tasks(dag_id, execution_date):
    cursor = TaskInstance.query.filter_by(dag_id=dag_id, execution_date=execution_date).all()
    return list(cursor)


def get_task_log(dag_id,task_id, execution_date):
    logs_folder = current_app.config['AIRFLOW_LOG_FOLDER']
    log_file_path = os.path.join(logs_folder, dag_id, task_id, execution_date.replace(":", ""), '1.log')
    return send_file(log_file_path)

def update_task(dag_id, task_id, execution_date, status, output):
    session = db.session
    taskInstance = session.query(TaskInstance).filter_by(task_id=task_id, dag_id=dag_id, execution_date=execution_date).first()
    
    taskInstance.state = status
    taskInstance.output = output
    
    session.commit()
    
