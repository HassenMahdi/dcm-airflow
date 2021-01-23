from api.models.dag_run import DagRun
from api.services.tasks_service import get_run_tasks


def get_runs(dag_id):
    selector = DagRun.query
    if dag_id:
        selector = selector.filter_by(dag_id=dag_id)

    cursor = selector.all()
    return list(cursor)


def get_run_details(run_id):
    dag_run = DagRun.query.filter_by(run_id=run_id).first()

    tasks = get_run_tasks(dag_run.dag_id, dag_run.execution_date)

    dag_run.tasks = tasks

    return dag_run