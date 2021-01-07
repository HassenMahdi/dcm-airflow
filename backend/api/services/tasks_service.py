from api.models.dag_run import DagRun


def get_runs(dag_id):
    cursor = DagRun.query.filter_by(dag_id=dag_id).all()
    return list(cursor)


def get_run_details(run_id):
    dag_run = DagRun.query.filter_by(run_id=run_id).first()
    return dag_run