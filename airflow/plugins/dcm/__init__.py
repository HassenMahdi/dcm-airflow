from airflow.plugins_manager import AirflowPlugin
from dcm.factory import dcm_hook_factory
from dcm.utils.dag import unpause_dag, pause_dag


class DcmAi(AirflowPlugin):
    name = "dcm"
    operators = []
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = [dcm_hook_factory, unpause_dag, pause_dag]
    admin_views = []
    flask_blueprints = []
    menu_links = []