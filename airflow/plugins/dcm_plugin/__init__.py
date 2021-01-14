from airflow.plugins_manager import AirflowPlugin
from dcm_plugin.factory import dcm_hook_factory


class DcmAi(AirflowPlugin):
    name = "dcm_plugin"
    operators = []
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = [dcm_hook_factory]
    admin_views = []
    flask_blueprints = []
    menu_links = []