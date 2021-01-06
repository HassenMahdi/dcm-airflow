# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
from rest_api.generator import generator

# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = [generator]
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []