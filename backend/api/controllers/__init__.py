from flask_restx import Api
from flask import Blueprint

from api.controllers.monitor_controller import api as monitor
from api.controllers.pipeline_controller import api as dataflow
from api.controllers.run_controller import api as run
from api.controllers.task_controller import api as task

blueprint = Blueprint('api', __name__)

api = Api(blueprint,
          title='AIRFLOW BACKEND',
          version='1.0',
          description=''
          )

api.add_namespace(dataflow, path='')
api.add_namespace(monitor, path='')
api.add_namespace(run, path='')
api.add_namespace(task, path='')
