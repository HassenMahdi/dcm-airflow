from flask_restplus import Api
from flask import Blueprint

from api.controllers.monitor_controller import api as monitor
from api.controllers.pipline_controller import pipeline_namespace as pipeline

blueprint = Blueprint('api', __name__)

api = Api(blueprint,
          title='AIRFLOW BACKEND',
          version='1.0',
          description=''
          )

api.add_namespace(monitor, path='/')
api.add_namespace(pipeline, path='/')