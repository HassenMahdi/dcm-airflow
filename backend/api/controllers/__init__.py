from flask_restplus import Api
from flask import Blueprint

from api.controllers.monitor_controller import api as monitor

blueprint = Blueprint('api', __name__)

api = Api(blueprint,
          title='AIRFLOW BACKEND',
          version='1.0',
          description=''
          )

api.add_namespace(monitor, path='/monitor')