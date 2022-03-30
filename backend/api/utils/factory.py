#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
from flask import Flask
from flask_cors import CORS
from api.db.airflow import db, mongo
from config import config

from api.controllers import blueprint


def create_app(env):
    app = Flask(__name__)

    env_config = config.get(env)
    app.config.from_object(env_config)

    # REGISTER BLUEPRINTS HERE
    app.register_blueprint(blueprint)

    db.init_app(app)
    mongo.init_app(app)

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)s|%(message)s',
                        level=logging.DEBUG)

    CORS(app)
    return app
