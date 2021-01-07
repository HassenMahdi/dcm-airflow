import os

class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = ""


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@localhost:5433/airflow'


class EnvConfig(Config):
    TESTING = True



config = dict(
    prod=ProductionConfig,
    dev=DevelopmentConfig,
    env=EnvConfig
)