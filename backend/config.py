import os

class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True
    AIRFLOW_LOG_FOLDER = os.getenv("AIRFLOW_LOG_FOLDER","C://Users//Hassen//Desktop//DCM//dcm-airflow//airflow-logs")
    SQLALCHEMY_DATABASE_URI = os.getenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN', 'postgresql+psycopg2://airflow:airflow@localhost:5433/airflow')


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.getenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN', 'postgresql+psycopg2://airflow:airflow@localhost:5433/airflow')


config = dict(
    dev=DevelopmentConfig,
)