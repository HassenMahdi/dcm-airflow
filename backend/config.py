import os

class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True

    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Bxia!2020DaaTa1920CAvlmd@a4ec5441fc63a4fefbc97353d13465d2-1236515762.eu-west-3.elb.amazonaws.com:27017/dcm?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false")
    AIRFLOW_LOG_FOLDER = os.getenv("AIRFLOW_LOG_FOLDER","C://Users//Hassen//Desktop//DCM//dcm-airflow//airflow-logs")
    SQLALCHEMY_DATABASE_URI = os.getenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN', 'postgresql+psycopg2://airflow:airflow@localhost:5433/airflow')
    AIRFLOW_ENDPOINT = os.getenv('AIRFLOW_ENDPOINT', 'http://localhost:8080/admin/')


class DevelopmentConfig(Config):
    DEBUG = True

config = dict(
    dev=DevelopmentConfig,
)
