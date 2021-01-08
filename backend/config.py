import os

class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True
    MONGO_URI = "mongodb://dcm-consmos" \
                ":pUQRAZMYnTiYikWTxjcq7zQch27litMHCSJnHOu9XCssYxBqVRWmMpd8sSnd0G7w66dQ7GMS4UK8iAvOsoBGtw==@dcm" \
                "-consmos.mongo.cosmos.azure.com:10255/dcm?ssl=true&replicaSet=globaldb&retrywrites=false" \
                "&maxIdleTimeMS=120000&appName=@dcm-consmos@"


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