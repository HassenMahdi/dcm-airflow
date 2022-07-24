FROM apache/airflow:1.10.14-python3.8

COPY ./plugins /opt/airflow/plugins
COPY airflow-entrypoint.sh /
ENTRYPOINT ["sh", "/airflow-entrypoint.sh"]