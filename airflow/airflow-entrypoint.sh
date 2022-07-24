#!/usr/bin/env bash
airflow db init
airflow webserver
airflow scheduler