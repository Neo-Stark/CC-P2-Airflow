import json
from os import sep
import time
from datetime import timedelta

import pandas as pd
import pymongo
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# CONSTANTS
DATA_DIR = '/tmp/datos'
MONGO_CLIENT = 'mongodb+srv://neostark:T19blHfuaefxocwA@sandbox.l4kky.mongodb.net'

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fran98@correo.ugr.es'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
}
with DAG(
    'heroku_deploy',
    default_args=default_args,
    description='Predicción de tiempo en San Francisco para las próximas 24, 48 y 72 horas',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['CC'],
) as dag:
  heroku_app = BashOperator(
    task_id="heroku_app",
    bash_command="cd /tmp/services && heroku git:remote -a forecast-cc && git push heroku main"
  )