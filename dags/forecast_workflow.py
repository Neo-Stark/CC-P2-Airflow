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
    'forecast',
    default_args=default_args,
    description='Predicción de tiempo en San Francisco para las próximas 24, 48 y 72 horas',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['CC'],
) as dag:

  # Fase 1: Descarga y descompresión de datos de entrenamiento
  t1 = BashOperator(
      task_id='download_temperature',
      bash_command=f'curl -L --create-dirs -o {DATA_DIR}/temperature.csv.zip\
      https://github.com/manuparra/MaterialCC2020/raw/master/temperature.csv.zip',
  )
  t2 = BashOperator(
      task_id='download_humidity',
      bash_command=f'curl -L --create-dirs -o {DATA_DIR}/humidity.csv.zip\
      https://github.com/manuparra/MaterialCC2020/raw/master/humidity.csv.zip',
  )
  t3 = BashOperator(
      task_id='unzip_temperature',
      bash_command=f'unzip -o {DATA_DIR}/temperature.csv.zip -d {DATA_DIR}/',
  )
  t4 = BashOperator(
      task_id='unzip_humidity',
      bash_command=f'unzip -o {DATA_DIR}/humidity.csv.zip -d {DATA_DIR}/',
  )
  t1 >> t3
  t2 >> t4

  # Fase 2: Preprocesamiento de datos
  def preprocessing():
    temperature   = pd.read_csv(f'{DATA_DIR}/temperature.csv')
    humidity      = pd.read_csv(f'{DATA_DIR}/humidity.csv')

    humidity_sf          = humidity['San Francisco']
    temperature_sf      = temperature['San Francisco']
    datetime            = temperature['datetime']

    col_names = {'DATE':datetime, 'TEMP':temperature_sf, 'HUM':humidity_sf}
    dataframe = pd.DataFrame(data=col_names)
    dataframe = dataframe.dropna()
    dataframe.to_csv(f'{DATA_DIR}/forecast_sf.csv', sep=';', encoding='utf-8', index=False)
  
  preprocess_data = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocessing,
  )

  [t3,t4] >> preprocess_data

  # Fase 3: Almacenamiento en BBDD
  def save_csv_db(client, db, collection, file_name):
    # guardando el modelo en mongo
    myclient = pymongo.MongoClient(client)
    mydb = myclient[db]
    mycol = mydb[collection]
    data = pd.read_csv(f'{DATA_DIR}/{file_name}', sep=';')
    data_dict = data.to_dict('records')
    # limpiamos los datos anteriores
    mycol.drop()
    # insertamos los nuevos
    mycol.insert_many(data_dict)

  save_data = PythonOperator(
    task_id='save_data',
    python_callable=save_csv_db,
    op_kwargs= {
      'client': MONGO_CLIENT,
      'db': 'forecast',
      'collection': 'training_data',
      'file_name': 'forecast_sf.csv'
    }
  )

  preprocess_data >> save_data
  
  # Fase 4: Realizar predicciones 
  # Descargamos (clonamos) el código necesario desde github
  get_code = BashOperator(
    task_id='get_code',
    bash_command='rm -r /tmp/services; git clone https://github.com/Neo-Stark/CC-P2-Services.git /tmp/services'
  )

  # Testeamos que el servicio funciona correctamente
  test_service = BashOperator(
    task_id="test_service",
    bash_command='cd /tmp/services && pytest tests.py'
  )

  # Calculamos los modelos (o el modelo, ya que en la segunda versión usamos algorithmia)
  make_model_v1 = BashOperator(
    task_id='make_model_v1',
    bash_command='python /tmp/services/v1/arima_prediction.py'
  )
  
  get_code >> test_service 
  [test_service, save_data] >> make_model_v1

  #Fase 5: Despliegue
  heroku_app = BashOperator(
    task_id="heroku_app",
    bash_command="cd /tmp/services && heroku git:remote -a forecast-cc && git push heroku main"
  )

  make_model_v1 >> heroku_app
