import json
import time
from datetime import timedelta

import pandas as pd
import pymongo
MONGO_CLIENT = 'mongodb+srv://neostark:T19blHfuaefxocwA@sandbox.l4kky.mongodb.net'
DATA_DIR = '.'
def save_csv_db(client, db, collection, file_name):
   # guardando el modelo en mongo
   myclient = pymongo.MongoClient(client)
   mydb = myclient[db]
   mycol = mydb[collection]
   data = pd.read_csv(f'{DATA_DIR}/{file_name}', sep=';')
   data_dict = data.to_dict('records')
   print(data_dict)
   mycol.drop()
   mycol.insert_many(data_dict)
   data_recovery = mycol.find({}, {'_id':0})
   df = pd.DataFrame(data_recovery)
   print(df)

save_csv_db(MONGO_CLIENT, 'forecast', 'training_data','forecast_sf.csv')