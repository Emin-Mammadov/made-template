import pandas as pd
import urllib.request
import zipfile
import os
import sqlite3

url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
zip_path = 'mowesta-dataset-20221107.zip' 
extract_path = 'mowesta-dataset' 

urllib.request.urlretrieve(url, zip_path)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

data_csv_path = os.path.join(extract_path, 'data.csv')
data = pd.read_csv(data_csv_path)

data = data[['Geraet', 'Hersteller', 'Model', 'Monat', 
             'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv']]
data.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 
                     'Batterietemperatur in °C': 'Batterietemperatur'}, inplace=True)

data['Temperatur'] = data['Temperatur'] * 9/5 + 32
data['Batterietemperatur'] = data['Batterietemperatur'] * 9/5 + 32

data = data[data['Geraet'] > 0]

db_path = 'temperatures.sqlite'
conn = sqlite3.connect(db_path)
data.to_sql('temperatures', conn, if_exists='replace', index=False, 
            dtype={'Geraet': 'BIGINT', 
                   'Hersteller': 'TEXT', 
                   'Model': 'TEXT', 
                   'Monat': 'TEXT', 
                   'Temperatur': 'FLOAT', 
                   'Batterietemperatur': 'FLOAT', 
                   'Geraet aktiv': 'BIGINT'})

conn.close()
