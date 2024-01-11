import pandas as pd
import urllib.request
import zipfile
import os
import sqlite3

# Download and extract dataset
url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
zip_path = 'mowesta-dataset-20221107.zip' 
extract_path = 'mowesta-dataset' 

urllib.request.urlretrieve(url, zip_path)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# Load data
data_csv_path = os.path.join(extract_path, 'data.csv')
data = pd.read_csv(data_csv_path, sep=';', on_bad_lines='skip')

# Select and rename columns
data = data[['Geraet', 'Hersteller', 'Model', 'Monat', 
             'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv']]
data.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 
                     'Batterietemperatur in °C': 'Batterietemperatur'}, inplace=True)

# Convert temperatures from Celsius to Fahrenheit
data['Temperatur'] = pd.to_numeric(data['Temperatur'], errors='coerce') * 9/5 + 32
data['Batterietemperatur'] = pd.to_numeric(data['Batterietemperatur'], errors='coerce') * 9/5 + 32

# Filter out rows where 'Geraet' is less than or equal to 0
data = data[data['Geraet'] > 0]

# Save to SQLite database
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
