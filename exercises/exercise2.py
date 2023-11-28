import pandas as pd
from sqlalchemy import create_engine

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
try:
    data = pd.read_csv(url, delimiter=';', error_bad_lines=False, na_values=['', '?'])
except pd.errors.ParserError as e:
    print(f"Error parsing CSV: {e}")
    
data.drop(columns=['Status'], inplace=True)
data.dropna(inplace=True)

data['Laenge'] = data['Laenge'].str.replace(',', '.').astype(float)
data['Breite'] = data['Breite'].str.replace(',', '.').astype(float)
data['DS100'] = data['DS100'].astype(str)
data['IFOPT'] = data['IFOPT'].astype(str)
data['NAME'] = data['NAME'].astype(str) 
data['Verkehr'] = data['Verkehr'].astype(str)
data['Betreiber_Name'] = data['Betreiber_Name'].astype(str)
data['Betreiber_Nr'] = data['Betreiber_Nr'].astype(int)

valid_data = data[
    (data['Verkehr'].isin(['FV', 'RV', 'nur DPN'])) &
    (data['Laenge'].between(-90, 90)) &
    (data['Breite'].between(-90, 90)) &
    (data['IFOPT'].str.match(r'^[a-zA-Z]{2}:\d+:\d+(?::\d+)?$'))
]

engine = create_engine('sqlite:///trainstops.sqlite')

valid_data.to_sql('trainstops', engine, if_exists='replace', index=False)
