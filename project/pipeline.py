import os
import requests
import sqlite3
import zipfile
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle(dataset, target_folder, bitcoin_filename):
    api = KaggleApi()
    api.authenticate()

    # Download the dataset zip file
    zip_file_path = os.path.join(target_folder, dataset.split('/')[-1] + '.zip')
    api.dataset_download_files(dataset, path=target_folder, unzip=False)

    # Extract only the Bitcoin CSV file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            if bitcoin_filename in member:
                zip_ref.extract(member, path=target_folder)
                break
            
    # Clean up the zip file
    os.remove(zip_file_path)
    
def download_file(url, target_folder, filename):
    response = requests.get(url)
    file_path = os.path.join(target_folder, filename)
    with open(file_path, 'wb') as file:
        file.write(response.content)
    return file_path

def process_sp500_data(df):
    df.rename(columns={'SP500': 'price', 'DATE': 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['price'].fillna(method='ffill', inplace=True)
    return df

def process_bitcoin_data(df):
    df.drop(columns=['Name', 'Symbol'], inplace=True)
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df = df[['SNo', 'Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'Marketcap']]
    return df

def align_dates(df1, df2):
    start_date = max(df1['Date'].min(), df2['Date'].min())
    end_date = min(df1['Date'].max(), df2['Date'].max())
    df1 = df1[(df1['Date'] >= start_date) & (df1['Date'] <= end_date)]
    df2 = df2[(df2['Date'] >= start_date) & (df2['Date'] <= end_date)]
    return df1, df2

def save_to_sqlite(df, db_name, table_name):
    db_path = os.path.join('../data', db_name)
    print(f"Saving to SQLite database at: {db_path}")  # Debug print
    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data saved to {db_name} in the {table_name} table.")  # Confirmation print
    except Exception as e:
        print(f"Error saving to SQLite database: {e}")  # Error print
    print(f"Absolute path for database: {os.path.abspath(db_path)}")



def main():
    # Define target folder
    data_folder = '../data'

    # Download and Process Bitcoin data
    kaggle_dataset = 'sudalairajkumar/cryptocurrencypricehistory'
    bitcoin_filename = 'coin_Bitcoin.csv'
    download_kaggle(kaggle_dataset, data_folder,bitcoin_filename)
    bitcoin_file_path = os.path.join(data_folder, bitcoin_filename)
    bitcoin_data_raw = pd.read_csv(bitcoin_file_path)
    bitcoin_data = process_bitcoin_data(bitcoin_data_raw)
    
    # Clean up Bitcoin CSV file
    os.remove(bitcoin_file_path)

    # Download and process S&P 500 data
    sp500_url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=SP500&scale=left&cosd=2013-11-18&coed=2021-07-06&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily%2C%20Close&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-11-18&revision_date=2023-11-18&nd=2013-11-18'
    sp500_filename = 'sp500_data.csv'
    sp500_file_path = download_file(sp500_url, data_folder, sp500_filename)
    sp500_data_raw = pd.read_csv(sp500_file_path)
    sp500_data = process_sp500_data(sp500_data_raw)
    
    # Clean up S&P 500 CSV file
    os.remove(sp500_file_path)

    # Align the date ranges of both datasets
    sp500_data, bitcoin_data = align_dates(sp500_data, bitcoin_data)

    # Save the processed data to SQLite databases
    save_to_sqlite(bitcoin_data, 'bitcoin_data.sqlite', 'bitcoin_prices')
    save_to_sqlite(sp500_data, 'sp500_data.sqlite', 'sp500_prices')

if __name__ == '__main__':
    main()
