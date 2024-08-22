import requests
import json
import pandas as pd
from google.cloud import storage
import os

def fetch_data(api_url, params):
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()

def transform_data_to_dataframe(data):
    # Extract parameters
    parameters = data['properties']['parameter']
    
    # Create DataFrames for each parameter
    df_t2m = pd.DataFrame(list(parameters['T2M'].items()), columns=['date', 'T2M'])
    df_t2m_max = pd.DataFrame(list(parameters['T2M_MAX'].items()), columns=['date', 'T2M_MAX'])
    df_t2m_min = pd.DataFrame(list(parameters['T2M_MIN'].items()), columns=['date', 'T2M_MIN'])
    df_ps = pd.DataFrame(list(parameters['PS'].items()), columns=['date', 'PS'])

    # Merge all DataFrames on 'date'
    df = df_t2m.merge(df_t2m_max, on='date')
    df = df.merge(df_t2m_min, on='date')
    df = df.merge(df_ps, on='date')
    
    return df

def upload_to_gcs(bucket_name, destination_blob_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/griffinbrown/Desktop/Projects/nasa_power/erudite-creek-418015-9ca3a702a29e.json"

    API_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
    PARAMETERS = {
        "latitude": 40.0,
        "longitude": -111.0,
        "start": 20201201,
        "end": 20201231,
        "parameters": "T2M,T2M_MAX,T2M_MIN,PS",
        "community": "AG",
        "format": "json"
    }
    BUCKET_NAME = "power-gcp"
    DESTINATION_BLOB_NAME = "raw_data/nasa_power_data.csv"
    
    # Fetch data from NASA POWER API
    data = fetch_data(API_URL, PARAMETERS)
    
    # Transform JSON to DataFrame
    df = transform_data_to_dataframe(data)
    
    # Save DataFrame to CSV
    csv_file_path = 'nasa_power_data.csv'
    df.to_csv(csv_file_path, index=False)
    
    # Upload CSV to Google Cloud Storage
    try:
        upload_to_gcs(BUCKET_NAME, DESTINATION_BLOB_NAME, csv_file_path)
    except Exception as e:
        print(f"An error occurred: {e}")
