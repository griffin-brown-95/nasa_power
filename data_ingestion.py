import requests
import json
from google.cloud import storage
import os

def fetch_data(api_url, params):
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()

def upload_to_gcs(bucket_name, destination_blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "erudite-creek-418015-9ca3a702a29e.json"

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

    DESTINATION_BLOB_NAME = "raw_data/nasa_power_data.json"
        
        # Fetch data from NASA POWER API
    data = fetch_data(API_URL, PARAMETERS)
        
        # Upload data to Google Cloud Storage
    upload_to_gcs(BUCKET_NAME, DESTINATION_BLOB_NAME, data)

