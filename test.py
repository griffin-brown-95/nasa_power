import os
from google.cloud import storage

# Set the path to your Google Cloud credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/griffinbrown/Desktop/Projects/nasa_power/erudite-creek-418015-9ca3a702a29e.json"

# Initialize the client
storage_client = storage.Client()

# List the buckets in your project
buckets = list(storage_client.list_buckets())
print("Buckets in your project:")
for bucket in buckets:
    print(bucket.name)
