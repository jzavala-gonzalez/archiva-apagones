import os
import boto3
import json
import requests
from dotenv import load_dotenv
from utils import get_atc_now

# Para archivar la copia actual de regiones sin servicio

load_dotenv()  # take environment variables from .env.

timestamp_saved_datetime = get_atc_now()
timestamp_saved = timestamp_saved_datetime.strftime("%Y-%m-%dT%H-%M-%S%z")
date_saved = timestamp_saved_datetime.strftime("%Y-%m-%d")
print('timestamp saved:', timestamp_saved)

# Get the data from the API
url = 'https://operationdata.prepa.pr.gov/dataSource.js'
r = requests.get(url)
print(r)
print(r.headers)
r.raise_for_status()
print()

if r.headers['Content-Type'] != 'application/x-javascript':
    raise Exception('Unexpected content type: ' + r.headers['Content-Type'])

data_source_contents = r.text
print('data_source_contents:')
print(data_source_contents)
print()

s3 = boto3.client('s3',
    endpoint_url='https://'+os.getenv('DUCKDB_S3_ENDPOINT'),
)

filekey = f'genera/data_source/{date_saved}/genera_data_source__{timestamp_saved}.js'
print(f"Saving to R2 at '{filekey}'")
s3.put_object(
    Bucket='archiva-apagones',
    Key=filekey,
    Body=data_source_contents,
)