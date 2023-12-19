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
url = 'https://api.miluma.lumapr.com/miluma-outage-api/outage/regionsWithoutService'
r = requests.get(url)
print(r)
print(r.headers)
r.raise_for_status()
print()

if r.headers['Content-Type'] != 'application/json':
    raise Exception('Unexpected content type: ' + r.headers['Content-Type'])

regions_dict = r.json()
# regions_dict['timestamp_saved'] = timestamp_saved

regions_json_string = json.dumps(regions_dict, indent=2, sort_keys=True)

# print('Probando desde boto3:')
s3 = boto3.client('s3',
    endpoint_url='https://'+os.getenv('DUCKDB_S3_ENDPOINT'),
)

filekey = f'regions_without_service/{date_saved}/regions_without_service__{timestamp_saved}.json'
print(f"Saving to R2 at '{filekey}'")
s3.put_object(
    Bucket='archiva-apagones',
    Key=filekey,
    Body=regions_json_string,
)