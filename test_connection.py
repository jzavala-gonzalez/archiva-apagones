import os
import duckdb
import boto3
from dotenv import load_dotenv

# Para probar que podamos conectarnos al bucket correctamente

load_dotenv()  # take environment variables from .env.

print('Probando desde boto3:')
s3 = boto3.client('s3',
    endpoint_url='https://'+os.getenv('DUCKDB_S3_ENDPOINT'),
)
object_information = s3.head_object(Bucket='archiva-apagones', Key='penguins.csv')
print(object_information)
print()

print('Probando desde duckdb:')
res = duckdb.sql(
'''
select *
from read_csv_auto('s3://archiva-apagones/penguins.csv')   
''')
print(res)