import duckdb
from dotenv import load_dotenv

# Para probar que podamos conectarnos al bucket correctamente

load_dotenv()  # take environment variables from .env.

res = duckdb.sql(
'''
select *
from read_csv_auto('s3://archiva-apagones/penguins.csv')   
''')

print(res)