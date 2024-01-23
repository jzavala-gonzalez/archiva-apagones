import duckdb

local_db_path = './archiva-apagones/databases/regions_without_service_cache.db'
table_name = 'regions_without_service_staging'

con = duckdb.connect(local_db_path)
print(f'{table_name}')
print(con.sql(f'select * from {table_name}'))
print()

print(f'old_{table_name}')
print(con.sql(f'select * from old_{table_name}'))
print()

print(f'new_{table_name}')
print(con.sql(f'select * from new_{table_name}'))
print()
con.close()