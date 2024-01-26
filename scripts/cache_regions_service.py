import os
import sys
import botocore
import boto3
import boto3.s3.transfer as s3transfer
import time
import duckdb
from dotenv import load_dotenv
from utils import download_object, get_all_objects

load_dotenv()  # take environment variables from .env.

db_object_key = 'databases/regions_without_service_cache.db'
table_name = 'regions_without_service_staging'

## Check if local_bucket_path env var is set
local_bucket_path = os.getenv('LOCAL_BUCKET_PATH')
if local_bucket_path is None:
    print("Using default local bucket path: /tmp/archiva-apagones")
    local_bucket_path = '/tmp/archiva-apagones'
else:
    print(f"Using local bucket path: {local_bucket_path}")
local_db_path = os.path.join(local_bucket_path, db_object_key)
os.makedirs(local_bucket_path, exist_ok=True)
os.makedirs(os.path.dirname(local_db_path), exist_ok=True)

## Configure Boto3
botocore_config = botocore.config.Config(max_pool_connections=20)
s3 = boto3.client('s3',
    endpoint_url='https://'+os.getenv('DUCKDB_S3_ENDPOINT'),
    config=botocore_config
)
transfer_config = s3transfer.TransferConfig(
    use_threads=True,
    max_concurrency=20,
)
s3t = s3transfer.create_transfer_manager(s3, transfer_config)

## Check if the database exists in the cloud bucket
if os.path.isfile(local_db_path):
    print("Local DB exists. Deleting just in case.")
    os.remove(local_db_path)
try:
    s3.head_object(
        Bucket='archiva-apagones',
        Key=db_object_key,
    )
    remote_db_exists = True
    print("Remote DB exists. Downloading now...")
    download_object(
        s3,
        db_object_key,
        local_db_path,
        verbose=True,
    )
    if not os.path.isfile(local_db_path):
        raise Exception("Download failed???")
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        remote_db_exists = False
        print("Remote DB does not exist yet")
        
    else:
        raise e
print()


## Connect to the database
con = duckdb.connect(local_db_path)
tables = con.sql("show tables;").fetchall()
tables = [t[0] for t in tables]
table_exists = table_name in tables
database_size_string = con.sql('call pragma_database_size();').fetchone()[1]
# cast '1.8MB' string to float (eliminate all non-numeric characters)
database_size = float(''.join([c for c in database_size_string if c.isdigit() or c == '.']))
print(f"Initial database size: {database_size_string} ({database_size})")
print("Existing tables:", tables)

# Get list of all objects already cached in the database
if table_exists:
    cached_object_keys = con.sql(f"select distinct object_key from {table_name};").fetchall()
    cached_object_keys = [o[0] for o in cached_object_keys]
else:
    cached_object_keys = []
print(f"{len(cached_object_keys)} objects already cached in the database")

# Get list of all objects in the cloud bucket
prefix = 'regions_without_service/'
all_objects = list(get_all_objects(s3, prefix))
all_object_keys = [obj['Key'] for obj in all_objects]
all_object_keys = [o for o in all_object_keys if o.endswith('.json')]
print(f"{len(all_object_keys)} objects in the cloud bucket")

# Get list of objects in the local bucket
local_object_keys = []
for root, dirs, files in os.walk(local_bucket_path):
    for file in files:
        # Add to local_obj_list but strip off the local_bucket_path
        obj_key = os.path.join(root, file).replace(local_bucket_path, '')[1:]
        local_object_keys.append(obj_key)
local_object_keys = [o for o in local_object_keys if (o.startswith(prefix) and o.endswith('.json'))]
print(f"{len(local_object_keys)} objects in the local bucket")
# print("Last 10 local objects:")
# print(*sorted(local_object_keys)[-10:], sep='\n')
# print()

# Get list of objects missing from the database
missing_cache_keys = set(all_object_keys).difference(set(cached_object_keys))
num_missing_cache_keys = len(missing_cache_keys)
if num_missing_cache_keys > 0:
    print(f'{num_missing_cache_keys} objects missing in cache')
else:
    print("All objects are already cached. Closing script...")
    sys.exit(0)

# Get list of missing cache objects
# that are also missing from the local bucket
missing_local_keys = set(missing_cache_keys).difference(set(local_object_keys))
if len(missing_local_keys) == 0:
    print("All missing cache objects are already in the local bucket. No need to download more.")
    print()
else:
    print(f'{len(missing_local_keys)} of the missing cache objects are also missing from the local bucket. These will be downloaded now.')
# print(missing_local_keys)

# Download missing objects
pending_futures = []
start_time = time.time()
for obj_key in missing_local_keys:
    local_path = os.path.join(local_bucket_path, obj_key)
    download_future = download_object(s3t, obj_key, local_path, verbose=False)
    pending_futures.append(download_future)

# Wait for all downloads to finish
all_downloads_finished = False
while not all_downloads_finished:
    pending_futures = [f for f in pending_futures if not f.done()]
    if len(pending_futures) == 0:
        all_downloads_finished = True
        print("All downloads finished!")
        break
    else:
        print(f"{len(pending_futures)} downloads pending...")
        time.sleep(1)

end_time = time.time()
elapsed_time = end_time - start_time
if len(missing_local_keys) > 0:
    print(f"Elapsed time downloading: {elapsed_time} seconds")
    print()

# Load missing objects into the database
# Nota: Cargamos todos los objectos JSON disponibles localmente,
#       no solo los que faltan en la base de datos.
#       Mas adelante deduplicamos.
print("Loading local objects into the database...")
con.execute(
f'''
create or replace TEMP table local_raw_regions_without_service as (
    select *
    from read_json('{local_bucket_path}/regions_without_service/*/*.json', filename=true, auto_detect=true, format='auto') 
)
''')

print('local_raw_regions_without_service:')
print(con.sql(
'''
describe
    select
        *
    from local_raw_regions_without_service
'''))
print()

con.execute(
f'''
create or replace TEMP table local_regions_without_service_staging as (
    select
        strptime("timestamp", '%m/%d/%Y %I:%M %p') as "marca_hora_presentada",
        filename
            .string_split('__')[2]
            .regexp_extract('(.*).json', 1)
            .strptime('%Y-%m-%dT%H-%M-%S%z')
            ::TIMESTAMP -- drop timezone
            as "marca_hora_accedida",
        regions,
        totals,
        filename
            .string_split('{local_bucket_path}')[2]
            .ltrim('/')
            as object_key,
    from local_raw_regions_without_service
);
''')

print('local_regions_without_service_staging:')
print(con.sql(
'''
describe
    select
        *
    from local_regions_without_service_staging
'''))
print()

if not table_exists:
    print(f"Creating table {table_name}...")
    con.execute(f'''
    create table {table_name} as (
        select *
        from local_regions_without_service_staging
        order by object_key
    )
    ''')
else:
    print(f"Appending to table {table_name}...")
    con.execute(f'''
    create or replace table {table_name} as (
    with initial_union as (
        select *
        from {table_name}
        union all
        select *
        from local_regions_without_service_staging
    ),

    regions_with_dupe_counts as (
        select
            *,
            row_number() over (partition by object_key) as object_key_copy_number
        from initial_union
    )

    select * exclude (object_key_copy_number)
    from regions_with_dupe_counts
    where object_key_copy_number = 1
    order by object_key
    );
    ''')

print(f"{table_name}:")
print(con.sql(f'select * from {table_name}'))
print(f"Table {table_name} now has {con.execute(f'select count(*) from {table_name}').fetchone()[0]} rows")
con.close()

# Hack: save space by exporting and reimporting database
# see https://github.com/duckdb/duckdb/issues/8261
con = duckdb.connect(local_db_path)
print('Exporting database...')
con.execute("export database 'regions_without_service_cache';")
print('Closing local database connection...')
con.close()

os.remove(local_db_path)
print('Reimporting database...')
con = duckdb.connect(local_db_path)
con.execute("import database 'regions_without_service_cache';")
con.close()
print()

con = duckdb.connect(local_db_path)
new_database_size_string = con.sql('call pragma_database_size();').fetchone()[1]
# cast '1.8MB' string to float (eliminate all non-numeric characters)
new_database_size = float(''.join([c for c in new_database_size_string if c.isdigit() or c == '.']))
size_change = new_database_size - database_size
print(f"Initial database size: {database_size_string} ({database_size})")
print(f"    New database size: {new_database_size_string} ({new_database_size})")
print(f"          Size change: {size_change}")
print()

# Save the database back to the cloud bucket
print(f"Saving database to R2 at '{db_object_key}'...")
s3.put_object(
    Bucket='archiva-apagones',
    Key=db_object_key,
    Body=open(local_db_path, 'rb'),
)
# s3.put_object(
#     Bucket='publica-apagones',
#     Key=db_object_key,
#     Body=open(local_db_path, 'rb'),
# )
print('Done!')

