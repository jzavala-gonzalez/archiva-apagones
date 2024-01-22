import os
from datetime import datetime, timezone, timedelta

def get_atc_now():
    """Get current time in AST (GMT-4)"""
    # Get current UTC time
    utc_now = datetime.utcnow()

    # Define the AST timezone offset (4 hours behind UTC)
    ast_offset = timedelta(hours=-4)

    # Create a timezone object for AST (GMT-4)
    ast_tz = timezone(ast_offset)

    # Apply the AST timezone offset to the current UTC time
    ast_time = utc_now.replace(tzinfo=timezone.utc).astimezone(ast_tz)

    return ast_time

def get_all_objects(s3_client, prefix=None):
    ''' Get list of all objects in the bucket '''
    paginator = s3_client.get_paginator('list_objects_v2')
    if prefix is None:
        pages = paginator.paginate(Bucket='archiva-apagones')
    else:
        pages = paginator.paginate(Bucket='archiva-apagones', Prefix=prefix)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                yield obj

def download_object(s3_manager, obj_key, local_path, verbose=False):
    ''' Download an object from S3 to a local path '''
    file_directory = os.path.dirname(local_path)
    os.makedirs(file_directory, exist_ok=True)
    if verbose:
        print(f'Downloading {obj_key} to {local_path}')
    # s3_client.download_file('archiva-apagones', obj_key, local_path) # when using just a client
    s3_manager.download(
        bucket='archiva-apagones',
        key=obj_key,
        fileobj=local_path,
    )
