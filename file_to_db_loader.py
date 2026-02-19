import pandas as pd
import os
import glob
import sys
import json
import re
from dotenv import load_dotenv

load_dotenv()

def get_column_name(schemas, ds_name, sorting_key='column_position'):
    column_details=schemas[ds_name]
    columns= sorted(column_details, key= lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    file_path_list = re.split(r'/', file)
    ds_name = file_path_list[2]
    columns = get_column_name(schemas, ds_name)
    df_reader= pd.read_csv(file, names=columns, chunksize=10000)
    return df_reader

def to_sql(df, db_conn_uri, ds_name):
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists='append',
        index=False
    )

def db_loader(src_base_dir, db_conn_uri,ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No Files found in {ds_name}')
    
    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_uri, ds_name)


def process_file(ds_names=None):
    src_base_dir = os.getenv('SRC_BASE_DIR')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas=json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f' Proceessing {ds_name}')
            db_loader(src_base_dir, db_conn_uri, ds_name)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'Error Processing {ds_name}')

if __name__ == '__main__':
    process_file()
