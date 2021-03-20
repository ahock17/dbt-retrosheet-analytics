from util import AzureBlobFileDownloader
from psycopg2 import connect as pg_connect, sql as pg_sql
import configs
import os
import pandas as pd
import shutil

data_dict = [
    {
        'file_path': 'data/parkcode.txt',
        'table_name': 'park_code',
        'header': True,
        'delimiter': ',',
        'null_value': ''
    },
    {
        'file_path': 'data/people.txt',
        'table_name': 'people',
        'header': True,
        'delimiter': ',',
        'null_value': ''
    },
    {
        'file_path': 'data/teamabr.txt',
        'table_name': 'teams',
        'header': False,
        'delimiter': ',',
        'null_value': ''
    },
    {
        'file_path': 'data/game-logs/',
        'table_name': 'game_logs',
        'header': False,
        'delimiter': ',',
        'null_value': ''
    }
]

schema = 'stage'

def main():
    for data in os.listdir(configs.LOCAL_BLOB_PATH):
        full_path = os.path.join(configs.LOCAL_BLOB_PATH, data)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
        else:
            if data != '.gitkeep':
                os.remove(full_path)

    azure_blob_file_downloader = AzureBlobFileDownloader(configs.AZURE_STORAGE_CONNECTION_STRING, configs.BLOB_CONTAINER)
    azure_blob_file_downloader.download_all_blobs_in_container(configs.LOCAL_BLOB_PATH)
    
    def pg_truncate_table(conn, schema, table_name):
        try:
            with conn.cursor() as cur:
                cur.execute(pg_sql.SQL("TRUNCATE TABLE {} CASCADE;").format(pg_sql.Identifier(schema, table_name)))
                print(f'Truncated {table_name}')
            conn.commit()
        except Exception as e:
            print(e)

    def pg_load_table(conn, schema, file_path, table_name, header, delimiter, null_value):
        try:
            with conn.cursor() as cur, open(file_path, 'r') as f:
                cur.copy_expert(pg_sql.SQL(
                    'COPY {} FROM STDIN WITH ( \
                        FORMAT csv, \
                        HEADER {}, \
                        NULL {}, \
                        DELIMITER {});'
                    ).format(
                        pg_sql.Identifier(schema, table_name),
                        pg_sql.Literal(header),
                        pg_sql.Literal(null_value),
                        pg_sql.Literal(delimiter)
                    ), f)
                print(f'Copied data to {table_name}')
        except Exception as e:
            print(f'Error: {e}')

    conn = pg_connect(
        dbname=configs.POSTGRES_DB, 
        host=configs.POSTGRES_HOST, 
        port=configs.POSTGRES_PORT, 
        user=configs.POSTGRES_USER, 
        password=configs.POSTGRES_PASSWORD)

    for files in data_dict:
        file_path = files['file_path']
        table_name = files['table_name']
        header = files['header']
        delimiter = files['delimiter']
        null_value = files['null_value']

        pg_truncate_table(conn, schema, table_name)
        
        if os.path.isdir(file_path):
            for file in os.listdir(file_path):
                file_path_sub = os.path.join(file_path, file)
                pg_load_table(
                    conn,
                    schema,
                    file_path_sub, 
                    table_name, 
                    header, 
                    delimiter, 
                    null_value)
        else:
            pg_load_table(
                    conn,
                    schema,
                    file_path, 
                    table_name, 
                    header, 
                    delimiter, 
                    null_value)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()