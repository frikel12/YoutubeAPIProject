from datawarehouse.data_modification import insert_data, update_rows, delete_rows
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_transformation import transform_data

from airflow.decorators import task
import os


table = 'youtube_video_stats'
# Youtube Database credentials
yt_db_host = os.getenv("POSTGRES_CONN_HOST")
yt_db_name = os.getenv("ELT_DATABASE_NAME")
yt_db_user = os.getenv("ELT_DATABASE_USERNAME")
yt_db_password = os.getenv("ELT_DATABASE_PASSWORD")


@task
def staging_table():
    schema = "staging"
    connection, cursor = None, None

    try:
        connection, cursor = get_conn_cursor(yt_db_name, yt_db_user, yt_db_password, yt_db_host, port=5432)
        data = load_data()

        create_schema(schema, connection, cursor)
        create_table(schema, connection, cursor)

        table_ids = get_video_ids(cursor, schema)
        
        for row in data:
            if len(table_ids) == 0:
                insert_data(schema, connection, cursor, row)
            
            elif row.get("videoId") in table_ids:
                update_rows(schema, connection, cursor, row)
            else:
                insert_data(schema, connection, cursor, row)
        
        ids_json = {row["videoId"] for row in data}
        ids_to_delete = set(table_ids) - ids_json

        if ids_to_delete:
            delete_rows(schema, connection, cursor, ids_to_delete)

    except Exception as e:
        raise e
    finally:
        close_conn_cursor(connection, cursor)


@task
def core_table():
    schema = 'core'

    connection, cursor = None, None

    try:
        connection, cursor = get_conn_cursor(yt_db_name, yt_db_user, yt_db_password, yt_db_host, port=5432)

        create_schema(schema, connection, cursor)
        create_table(schema, connection, cursor)

        table_ids = get_video_ids(cursor, schema)

        current_video_ids = set()

        # Fetch all rows from staging and map to dictionaries so transform_data can work
        cursor.execute(f"SELECT * FROM staging.{table}")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        for row in rows:
            row_dict = dict(zip(col_names, row))

            # Find the key used for video id (handles different column name cases)
            id_key = None
            for k in row_dict.keys():
                if k.lower() == 'videoid' or 'videoid' in k.lower():
                    id_key = k
                    break

            if not id_key:
                # Cannot find id column; skip this row
                continue

            current_video_ids.add(row_dict[id_key])

            # Transform row (expects dict)
            transform_row = transform_data(row_dict)

            if len(table_ids) == 0:
                insert_data(schema, connection, cursor, transform_row)
            else:
                if row_dict[id_key] in table_ids:
                    update_rows(schema, connection, cursor, transform_row)
                else:
                    insert_data(schema, connection, cursor, transform_row)

        ids_to_delete = set(table_ids) - current_video_ids
        if ids_to_delete:
            delete_rows(schema, connection, cursor, ids_to_delete)

    except Exception as e:
        raise e
    finally:
        close_conn_cursor(connection, cursor)


