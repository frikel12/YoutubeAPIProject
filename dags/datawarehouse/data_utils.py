import psycopg2
import os

table = "youtube_video_stats"


def get_conn_cursor(database, user, password, host, port=5432):
    try:

        connection = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        raise e


def close_conn_cursor(connection, cursor):
    try:
        cursor.close()
    except Exception:
        pass
    try:
        connection.close()
    except Exception:
        pass

def create_schema(schema, connection, cursor):
    # Create schema if it does not exist
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    cursor.execute(create_schema_query)

    connection.commit()


def create_table(schema, connection, cursor):
    # Create table if it does not exist
    if schema=="staging":
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "videoId" VARCHAR(11) PRIMARY KEY NOT NULL,
            "title" TEXT NOT NULL,
            "publishedAt" TIMESTAMP NOT NULL,
            "duration" VARCHAR(20) NOT NULL,
            "viewCount" INT,
            "likeCount" INT,
            "commentCount" INT
        )
        """
    else:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            videoId TEXT PRIMARY KEY,
            title TEXT,
            publishedAt TIMESTAMP,
            duration TEXT,
            viewCount BIGINT,
            likeCount BIGINT,
            commentCount BIGINT
        )
        """
    cursor.execute(create_table_query)

    connection.commit()


def get_video_ids(cur, schema):
    cur.execute(f"SELECT videoId FROM {schema}.{table}")
    rows = cur.fetchall()
    return [row[0] for row in rows]
    
