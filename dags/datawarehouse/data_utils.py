import psycopg2
from psycopg2 import sql
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
    try:
        cursor.execute(create_schema_query)
        connection.commit()
    except psycopg2.errors.UniqueViolation:
        # Concurrent create may cause a duplicate key error; rollback and continue
        try:
            connection.rollback()
        except Exception:
            pass


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
    # Detect the actual column name for the video id (handles camelCase or snake_case)
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND lower(column_name) = lower(%s)
        LIMIT 1
        """,
        (schema, table, 'videoId')
    )
    row = cur.fetchone()

    if not row:
        # Fallback: look for any column that contains 'videoid' (case-insensitive)
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND lower(column_name) LIKE %s
            LIMIT 1
            """,
            (schema, table, '%videoid%')
        )
        row = cur.fetchone()

    if not row:
        # Table or column does not exist yet or has unexpected schema
        return []

    col_name = row[0]

    # Use psycopg2.sql to safely compose identifiers with proper quoting
    query = sql.SQL("SELECT {col} FROM {schema}.{tbl}").format(
        col=sql.Identifier(col_name),
        schema=sql.Identifier(schema),
        tbl=sql.Identifier(table)
    )

    cur.execute(query)
    rows = cur.fetchall()
    return [r[0] for r in rows]
    
