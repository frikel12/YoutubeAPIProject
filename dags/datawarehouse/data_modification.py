# Deprecated: Renamed to `data_modification.py` (use `datawarehouse.data_modification`)
# This file is kept for historical reasons but should not be imported.
# If you want to remove it, delete this file after verifying no references remain.

import json

table = "youtube_video_stats"


def insert_data(schema, connection, cursor, row):

    try:
        if schema == "staging":
            cursor.execute(
                f"""
                INSERT INTO {schema}.{table} (videoId, title, publishedAt, duration, viewCount, likeCount, commentCount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("videoId"),
                    row.get("title"),
                    row.get("publishedAt"),
                    row.get("duration"),
                    int(row["viewCount"]) if row.get("viewCount") is not None else None,
                    int(row["likeCount"]) if row.get("likeCount") is not None else None,
                    int(row["commentCount"]) if row.get("commentCount") is not None else None
                )
            )
        else:
            cursor.execute(
                f"""
                INSERT INTO {schema}.{table} (videoId, title, publishedAt, duration, viewCount, likeCount, commentCount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("videoId"),
                    row.get("title"),
                    row.get("publishedAt"),
                    row.get("duration"),
                    int(row["viewCount"]) if row.get("viewCount") is not None else None,
                    int(row["likeCount"]) if row.get("likeCount") is not None else None,
                    int(row["commentCount"]) if row.get("commentCount") is not None else None
                )
            )

        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e
    
def update_rows(schema, connection, cursor, row):
    try:
        if schema != "staging":
            cursor.execute(
                f"""
                UPDATE {schema}.{table}
                SET title = %s,
                    publishedAt = %s,
                    duration = %s,
                    viewCount = %s,
                    likeCount = %s,
                    commentCount = %s
                WHERE videoId = %s
                """,
                (
                    row.get("title"),
                    row.get("publishedAt"),
                    row.get("duration"),
                    int(row["viewCount"]) if row.get("viewCount") is not None else None,
                    int(row["likeCount"]) if row.get("likeCount") is not None else None,
                    int(row["commentCount"]) if row.get("commentCount") is not None else None,
                    row.get("videoId")
                )
            )

        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e
    
def delete_rows(schema, connection, cursor, video_ids):
    try:
        if schema != "staging":
            format_strings = ','.join(['%s'] * len(video_ids))
            cursor.execute(
                f"""
                DELETE FROM {schema}.{table}
                WHERE videoId IN ({format_strings})
                """,
                tuple(video_ids)
            )

        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e