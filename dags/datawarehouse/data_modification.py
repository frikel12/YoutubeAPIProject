import json

table = "youtube_video_stats"


def insert_data(schema, connection, cursor, row):

    try:
        if schema == "staging":
            cursor.execute(
                f"""
                INSERT INTO {schema}.{table} (\"videoId\", \"title\", \"publishedAt\", \"duration\", \"viewCount\", \"likeCount\", \"commentCount\")
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
            # Use unquoted identifiers for production/core tables (they are created with unquoted column names)
            cursor.execute(
                f"""
                INSERT INTO {schema}.{table} (videoId, title, publishedAt, duration, viewCount, likeCount, commentCount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("videoId") or row.get("videoid"),
                    row.get("title"),
                    row.get("publishedAt"),
                    row.get("duration"),
                    int(row["viewCount"]) if row.get("viewCount") is not None else (int(row.get("viewcount")) if row.get("viewcount") is not None else None),
                    int(row["likeCount"]) if row.get("likeCount") is not None else (int(row.get("likecount")) if row.get("likecount") is not None else None),
                    int(row["commentCount"]) if row.get("commentCount") is not None else (int(row.get("commentcount")) if row.get("commentcount") is not None else None)
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