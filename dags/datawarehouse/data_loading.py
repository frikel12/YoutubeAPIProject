from datetime import date
from .data_utils import get_conn_cursor, close_conn_cursor
import json
    

def load_data():
    filename = f"data/youtube_video_stats_{date.today()}.json"

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data
    
