import requests
import pytest


def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")


    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.exceptions.RequestException as e:
        pytest.fail(f"Request failed: {e}")

    
def test_real_postgres_connection(real_postgres_connection):

    connection = real_postgres_connection["connection"]
    cursor = real_postgres_connection["cursor"]
    try:
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Database query failed: {e}")
    