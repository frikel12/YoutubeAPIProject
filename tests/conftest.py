import os
import pytest 
from unittest import mock
from airflow.models import Variable, DagBag
import datawarehouse.data_utils as du


@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")


@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")


@pytest.fixture
def postgres_conn():

    mock_conn = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch.object(du.psycopg2, "connect", return_value=mock_conn) as mock_connect:
        yield {"mock_connect": mock_connect, "connection": mock_conn, "cursor": mock_cursor}


@pytest.fixture
def dagbag():
    yield DagBag()


@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    
    return get_airflow_variable


@pytest.fixture
def real_postgres_connection():
    host = os.getenv("POSTGRES_CONN_HOST")
    db_name = os.getenv("ELT_DATABASE_NAME")
    db_user = os.getenv("ELT_DATABASE_USERNAME")
    db_password = os.getenv("ELT_DATABASE_PASSWORD")

    connection, cursor = du.get_conn_cursor(db_name, db_user, db_password, host, port=5432)

    yield {"connection": connection, "cursor": cursor}

    du.close_conn_cursor(connection, cursor)
    
    