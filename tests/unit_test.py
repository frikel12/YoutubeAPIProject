import datawarehouse.data_utils as du

def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


def test_postgres_connection(postgres_conn):

    mc = postgres_conn["mock_connect"]
    conn = postgres_conn["connection"]
    cur = postgres_conn["cursor"]

    connection, cursor = du.get_conn_cursor("test_db", "user", "pass", "host", port=5432)

    mc.assert_called_once_with(database="test_db", user="user", password="pass", host="host", port=5432)
    assert connection is conn
    assert cursor is cur

def test_dags_integrity(dagbag):
    # 1.
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=============================")
    print(dagbag.import_errors)
    
    # 2.
    dag_ids = dagbag.dag_ids
    expected_dags = ["youtube_video_stats_etl" ,"youtube_video_stats_dwh", "data_quality"]
    print("=============================")
    print(dag_ids)
    for dag_id in expected_dags:
        assert dag_id in dag_ids

    # 3.
    assert dagbag.size() == 3
    print("=============================")
    print(dagbag.size())

    # 4.

    expected_task_counts = {
        "youtube_video_stats_etl": 5,
        "youtube_video_stats_dwh": 3,
        "data_quality": 2
    } 
    print("=============================")
    for dag_id, dag in dagbag.dags.items():
        expected_counts = expected_task_counts[dag_id]
        actual_counts = len(dag.tasks)
        assert actual_counts == expected_counts, f"DAG '{dag_id}' has {actual_counts} tasks; expected {expected_counts}."
        print(f"DAG '{dag_id}' has correct number of tasks: {actual_counts}")