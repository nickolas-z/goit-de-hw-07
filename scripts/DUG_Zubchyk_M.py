from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
import random
import time

def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 31, 0, 0),
}

# MySQL database connection name
CONNECTION = "goit_mysql_db_zmm"
SCHEMA = "olympic_db_zmm"
TABLE = "medals"
# Execution delay for sensor testing (failed with value 35 seconds)
DELAY_S = 5

FRESH_LIMIT = 30
TIMEOUT = 60

def choose_medal_func():
    """Randomly selects one of three medal types"""
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Chosen medal: {medal}")
    return {"Bronze": "bronze_task", "Silver": "silver_task"}.get(medal, "gold_task")

def delay_func():
    """Execution delay for sensor testing (35 seconds > 30 seconds limit)"""
    print(f"Chosen delay: {DELAY_S} seconds...")
    time.sleep(DELAY_S)

def generate_insert_sql(medal: str) -> str:
    """Generates SQL query to count and insert medal quantities"""
    return f"""
    INSERT INTO {SCHEMA}.{TABLE} (medal_type, count, created_at)
    SELECT '{medal}', COUNT(*), NOW()
    FROM olympic_dataset.athlete_event_results
    WHERE medal = '{medal}';
    """

with DAG(
        'DUG_Zubchyk_M',
        default_args=default_args,
    schedule=None,
        catchup=False,
        tags=["Zubchyk"]
) as dag:
    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id=CONNECTION,
        sql=f"CREATE DATABASE IF NOT EXISTS {SCHEMA};",
    )

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(16) NOT NULL,
            count INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    )

    choose_medal = BranchPythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal_func,
    )

    bronze_task = MySqlOperator(
        task_id="bronze_task",
        mysql_conn_id=CONNECTION,
        sql=generate_insert_sql("Bronze"),
    )
    silver_task = MySqlOperator(
        task_id="silver_task",
        mysql_conn_id=CONNECTION,
        sql=generate_insert_sql("Silver"),
    )
    gold_task = MySqlOperator(
        task_id="gold_task",
        mysql_conn_id=CONNECTION,
        sql=generate_insert_sql("Gold"),
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_func,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id=CONNECTION,
        sql=f"""
        SELECT
          CASE
            WHEN TIMESTAMPDIFF(
                   SECOND, COALESCE(MAX(created_at), '1970-01-01'), NOW()
                 ) <= {FRESH_LIMIT}
            THEN 1 ELSE 0 END AS is_recent
        FROM {SCHEMA}.{TABLE};
        """,
        mode="reschedule",
        poke_interval=5,
        timeout=TIMEOUT,
    )

    create_schema >> create_table >> choose_medal >> [bronze_task, silver_task, gold_task]
    [bronze_task, silver_task, gold_task] >> delay_task >> check_recent_record