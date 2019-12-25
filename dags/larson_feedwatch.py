from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.my_operators import StartOperator, DirectorySensor, ScriptParser

dag = DAG(
    dag_id="larson_feedwatch",
    description="larson feedwatch data",
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20),
    catchup=False
)

starter_task = StartOperator(
    task_id="start_operator",
    dag=dag
)

sensor_task = DirectorySensor(
    directory="/usr/local/airflow/test/larson/feedwatch",
    task_id="directory_sensor",
    dag=dag
)

parser_task = ScriptParser(
    directory="/usr/local/airflow/test/larson/feedwatch",
    task_id="script_parser",
    dag=dag
)

finish_task = DummyOperator(
    task_id="finish_task",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

starter_task >> sensor_task >> parser_task >> finish_task

