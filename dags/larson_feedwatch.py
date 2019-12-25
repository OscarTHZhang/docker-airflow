import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.my_operators import StartOperator, DirectorySensor, ScriptParser


def walking(directory):
    """
    Walking through the directory and retrieve sub-directory with dates
    :param directory: some directory with /test/larson/feedwatch
    :return: a list of absolute directories with dates
    """
    res = []
    for dirs in os.listdir(directory):
        res.append(os.path.join(directory, dirs))
    return res


dag = DAG(
    dag_id="larson_feedwatch",
    description="larson dairycomp data",
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20),
    catchup=False
)

starter_task = StartOperator(
    task_id="start_operator",
    dag=dag
)

# sensor_task = DirectorySensor(
#     directory="/usr/local/airflow/test/larson/feedwatch",
#     task_id="directory_sensor",
#     dag=dag
# )

separator_task = DummyOperator(task_id='separate_date_files', dag=dag)

# parser_task = ScriptParser(
#     directory="/usr/local/airflow/test/larson/feedwatch",
#     task_id="script_parser",
#     dag=dag
# )

finish_task = DummyOperator(
    task_id="finish_task",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

starter_task >> separator_task

task_id = 0
for date_dir in walking('/usr/local/airflow/test/larson/feedwatch'):
    task_id += 1
    sub_sensing_task = DirectorySensor(
        directory=date_dir,
        task_id="directory_sensor_{}".format(task_id),
        dag=dag
    )
    sub_parsing_task = ScriptParser(
        directory=date_dir,
        task_id="script_parser_{}".format(task_id),
        dag=dag
    )
    separator_task >> sub_sensing_task >> sub_parsing_task >> finish_task


