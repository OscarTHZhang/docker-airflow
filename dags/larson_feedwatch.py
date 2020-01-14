"""
This is the dag file for larson/feedwatch data. Trigger 'larson_feedwatch' DAG in the Airflow web server will
start the data pipeline for larson/feedwatch
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.my_operators import StartOperator, DirectorySensor, ScriptParser

__author__ = 'Oscar Zhang'
__email__ = 'tzhang383@wisc.edu'
__version__ = '0.1'
__status__ = 'Development'

# This is the path that defines the place of data source. In the development phase, it is inside
# this folder path in the docker container
DATA_SOURCE_DIRECTORY = '/usr/local/airflow/test/larson/feedwatch'
# COULD CHANGE THE PATH FOR FURTHER DEVELOPMENT OR REAL-ENVIRONMENT TESTING


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

separator_task = DummyOperator(task_id='separate_date_files', dag=dag)

finish_task = DummyOperator(
    task_id="finish_task",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

starter_task >> separator_task

task_id = 0
for date_dir in walking(DATA_SOURCE_DIRECTORY):
    task_id += 1
    label = os.path.basename(date_dir)
    sub_sensing_task = DirectorySensor(
        directory=date_dir,
        task_id="directory_sensor_{}_{}".format(task_id, label),
        dag=dag
    )
    sub_parsing_task = ScriptParser(
        directory=date_dir,
        task_id="script_parser_{}_{}".format(task_id, label),
        dag=dag
    )
    separator_task >> sub_sensing_task >> sub_parsing_task >> finish_task


