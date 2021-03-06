"""
This file is written originally for testing the csv files directly in the test/ directory. Now as the DAG is
going to call the parser and will create tables in the tables in the database, this file will be only for a
reference on how to use Airflow DAGs and self-defined operators
"""

from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.my_operators import StartOperator, DirectorySensor, ScriptParser

__author__ = "Oscar Zhang"
__email__ = "tzhang383@wisc.edu"
__version__ = '0.1'
__status__ = 'Development'

dag = DAG('my_operator_tester_dag',
          description="'Testing custom operators",
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

starter = StartOperator(task_id="start_operator", dag=dag)
sensor = DirectorySensor(directory="/usr/local/airflow/test", task_id="directory_sensor", dag=dag)
parser = ScriptParser(directory="/usr/local/airflow/test", task_id="script_parser", dag=dag)
failed = DummyOperator(task_id="failing_state", trigger_rule=TriggerRule.ONE_FAILED, dag=dag)
success = DummyOperator(task_id="success_state", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
done = DummyOperator(task_id="finish_state", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

starter >> sensor >> parser >> (failed, success) >> done
