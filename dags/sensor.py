from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor

from operators.my_operators import StartOperator, DirectorySensor, ScriptParser

dag = DAG('my_operator_tester_dag',
          description="'Testing custom operators",
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

starter = StartOperator(task_id="start_operator", dag=dag)
sensor = DirectorySensor(directory="/usr/local/airflow/test", task_id="directory_sensor", dag=dag)
parser = ScriptParser(directory="/usr/local/airflow/test", task_id="script_parser", dag=dag)

starter >> sensor >> parser
