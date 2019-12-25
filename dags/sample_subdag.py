#  dags/subdag.py
#  main_dag.py
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='branch_without_trigger',
    schedule_interval='@once',
    start_date=datetime(2019, 2, 28)
)

with dag:
    kick_off_dag = DummyOperator(task_id='run_this_first')

    branching = DummyOperator(task_id="branching_start")

    kick_off_dag >> branching

    for i in range(0, 5):
        d = DummyOperator(task_id='branch_{0}'.format(i))
        for j in range(0, 3):
            m = DummyOperator(task_id='branch_{0}_{1}'.format(i, j))
            d >> m
        branching >> d