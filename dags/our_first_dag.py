from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'VMD',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)

}

with DAG(
    dag_id = 'our_first_dag_v4',
    default_args = default_args,
    description = 'this is our first dag that we write',
    start_date = datetime(2022,11,19,2),
    schedule_interval = '@daily'

) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo i am task2, i run after task1"
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo i am task3, i run after task1 and same time with task2"
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)