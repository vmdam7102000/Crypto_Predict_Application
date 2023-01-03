from airflow.decorators import dag,task
from datetime import datetime,timedelta


default_args = {
    'owner': 'VMD',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id = 'dag_with_taskflow_api_v3',
     default_args = default_args,
     start_date=datetime(2022,11,20,2),
     schedule_interval = '@daily')
def hello_world_etl():
    

    @task(multiple_outputs = True)
    def get_name():
        return {
            'first_name': 'Dam',
            'last_name': 'Vu'
        }

    @task()
    def get_age():
        return 22

    @task()
    def greet(fisrt_name, last_name,age):
        print(f"hello world! my name is {fisrt_name} {last_name} and {age} year old")

    name_dict = get_name()
    age = get_age()

    greet(fisrt_name=name_dict['first_name'], last_name=name_dict['last_name'],age=age)

greet_dag = hello_world_etl()