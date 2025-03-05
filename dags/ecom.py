from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
from airflow import dataset

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,  # Airflow Catchup is True by default
    description="This DAG processes Ecommerce Data",
    tags=["team_1", "ecom", "pii"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20),
    max_active_runs=1,  # Ensures only one active DAG run at a time
    max_consecutive_failed_dag_runs = 2
)
def ecom():
    ta = EmptyOperator(task_id="task_a")


# Using datasets as triggers for a dag {
file = Dataset("file.txt")

@dag(...)
def my_dag():

    def task_a(outlets = [file]):
        with open('file.txt', 'r') as f:
            f.write('Hello World')

@dag(schedule=[file])
def my_dag():
    ...

# Using datasets as triggers for a dag }

ecom()  # Assign DAG instance