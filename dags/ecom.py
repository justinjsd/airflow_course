from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

# Best way to use Dag - using Dag Decorators
@dag(
    start_date = datetime(2025, 1, 1),
    schedule = '@daily',
    catchup = False, #Airflow Catchup is by default True
    # airflow dags backfill ecom -s <2025-01-01> -e <2025-01-02> dag_id
    description = "This DAG processes Ecommerce Data",
    tags = ["team_1", "ecom", "pii"],
    default_args = {
        "retries": 1
    },
    dagrun_timeout = timedelta(minutes=20),
    max_consecutive_failed_dag_runs = 2
    # max_active_runs = 1 -> will always wait for the dag to complete befer starting a new one
    )

def ecom(): # Dag ID -> Ecom

    ta = PythonOperator(
        task_id="ta"
    )

    tb = PythonOperator(
        task_id="tb"
    )

    tc = PythonOperator(
        task_id="tc"
    )

ecom()