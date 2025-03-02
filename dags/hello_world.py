from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    "owner": "causality",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "hello_world",
    default_args=default_args,
    description="A simple hello world DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print hello world with bash
    t1 = BashOperator(
        task_id="print_hello",
        bash_command='echo "Hello World from Bash!"',
    )

    # Task 2: Print hello world with Python
    def print_hello():
        print("Hello World from Python!")
        return "Hello World returned!"

    t2 = PythonOperator(
        task_id="print_hello_python",
        python_callable=print_hello,
    )

    # Task 3: Print date
    t3 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    # Set task dependencies
    t1 >> t2 >> t3
