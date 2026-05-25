from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id = "sch_dag",
    start_date=datetime(2025, 5, 20),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=False
)
def sch_dag():
    
    @task.python
    def first_task():
        print(f"This is the first task")

    @task.python 
    def second_task():
        print(f"This is the second task")

    @task.python 
    def third_task():
        print(f"This is the third task. DAG is complete")

    # Define task dependencies 
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


sch_dag()
