from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pendulum import duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id = "delta_dag",
    start_date = datetime(year=2026, month=5, day=20),
    schedule=DeltaTriggerTimetable(duration(days=1)),
    end_date = datetime(year=2026, month=5, day=31),
    is_paused_upon_creation=False,
    catchup=False
)
def delta_dag():
    
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


delta_dag()
