from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id = "cron_dag",
    start_date= datetime(year=2026, month=5, day=20),
    schedule=CronTriggerTimetable("0 16 * * MON-FRI",timezone="America/Halifax"),
    end_date= datetime(year=2026, month=5, day=31),
    is_paused_upon_creation=False,
    catchup=False
)
def cron_dag():
    
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


cron_dag()
