from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime, timedelta

@dag(
    dag_id = "increment_dag",
    schedule=CronDataIntervalTimetable("@daily",timezone="America/Halifax"),
    start_date=datetime(year=2026, month=5, day=20),
    end_date=datetime(year=2026, month=5, day=31),
    catchup=True
)
def incremental_load_dag():

    @task.python
    def incremental_load_fetch_task(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")
    
    @task.bash
    def incremental_data_process_task(**kwargs):
        return "echo 'Processing data from {{ data_interval_start }} to {{ data_interval_end }}'"   # Jinja template
    

    fetch_task = incremental_load_fetch_task()
    process_task = incremental_data_process_task()

    fetch_task >> process_task

incremental_load_dag()
