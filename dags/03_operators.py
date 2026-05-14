from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag(
    dag_id = "bash_dag",
)
def bash_dag():
    
    @task.python
    def first_task():
        print(f"This is the first task")

    @task.python 
    def second_task():
        print(f"This is the second task")
    
    @task.bash
    def bash_task() -> str:
        return "echo https://airflow.apache.org/"
    
    run_this = BashOperator(
    task_id="bash_task_old",
    bash_command="echo https://airflow.apache.org/",
    )

    # Define task dependencies 
    first = first_task()
    second = second_task()
    bash_task = bash_task()

    first >> second >> bash_task >> run_this


bash_dag()
