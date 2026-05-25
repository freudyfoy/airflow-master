"""
XCOMS : Mechanism that let Tasks talk to each other; sharing data with the storage called "XCOMS"; Every tasks can approach.
        Make local variable to global variable.

"""

from airflow.sdk import dag, task

@dag(
    dag_id = "xcoms_dag",
)
def xcoms_dag():
    
    @task.python
    def first_task():
        print(f"Extracting data...")
        fetch_data = {"data": [1,2,3,4,5]}
        return fetch_data

    @task.python 
    def second_task(data: dict):
        print(f"Transforming data...")
        fetched_data = data['data']
        transf_data = fetched_data*2
        transf_data_dict = {"transf_data": transf_data}
        return transf_data_dict
    
    @task.python
    def third_task(data: dict):
        loaded_data = data
        return loaded_data

    # Define task dependencies 
    first = first_task()
    second = second_task(first)
    third = third_task(second)


xcoms_dag()
