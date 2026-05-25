"""
XCOMS : Mechanism that let Tasks talk to each other; sharing data with the storage called "XCOMS"; Every tasks can approach.
        Make local variable to global variable.

"""

from airflow.sdk import dag, task

@dag(
    dag_id = "xcoms_dag_manual",
)
def xcoms_dag_manual():
    
    @task.python
    def first_task(**kwargs):
        # Extracting 'ti' (task instance) from kwargs to push XCOMS manually
        print(f"Extracting data...")
        
        ti = kwargs['ti']
        fetched_data = {"data": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value=fetched_data)


    @task.python 
    def second_task(**kwargs):
        print(f"Transforming data...")
        
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids='first_task', key='return_result')['data']
        
        transf_data = fetched_data*2
        transf_data_dict = {"transf_data": transf_data}
        
        ti.xcom_push(key='return_result', value=transf_data_dict)
    
    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids='second_task', key='return_result')
        return load_data

    # Define task dependencies 
    first = first_task()
    second = second_task()
    third = third_task()


xcoms_dag_manual()
