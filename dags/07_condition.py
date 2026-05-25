from airflow.sdk import dag, task

@dag(
    dag_id = "branch_dag",
)
def branch_dag():
    
    @task.python
    def extract_task(**kwargs):
        print(f"Extracting data...")

        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data":[1,2,3],
                               "db_extracted_data":[4,5,6],
                               "s3_extracted_data":[7,8,9],
                               "weekend_flag": "false"}
        ti.xcom_push(key='return_value',value=extracted_data_dict)

    @task.python 
    def transform_api_task(**kwargs):
        print(f"Transforming data...")

        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['api_extracted_data']
        transformed_api_data = [i*10 for i in api_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

 
    @task.python 
    def transform_db_task(**kwargs):
        print(f"Transforming data...")

        ti = kwargs['ti']
        db_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['db_extracted_data']
        transformed_db_data = [i*100 for i in db_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)


    @task.python 
    def transform_s3_task(**kwargs):
        print(f"Transforming data...")

        ti = kwargs['ti']
        s3_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['s3_extracted_data']
        transformed_s3_data = [i*1000 for i in s3_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)

    # Creating a Decider Node
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task', key='return_value')['weekend_flag']
        if weekend_flag == "true":
            return 'no_load_task'    # Need to match task name !!
        else:
            return 'load_data'

    @task.bash
    def load_data(**kwargs):
        api_data = kwargs['ti'].xcom_pull(task_ids='transform_api_task')
        db_data = kwargs['ti'].xcom_pull(task_ids='transform_db_task')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transform_s3_task')
        return f"echo 'Loaded data: {api_data}, {db_data}, {s3_data}' "
    
    @task.bash
    def no_load_task():
        print("No loading on weekends.....")
        return "echo 'No Load Task Executed'"


    # Define task dependencies 
    extract = extract_task()
    transform_api = transform_api_task()
    transform_db = transform_db_task()
    transform_s3 = transform_s3_task()
    loaded_data = load_data()
    no_load = no_load_task()

    extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [loaded_data, no_load]
    

branch_dag()
