import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def extract_data(ti):
    
    url = "http://catfact.ninja/fact"
    res = requests.get(url)

    # Storing the API result in XCom
    ti.xcom_push(key='api_result', value=json.loads(res.text)["fact"])

def process_cat(ti):
    # getting the cat
    cat_fact = ti.xcom_pull(key="api_result", task_ids="get_a_cat_fact")
    
    #processing 
    cat_fact = cat_fact + ": Adding bonus process text!!!!"
    
    # Storing the API result in XCom
    print(cat_fact)

with DAG (
    dag_id = "my_second_dag",
    start_date = datetime(2023, 6, 10),
    schedule = "0 9 * * *",
    catchup = False 

) as dag:
    
    get_cat_data = PythonOperator(
        task_id = "get_a_cat_fact", 
        python_callable = extract_data,
    ),

    process_task = PythonOperator(
        task_id='process_cat',
        python_callable=process_cat,
    )

    get_cat_data >> process_task
 