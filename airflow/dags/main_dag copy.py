import re
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator

import json
import requests
from utils.sqlhelpers import create_csv_table, db_columns, db_table_name, delete_xcom_data, db_init_columns
from utils.constants import database_url, data_path, email_filter

# Import this main_var from env
# Note: export AIRFLOW_VAR_MAIN_VAR='{"email":"user@gmail.com"}'
airflow_vars = Variable.get('main_var', deserialize_json=True)
internal_email = airflow_vars.get('email')
# uncomment to use this email
# internal_email = "user@gmail.com"

AIRFLOW_DAG_ID = 'my_etl_pipeline'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 7),
    'email': internal_email,
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True
}

# create dag
dag = DAG(
    dag_id=AIRFLOW_DAG_ID,
    default_args=default_args,
    description='',
    schedule_interval='00 1 * * *',
    max_active_runs=1,
    concurrency=20,
    catchup=False
)


def read_csv(file_path: str) -> pd.DataFrame:
    '''
    Read CSV data into dataframe

    Attrs:
        file_path::[str]
        Location of the CSV file

    Returns:
        pd.DataFrame
        Pandas Dataframe 
    '''
    data = pd.read_csv(file_path)
    return data


def ipInfo(addr: str) -> str:
    '''
    Function to get city name from IP address

    Attrs:
        addr::[str]
        Ip address

    Returns:
        str
        City information
    '''
    # The code has been commented, because the API is called
    # for each row of the table. Uncomment to execute this once.
    url = "https://geolocation-db.com/json/"+addr+"&position=true"
    # res = requests.get(url).json()
    # response from url(if res==None then check connection)
    # try:
    #     data = json.loads(res)
    #     return data.get("city")
    # except:
    #     return ""
    return "dummy"


def extract_website(email: str) -> str:
    '''
    Regex to extract domain from email address

    Attrs:
        email::[str]
        Email Address

    Returns:
        str
        Domain Name
    '''
    result = re.search(email_filter, email)
    if result:
        return result.group(1)
    return ""

# Transform Step
def transform_data(
    data: pd.DataFrame
    ) -> pd.DataFrame:
    '''
    Transform the data to remove null values,
    and apply column transformations.

    Attrs:
        data::[pd.DataFrame]
        Main data

    Returns:
        pd.DataFrame
        Transformed data
    '''
    # handle null values
    data = data.fillna({'first_name': '',
                        'last_name': '',
                        'email': '',
                        'gender': '',
                        'ip_address': ''})

    # extract domains
    data["website"] = data["email"].apply(extract_website)
    # extract cities
    data["city"] = data["ip_address"].apply(ipInfo)
    
    return data

def validate_columns(**kwargs):
    '''
    Validation of columns expected 
    in the csv vs columns in the SQL table
    '''
    data = read_csv(kwargs["filepath"])
    if sorted(kwargs["column_list"]) != sorted(data.columns):
        return "schema_error"
    return "create_table_taskid"

# Load step
def load_data_pipeline(**kwargs):
    '''
    Load the transformed data into
    postgres database
    '''
    data = read_csv(kwargs["filepath"])
    data = transform_data(data)
    engine = create_engine(kwargs["database_url"])
    data.to_sql(kwargs["tablename"], engine, if_exists="append", index=False)
    return "clear_xcom_taskid"


load_data = BranchPythonOperator(
    task_id="load_data_taskid",
    python_callable=load_data_pipeline,
    op_kwargs={
        "filepath": data_path,
        "tablename": db_table_name,
        "column_list": db_columns,
        "database_url": database_url
    },
    retries=1,
    retry_delay=5,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag
)


validate_columns = BranchPythonOperator(
    task_id="validate_data_taskid",
    python_callable=validate_columns,
    op_kwargs={
        "filepath": data_path,
        "column_list": db_init_columns
    },
    retries=1,
    retry_delay=5,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_table_taskid",
    sql=create_csv_table.format(db_table_name),
    trigger_rule="one_success",
    postgres_conn_id="airflow_db",
    dag=dag
)

schema_error = DummyOperator(
    task_id='schema_error',
    dag=dag
)

clear_xcom = PostgresOperator(
    task_id='clear_xcom_taskid',
    postgres_conn_id='airflow_db',
    trigger_rule='all_done',
    sql=delete_xcom_data.format(AIRFLOW_DAG_ID),
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start >> validate_columns >> create_table  >> load_data >> clear_xcom >>  end
validate_columns >> schema_error >> clear_xcom >> end