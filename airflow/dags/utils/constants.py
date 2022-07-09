import re

database_url = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
data_path = "/opt/airflow/dags/data/DATA.csv"
email_filter = re.compile(r"\w+@([\w\.]+)")
