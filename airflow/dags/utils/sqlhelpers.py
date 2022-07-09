'''
This file contains variables related to the database tables 
and ddl commands to manage the database.
'''

create_csv_table = """
CREATE TABLE IF NOT EXISTS {} (
    id INTEGER NOT NULL,
    first_name VARCHAR (50) NOT NULL,
    last_name VARCHAR (50) NOT NULL,
    email VARCHAR (100) NOT NULL,
    website VARCHAR (100) NOT NULL,
    gender VARCHAR (50) NOT NULL,
    ip_address VARCHAR (50) NOT NULL,
    city VARCHAR (50) NOT NULL
);"""

delete_xcom_data = """
DELETE FROM public.xcom
WHERE
    dag_id = '{}'
;"""

db_table_name = "analysis_data"

# columns after transforms
db_columns = ["id", "first_name", "last_name", "email", "gender", "ip_address", "website","city"]

# columns expected to be in the csv
db_init_columns = ["id", "first_name", "last_name", "email", "gender", "ip_address"]

