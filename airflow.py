from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from sqlalchemy import create_engine
import pandas as pd

# This code consist in for steps for processing ING_MultiDays file

def create_dataframe():
    # Read the CSV file
    df = pd.read_csv('/path/to/BING_MultiDays.csv')

    # Return the DataFrame
    return df

def remove_rows_before_header(df):
    # Find the index of the row that contains the header
    header_index = df[df['header_column_name'] == 'header_value'].index[0]

    # Remove the rows that are before the header
    df = df[header_index:]

    # Return the modified DataFrame
    return df

def insert_dataframe_into_mysql(df):
    # Connect to the MySQL database
    engine = create_engine('mysql://username:password@host:port/database_name')

    # Write the modified DataFrame to the database
    df.to_sql('bing_ads_model', con=engine, if_exists='append', index=False)

def insert_dimensions_into_mysql(df):
    # Connect to the MySQL database
    engine = create_engine('mysql://username:password@host:port/database_name')

    # Extract unique customers and account numbers
    customers = df[['Customers', 'Account_number']].drop_duplicates()

    # Write the customers DataFrame to the database
    customers.to_sql('customers', con=engine, if_exists='replace', index=False)

    # Extract unique ad group IDs and ad groups
    ad_groups = df[['Ad_groupid', 'Ad_group']].drop_duplicates()

    # Write the ad groups DataFrame to the database
    ad_groups.to_sql('ad_group', con=engine, if_exists='replace', index=False)

# Define the DAG
dag = DAG(
    'process_csv_and_insert_into_mysql',
    description='Read a CSV file, remove rows before the header, and insert the modified DataFrame into a MySQL database',
    schedule_interval=None,
    start_date=datetime(2023, 4, 13),
    catchup=False
)

# Define the task that creates the DataFrame
create_dataframe_task = PythonOperator(
    task_id='create_dataframe',
    python_callable=create_dataframe,
    dag=dag
)

# Define the task that removes rows before the header
remove_rows_task = PythonOperator(
    task_id='remove_rows',
    python_callable=remove_rows_before_header,
    op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="create_dataframe") }}'},
    dag=dag
)

# Define the task that inserts the modified DataFrame into MySQL
insert_into_mysql_task = PythonOperator(
    task_id='insert_into_mysql',
    python_callable=insert_dataframe_into_mysql,
    op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="remove_rows") }}'},
    dag=dag
)

# Define the task that inserts dimensions into MySQL
insert_dimensions_task = PythonOperator(
    task_id='insert_dimensions',
    python_callable=insert_dimensions_into_mysql,
    op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="remove_rows") }}'},
    dag=dag
)

# Set the dependencies between the tasks
create_dataframe_task >> remove_rows_task >> [insert_into_mysql_task, insert_dimensions_task]
