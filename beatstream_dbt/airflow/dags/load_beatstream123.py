from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'concurrency': 1,
    'max_active_runs' : 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test123',
    default_args=default_args,
    description='Orchestration of table loads and DBT run',
    schedule_interval='*/10 * * * *',
    catchup=False,
)

# Command to call main.py script for table loads
#load_tables_cmd = "python3 /home/praveen/beatstream/ETL/gc_load/main.py"
load_tables_cmd  = 'echo "Start executing airflow DAG ro laod Beastream data batch" >> ~/beatstream_20220511.log;python3 ~/beatstream/ETL/gc_load/main.py'

# Command to run DBT
#run_dbt_cmd = "dbt run --project-dir  /home/praveen/beatstream/ETL/beat_dbt --profiles-dir /home/praveen/beatstream/ETL/beat_dbt"
run_dbt_cmd = 'sleep 30;echo "Complete executing airflow DAG ro laod Beastream data batch" >> ~/beatstream_20220511.log'

# Task to call main.py script for table loads
load_tables_task = BashOperator(
    task_id='load_tables',
    bash_command=load_tables_cmd,
    dag=dag,
)

# Task to run DBT
run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command=run_dbt_cmd,
    dag=dag,
)

load_tables_task >> run_dbt_task

