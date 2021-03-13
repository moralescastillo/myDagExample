from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

seven_days_ago = (datetime.combine(datetime.today() - timedelta(7), 
					datetime.min.time()))

def print_current_timestamp():
    
    return print(datetime.today())

def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
  }

dag = DAG('send_email_morning', 
          default_args=default_args,
          schedule_interval= '0 0 * * *')

t1 = BashOperator(
    task_id='hello_world_print',
    bash_command='printf "hello world"',
    dag=dag)

t2 = PythonOperator(
    task_id = 'current_timestamp_print',
    python_callable = print_context,
    dag=dag)

## dependencies

t1 >> t2


print_current_timestamp()
