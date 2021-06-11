from airflow import DAG
from airflow.utils.dates import parse_execution_date

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def pas_grand_chose():
    print("pas grand chose")


with DAG(
    'chat-saver',
    default_args=default_args,
    description='A simple DAG to get chat from Twitch streams and save it to BigQuery.',
    schedule_interval="0 2 * * *",
    catchup=True,
    start_date=parse_execution_date("2021-04-01"),
) as dag:
    t1 = PythonOperator(
        task_id="python_script",
        python_callable=pas_grand_chose,
    )

