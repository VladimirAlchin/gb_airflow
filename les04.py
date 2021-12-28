import json
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from sqlalchemy import create_engine


SPARK_APP_PATH = Variable.get('spark_app_path')  # Путь до spark файлов
POSTGRES_JAR = Variable.get('postgres_jar') # Путь до зависимостей для postgres
POSTGRES_URL = Variable.get('postgres_url')
POSTGRES_USER, POSTGRES_PASSWORD= Variable.get('postgres_user'),  Variable.get('postgres_pass_secret')


@task()
def start_dag():
    print('Here we start!')
    return 'start'

@task()
def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    df.to_sql('titanic', engine, index=False, if_exists='replace', schema='datasets' )
    return 'datasets.titanic'

start_task = \
    BashOperator(task_id='first_task',
                 bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"'
                )
spark_task_ags = {
    "conn_id": "spark_master_conn",
    'conf': {"spark.jars": POSTGRES_JAR,
             "spark.driver.extraClassPath": POSTGRES_JAR}

}
spark_submit_job = \
    SparkSubmitOperator(application=f"{SPARK_APP_PATH}/spark_titanic_etl.py",
                        task_id="spark_etl_job",
                        name="spark_etl",
                        driver_class_path=POSTGRES_JAR,
                        application_args=[
                            "--postgres_url", POSTGRES_URL,
                            "--postgres_dbtable", "datasets.titanic",
                            "--postgres_user", POSTGRES_USER,
                            "--postgres_password", POSTGRES_PASSWORD
                        ],
                        **spark_task_ags
                        )
send_message_telegram_task = \
    TelegramOperator(task_id='send_message_telegram',
                     telegram_conn_id='telegram_alerts',
                     chat_id='-1001764749381',
                     text="""DAG `{{ti.dag_id}}` is done!""",
                     )
@dag(
    schedule_interval=None,
    start_date=datetime(2020, 12, 23),
    catchup=False,
    tags=['taskflow']
)
def main():
    a = start_dag()
    b = download_titanic_dataset()
    spark_task_ags = {
        "conn_id": "spark_master_conn",

        'conf': {"spark.jars": POSTGRES_JAR,
                 "spark.driver.extraClassPath": POSTGRES_JAR}

    }

    a >> start_task >> b >> spark_submit_job >> send_message_telegram_task

zapusk = main()