from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.helpers.flush_cache import flush_cache
from dag_datalake_sirene.helpers.execute_slow_queries import (
    execute_slow_requests,
)
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    EMAIL_LIST,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
)

DAG_NAME = "flush_cache_and_execute_queries"

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=10),
    tags=["flush cache and execute queries"],
) as dag:
    flush_cache = PythonOperator(
        task_id="flush_cache",
        provide_context=True,
        python_callable=flush_cache,
        op_args=(
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            REDIS_PASSWORD,
        ),
    )

    execute_slow_requests = PythonOperator(
        task_id="execute_slow_requests",
        provide_context=True,
        python_callable=execute_slow_requests,
    )

    success_email_body = f"""
    Hi, <br><br>
    Flush cache ***{AIRFLOW_ENV}*** DAG has been executed successfully
    at {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-{AIRFLOW_ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    execute_slow_requests.set_upstream(flush_cache)
    send_email.set_upstream(execute_slow_requests)
