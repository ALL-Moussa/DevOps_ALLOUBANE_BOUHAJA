from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'post_pusher_dag',
    default_args=default_args,
    description='Publish messages to Kafka',
    schedule_interval='*/1 * * * *',  
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    post_pusher_task = KubernetesPodOperator(
        namespace='default',
        image='2024_kubernetes_post_pusher',
        name='post-pusher',
        task_id='publish-messages',
        is_delete_operator_pod=False,
        get_logs=True,
        image_pull_policy='Never'  
    )

    post_pusher_task