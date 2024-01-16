from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectSensor
from airflow.providers.kafka.operators.kafka_publish import KafkaPublishOperator

# Define the DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 7),
}

# Define the DAG
dag = DAG('file_change_monitoring_dag', default_args=default_args, schedule_interval='@once')

# Define the bucket name and prefix
bucket_name = 'acee-raw-documents'

# Create the sensor
sensor = GCSObjectSensor(
    task_id='check_for_file_changes',
    bucket_name=bucket_name,
    dag=dag
)

# Define the Kafka topic name
topic_name = 'bucket-events'

# Create the publishing task
publish_task = KafkaPublishOperator(
    task_id='publish_file_change_notifications',
    topic=topic_name,
    request_timeout=60,
    dag=dag
)

sensor >> publish_task
