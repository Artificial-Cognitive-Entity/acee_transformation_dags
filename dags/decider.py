from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from google.cloud import pubsub_v1
import os
import json
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('gcs_to_local',
         default_args=default_args,
         description='DAG to list files from GCS bucket',
         schedule_interval=timedelta(days=1),
         catchup=False)

# Kafka topic name
TOPIC_ID = "bucket-changes"
SUBSCRIPTION = "bucket-changes-sub"
PROJECT_ID = "acee-sd"

# Create a Pub/Sub hook
pubsub_hook = PubSubHook(gcp_conn_id='google_cloud_default')


#? 2 - Get each object's metadata -> create initial JSON
def get_metadata(bucket_name, gcp_conn_id, **context):
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    client = gcs_hook.get_conn()
    bucket = client.bucket(bucket_name)

    bucket_objects = context['ti'].xcom_pull(task_ids='list_gcs_files')
    array_of_json_dicts = []

    # Get the blob for each object
    for object_name in bucket_objects:

        blob = bucket.get_blob(object_name)

        if blob is not None:
            # Access metadata properties - ensure they exist or provide defaults
            uuid = blob.metadata.get('uuid', 'default_uuid') if blob.metadata else 'default_uuid'
            id_in_source = blob.metadata.get('id_in_source', 'default_id') if blob.metadata else 'default_id'
            source_id = blob.metadata.get('source_id', 'default_source') if blob.metadata else 'default_source'
            category = blob.metadata.get('category', 'default_category') if blob.metadata else 'default_category'
            author = blob.metadata.get('author', 'default_author') if blob.metadata else 'default_author'
            url = blob.metadata.get('url', 'default_url') if blob.metadata else 'default_url'
            title = blob.metadata.get('title', 'default_title') if blob.metadata else 'default_title'

            # Create initial JSON
            json_dict = {
                "document_data": {
                    "doc_id": uuid,
                    "source_id": source_id,
                    "id_in_source": id_in_source,
                    "doc_category": category,
                    "author": author,
                    "url": url,
                    "title": title,
                    "file_type": blob.content_type
                },
                "content_data": {}
            }

            # Append file JSON to array
            array_of_json_dicts.append(json_dict)

            print(f"Metadata for {object_name}: {json_dict}")
        else:
            print(f"No object found with name {object_name}")
    
    context['ti'].xcom_push(key='array_of_json_dicts', value=array_of_json_dicts)

#? LAST - Upload JSON files to bucket
def upload_to_bucket(bucket_name, gcp_conn_id, **context):

    array_of_json_dicts = context['ti'].xcom_pull(task_ids='get_metadata', key='array_of_json_dicts')
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    for i, json_dict in enumerate(array_of_json_dicts):
        # Determine Directory Based on Source
        directory = ''
        if json_dict["document_data"].get("source_id") == "confluence":
            directory = 'confluence'

        # Generate a file name
        file_name = f"{directory}/file_{i}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

        # Convert the dictionary to a JSON string
        json_str = json.dumps(json_dict, indent=4)

        # Upload the JSON string to the GCP bucket
        gcs_hook.upload(bucket_name=bucket_name, object_name=file_name, data=json_str, mime_type='application/json')

        print(f"Uploaded JSON file to GCS: {file_name}")

def process_pubsub_messages(ti):
    # Use the result method of the XComArg to pull the message data from XCom.
    # messages = ti.xcom_pull(task_ids='pubsub_sensor', key='messages')
    messages = ti.xcom_pull(task_ids='pubsub_sensor', key='return_value')
    
    if messages:
        for message in messages:
            message_data = message.get('message')

            data = message_data.get('data')
            attributes = message_data.get('attributes')

            # Process the message as required
            logging.info(f"Received message: {data}, Attributes: {attributes}")

    else:
        logging.info("No messages to process.")

    # """Pulls messages from a Pub/Sub subscription and processes them."""
    # if acknowledge set to false
    # subscriber = pubsub_v1.SubscriberClient()
    # subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)
    
    # # Use the result method of the XComArg to pull the message data from XCom.
    # # When 'return_value' is used as a task_id, it refers to the return value of the previous task.
    # messages = ti.xcom_pull(task_ids='pubsub_sensor', key='messages')
    
    # if messages:
    #     for message in messages:
    #         data = message.data
    #         attributes = message.attributes

    #         # Process the message as required
    #         print(f"Received message: {data}, Attributes: {attributes}")

    #         # Acknowledge the message so it won't be sent again
    #         # subscriber.acknowledge(subscription_path, [message.ack_id])
    # else:
    #     print("No messages to process.")

# Create a sensor to check for new events in the topic
sensor = PubSubPullSensor(
    task_id='pubsub_sensor',
    ack_messages=True, # Acknowledge the message after pulling
    subscription=SUBSCRIPTION,
    gcp_conn_id='google_cloud_default',
    project_id=PROJECT_ID,
    do_xcom_push=True,
    dag=dag
)

process_messages = PythonOperator(
    task_id='process_pubsub_messages',
    python_callable=process_pubsub_messages,
    dag=dag,
)


# Task to list files in GCS bucket
list_gcs_files = GCSListObjectsOperator(
    task_id='list_gcs_files',
    bucket='acee-raw-documents',
    gcp_conn_id='google_cloud_default',
    dag=dag
)

    # Define the task using a PythonOperator
get_metadata = PythonOperator(
    task_id='get_metadata',
    python_callable=get_metadata,
    op_kwargs={
        'bucket_name': 'acee-raw-documents',
        'gcp_conn_id': 'google_cloud_default'
    },
    dag=dag
)

# Define the task using a PythonOperator
upload_to_bucket = PythonOperator(
    task_id='upload_to_bucket',
    python_callable=upload_to_bucket,
    op_kwargs={
        'bucket_name': 'acee-normalized-json',
        'gcp_conn_id': 'google_cloud_default'
    },
    dag=dag
)


# Set task dependencies
sensor >> process_messages >> list_gcs_files >> get_metadata >> upload_to_bucket 

# Add a while True loop to make the DAG stand by for incoming messages
while True:
    # Trigger the DAG
    sensor.trigger('pubsub_sensor')