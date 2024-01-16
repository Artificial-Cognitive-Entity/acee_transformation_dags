from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.models import Variable
import os
import json
import logging

# object_name = Variable.get("object_name", default_var=None)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('transform',
         default_args=default_args,
         description='DAG to list files from GCS bucket',
         schedule_interval=None,
         catchup=False)

PROJECT_ID = "acee-sd"


#? 2 - Get each object's metadata -> create initial JSON
def get_object_data(bucket_name, gcp_conn_id, **kwargs):
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    client = gcs_hook.get_conn()
    bucket = client.bucket(bucket_name)
    object_name = kwargs['dag_run'].conf.get('object_name', 'null')
    object_blob = bucket.get_blob(object_name)
        


    if object_blob is not None:
        # Access metadata properties - ensure they exist or provide defaults
        uuid = object_blob.metadata.get('uuid', 'default_uuid') if object_blob.metadata else 'default_uuid'
        id_in_source = object_blob.metadata.get('id_in_source', 'default_id') if object_blob.metadata else 'default_id'
        source_id = object_blob.metadata.get('source_id', 'default_source') if object_blob.metadata else 'default_source'
        category = object_blob.metadata.get('category', 'default_category') if object_blob.metadata else 'default_category'
        author = object_blob.metadata.get('author', 'default_author') if object_blob.metadata else 'default_author'
        url = object_blob.metadata.get('url', 'default_url') if object_blob.metadata else 'default_url'
        title = object_blob.metadata.get('title', 'default_title') if object_blob.metadata else 'default_title'

        # Create initial JSON
        json_dict = {
            "document_data": {
                "doc_id": uuid,
                "source_id": source_id,
                "id_in_source": id_in_source,
                "file_name": object_name,
                "doc_category": category,
                "author": author,
                "url": url,
                "title": title,
                "file_type": object_blob.content_type
            },
            "content_data": {}
        }

        print(f"Metadata for {object_name}: {json_dict}")
        kwargs['ti'].xcom_push(key='object_json', value=json_dict)

    else:
        print(f"No object found with name {object_name}")
    

#? LAST - Upload JSON files to bucket
def upload_to_bucket(bucket_name, gcp_conn_id, **kwargs):

    object_json = kwargs['ti'].xcom_pull(task_ids='get_object_data', key='object_json')
    object_name = object_json["document_data"].get("file_name")
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)


    # Determine Directory Based on Source
    directory = ''
    if object_json["document_data"].get("source_id") == "confluence":
        directory = 'confluence'

    # Generate a file name
    file_name = f"{directory}/file_{datetime.now().strftime('%Y%m%d%H%M%S')}_{object_name}.json"

    # Convert the dictionary to a JSON string
    json_str = json.dumps(object_json, indent=4)

    # Upload the JSON string to the GCP bucket
    gcs_hook.upload(bucket_name=bucket_name, object_name=file_name, data=json_str, mime_type='application/json')

    print(f"Uploaded JSON file to GCS: {file_name}")


    # Define the task using a PythonOperator
get_object_data = PythonOperator(
    task_id='get_object_data',
    python_callable=get_object_data,
    op_kwargs={
        'bucket_name': 'acee-raw-documents',
        'gcp_conn_id': 'google_cloud_default',
    },
    provide_context=True,
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
get_object_data >> upload_to_bucket 
