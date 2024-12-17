import logging
import json
import os
from kafka import KafkaConsumer
from google.cloud import bigquery
import argparse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TOPIC = "posts"

# Function to create dataset if it doesn't exist
def create_dataset_if_not_exists(client, dataset_id, project_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)  # Make an API request.
        log.info(f"Dataset {dataset_id} already exists.")
    except Exception:
        log.info(f"Dataset {dataset_id} does not exist. Creating it.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # Set to 'EU' or your desired location
        client.create_dataset(dataset)  # Make an API request.
        log.info(f"Created dataset {dataset_id}.")

def insert_post_to_bigquery(client, dataset_id, table_id, post, schema):
    table_ref = client.dataset(dataset_id).table(table_id)
    allowed_columns = {field.name for field in schema}
    filtered_post = {k: v for k, v in post.items() if k in allowed_columns}

    errors = client.insert_rows_json(table_ref, [filtered_post])
    if errors:
        log.error(f"Error inserting post: {errors}")
    else:
        log.info(f"Inserted post with id: {filtered_post.get('id')}")

def consume_and_write_to_bigquery(kafka_host):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[kafka_host],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='post-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./service-account.json"
    client = bigquery.Client()
    dataset_id = 'data_devops'
    table_id = 'posts'

    # Ensure dataset exists
    create_dataset_if_not_exists(client, dataset_id, client.project)
    
    schema = [
        bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('post_type_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('accepted_answer_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('creation_date', 'TIMESTAMP', mode='REQUIRED'),
        bigquery.SchemaField('score', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('view_count', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('body', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('owner_user_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('last_editor_user_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('last_edit_date', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('last_activity_date', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('tags', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('answer_count', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('comment_count', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('content_license', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('parent_id', 'STRING', mode='NULLABLE')
    ]

    for message in consumer:
        try:
            log.info(f"Consumed message: {message.value}")
            insert_post_to_bigquery(client, dataset_id, table_id, message.value, schema)
        except Exception as e:
            log.error(f"Error processing message: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_host', type=str, required=True, help='The Kafka host address')
    args = parser.parse_args()
    
    consume_and_write_to_bigquery(args.kafka_host)