from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from flask_appbuilder import action
from flask_babel import refresh
from botocore.utils import parse_timestamp
import json
import logging
import boto3
import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  


def get_secret(secret_name, region_name="us-east-1"):
    """Retrieve secrets from AWS secret Manager"""
    logger.info(f"Retrieving secret: {secret_name} from region: {region_name}")
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        logger.info("Secret retrieved successfully.")
        return json.loads(response["SecretString"])
    except Exception as e:
        logger.error(f"Secret retrieval error: {e}")
        raise


def parsed_log_entry(log_entry):
    """Parse a log entry in Common Log Format (CLF)."""
    logger.info(f"Parsing log entry: {log_entry}")

    # Expressão regular corrigida
    log_pattern = r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)" (?P<status>\d+) (?P<bytes>\d+) "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"'

    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Invalid log format: {log_entry}")
        return None

    data = match.groupdict()
    logger.info(f"Parsed log data: {data}")

    try:
        parsed_timestamp = datetime.strptime(data["timestamp"], "%b %d %Y, %H:%M:%S")
        data["@timestamp"] = parsed_timestamp.isoformat()
        logger.info(f"Parsed timestamp: {data['@timestamp']}")
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None

    return data


def consume_and_index_logs(**context):
    """Consume logs from Kafka and index them into Elasticsearch."""
    logger.info("Starting consume_and_index_logs function.")

    # Retrieve secrets from AWS Secrets Manager
    secrets = get_secret("production_secrets_v2")
    logger.info("Secrets retrieved successfully.")

    # Kafka consumer configuration
    consumer_config = {
        "bootstrap.servers": secrets["KAFKA_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": secrets["KAFKA_SASL_USERNAME"],
        "sasl.password": secrets["KAFKA_SASL_PASSWORD"],
        "group.id": "lkc-g776w3",
        "auto.offset.reset": "earliest",
    }
    logger.info(f"Kafka consumer config: {consumer_config}")

    # Elasticsearch configuration
    es_config = {
        "hosts": [secrets["ELASTICSEARCH_URL"]],
        "api_key": secrets["ELASTICSEARCH_API_KEY"],
    }
    logger.info(f"Elasticsearch config: {es_config}")

    # Create Kafka consumer
    consumer = Consumer(consumer_config)
    logger.info("Kafka consumer created successfully.")

    # Create Elasticsearch client
    es = Elasticsearch(**es_config)
    logger.info("Elasticsearch client created successfully.")

    # Subscribe to Kafka topic
    topic = "billion_website_logs"
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")

    # Check if Elasticsearch index exists
    index_name = "billion_website_logs"
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index already exists: {index_name}")
    except Exception as e:
        logger.error(f"Failed to create index: {index_name} {e}")

    logs = []
    max_attempts = 10
    attempts = 0

    try:
        while attempts < max_attempts:
            logger.info(
                f"Polling Kafka for messages. Attempt: {attempts + 1}/{max_attempts}"
            )
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.info("No message received from Kafka.")
                attempts += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.info("Reached end of partition.")
                    break
                logger.error(f"Kafka error: {msg.error()}")
                raise KafkaException(msg.error())

            log_entry = msg.value().decode("utf-8")
            logger.info(f"Received log entry: {log_entry}")
            parsed_log = parsed_log_entry(log_entry)

            if parsed_log:
                logs.append(parsed_log)
                logger.info(
                    f"Parsed log added to batch. Current batch size: {len(logs)}"
                )

            # Reset attempts if a message is received
            attempts = 0

            # Index logs when 15000 logs are collected
            if len(logs) >= 15000:
                actions = [
                    {"_op_type": "create", "_index": index_name, "_source": log}
                    for log in logs
                ]
                logger.info(f"Indexing {len(logs)} logs into Elasticsearch.")

                success, failed = bulk(es, actions, refresh=True)
                logger.info(f"Indexed {success} logs, {len(failed)} failed")
                logs = []

    except Exception as e:
        logger.error(f"Failed to index log: {e}")

    try:
        # Index any remaining logs
        if logs:
            actions = [
                {"_op_type": "create", "_index": index_name, "_source": log}
                for log in logs
            ]
            logger.info(f"Indexing remaining {len(logs)} logs into Elasticsearch.")

            bulk(es, actions, refresh=True)

    except Exception as e:
        logger.error(f"Log processing error: {e}")

    finally:
        consumer.close()
        es.close()
        logger.info("Consumer and Elasticsearch client closed.")


default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    dag_id="log_consumer_pipeline",
    default_args=default_args,
    description="Generate and consume synthetic logs",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["logs", "kafka", "production"],
)

consume_logs_task = PythonOperator(
    task_id='generate_and_consume_logs',
    python_callable=consume_and_index_logs,
    dag=dag
)
