import json
import os
import random
import boto3
from typing import Dict, Any
from kafka import KafkaProducer

# AWS Lambda Powertools
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit

# Initialize Powertools
logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="MSKProducer")

# Contact schema definition (matches the Java version)
CONTACT_SCHEMA = {
    "type": "record",
    "name": "Contact",
    "namespace": "com.amazonaws.services.lambda.samples.events.msk",
    "fields": [
        {"name": "firstname", "type": ["null", "string"], "default": None},
        {"name": "lastname", "type": ["null", "string"], "default": None},
        {"name": "company", "type": ["null", "string"], "default": None},
        {"name": "street", "type": ["null", "string"], "default": None},
        {"name": "city", "type": ["null", "string"], "default": None},
        {"name": "county", "type": ["null", "string"], "default": None},
        {"name": "state", "type": ["null", "string"], "default": None},
        {"name": "zip", "type": ["null", "string"], "default": None},
        {"name": "homePhone", "type": ["null", "string"], "default": None},
        {"name": "cellPhone", "type": ["null", "string"], "default": None},
        {"name": "email", "type": ["null", "string"], "default": None},
        {"name": "website", "type": ["null", "string"], "default": None},
    ],
}


def create_sample_contact(index: int) -> Dict[str, Any]:
    """Create a sample contact dictionary."""
    # Sample data for variety
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
    ]
    companies = [
        "TechCorp",
        "DataSys",
        "CloudInc",
        "DevCo",
        "InfoTech",
        "SoftWare",
        "NetSolutions",
    ]
    cities = [
        "Seattle",
        "Portland",
        "San Francisco",
        "Los Angeles",
        "Denver",
        "Austin",
        "Chicago",
    ]
    states = ["WA", "OR", "CA", "CA", "CO", "TX", "IL"]

    # Create zip codes with 1000 or 2000 prefix for tracking
    zip_prefix = "1000" if index % 2 == 0 else "2000"
    zip_suffix = f"{random.randint(10, 99):02d}"

    return {
        "firstname": random.choice(first_names),
        "lastname": random.choice(last_names),
        "company": random.choice(companies),
        "street": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
        "city": random.choice(cities),
        "county": f"{random.choice(['King', 'Pierce', 'Snohomish', 'Clark'])} County",
        "state": random.choice(states),
        "zip": f"{zip_prefix}{zip_suffix}",
        "homePhone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "cellPhone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "email": f"user{index}@example.com",
        "website": f"https://www.example{index}.com",
    }


def get_bootstrap_brokers(cluster_arn: str) -> str:
    """Get bootstrap brokers for an MSK cluster."""
    try:
        logger.info(f"Getting bootstrap brokers for cluster: {cluster_arn}")
        kafka_client = boto3.client("kafka")
        response = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_brokers = response["BootstrapBrokerStringSaslIam"]
        logger.info(f"Bootstrap brokers retrieved successfully: {bootstrap_brokers}")
        return bootstrap_brokers
    except Exception as e:
        logger.exception(
            f"Failed to get bootstrap brokers for cluster {cluster_arn}: {str(e)}"
        )
        raise RuntimeError(f"Failed to get bootstrap brokers: {str(e)}") from e


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics
def lambda_handler(event: Dict[str, Any], context: Any) -> str:
    """
    Lambda function handler that produces Avro messages to a Kafka topic.

    Args:
        event: Lambda event containing configuration
        context: Lambda context

    Returns:
        Success message
    """
    logger.info("=== Kafka Producer Lambda started ===")
    logger.info("Event received", extra={"event": event})

    try:
        # Get configuration from environment variables
        cluster_arn = os.environ.get("MSK_CLUSTER_ARN")
        kafka_topic = os.environ.get("MSK_TOPIC", "msk-serverless-topic")
        message_count = int(os.environ.get("MESSAGE_COUNT", "10"))

        if not cluster_arn:
            raise ValueError("MSK_CLUSTER_ARN environment variable is required")

        logger.info(
            "Configuration loaded",
            extra={
                "cluster_arn": cluster_arn,
                "kafka_topic": kafka_topic,
                "message_count": message_count,
            },
        )

        # Get bootstrap brokers
        bootstrap_servers = get_bootstrap_brokers(cluster_arn)
        logger.info(
            "Bootstrap servers retrieved",
            extra={"bootstrap_servers": bootstrap_servers},
        )

        # Create Kafka producer with simplified configuration
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="AWS_MSK_IAM",
            key_serializer=lambda x: x.encode("utf-8") if x else None,
            value_serializer=lambda x: json.dumps(x).encode(
                "utf-8"
            ),  # Simple JSON serialization
            acks="all",
            retries=3,
            max_block_ms=120000,  # 2 minutes
            request_timeout_ms=60000,  # 1 minute
        )
        logger.info("Kafka producer created successfully")

        logger.info(
            "Starting message production",
            extra={"topic": kafka_topic, "message_count": message_count},
        )

        # Track zip code distribution
        zip_1000_count = 0
        zip_2000_count = 0

        # Send messages
        for i in range(message_count):
            # Create sample contact
            contact = create_sample_contact(i)
            message_key = f"contact-{i+1}"

            # Track zip code distribution
            if contact["zip"].startswith("1000"):
                zip_1000_count += 1
            elif contact["zip"].startswith("2000"):
                zip_2000_count += 1

            # Log the contact details
            logger.info(
                "Sending contact",
                extra={
                    "contact_number": i + 1,
                    "message_key": message_key,
                    "contact": contact,
                },
            )

            # Send the message
            future = producer.send(kafka_topic, key=message_key, value=contact)
            record_metadata = future.get(timeout=60)

            logger.info(
                "Message sent successfully",
                extra={
                    "message_number": i + 1,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "topic": record_metadata.topic,
                },
            )

            # Add metrics
            metrics.add_metric(name="MessagesSent", unit=MetricUnit.Count, value=1)

        # Log summary
        logger.info(
            "Message distribution summary",
            extra={
                "zip_1000_count": zip_1000_count,
                "zip_2000_count": zip_2000_count,
                "total_messages": message_count,
            },
        )

        # Add distribution metrics
        metrics.add_metric(
            name="Messages1000Prefix", unit=MetricUnit.Count, value=zip_1000_count
        )
        metrics.add_metric(
            name="Messages2000Prefix", unit=MetricUnit.Count, value=zip_2000_count
        )

        # Close producer
        producer.close()

        success_message = (
            f"Successfully sent {message_count} messages to Kafka topic: {kafka_topic} "
            f"(Zip codes: {zip_1000_count} with prefix 1000, {zip_2000_count} with prefix 2000)"
        )

        logger.info(
            "Kafka Producer Lambda completed successfully",
            extra={"success_message": success_message},
        )
        return success_message

    except Exception as e:
        logger.exception(
            "Error in lambda_handler",
            extra={"error": str(e), "error_type": type(e).__name__},
        )
        metrics.add_metric(name="Errors", unit=MetricUnit.Count, value=1)
        raise RuntimeError(f"Failed to send messages: {str(e)}") from e
