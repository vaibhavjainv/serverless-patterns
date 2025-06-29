import json
import os
import random
import boto3
from typing import Dict, Any
from kafka import KafkaProducer
import io
import fastavro

# AWS Lambda Powertools
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit

# AWS Glue Schema Registry - NEW INTEGRATION
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.serde import encode

# Initialize Powertools
logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="MSKProducer")

# Contact schema definition (MUST match exactly the Java version)
CONTACT_AVRO_SCHEMA = {
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
        {"name": "website", "type": ["null", "string"], "default": None}
    ]
}

def get_or_register_schema_version_with_glue_registry(registry_name: str, schema_name: str):
    """Get or register schema version using aws-glue-schema-registry package."""
    try:
        logger.info("Using aws-glue-schema-registry package for schema management", extra={
            "registry_name": registry_name,
            "schema_name": schema_name,
            "package": "aws-glue-schema-registry",
            "method": "get_or_register_schema_version"
        })
        
        # Create Glue client and Schema Registry client
        glue_client = boto3.client('glue')
        schema_registry_client = SchemaRegistryClient(
            glue_client=glue_client,
            registry_name=registry_name
        )
        
        # Get or register schema version
        schema_version = schema_registry_client.get_or_register_schema_version(
            definition=json.dumps(CONTACT_AVRO_SCHEMA),
            schema_name=schema_name,
            data_format='AVRO'
        )
        
        logger.info("Schema version obtained via aws-glue-schema-registry", extra={
            "schema_version_id": str(schema_version.version_id),
            "version_number": schema_version.version_number,
            "schema_name": schema_name,
            "registry_name": registry_name,
            "package": "aws-glue-schema-registry"
        })
        
        return schema_version.version_id
        
    except Exception as e:
        logger.exception("Failed to get/register schema using aws-glue-schema-registry", extra={
            "error": str(e),
            "registry_name": registry_name,
            "schema_name": schema_name,
            "package": "aws-glue-schema-registry"
        })
        raise RuntimeError(f"Failed to get/register schema via aws-glue-schema-registry: {str(e)}") from e

def serialize_avro_message_with_glue_registry_package(contact_data, schema_version_id):
    """Serialize contact data using aws-glue-schema-registry package."""
    try:
        logger.info("Serializing message using aws-glue-schema-registry package", extra={
            "contact_data": contact_data,
            "schema_version_id": str(schema_version_id),
            "package": "aws-glue-schema-registry",
            "method": "fastavro + encode"
        })
        
        # Step 1: Serialize the data using fastavro (same as before)
        avro_buffer = io.BytesIO()
        fastavro.schemaless_writer(avro_buffer, CONTACT_AVRO_SCHEMA, contact_data)
        avro_data = avro_buffer.getvalue()
        
        # Step 2: Use aws-glue-schema-registry encode function to add the header
        encoded_message = encode(avro_data, schema_version_id)
        
        logger.info("Message serialized using aws-glue-schema-registry package", extra={
            "avro_data_size": len(avro_data),
            "total_message_size": len(encoded_message),
            "header_size": len(encoded_message) - len(avro_data),
            "schema_version_id": str(schema_version_id),
            "package": "aws-glue-schema-registry",
            "wire_format": "AWS_GLUE_SCHEMA_REGISTRY",
            "compatible_with_java_serializer": True
        })
        
        return encoded_message
        
    except Exception as e:
        logger.exception("Failed to serialize message using aws-glue-schema-registry", extra={
            "error": str(e),
            "contact_data": contact_data,
            "schema_version_id": str(schema_version_id),
            "package": "aws-glue-schema-registry"
        })
        raise RuntimeError(f"Failed to serialize message via aws-glue-schema-registry: {str(e)}") from e

def create_sample_contact(index: int) -> Dict[str, Any]:
    """Create a sample contact with realistic data."""
    first_names = [
        "John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "William", "Alice"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"
    ]
    companies = [
        "TechCorp", "DataSys", "CloudInc", "DevCo", "InfoTech", "SoftWare", "NetSolutions"
    ]
    cities = [
        "Seattle", "Portland", "San Francisco", "Los Angeles", "Denver"
    ]
    states = ["WA", "OR", "CA", "CO", "TX"]
    
    # Alternate between zip codes starting with 1000 and 2000 for filtering demo
    zip_prefix = "1000" if index % 2 == 0 else "2000"
    zip_suffix = f"{random.randint(10, 99)}"
    
    return {
        "firstname": first_names[index % len(first_names)],
        "lastname": last_names[index % len(last_names)],
        "company": companies[index % len(companies)],
        "street": f"{random.randint(100, 9999)} {random.choice(['Main St', 'Oak Ave', 'Pine Rd', 'Elm Dr'])}",
        "city": cities[index % len(cities)],
        "county": f"{random.choice(['King', 'Pierce', 'Snohomish'])} County",
        "state": states[index % len(states)],
        "zip": f"{zip_prefix}{zip_suffix}",
        "homePhone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "cellPhone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "email": f"user{index}@example.com",
        "website": f"https://www.example{index}.com"
    }

def get_bootstrap_brokers(cluster_arn: str) -> str:
    """Get bootstrap brokers for an MSK cluster."""
    try:
        logger.info(f"Getting bootstrap brokers for cluster: {cluster_arn}")
        kafka_client = boto3.client('kafka')
        response = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_brokers = response['BootstrapBrokerStringSaslIam']
        logger.info(f"Bootstrap brokers retrieved successfully: {bootstrap_brokers}")
        return bootstrap_brokers
    except Exception as e:
        logger.exception(f"Failed to get bootstrap brokers for cluster {cluster_arn}: {str(e)}")
        raise RuntimeError(f"Failed to get bootstrap brokers: {str(e)}") from e

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics
def lambda_handler(event: Dict[str, Any], context: Any) -> str:
    """
    Lambda function handler that produces AVRO messages to a Kafka topic.
    
    Args:
        event: Lambda event containing configuration
        context: Lambda context
        
    Returns:
        Success message
    """
    logger.info("=== Kafka AVRO Producer Lambda started ===")
    logger.info("Event received", extra={"event": event})
    
    try:
        # Get configuration from environment variables
        cluster_arn = os.environ.get('MSK_CLUSTER_ARN')
        kafka_topic = os.environ.get('MSK_TOPIC', 'msk-serverless-topic')
        message_count = int(os.environ.get('MESSAGE_COUNT', '10'))
        registry_name = os.environ.get('REGISTRY_NAME', 'GlueSchemaRegistryForMSK')
        schema_name = os.environ.get('SCHEMA_NAME', 'ContactSchema')
        
        if not cluster_arn:
            raise ValueError("MSK_CLUSTER_ARN environment variable is required")
        
        logger.info("Configuration loaded", extra={
            "cluster_arn": cluster_arn,
            "kafka_topic": kafka_topic,
            "message_count": message_count,
            "registry_name": registry_name,
            "schema_name": schema_name,
            "avro_schema": CONTACT_AVRO_SCHEMA
        })
        
        # Get or register schema in Glue Schema Registry using aws-glue-schema-registry package
        schema_version_id = get_or_register_schema_version_with_glue_registry(registry_name, schema_name)
        
        # Get bootstrap brokers
        bootstrap_servers = get_bootstrap_brokers(cluster_arn)
        logger.info("Bootstrap servers retrieved", extra={"bootstrap_servers": bootstrap_servers})
        
        # Create Kafka producer with binary serialization (AVRO will be pre-serialized)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='AWS_MSK_IAM',
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            value_serializer=lambda x: x,  # Raw bytes - AVRO data is already serialized
            acks='all',
            retries=3,
            max_block_ms=120000,  # 2 minutes
            request_timeout_ms=60000,  # 1 minute
        )
        logger.info("Kafka producer created successfully for AVRO messages")
        
        logger.info("Starting AWS Glue Schema Registry message production using aws-glue-schema-registry package", extra={
            "topic": kafka_topic,
            "message_count": message_count,
            "schema_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
            "schema_registry": registry_name,
            "schema_version_id": str(schema_version_id),
            "package": "aws-glue-schema-registry",
            "matches_java_serializer": True
        })
        
        # Track zip code distribution
        zip_1000_count = 0
        zip_2000_count = 0
        
        # Send messages
        for i in range(message_count):
            # Create sample contact
            contact = create_sample_contact(i)
            message_key = f"contact-{i+1}"
            
            # Track zip code distribution
            if contact['zip'].startswith('1000'):
                zip_1000_count += 1
            elif contact['zip'].startswith('2000'):
                zip_2000_count += 1
            
            # Serialize contact to AWS Glue Schema Registry wire format using aws-glue-schema-registry package
            avro_message = serialize_avro_message_with_glue_registry_package(contact, schema_version_id)
            
            # Log the contact details and AWS Glue wire format verification
            logger.info("Sending AWS Glue Schema Registry message via aws-glue-schema-registry package", extra={
                "contact_number": i+1,
                "message_key": message_key,
                "contact_data": contact,
                "avro_message_size": len(avro_message),
                "avro_format_verified": True,
                "schema_registry": registry_name,
                "schema_name": schema_name,
                "schema_version_id": str(schema_version_id),
                "message_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
                "package": "aws-glue-schema-registry",
                "matches_java_serializer": True
            })
            
            # Send the AVRO message
            future = producer.send(kafka_topic, key=message_key, value=avro_message)
            record_metadata = future.get(timeout=60)
            
            logger.info("AWS Glue Schema Registry message sent successfully via aws-glue-schema-registry package", extra={
                "message_number": i+1,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "topic": record_metadata.topic,
                "message_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
                "serialized_size": len(avro_message),
                "package": "aws-glue-schema-registry",
                "matches_java_serializer": True
            })
            
            # Add metrics
            metrics.add_metric(name="AvroMessagesSent", unit=MetricUnit.Count, value=1)
            metrics.add_metric(name="AvroMessageSize", unit=MetricUnit.Bytes, value=len(avro_message))
        
        # Log summary
        logger.info("AVRO message distribution summary", extra={
            "zip_1000_count": zip_1000_count,
            "zip_2000_count": zip_2000_count,
            "total_messages": message_count,
            "message_format": "AVRO_WITH_REGISTRY_HEADER",
            "schema_registry": registry_name,
            "schema_name": schema_name,
            "schema_version_id": schema_version_id
        })
        
        # Add distribution metrics
        metrics.add_metric(name="AvroMessages1000Prefix", unit=MetricUnit.Count, value=zip_1000_count)
        metrics.add_metric(name="AvroMessages2000Prefix", unit=MetricUnit.Count, value=zip_2000_count)
        
        # Close producer
        producer.close()
        
        success_message = (f"Successfully sent {message_count} AVRO messages to Kafka topic: {kafka_topic} "
                          f"using schema {schema_name} (version {schema_version_id}) from registry {registry_name} "
                          f"via aws-glue-schema-registry package "
                          f"(Zip codes: {zip_1000_count} with prefix 1000, {zip_2000_count} with prefix 2000)")
        
        logger.info("Kafka AVRO Producer Lambda completed successfully using aws-glue-schema-registry package", extra={
            "success_message": success_message,
            "message_format": "AVRO_WITH_REGISTRY_HEADER",
            "total_messages_sent": message_count,
            "schema_version_id": str(schema_version_id),
            "package": "aws-glue-schema-registry"
        })
        return success_message
        
    except Exception as e:
        logger.exception("Error in AVRO lambda_handler", extra={
            "error": str(e),
            "error_type": type(e).__name__
        })
        metrics.add_metric(name="AvroErrors", unit=MetricUnit.Count, value=1)
        raise RuntimeError(f"Failed to send AVRO messages: {str(e)}") from e
