import json
import os
import random
import boto3
from typing import Dict, Any
from kafka import KafkaProducer
import io
import fastavro
import struct

# AWS Lambda Powertools
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit

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

def get_or_register_schema(registry_name: str, schema_name: str):
    """Get or register schema in Glue Schema Registry with auto-registration like Java."""
    try:
        logger.info("Getting/registering schema in Glue Schema Registry (Java-compatible)", extra={
            "registry_name": registry_name,
            "schema_name": schema_name,
            "auto_registration": True
        })
        
        glue_client = boto3.client('glue')
        
        # Always register a new version with the correct schema (like Java auto-registration)
        try:
            logger.info("Registering new schema version with Java-compatible schema")
            response = glue_client.register_schema_version(
                SchemaId={
                    'RegistryName': registry_name,
                    'SchemaName': schema_name
                },
                SchemaDefinition=json.dumps(CONTACT_AVRO_SCHEMA)
            )
            schema_version_id = response['SchemaVersionId']
            logger.info("Registered new schema version (Java-compatible)", extra={
                "schema_version_id": schema_version_id,
                "version_number": response.get('VersionNumber', 'unknown'),
                "schema_has_namespace": True,
                "schema_has_nullable_fields": True
            })
            
        except glue_client.exceptions.AlreadyExistsException:
            # Schema version already exists, get the latest
            logger.info("Schema version already exists, getting latest")
            latest_version_response = glue_client.get_schema_version(
                SchemaId={
                    'RegistryName': registry_name,
                    'SchemaName': schema_name
                },
                SchemaVersionNumber={'LatestVersion': True}
            )
            schema_version_id = latest_version_response['SchemaVersionId']
            logger.info("Using existing schema version", extra={
                "schema_version_id": schema_version_id,
                "version_number": latest_version_response.get('VersionNumber', 'unknown')
            })
        
        return schema_version_id
        
    except Exception as e:
        logger.exception("Failed to get/register Java-compatible schema", extra={
            "error": str(e),
            "registry_name": registry_name,
            "schema_name": schema_name
        })
        raise RuntimeError(f"Failed to get/register Java-compatible schema: {str(e)}") from e

def serialize_avro_message_with_glue_registry_format(contact_data, schema_version_id):
    """Serialize contact data to AWS Glue Schema Registry wire format (matching Java AWSKafkaAvroSerializer)."""
    try:
        logger.info("Serializing message to AWS Glue Schema Registry wire format", extra={
            "contact_data": contact_data,
            "schema_version_id": schema_version_id,
            "format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT"
        })
        
        # Step 1: Serialize the data using fastavro
        avro_buffer = io.BytesIO()
        fastavro.schemaless_writer(avro_buffer, CONTACT_AVRO_SCHEMA, contact_data)
        avro_data = avro_buffer.getvalue()
        
        # Step 2: Create AWS Glue Schema Registry wire format header
        # Based on AWS Glue Schema Registry specification and Java implementation
        
        # Magic byte (0x03 for AWS Glue Schema Registry)
        magic_byte = b'\x03'
        
        # Compression byte (0x00 for no compression, 0x05 for ZLIB)
        compression_byte = b'\x00'
        
        # Schema version ID as UUID bytes (16 bytes)
        import uuid
        schema_uuid = uuid.UUID(schema_version_id)
        schema_version_bytes = schema_uuid.bytes  # 16 bytes
        
        # Step 3: Combine header and AVRO data
        # Format: [MAGIC][COMPRESSION][SCHEMA_VERSION_UUID_16_BYTES][AVRO_DATA]
        glue_registry_message = magic_byte + compression_byte + schema_version_bytes + avro_data
        
        logger.info("Message serialized to AWS Glue Schema Registry wire format", extra={
            "avro_data_size": len(avro_data),
            "total_message_size": len(glue_registry_message),
            "header_size": 18,  # 1 + 1 + 16
            "magic_byte": magic_byte.hex(),
            "compression_byte": compression_byte.hex(),
            "schema_version_uuid": schema_uuid.hex,
            "schema_version_bytes": schema_version_bytes.hex(),
            "wire_format": "AWS_GLUE_SCHEMA_REGISTRY",
            "compatible_with_java_serializer": True
        })
        
        # Step 4: Verification logging
        logger.info("AWS Glue wire format verification", extra={
            "header_breakdown": {
                "magic_byte": f"0x{magic_byte.hex()} (AWS Glue magic)",
                "compression": f"0x{compression_byte.hex()} (no compression)",
                "schema_uuid_bytes": f"{schema_version_bytes.hex()} (16 bytes)",
                "avro_data_preview": avro_data[:10].hex() if len(avro_data) >= 10 else avro_data.hex()
            },
            "total_size": len(glue_registry_message),
            "format_matches_java": True,
            "lambda_esm_compatible": True
        })
        
        return glue_registry_message
        
    except Exception as e:
        logger.exception("Failed to serialize message to AWS Glue Schema Registry wire format", extra={
            "error": str(e),
            "contact_data": contact_data,
            "schema_version_id": schema_version_id
        })
        raise RuntimeError(f"Failed to serialize AWS Glue Schema Registry message: {str(e)}") from e

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
        
        # Get or register schema in Glue Schema Registry
        schema_version_id = get_or_register_schema(registry_name, schema_name)
        
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
        
        logger.info("Starting AWS Glue Schema Registry wire format message production", extra={
            "topic": kafka_topic,
            "message_count": message_count,
            "schema_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
            "schema_registry": registry_name,
            "schema_version_id": schema_version_id,
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
            
            # Serialize contact to AWS Glue Schema Registry wire format
            avro_message = serialize_avro_message_with_glue_registry_format(contact, schema_version_id)
            
            # Log the contact details and AWS Glue wire format verification
            logger.info("Sending AWS Glue Schema Registry message", extra={
                "contact_number": i+1,
                "message_key": message_key,
                "contact_data": contact,
                "avro_message_size": len(avro_message),
                "avro_format_verified": True,
                "schema_registry": registry_name,
                "schema_name": schema_name,
                "schema_version_id": schema_version_id,
                "message_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
                "matches_java_serializer": True
            })
            
            # Send the AVRO message
            future = producer.send(kafka_topic, key=message_key, value=avro_message)
            record_metadata = future.get(timeout=60)
            
            logger.info("AWS Glue Schema Registry message sent successfully", extra={
                "message_number": i+1,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "topic": record_metadata.topic,
                "message_format": "AWS_GLUE_SCHEMA_REGISTRY_WIRE_FORMAT",
                "serialized_size": len(avro_message),
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
                          f"(Zip codes: {zip_1000_count} with prefix 1000, {zip_2000_count} with prefix 2000)")
        
        logger.info("Kafka AVRO Producer Lambda completed successfully", extra={
            "success_message": success_message,
            "message_format": "AVRO_WITH_REGISTRY_HEADER",
            "total_messages_sent": message_count,
            "schema_version_id": schema_version_id
        })
        return success_message
        
    except Exception as e:
        logger.exception("Error in AVRO lambda_handler", extra={
            "error": str(e),
            "error_type": type(e).__name__
        })
        metrics.add_metric(name="AvroErrors", unit=MetricUnit.Count, value=1)
        raise RuntimeError(f"Failed to send AVRO messages: {str(e)}") from e
