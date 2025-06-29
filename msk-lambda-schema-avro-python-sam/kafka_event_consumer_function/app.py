from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.kafka import ConsumerRecords, SchemaConfig, kafka_consumer
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

# Define the Contact Avro schema (matches the Java version)
contact_avro_schema = """
{
    "type": "record",
    "name": "Contact",
    "namespace": "com.amazonaws.services.lambda.samples.events.msk",
    "fields": [
        {"name": "firstname", "type": ["null", "string"], "default": null},
        {"name": "lastname", "type": ["null", "string"], "default": null},
        {"name": "company", "type": ["null", "string"], "default": null},
        {"name": "street", "type": ["null", "string"], "default": null},
        {"name": "city", "type": ["null", "string"], "default": null},
        {"name": "county", "type": ["null", "string"], "default": null},
        {"name": "state", "type": ["null", "string"], "default": null},
        {"name": "zip", "type": ["null", "string"], "default": null},
        {"name": "homePhone", "type": ["null", "string"], "default": null},
        {"name": "cellPhone", "type": ["null", "string"], "default": null},
        {"name": "email", "type": ["null", "string"], "default": null},
        {"name": "website", "type": ["null", "string"], "default": null}
    ]
}
"""

# Configure schema for Avro deserialization
schema_config = SchemaConfig(
    value_schema_type="AVRO",
    value_schema=contact_avro_schema,
)


@kafka_consumer(schema_config=schema_config)
def lambda_handler(event: ConsumerRecords, context: LambdaContext):
    """
    Lambda handler for processing MSK events with Avro deserialization using AWS Lambda Powertools.
    
    Args:
        event: ConsumerRecords containing Kafka records with automatic Avro deserialization
        context: Lambda context
        
    Returns:
        Success response
    """
    logger.info("=== AvroKafkaHandler called ===")
    
    # Convert generator to list to get count
    records_list = list(event.records)
    logger.info(f"Processing {len(records_list)} records")
    
    try:
        for record in records_list:
            # The record.value is automatically deserialized from Avro to a dictionary
            contact = record.value
            
            logger.info(f"Processing record - Topic: {record.topic}, Partition: {record.partition}, Offset: {record.offset}")
            logger.info(f"Timestamp: {record.timestamp}, TimestampType: {record.timestamp_type}")
            
            # Log the key if present
            if record.key:
                logger.info(f"Record key: {record.key}")
            else:
                logger.info("Record key: None")
            
            # Process the contact data (already deserialized from Avro)
            logger.info(f"Contact data: {contact}")
            
            # Extract specific fields
            if contact:
                firstname = contact.get('firstname')
                lastname = contact.get('lastname')
                zip_code = contact.get('zip')
                email = contact.get('email')
                
                logger.info(f"Contact details - Name: {firstname} {lastname}, Zip: {zip_code}, Email: {email}")
            
            # Process headers if present
            if record.headers:
                logger.info(f"Headers: {record.headers}")
        
        logger.info(f"Successfully processed {len(list(event.records))} records")
        logger.info("=== AvroKafkaHandler completed ===")
        
        return {"statusCode": 200}
        
    except Exception as e:
        logger.error(f"Error processing Kafka records: {str(e)}")
        raise
