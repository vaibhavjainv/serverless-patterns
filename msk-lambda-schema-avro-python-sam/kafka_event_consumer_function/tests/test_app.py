import json
import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import the app module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import lambda_handler, decode_avro_message, process_kafka_record

class TestLambdaHandler:
    """Test cases for the Lambda handler function."""
    
    def test_lambda_handler_empty_event(self):
        """Test lambda handler with empty event."""
        event = {"records": {}}
        context = MagicMock()
        
        result = lambda_handler(event, context)
        assert result == "OK"
    
    def test_lambda_handler_with_records(self):
        """Test lambda handler with sample MSK records."""
        event = {
            "records": {
                "arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid": [
                    {
                        "topic": "test-topic",
                        "partition": 0,
                        "offset": 123,
                        "timestamp": 1234567890,
                        "timestampType": "CREATE_TIME",
                        "key": "dGVzdC1rZXk=",  # base64 encoded "test-key"
                        "value": "eyJmaXJzdG5hbWUiOiAiSm9obiIsICJsYXN0bmFtZSI6ICJEb2UifQ==",  # base64 encoded JSON
                        "headers": []
                    }
                ]
            }
        }
        context = MagicMock()
        
        result = lambda_handler(event, context)
        assert result == "OK"
    
    def test_decode_avro_message_json_fallback(self):
        """Test Avro message decoding with JSON fallback."""
        # Base64 encoded JSON: {"firstname": "John", "lastname": "Doe"}
        encoded_value = "eyJmaXJzdG5hbWUiOiAiSm9obiIsICJsYXN0bmFtZSI6ICJEb2UifQ=="
        
        result = decode_avro_message(encoded_value)
        
        # Should fallback to JSON decoding
        assert isinstance(result, dict)
        assert result.get("firstname") == "John"
        assert result.get("lastname") == "Doe"
    
    def test_decode_avro_message_invalid_data(self):
        """Test Avro message decoding with invalid data."""
        # Invalid base64
        encoded_value = "invalid-base64-data"
        
        result = decode_avro_message(encoded_value)
        
        # Should return error dict
        assert isinstance(result, dict)
        assert "error" in result
    
    def test_process_kafka_record(self):
        """Test processing a single Kafka record."""
        record = {
            "topic": "test-topic",
            "partition": 0,
            "offset": 123,
            "timestamp": 1234567890,
            "timestampType": "CREATE_TIME",
            "key": "dGVzdC1rZXk=",  # base64 encoded "test-key"
            "value": "eyJmaXJzdG5hbWUiOiAiSm9obiIsICJsYXN0bmFtZSI6ICJEb2UifQ==",  # base64 encoded JSON
            "headers": []
        }
        
        # Should not raise any exceptions
        process_kafka_record(record)

if __name__ == "__main__":
    pytest.main([__file__])
