"""
Utility functions for Kafka operations and common tasks.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer

from src.config import KAFKA_CONFIG, TOPICS, LOG_CONFIG

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=LOG_CONFIG['handlers']
)
logger = logging.getLogger(__name__)


def setup_logging(name: str) -> logging.Logger:
    """Setup logger for a specific module."""
    return logging.getLogger(name)


def create_kafka_topics() -> bool:
    """
    Create Kafka topics if they don't exist.
    
    Returns:
        bool: True if topics created successfully, False otherwise
    """
    try:
        admin_client = AdminClient(KAFKA_CONFIG)
        
        # Define topics
        topics = [
            NewTopic(
                TOPICS['events'],
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                TOPICS['dead_letter'],
                num_partitions=1,
                replication_factor=1
            )
        ]
        
        # Create topics
        futures = admin_client.create_topics(topics)
        
        # Wait for topics to be created
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                return False
        
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Kafka topics: {e}")
        return False


def test_kafka_connectivity() -> bool:
    """
    Test basic Kafka connectivity.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Test producer connection
        producer = Producer(KAFKA_CONFIG)
        producer.flush(timeout=10)
        producer.close()
        
        # Test consumer connection
        consumer = Consumer(KAFKA_CONFIG)
        consumer.close()
        
        logger.info("Kafka connectivity test passed")
        return True
        
    except KafkaException as e:
        logger.error(f"Kafka connectivity test failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during connectivity test: {e}")
        return False


def get_producer() -> Producer:
    """Get a configured Kafka producer."""
    return Producer(KAFKA_CONFIG)


def get_consumer() -> Consumer:
    """Get a configured Kafka consumer."""
    return Consumer(KAFKA_CONFIG)


def safe_json_serialize(obj: Any) -> str:
    """
    Safely serialize object to JSON string.
    
    Args:
        obj: Object to serialize
        
    Returns:
        str: JSON string representation
    """
    import json
    
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return super().default(obj)
    
    return json.dumps(obj, cls=DateTimeEncoder)


def format_error_message(error: Exception, context: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Format error message for dead letter queue.
    
    Args:
        error: The exception that occurred
        context: Additional context about the error
        
    Returns:
        Dict containing error details
    """
    error_info = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'timestamp': datetime.now().isoformat()
    }
    
    if context:
        error_info['context'] = context
    
    return error_info 