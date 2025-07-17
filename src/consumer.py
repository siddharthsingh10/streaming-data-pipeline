"""
Kafka consumer service that processes events and applies transformations.
"""
import json
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError

from src.config import CONSUMER_CONFIG, TOPICS, BATCH_SIZE, BATCH_TIMEOUT_SECONDS
from src.utils import setup_logging, format_error_message
from schema.schema_validator import SchemaValidator
from src.transform import EventTransformer
from src.sink_writer import ParquetSinkWriter
from src.dead_letter_handler import DeadLetterHandler

logger = setup_logging(__name__)


class EventConsumer:
    """
    Kafka consumer that processes events and applies transformations.
    
    Features:
    - Batch processing for efficiency
    - Schema validation at consumer level
    - Error handling and dead letter queue
    - Graceful shutdown handling
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the event consumer."""
        self.config = config or CONSUMER_CONFIG
        self.consumer = Consumer(self.config)
        self.validator = SchemaValidator()
        self.transformer = EventTransformer()
        self.sink_writer = ParquetSinkWriter()
        self.dead_letter_handler = DeadLetterHandler()
        
        # Statistics
        self.processed_count = 0
        self.error_count = 0
        self.batch_count = 0
        
        # Subscribe to events topic
        self.consumer.subscribe([TOPICS['events']])
        logger.info("Event consumer initialized")
    
    def consume_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Consume a single message from Kafka.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Parsed message or None if no message available
        """
        try:
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                    return None
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    return None
            
            # Parse JSON message
            try:
                event_data = json.loads(msg.value().decode('utf-8'))
                logger.debug(f"Consumed message: {event_data.get('event_id', 'unknown')}")
                return event_data
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON message: {e}")
                return None
                
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error consuming message: {e}")
            return None
    
    def validate_event(self, event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate event against schema.
        
        Args:
            event: Event data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            self.validator.validate_user_event(event)
            return True, None
        except Exception as e:
            return False, str(e)
    
    def process_event(self, event: Dict[str, Any]) -> bool:
        """
        Process a single event (validate, transform, and store).
        
        Args:
            event: Event data to process
            
        Returns:
            bool: True if processed successfully
        """
        try:
            # Validate event
            is_valid, error_message = self.validate_event(event)
            
            if not is_valid:
                logger.warning(f"Invalid event: {error_message}")
                self.error_count += 1
                return False
            
            # Transform event
            transformed_event = self.transformer.transform_user_event(event)
            
            # Write to sink
            success = self.sink_writer.add_event(transformed_event)
            
            if success:
                logger.debug(f"Transformed event: {transformed_event.get('event_id', 'unknown')}")
                self.processed_count += 1
                return True
            else:
                logger.error(f"Failed to write event to sink: {transformed_event.get('event_id', 'unknown')}")
                self.error_count += 1
                return False
            
        except Exception as e:
            self.error_count += 1
            error_message = format_error_message(e)
            
            # Create dead letter event
            dead_letter_event = {
                "original_event": event,
                "error_type": type(e).__name__,
                "error_message": error_message,
                "failed_at": datetime.now().isoformat(),
                "processing_stage": "consumer_validation"
            }
            
            # Process dead letter event
            self.dead_letter_handler.process_dead_letter_event(dead_letter_event)
            
            logger.error(f"Failed to process event: {error_message}")
            return False
    
    def process_batch(self, events: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process a batch of events.
        
        Args:
            events: List of events to process
            
        Returns:
            Tuple of (processed_count, error_count)
        """
        processed = 0
        errors = 0
        
        for event in events:
            if self.process_event(event):
                processed += 1
            else:
                errors += 1
        
        return processed, errors
    
    def consume_batch(self, batch_size: int = None, timeout: float = None) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages from Kafka.
        
        Args:
            batch_size: Maximum number of messages to consume
            timeout: Maximum time to wait for messages
            
        Returns:
            List of consumed messages
        """
        batch_size = batch_size or BATCH_SIZE
        timeout = timeout or BATCH_TIMEOUT_SECONDS
        
        messages = []
        start_time = time.time()
        
        while len(messages) < batch_size and (time.time() - start_time) < timeout:
            message = self.consume_message(timeout=0.1)
            
            if message is not None:
                messages.append(message)
            else:
                # No more messages available
                break
        
        return messages
    
    def run(self, duration_seconds: int = 60) -> None:
        """
        Run the consumer for a specified duration.
        
        Args:
            duration_seconds: How long to run (seconds)
        """
        logger.info(f"Starting consumer for {duration_seconds} seconds")
        
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                # Consume batch of messages
                messages = self.consume_batch()
                
                if messages:
                    # Process batch
                    processed, errors = self.process_batch(messages)
                    
                    self.batch_count += 1
                    logger.info(f"Batch {self.batch_count}: Processed {processed}, Errors {errors}")
                else:
                    # No messages, sleep briefly
                    time.sleep(0.1)
                    
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()
            logger.info(f"Consumer finished. Processed: {self.processed_count}, Errors: {self.error_count}")
    
    def close(self):
        """Close the consumer and flush remaining events."""
        try:
            # Flush remaining events to sink
            if self.sink_writer:
                self.sink_writer.close()
            
            # Close dead letter handler
            if self.dead_letter_handler:
                self.dead_letter_handler.close()
            
            # Close Kafka consumer
            if self.consumer:
                self.consumer.close()
            
            logger.info("Consumer closed")
            
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the consumer.
        
        Returns:
            Dict containing statistics
        """
        sink_stats = self.sink_writer.get_stats() if self.sink_writer else {}
        dead_letter_stats = self.dead_letter_handler.get_error_statistics() if self.dead_letter_handler else {}
        
        return {
            "processed_events": self.processed_count,
            "error_count": self.error_count,
            "batch_count": self.batch_count,
            "sink_stats": sink_stats,
            "dead_letter_stats": dead_letter_stats
        }


class DeadLetterConsumer:
    """
    Consumer for dead letter queue events.
    
    This consumer processes events that failed validation or processing
    and logs them for debugging and potential reprocessing.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the dead letter consumer."""
        self.config = config or CONSUMER_CONFIG.copy()
        self.config['group.id'] = 'dead-letter-consumer-group'
        self.consumer = Consumer(self.config)
        
        # Initialize dead letter handler for file writing
        self.dead_letter_handler = DeadLetterHandler()
        
        # Subscribe to dead letter topic
        self.consumer.subscribe([TOPICS['dead_letter']])
        logger.info("Dead letter consumer initialized")
    
    def process_dead_letter_event(self, event: Dict[str, Any]) -> None:
        """
        Process a dead letter event.
        
        Args:
            event: Dead letter event data
        """
        try:
            # Log the dead letter event
            logger.warning(f"Dead letter event: {event.get('error_message', 'Unknown error')}")
            logger.warning(f"Original event: {event.get('original_event', {})}")
            logger.warning(f"Processing stage: {event.get('processing_stage', 'Unknown')}")
            
            # Write dead letter event to file using the handler
            success = self.dead_letter_handler.process_dead_letter_event(event)
            
            if success:
                logger.warning(f"Successfully wrote dead letter event to file")
            else:
                logger.error(f"Failed to write dead letter event to file")
            
        except Exception as e:
            logger.error(f"Failed to process dead letter event: {e}")
    
    def run(self, duration_seconds: int = 60) -> None:
        """
        Run the dead letter consumer.
        
        Args:
            duration_seconds: How long to run (seconds)
        """
        logger.info(f"Starting dead letter consumer for {duration_seconds} seconds")
        
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                message = self.consume_message(timeout=1.0)
                
                if message is not None:
                    self.process_dead_letter_event(message)
                    
        except KeyboardInterrupt:
            logger.info("Dead letter consumer stopped by user")
        except Exception as e:
            logger.error(f"Dead letter consumer error: {e}")
        finally:
            self.close()
    
    def consume_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Consume a single message from dead letter topic."""
        try:
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                logger.error(f"Dead letter consumer error: {msg.error()}")
                return None
            
            # Parse JSON message
            try:
                event_data = json.loads(msg.value().decode('utf-8'))
                return event_data
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse dead letter JSON: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Unexpected error in dead letter consumer: {e}")
            return None
    
    def close(self):
        """Close the dead letter consumer."""
        try:
            if self.dead_letter_handler:
                self.dead_letter_handler.close()
            
            if self.consumer:
                self.consumer.close()
            
            logger.info("Dead letter consumer closed")
        except Exception as e:
            logger.error(f"Error closing dead letter consumer: {e}")


def main():
    """Main function to run the consumer."""
    consumer = EventConsumer()
    
    try:
        # Run for 60 seconds
        consumer.run(duration_seconds=60)
    finally:
        consumer.close()


if __name__ == "__main__":
    main() 