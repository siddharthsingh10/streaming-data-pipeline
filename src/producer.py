"""
Event producer service that generates synthetic events and publishes to Kafka.
"""
import json
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer, KafkaException

from src.config import PRODUCER_CONFIG, TOPICS
from src.utils import setup_logging, format_error_message
from schema.schema_validator import SchemaValidator

# Setup logging
logger = setup_logging(__name__)
fake = Faker()


class EventProducer:
    """
    Producer service that generates synthetic events and publishes to Kafka.
    
    Features:
    - Synthetic event generation using Faker
    - Schema validation using JSON Schema
    - Dead letter queue routing for invalid events
    - Configurable event types and frequencies
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the event producer."""
        self.config = config or PRODUCER_CONFIG
        self.producer = Producer(self.config)
        self.validator = SchemaValidator()
        self.event_count = 0
        self.error_count = 0
        
        logger.info("Event producer initialized")
    
    def generate_user_event(self) -> Dict[str, Any]:
        """
        Generate a synthetic user event.
        
        Returns:
            Dict containing event data
        """
        event_types = ["page_view", "click", "purchase", "signup", "login", "logout"]
        event_type = fake.random_element(event_types)
        
        # Base event structure
        event = {
            "user_id": fake.uuid4(),
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            "session_id": fake.uuid4(),
            "source": "web",
            "version": "1.0"
        }
        
        # Add event-specific data
        if event_type == "page_view":
            event.update({
                "page_url": fake.url(),
                "user_agent": fake.user_agent(),
                "ip_address": fake.ipv4(),
                "country": fake.country()
            })
        elif event_type == "click":
            event.update({
                "page_url": fake.url(),
                "element_id": f"btn_{fake.random_int(min=1, max=100)}",
                "user_agent": fake.user_agent(),
                "ip_address": fake.ipv4()
            })
        elif event_type == "purchase":
            event.update({
                "product_id": f"prod_{fake.random_int(min=1, max=1000)}",
                "amount": round(fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=10.0, max_value=500.0), 2),
                "page_url": fake.url(),
                "user_agent": fake.user_agent(),
                "ip_address": fake.ipv4(),
                "country": fake.country()
            })
        elif event_type in ["signup", "login", "logout"]:
            event.update({
                "page_url": fake.url(),
                "user_agent": fake.user_agent(),
                "ip_address": fake.ipv4(),
                "country": fake.country()
            })
        
        return event
    
    def generate_invalid_event(self) -> Dict[str, Any]:
        """
        Generate an invalid event for testing dead letter queue.
        
        Returns:
            Dict containing invalid event data
        """
        # Generate a valid event first
        event = self.generate_user_event()
        
        # Make it invalid by changing event_type
        event["event_type"] = "invalid_event_type"
        
        return event
    
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
    
    def publish_event(self, event: Dict[str, Any], topic: str) -> bool:
        """
        Publish event to Kafka topic.
        
        Args:
            event: Event data to publish
            topic: Kafka topic name
            
        Returns:
            bool: True if published successfully
        """
        try:
            # Serialize event to JSON
            event_json = json.dumps(event)
            
            # Publish to Kafka
            self.producer.produce(
                topic=topic,
                value=event_json.encode('utf-8'),
                callback=self._delivery_report
            )
            
            # Flush to ensure delivery
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        """Handle delivery reports from Kafka."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
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
                
                # Send to dead letter topic
                dead_letter_event = {
                    "original_event": event,
                    "error_type": "validation_error",
                    "error_message": error_message,
                    "failed_at": datetime.now().isoformat(),
                    "processing_stage": "producer_validation"
                }
                
                self.producer.produce(
                    topic=TOPICS['dead_letter'],
                    value=json.dumps(dead_letter_event),
                    callback=self._delivery_report
                )
                
                return False
            
            # Publish to events topic (no partition key)
            self.producer.produce(
                topic=TOPICS['events'],
                value=json.dumps(event),
                callback=self._delivery_report
            )
            
            self.event_count += 1
            logger.debug(f"Event sent: {event.get('event_id', 'unknown')}")
            return True
        except Exception as e:
            self.error_count += 1
            error_message = format_error_message(e)
            logger.error(f"Failed to send event: {error_message}")
            return False
    
    def run(self, duration_seconds: int = 60, events_per_second: int = 10, 
            invalid_event_ratio: float = 0.1) -> None:
        """
        Run the producer for a specified duration.
        
        Args:
            duration_seconds: How long to run (seconds)
            events_per_second: Events to generate per second
            invalid_event_ratio: Ratio of invalid events to generate
        """
        logger.info(f"Starting producer for {duration_seconds} seconds")
        logger.info(f"Generating {events_per_second} events/second")
        logger.info(f"Invalid event ratio: {invalid_event_ratio}")
        
        start_time = time.time()
        event_interval = 1.0 / events_per_second
        
        try:
            while time.time() - start_time < duration_seconds:
                # Generate event (valid or invalid)
                if fake.random_float() < invalid_event_ratio:
                    event = self.generate_invalid_event()
                else:
                    event = self.generate_user_event()
                
                # Process event
                self.process_event(event)
                
                # Sleep to maintain event rate
                time.sleep(event_interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            # Flush remaining messages
            self.producer.flush()
            logger.info(f"Producer finished. Events: {self.event_count}, Errors: {self.error_count}")
    
    def close(self):
        """Close the producer."""
        self.producer.flush()
        logger.info("Producer closed")


def main():
    """Main function to run the producer."""
    producer = EventProducer()
    
    try:
        # Run for 60 seconds, 10 events/second, 10% invalid events
        producer.run(duration_seconds=60, events_per_second=10, invalid_event_ratio=0.1)
    finally:
        producer.close()


if __name__ == "__main__":
    main() 