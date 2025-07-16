#!/usr/bin/env python3
"""
Simple test producer to generate messages visible in Control Center UI.
"""
import json
import time
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Kafka configuration
config = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'test-producer'
}

producer = Producer(config)

def generate_event():
    """Generate a test event."""
    event_types = ["page_view", "click", "purchase", "signup", "login", "logout"]
    event_type = fake.random_element(event_types)
    
    event = {
        "user_id": fake.uuid4(),
        "event_type": event_type,
        "timestamp": fake.iso8601(),
        "session_id": fake.uuid4(),
        "source": "web",
        "version": "1.0",
        "page_url": fake.url(),
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
        "country": fake.country()
    }
    
    return event

def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():
    """Produce test messages."""
    print("Producing test messages...")
    
    for i in range(10):
        # Generate valid event
        event = generate_event()
        event_json = json.dumps(event)
        
        # Produce to events topic
        producer.produce(
            topic='events-topic',
            value=event_json.encode('utf-8'),
            callback=delivery_report
        )
        
        # Generate invalid event for dead letter
        invalid_event = event.copy()
        invalid_event['event_type'] = 'invalid_event_type'
        invalid_event_json = json.dumps(invalid_event)
        
        # Produce to dead letter topic
        producer.produce(
            topic='dead-letter-topic',
            value=invalid_event_json.encode('utf-8'),
            callback=delivery_report
        )
        
        print(f"Produced message {i+1}")
        time.sleep(1)
    
    # Flush to ensure delivery
    producer.flush()
    print("Done producing messages!")
    print("Now check http://localhost:9021 to see the messages in Control Center")

if __name__ == "__main__":
    main() 