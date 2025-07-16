#!/usr/bin/env python3
"""
MVP Demo: Producer → Kafka → Consumer → Sink

This script demonstrates the minimal viable streaming pipeline:
1. Producer generates events and sends to Kafka
2. Consumer reads from Kafka and processes events
3. Sink writes processed events to Parquet files

Simple, visual demonstration of the core pipeline flow.
"""

import time
import logging
import json
from datetime import datetime
from src.producer import EventProducer
from src.consumer import EventConsumer
from src.transform import EventTransformer
from src.sink_writer import ParquetSinkWriter
from src.config import TOPICS, BATCH_SIZE, BATCH_TIMEOUT_SECONDS

# Setup logging for demo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_mvp_pipeline():
    """Demo the MVP pipeline flow: Producer → Kafka → Consumer → Sink"""
    
    print("🚀 MVP Demo: Producer → Kafka → Consumer → Sink")
    print("=" * 60)
    
    # Step 1: Initialize Components
    print("\n📦 Step 1: Initializing Pipeline Components")
    print("-" * 40)
    
    producer = EventProducer()
    consumer = EventConsumer()
    transformer = EventTransformer()
    sink_writer = ParquetSinkWriter()
    
    print("✅ Producer initialized")
    print("✅ Consumer initialized") 
    print("✅ Transformer initialized")
    print("✅ Sink writer initialized")
    
    # Step 2: Generate and Send Events
    print("\n📤 Step 2: Producer → Kafka")
    print("-" * 40)
    
    events_to_send = 5
    print(f"📝 Generating {events_to_send} events...")
    
    for i in range(events_to_send):
        event = producer.generate_event()
        producer.send_event(event)
        print(f"  📨 Sent event {i+1}: {event['event_type']} from user {event['user_id']}")
        time.sleep(0.5)  # Visual delay
    
    print("✅ All events sent to Kafka")
    
    # Step 3: Consume and Process Events
    print("\n📥 Step 3: Kafka → Consumer")
    print("-" * 40)
    
    print("🔄 Consuming events from Kafka...")
    messages = consumer.consume_batch(timeout_seconds=5)
    
    if messages:
        print(f"✅ Consumed {len(messages)} events from Kafka")
        for i, msg in enumerate(messages):
            event = json.loads(msg.value())
            print(f"  📥 Received event {i+1}: {event['event_type']} from user {event['user_id']}")
    else:
        print("❌ No events consumed from Kafka")
        return
    
    # Step 4: Transform Events
    print("\n🔄 Step 4: Consumer → Transformer")
    print("-" * 40)
    
    print("🔄 Transforming events...")
    transformed_events = []
    
    for i, msg in enumerate(messages):
        event = json.loads(msg.value())
        try:
            transformed = transformer.transform_user_event(event)
            transformed_events.append(transformed)
            print(f"  🔄 Transformed event {i+1}: {event['event_type']} → {transformed['normalized_event_type']}")
        except Exception as e:
            print(f"  ❌ Failed to transform event {i+1}: {e}")
    
    print(f"✅ Transformed {len(transformed_events)} events")
    
    # Step 5: Write to Sink
    print("\n💾 Step 5: Transformer → Sink")
    print("-" * 40)
    
    if transformed_events:
        print("🔄 Writing events to Parquet sink...")
        
        for i, event in enumerate(transformed_events):
            sink_writer.write_event(event)
            print(f"  💾 Wrote event {i+1}: {event['event_type']} → {event['normalized_event_type']}")
        
        # Flush final batch
        sink_writer.flush()
        stats = sink_writer.get_stats()
        print(f"✅ Wrote {stats['events_written']} events to Parquet files")
        print(f"📁 Output location: {sink_writer.output_dir}")
    else:
        print("❌ No events to write to sink")
    
    # Step 6: Summary
    print("\n📊 MVP Pipeline Summary")
    print("=" * 60)
    print(f"📤 Events Produced: {events_to_send}")
    print(f"📥 Events Consumed: {len(messages)}")
    print(f"🔄 Events Transformed: {len(transformed_events)}")
    print(f"💾 Events Written: {len(transformed_events)}")
    print(f"📈 Success Rate: {(len(transformed_events)/events_to_send)*100:.1f}%")
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    producer.close()
    consumer.close()
    sink_writer.close()
    print("✅ Pipeline components closed")
    
    print("\n🎉 MVP Demo Complete!")
    print("=" * 60)
    print("This demonstrates the core flow:")
    print("Producer → Kafka → Consumer → Transformer → Sink")
    print("\nTo run the full pipeline: python main.py")


def demo_with_error_handling():
    """Demo with error handling to show dead letter queue."""
    
    print("\n🔄 Bonus Demo: Error Handling")
    print("=" * 40)
    
    # Create an invalid event
    invalid_event = {
        "user_id": "user123",
        "event_type": "invalid_event_type",  # This will fail validation
        "timestamp": datetime.now().isoformat()
    }
    
    print("📝 Creating invalid event for error handling demo...")
    
    producer = EventProducer()
    consumer = EventConsumer()
    transformer = EventTransformer()
    
    # Send invalid event
    producer.send_event(invalid_event)
    print("📨 Sent invalid event to Kafka")
    
    # Try to consume and process
    messages = consumer.consume_batch(timeout_seconds=3)
    
    if messages:
        event = json.loads(messages[0].value())
        print(f"📥 Consumed event: {event['event_type']}")
        
        try:
            transformed = transformer.transform_user_event(event)
            print("✅ Event transformed successfully")
        except Exception as e:
            print(f"❌ Event failed transformation: {e}")
            print("💡 This would be sent to dead letter queue in full pipeline")
    
    # Cleanup
    producer.close()
    consumer.close()


if __name__ == "__main__":
    try:
        # Main MVP demo
        demo_mvp_pipeline()
        
        # Optional error handling demo
        demo_with_error_handling()
        
    except KeyboardInterrupt:
        print("\n⏹️ Demo stopped by user")
    except Exception as e:
        print(f"\n❌ Demo error: {e}")
        print("Make sure Kafka is running: docker-compose up -d") 