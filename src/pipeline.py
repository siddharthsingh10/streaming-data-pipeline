"""
End-to-End Streaming Pipeline Orchestrator

This module provides a complete pipeline that orchestrates all components:
- Producer, Consumer, Transformer, Sink Writer, Dead Letter Handler
- Comprehensive monitoring and observability
- Health checks and metrics collection
- Graceful shutdown and error recovery
"""

import json
import time
import threading
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict

from src.producer import EventProducer, fake
from src.consumer import EventConsumer, DeadLetterConsumer
from src.transform import EventTransformer
from src.sink_writer import ParquetSinkWriter
from src.dead_letter_handler import DeadLetterHandler
from src.config import TOPICS, BATCH_SIZE, BATCH_TIMEOUT_SECONDS
from src.utils import setup_logging, format_error_message

logger = setup_logging(__name__)


@dataclass
class PipelineMetrics:
    """Metrics for the streaming pipeline."""
    start_time: datetime
    events_produced: int = 0
    events_consumed: int = 0
    valid_events_consumed: int = 0  # Events from events-topic
    dead_letter_events_consumed: int = 0  # Events from dead-letter-topic
    events_transformed: int = 0
    events_written: int = 0
    dead_letter_events: int = 0
    errors: int = 0
    batches_processed: int = 0
    processing_time_seconds: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return asdict(self)
    
    def get_success_rate(self) -> float:
        """Calculate success rate."""
        total_events = self.valid_events_consumed
        if total_events == 0:
            return 0.0
        return (self.events_written / total_events) * 100
    
    def get_error_rate(self) -> float:
        """Calculate error rate."""
        total_events = self.valid_events_consumed
        if total_events == 0:
            return 0.0
        return (self.errors / total_events) * 100
    
    def get_total_consumed(self) -> int:
        """Get total events consumed (valid + dead letter)."""
        return self.valid_events_consumed + self.dead_letter_events_consumed


class PipelineHealthChecker:
    """Health checker for pipeline components."""
    
    def __init__(self):
        """Initialize health checker."""
        self.health_status = {}
        self.last_check = None
        
    def check_producer_health(self, producer: EventProducer) -> Dict[str, Any]:
        """Check producer health."""
        try:
            stats = {
                'events_produced': producer.event_count,
                'errors': producer.error_count,
                'status': 'healthy'
            }
            
            # Check for high error rates
            total_events = producer.event_count + producer.error_count
            if total_events > 0 and (producer.error_count / total_events) > 0.1:
                stats['status'] = 'warning'
                stats['message'] = 'High error rate detected'
            
            return stats
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def check_consumer_health(self, consumer: EventConsumer) -> Dict[str, Any]:
        """Check consumer health."""
        try:
            stats = {
                'events_processed': consumer.processed_count,
                'errors': consumer.error_count,
                'batches_processed': consumer.batch_count,
                'status': 'healthy'
            }
            
            # Check for high error rates
            total_events = consumer.processed_count + consumer.error_count
            if total_events > 0 and (consumer.error_count / total_events) > 0.1:
                stats['status'] = 'warning'
                stats['message'] = 'High error rate detected'
            
            return stats
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def check_sink_health(self, sink_writer: ParquetSinkWriter) -> Dict[str, Any]:
        """Check sink writer health."""
        try:
            stats = sink_writer.get_stats()
            stats['status'] = 'healthy'
            
            # Check for write failures
            if stats.get('write_failures', 0) > 0:
                stats['status'] = 'warning'
                stats['message'] = 'Write failures detected'
            
            return stats
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def check_dead_letter_health(self, handler: DeadLetterHandler) -> Dict[str, Any]:
        """Check dead letter handler health."""
        try:
            stats = handler.get_error_statistics()
            stats['status'] = 'healthy'
            
            # Check for processing failures
            if stats.get('failed_dead_letter_events', 0) > 0:
                stats['status'] = 'warning'
                stats['message'] = 'Dead letter processing failures detected'
            
            return stats
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def check_dead_letter_consumer_health(self, consumer: DeadLetterConsumer) -> Dict[str, Any]:
        """Check dead letter consumer health."""
        try:
            stats = {
                'status': 'healthy',
                'message': 'Dead letter consumer operational'
            }
            
            # Basic health check - in a real system you'd track more metrics
            return stats
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def check_overall_health(self, components: Dict[str, Any]) -> Dict[str, Any]:
        """Check overall pipeline health."""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'components': {}
        }
        
        # Check each component
        for name, component in components.items():
            if name == 'producer':
                health_status['components'][name] = self.check_producer_health(component)
            elif name == 'events_consumer':
                health_status['components'][name] = self.check_consumer_health(component)
            elif name == 'dead_letter_consumer':
                health_status['components'][name] = self.check_dead_letter_consumer_health(component)
            elif name == 'sink_writer':
                health_status['components'][name] = self.check_sink_health(component)
            elif name == 'dead_letter_handler':
                health_status['components'][name] = self.check_dead_letter_health(component)
        
        # Determine overall status
        unhealthy_components = [
            comp for comp in health_status['components'].values()
            if comp.get('status') == 'unhealthy'
        ]
        
        warning_components = [
            comp for comp in health_status['components'].values()
            if comp.get('status') == 'warning'
        ]
        
        if unhealthy_components:
            health_status['overall_status'] = 'unhealthy'
        elif warning_components:
            health_status['overall_status'] = 'warning'
        
        self.health_status = health_status
        self.last_check = datetime.now()
        
        return health_status


class StreamingPipeline:
    """
    End-to-End Streaming Pipeline Orchestrator.
    
    Features:
    - Integrated producer and consumer
    - Real-time monitoring and metrics
    - Health checks and alerting
    - Graceful shutdown handling
    - Comprehensive error recovery
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the streaming pipeline."""
        self.config = config or {}
        self.metrics = PipelineMetrics(start_time=datetime.now())
        self.health_checker = PipelineHealthChecker()
        
        # Initialize components
        self.producer = EventProducer()
        self.consumer = EventConsumer()
        self.dead_letter_consumer = DeadLetterConsumer()
        self.transformer = EventTransformer()
        self.sink_writer = ParquetSinkWriter()
        self.dead_letter_handler = DeadLetterHandler()
        
        # Pipeline state
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # Monitoring
        self.monitoring_thread = None
        self.health_check_interval = 30  # seconds
        
        logger.info("Streaming pipeline initialized")
    
    def start(self, duration_seconds: int = 300, events_per_second: int = 10) -> None:
        """
        Start the streaming pipeline.
        
        Args:
            duration_seconds: How long to run the pipeline
            events_per_second: Events to generate per second
        """
        logger.info(f"Starting streaming pipeline for {duration_seconds} seconds")
        logger.info(f"Target rate: {events_per_second} events/second")
        
        self.is_running = True
        self.metrics.start_time = datetime.now()
        
        # Start monitoring thread
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self.monitoring_thread.start()
        
        try:
            # Start producer in separate thread
            producer_thread = threading.Thread(
                target=self._run_producer,
                args=(duration_seconds, events_per_second),
                daemon=True
            )
            producer_thread.start()
            
            # Start events consumer in separate thread
            events_consumer_thread = threading.Thread(
                target=self._run_events_consumer,
                args=(duration_seconds,),
                daemon=True
            )
            events_consumer_thread.start()
            
            # Start dead letter consumer in separate thread
            dead_letter_consumer_thread = threading.Thread(
                target=self._run_dead_letter_consumer,
                args=(duration_seconds,),
                daemon=True
            )
            dead_letter_consumer_thread.start()
            
            # Wait for all threads to complete
            producer_thread.join()
            events_consumer_thread.join()
            dead_letter_consumer_thread.join()
            
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            self.stop()
    
    def _run_producer(self, duration_seconds: int, events_per_second: int) -> None:
        """Run the producer in a separate thread."""
        try:
            start_time = time.time()
            event_interval = 1.0 / events_per_second
            invalid_event_ratio = 0.05  # 5% invalid events (reduced from 10%)
            
            while time.time() - start_time < duration_seconds and not self.shutdown_event.is_set():
                # Generate event (valid or invalid)
                if fake.pyfloat() < invalid_event_ratio:
                    event = self.producer.generate_invalid_event()
                else:
                    event = self.producer.generate_user_event()
                
                # Process event
                success = self.producer.process_event(event)
                
                if success:
                    self.metrics.events_produced += 1
                else:
                    self.metrics.errors += 1
                
                time.sleep(event_interval)
                
        except Exception as e:
            logger.error(f"Producer error: {e}")
    
    def _run_events_consumer(self, duration_seconds: int) -> None:
        """Run the events consumer in a separate thread."""
        try:
            start_time = time.time()
            
            while time.time() - start_time < duration_seconds and not self.shutdown_event.is_set():
                # Consume batch of messages from events topic
                messages = self.consumer.consume_batch()
                
                if messages:
                    # Process valid events (these are already validated by producer)
                    processed, errors = self.process_valid_events(messages)
                    
                    self.metrics.valid_events_consumed += len(messages)
                    self.metrics.events_transformed += processed
                    self.metrics.errors += errors
                    self.metrics.batches_processed += 1
                    
                    logger.info(f"Events batch processed: {processed} events, {errors} errors")
                else:
                    # No messages, sleep briefly
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Events consumer error: {e}")
    
    def process_valid_events(self, messages: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process valid events from the events topic.
        
        Args:
            messages: List of valid events to process
            
        Returns:
            Tuple of (processed_count, error_count)
        """
        processed = 0
        errors = 0
        
        for message in messages:
            try:
                # Transform valid event
                transformed_event = self.transformer.transform_user_event(message)
                
                # Write to Parquet sink
                success = self.sink_writer.add_event(transformed_event)
                
                if success:
                    processed += 1
                    self.metrics.events_written += 1
                else:
                    errors += 1
                    self.metrics.errors += 1
                    
            except Exception as e:
                errors += 1
                self.metrics.errors += 1
                
                # Create dead letter event for processing errors
                dead_letter_event = {
                    "original_event": message,
                    "error_type": type(e).__name__,
                    "error_message": format_error_message(e),
                    "failed_at": datetime.now().isoformat(),
                    "processing_stage": "events_consumer_processing"
                }
                
                # Process dead letter event
                self.dead_letter_handler.process_dead_letter_event(dead_letter_event)
                self.metrics.dead_letter_events += 1
                
                logger.error(f"Failed to process valid event: {e}")
        
        return processed, errors
    
    def _run_dead_letter_consumer(self, duration_seconds: int) -> None:
        """Run the dead letter consumer in a separate thread."""
        try:
            start_time = time.time()
            
            while time.time() - start_time < duration_seconds and not self.shutdown_event.is_set():
                # Consume dead letter messages from dead letter topic
                message = self.dead_letter_consumer.consume_message(timeout=1.0)
                
                if message is not None:
                    # Process dead letter event (write as JSON)
                    self.process_dead_letter_event(message)
                    self.metrics.dead_letter_events += 1
                    self.metrics.dead_letter_events_consumed += 1 # Increment dead letter consumed count
                    logger.warning(f"Processed dead letter event from topic")
                else:
                    # No messages, sleep briefly
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Dead letter consumer error: {e}")
    
    def process_dead_letter_event(self, message: Dict[str, Any]) -> None:
        """
        Process a dead letter event from the dead letter topic.
        Simply write to JSON file without further processing.
        
        Args:
            message: Dead letter event to process
        """
        try:
            # Write dead letter event as JSON for debugging (no further processing)
            success = self.dead_letter_handler.process_dead_letter_event(message)
            
            if success:
                logger.warning(f"Successfully wrote dead letter event to JSON file")
            else:
                logger.error(f"Failed to write dead letter event to JSON file")
                
        except Exception as e:
            logger.error(f"Failed to write dead letter event to JSON: {e}")
            # Don't create new dead letter events - just log the error
    
    def _monitoring_loop(self) -> None:
        """Monitoring loop for health checks and metrics."""
        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Perform health check
                components = {
                    'producer': self.producer,
                    'events_consumer': self.consumer,
                    'dead_letter_consumer': self.dead_letter_consumer,
                    'sink_writer': self.sink_writer,
                    'dead_letter_handler': self.dead_letter_handler
                }
                
                health_status = self.health_checker.check_overall_health(components)
                
                # Log health status
                if health_status['overall_status'] != 'healthy':
                    logger.warning(f"Pipeline health check: {health_status['overall_status']}")
                    logger.warning(f"Health details: {json.dumps(health_status, indent=2)}")
                
                # Update processing time
                self.metrics.processing_time_seconds = (
                    datetime.now() - self.metrics.start_time
                ).total_seconds()
                
                # Sleep until next check
                time.sleep(self.health_check_interval)
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                time.sleep(5)  # Brief sleep on error
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current pipeline metrics."""
        metrics_dict = self.metrics.to_dict()
        metrics_dict['success_rate'] = self.metrics.get_success_rate()
        metrics_dict['error_rate'] = self.metrics.get_error_rate()
        metrics_dict['events_per_second'] = (
            self.metrics.get_total_consumed() / 
            max(self.metrics.processing_time_seconds, 1)
        )
        return metrics_dict
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return self.health_checker.health_status
    
    def analyze_dead_letters(self) -> Dict[str, Any]:
        """Analyze dead letter events for patterns."""
        # This would typically read from dead letter storage
        # For now, return empty analysis
        return {} # Removed DeadLetterAnalyzer.analyze_batch([])
    
    def stop(self) -> None:
        """Stop the pipeline gracefully."""
        logger.info("Stopping streaming pipeline...")
        
        self.is_running = False
        self.shutdown_event.set()
        
        # Close components
        try:
            if self.producer:
                self.producer.close()
            
            if self.consumer:
                self.consumer.close()
            
            if self.dead_letter_consumer:
                self.dead_letter_consumer.close()
            
            if self.sink_writer:
                self.sink_writer.close()
            
            if self.dead_letter_handler:
                self.dead_letter_handler.close()
            
            # Wait for monitoring thread
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=5)
            
        except Exception as e:
            logger.error(f"Error stopping pipeline: {e}")
        
        # Log final metrics
        final_metrics = self.get_metrics()
        # Convert datetime to ISO string for JSON serialization
        if 'start_time' in final_metrics and isinstance(final_metrics['start_time'], (str, int)) is False:
            final_metrics['start_time'] = final_metrics['start_time'].isoformat()
        logger.info(f"Pipeline stopped. Final metrics: {json.dumps(final_metrics, indent=2)}")
        
        self.metrics.processing_time_seconds = (
            datetime.now() - self.metrics.start_time
        ).total_seconds()


def run_pipeline_demo(duration_seconds: int = 60, events_per_second: int = 5) -> None:
    """
    Run a demo of the complete streaming pipeline.
    
    Args:
        duration_seconds: How long to run the demo
        events_per_second: Events per second to generate
    """
    print("=" * 80)
    print("STREAMING PIPELINE DEMO")
    print("=" * 80)
    print(f"Duration: {duration_seconds} seconds")
    print(f"Event Rate: {events_per_second} events/second")
    print("=" * 80)
    
    # Initialize pipeline
    pipeline = StreamingPipeline()
    
    try:
        # Start pipeline
        pipeline.start(duration_seconds, events_per_second)
        
        # Get final metrics
        metrics = pipeline.get_metrics()
        health_status = pipeline.get_health_status()
        
        print("\n" + "=" * 80)
        print("PIPELINE DEMO COMPLETED")
        print("=" * 80)
        
        print("\n📊 FINAL METRICS:")
        print(f"  Events Produced: {metrics['events_produced']}")
        print(f"  Total Events Consumed: {metrics['valid_events_consumed'] + metrics['dead_letter_events_consumed']}")
        print(f"    ├─ Valid Events Consumed: {metrics['valid_events_consumed']}")
        print(f"    └─ Dead Letter Events Consumed: {metrics['dead_letter_events_consumed']}")
        print(f"  Events Transformed: {metrics['events_transformed']}")
        print(f"  Events Written: {metrics['events_written']}")
        print(f"  Dead Letter Events: {metrics['dead_letter_events']}")
        print(f"  Errors: {metrics['errors']}")
        print(f"  Batches Processed: {metrics['batches_processed']}")
        print(f"  Success Rate: {metrics['success_rate']:.2f}%")
        print(f"  Error Rate: {metrics['error_rate']:.2f}%")
        print(f"  Events/Second: {metrics['events_per_second']:.2f}")
        print(f"  Processing Time: {metrics['processing_time_seconds']:.2f} seconds")
        
        print("\n🏥 HEALTH STATUS:")
        for component, status in health_status['components'].items():
            print(f"  {component.title()}: {status['status']}")
        
        print("\n✅ DEMO COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        print(f"\n❌ DEMO FAILED: {e}")
        logger.error(f"Pipeline demo failed: {e}")
    
    finally:
        pipeline.stop()


if __name__ == "__main__":
    # Run pipeline demo
    run_pipeline_demo() 