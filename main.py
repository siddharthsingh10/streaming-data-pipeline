#!/usr/bin/env python3
"""
Minimal Streaming Data Pipeline - Main Script

This script demonstrates a minimal yet complete streaming data ingestion pipeline
as specified in the assignment requirements.

Features:
- Simulates streaming data source (Kafka-based)
- Applies meaningful transformations
- Writes results to structured sink (Parquet)
- Runnable end-to-end with clear setup instructions
"""

import time
import logging
from datetime import datetime
from src.pipeline import StreamingPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run the minimal streaming pipeline demo."""
    logger.info("🚀 Starting Minimal Streaming Data Pipeline")
    
    # Initialize pipeline
    pipeline = StreamingPipeline()
    
    try:
        # Run pipeline for 60 seconds with 5 events per second
        logger.info("📊 Running pipeline for 60 seconds...")
        pipeline.start(duration_seconds=60, events_per_second=5)
        
        # Wait for completion
        time.sleep(65)  # Allow extra time for graceful shutdown
        
        # Get final metrics
        metrics = pipeline.get_metrics()
        logger.info("📈 Pipeline completed successfully!")
        logger.info(f"Events produced: {metrics['events_produced']}")
        logger.info(f"Events processed: {metrics['events_consumed']}")
        logger.info(f"Events written: {metrics['events_written']}")
        logger.info(f"Success rate: {metrics.get('success_rate', 0):.1f}%")
        
        # Show health status
        health = pipeline.get_health_status()
        logger.info(f"Overall health: {health['overall_status']}")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Pipeline stopped by user")
    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}")
    finally:
        # Graceful shutdown
        pipeline.stop()
        logger.info("🏁 Pipeline shutdown complete")


if __name__ == "__main__":
    main() 