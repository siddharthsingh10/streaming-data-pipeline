"""
Configuration settings for the streaming pipeline.
"""
import os
import logging
from typing import Dict, Any

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'streaming-pipeline',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'group.id': 'streaming-pipeline-group'
}

# Producer Configuration
PRODUCER_CONFIG = {
    **KAFKA_CONFIG,
    'acks': 'all',
    'retries': 3,
    'batch.size': 16384,
    'linger.ms': 10
}

# Consumer Configuration
CONSUMER_CONFIG = {
    **KAFKA_CONFIG,
    'enable.auto.commit': False,
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 3000
}

# Topics
TOPICS = {
    'events': 'events-topic',
    'dead_letter': 'dead-letter-topic'
}

# Data Processing
BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3

# File Paths
DATA_DIR = 'data'
OUTPUT_DIR = os.path.join(DATA_DIR, 'output')
DEAD_LETTER_DIR = os.path.join(DATA_DIR, 'dead')
LOGS_DIR = 'logs'

# Ensure directories exist
for directory in [OUTPUT_DIR, DEAD_LETTER_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging Configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': [
        logging.FileHandler(os.path.join(LOGS_DIR, 'pipeline.log')),
        logging.StreamHandler()
    ]
} 