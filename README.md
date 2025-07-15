# Streaming Data Ingestion Pipeline

A minimal yet complete streaming data pipeline that ingests JSON events via Kafka, applies meaningful transformations, and writes output to structured sinks with error handling.

## 🏗 Architecture

```
Producer → Kafka → Consumer → Transform → Parquet Sink
                ↓
            Dead Letter Queue
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### Setup

1. **Start Kafka Infrastructure**
   ```bash
   docker-compose up -d
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Pipeline**
   ```bash
   python main.py
   ```

## 📁 Project Structure

```
streaming_pipeline/
├── src/                    # Core application code
├── schema/                 # Data schemas and validation
├── data/                   # Output data storage
│   ├── output/            # Processed data (Parquet)
│   └── dead/              # Failed events
├── logs/                  # Application logs
├── tests/                 # Unit tests
└── docker-compose.yml     # Kafka infrastructure
```

## 🔧 Components

- **Producer**: Generates synthetic events with validation
- **Consumer**: Processes events with transformations
- **Dead Letter Queue**: Handles failed events
- **Schema Validation**: Language-agnostic validation using JSON Schema
- **Parquet Sink**: Efficient data storage

## 📊 Features

- ✅ Streaming data source simulation (Kafka-based)
- ✅ Meaningful transformations (normalization, enrichment)
- ✅ Structured sink storage (Parquet format)
- ✅ Error handling with dead letter queue
- ✅ Batch processing for efficiency
- ✅ Comprehensive test coverage
- ✅ Runnable end-to-end pipeline

## 🧪 Testing

```bash
pytest tests/
```

## 📝 Design Decisions

See `DESIGN_DOCUMENT.md` for detailed architecture and design rationale.

## 🚀 Quick Demo

Run the minimal pipeline:

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python main.py
```

This will run a 60-second demo with 5 events per second, demonstrating the complete streaming pipeline. 