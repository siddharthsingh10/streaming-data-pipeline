# Streaming Data Ingestion Pipeline

Data pipeline that ingests JSON events via Kafka, applies meaningful transformations, and writes output to structured sinks with error handling.

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

1. **Start Kafka Infrastructure (Confluent)**
   ```bash
   docker-compose up -d
   ```
   This starts:
   - **Zookeeper**: For Kafka coordination
   - **Confluent Kafka**: Message broker
   - **Confluent Control Center**: Web UI for monitoring

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Pipeline**
   ```bash
   python main.py
   ```

4. **Monitor via Web UI** (Optional)
   - Open http://localhost:9021 in your browser
   - View topics, messages, and cluster health
   - Monitor both `events-topic` and `dead-letter-topic`

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
├── docker-compose.yml     # Confluent Kafka infrastructure
└── test_producer.py       # Manual message generation tool
```

## 🔧 Components

- **Producer**: Generates synthetic events with validation (10% invalid events for testing)
- **Consumer**: Processes events with transformations
- **Dead Letter Queue**: Handles failed events with detailed error information
- **Schema Validation**: Language-agnostic validation using JSON Schema
- **Parquet Sink**: Efficient data storage
- **Confluent Control Center**: Web-based monitoring and management

## 📊 Features

- ✅ Streaming data source simulation (Kafka-based)
- ✅ Meaningful transformations (normalization, enrichment)
- ✅ Structured sink storage (Parquet format)
- ✅ Error handling with dead letter queue
- ✅ Batch processing for efficiency
- ✅ Comprehensive test coverage
- ✅ Runnable end-to-end pipeline
- ✅ Web UI monitoring (Confluent Control Center)
- ✅ Manual message generation for testing

## 🧪 Testing

```bash
pytest tests/
```

## 📝 Design Decisions

See `DESIGN_DOCUMENT.md` for detailed architecture and design rationale.

## 🚀 Quick Demo

### Option 1: MVP Demo (Recommended)
See the core pipeline flow step-by-step:

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the MVP demo
python demo_mvp.py
```

This demonstrates: **Producer → Kafka → Consumer → Transformer → Sink**

### Option 2: Full Pipeline
Run the complete streaming pipeline:

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python main.py
```

This runs a 60-second demo with 5 events per second, demonstrating the complete streaming pipeline with monitoring.

### Option 3: Manual Message Generation
Generate test messages for UI monitoring:

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Generate test messages
python test_producer.py
```

Then visit http://localhost:9021 to see the messages in the Control Center UI.

## 🔍 Monitoring

### Confluent Control Center
- **URL**: http://localhost:9021
- **Features**:
  - Real-time topic monitoring
  - Message browsing and search
  - Cluster health metrics
  - Consumer group monitoring
  - Dead letter queue inspection

### Topics to Monitor
- **events-topic**: Valid processed events
- **dead-letter-topic**: Invalid events with error details

## 🛠 Troubleshooting

### Kafka Connection Issues
If you can't connect to Kafka:
1. Ensure Docker is running
2. Check container status: `docker ps`
3. Verify Kafka logs: `docker logs kafka`

### Control Center Not Loading
1. Wait 2-3 minutes for full startup
2. Check logs: `docker logs control-center`
3. Ensure port 9021 is accessible

### No Messages in UI
1. Run `python test_producer.py` to generate fresh messages
2. Messages are consumed quickly by the pipeline
3. Use the test producer to create persistent messages for UI viewing 
