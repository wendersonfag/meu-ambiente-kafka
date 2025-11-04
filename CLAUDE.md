# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Kafka + Spark Streaming development environment for data engineering education. It consists of a Python Kafka producer generating fake order, event, and GPS data, and a Spark consumer that processes these streams in real-time.

The environment supports both local development (manual Spark execution) and remote testing via Databricks (using Bore tunneling).

## Architecture

### Services (docker-compose.yml)

- **Zookeeper** (port 2181): Kafka coordination service
- **Kafka Broker** (ports 9092, 29092, 19092): Message broker with three listeners:
  - `kafka:9092` - Internal container communication (PLAINTEXT)
  - `localhost:29092` - Local host access (PLAINTEXT_HOST)
  - `bore.pub:xxxxx` - External access via Bore tunnel (EXTERNAL)
- **python-producer**: Generates fake data to 3 Kafka topics every 2 seconds
- **spark-dev**: Spark container for manual consumer execution (kept alive with `tail -f /dev/null`)

### Data Flow

1. **Producer** (`producer/producer.py`) generates linked data:
   - Topic `ordem`: Order/payment data (payment_id, order_key, amount, currency, method, status, card info)
   - Topic `eventos`: Payment events (event_id, payment_id, event_name, timestamp)
   - Topic `gps`: GPS location data (gps_id, order_id, lat, lon, speed_kph)
   - Data is linked: eventos uses payment_id from ordem, gps uses order_key from ordem

2. **Consumer** (`spark-consumer/consumer.py`):
   - Spark Structured Streaming application
   - Consumes from all 3 topics simultaneously
   - Parses JSON with explicit schemas
   - Outputs to console with 5-second trigger interval
   - Uses checkpoint location `/tmp/checkpoint`

## Development Workflows

### Standard Local Development (Windows)

**IMPORTANT**: On Windows, use PowerShell or CMD, NOT Git Bash (path conversion issues).

```powershell
# Start infrastructure (Kafka + Zookeeper + Producer stay running)
docker-compose up -d --build

# Verify all services are running
docker-compose ps

# Execute Spark consumer manually (from PowerShell/CMD)
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# Stop consumer: Ctrl+C

# Edit code in spark-consumer/consumer.py (changes are immediate via volume mount)

# Re-run consumer to test changes
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# Stop all services
docker-compose down
```

### Alternative: Interactive Container Session

```bash
# Enter the Spark container
docker exec -it spark-dev bash

# Inside container, run with custom configs
spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  /app/consumer.py
```

### Databricks Remote Testing

Use the automated Bore tunnel setup:

```powershell
# Start environment with external Bore tunnel
.\start-kafka-env.ps1

# Script automatically:
# 1. Stops old processes
# 2. Starts Bore tunnel to expose port 19092
# 3. Captures public URL (e.g., bore.pub:53049)
# 4. Updates docker-compose.yml with the URL
# 5. Starts all Docker services
# 6. Displays connection info

# The Bore URL is saved to: bore_url.txt
# Use this URL in Databricks: kafka.bootstrap.servers = "bore.pub:xxxxx"
```

**Note**: Bore URL changes every time you restart the script.

## Key Commands

### Kafka Operations

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic ordem

# Consume messages manually (max 5 from beginning)
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ordem \
  --from-beginning \
  --max-messages 5
```

### Monitoring

```bash
# View all logs
docker-compose logs -f

# View producer logs only
docker-compose logs -f python-producer

# Access Spark UI (when consumer is running)
# http://localhost:4040
```

### Maintenance

```bash
# Clear Spark checkpoint (if needed for clean restart)
docker exec -it spark-dev rm -rf /tmp/checkpoint

# Rebuild specific service after Dockerfile/requirements.txt changes
docker-compose up -d --build spark-dev

# Rebuild producer after code changes
docker-compose up -d --build python-producer
```

## Code Structure

### Producer (`producer/producer.py`)
- Uses `kafka-python` and `Faker` for data generation
- Generates Brazilian Portuguese fake data (Faker 'pt_BR')
- Creates correlated messages across topics using shared IDs
- Includes retry logic with `wait_for_kafka()` helper

### Consumer (`spark-consumer/consumer.py`)
- PySpark 3.5.0 with Structured Streaming
- Defines explicit schemas for all three topics (ordem, eventos, gps)
- Each topic is consumed by separate `consume_topic()` calls returning query objects
- All queries run simultaneously with `awaitTermination()`
- Uses `from_json()` to parse Kafka value field into structured columns

## Windows-Specific Considerations

1. **Terminal Choice**: Git Bash incorrectly converts `/app/consumer.py` paths to Windows paths like `C:/Program Files/Git/app/consumer.py`. Always use PowerShell or CMD for docker exec commands.

2. **Path Workarounds for Git Bash** (if necessary):
   - Prefix commands with `MSYS_NO_PATHCONV=1`
   - Or use `//app` instead of `/app`

3. **PowerShell Aliases** (optional, add to `$PROFILE`):
   ```powershell
   function Run-SparkJob { docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py }
   function Spark-Shell { docker exec -it spark-dev bash }
   Set-Alias spark-run Run-SparkJob
   Set-Alias spark-shell Spark-Shell
   ```

## Databricks Testing

After running `start-kafka-env.ps1`:

```python
# Test connectivity first
%sh
nc -zv bore.pub 53049  # Replace with your URL from bore_url.txt

# Read from Kafka in Databricks
kafka_bootstrap_servers = "bore.pub:53049"  # Use actual URL from bore_url.txt

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "ordem") \
  .option("startingOffsets", "earliest") \
  .load()
```

See `TESTE-DATABRICKS.md` for complete Databricks testing examples.

## File Changes and Hot Reload

- **Consumer code** (`spark-consumer/*.py`): Changes are immediate (volume mounted), just re-run spark-submit
- **Producer code** (`producer/*.py`): Changes are immediate, but restart container if needed: `docker-compose restart python-producer`
- **Requirements files**: Rebuild the respective container: `docker-compose up -d --build [service-name]`
- **docker-compose.yml**: Requires `docker-compose up -d --build`

## Important Notes

- The Spark container does NOT auto-execute on startup (`command: tail -f /dev/null`). This is intentional for development flexibility.
- The producer continuously generates data every 2 seconds - check logs with `docker-compose logs -f python-producer`
- Bore tunnel URL is temporary and changes on each script restart - always check `bore_url.txt` for the current URL
- Kafka has health checks configured - wait for "healthy" status before running consumers
