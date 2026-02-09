# CDC Architecture POC

> Proof of Concept untuk arsitektur Change Data Capture (CDC) dengan Solace PubSub+

## Overview

POC ini memvalidasi arsitektur CDC yang dapat:
- Memproses **5,000 events/menit** dari Oracle (simulasi Debezium)
- Melakukan **3-way join** antar topic (ORDERS + CUSTOMERS + ORDER_ITEMS)
- Handle **dynamic schema changes** dengan JSONB fallback
- Extract **XML columns** (SHIPPING_INFO) menjadi table baru
- Resilient terhadap **common failures** (DLQ, circuit breaker, retry)
- **Dual sink**: PostgreSQL ODS + REST API push

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER ENVIRONMENT                                  │
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌──────────────────┐       │
│  │  Data Generator │────▶│     Solace      │────▶│  PySpark/Flink   │       │
│  │  (Debezium Sim) │     │    PubSub+      │     │    Consumer      │       │
│  └─────────────────┘     └─────────────────┘     └────────┬─────────┘       │
│                                                           │                  │
│                                              ┌────────────┼────────────┐    │
│                                              ▼                         ▼    │
│                                    ┌─────────────────┐   ┌──────────────┐  │
│                                    │   PostgreSQL    │   │   REST API   │  │
│                                    │     (ODS)       │   │   (FastAPI)  │  │
│                                    └─────────────────┘   └──────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Two Independent Architectures

| | Architecture A | Architecture B |
|:--|:---------------|:---------------|
| **Consumer** | PySpark | PyFlink |
| **Primary Sink** | PostgreSQL (ODS) | PostgreSQL (ODS) + REST API |
| **REST API Sink** | Configurable (off by default) | Enabled by default |
| **Use Case** | Batch aggregation, complex joins | Real-time streaming + alerting |
| **State Store** | Keyed dict + broadcast variables | Keyed dict + TTL |
| **Crash Recovery** | Bootstrap from PostgreSQL | Bootstrap from PostgreSQL |

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.11+ (for local development)
- 16GB RAM (minimum)
- 50GB Storage

### Option 1: Full Container Mode (Recommended for Demo)

```bash
# Architecture A (PySpark + PostgreSQL)
./scripts/start-spark-arch.sh

# OR Architecture B (PyFlink + PostgreSQL + REST API)
./scripts/start-flink-arch.sh
```

### Option 2: Hybrid Mode (Development)

```bash
# Start infrastructure only
./scripts/start-infra-only.sh spark   # or: flink

# Run generator locally (in another terminal)
cd src/generator
source venv/bin/activate
SOLACE_HOST=localhost SOLACE_PORT=55555 python main.py
```

### Stop Services

```bash
./scripts/stop-all.sh

# With volume cleanup
./scripts/stop-all.sh --clean
```

## Monitoring URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Solace Manager | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:4040 | - |
| Flink UI | http://localhost:8081 | - |
| REST API Docs | http://localhost:8000/docs | - |
| PostgreSQL | localhost:5432 | cdc_user / cdc_pass |

## Project Structure

```
solace_consumer_service/
├── docker/
│   ├── docker-compose.spark.yml       # Architecture A
│   ├── docker-compose.flink.yml       # Architecture B
│   └── dockerfiles/
│       ├── Dockerfile.generator
│       ├── Dockerfile.spark
│       ├── Dockerfile.flink
│       └── Dockerfile.api
├── src/
│   ├── generator/                     # Debezium CDC simulator
│   │   ├── main.py                    # Entry point
│   │   ├── config.py                  # Generator config
│   │   ├── publisher.py               # Solace publisher
│   │   ├── debezium_simulator.py      # Debezium format builder
│   │   └── models/                    # Table generators (5 tables)
│   ├── spark_consumer/                # Architecture A (PySpark)
│   │   ├── main.py                    # SparkCDCConsumer orchestrator
│   │   ├── config.py                  # Spark config
│   │   ├── solace_source.py           # Solace connector
│   │   ├── state_store.py             # Keyed state store (shared)
│   │   ├── dlq_handler.py             # Dead Letter Queue
│   │   ├── transformations/           # CDC processing pipeline
│   │   │   ├── cdc_processor.py       # Parse + dedup
│   │   │   ├── schema_handler.py      # Dynamic schema
│   │   │   ├── stream_joiner.py       # 3-way join
│   │   │   └── xml_extractor.py       # XML column extraction
│   │   └── writers/
│   │       ├── postgres_writer.py     # PostgreSQL ODS sink
│   │       └── rest_sink.py           # REST API sink (optional)
│   ├── flink_consumer/                # Architecture B (PyFlink)
│   │   ├── main.py                    # FlinkCDCConsumer orchestrator
│   │   ├── config.py                  # Flink config
│   │   ├── solace_source.py           # Solace connector
│   │   ├── dlq_handler.py             # Dead Letter Queue
│   │   ├── processors/
│   │   │   ├── cdc_processor.py       # Parse + dedup
│   │   │   ├── order_enrichment.py    # 3-way join + aggregation
│   │   │   └── xml_extractor.py       # XML column extraction
│   │   └── sinks/
│   │       ├── postgres_sink.py       # PostgreSQL ODS sink
│   │       └── rest_sink.py           # REST API sink
│   └── rest_api/                      # FastAPI mock server
│       ├── main.py                    # FastAPI app
│       ├── storage.py                 # In-memory event store
│       ├── models/events.py           # Pydantic models
│       └── routes/events.py           # API endpoints
├── configs/                           # YAML configurations
│   ├── spark.yaml                     # PySpark consumer config
│   ├── flink.yaml                     # PyFlink consumer config
│   ├── solace.yaml                    # Solace broker config
│   ├── generator.yaml                 # Generator config
│   ├── postgres.yaml                  # PostgreSQL ODS config
│   └── api.yaml                       # REST API config
├── scripts/                           # Helper scripts
│   ├── start-spark-arch.sh            # Launch Architecture A
│   ├── start-flink-arch.sh            # Launch Architecture B
│   ├── start-infra-only.sh            # Infrastructure only (dev)
│   └── stop-all.sh                    # Stop all services
├── sql/
│   └── init.sql                       # PostgreSQL schema
├── tests/                             # Test suite (220 tests)
│   ├── conftest.py
│   ├── common/                        # Shared state store tests
│   ├── spark_consumer/                # PySpark consumer tests
│   ├── flink_consumer/                # PyFlink consumer tests
│   └── rest_api/                      # REST API tests
├── requirements/                      # Python dependencies
│   ├── base.txt
│   ├── spark.txt
│   ├── flink.txt
│   ├── generator.txt
│   └── api.txt
├── docs/
│   ├── BLUEPRINT_CDC_ARCHITECTURE.md  # Full technical spec
│   └── INSTALLATION_GUIDE.md          # Step-by-step setup guide
├── .env.example
├── pytest.ini
└── README.md
```

## Configuration

Copy `.env.example` to `.env` and modify as needed:

```bash
cp .env.example .env
```

Key configurations:
- `EVENTS_PER_MINUTE`: Event generation rate (default: 5000)
- `SOLACE_HOST`: Solace broker host
- `POSTGRES_*`: PostgreSQL connection settings
- `REST_API_URL`: REST API target URL

## Testing

```bash
# Activate virtual environment
source src/generator/venv/bin/activate

# Run all 220 tests
python -m pytest tests/ -v

# Run specific test suite
python -m pytest tests/common/ -v           # State store tests
python -m pytest tests/spark_consumer/ -v   # PySpark tests
python -m pytest tests/flink_consumer/ -v   # PyFlink tests
python -m pytest tests/rest_api/ -v         # REST API tests
```

| Test Suite | Tests | Coverage |
|------------|-------|----------|
| Common (state_store) | 21 | TTL, upsert, dedup, memory efficiency |
| PySpark Consumer | 62 | Config, DLQ, XML, PG writer, REST sink |
| PyFlink Consumer | 96 | CDC parser, enrichment, dedup, sinks |
| REST API | 28 | Storage, all endpoints |
| **Total** | **220** | **All passing** |

## Resource Requirements

### POC Environment (16GB RAM)

| Component | RAM |
|-----------|-----|
| Solace PubSub+ | 2GB |
| PostgreSQL | 1GB |
| Spark Consumer | 4GB |
| Flink Consumer | 2GB |
| REST API | 512MB |
| Generator | 512MB |

**Note:** Run Architecture A and B separately, not simultaneously.

## Documentation

- [Blueprint CDC Architecture](docs/BLUEPRINT_CDC_ARCHITECTURE.md) - Full technical specification
- [Installation Guide](docs/INSTALLATION_GUIDE.md) - Step-by-step setup manual

## Implementation Status

### Phase 1: Infrastructure ✅
- [x] Docker Compose configurations (spark + flink)
- [x] PostgreSQL schema (18 tables + views)
- [x] Dockerfiles (generator, spark, flink, api)
- [x] YAML configuration files (6 configs)
- [x] Helper scripts (start/stop)

### Phase 2: Data Generator ✅
- [x] Debezium format models (5 tables)
- [x] Solace publisher
- [x] Table generators with distribution weights
- [x] XML generation (30% of orders)

### Phase 3: PySpark Consumer ✅
- [x] Solace source connector
- [x] CDC processor (parse, dedup, metadata)
- [x] 3-way stream join (broadcast + keyed state)
- [x] PostgreSQL ODS sink (circuit breaker + retry)
- [x] REST API sink (optional, configurable)
- [x] XML extraction (SHIPPING_INFO)
- [x] Dynamic schema handling (JSONB fallback)
- [x] DLQ handler
- [x] State bootstrap from PostgreSQL (crash recovery)

### Phase 4: PyFlink Consumer ✅
- [x] Solace source connector
- [x] CDC event parser + deduplicator
- [x] 3-way join enrichment (keyed state store)
- [x] Dual sink: PostgreSQL ODS + REST API
- [x] XML extraction
- [x] DLQ handler (dual: PG + REST notification)
- [x] State bootstrap from PostgreSQL (crash recovery)
- [x] High-value order alerting

### Phase 5: Testing ✅
- [x] State store tests (TTL, upsert, dedup, memory)
- [x] PySpark consumer tests (62 tests)
- [x] PyFlink consumer tests (96 tests)
- [x] REST API tests (28 tests)
- [x] 220 tests total, all passing

---

*Version: 2.0 | Last Updated: 2025-02-09*
