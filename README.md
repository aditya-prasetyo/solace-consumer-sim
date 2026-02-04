# CDC Architecture POC

> Proof of Concept untuk arsitektur Change Data Capture (CDC) dengan Solace PubSub+

## Overview

POC ini memvalidasi arsitektur CDC yang dapat:
- Memproses **5,000 events/menit** dari Oracle (simulasi Debezium)
- Melakukan **3-way join** antar topic
- Handle **dynamic schema changes**
- Extract **XML columns** menjadi table baru
- Resilient terhadap **common failures**

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER ENVIRONMENT                                 │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │  Data Generator │────▶│     Solace      │────▶│  PySpark/Flink  │       │
│  │  (Debezium Sim) │     │    PubSub+      │     │    Consumer     │       │
│  └─────────────────┘     └─────────────────┘     └────────┬────────┘       │
│                                                           │                 │
│                                                           ▼                 │
│                                    ┌──────────────────────────────────┐    │
│                                    │  PostgreSQL (ODS) / REST API     │    │
│                                    └──────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Two Independent Architectures

| | Architecture A | Architecture B |
|:--|:---------------|:---------------|
| **Consumer** | PySpark | PyFlink |
| **Output** | PostgreSQL (ODS) | REST API |
| **Use Case** | Batch aggregation, complex joins | Real-time alerting |
| **Latency** | Seconds to minutes | < 5 seconds |

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.11+ (for local development)
- 16GB RAM (minimum)
- 100GB Storage

### Option 1: Hybrid Mode (Development) - Recommended

```bash
# Start infrastructure only
./scripts/start-infra-only.sh spark

# Run generator locally (in another terminal)
./scripts/start-generator-local.sh
```

### Option 2: Full Container Mode (Integration/Demo)

```bash
# Architecture A (PySpark → PostgreSQL)
./scripts/start-spark-arch.sh

# OR Architecture B (PyFlink → REST API)
./scripts/start-flink-arch.sh
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
│   ├── docker-compose.spark.yml    # Architecture A
│   ├── docker-compose.flink.yml    # Architecture B
│   └── dockerfiles/
│       ├── Dockerfile.generator
│       ├── Dockerfile.spark
│       ├── Dockerfile.flink
│       └── Dockerfile.api
├── src/
│   ├── generator/                  # Debezium simulator
│   ├── spark_consumer/             # Architecture A consumer
│   ├── flink_consumer/             # Architecture B consumer
│   ├── rest_api/                   # Mock API target
│   └── common/                     # Shared utilities
├── configs/                        # YAML configurations
├── scripts/                        # Helper scripts
├── sql/                           # PostgreSQL init scripts
├── tests/                         # Test files
├── requirements/                  # Python dependencies
└── docs/
    ├── BLUEPRINT_CDC_ARCHITECTURE.md  # Full technical spec
    └── BLUEPRINT_SUMMARY.md           # Executive summary
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

## Test Scenarios

| Category | Count | IDs |
|----------|-------|-----|
| Failure Scenarios | 10 | F1-F10 |
| Schema Evolution | 6 | S1-S6 |
| XML Extraction | 8 | X1-X8 |
| **Total** | **24** | |

## Resource Requirements

### POC Environment (16GB RAM)

| Component | RAM |
|-----------|-----|
| Solace PubSub+ | 2GB |
| PostgreSQL | 1GB |
| Spark Consumer | 4GB |
| Flink Consumer | 2GB |
| Generator | 512MB |

**Note:** Run Architecture A and B separately, not simultaneously.

## Documentation

- [Blueprint CDC Architecture](docs/BLUEPRINT_CDC_ARCHITECTURE.md) - Full technical specification
- [Blueprint Summary](docs/BLUEPRINT_SUMMARY.md) - Executive summary

## Implementation Status

### Phase 1: Infrastructure ✅
- [x] Docker Compose configurations
- [x] PostgreSQL schema
- [x] Dockerfiles
- [x] Configuration files
- [x] Helper scripts

### Phase 2: Data Generator 🔄
- [ ] Debezium format models
- [ ] Solace publisher
- [ ] 5 table generators
- [ ] XML generation

### Phase 3: PySpark Consumer ⏳
- [ ] Solace source
- [ ] 3-way join
- [ ] PostgreSQL sink
- [ ] XML extraction

### Phase 4: PyFlink Consumer ⏳
- [ ] Solace source
- [ ] Stream processing
- [ ] REST API sink

### Phase 5: Testing ⏳
- [ ] Failure scenarios (F1-F10)
- [ ] Schema evolution (S1-S6)
- [ ] XML extraction (X1-X8)

---

*Version: 1.7 | Last Updated: 2025-02-04*
