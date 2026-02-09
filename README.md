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

---

## Daftar Isi

1. [Prerequisites](#1-prerequisites)
2. [Konfigurasi Environment](#2-konfigurasi-environment)
3. [Mode Deployment](#3-mode-deployment)
4. [Full Container Mode - Architecture A (PySpark)](#4-full-container-mode---architecture-a-pyspark)
5. [Full Container Mode - Architecture B (PyFlink)](#5-full-container-mode---architecture-b-pyflink)
6. [Hybrid Development Mode](#6-hybrid-development-mode)
7. [Menjalankan Generator Lokal](#7-menjalankan-generator-lokal)
8. [Verifikasi Instalasi](#8-verifikasi-instalasi)
9. [Testing](#9-testing)
10. [Monitoring & Debugging](#10-monitoring--debugging)
11. [Menghentikan Service](#11-menghentikan-service)
12. [Troubleshooting](#12-troubleshooting)
13. [Project Structure](#13-project-structure)
14. [Implementation Status](#14-implementation-status)

---

## 1. Prerequisites

### Software

| Software | Versi Minimum | Cara Cek |
|----------|--------------|----------|
| Docker | 24.0+ | `docker --version` |
| Docker Compose | 2.20+ (V2) | `docker compose version` |
| Python | 3.10+ | `python3 --version` |
| Git | 2.30+ | `git --version` |
| curl | any | `curl --version` |

### Hardware Minimum

| Resource | Architecture A (PySpark) | Architecture B (PyFlink) |
|----------|--------------------------|--------------------------|
| RAM | 8 GB | 6 GB |
| CPU | 4 cores | 3 cores |
| Disk | 10 GB (Docker images) | 10 GB |

### Port yang Digunakan

| Port | Service | Keterangan |
|------|---------|------------|
| 55555 | Solace SMF | Solace Message Format protocol |
| 8080 | Solace SEMP | Management UI |
| 8008 | Solace Web Transport | Web messaging |
| 9000 | Solace REST | REST messaging |
| 5672 | Solace AMQP | AMQP protocol |
| 5432 | PostgreSQL | Database ODS |
| 8000 | REST API | FastAPI mock server |
| 4040 | Spark UI | PySpark Web UI (Architecture A) |
| 8081 | Flink UI | PyFlink Web UI (Architecture B) |

---

## 2. Konfigurasi Environment

### Buat File `.env`

```bash
cp .env.example .env
```

Edit jika perlu mengubah default:

```bash
# .env
SOLACE_HOST=localhost
SOLACE_PORT=55555
SOLACE_VPN=default
SOLACE_USERNAME=admin
SOLACE_PASSWORD=admin

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ods
POSTGRES_USER=cdc_user
POSTGRES_PASSWORD=cdc_pass

REST_API_URL=http://localhost:8000

EVENTS_PER_MINUTE=5000
LOG_LEVEL=INFO
```

### Konfigurasi YAML

File konfigurasi YAML ada di folder `configs/`:

| File | Keterangan |
|------|------------|
| `generator.yaml` | Konfigurasi CDC event generator |
| `solace.yaml` | Konfigurasi Solace PubSub+ |
| `spark.yaml` | Konfigurasi PySpark consumer |
| `flink.yaml` | Konfigurasi PyFlink consumer |
| `postgres.yaml` | Konfigurasi PostgreSQL ODS |
| `api.yaml` | Konfigurasi REST API |

---

## 3. Mode Deployment

### Full Container Mode
Semua komponen berjalan di Docker container. Cocok untuk demo dan testing end-to-end.

```
[Generator] → [Solace] → [Consumer] → [PostgreSQL + REST API]
     ↑ container   ↑ container  ↑ container    ↑ container
```

### Hybrid Development Mode
Hanya infrastructure di Docker. Generator dan consumer dijalankan lokal. Cocok untuk development dan debugging.

```
[Generator (lokal)] → [Solace (Docker)] → [Consumer (lokal)] → [PG (Docker) + REST API (Docker)]
```

---

## 4. Full Container Mode - Architecture A (PySpark)

PySpark Structured Streaming dengan dual sink: PostgreSQL ODS + REST API.

### Jalankan dengan Script

```bash
chmod +x scripts/start-spark-arch.sh
./scripts/start-spark-arch.sh
```

### Atau Jalankan Manual

```bash
cd docker
docker compose -f docker-compose.spark.yml --profile full up -d --build
docker compose -f docker-compose.spark.yml ps
```

### Service yang Berjalan

| Container | Image | Keterangan |
|-----------|-------|------------|
| solace | solace/solace-pubsub-standard | Message broker |
| postgres-ods | postgres:15 | PostgreSQL ODS database |
| rest-api | custom build | FastAPI mock server |
| cdc-generator | custom build | Debezium CDC simulator |
| spark-consumer | custom build | PySpark consumer |

### Verifikasi

```bash
docker compose -f docker/docker-compose.spark.yml ps
curl -s http://localhost:8080/health-check/guaranteed-active
docker exec postgres-ods pg_isready -U cdc_user -d ods
curl -s http://localhost:8000/health
docker logs -f spark-consumer
```

---

## 5. Full Container Mode - Architecture B (PyFlink)

PyFlink DataStream dengan dual sink: PostgreSQL ODS + REST API.

### Jalankan dengan Script

```bash
chmod +x scripts/start-flink-arch.sh
./scripts/start-flink-arch.sh
```

### Atau Jalankan Manual

```bash
cd docker
docker compose -f docker-compose.flink.yml --profile full up -d --build
docker compose -f docker-compose.flink.yml ps
```

### Service yang Berjalan

| Container | Image | Keterangan |
|-----------|-------|------------|
| solace | solace/solace-pubsub-standard | Message broker |
| postgres-ods | postgres:15 | PostgreSQL ODS database |
| rest-api | custom build | FastAPI mock server |
| cdc-generator | custom build | Debezium CDC simulator |
| flink-consumer | custom build | PyFlink consumer |

### Verifikasi

```bash
docker compose -f docker/docker-compose.flink.yml ps
curl -s http://localhost:8080/health-check/guaranteed-active
docker exec postgres-ods pg_isready -U cdc_user -d ods
curl -s http://localhost:8000/health
docker logs -f flink-consumer
```

---

## 6. Hybrid Development Mode

Hanya menjalankan infrastructure di Docker (Solace + PostgreSQL + REST API).

### Jalankan Infrastructure

```bash
chmod +x scripts/start-infra-only.sh

# Untuk Architecture A
./scripts/start-infra-only.sh spark

# Untuk Architecture B
./scripts/start-infra-only.sh flink
```

### Tunggu Sampai Service Sehat

```bash
# Tunggu Solace (biasanya 30-60 detik)
until curl -s http://localhost:8080/health-check/guaranteed-active > /dev/null 2>&1; do
    sleep 2; echo -n "."
done
echo " Solace READY"

# Tunggu PostgreSQL
until docker exec postgres-ods pg_isready -U cdc_user -d ods > /dev/null 2>&1; do
    sleep 2; echo -n "."
done
echo " PostgreSQL READY"
```

---

## 7. Menjalankan Generator Lokal

Generator mensimulasikan Debezium CDC events dan mempublish ke Solace.

### Setup Virtual Environment

```bash
cd src/generator
python3 -m venv venv
source venv/bin/activate    # Linux/macOS
pip install -r ../../requirements/generator.txt
```

### Jalankan Generator

```bash
cd src/generator
source venv/bin/activate

export SOLACE_HOST=localhost
export SOLACE_PORT=55555
export SOLACE_VPN=default
export SOLACE_USERNAME=admin
export SOLACE_PASSWORD=admin
export EVENTS_PER_MINUTE=5000

python main.py
```

Generator memproduksi CDC events ke topik Solace:
- `cdc/oracle/ECOM/ORDERS/INSERT`
- `cdc/oracle/ECOM/CUSTOMERS/INSERT`
- `cdc/oracle/ECOM/ORDER_ITEMS/INSERT`
- `cdc/oracle/ECOM/PRODUCTS/INSERT`
- `cdc/oracle/ECOM/AUDIT_LOG/INSERT`

### Verifikasi Events

Buka Solace Manager: http://localhost:8080 (admin/admin)
Navigasi ke: **Message VPN** → **default** → **Queues/Topics**

---

## 8. Verifikasi Instalasi

### Cek PostgreSQL Tables

```bash
docker exec -it postgres-ods psql -U cdc_user -d ods

# Di dalam psql:
\dt                               -- List semua tables
SELECT COUNT(*) FROM cdc_orders;  -- Cek data orders
SELECT COUNT(*) FROM cdc_customers;
SELECT * FROM v_dlq_summary;      -- Cek DLQ summary
\q                                -- Keluar
```

### Cek REST API

```bash
curl -s http://localhost:8000/health | python3 -m json.tool
curl -s http://localhost:8000/metrics | python3 -m json.tool
curl -s "http://localhost:8000/api/v1/events?limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/api/v1/enriched-orders?limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/api/v1/notifications" | python3 -m json.tool
```

### Web UIs

| URL | Keterangan |
|-----|------------|
| http://localhost:8080 | Solace Management UI (admin/admin) |
| http://localhost:8000/docs | REST API Swagger Documentation |
| http://localhost:4040 | Spark Web UI (Architecture A) |
| http://localhost:8081 | Flink Web UI (Architecture B) |

---

## 9. Testing

Project ini memiliki **220 unit tests** yang mencakup semua komponen.

### Setup

```bash
cd src/generator
source venv/bin/activate
pip install pytest httpx
cd ../..
```

### Jalankan Semua Tests

```bash
python -m pytest tests/ -v
```

### Tests per Komponen

```bash
python -m pytest tests/common/ -v           # State store (21 tests)
python -m pytest tests/spark_consumer/ -v   # PySpark (62 tests)
python -m pytest tests/flink_consumer/ -v   # PyFlink (96 tests)
python -m pytest tests/rest_api/ -v         # REST API (28 tests)
```

### Test Spesifik

```bash
python -m pytest tests/spark_consumer/test_dlq_handler.py -v
python -m pytest tests/ -k "circuit_breaker" -v
```

| Test Suite | Tests | Coverage |
|------------|-------|----------|
| Common (state_store) | 21 | TTL, upsert, dedup, memory efficiency |
| PySpark Consumer | 62 | Config, DLQ, XML, PG writer, REST sink |
| PyFlink Consumer | 96 | CDC parser, enrichment, dedup, sinks |
| REST API | 28 | Storage, all endpoints |
| **Total** | **220** | **All passing** |

---

## 10. Monitoring & Debugging

### Lihat Logs

```bash
# Logs semua service
docker compose -f docker/docker-compose.spark.yml logs -f

# Logs service tertentu
docker logs -f spark-consumer
docker logs -f flink-consumer
docker logs -f cdc-generator
docker logs -f solace
docker logs -f postgres-ods
docker logs -f rest-api
```

### Masuk ke Container

```bash
docker exec -it postgres-ods psql -U cdc_user -d ods
docker exec -it spark-consumer /bin/bash
docker exec -it flink-consumer /bin/bash
```

### Monitoring Database

```sql
-- Connect: docker exec -it postgres-ods psql -U cdc_user -d ods

-- Jumlah record per tabel
SELECT 'cdc_orders' as tabel, COUNT(*) as jumlah FROM cdc_orders
UNION ALL SELECT 'cdc_customers', COUNT(*) FROM cdc_customers
UNION ALL SELECT 'cdc_products', COUNT(*) FROM cdc_products
UNION ALL SELECT 'cdc_order_items', COUNT(*) FROM cdc_order_items
UNION ALL SELECT 'cdc_orders_enriched', COUNT(*) FROM cdc_orders_enriched
UNION ALL SELECT 'cdc_dlq', COUNT(*) FROM cdc_dlq;

-- Cek DLQ errors
SELECT error_type, COUNT(*) FROM cdc_dlq GROUP BY error_type;

-- Latest enriched orders
SELECT * FROM cdc_orders_enriched ORDER BY processed_at DESC LIMIT 5;
```

---

## 11. Menghentikan Service

### Menggunakan Script

```bash
chmod +x scripts/stop-all.sh
./scripts/stop-all.sh              # Stop (data tetap ada)
./scripts/stop-all.sh --clean      # Stop + hapus semua data
```

### Manual

```bash
docker compose -f docker/docker-compose.spark.yml --profile full down
docker compose -f docker/docker-compose.flink.yml --profile full down

# Hapus volumes (HATI-HATI: menghapus semua data!)
docker volume rm solace-data postgres-data spark-checkpoints flink-checkpoints 2>/dev/null
```

---

## 12. Troubleshooting

### Solace tidak bisa start

**Gejala:** Container solace terus restart atau tidak healthy.

```bash
docker info | grep "Total Memory"   # Cek memory
lsof -i :55555                       # Cek port conflict
docker logs solace                   # Cek logs
docker volume rm solace-data         # Reset data
```

### PostgreSQL connection refused

```bash
docker exec postgres-ods pg_isready -U cdc_user -d ods
docker logs postgres-ods | head -50

# Reset database
docker compose -f docker/docker-compose.spark.yml down
docker volume rm postgres-data
docker compose -f docker/docker-compose.spark.yml up -d postgres
```

### Port sudah digunakan

```bash
lsof -i :<port>
ss -tlnp | grep <port>
kill -9 <PID>
```

### Tests gagal: ModuleNotFoundError

```bash
# Pastikan di root project
cd /path/to/solace_consumer_service
source src/generator/venv/bin/activate
python -m pytest tests/ -v          # Gunakan python -m pytest, bukan pytest
```

### Docker build gagal

```bash
docker compose -f docker/docker-compose.spark.yml build --no-cache
df -h && docker system df
docker system prune -f
```

### Memory tidak cukup (OOM)

1. Tambah memory Docker Desktop (Settings → Resources → Memory)
2. Kurangi `EVENTS_PER_MINUTE` di `.env`
3. Kurangi memory limit di docker-compose:
   ```yaml
   deploy:
     resources:
       limits:
         memory: 1G
   ```

---

## 13. Project Structure

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
├── docs/
│   └── BLUEPRINT_CDC_ARCHITECTURE.md  # Full technical specification
├── pytest.ini
├── .env.example
└── README.md
```

---

## 14. Implementation Status

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

## Documentation

Untuk detail teknis arsitektur (desain, spesifikasi, failure scenarios, schema evolution, XML extraction, benchmarks):
- [Blueprint CDC Architecture](docs/BLUEPRINT_CDC_ARCHITECTURE.md)

---

## Quick Reference

```bash
# === SETUP ===
cp .env.example .env

# === ARCHITECTURE A (PySpark) ===
./scripts/start-spark-arch.sh                           # Full mode
./scripts/start-infra-only.sh spark                     # Hybrid mode

# === ARCHITECTURE B (PyFlink) ===
./scripts/start-flink-arch.sh                           # Full mode
./scripts/start-infra-only.sh flink                     # Hybrid mode

# === GENERATOR LOKAL ===
cd src/generator && source venv/bin/activate
SOLACE_HOST=localhost SOLACE_PORT=55555 python main.py

# === TESTS ===
source src/generator/venv/bin/activate
python -m pytest tests/ -v                              # 220 tests

# === MONITORING ===
curl http://localhost:8080/health-check/guaranteed-active  # Solace
curl http://localhost:8000/health                          # REST API
docker exec postgres-ods pg_isready -U cdc_user -d ods    # PostgreSQL

# === STOP ===
./scripts/stop-all.sh                                   # Stop all
./scripts/stop-all.sh --clean                           # Stop + hapus data
```

---

*Version: 3.0 | Last Updated: 2025-02-10*
