# CDC Architecture POC - Executive Summary

> **Versi ringkas dari [BLUEPRINT_CDC_ARCHITECTURE.md](./BLUEPRINT_CDC_ARCHITECTURE.md)**
> Untuk detail teknis lengkap, lihat dokumen utama.

---

## 1. Tujuan POC

Memvalidasi arsitektur CDC yang dapat:
- Memproses **5,000 events/menit** dari Oracle (simulasi Debezium)
- Melakukan **3-way join** antar topic
- Handle **dynamic schema changes**
- Extract **XML columns** menjadi table baru
- Resilient terhadap **common failures**

---

## 2. Architecture Overview

### 2.1 High-Level Flow

```
                                    ┌─────────────────────────────────────┐
                                    │         DOCKER ENVIRONMENT          │
                                    │                                     │
┌──────────────┐                    │  ┌─────────┐      ┌─────────────┐  │
│    Oracle    │  (simulated by)    │  │         │      │  PySpark    │──┼──▶ PostgreSQL (ODS)
│   Database   │ ─────────────────▶ │  │ Solace  │ ───▶ │    OR       │  │
│  (20 tables) │                    │  │ PubSub+ │      │  PyFlink    │──┼──▶ REST API
└──────────────┘                    │  │         │      │             │  │
        │                           │  └─────────┘      └─────────────┘  │
        │                           │                                     │
        ▼                           └─────────────────────────────────────┘
┌──────────────┐
│   Debezium   │
│  Simulator   │ ◀── Python script yang generate CDC events
└──────────────┘
```

### 2.2 Dua Arsitektur Terpisah

| | Architecture A | Architecture B |
|:--|:---------------|:---------------|
| **Consumer** | PySpark | PyFlink |
| **Output** | PostgreSQL (ODS) | REST API |
| **Use Case** | Batch aggregation, complex joins | Real-time alerting |
| **Latency** | Seconds to minutes | < 5 seconds |

---

## 3. Key Requirements

### 3.1 Performance

| Metric | Target |
|:-------|:-------|
| Throughput | 5,000 events/min (~83/sec) |
| Peak Load | 10,000 events/min |
| Latency (p99) | < 10 seconds |
| Error Rate | < 0.1% |

### 3.2 Data Simulation

```
┌─────────────────────────────────────────────────────────┐
│  5 Tables (simulasi 20 tables)                          │
├─────────────────────────────────────────────────────────┤
│  ORDERS        │ 2,000/min │ 40% │ High volume         │
│  ORDER_ITEMS   │ 2,000/min │ 40% │ High volume         │
│  CUSTOMERS     │   500/min │ 10% │ Dimension (join)    │
│  PRODUCTS      │   400/min │  8% │ Dimension (join)    │
│  AUDIT_LOG     │   100/min │  2% │ Reference           │
├─────────────────────────────────────────────────────────┤
│  TOTAL         │ 5,000/min │100% │                     │
└─────────────────────────────────────────────────────────┘
```

### 3.3 Transformation: 3-Way Join

```
ORDERS ──┐
         ├──▶ JOIN ──▶ Enriched Data ──▶ ODS / API
CUSTOMERS┤
         │
PRODUCTS─┘
```

---

## 4. Special Features (WAJIB)

### 4.1 Dynamic Schema Evolution

| Event | Behavior |
|:------|:---------|
| New column added | Auto-detect, include in processing |
| Column removed | Treat as NULL, no error |
| Type changed | Safe coercion |

**Storage:** PostgreSQL dengan JSONB untuk flexibility

### 4.2 XML Column Extraction

```
1 ORDER with XML ──▶ XML Extractor ──▶ N ORDER_ITEMS rows
                                   ──▶ 1 ORDER row (cleaned)
```

| Scenario | Handling |
|:---------|:---------|
| Valid XML | Extract to new table |
| Malformed XML | Send to DLQ |
| Empty/NULL XML | Skip, no error |

### 4.3 Failure Handling

| Category | Scenario | Expected |
|:---------|:---------|:---------|
| Consumer | Crash/restart | Resume from checkpoint |
| Broker | Solace restart | Auto-reconnect |
| Target | DB/API down | Circuit breaker + retry |
| Data | Invalid message | DLQ + continue |

---

## 5. Test Scenarios Summary

### 5.1 Mandatory Tests

| Category | Count | IDs |
|:---------|:------|:----|
| Failure Scenarios | 10 | F1-F10 |
| Schema Evolution | 6 | S1-S6 |
| XML Extraction | 8 | X1-X8 |
| **Total** | **24** | |

### 5.2 Benchmark Scenarios

| Scenario | Load | Duration |
|:---------|:-----|:---------|
| Baseline | 1K/min | 10 min |
| Target | 5K/min | 30 min |
| Peak | 10K/min | 15 min |
| Sustained | 5K/min | 60 min |

---

## 6. Technology Stack

| Layer | Technology |
|:------|:-----------|
| Message Broker | Solace PubSub+ (Docker) |
| Stream Processing | PySpark 3.5 / PyFlink 1.18 |
| Database | PostgreSQL 15 |
| API Mock | FastAPI |
| Container | Docker Compose |

---

## 7. Project Structure (Simplified)

```
solace_consumer_service/
├── docker/
│   ├── docker-compose.spark.yml    # Architecture A
│   └── docker-compose.flink.yml    # Architecture B
├── src/
│   ├── generator/                  # Debezium simulator
│   ├── spark_consumer/             # Architecture A
│   ├── flink_consumer/             # Architecture B
│   └── rest_api/                   # Mock API target
├── configs/
└── docs/
    ├── BLUEPRINT_SUMMARY.md        # This file
    └── BLUEPRINT_CDC_ARCHITECTURE.md  # Full details
```

---

## 8. Implementation Phases

```
Phase 1: Infrastructure     ──▶ Docker, Solace, PostgreSQL, API
          │
Phase 2: Data Generator     ──▶ Debezium simulator, 5 tables
          │
Phase 3: PySpark Consumer   ──▶ 3-way join, PostgreSQL sink
          │
Phase 4: PyFlink Consumer   ──▶ Real-time processing, REST sink
          │
Phase 5: Testing            ──▶ 24 mandatory test scenarios
```

---

## 9. Quick Commands

```bash
# Start Architecture A (PySpark → PostgreSQL)
docker compose -f docker/docker-compose.spark.yml up -d

# Start Architecture B (PyFlink → REST API)
docker compose -f docker/docker-compose.flink.yml up -d

# View logs
docker compose logs -f spark-consumer
docker compose logs -f flink-consumer
```

**Monitoring URLs:**
- Solace Manager: http://localhost:8080
- Spark UI: http://localhost:4040
- Flink UI: http://localhost:8081
- REST API Docs: http://localhost:8000/docs

---

## 10. Checklist Summary

### WAJIB
- [ ] CDC event generation (5 tables, 5K/min)
- [ ] Solace pub/sub with dynamic topics
- [ ] PySpark 3-way join → PostgreSQL
- [ ] PyFlink processing → REST API
- [ ] Checkpoint/recovery
- [ ] DLQ implementation
- [ ] Dynamic schema handling
- [ ] XML extraction
- [ ] 24 test scenarios (F1-F10, S1-S6, X1-X8)

### OPSIONAL
- [ ] Distributed Spark/Flink cluster
- [ ] 20K+ events/min stress test
- [ ] Prometheus/Grafana monitoring

---

## 11. Configuration Defaults

| Setting | Value |
|:--------|:------|
| Target latency | < 5s |
| Solace mode | Persistent Queue |
| State backend | In-memory |
| DLQ retention | 7 days |
| Checkpoint interval | 10 seconds |

---

## 12. Resource Requirements

### POC Environment (16GB RAM)

| Component | CPU | RAM | Storage |
|:----------|:----|:----|:--------|
| Solace PubSub+ | 1 core | 2GB | 5GB |
| PostgreSQL | 0.5 core | 1GB | 10GB |
| Spark Consumer | 2 cores | 4GB | - |
| Flink Consumer | 1 core | 2GB | - |
| Generator | 0.5 core | 512MB | - |
| REST API | 0.5 core | 512MB | - |

**Total per Architecture:**
- Architecture A (Spark): ~10GB RAM
- Architecture B (Flink): ~8GB RAM

> ⚠️ **Recommendation:** Jalankan Architecture A dan B secara terpisah, tidak bersamaan.

### Production Environment

| Component | CPU | RAM | Instances |
|:----------|:----|:----|:----------|
| Solace (HA) | 4 cores | 8GB | 3 nodes |
| PostgreSQL (HA) | 4 cores | 16GB | 2 nodes |
| Spark Cluster | 8 cores | 16GB | 3 workers |
| Flink Cluster | 4 cores | 8GB | 3 TMs |

---

## 13. Document References

| Document | Description |
|:---------|:------------|
| [BLUEPRINT_CDC_ARCHITECTURE.md](./BLUEPRINT_CDC_ARCHITECTURE.md) | Full technical specification |
| [BLUEPRINT_SUMMARY.md](./BLUEPRINT_SUMMARY.md) | This executive summary |

---

*Version: 1.7 | Last Updated: 2025-02-04 | Status: DRAFT*
