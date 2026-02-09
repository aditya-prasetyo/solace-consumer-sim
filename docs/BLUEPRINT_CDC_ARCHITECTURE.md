# Blueprint: CDC Architecture POC dengan Solace

## 1. Overview

### 1.1 Tujuan
Membangun Proof of Concept (POC) arsitektur Change Data Capture (CDC) yang mensimulasikan:
- Data capture dari Oracle database (simulasi Debezium format)
- Message streaming via Solace PubSub+
- Consumer processing dengan **2 arsitektur terpisah**: PySpark dan PyFlink
- Output ke ODS (Operational Data Store) dan REST API
- **Seluruh komponen berjalan di atas Docker**

### 1.2 Arsitektur Overview

POC ini terdiri dari **2 arsitektur independen** yang dapat dijalankan terpisah:

| Arsitektur | Consumer | Output Target | Use Case |
|------------|----------|---------------|----------|
| **Architecture A** | PySpark | ODS (PostgreSQL) | Batch/Micro-batch processing |
| **Architecture B** | PyFlink | REST API | Real-time streaming |

---

## 2. Architecture A: PySpark Consumer

### 2.1 Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER ENVIRONMENT                                 │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │                 │     │                 │     │                 │       │
│  │  Data Generator │────▶│     Solace      │────▶│    PySpark      │       │
│  │  (Debezium Sim) │     │    PubSub+      │     │    Consumer     │       │
│  │                 │     │                 │     │                 │       │
│  │  [Container]    │     │  [Container]    │     │  [Container]    │       │
│  └─────────────────┘     └─────────────────┘     └────────┬────────┘       │
│                                                           │                 │
│                                                           ▼                 │
│                                                  ┌─────────────────┐       │
│                                                  │                 │       │
│                                                  │   PostgreSQL    │       │
│                                                  │     (ODS)       │       │
│                                                  │                 │       │
│                                                  │  [Container]    │       │
│                                                  └─────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Docker Services (Architecture A)

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `generator` | Custom Python | - | Debezium event simulator |
| `solace` | solace/solace-pubsub-standard | 55555, 8080, 8008 | Message broker |
| `spark-consumer` | Custom PySpark | 4040 (Spark UI) | Stream processing |
| `postgres` | postgres:15 | 5432 | ODS Database |

### 2.3 Data Flow

```
1. Generator menghasilkan CDC events (format Debezium)
2. Events di-publish ke Solace topic: cdc/oracle/{schema}/{table}/*
3. PySpark Structured Streaming consume dari Solace
4. Transformasi: agregasi, windowing, enrichment
5. Hasil disimpan ke PostgreSQL (ODS)
```

### 2.4 Use Cases
- Agregasi data per window (5 min, 1 hour, daily)
- Complex joins antar entity
- Heavy batch transformations
- Historical data processing

---

## 3. Architecture B: PyFlink Consumer

### 3.1 Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER ENVIRONMENT                                 │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │                 │     │                 │     │                 │       │
│  │  Data Generator │────▶│     Solace      │────▶│    PyFlink      │       │
│  │  (Debezium Sim) │     │    PubSub+      │     │    Consumer     │       │
│  │                 │     │                 │     │                 │       │
│  │  [Container]    │     │  [Container]    │     │  [Container]    │       │
│  └─────────────────┘     └─────────────────┘     └────────┬────────┘       │
│                                                           │                 │
│                                                           ▼                 │
│                                                  ┌─────────────────┐       │
│                                                  │                 │       │
│                                                  │    REST API     │       │
│                                                  │    (FastAPI)    │       │
│                                                  │                 │       │
│                                                  │  [Container]    │       │
│                                                  └─────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Docker Services (Architecture B)

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `generator` | Custom Python | - | Debezium event simulator |
| `solace` | solace/solace-pubsub-standard | 55555, 8080, 8008 | Message broker |
| `flink-consumer` | Custom PyFlink | 8081 (Flink UI) | Real-time processing |
| `rest-api` | Custom FastAPI | 8000 | Target REST API |

### 3.3 Data Flow

```
1. Generator menghasilkan CDC events (format Debezium)
2. Events di-publish ke Solace topic: cdc/oracle/{schema}/{table}/*
3. PyFlink DataStream consume dari Solace (real-time)
4. Processing: filtering, transformation, pattern detection
5. Hasil dikirim ke REST API endpoint
```

### 3.4 Use Cases
- Real-time event notifications
- Low-latency alerting
- Pattern detection (fraud, anomaly)
- Immediate downstream integration

---

## 4. Shared Components

### 4.1 Data Generator (Debezium Simulator)

**Fungsi:** Mensimulasikan output Debezium Connector dari Oracle Database

**Output Format:** Debezium CDC JSON Format

```json
{
  "schema": { ... },
  "payload": {
    "before": null | { ... },
    "after": { ... },
    "source": {
      "version": "2.4.0",
      "connector": "oracle",
      "name": "oracle-cdc",
      "ts_ms": 1706000000000,
      "snapshot": "false",
      "db": "ORCL",
      "schema": "HR",
      "table": "EMPLOYEES",
      "txId": "000000000001",
      "scn": "123456789"
    },
    "op": "c" | "u" | "d" | "r",
    "ts_ms": 1706000000000
  }
}
```

**Operations:**
| Op Code | Operation | Description |
|---------|-----------|-------------|
| `c` | CREATE | Insert new record |
| `u` | UPDATE | Update existing record |
| `d` | DELETE | Delete record |
| `r` | READ | Snapshot read |

### 4.2 Solace PubSub+ Message Broker

**Deployment:** Docker Container

**Port Mapping:**
| Port | Protocol | Purpose |
|------|----------|---------|
| 55555 | SMF | Solace Message Format |
| 8008 | Web Transport | WebSocket |
| 9000 | REST | REST Messaging |
| 8080 | SEMP | Management API |
| 5672 | AMQP | AMQP Protocol |

**Topic Structure:**
```
cdc/oracle/{schema}/{table}/{operation}

Examples:
├── cdc/oracle/HR/EMPLOYEES/insert
├── cdc/oracle/HR/EMPLOYEES/update
├── cdc/oracle/HR/EMPLOYEES/delete
├── cdc/oracle/SALES/ORDERS/insert
└── cdc/oracle/SALES/ORDERS/update
```

---


## 5. State Store & Bootstrap

### 5.1 Overview

Consumer service menggunakan **in-memory state store** untuk menyimpan data dimensi (CUSTOMERS, PRODUCTS) dan ORDER_ITEMS agar multi-topic join dapat dilakukan secara real-time tanpa query ulang ke database.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STATE STORE ARCHITECTURE                             │
│                                                                             │
│  ┌───────────────┐     ┌──────────────────────────────────────────────┐    │
│  │               │     │              IN-MEMORY STATE                  │    │
│  │    Solace      │     │  ┌──────────────────┐  ┌──────────────────┐ │    │
│  │    Messages    │────▶│  │ DimensionState   │  │ DimensionState   │ │    │
│  │               │     │  │ (CUSTOMERS)      │  │ (PRODUCTS)       │ │    │
│  └───────────────┘     │  │ key: customer_id │  │ key: product_id  │ │    │
│                         │  └──────────────────┘  └──────────────────┘ │    │
│                         │  ┌──────────────────┐                       │    │
│                         │  │ OrderItemsState  │                       │    │
│                         │  │ key: order_id    │                       │    │
│                         │  │ value: [items]   │                       │    │
│                         │  └──────────────────┘                       │    │
│                         └──────────────────────────────────────────────┘    │
│                                      │                                      │
│                                      ▼                                      │
│                         ┌──────────────────────┐                           │
│                         │    3-WAY JOIN         │                           │
│                         │  ORDER + CUSTOMER     │                           │
│                         │  + PRODUCT            │                           │
│                         └──────────────────────┘                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 DimensionStateStore

State store generik untuk dimension tables (CUSTOMERS, PRODUCTS). Satu instance per dimension table.

**Karakteristik:**

| Property | Value | Keterangan |
|----------|-------|------------|
| Data Structure | `Dict[key, StateEntry]` | Keyed dictionary |
| Lookup | O(1) | Hash-based |
| Deduplication | Auto (by key) | Latest value per key wins |
| TTL | 3600 seconds (default) | Configurable per store |
| Expiry Check | Setiap 500 updates | Periodic, bukan per-access |

**API:**

```python
class DimensionStateStore:
    def __init__(self, name: str, ttl_seconds: int = 3600)
    def put(self, key, data: dict) -> None      # Upsert by key
    def get(self, key) -> Optional[dict]         # Get, None if expired
    def get_all() -> Dict[key, dict]             # All non-expired entries
    def get_all_values() -> List[dict]           # For DataFrame creation
    def remove(self, key) -> None                # Delete specific key
    def expire() -> int                          # Manual expiry, returns count
    def size() -> int                            # Current entry count
    def clear() -> None                          # Remove all entries
```

### 5.3 OrderItemsStateStore

State store khusus untuk ORDER_ITEMS yang mengelompokkan items berdasarkan `order_id`.

**Karakteristik:**

| Property | Value | Keterangan |
|----------|-------|------------|
| Data Structure | `Dict[order_id, List[item]]` | Grouped by order |
| Max Items/Order | 100 | Trim ke 50 jika melebihi |
| TTL | 3600 seconds | Per-order group |
| Expiry Check | Setiap 500 updates | Periodic cleanup |

**API:**

```python
class OrderItemsStateStore:
    def __init__(self, ttl_seconds: int = 3600)
    def add_item(self, order_id: int, item: dict)  # Append item to order
    def get_items(self, order_id: int) -> List[dict] # All items for order
    def get_item_count(self, order_id: int) -> int
    def get_all_items_flat() -> List[dict]           # Flat list for DataFrame
    def expire() -> int
    def size() -> int                                 # Unique order count
    def total_items() -> int                          # Total items across orders
```

### 5.4 Bootstrap dari PostgreSQL

Saat consumer startup/restart, state store di-bootstrap dari PostgreSQL ODS agar data dimensi langsung tersedia tanpa menunggu CDC events masuk.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BOOTSTRAP FLOW                                       │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────────────────────────────────┐   │
│  │   PostgreSQL    │     │           STATE STORES                       │   │
│  │   (ODS)         │     │                                             │   │
│  │  ┌───────────┐  │     │  ┌─────────────────┐                       │   │
│  │  │cdc_       │──┼────▶│  │ customer_store  │  DISTINCT ON          │   │
│  │  │customers  │  │     │  │ (latest/key)    │  (customer_id)        │   │
│  │  └───────────┘  │     │  └─────────────────┘                       │   │
│  │  ┌───────────┐  │     │  ┌─────────────────┐                       │   │
│  │  │cdc_       │──┼────▶│  │ product_store   │  DISTINCT ON          │   │
│  │  │products   │  │     │  │ (latest/key)    │  (product_id)         │   │
│  │  └───────────┘  │     │  └─────────────────┘                       │   │
│  │  ┌───────────┐  │     │  ┌─────────────────┐                       │   │
│  │  │cdc_order_ │──┼────▶│  │ order_items     │  Last 1 hour          │   │
│  │  │items      │  │     │  │ store           │  of items             │   │
│  │  └───────────┘  │     │  └─────────────────┘                       │   │
│  └─────────────────┘     └─────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Bootstrap Queries:**

```sql
-- Load customers (latest per customer_id)
SELECT DISTINCT ON (customer_id)
    customer_id, name, email, tier, region, created_at
FROM cdc_customers
ORDER BY customer_id, source_ts DESC NULLS LAST;

-- Load products (latest per product_id)
SELECT DISTINCT ON (product_id)
    product_id, name, category, price, stock_qty, is_active
FROM cdc_products
ORDER BY product_id, source_ts DESC NULLS LAST;

-- Load recent order items (last 1 hour)
SELECT item_id, order_id, product_id, quantity, unit_price, subtotal
FROM cdc_order_items
WHERE source_ts >= NOW() - INTERVAL '1 hour'
   OR source_ts IS NULL
ORDER BY order_id, item_id;
```

**Error Handling:** Jika bootstrap gagal (PostgreSQL belum ready), consumer tetap start dengan state kosong dan akan terisi secara inkremental dari CDC events.

### 5.5 State Store vs Checkpointing

| Aspect | State Store | Checkpointing |
|--------|-------------|---------------|
| **Fungsi** | Menyimpan data dimensi untuk join | Menyimpan posisi consumer di message broker |
| **Sifat** | Volatile (hilang saat restart) | Persistent (disimpan ke disk) |
| **Recovery** | Bootstrap dari PostgreSQL | Resume dari offset terakhir |
| **Data** | CUSTOMERS, PRODUCTS, ORDER_ITEMS | Topic offset, partition ID |
| **Lokasi** | In-memory (Python dict) | File system / checkpoint dir |
| **Update** | Setiap CDC event masuk | Periodic (setiap N messages) |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CRASH RECOVERY FLOW                                       │
│                                                                             │
│  Consumer Crash → Restart                                                   │
│       │                                                                     │
│       ├──▶ 1. Load checkpoint → resume dari offset terakhir                │
│       │                                                                     │
│       ├──▶ 2. Bootstrap state store dari PostgreSQL                        │
│       │       • Customers: semua (DISTINCT ON)                              │
│       │       • Products: semua (DISTINCT ON)                               │
│       │       • Order Items: 1 jam terakhir                                 │
│       │                                                                     │
│       └──▶ 3. Resume processing → state terus di-update dari CDC events    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.6 Memory Management

| Mekanisme | Konfigurasi | Tujuan |
|-----------|-------------|--------|
| TTL Expiry | 3600s default | Hapus data lama otomatis |
| Periodic Cleanup | Setiap 500 updates | Hindari memory leak |
| Bounded Collections | Max 100 items/order | Cegah unbounded growth |
| Trim Strategy | Trim ke 50 saat > 100 | Keep recent items |

**Estimasi Memory Usage (5K events/min):**

| Store | Est. Entries | Entry Size | Total |
|-------|-------------|------------|-------|
| Customers | ~1,000 | ~500 bytes | ~500 KB |
| Products | ~500 | ~400 bytes | ~200 KB |
| Order Items | ~5,000 orders | ~2 KB/order | ~10 MB |
| **Total** | | | **~11 MB** |

> Sangat ringan untuk in-memory. TTL memastikan data lama otomatis dibersihkan.

---
## 6. Docker Compose Configurations

> **🔄 HYBRID MODE SUPPORT**
>
> Generator dapat dijalankan dalam 2 mode:
> - **Container Mode**: Include `generator` service (untuk integration test/demo)
> - **Local Mode**: Comment out `generator`, run locally (untuk development)
>
> Gunakan `profiles` untuk kontrol lebih granular.

> **Note:** Lihat file docker-compose aktual di `docker/docker-compose.spark.yml` dan `docker/docker-compose.flink.yml`.
> Kedua architecture menggunakan **dual sink** (PostgreSQL + REST API).
> Untuk panduan menjalankan, lihat [README.md](../README.md).

---

## 7. Technology Stack

| Component | Technology | Version | Container |
|-----------|------------|---------|-----------|
| Language | Python | 3.11+ | All |
| Message Broker | Solace PubSub+ | Standard | solace |
| Stream Processing | Apache Spark | 3.5.x | spark-consumer |
| Stream Processing | Apache Flink | 1.18.x | flink-consumer |
| Database | PostgreSQL | 15+ | postgres-ods |
| REST API | FastAPI | 0.109+ | rest-api |
| Container Runtime | Docker | 24+ | Host |
| Orchestration | Docker Compose | 2.x | Host |

---

## 8. Data Schemas

### 8.1 Customer CDC Event

```json
{
  "payload": {
    "before": null,
    "after": {
      "CUSTOMER_ID": 500,
      "NAME": "John Doe",
      "EMAIL": "john.doe@example.com",
      "TIER": "GOLD",
      "REGION": "Jakarta",
      "CREATED_AT": "2024-01-15T10:00:00Z"
    },
    "source": {
      "connector": "oracle",
      "db": "ORCL",
      "schema": "SALES",
      "table": "CUSTOMERS"
    },
    "op": "c",
    "ts_ms": 1706000000000
  }
}
```

### 8.2 Order CDC Event

```json
{
  "payload": {
    "before": null,
    "after": {
      "ORDER_ID": 1001,
      "CUSTOMER_ID": 500,
      "ORDER_DATE": "2024-01-20",
      "TOTAL_AMOUNT": 1500.00,
      "STATUS": "PENDING",
      "ITEMS": [
        {"product_id": 1, "quantity": 2, "price": 500.00},
        {"product_id": 2, "quantity": 1, "price": 500.00}
      ]
    },
    "source": {
      "connector": "oracle",
      "db": "ORCL",
      "schema": "SALES",
      "table": "ORDERS"
    },
    "op": "c",
    "ts_ms": 1706000000000
  }
}
```

---

## 9. Output Schemas

### 9.1 Architecture A: PostgreSQL ODS Tables

> **Master Table List (PostgreSQL ODS)**

| Table Name | Source | Purpose |
|------------|--------|---------|
| `cdc_orders` | CDC ORDERS | Cleaned orders (no XML) |
| `cdc_order_items` | CDC ORDER_ITEMS | Normalized order items |
| `cdc_order_items_extracted` | XML Extraction | Items from SHIPPING_INFO XML |
| `cdc_customers` | CDC CUSTOMERS | Customer dimension |
| `cdc_products` | CDC PRODUCTS | Product dimension |
| `cdc_orders_enriched` | 3-way Join | Enriched orders dengan customer & product |
| `cdc_audit_log` | CDC AUDIT_LOG | Audit trail |
| `cdc_dlq` | Error handling | Dead letter queue records |

```sql
-- Orders (dari CDC, cleaned tanpa XML)
CREATE TABLE cdc_orders (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(15,2),
    status VARCHAR(50),
    source_ts TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items (dari CDC ORDER_ITEMS table)
CREATE TABLE cdc_order_items (
    id SERIAL PRIMARY KEY,
    item_id INT NOT NULL,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    subtotal DECIMAL(15,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items Extracted (dari XML parsing - Section 20)
CREATE TABLE cdc_order_items_extracted (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    subtotal DECIMAL(15,2),
    extracted_from VARCHAR(50) DEFAULT 'SHIPPING_INFO_XML',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enriched Orders (hasil 3-way join)
CREATE TABLE cdc_orders_enriched (
    id SERIAL PRIMARY KEY,
    order_id INT,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_tier VARCHAR(20),
    total_amount DECIMAL(15,2),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit Log
CREATE TABLE cdc_audit_log (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    table_name VARCHAR(100),
    operation VARCHAR(10),
    record_id VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dead Letter Queue
CREATE TABLE cdc_dlq (
    id SERIAL PRIMARY KEY,
    original_topic VARCHAR(200),
    error_type VARCHAR(50),
    error_message TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 9.2 Architecture B: REST API Payloads

**Event Notification:**
```json
POST /api/v1/events
{
  "event_type": "ORDER_CREATED",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2024-01-20T10:00:00Z",
  "data": {
    "order_id": 1001,
    "customer_id": 500,
    "total_amount": 1500.00,
    "status": "PENDING"
  },
  "metadata": {
    "source": "cdc-pyflink",
    "source_table": "ORDERS",
    "processing_time_ms": 15
  }
}
```

**Alert Notification:**
```json
POST /api/v1/notifications
{
  "notification_type": "HIGH_VALUE_ORDER",
  "priority": "HIGH",
  "timestamp": "2024-01-20T10:00:00Z",
  "payload": {
    "order_id": 1001,
    "amount": 10000.00,
    "threshold": 5000.00
  }
}
```

---

## 10. Implementation Phases

### Phase 1: Infrastructure Setup ✅
- [x] Setup base Docker configurations
- [x] Configure Solace container dengan topics
- [x] Setup PostgreSQL dengan init scripts
- [x] Setup FastAPI mock server
- [x] Test connectivity antar containers

### Phase 2: Data Generator ✅
- [x] Implement Debezium format models
- [x] Implement Solace publisher
- [x] Create data generators untuk 5 tables (ORDERS, ORDER_ITEMS, CUSTOMERS, PRODUCTS, AUDIT_LOG)
- [x] Add configurable event rates dan patterns
- [x] Dockerize generator

### Phase 3: Architecture A - PySpark ✅
- [x] Setup PySpark dengan Solace connector
- [x] Implement stream consumer with dual sink (PostgreSQL + REST API)
- [x] Create transformations (CDC parsing, 3-way join, XML extraction)
- [x] Implement PostgreSQL + REST API sinks with circuit breaker
- [x] DLQ handler, state store, crash recovery
- [x] Dockerize spark-consumer

### Phase 4: Architecture B - PyFlink ✅
- [x] Setup PyFlink environment
- [x] Implement Solace source
- [x] Create event processors (CDC, enrichment, XML extraction)
- [x] Implement dual sink: PostgreSQL + REST API
- [x] DLQ handler, state store, crash recovery
- [x] Dockerize flink-consumer

### Phase 5: Testing ✅
- [x] 220 unit tests (PySpark: 62, PyFlink: 96, REST API: 28, State Store: 21)
- [x] All tests passing

---

> **Note:** Untuk panduan instalasi, deployment, dan menjalankan POC, lihat [README.md](../README.md).

---

## 11. Use Case & Performance Requirements

### 11.1 Target Performance

| Metric | Target | Per Second |
|--------|--------|------------|
| Event Throughput | 5,000 events/menit | ~83 events/sec |
| Peak Load (2x buffer) | 10,000 events/menit | ~167 events/sec |

### 11.2 Transformation Complexity

Consumer service akan melakukan **multi-topic join** yang melibatkan 3 topic:

```
┌─────────────┐
│  Topic A    │───┐
│ (ORDERS)    │   │
└─────────────┘   │    ┌──────────────────┐    ┌─────────────┐
                  ├───▶│   JOIN/ENRICH    │───▶│   OUTPUT    │
┌─────────────┐   │    │   Transformation │    │  ODS / API  │
│  Topic B    │───┤    └──────────────────┘    └─────────────┘
│ (CUSTOMERS) │   │
└─────────────┘   │
                  │
┌─────────────┐   │
│  Topic C    │───┘
│ (PRODUCTS)  │
└─────────────┘
```

**Transformation Pattern (Core 3-way):**
- Join ORDERS dengan CUSTOMERS (customer enrichment)
- Join ORDERS dengan PRODUCTS via ORDER_ITEMS (product enrichment)
- Agregasi dan kalkulasi

> **Note:** Untuk full enrichment dengan ORDER_ITEMS detail, menjadi 4-way join.
> Core validation tetap 3-way: ORDERS ↔ CUSTOMERS ↔ PRODUCTS.

### 11.3 Simulasi vs Real Case

| Aspect | Real Case | Simulasi POC | Justifikasi |
|--------|-----------|--------------|-------------|
| Jumlah Table | 20 tables | 5 tables | ✅ Sufficient (lihat analisis) |
| Events/menit | 5,000 | 5,000 - 10,000 | ✅ Match + buffer |
| Multi-topic Join | 3 topics | 3 topics | ✅ Exact match |
| Topic Creation | Dynamic | Dynamic | ✅ Match |

---

## 12. Rekomendasi Simulasi: 5 Table vs 20 Table

### 12.1 Analisis: Apakah 5 Table Cukup?

**✅ YA, 5 table CUKUP untuk POC dengan syarat:**

#### Alasan Teknis:

1. **Bottleneck bukan di jumlah topic**
   - Solace dapat handle ratusan topic dengan mudah
   - Yang krusial adalah **throughput total** dan **kompleksitas join**

2. **Join complexity sudah ter-cover**
   - Real case: join 3 topic → Simulasi: join 3 topic ✅
   - Pattern yang sama = validasi yang sama

3. **5K events/menit relatif moderate**
   - ~83 events/sec adalah load yang manageable
   - Modern streaming (Spark/Flink) bisa handle jutaan events/sec

4. **Scaling linear**
   - Jika 5 topic @1000 events/min works → 20 topic @250 events/min juga works
   - Yang penting: total throughput dan join pattern

### 12.2 Rekomendasi: 5 Table dengan Distribusi Realistis

```
┌─────────────────────────────────────────────────────────────────┐
│                    SIMULASI 5 TABLE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Table          │ Events/min │ Ratio │ Role in Join            │
│  ───────────────┼────────────┼───────┼──────────────────────── │
│  ORDERS         │ 2,000      │ 40%   │ Primary (high volume)   │
│  ORDER_ITEMS    │ 2,000      │ 40%   │ Primary (high volume)   │
│  CUSTOMERS      │ 500        │ 10%   │ Dimension (join target) │
│  PRODUCTS       │ 400        │ 8%    │ Dimension (join target) │
│  AUDIT_LOG      │ 100        │ 2%    │ Low volume reference    │
│  ───────────────┼────────────┼───────┼──────────────────────── │
│  TOTAL          │ 5,000      │ 100%  │                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 12.3 Kenapa Distribusi Ini Realistis?

| Pattern | Penjelasan |
|---------|------------|
| **High-volume tables** (ORDERS, ORDER_ITEMS) | Transactional data yang sering berubah |
| **Low-volume tables** (CUSTOMERS, PRODUCTS) | Master/dimension data, jarang update |
| **Mixed workload** | Kombinasi INSERT heavy + UPDATE moderate |

### 12.4 Yang Perlu Divalidasi di POC

| # | Validation Point | Success Criteria |
|---|------------------|------------------|
| 1 | **Throughput** | Sustain 5K events/min tanpa backpressure |
| 2 | **3-way Join Latency** | < 5 detik end-to-end (adjustable) |
| 3 | **Memory Stability** | No OOM dalam 1 jam continuous load |
| 4 | **Dynamic Topic** | Auto-create topic saat table baru muncul |
| 5 | **Error Recovery** | Consumer resume setelah restart |

---

## 13. Detailed Table Schemas untuk Simulasi

### 13.1 ORDERS (High Volume - Primary)

```json
{
  "table": "ORDERS",
  "events_per_min": 2000,
  "operations": {"insert": 70, "update": 25, "delete": 5},
  "schema": {
    "ORDER_ID": "NUMBER(10)",
    "CUSTOMER_ID": "NUMBER(10)",      // FK → CUSTOMERS
    "ORDER_DATE": "TIMESTAMP",
    "STATUS": "VARCHAR2(20)",
    "TOTAL_AMOUNT": "NUMBER(15,2)",
    "SHIPPING_INFO": "CLOB",          // XML column untuk extraction (Section 20)
    "CREATED_AT": "TIMESTAMP",
    "UPDATED_AT": "TIMESTAMP"
  }
}
```

> **Note:** Kolom `SHIPPING_INFO` berisi XML data yang akan di-extract ke table terpisah.
> Ini berbeda dengan table `ORDER_ITEMS` yang merupakan table normalized terpisah.

### 13.2 ORDER_ITEMS (High Volume - Primary)

```json
{
  "table": "ORDER_ITEMS",
  "events_per_min": 2000,
  "operations": {"insert": 80, "update": 15, "delete": 5},
  "schema": {
    "ITEM_ID": "NUMBER(10)",
    "ORDER_ID": "NUMBER(10)",         // FK → ORDERS
    "PRODUCT_ID": "NUMBER(10)",       // FK → PRODUCTS
    "QUANTITY": "NUMBER(5)",
    "UNIT_PRICE": "NUMBER(10,2)",
    "SUBTOTAL": "NUMBER(15,2)"
  }
}
```

### 13.3 CUSTOMERS (Low Volume - Dimension)

```json
{
  "table": "CUSTOMERS",
  "events_per_min": 500,
  "operations": {"insert": 20, "update": 75, "delete": 5},
  "schema": {
    "CUSTOMER_ID": "NUMBER(10)",
    "NAME": "VARCHAR2(100)",
    "EMAIL": "VARCHAR2(100)",
    "TIER": "VARCHAR2(20)",           // GOLD, SILVER, BRONZE
    "REGION": "VARCHAR2(50)",
    "CREATED_AT": "TIMESTAMP"
  }
}
```

### 13.4 PRODUCTS (Low Volume - Dimension)

```json
{
  "table": "PRODUCTS",
  "events_per_min": 400,
  "operations": {"insert": 10, "update": 85, "delete": 5},
  "schema": {
    "PRODUCT_ID": "NUMBER(10)",
    "NAME": "VARCHAR2(200)",
    "CATEGORY": "VARCHAR2(50)",
    "PRICE": "NUMBER(10,2)",
    "STOCK_QTY": "NUMBER(10)",
    "IS_ACTIVE": "NUMBER(1)"
  }
}
```

### 13.5 AUDIT_LOG (Low Volume - Reference)

```json
{
  "table": "AUDIT_LOG",
  "events_per_min": 100,
  "operations": {"insert": 100, "update": 0, "delete": 0},
  "schema": {
    "LOG_ID": "NUMBER(10)",
    "TABLE_NAME": "VARCHAR2(50)",
    "OPERATION": "VARCHAR2(10)",
    "RECORD_ID": "VARCHAR2(100)",
    "CHANGED_BY": "VARCHAR2(50)",
    "CHANGED_AT": "TIMESTAMP"
  }
}
```

---

## 14. Transformation Use Cases

### 14.1 Architecture A (PySpark) - Multi-Topic Join

**Use Case: Order Enrichment dengan Customer & Product Data**

```python
# Pseudocode - PySpark Structured Streaming

# Stream dari 4 topic (3-way core + ORDER_ITEMS untuk detail)
orders_stream = spark.readStream.format("solace")...      # Core
customers_stream = spark.readStream.format("solace")...   # Core
products_stream = spark.readStream.format("solace")...    # Core
order_items_stream = spark.readStream.format("solace")... # Extended

# Watermark untuk late data handling
orders_with_watermark = orders_stream.withWatermark("event_time", "10 seconds")

# Multi-way join
enriched_orders = (
    orders_stream
    .join(customers_stream, "CUSTOMER_ID")           # Join 1
    .join(order_items_stream, "ORDER_ID")            # Join 2
    .join(products_stream, "PRODUCT_ID")             # Join 3
    .groupBy(
        window("event_time", "5 minutes"),
        "CUSTOMER_TIER",
        "PRODUCT_CATEGORY"
    )
    .agg(
        count("ORDER_ID").alias("order_count"),
        sum("TOTAL_AMOUNT").alias("total_revenue"),
        avg("TOTAL_AMOUNT").alias("avg_order_value")
    )
)

# Write ke PostgreSQL ODS
enriched_orders.writeStream.format("jdbc")...
```

### 14.2 Architecture B (PyFlink) - Real-time Alerting

**Use Case: High-Value Order Detection**

```python
# Pseudocode - PyFlink DataStream

# Stream dari Solace
orders_stream = env.add_source(SolaceSource(...))
customers_stream = env.add_source(SolaceSource(...))

# Join order dengan customer untuk enrichment
enriched_stream = (
    orders_stream
    .key_by(lambda x: x.customer_id)
    .connect(customers_stream.key_by(lambda x: x.customer_id))
    .process(EnrichmentFunction())
)

# Detect high-value orders
alerts = (
    enriched_stream
    .filter(lambda x: x.total_amount > 10000)
    .filter(lambda x: x.customer_tier == "GOLD")
    .map(lambda x: create_alert(x))
)

# Send ke REST API
alerts.add_sink(RestApiSink(...))
```

---

## 15. Benchmark Scenarios

### 15.1 Scenario Matrix

| Scenario | Events/min | Tables | Join | Duration | Purpose |
|----------|------------|--------|------|----------|---------|
| **Baseline** | 1,000 | 5 | 3-way | 10 min | Functionality check |
| **Target Load** | 5,000 | 5 | 3-way | 30 min | Target validation |
| **Peak Load** | 10,000 | 5 | 3-way | 15 min | Burst handling |
| **Sustained** | 5,000 | 5 | 3-way | 60 min | Stability test |
| **Recovery** | 5,000 | 5 | 3-way | - | Restart during load |

### 15.2 Success Metrics

```
┌────────────────────────────────────────────────────────────────┐
│                    BENCHMARK SUCCESS CRITERIA                   │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Metric                    │ Target         │ Acceptable       │
│  ─────────────────────────┼────────────────┼───────────────── │
│  Throughput                │ 5,000 evt/min  │ > 4,500 evt/min  │
│  End-to-end Latency (p50)  │ < 2 sec        │ < 5 sec          │
│  End-to-end Latency (p99)  │ < 5 sec        │ < 10 sec         │
│  Error Rate                │ 0%             │ < 0.1%           │
│  Consumer Lag              │ < 1,000 msgs   │ < 5,000 msgs     │
│  Memory Usage              │ Stable         │ No OOM           │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## 16. Rekomendasi Akhir

### ✅ Gunakan 5 Table untuk POC

**Reasoning:**
1. 5 table sudah mencakup semua pattern yang dibutuhkan
2. Kompleksitas join (3-way) sudah match dengan real case
3. Total throughput 5K/min adalah metric yang valid
4. Jika 5 table @5K/min works → 20 table @5K/min juga works (sama saja)

### ⚠️ Yang Perlu Diperhatikan

1. **Distribusi event harus realistis** - bukan flat 1000/table
2. **Join pattern harus exact** - simulasi 3-way join seperti real case
3. **Test sustained load** - minimal 1 jam untuk validasi stability
4. **Include failure scenarios** - consumer restart, Solace restart

### 📋 Optional: Scaling Test

Jika ingin extra confidence, tambahkan scenario:
- 10 table @5K/min (scale topic count)
- 5 table @10K/min (scale throughput)

---

## 17. Failure Scenarios (WAJIB)

### 17.1 Overview

POC **WAJIB** menghandle semua common failure scenarios untuk memvalidasi production-readiness.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         FAILURE SCENARIOS MATRIX                            │
├────────────────────────────────────────────────────────────────────────────┤
│  Category          │ Scenario                    │ Expected Behavior       │
│  ──────────────────┼─────────────────────────────┼─────────────────────────│
│  Consumer Failure  │ Consumer crash/restart      │ Resume from checkpoint  │
│  Consumer Failure  │ Consumer OOM                │ Graceful restart        │
│  Consumer Failure  │ Processing timeout          │ Retry atau DLQ          │
│  ──────────────────┼─────────────────────────────┼─────────────────────────│
│  Broker Failure    │ Solace restart              │ Auto-reconnect          │
│  Broker Failure    │ Network partition           │ Retry with backoff      │
│  Broker Failure    │ Queue full                  │ Backpressure handling   │
│  ──────────────────┼─────────────────────────────┼─────────────────────────│
│  Target Failure    │ PostgreSQL down             │ Retry + circuit breaker │
│  Target Failure    │ REST API timeout            │ Retry + DLQ             │
│  Target Failure    │ REST API 5xx error          │ Retry dengan backoff    │
│  ──────────────────┼─────────────────────────────┼─────────────────────────│
│  Data Failure      │ Malformed message           │ DLQ + continue          │
│  Data Failure      │ Schema mismatch             │ DLQ + alert             │
│  Data Failure      │ Late arriving data          │ Watermark handling      │
│  ──────────────────┼─────────────────────────────┼─────────────────────────│
│  Resource Failure  │ Disk full                   │ Alert + graceful stop   │
│  Resource Failure  │ Memory pressure             │ Backpressure + GC       │
└────────────────────────────────────────────────────────────────────────────┘
```

### 17.2 Consumer Failure Handling

#### 17.2.1 Consumer Crash/Restart

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Consumer   │     │  Checkpoint │     │   Solace    │
│   Crash     │────▶│   Storage   │────▶│   Resume    │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │  No Data    │
                    │    Loss     │
                    └─────────────┘
```

**Implementation:**

```python
# PySpark - Checkpoint Configuration
spark.conf.set("spark.sql.streaming.checkpointLocation", "/checkpoints/spark")

# PyFlink - Checkpoint Configuration
env.enable_checkpointing(10000)  # 10 seconds
env.get_checkpoint_config().set_checkpoint_storage_dir("file:///checkpoints/flink")
```

**Test Scenario:**
1. Start consumer dengan load 5K/min
2. Kill consumer process (SIGKILL)
3. Restart consumer
4. Verify: No duplicate, no data loss

#### 17.2.2 Processing Timeout

```yaml
# Retry Policy Configuration
retry:
  max_attempts: 3
  initial_delay_ms: 1000
  max_delay_ms: 30000
  multiplier: 2.0

dead_letter:
  enabled: true
  topic: cdc/dlq/{original_topic}
  include_error_metadata: true
```

### 17.3 Broker (Solace) Failure Handling

#### 17.3.1 Auto-Reconnect

```python
# Solace Connection with Reconnect
solace_config = {
    "host": "solace:55555",
    "vpn": "default",
    "reconnect_retries": -1,           # Infinite retry
    "reconnect_retry_wait_ms": 3000,   # 3 seconds
    "connect_timeout_ms": 30000,
    "keepalive_interval_ms": 3000
}
```

#### 17.3.2 Backpressure Handling

```
┌─────────────────────────────────────────────────────────────────┐
│                    BACKPRESSURE STRATEGY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Queue Depth     │ Action                                       │
│  ────────────────┼──────────────────────────────────────────── │
│  < 70%           │ Normal processing                            │
│  70% - 85%       │ Log warning, increase batch size             │
│  85% - 95%       │ Alert, pause generator (if controlled)       │
│  > 95%           │ Critical alert, reject new messages          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 17.4 Target System Failure Handling

#### 17.4.1 PostgreSQL (Architecture A)

```python
# Circuit Breaker Pattern
class PostgresWriter:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30,
            expected_exception=OperationalError
        )

    @circuit_breaker
    def write_batch(self, records):
        with self.connection.begin():
            # Batch insert with retry
            pass
```

**Failure Matrix:**

| Error Type | Action | Max Retry |
|------------|--------|-----------|
| Connection refused | Retry with backoff | 10 |
| Timeout | Retry | 3 |
| Constraint violation | DLQ | 0 |
| Disk full | Alert + Stop | 0 |

#### 17.4.2 REST API (Architecture B)

```python
# Async HTTP Client with Retry
class RestApiSink:
    async def send_event(self, event):
        for attempt in range(self.max_retries):
            try:
                async with self.session.post(
                    self.endpoint,
                    json=event,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        return True
                    elif response.status >= 500:
                        await self.backoff(attempt)
                    elif response.status >= 400:
                        await self.send_to_dlq(event, response)
                        return False
            except asyncio.TimeoutError:
                await self.backoff(attempt)

        # Max retries exceeded
        await self.send_to_dlq(event, "max_retries_exceeded")
        return False
```

### 17.5 Data Failure Handling

#### 17.5.1 Dead Letter Queue (DLQ) Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DLQ ARCHITECTURE                                   │
│                                                                             │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                   │
│  │   Source    │────▶│  Consumer   │────▶│   Target    │                   │
│  │   Topic     │     │  Processing │     │  ODS/API    │                   │
│  └─────────────┘     └──────┬──────┘     └─────────────┘                   │
│                             │                                               │
│                             │ On Error                                      │
│                             ▼                                               │
│                      ┌─────────────┐     ┌─────────────┐                   │
│                      │    DLQ      │────▶│   DLQ       │                   │
│                      │   Topic     │     │  Consumer   │                   │
│                      └─────────────┘     └──────┬──────┘                   │
│                                                 │                           │
│                                                 ▼                           │
│                                          ┌─────────────┐                   │
│                                          │  Manual     │                   │
│                                          │  Review DB  │                   │
│                                          └─────────────┘                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 17.5.2 DLQ Message Format

```json
{
  "dlq_metadata": {
    "original_topic": "cdc/oracle/HR/EMPLOYEES/insert",
    "error_type": "SCHEMA_MISMATCH",
    "error_message": "Missing required field: EMPLOYEE_ID",
    "retry_count": 3,
    "first_failure_at": "2024-02-04T10:00:00Z",
    "last_failure_at": "2024-02-04T10:00:30Z",
    "consumer_id": "spark-consumer-1"
  },
  "original_payload": {
    "payload": { ... }
  }
}
```

### 17.6 Failure Test Scenarios

| # | Scenario | How to Simulate | Expected Result | Priority |
|---|----------|-----------------|-----------------|----------|
| F1 | Consumer crash | `docker kill spark-consumer` | Resume from checkpoint, no data loss | **WAJIB** |
| F2 | Consumer restart | `docker restart spark-consumer` | Graceful recovery | **WAJIB** |
| F3 | Solace restart | `docker restart solace` | Auto-reconnect, resume | **WAJIB** |
| F4 | PostgreSQL down | `docker stop postgres` | Circuit breaker, retry | **WAJIB** |
| F5 | REST API 503 | Return 503 from mock | Retry with backoff | **WAJIB** |
| F6 | Malformed JSON | Send invalid JSON | DLQ, processing continues | **WAJIB** |
| F7 | Network partition | `iptables` block | Reconnect after recovery | **WAJIB** |
| F8 | Memory pressure | Limit container memory | Backpressure, no OOM | **WAJIB** |
| F9 | Slow consumer | Add artificial delay | Queue buildup, then recover | **WAJIB** |
| F10 | Late data | Send out-of-order events | Watermark handling | **WAJIB** |

### 17.7 Monitoring & Alerting untuk Failure

```yaml
# Alert Rules
alerts:
  - name: ConsumerDown
    condition: consumer_heartbeat_missing > 30s
    severity: CRITICAL

  - name: HighErrorRate
    condition: error_rate > 1%
    severity: HIGH

  - name: DLQGrowing
    condition: dlq_depth > 100
    severity: MEDIUM

  - name: HighLatency
    condition: processing_latency_p99 > 10s
    severity: MEDIUM

  - name: QueueBacklog
    condition: queue_depth > 5000
    severity: HIGH
```

---

## 18. Distributed Processing (OPSIONAL)

### 18.1 Overview

Untuk extra scalability validation, arsitektur dapat di-extend dengan distributed processing.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DISTRIBUTED ARCHITECTURE (OPSIONAL)                       │
│                                                                             │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────┐   │
│  │             │     │             │     │      Consumer Cluster        │   │
│  │  Generator  │────▶│   Solace    │────▶├───────────┬───────────┬─────┤   │
│  │  (1 node)   │     │  (Cluster)  │     │ Consumer  │ Consumer  │ ... │   │
│  │             │     │             │     │    #1     │    #2     │     │   │
│  └─────────────┘     └─────────────┘     └─────┬─────┴─────┬─────┴─────┘   │
│                                                │           │               │
│                                                ▼           ▼               │
│                                         ┌───────────────────────┐         │
│                                         │   Shared State Store   │         │
│                                         │   (Redis / RocksDB)    │         │
│                                         └───────────────────────┘         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 18.2 PySpark Distributed Mode

#### docker-compose.spark-distributed.yml

```yaml
version: '3.8'

services:
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"   # Master UI
      - "7077:7077"   # Master port
    networks:
      - cdc-network

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    depends_on:
      - spark-master
    networks:
      - cdc-network

  spark-worker-2:
    image: bitnami/spark:3.5
    container_name: spark-worker-2
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    depends_on:
      - spark-master
    networks:
      - cdc-network

  spark-consumer:
    build:
      context: .
      dockerfile: docker/dockerfiles/Dockerfile.spark
    container_name: spark-consumer
    environment:
      - SPARK_MASTER=spark://spark-master:7077
    depends_on:
      - spark-master
      - spark-worker-1
      - spark-worker-2
    networks:
      - cdc-network
```

### 18.3 PyFlink Distributed Mode

#### docker-compose.flink-distributed.yml

```yaml
version: '3.8'

services:
  flink-jobmanager:
    image: flink:1.18
    container_name: flink-jobmanager
    command: jobmanager
    ports:
      - "8081:8081"
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
    networks:
      - cdc-network

  flink-taskmanager-1:
    image: flink:1.18
    container_name: flink-taskmanager-1
    command: taskmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2
    depends_on:
      - flink-jobmanager
    networks:
      - cdc-network

  flink-taskmanager-2:
    image: flink:1.18
    container_name: flink-taskmanager-2
    command: taskmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2
    depends_on:
      - flink-jobmanager
    networks:
      - cdc-network
```

### 18.4 Partitioning Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                    PARTITIONING STRATEGY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Data Type      │ Partition Key    │ Reason                     │
│  ───────────────┼──────────────────┼─────────────────────────── │
│  ORDERS         │ CUSTOMER_ID      │ Co-locate dengan customer  │
│  ORDER_ITEMS    │ ORDER_ID         │ Co-locate dengan order     │
│  CUSTOMERS      │ CUSTOMER_ID      │ Primary key                │
│  PRODUCTS       │ PRODUCT_ID       │ Primary key                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Benefit:** Events dengan key yang sama akan diproses oleh partition/worker yang sama, mengurangi shuffle untuk join operations.

### 18.5 State Management untuk Distributed Join

```python
# PyFlink - Keyed State untuk Join
class OrderCustomerJoinFunction(KeyedCoProcessFunction):
    def open(self, runtime_context):
        # State untuk customer data (dimension)
        customer_state_desc = ValueStateDescriptor(
            "customer_state",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.customer_state = runtime_context.get_state(customer_state_desc)

        # State untuk pending orders (waiting for customer)
        orders_state_desc = ListStateDescriptor(
            "pending_orders",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.pending_orders = runtime_context.get_list_state(orders_state_desc)

    def process_element1(self, order, ctx):
        # Process order, join dengan customer jika ada
        customer = self.customer_state.value()
        if customer:
            yield enrich_order(order, customer)
        else:
            self.pending_orders.add(order)

    def process_element2(self, customer, ctx):
        # Update customer state, process pending orders
        self.customer_state.update(customer)
        for order in self.pending_orders.get():
            yield enrich_order(order, customer)
        self.pending_orders.clear()
```

### 18.6 Distributed Mode Test Scenarios

| # | Scenario | Purpose | Priority |
|---|----------|---------|----------|
| D1 | 2 workers, 5K events/min | Basic distribution | OPSIONAL |
| D2 | Worker failure + recovery | Fault tolerance | OPSIONAL |
| D3 | Scale up (add worker) | Dynamic scaling | OPSIONAL |
| D4 | Scale down (remove worker) | Graceful degradation | OPSIONAL |
| D5 | 4 workers, 20K events/min | High throughput | OPSIONAL |

### 18.7 Kapan Perlu Distributed?

| Kondisi | Rekomendasi |
|---------|-------------|
| 5K events/min, 3-way join | Single node cukup |
| 50K+ events/min | Consider distributed |
| Complex stateful processing | Consider distributed |
| High availability requirement | Distributed wajib |
| POC untuk validate architecture | Single node dulu |

---

## 19. Dynamic Schema Evolution (WAJIB)

### 19.1 Requirement

CDC events dari Oracle mungkin mengalami schema changes:
- **Kolom baru ditambahkan** → Consumer harus otomatis menyesuaikan
- **Kolom dihapus** → Treat as NULL, tidak boleh error

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       SCHEMA EVOLUTION SCENARIOS                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Scenario              │ Source (Debezium)      │ Consumer Behavior         │
│  ──────────────────────┼────────────────────────┼───────────────────────────│
│  New column added      │ + LOYALTY_POINTS: 100  │ Auto-detect, process      │
│  Column removed        │ - EMAIL (missing)      │ Treat as NULL             │
│  Column type changed   │ PRICE: "100" → 100     │ Safe type coercion        │
│  Column renamed        │ NAME → FULL_NAME       │ Treat as new + old NULL   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 19.2 Implementation Strategy

#### Schema-on-Read Approach

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Solace    │────▶│   Schema    │────▶│  Dynamic    │────▶│   Target    │
│   (JSON)    │     │  Inference  │     │  Transform  │     │   (Flex)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │   Schema    │
                    │   Cache     │
                    └─────────────┘
```

**Key Principles:**
1. **Tidak menggunakan fixed schema** - Parse JSON secara dinamis
2. **Schema inference** - Detect schema dari incoming messages
3. **Flexible target storage** - JSONB atau dynamic DDL

### 19.3 PySpark Implementation

```python
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, col, schema_of_json

class DynamicSchemaConsumer:
    def __init__(self, spark):
        self.spark = spark
        self.schema_cache = {}

    def process_stream(self, df):
        # Option 1: Parse as Map (fully dynamic)
        dynamic_df = df.select(
            col("key"),
            from_json(col("value"), MapType(StringType(), StringType())).alias("data")
        )

        # Option 2: Infer schema from sample
        # sample = df.select("value").first()[0]
        # inferred_schema = schema_of_json(sample)

        return dynamic_df

    def handle_schema_change(self, new_record, expected_columns):
        """
        Handle missing/extra columns gracefully
        """
        result = {}
        for col_name in expected_columns:
            # Missing column → NULL
            result[col_name] = new_record.get(col_name, None)

        # Extra columns → include them (schema evolution)
        for col_name, value in new_record.items():
            if col_name not in result:
                result[col_name] = value

        return result
```

#### PySpark - Flexible Join dengan Dynamic Schema

```python
def dynamic_join(orders_df, customers_df):
    """
    Join yang toleran terhadap schema changes
    """
    # Get actual columns at runtime
    order_cols = set(orders_df.columns)
    customer_cols = set(customers_df.columns)

    # Join key must exist
    if "CUSTOMER_ID" not in order_cols or "CUSTOMER_ID" not in customer_cols:
        raise SchemaError("Join key CUSTOMER_ID missing")

    # Perform join - extra columns automatically included
    joined = orders_df.join(
        customers_df,
        on="CUSTOMER_ID",
        how="left"  # Left join → missing customer = NULL
    )

    return joined
```

### 19.4 PyFlink Implementation

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import MapFunction
import json

class DynamicSchemaMapper(MapFunction):
    def __init__(self, required_fields):
        self.required_fields = required_fields
        self.known_fields = set(required_fields)

    def map(self, value):
        record = json.loads(value)
        payload = record.get("payload", {}).get("after", {})

        result = {}

        # Required fields - NULL if missing
        for field in self.required_fields:
            result[field] = payload.get(field, None)

        # New fields - auto include
        for field, val in payload.items():
            if field not in result:
                result[field] = val
                # Log new field detection
                if field not in self.known_fields:
                    self.known_fields.add(field)
                    logger.info(f"New field detected: {field}")

        return result

# Usage
stream = env.add_source(solace_source)
dynamic_stream = stream.map(
    DynamicSchemaMapper(required_fields=["ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT"])
)
```

### 19.5 Target Storage Strategy

#### Option A: PostgreSQL dengan JSONB (Recommended untuk POC)

```sql
-- Flexible schema dengan JSONB
CREATE TABLE cdc_orders_dynamic (
    id SERIAL PRIMARY KEY,
    order_id INT,                          -- Known required field
    customer_id INT,                       -- Known required field
    data JSONB,                            -- All other fields (dynamic)
    source_table VARCHAR(100),
    operation VARCHAR(10),
    event_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index untuk query performance
CREATE INDEX idx_orders_data ON cdc_orders_dynamic USING GIN (data);

-- Query new column (even if added later)
SELECT
    order_id,
    data->>'LOYALTY_POINTS' as loyalty_points,  -- New column
    data->>'STATUS' as status
FROM cdc_orders_dynamic
WHERE data->>'LOYALTY_POINTS' IS NOT NULL;
```

#### Option B: Hybrid Approach (Known + Dynamic)

```sql
-- Known columns as regular columns, unknown as JSONB
CREATE TABLE cdc_orders_hybrid (
    id SERIAL PRIMARY KEY,
    -- Known stable columns
    order_id INT NOT NULL,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(15,2),
    status VARCHAR(50),
    -- Dynamic/overflow columns
    extra_fields JSONB DEFAULT '{}',
    -- Metadata
    source_table VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Option C: Auto ALTER TABLE (Advanced)

```python
class DynamicTableManager:
    def __init__(self, connection):
        self.conn = connection
        self.column_cache = {}

    async def ensure_column_exists(self, table_name, column_name, column_type):
        """
        Automatically add column if not exists
        """
        cache_key = f"{table_name}.{column_name}"
        if cache_key in self.column_cache:
            return

        # Check if column exists
        exists = await self.check_column_exists(table_name, column_name)
        if not exists:
            # Add column with NULL default
            await self.conn.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS {column_name} {column_type} DEFAULT NULL
            """)
            logger.info(f"Added new column: {table_name}.{column_name}")

        self.column_cache[cache_key] = True
```

### 19.6 REST API Target (Architecture B)

```python
class DynamicRestApiSink:
    """
    REST API sink yang forward semua fields tanpa filtering
    """
    async def send_event(self, event):
        # Forward semua fields as-is
        payload = {
            "event_type": self.determine_event_type(event),
            "timestamp": datetime.utcnow().isoformat(),
            "data": event,  # Include ALL fields, termasuk yang baru
            "metadata": {
                "source": "cdc-pyflink",
                "schema_version": "dynamic"
            }
        }

        await self.http_client.post(self.endpoint, json=payload)
```

### 19.7 Schema Change Detection & Logging

```python
class SchemaChangeDetector:
    def __init__(self):
        self.known_schemas = {}  # table_name -> set of columns

    def detect_changes(self, table_name, record):
        current_columns = set(record.keys())
        known_columns = self.known_schemas.get(table_name, set())

        # New columns
        new_columns = current_columns - known_columns
        if new_columns:
            logger.warning(f"[SCHEMA_CHANGE] New columns in {table_name}: {new_columns}")
            self.emit_alert("NEW_COLUMNS", table_name, new_columns)

        # Removed columns (present in known but not in current)
        removed_columns = known_columns - current_columns
        if removed_columns:
            logger.warning(f"[SCHEMA_CHANGE] Missing columns in {table_name}: {removed_columns}")
            # Don't alert - treat as NULL per requirement

        # Update cache
        self.known_schemas[table_name] = current_columns

        return {
            "new_columns": list(new_columns),
            "removed_columns": list(removed_columns)
        }
```

### 19.8 Schema Evolution Test Scenarios

| # | Scenario | Test Steps | Expected Result | Priority |
|---|----------|------------|-----------------|----------|
| S1 | New column added | Add LOYALTY_POINTS to CUSTOMERS | Auto-detected, stored | **WAJIB** |
| S2 | Column removed | Remove EMAIL from payload | Treated as NULL | **WAJIB** |
| S3 | Multiple new columns | Add 3 columns at once | All detected & stored | **WAJIB** |
| S4 | Type change (safe) | String "100" → Number 100 | Coerced safely | **WAJIB** |
| S5 | Nested JSON added | Add address: {city, zip} | Stored as JSONB | **WAJIB** |
| S6 | Join with new column | New column used in enrichment | Join still works | **WAJIB** |

### 19.9 Transformation dengan Dynamic Schema

```python
def dynamic_aggregation(df):
    """
    Agregasi yang toleran terhadap schema changes
    """
    # Required columns untuk agregasi
    required = ["ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT"]

    # Verify required columns exist
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SchemaError(f"Required columns missing: {missing}")

    # Aggregation - only use known columns
    result = df.groupBy("CUSTOMER_ID").agg(
        F.count("ORDER_ID").alias("order_count"),
        F.sum("TOTAL_AMOUNT").alias("total_spent")
    )

    # Optional enrichment columns (if exists)
    optional_cols = ["LOYALTY_POINTS", "TIER", "REGION"]
    for col_name in optional_cols:
        if col_name in df.columns:
            result = result.withColumn(col_name, F.first(col_name))

    return result
```

---

## 20. XML Column Extraction (WAJIB)

### 20.1 Requirement

Beberapa table memiliki kolom yang berisi **XML data** yang perlu di-extract menjadi table terpisah.

> **⚠️ CLARIFICATION: ORDER_ITEMS CDC vs XML Extraction**
>
> | Source | Table | Description |
> |--------|-------|-------------|
> | **CDC Table** | `cdc_order_items` | Dari Oracle table ORDER_ITEMS (Section 13.2) - normalized data |
> | **XML Extraction** | `cdc_order_items_extracted` | Dari kolom ORDERS.SHIPPING_INFO XML - denormalized/legacy data |
>
> POC ini men-demonstrasikan **KEDUA** skenario untuk memvalidasi fleksibilitas arsitektur.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       XML EXTRACTION FLOW                                    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  SOURCE TABLE: ORDERS                                                │   │
│  │  ┌─────────────────────────────────────────────────────────────┐    │   │
│  │  │ ORDER_ID │ CUSTOMER_ID │ ORDER_DETAILS (XML)                │    │   │
│  │  │ 1001     │ 500         │ <items>                            │    │   │
│  │  │          │             │   <item id="1" qty="2" price="50"/>│    │   │
│  │  │          │             │   <item id="2" qty="1" price="30"/>│    │   │
│  │  │          │             │ </items>                           │    │   │
│  │  └─────────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│                            ┌─────────────────┐                             │
│                            │  XML EXTRACTOR  │                             │
│                            └────────┬────────┘                             │
│                                     │                                       │
│                    ┌────────────────┼────────────────┐                     │
│                    ▼                ▼                ▼                     │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────┐   │
│  │  ORDERS (cleaned)   │  │  ORDER_ITEMS (new)  │  │  ITEM_DETAILS   │   │
│  │  - ORDER_ID         │  │  - ITEM_ID          │  │  (if nested)    │   │
│  │  - CUSTOMER_ID      │  │  - ORDER_ID (FK)    │  │                 │   │
│  │  - (no XML col)     │  │  - QTY              │  │                 │   │
│  └─────────────────────┘  │  - PRICE            │  └─────────────────┘   │
│                           └─────────────────────┘                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 20.2 XML Column Examples

#### Example 1: Order Items dalam XML

```xml
<!-- ORDERS.ORDER_DETAILS column -->
<order_details>
  <items>
    <item product_id="101" quantity="2" unit_price="49.99" />
    <item product_id="205" quantity="1" unit_price="129.99" />
    <item product_id="307" quantity="3" unit_price="19.99" />
  </items>
  <shipping>
    <method>EXPRESS</method>
    <address>123 Main St</address>
  </shipping>
</order_details>
```

#### Example 2: Customer Addresses dalam XML

```xml
<!-- CUSTOMERS.ADDRESSES column -->
<addresses>
  <address type="billing">
    <street>123 Main St</street>
    <city>Jakarta</city>
    <zip>12345</zip>
  </address>
  <address type="shipping">
    <street>456 Oak Ave</street>
    <city>Bandung</city>
    <zip>67890</zip>
  </address>
</addresses>
```

### 20.3 Debezium Simulator - XML Handling

```json
{
  "payload": {
    "before": null,
    "after": {
      "ORDER_ID": 1001,
      "CUSTOMER_ID": 500,
      "ORDER_DATE": "2024-02-04",
      "TOTAL_AMOUNT": 189.96,
      "ORDER_DETAILS": "<order_details><items><item product_id=\"101\" quantity=\"2\" unit_price=\"49.99\" /><item product_id=\"205\" quantity=\"1\" unit_price=\"129.99\" /></items></order_details>"
    },
    "source": {
      "connector": "oracle",
      "table": "ORDERS"
    },
    "op": "c"
  }
}
```

### 20.4 PySpark XML Extraction

```python
from pyspark.sql.functions import explode, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType
import xml.etree.ElementTree as ET

class XMLExtractor:
    """
    Extract XML columns into separate DataFrames
    """

    @staticmethod
    def parse_order_details_udf(xml_string):
        """
        Parse ORDER_DETAILS XML and return list of items
        """
        if not xml_string:
            return []

        try:
            root = ET.fromstring(xml_string)
            items = []
            for item in root.findall('.//item'):
                items.append({
                    'product_id': int(item.get('product_id')),
                    'quantity': int(item.get('quantity')),
                    'unit_price': float(item.get('unit_price'))
                })
            return items
        except ET.ParseError:
            return []  # Malformed XML → empty list (or send to DLQ)

    def extract_order_items(self, orders_df):
        """
        Extract items from ORDER_DETAILS XML column

        Input:  1 ORDER with XML containing 3 items
        Output: 3 ORDER_ITEMS rows
        """
        # Register UDF
        parse_udf = udf(self.parse_order_details_udf, ArrayType(
            StructType([
                StructField("product_id", IntegerType()),
                StructField("quantity", IntegerType()),
                StructField("unit_price", DoubleType())
            ])
        ))

        # Parse XML and explode into rows
        items_df = (
            orders_df
            .withColumn("items", parse_udf(col("ORDER_DETAILS")))
            .select(
                col("ORDER_ID"),
                col("CUSTOMER_ID"),
                explode(col("items")).alias("item")
            )
            .select(
                col("ORDER_ID"),
                col("item.product_id").alias("PRODUCT_ID"),
                col("item.quantity").alias("QUANTITY"),
                col("item.unit_price").alias("UNIT_PRICE"),
                (col("item.quantity") * col("item.unit_price")).alias("SUBTOTAL")
            )
        )

        return items_df
```

#### PySpark - Full Extraction Pipeline

```python
def process_orders_with_xml(orders_stream):
    """
    Process orders stream, extract XML to separate tables
    """
    extractor = XMLExtractor()

    # 1. Extract ORDER_ITEMS from XML
    order_items_df = extractor.extract_order_items(orders_stream)

    # 2. Clean orders (remove XML column, keep metadata)
    orders_clean_df = orders_stream.drop("ORDER_DETAILS")

    # 3. Write to separate tables
    # Orders → cdc_orders
    orders_clean_df.writeStream \
        .format("jdbc") \
        .option("dbtable", "cdc_orders") \
        .start()

    # Order Items → cdc_order_items (extracted from XML)
    order_items_df.writeStream \
        .format("jdbc") \
        .option("dbtable", "cdc_order_items_extracted") \
        .start()
```

### 20.5 PyFlink XML Extraction

```python
from pyflink.datastream import MapFunction, FlatMapFunction
import xml.etree.ElementTree as ET

class XMLFlatMapper(FlatMapFunction):
    """
    FlatMap: 1 record with XML → N records (extracted items)
    """

    def flat_map(self, value):
        order_id = value.get('ORDER_ID')
        xml_content = value.get('ORDER_DETAILS')

        if not xml_content:
            return

        try:
            root = ET.fromstring(xml_content)
            for item in root.findall('.//item'):
                yield {
                    'ORDER_ID': order_id,
                    'PRODUCT_ID': int(item.get('product_id')),
                    'QUANTITY': int(item.get('quantity')),
                    'UNIT_PRICE': float(item.get('unit_price')),
                    'SUBTOTAL': int(item.get('quantity')) * float(item.get('unit_price')),
                    'EXTRACTED_FROM': 'ORDER_DETAILS_XML'
                }
        except ET.ParseError as e:
            # Send to DLQ for malformed XML
            yield {
                'ORDER_ID': order_id,
                'ERROR': str(e),
                'RAW_XML': xml_content,
                '_DLQ': True
            }

# Usage in PyFlink pipeline
orders_stream = env.add_source(solace_source)

# Extract XML → multiple records
extracted_items = orders_stream.flat_map(XMLFlatMapper())

# Filter: Normal records vs DLQ
normal_items = extracted_items.filter(lambda x: not x.get('_DLQ', False))
dlq_items = extracted_items.filter(lambda x: x.get('_DLQ', False))

# Sink to REST API
normal_items.add_sink(RestApiSink(endpoint="/api/v1/order-items"))
dlq_items.add_sink(DLQSink())
```

### 20.6 Target Schema untuk Extracted Data

> **Note:** Schema untuk `cdc_orders` dan `cdc_order_items_extracted` sudah didefinisikan di Section 9.1.
> Di bawah ini hanya schema tambahan khusus untuk XML extraction.

```sql
-- Extracted addresses (if applicable)
CREATE TABLE cdc_customer_addresses_extracted (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    address_type VARCHAR(20),           -- 'billing', 'shipping'
    street VARCHAR(200),
    city VARCHAR(100),
    zip VARCHAR(20),
    extracted_from VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 20.7 XML Extraction Configuration

```yaml
xml_extraction:
  enabled: true

  mappings:
    - source_table: ORDERS
      xml_column: ORDER_DETAILS
      target_table: cdc_order_items_extracted
      xpath: ".//item"
      attribute_mapping:
        product_id: "@product_id"
        quantity: "@quantity"
        unit_price: "@unit_price"
      parent_key: ORDER_ID

    - source_table: CUSTOMERS
      xml_column: ADDRESSES
      target_table: cdc_customer_addresses_extracted
      xpath: ".//address"
      attribute_mapping:
        address_type: "@type"
        street: "street/text()"
        city: "city/text()"
        zip: "zip/text()"
      parent_key: CUSTOMER_ID

  error_handling:
    malformed_xml: DLQ              # DLQ | SKIP | FAIL
    missing_attributes: NULL        # NULL | DEFAULT | FAIL
```

### 20.8 Data Flow dengan XML Extraction

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA FLOW WITH XML EXTRACTION                             │
│                                                                             │
│  ┌─────────────┐                                                            │
│  │  Debezium   │                                                            │
│  │  Simulator  │                                                            │
│  └──────┬──────┘                                                            │
│         │ ORDER with XML column                                             │
│         ▼                                                                   │
│  ┌─────────────┐                                                            │
│  │   Solace    │  Topic: cdc/oracle/SALES/ORDERS/insert                    │
│  └──────┬──────┘                                                            │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        CONSUMER SERVICE                              │   │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │   │
│  │  │   Parse     │────▶│   XML       │────▶│   Route     │           │   │
│  │  │   CDC Event │     │  Extractor  │     │   Records   │           │   │
│  │  └─────────────┘     └─────────────┘     └──────┬──────┘           │   │
│  │                                                  │                   │   │
│  │                           ┌──────────────────────┼──────────────┐   │   │
│  │                           ▼                      ▼              ▼   │   │
│  │                    ┌───────────┐          ┌───────────┐  ┌───────┐ │   │
│  │                    │  Orders   │          │  Items    │  │  DLQ  │ │   │
│  │                    │ (cleaned) │          │(extracted)│  │       │ │   │
│  │                    └─────┬─────┘          └─────┬─────┘  └───┬───┘ │   │
│  └──────────────────────────┼──────────────────────┼────────────┼─────┘   │
│                             ▼                      ▼            ▼         │
│                      ┌───────────┐          ┌───────────┐ ┌───────────┐  │
│                      │cdc_orders │          │cdc_order_ │ │ dlq_table │  │
│                      │           │          │items_ext  │ │           │  │
│                      └───────────┘          └───────────┘ └───────────┘  │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 20.9 XML Extraction Test Scenarios

| # | Scenario | Test Data | Expected Result | Priority |
|---|----------|-----------|-----------------|----------|
| X1 | Simple XML extraction | 1 order, 3 items in XML | 3 rows in extracted table | **WAJIB** |
| X2 | Empty XML | `<items></items>` | 0 rows extracted, no error | **WAJIB** |
| X3 | Malformed XML | Invalid XML syntax | Record sent to DLQ | **WAJIB** |
| X4 | Missing attributes | `<item qty="1"/>` (no price) | NULL for missing | **WAJIB** |
| X5 | Nested XML | Multi-level nesting | All levels extracted | **WAJIB** |
| X6 | Large XML | 100+ items in single record | All extracted, no timeout | **WAJIB** |
| X7 | NULL XML column | ORDER_DETAILS is NULL | Skip extraction, no error | **WAJIB** |
| X8 | Unicode in XML | Non-ASCII characters | Properly encoded | **WAJIB** |

### 20.10 Performance Consideration

```
┌─────────────────────────────────────────────────────────────────┐
│               XML EXTRACTION PERFORMANCE NOTES                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Factor              │ Impact           │ Mitigation            │
│  ────────────────────┼──────────────────┼────────────────────── │
│  Large XML (>1MB)    │ Memory spike     │ Streaming XML parser  │
│  Many items/record   │ Explosion ratio  │ Batch writes          │
│  Complex XPath       │ CPU intensive    │ Pre-compile XPath     │
│  Malformed XML       │ Parse failures   │ DLQ + continue        │
│                                                                 │
│  Explosion Ratio Example:                                       │
│  - 1000 orders/min with avg 5 items each                       │
│  - = 5000 extracted items/min                                   │
│  - Plan capacity accordingly!                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 21. Configuration Defaults (RESOLVED)

Semua open questions sudah di-resolve dengan default values:

| # | Question | Default Value | Rationale |
|---|----------|---------------|-----------|
| 1 | Target latency (PyFlink) | **< 5s** | Balance antara latency dan reliability |
| 2 | Solace persistence mode | **Persistent Queue** | Durability lebih penting untuk CDC |
| 3 | State backend | **In-memory** | Sufficient untuk POC 5K/min |
| 4 | DLQ retention | **7 days** | Cukup untuk investigation |

---

## 22. Implementation Checklist

### 22.1 WAJIB (Must Have)

- [ ] Basic CDC event generation (5 tables)
- [ ] Solace publish/subscribe
- [ ] PySpark consumer dengan 3-way join
- [ ] PyFlink consumer dengan enrichment
- [ ] PostgreSQL sink (JSONB untuk dynamic schema)
- [ ] REST API sink
- [ ] Checkpoint/recovery mechanism
- [ ] DLQ implementation
- [ ] All 10 failure scenarios (F1-F10)
- [ ] Dynamic schema handling (S1-S6)
- [ ] **XML column extraction (X1-X8)** ← NEW
- [ ] Basic monitoring/logging

### 22.2 OPSIONAL (Nice to Have)

- [ ] Distributed Spark cluster
- [ ] Distributed Flink cluster
- [ ] Dynamic scaling test
- [ ] 20K+ events/min stress test
- [ ] Advanced monitoring (Prometheus/Grafana)
- [ ] Auto-recovery scripts
- [ ] Auto ALTER TABLE for PostgreSQL

---

## 23. Resource Requirements

### 23.1 POC Environment (Local Development)

**Minimum Spec untuk PC dengan RAM 16GB, Storage 100GB:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    POC RESOURCE ALLOCATION (16GB RAM)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Component              │ CPU    │ Memory  │ Storage │ Notes               │
│  ───────────────────────┼────────┼─────────┼─────────┼──────────────────── │
│  Solace PubSub+         │ 1 core │ 2 GB    │ 5 GB    │ shm_size: 1g        │
│  PostgreSQL             │ 0.5    │ 1 GB    │ 10 GB   │ With indexes        │
│  Generator              │ 0.5    │ 512 MB  │ 100 MB  │ Python script       │
│  ───────────────────────┼────────┼─────────┼─────────┼──────────────────── │
│  ARCHITECTURE A ONLY:                                                       │
│  Spark Consumer         │ 2 core │ 4 GB    │ 2 GB    │ Single node         │
│  ───────────────────────┼────────┼─────────┼─────────┼──────────────────── │
│  ARCHITECTURE B ONLY:                                                       │
│  Flink Consumer         │ 1 core │ 2 GB    │ 1 GB    │ Single TaskManager  │
│  REST API (FastAPI)     │ 0.5    │ 256 MB  │ 100 MB  │ Mock server         │
│  ───────────────────────┼────────┼─────────┼─────────┼──────────────────── │
│  OS + Docker Overhead   │ 2 core │ 2 GB    │ 20 GB   │ Docker images       │
│  ───────────────────────┼────────┼─────────┼─────────┼──────────────────── │
│                                                                             │
│  TOTAL (Arch A only)    │ ~6 core│ ~10 GB  │ ~37 GB  │ ✅ Fits in 16GB    │
│  TOTAL (Arch B only)    │ ~5 core│ ~8 GB   │ ~36 GB  │ ✅ Fits in 16GB    │
│  TOTAL (Both Arch)      │ ~8 core│ ~12 GB  │ ~38 GB  │ ⚠️ Not recommended │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Hybrid Mode Resource Savings

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID MODE vs FULL CONTAINER                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Mode                 │ Generator RAM │ Total RAM (Arch A) │ Benefit        │
│  ─────────────────────┼───────────────┼────────────────────┼─────────────── │
│  Full Container       │ 512 MB        │ ~10 GB             │ Isolated       │
│  Hybrid (Local Gen)   │ ~100 MB*      │ ~9.5 GB            │ -512 MB, Debug │
│                                                                             │
│  * Generator berjalan di Python native, lebih efisien dari container        │
│                                                                             │
│  Keuntungan Hybrid Mode:                                                    │
│  • RAM lebih hemat (~500MB)                                                 │
│  • Debugging lebih mudah (IDE breakpoints)                                  │
│  • Hot reload code changes                                                  │
│  • No container rebuild untuk development                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Docker Compose Resource Limits (POC)

```yaml
# docker-compose.spark.yml - POC settings
services:
  solace:
    deploy:
      resources:
        limits:
          cpus: '1'
          memory: 2G
        reservations:
          memory: 1G
    shm_size: 1g

  postgres:
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 1G

  generator:
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M

  spark-consumer:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
    environment:
      - SPARK_DRIVER_MEMORY=2g
      - SPARK_EXECUTOR_MEMORY=1g
```

```yaml
# docker-compose.flink.yml - POC settings
services:
  flink-consumer:
    deploy:
      resources:
        limits:
          cpus: '1'
          memory: 2G
    environment:
      - FLINK_PROPERTIES=taskmanager.memory.process.size: 1600m
```

### 23.2 Rekomendasi untuk PC 16GB RAM

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    REKOMENDASI UNTUK 16GB RAM                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ✅ RECOMMENDED:                                                            │
│  • Jalankan Architecture A dan B secara TERPISAH, bukan bersamaan          │
│  • Close aplikasi lain (browser, IDE heavy) saat testing                   │
│  • Gunakan docker compose down setelah selesai testing satu arsitektur     │
│                                                                             │
│  ⚠️ JIKA INGIN JALANKAN KEDUANYA:                                          │
│  • Kurangi Spark memory ke 3GB                                             │
│  • Kurangi Solace shm_size ke 512m                                         │
│  • Monitor dengan: docker stats                                             │
│                                                                             │
│  💡 TIPS OPTIMASI:                                                          │
│  • Disable Docker Desktop GUI (gunakan CLI)                                │
│  • Set Docker memory limit di settings: 12GB max                           │
│  • Gunakan WSL2 (Windows) untuk performa lebih baik                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 23.3 Storage Breakdown (100GB Available)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STORAGE ALLOCATION (100GB)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Component                      │ Size      │ Notes                         │
│  ───────────────────────────────┼───────────┼────────────────────────────── │
│  Docker Images                  │ 15 GB     │ Solace, Spark, Flink, PG     │
│  PostgreSQL Data                │ 10 GB     │ ODS tables + indexes         │
│  Solace Persistent Store        │ 5 GB      │ Message spool                │
│  Spark Checkpoints              │ 5 GB      │ Streaming checkpoints        │
│  Flink Checkpoints              │ 3 GB      │ State snapshots              │
│  Application Logs               │ 2 GB      │ Rotate after 7 days          │
│  Source Code + Dependencies     │ 2 GB      │ Python venv, node_modules    │
│  ───────────────────────────────┼───────────┼────────────────────────────── │
│  TOTAL USED                     │ ~42 GB    │                              │
│  FREE SPACE                     │ ~58 GB    │ ✅ Plenty of headroom        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 23.4 Production Environment

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PRODUCTION RESOURCE REQUIREMENTS                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Component              │ CPU     │ Memory   │ Storage  │ Instances        │
│  ───────────────────────┼─────────┼──────────┼──────────┼───────────────── │
│  Solace PubSub+         │ 4 cores │ 8 GB     │ 100 GB   │ 1 (HA: 3)        │
│  PostgreSQL             │ 4 cores │ 16 GB    │ 500 GB   │ 1 (HA: 2)        │
│  ───────────────────────┼─────────┼──────────┼──────────┼───────────────── │
│  ARCHITECTURE A:                                                            │
│  Spark Master           │ 2 cores │ 4 GB     │ 50 GB    │ 1                │
│  Spark Worker           │ 4 cores │ 8 GB     │ 100 GB   │ 2-4              │
│  ───────────────────────┼─────────┼──────────┼──────────┼───────────────── │
│  ARCHITECTURE B:                                                            │
│  Flink JobManager       │ 2 cores │ 4 GB     │ 50 GB    │ 1 (HA: 2)        │
│  Flink TaskManager      │ 4 cores │ 8 GB     │ 100 GB   │ 2-4              │
│  ───────────────────────┼─────────┼──────────┼──────────┼───────────────── │
│                                                                             │
│  MINIMUM PRODUCTION (Single Arch):                                          │
│  • CPU: 16 cores                                                            │
│  • RAM: 48 GB                                                               │
│  • Storage: 500 GB SSD                                                      │
│                                                                             │
│  RECOMMENDED PRODUCTION (Both Arch + HA):                                   │
│  • CPU: 32+ cores                                                           │
│  • RAM: 96+ GB                                                              │
│  • Storage: 1 TB+ SSD                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 23.5 Scaling Guidelines

| Throughput | Spark Workers | Flink TaskManagers | PostgreSQL | Solace |
|------------|---------------|---------------------|------------|--------|
| 5K/min | 1 (local) | 1 | 1 GB RAM | 2 GB RAM |
| 20K/min | 2 | 2 | 4 GB RAM | 4 GB RAM |
| 50K/min | 4 | 4 | 8 GB RAM | 8 GB RAM |
| 100K+/min | 8+ | 8+ | 16+ GB RAM | Enterprise |

### 23.6 POC vs Production Comparison

| Aspect | POC (16GB PC) | Production |
|--------|---------------|------------|
| **Deployment** | Docker Compose | Kubernetes / VM |
| **Solace** | Standard (free) | Enterprise |
| **Spark Mode** | local[*] | Cluster (Standalone/YARN) |
| **Flink Mode** | Single TM | Cluster (HA) |
| **PostgreSQL** | Single instance | Primary + Replica |
| **Monitoring** | Docker logs | Prometheus + Grafana |
| **Checkpoints** | Local filesystem | S3 / HDFS |
| **Throughput** | 5K-10K/min | 50K+/min |

### 23.7 Quick Start Commands (Memory-Optimized)

```bash
# Check available resources
free -h
docker system df

# Start Architecture A only (recommended for 16GB)
docker compose -f docker/docker-compose.spark.yml up -d

# Monitor resource usage
docker stats

# Stop when done
docker compose -f docker/docker-compose.spark.yml down

# Clean up to free space
docker system prune -a
```

---

## 24. Next Steps

Setelah blueprint disetujui:

1. ✅ Finalisasi arsitektur (5 table, 5K events/min, 3-way join)
2. ✅ Configuration defaults resolved
3. Setup development environment
4. Mulai Phase 1: Infrastructure Setup
5. Parallel development Phase 2 & Phase 3/4
6. Run benchmark scenarios (Section 15)
7. **Run ALL failure scenarios (Section 17)** ← WAJIB
8. **Run ALL schema evolution scenarios (Section 19)** ← WAJIB
9. **Run ALL XML extraction scenarios (Section 20)** ← WAJIB
10. Optional: Setup distributed mode (Section 18)

---

## 25. Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-02-04 | Initial draft |
| 1.1 | 2024-02-04 | Separated PySpark & PyFlink architectures |
| 1.2 | 2024-02-04 | Added use case, 5 table simulation analysis |
| 1.3 | 2024-02-04 | Added failure scenarios (WAJIB), distributed processing (OPSIONAL) |
| 1.4 | 2024-02-04 | Added dynamic schema evolution (WAJIB), resolved config defaults |
| 1.5 | 2024-02-04 | Added XML column extraction to new table (WAJIB) |
| 1.6 | 2025-02-04 | Fixed conflicts: section numbering, ORDER_ITEMS ambiguity, PostgreSQL tables, EMPLOYEES→CUSTOMERS example, resource recommendations |
| 1.7 | 2025-02-04 | Added Hybrid Development Workflow for generator (local + container modes) |
| 2.0 | 2025-02-10 | All phases implemented. Consolidated docs: operational content moved to README.md, blueprint focused on design & specs only |
| 2.1 | 2025-02-10 | Added State Store & Bootstrap design section. Removed duplicate sections (Implementation Checklist, Next Steps). Cleaned up duplicate table schemas. Fixed cross-references |

---

*Document Version: 2.1*
*Last Updated: 2025-02-10*
*Status: IMPLEMENTED - All Phases Complete*
