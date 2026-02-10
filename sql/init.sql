-- =============================================================================
-- CDC Architecture POC - PostgreSQL ODS Schema
-- =============================================================================
-- This script initializes the ODS (Operational Data Store) database
-- with all tables required for the CDC POC.
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- CDC SOURCE TABLES (Mirroring Oracle CDC data)
-- =============================================================================

-- Orders (from CDC ORDERS table)
CREATE TABLE IF NOT EXISTS cdc_orders (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(15,2),
    status VARCHAR(50),
    shipping_info TEXT,                    -- XML column for extraction demo
    source_ts TIMESTAMP,                   -- Original event timestamp
    operation VARCHAR(10),                 -- c=create, u=update, d=delete
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_orders_order_id UNIQUE (order_id, processed_at)
);

CREATE INDEX idx_cdc_orders_order_id ON cdc_orders(order_id);
CREATE INDEX idx_cdc_orders_customer_id ON cdc_orders(customer_id);
CREATE INDEX idx_cdc_orders_processed_at ON cdc_orders(processed_at);

-- Order Items (from CDC ORDER_ITEMS table)
CREATE TABLE IF NOT EXISTS cdc_order_items (
    id SERIAL PRIMARY KEY,
    item_id INT NOT NULL,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    subtotal DECIMAL(15,2),
    source_ts TIMESTAMP,
    operation VARCHAR(10),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_order_items_order_id ON cdc_order_items(order_id);
CREATE INDEX idx_cdc_order_items_product_id ON cdc_order_items(product_id);

-- Order Items Extracted (from XML parsing - Section 23)
CREATE TABLE IF NOT EXISTS cdc_order_items_extracted (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    subtotal DECIMAL(15,2),
    extracted_from VARCHAR(50) DEFAULT 'SHIPPING_INFO_XML',
    source_ts TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_order_items_ext_order_id ON cdc_order_items_extracted(order_id);

-- Customers (from CDC CUSTOMERS table) - Upsert by customer_id
CREATE TABLE IF NOT EXISTS cdc_customers (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL UNIQUE,
    name VARCHAR(100),
    email VARCHAR(100),
    tier VARCHAR(20),                      -- GOLD, SILVER, BRONZE
    region VARCHAR(50),
    created_at TIMESTAMP,
    source_ts TIMESTAMP,
    operation VARCHAR(10),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_customers_customer_id ON cdc_customers(customer_id);
CREATE INDEX idx_cdc_customers_tier ON cdc_customers(tier);

-- Products (from CDC PRODUCTS table) - Upsert by product_id
CREATE TABLE IF NOT EXISTS cdc_products (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL UNIQUE,
    name VARCHAR(200),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_qty INT,
    is_active BOOLEAN DEFAULT true,
    source_ts TIMESTAMP,
    operation VARCHAR(10),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_products_product_id ON cdc_products(product_id);
CREATE INDEX idx_cdc_products_category ON cdc_products(category);

-- =============================================================================
-- ENRICHED/AGGREGATED TABLES (Output from transformations)
-- =============================================================================

-- Enriched Orders (hasil 3-way join: orders + customers + products) - Upsert by order_id
CREATE TABLE IF NOT EXISTS cdc_orders_enriched (
    id SERIAL PRIMARY KEY,
    order_id INT UNIQUE,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_tier VARCHAR(20),
    customer_region VARCHAR(50),
    total_amount DECIMAL(15,2),
    item_count INT,
    order_date TIMESTAMP,
    status VARCHAR(50),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_orders_enriched_order_id ON cdc_orders_enriched(order_id);
CREATE INDEX idx_cdc_orders_enriched_customer_id ON cdc_orders_enriched(customer_id);
CREATE INDEX idx_cdc_orders_enriched_window ON cdc_orders_enriched(window_start, window_end);

-- Order Aggregations (per window)
CREATE TABLE IF NOT EXISTS cdc_order_aggregations (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    customer_tier VARCHAR(20),
    product_category VARCHAR(50),
    order_count INT,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(15,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_order_agg_window ON cdc_order_aggregations(window_start, window_end);

-- =============================================================================
-- OPERATIONAL TABLES
-- =============================================================================

-- Audit Log (from CDC AUDIT_LOG table)
CREATE TABLE IF NOT EXISTS cdc_audit_log (
    id SERIAL PRIMARY KEY,
    log_id INT,
    table_name VARCHAR(100),
    operation VARCHAR(10),
    record_id VARCHAR(100),
    changed_by VARCHAR(50),
    changed_at TIMESTAMP,
    event_type VARCHAR(50),
    source_ts TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_audit_log_table ON cdc_audit_log(table_name);
CREATE INDEX idx_cdc_audit_log_changed_at ON cdc_audit_log(changed_at);

-- Dead Letter Queue (for failed messages)
CREATE TABLE IF NOT EXISTS cdc_dlq (
    id SERIAL PRIMARY KEY,
    dlq_id UUID DEFAULT uuid_generate_v4(),
    original_topic VARCHAR(200),
    error_type VARCHAR(50),
    error_message TEXT,
    retry_count INT DEFAULT 0,
    payload JSONB,
    first_failure_at TIMESTAMP,
    last_failure_at TIMESTAMP,
    consumer_id VARCHAR(100),
    status VARCHAR(20) DEFAULT 'pending',  -- pending, retrying, resolved, abandoned
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_dlq_status ON cdc_dlq(status);
CREATE INDEX idx_cdc_dlq_error_type ON cdc_dlq(error_type);
CREATE INDEX idx_cdc_dlq_created_at ON cdc_dlq(created_at);

-- =============================================================================
-- DYNAMIC SCHEMA SUPPORT (JSONB tables for schema evolution)
-- =============================================================================

-- Dynamic Orders (flexible schema with JSONB)
CREATE TABLE IF NOT EXISTS cdc_orders_dynamic (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT,
    data JSONB,                            -- All dynamic/extra fields
    source_table VARCHAR(100),
    operation VARCHAR(10),
    event_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_orders_dynamic_order_id ON cdc_orders_dynamic(order_id);
CREATE INDEX idx_cdc_orders_dynamic_data ON cdc_orders_dynamic USING GIN (data);

-- =============================================================================
-- METADATA TABLES
-- =============================================================================

-- Schema Registry (track schema changes)
CREATE TABLE IF NOT EXISTS cdc_schema_registry (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    column_type VARCHAR(50),
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    CONSTRAINT uq_schema_registry UNIQUE (table_name, column_name)
);

-- Processing Checkpoints (track consumer progress)
CREATE TABLE IF NOT EXISTS cdc_checkpoints (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(100) NOT NULL,
    topic VARCHAR(200) NOT NULL,
    partition_id INT DEFAULT 0,
    last_offset BIGINT,
    last_timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_checkpoints UNIQUE (consumer_id, topic, partition_id)
);

-- Processing Metrics (for monitoring)
CREATE TABLE IF NOT EXISTS cdc_metrics (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(100),
    metric_name VARCHAR(100),
    metric_value DECIMAL(15,4),
    metric_unit VARCHAR(20),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_metrics_consumer ON cdc_metrics(consumer_id);
CREATE INDEX idx_cdc_metrics_window ON cdc_metrics(window_start, window_end);

-- =============================================================================
-- VIEWS (for reporting/querying)
-- =============================================================================

-- Latest customer data (most recent version)
CREATE OR REPLACE VIEW v_latest_customers AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    name,
    email,
    tier,
    region,
    created_at,
    processed_at
FROM cdc_customers
ORDER BY customer_id, processed_at DESC;

-- Latest product data (most recent version)
CREATE OR REPLACE VIEW v_latest_products AS
SELECT DISTINCT ON (product_id)
    product_id,
    name,
    category,
    price,
    stock_qty,
    is_active,
    processed_at
FROM cdc_products
ORDER BY product_id, processed_at DESC;

-- DLQ Summary
CREATE OR REPLACE VIEW v_dlq_summary AS
SELECT
    error_type,
    status,
    COUNT(*) as count,
    MIN(created_at) as first_occurrence,
    MAX(created_at) as last_occurrence
FROM cdc_dlq
GROUP BY error_type, status;

-- =============================================================================
-- GRANTS (for application access)
-- =============================================================================

-- Grant all privileges to cdc_user on all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cdc_user;

-- =============================================================================
-- Initial Data (optional - for testing)
-- =============================================================================

-- Insert some sample metadata
INSERT INTO cdc_schema_registry (table_name, column_name, column_type) VALUES
    ('ORDERS', 'ORDER_ID', 'NUMBER(10)'),
    ('ORDERS', 'CUSTOMER_ID', 'NUMBER(10)'),
    ('ORDERS', 'ORDER_DATE', 'TIMESTAMP'),
    ('ORDERS', 'TOTAL_AMOUNT', 'NUMBER(15,2)'),
    ('ORDERS', 'STATUS', 'VARCHAR2(20)'),
    ('ORDERS', 'SHIPPING_INFO', 'CLOB'),
    ('ORDER_ITEMS', 'ITEM_ID', 'NUMBER(10)'),
    ('ORDER_ITEMS', 'ORDER_ID', 'NUMBER(10)'),
    ('ORDER_ITEMS', 'PRODUCT_ID', 'NUMBER(10)'),
    ('ORDER_ITEMS', 'QUANTITY', 'NUMBER(5)'),
    ('ORDER_ITEMS', 'UNIT_PRICE', 'NUMBER(10,2)'),
    ('CUSTOMERS', 'CUSTOMER_ID', 'NUMBER(10)'),
    ('CUSTOMERS', 'NAME', 'VARCHAR2(100)'),
    ('CUSTOMERS', 'EMAIL', 'VARCHAR2(100)'),
    ('CUSTOMERS', 'TIER', 'VARCHAR2(20)'),
    ('CUSTOMERS', 'REGION', 'VARCHAR2(50)'),
    ('PRODUCTS', 'PRODUCT_ID', 'NUMBER(10)'),
    ('PRODUCTS', 'NAME', 'VARCHAR2(200)'),
    ('PRODUCTS', 'CATEGORY', 'VARCHAR2(50)'),
    ('PRODUCTS', 'PRICE', 'NUMBER(10,2)'),
    ('PRODUCTS', 'STOCK_QTY', 'NUMBER(10)')
ON CONFLICT (table_name, column_name) DO NOTHING;

-- =============================================================================
-- Done!
-- =============================================================================
