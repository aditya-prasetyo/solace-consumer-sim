"""
Schema Evolution Promotion — SQL Generator

Connects to PostgreSQL, reads pending columns from cdc_schema_registry,
and generates a .sql file for DBA to review and execute.

Usage:
    python sql/generate_promote_sql.py

    # Custom connection:
    POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=ods \
    POSTGRES_USER=cdc_user POSTGRES_PASSWORD=cdc_pass \
    python sql/generate_promote_sql.py

    # Custom output path:
    python sql/generate_promote_sql.py --output /path/to/promote.sql

Output:
    sql/promote_pending_<timestamp>.sql
"""

import os
import sys
import argparse
from datetime import datetime

import psycopg2

# Oracle type → PostgreSQL type
ORACLE_TO_PG_TYPE = {
    "NUMBER(1)": "BOOLEAN",
    "NUMBER(10)": "INT",
    "NUMBER(19)": "BIGINT",
    "CLOB": "TEXT",
    "TIMESTAMP": "TIMESTAMP",
}

# Oracle table → PG target table
ORACLE_TO_PG_TABLE = {
    "ORDERS": "cdc_orders",
    "ORDER_ITEMS": "cdc_order_items",
    "CUSTOMERS": "cdc_customers",
    "PRODUCTS": "cdc_products",
    "AUDIT_LOG": "cdc_audit_log",
}

# Oracle table → JSONB dynamic table (for backfill)
ORACLE_TO_DYNAMIC_TABLE = {
    "ORDERS": "cdc_orders_dynamic",
    "ORDER_ITEMS": "cdc_orders_dynamic",
}

# Oracle table → join key for backfill
ORACLE_TO_JOIN_KEY = {
    "ORDERS": "order_id",
    "ORDER_ITEMS": "order_id",
    "CUSTOMERS": "customer_id",
    "PRODUCTS": "product_id",
}


def convert_oracle_type(oracle_type: str) -> str:
    """Convert Oracle type to PostgreSQL type."""
    if oracle_type in ORACLE_TO_PG_TYPE:
        return ORACLE_TO_PG_TYPE[oracle_type]
    if oracle_type.startswith("VARCHAR2"):
        return oracle_type.replace("VARCHAR2", "VARCHAR")
    if oracle_type.startswith("NUMBER("):
        return oracle_type.replace("NUMBER", "DECIMAL")
    return "TEXT"


def pg_cast(col_ref: str, pg_type: str) -> str:
    """Generate JSONB-to-type cast expression."""
    if pg_type in ("INT", "BIGINT") or pg_type.startswith("DECIMAL"):
        return f"({col_ref})::{pg_type}"
    if pg_type == "BOOLEAN":
        return f"({col_ref})::BOOLEAN"
    if pg_type == "TIMESTAMP":
        return f"({col_ref})::TIMESTAMP"
    return col_ref  # VARCHAR/TEXT — no cast


def get_pg_table(oracle_table: str) -> str:
    """Get PG table name from Oracle table name."""
    return ORACLE_TO_PG_TABLE.get(oracle_table, f"cdc_{oracle_table.lower()}")


def ensure_schema(conn):
    """Migrate cdc_schema_registry to latest schema if needed."""
    with conn.cursor() as cur:
        # Check which columns exist
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'cdc_schema_registry'
        """)
        existing = {row[0] for row in cur.fetchall()}

    migrations = []

    # Rename old columns if present
    if "first_seen_at" in existing and "detected_at" not in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry RENAME COLUMN first_seen_at TO detected_at"
        )
    if "last_seen_at" in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry DROP COLUMN IF EXISTS last_seen_at"
        )

    # Add missing columns
    if "detected_at" not in existing and "first_seen_at" not in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry "
            "ADD COLUMN detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
    if "pg_table_name" not in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry ADD COLUMN pg_table_name VARCHAR(100)"
        )
    if "pg_column_name" not in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry ADD COLUMN pg_column_name VARCHAR(100)"
        )
    if "promoted_at" not in existing:
        migrations.append(
            "ALTER TABLE cdc_schema_registry ADD COLUMN promoted_at TIMESTAMP"
        )

    # Fix is_active default (old schema had DEFAULT true, new has DEFAULT false)
    migrations.append(
        "ALTER TABLE cdc_schema_registry ALTER COLUMN is_active SET DEFAULT false"
    )

    if migrations:
        print(f"Migrating cdc_schema_registry ({len(migrations)} changes) ...")
        with conn.cursor() as cur:
            for sql in migrations:
                try:
                    cur.execute(sql)
                except psycopg2.Error as e:
                    print(f"  Warning: {e.pgerror.strip() if e.pgerror else e}")
        conn.commit()
        print("Migration complete.")


def fetch_pending_columns(conn) -> list:
    """Fetch pending columns from cdc_schema_registry."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, table_name, column_name, column_type, detected_at
            FROM cdc_schema_registry
            WHERE is_active = false
            ORDER BY table_name, detected_at
        """)
        return cur.fetchall()


def generate_sql(pending_columns: list) -> str:
    """Generate promotion SQL from pending columns."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "-- =============================================================================",
        "-- Schema Evolution Promotion SQL",
        f"-- Generated at: {now}",
        f"-- Pending columns: {len(pending_columns)}",
        "-- =============================================================================",
        "-- REVIEW THIS FILE BEFORE EXECUTING!",
        "--",
        "-- Each column promotion is wrapped in its own BEGIN/COMMIT block.",
        "-- You can remove any block you don't want to promote yet.",
        "-- =============================================================================",
        "",
    ]

    for row_id, table_name, column_name, column_type, detected_at in pending_columns:
        pg_table = get_pg_table(table_name)
        pg_col = column_name.lower()
        pg_type = convert_oracle_type(column_type)
        dynamic_table = ORACLE_TO_DYNAMIC_TABLE.get(table_name)
        join_key = ORACLE_TO_JOIN_KEY.get(table_name)

        lines.append(f"-- -----------------------------------------------------")
        lines.append(f"-- Promote: {table_name}.{column_name} ({column_type})")
        lines.append(f"-- Detected: {detected_at}")
        lines.append(f"-- Registry ID: {row_id}")
        lines.append(f"-- -----------------------------------------------------")
        lines.append("")
        lines.append("BEGIN;")
        lines.append("")

        # Step 1: ALTER TABLE
        lines.append("-- Step 1: Add column to target table")
        lines.append(f"ALTER TABLE {pg_table} ADD COLUMN IF NOT EXISTS {pg_col} {pg_type};")
        lines.append("")

        # Step 2: UPDATE registry
        lines.append("-- Step 2: Promote in schema registry")
        lines.append("UPDATE cdc_schema_registry")
        lines.append(f"SET pg_table_name  = '{pg_table}',")
        lines.append(f"    pg_column_name = '{pg_col}',")
        lines.append(f"    is_active      = true,")
        lines.append(f"    promoted_at    = NOW()")
        lines.append(f"WHERE table_name   = '{table_name}'")
        lines.append(f"  AND column_name  = '{column_name}'")
        lines.append(f"  AND is_active    = false;")
        lines.append("")

        # Step 3: BACKFILL from dynamic table
        if dynamic_table and join_key:
            cast_expr = pg_cast(f"d.data->>'{column_name}'", pg_type)
            lines.append("-- Step 3: Backfill historical data from JSONB")
            lines.append(f"UPDATE {pg_table} t")
            lines.append(f"SET {pg_col} = {cast_expr}")
            lines.append(f"FROM {dynamic_table} d")
            lines.append(f"WHERE t.{join_key} = d.{join_key}")
            lines.append(f"  AND d.data ? '{column_name}'")
            lines.append(f"  AND t.{pg_col} IS NULL;")
        else:
            lines.append(f"-- Step 3: No dynamic table for {table_name} (backfill skipped)")

        lines.append("")
        lines.append("COMMIT;")
        lines.append("")
        lines.append("")

    # Verification queries
    lines.append("-- =============================================================================")
    lines.append("-- VERIFICATION (check results after execution)")
    lines.append("-- =============================================================================")
    lines.append("")
    lines.append("-- Recently promoted columns:")
    lines.append("SELECT table_name, column_name, pg_table_name, pg_column_name,")
    lines.append("       is_active, detected_at, promoted_at")
    lines.append("FROM cdc_schema_registry")
    lines.append("WHERE promoted_at >= NOW() - INTERVAL '1 hour'")
    lines.append("ORDER BY promoted_at DESC;")
    lines.append("")
    lines.append("-- Remaining pending columns:")
    lines.append("SELECT count(*) AS pending FROM cdc_schema_registry WHERE is_active = false;")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate schema evolution promotion SQL for DBA review"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output .sql file path (default: sql/promote_pending_<timestamp>.sql)",
    )
    args = parser.parse_args()

    # Connection from environment (same as consumer config)
    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "ods"),
        "user": os.getenv("POSTGRES_USER", "cdc_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdc_pass"),
    }

    print(f"Connecting to {conn_params['host']}:{conn_params['port']}/{conn_params['dbname']} ...")

    try:
        conn = psycopg2.connect(**conn_params)
    except psycopg2.Error as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    try:
        # Auto-migrate schema if needed
        ensure_schema(conn)

        pending = fetch_pending_columns(conn)
    finally:
        conn.close()

    if not pending:
        print("No pending columns found (is_active = false). Nothing to promote.")
        sys.exit(0)

    # Display pending columns
    print(f"\nFound {len(pending)} pending column(s):\n")
    print(f"  {'Table':<15} {'Column':<20} {'Type':<15} {'Detected'}")
    print(f"  {'-'*15} {'-'*20} {'-'*15} {'-'*20}")
    for _, table, col, ctype, detected in pending:
        print(f"  {table:<15} {col:<20} {ctype:<15} {detected}")

    # Generate SQL
    sql_content = generate_sql(pending)

    # Output path
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, f"promote_pending_{timestamp}.sql")

    with open(output_path, "w") as f:
        f.write(sql_content)

    print(f"\nGenerated: {output_path}")
    print(f"\nNext steps:")
    print(f"  1. Review:  cat {output_path}")
    print(f"  2. Execute: psql -d {conn_params['dbname']} -f {output_path}")
    print(f"  3. Notify Spark team to restart consumer")


if __name__ == "__main__":
    main()
