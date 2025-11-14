"""Main pipeline runner for Restaurant ELT Pipeline."""

import sys
import io
import os
import traceback
from datetime import datetime

import pandas as pd
import duckdb
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient

# Load environment variables
load_dotenv()


def run_bronze_layer():
    """Run Bronze layer ingestion."""
    print("=" * 60)
    print("🔵 BRONZE LAYER - Loading Raw Data")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    conn = duckdb.connect(db_path)

    try:
        # Create bronze schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        # Load CSV files
        csv_files = {
            "raw_customers": "raw_customers.csv",
            "raw_orders": "raw_orders.csv",
            "raw_items": "raw_items.csv",
            "raw_products": "raw_products.csv",
            "raw_stores": "raw_stores.csv",
            "raw_supplies": "raw_supplies.csv",
        }

        for table_name, filename in csv_files.items():
            df = pd.read_csv(f"{csv_dir}/{filename}")
            df["loaded_at"] = datetime.now()
            conn.execute(
                f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM df"
            )
            print(f"✅ Loaded {len(df):,} rows into bronze.{table_name}")

        # Load tickets from Azure
        _load_tickets_from_azure(conn)

    finally:
        conn.close()


def _load_tickets_from_azure(conn):
    """Load tickets from Azure Blob Storage."""
    print("\n📦 Loading tickets from Azure Blob Storage...")
    sas_url = os.getenv("CONTAINER_SAS_URL", "")
    cc = ContainerClient.from_container_url(sas_url)

    blob_names = [b.name for b in cc.list_blobs() if b.name.endswith(".jsonl")]
    print(f"   Found {len(blob_names)} JSONL files: {blob_names}")

    dfs = []
    for blob_name in blob_names:
        bc = cc.get_blob_client(blob_name)
        text = bc.download_blob().readall().decode("utf-8")
        df = pd.read_json(io.StringIO(text), lines=True)
        df["source_blob"] = blob_name
        dfs.append(df)

    df_tickets = pd.concat(dfs, ignore_index=True)
    df_tickets["loaded_at"] = datetime.now()
    conn.execute(
        "CREATE OR REPLACE TABLE bronze.raw_tickets AS SELECT * FROM df_tickets"
    )
    print(f"✅ Loaded {len(df_tickets):,} rows into bronze.raw_tickets")


def run_silver_layer():
    """Run Silver layer transformations."""
    print("\n" + "=" * 60)
    print("🥈 SILVER LAYER - Cleaning & Transforming Data")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    conn = duckdb.connect(db_path)

    sql_files = [
        "customers",
        "orders",
        "items",
        "products",
        "stores",
        "supplies",
        "tickets",
    ]

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

        for sql_file in sql_files:
            with open(f"sql/silver/{sql_file}.sql", "r", encoding="utf-8") as f:
                sql = f.read()
            conn.execute(sql)
            count = conn.execute(f"SELECT COUNT(*) FROM silver.{sql_file}").fetchone()[
                0
            ]
            print(f"✅ Created silver.{sql_file} with {count:,} rows")
    finally:
        conn.close()


def run_gold_layer():
    """Run Gold layer marts."""
    print("\n" + "=" * 60)
    print("🥇 GOLD LAYER - Creating Business Marts")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    conn = duckdb.connect(db_path)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

        # Create fact_orders
        with open("sql/gold/fact_orders.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
        print(f"✅ Created gold.fact_orders with {count:,} rows")

        # Create tickets_per_order
        with open("sql/gold/tickets_per_order.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM gold.tickets_per_order").fetchone()[
            0
        ]
        print(f"✅ Created gold.tickets_per_order with {count:,} rows")

        # Create metrics
        with open("sql/gold/metrics.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        conn.execute(sql)

        # Fetch and display metrics
        _display_metrics(conn)
    finally:
        conn.close()


def _display_metrics(conn):
    """Display KPI metrics."""
    result = conn.execute("SELECT * FROM gold.metrics").fetchone()
    if result:
        aov, avg_tickets = result
        print("✅ Created gold.metrics")
        print("\n" + "=" * 60)
        print("📊 KEY PERFORMANCE INDICATORS")
        print("=" * 60)
        print(f"💰 Average Order Value (AOV): ${aov:,.2f}")
        print(f"🎫 Avg Tickets per Order: {avg_tickets:.2f}")
        print("=" * 60)


def main():
    """Run the complete ELT pipeline."""
    print("\n🚀 Starting Restaurant ELT Pipeline")
    print("=" * 60)

    try:
        run_bronze_layer()
        run_silver_layer()
        run_gold_layer()

        print("\n✅ Pipeline completed successfully!")
        print("\nDatabase location: data/warehouse.duckdb")
        print("\nTo query the database:")
        print("  import duckdb")
        print("  conn = duckdb.connect('data/warehouse.duckdb')")
        print("  print(conn.execute('SELECT * FROM gold.metrics').df())")
        print("  conn.close()")

        return 0
    except Exception as error:
        print(f"\n❌ Pipeline failed: {error}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
