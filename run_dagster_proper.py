"""Proper Dagster pipeline runner using materialize with DataFrame passing."""

import sys
import os
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

from dagster import (
    asset,
    AssetExecutionContext,
    materialize,
    Definitions,
    AssetIn,
)
import pandas as pd
import duckdb
from datetime import datetime
from azure.storage.blob import ContainerClient
import io


# ============================================================================
# BRONZE LAYER ASSETS - Return DataFrames
# ============================================================================


@asset(group_name="bronze")
def raw_customers(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw customers CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_customers.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} customers")
    return df


@asset(group_name="bronze")
def raw_orders(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw orders CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_orders.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} orders")
    return df


@asset(group_name="bronze")
def raw_items(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw items CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_items.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} items")
    return df


@asset(group_name="bronze")
def raw_products(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw products CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_products.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} products")
    return df


@asset(group_name="bronze")
def raw_stores(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw stores CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_stores.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} stores")
    return df


@asset(group_name="bronze")
def raw_supplies(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw supplies CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_supplies.csv")
    df["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df)} supplies")
    return df


@asset(group_name="bronze")
def raw_tickets(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw tickets from Azure Blob Storage."""
    context.log.info("Fetching JSONL blobs from Azure...")
    sas_url = os.getenv("CONTAINER_SAS_URL", "")
    cc = ContainerClient.from_container_url(sas_url)

    blob_names = [b.name for b in cc.list_blobs() if b.name.endswith(".jsonl")]
    context.log.info(f"Found {len(blob_names)} JSONL files: {blob_names}")

    dfs = []
    for blob_name in blob_names:
        bc = cc.get_blob_client(blob_name)
        text = bc.download_blob().readall().decode("utf-8")
        df = pd.read_json(io.StringIO(text), lines=True)
        df["source_blob"] = blob_name
        dfs.append(df)

    df_tickets = pd.concat(dfs, ignore_index=True)
    df_tickets["loaded_at"] = datetime.now()
    context.log.info(f"Loaded {len(df_tickets)} tickets")
    return df_tickets


# ============================================================================
# SILVER LAYER ASSETS - Transform using DuckDB SQL
# ============================================================================


@asset(group_name="silver", ins={"raw_customers": AssetIn(key="raw_customers")})
def customers(
    context: AssetExecutionContext, raw_customers: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw customers to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        # Register DataFrame
        conn.register("raw_customers_df", raw_customers)

        # Execute transformation
        df = conn.execute(
            """
            SELECT DISTINCT
                id AS customer_id,
                name AS customer_name,
                loaded_at
            FROM raw_customers_df
            WHERE id IS NOT NULL
            ORDER BY id
        """
        ).df()

        context.log.info(f"Created silver.customers with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_orders": AssetIn(key="raw_orders")})
def orders(context: AssetExecutionContext, raw_orders: pd.DataFrame) -> pd.DataFrame:
    """Transform raw orders to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_orders_df", raw_orders)

        df = conn.execute(
            """
            SELECT
                id AS order_id,
                customer AS customer_id,
                store_id,
                CAST(ordered_at AS TIMESTAMP) AS order_ts,
                CAST(subtotal AS DECIMAL(10,2)) AS subtotal,
                CAST(tax_paid AS DECIMAL(10,2)) AS tax_paid,
                CAST(order_total AS DECIMAL(10,2)) AS order_total,
                loaded_at
            FROM raw_orders_df
            WHERE id IS NOT NULL AND customer IS NOT NULL
            ORDER BY id
        """
        ).df()

        context.log.info(f"Created silver.orders with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_items": AssetIn(key="raw_items")})
def items(context: AssetExecutionContext, raw_items: pd.DataFrame) -> pd.DataFrame:
    """Transform raw items to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_items_df", raw_items)

        df = conn.execute(
            """
            SELECT
                id AS item_id,
                order_id,
                sku AS product_sku,
                loaded_at
            FROM raw_items_df
            WHERE id IS NOT NULL
              AND order_id IS NOT NULL
              AND sku IS NOT NULL
            ORDER BY order_id, id
        """
        ).df()

        context.log.info(f"Created silver.items with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_products": AssetIn(key="raw_products")})
def products(
    context: AssetExecutionContext, raw_products: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw products to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_products_df", raw_products)

        df = conn.execute(
            """
            SELECT DISTINCT
                sku AS product_sku,
                name AS product_name,
                type AS product_type,
                CAST(price AS DECIMAL(10,2)) AS product_price,
                description AS product_description,
                loaded_at
            FROM raw_products_df
            WHERE sku IS NOT NULL
            ORDER BY sku
        """
        ).df()

        context.log.info(f"Created silver.products with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_stores": AssetIn(key="raw_stores")})
def stores(context: AssetExecutionContext, raw_stores: pd.DataFrame) -> pd.DataFrame:
    """Transform raw stores to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_stores_df", raw_stores)

        df = conn.execute(
            """
            SELECT DISTINCT
                id AS store_id,
                name AS store_name,
                CAST(opened_at AS TIMESTAMP) AS opened_at,
                CAST(tax_rate AS DECIMAL(5,4)) AS tax_rate,
                loaded_at
            FROM raw_stores_df
            WHERE id IS NOT NULL
            ORDER BY id
        """
        ).df()

        context.log.info(f"Created silver.stores with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_supplies": AssetIn(key="raw_supplies")})
def supplies(
    context: AssetExecutionContext, raw_supplies: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw supplies to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_supplies_df", raw_supplies)

        df = conn.execute(
            """
            SELECT
                id AS supply_id,
                name AS supply_name,
                CAST(cost AS DECIMAL(10,2)) AS supply_cost,
                perishable,
                sku AS product_sku,
                loaded_at
            FROM raw_supplies_df
            WHERE id IS NOT NULL AND sku IS NOT NULL
            ORDER BY id
        """
        ).df()

        context.log.info(f"Created silver.supplies with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="silver", ins={"raw_tickets": AssetIn(key="raw_tickets")})
def tickets(context: AssetExecutionContext, raw_tickets: pd.DataFrame) -> pd.DataFrame:
    """Transform raw tickets to silver layer."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("raw_tickets_df", raw_tickets)

        df = conn.execute(
            """
            SELECT
                ticket_id,
                customer_external_id AS customer_id,
                order_id,
                channel,
                priority,
                status,
                category,
                subject,
                body AS description,
                sentiment,
                CAST(sla_due_at AS TIMESTAMP) AS sla_due_at,
                CAST(first_response_at AS TIMESTAMP) AS first_response_at,
                CAST(resolved_at AS TIMESTAMP) AS resolved_at,
                CAST(updated_at AS TIMESTAMP) AS ticket_ts,
                tags,
                agent_id,
                source_blob,
                loaded_at
            FROM raw_tickets_df
            WHERE ticket_id IS NOT NULL
            ORDER BY ticket_id
        """
        ).df()

        context.log.info(f"Created silver.tickets with {len(df)} rows")
        return df
    finally:
        conn.close()


# ============================================================================
# GOLD LAYER ASSETS - Business Marts
# ============================================================================


@asset(
    group_name="gold",
    ins={"orders": AssetIn(key="orders"), "items": AssetIn(key="items")},
)
def fact_orders(
    context: AssetExecutionContext, orders: pd.DataFrame, items: pd.DataFrame
) -> pd.DataFrame:
    """Create fact_orders mart with order totals."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("orders_df", orders)
        conn.register("items_df", items)

        df = conn.execute(
            """
            SELECT
                o.order_id,
                o.customer_id,
                o.store_id,
                o.order_ts,
                DATE_TRUNC('day', o.order_ts) AS order_date,
                o.subtotal,
                o.tax_paid,
                o.order_total,
                COUNT(i.item_id) AS item_count
            FROM orders_df o
            LEFT JOIN items_df i ON i.order_id = o.order_id
            GROUP BY o.order_id, o.customer_id, o.store_id, o.order_ts, 
                     o.subtotal, o.tax_paid, o.order_total
            ORDER BY o.order_id
        """
        ).df()

        context.log.info(f"Created gold.fact_orders with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(group_name="gold", ins={"tickets": AssetIn(key="tickets")})
def tickets_per_order(
    context: AssetExecutionContext, tickets: pd.DataFrame
) -> pd.DataFrame:
    """Create tickets_per_order mart."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("tickets_df", tickets)

        df = conn.execute(
            """
            SELECT
                order_id,
                COUNT(*) AS ticket_count
            FROM tickets_df
            WHERE order_id IS NOT NULL
            GROUP BY order_id
            ORDER BY order_id
        """
        ).df()

        context.log.info(f"Created gold.tickets_per_order with {len(df)} rows")
        return df
    finally:
        conn.close()


@asset(
    group_name="gold",
    ins={
        "fact_orders": AssetIn(key="fact_orders"),
        "tickets_per_order": AssetIn(key="tickets_per_order"),
    },
)
def metrics(
    context: AssetExecutionContext,
    fact_orders: pd.DataFrame,
    tickets_per_order: pd.DataFrame,
) -> pd.DataFrame:
    """Create metrics mart with AOV and ticket metrics."""
    conn = duckdb.connect(":memory:")
    try:
        conn.register("fact_orders_df", fact_orders)
        conn.register("tickets_per_order_df", tickets_per_order)

        df = conn.execute(
            """
            WITH aov AS (
                SELECT AVG(order_total) AS average_order_value
                FROM fact_orders_df
                WHERE order_total > 0
            ),
            tickets AS (
                SELECT AVG(ticket_count) AS avg_tickets_per_order
                FROM tickets_per_order_df
            )
            SELECT
                ROUND(aov.average_order_value, 2) AS average_order_value,
                ROUND(COALESCE(tickets.avg_tickets_per_order, 0), 4) AS avg_tickets_per_order
            FROM aov
            LEFT JOIN tickets ON TRUE
        """
        ).df()

        if len(df) > 0:
            aov = df.iloc[0]["average_order_value"]
            avg_tickets = df.iloc[0]["avg_tickets_per_order"]
            context.log.info("📊 KPIs:")
            context.log.info(f"   Average Order Value (AOV): ${aov:.2f}")
            context.log.info(f"   Avg Tickets per Order: {avg_tickets:.2f}")

        return df
    finally:
        conn.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    """Run the complete ELT pipeline using proper Dagster."""
    print("\n🚀 Starting Restaurant ELT Pipeline (Proper Dagster)")
    print("=" * 60)

    # Collect all assets
    all_assets = [
        # Bronze
        raw_customers,
        raw_orders,
        raw_items,
        raw_products,
        raw_stores,
        raw_supplies,
        raw_tickets,
        # Silver
        customers,
        orders,
        items,
        products,
        stores,
        supplies,
        tickets,
        # Gold
        fact_orders,
        tickets_per_order,
        metrics,
    ]

    # Create definitions
    defs = Definitions(assets=all_assets)

    try:
        # Materialize all assets
        result = materialize(all_assets)

        if result.success:
            print("\n" + "=" * 60)
            print("✅ Pipeline completed successfully!")
            print("=" * 60)

            # Get the metrics from the result
            metrics_data = result.output_for_node("metrics")
            if metrics_data is not None and len(metrics_data) > 0:
                aov = metrics_data.iloc[0]["average_order_value"]
                avg_tickets = metrics_data.iloc[0]["avg_tickets_per_order"]
                print("\n📊 Key Performance Indicators:")
                print(f"   💰 Average Order Value (AOV): ${aov:.2f}")
                print(f"   🎫 Avg Tickets per Order: {avg_tickets:.2f}")
                print()

            return 0

        print("\n❌ Pipeline failed!")
        return 1

    except Exception as error:
        print(f"\n❌ Pipeline failed: {error}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
