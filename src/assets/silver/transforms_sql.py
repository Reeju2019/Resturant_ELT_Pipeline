"""Silver layer SQL transformation assets."""

from dagster import asset, AssetExecutionContext, AssetIn
from src.resources.warehouse import DuckDBResource


def read_sql_file(filename: str) -> str:
    """Read SQL file from sql/silver directory."""
    with open(f"sql/silver/{filename}", "r", encoding="utf-8") as f:
        return f.read()


@asset(
    group_name="silver",
    ins={"raw_customers": AssetIn(key="raw_customers")},
    metadata={"schema": "silver"},
)
def customers(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_customers,  # pylint: disable=unused-argument
) -> None:
    """Transform raw customers to silver layer."""
    sql = read_sql_file("customers.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.customers").fetchone()[0]
        context.log.info(f"Created silver.customers with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_orders": AssetIn(key="raw_orders")},
    metadata={"schema": "silver"},
)
def orders(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_orders,  # pylint: disable=unused-argument
) -> None:
    """Transform raw orders to silver layer."""
    sql = read_sql_file("orders.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.orders").fetchone()[0]
        context.log.info(f"Created silver.orders with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_items": AssetIn(key="raw_items")},
    metadata={"schema": "silver"},
)
def items(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_items,  # pylint: disable=unused-argument
) -> None:
    """Transform raw items to silver layer."""
    sql = read_sql_file("items.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.items").fetchone()[0]
        context.log.info(f"Created silver.items with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_products": AssetIn(key="raw_products")},
    metadata={"schema": "silver"},
)
def products(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_products,  # pylint: disable=unused-argument
) -> None:
    """Transform raw products to silver layer."""
    sql = read_sql_file("products.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.products").fetchone()[0]
        context.log.info(f"Created silver.products with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_stores": AssetIn(key="raw_stores")},
    metadata={"schema": "silver"},
)
def stores(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_stores,  # pylint: disable=unused-argument
) -> None:
    """Transform raw stores to silver layer."""
    sql = read_sql_file("stores.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.stores").fetchone()[0]
        context.log.info(f"Created silver.stores with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_supplies": AssetIn(key="raw_supplies")},
    metadata={"schema": "silver"},
)
def supplies(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_supplies,  # pylint: disable=unused-argument
) -> None:
    """Transform raw supplies to silver layer."""
    sql = read_sql_file("supplies.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.supplies").fetchone()[0]
        context.log.info(f"Created silver.supplies with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="silver",
    ins={"raw_tickets": AssetIn(key="raw_tickets")},
    metadata={"schema": "silver"},
)
def tickets(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_tickets,  # pylint: disable=unused-argument
) -> None:
    """Transform raw tickets to silver layer."""
    sql = read_sql_file("tickets.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM silver.tickets").fetchone()[0]
        context.log.info(f"Created silver.tickets with {count} rows")
    finally:
        conn.close()
