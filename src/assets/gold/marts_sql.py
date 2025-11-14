"""Gold layer SQL mart assets."""

from dagster import asset, AssetExecutionContext, AssetIn
from src.resources.warehouse import DuckDBResource


def read_sql_file(filename: str) -> str:
    """Read SQL file from sql/gold directory."""
    with open(f"sql/gold/{filename}", "r", encoding="utf-8") as f:
        return f.read()


@asset(
    group_name="gold",
    ins={"orders": AssetIn(key="orders"), "items": AssetIn(key="items")},
    metadata={"schema": "gold"},
)
def fact_orders(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    orders,  # pylint: disable=unused-argument,redefined-outer-name
    items,  # pylint: disable=unused-argument
) -> None:
    """Create fact_orders mart with order totals."""
    sql = read_sql_file("fact_orders.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
        context.log.info(f"Created gold.fact_orders with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="gold",
    ins={"tickets": AssetIn(key="tickets")},
    metadata={"schema": "gold"},
)
def tickets_per_order(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    tickets,  # pylint: disable=unused-argument,redefined-outer-name
) -> None:
    """Create tickets_per_order mart."""
    sql = read_sql_file("tickets_per_order.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute(sql)
        count = conn.execute("SELECT COUNT(*) FROM gold.tickets_per_order").fetchone()[
            0
        ]
        context.log.info(f"Created gold.tickets_per_order with {count} rows")
    finally:
        conn.close()


@asset(
    group_name="gold",
    ins={
        "fact_orders": AssetIn(key="fact_orders"),
        "tickets_per_order": AssetIn(key="tickets_per_order"),
    },
    metadata={"schema": "gold"},
)
def metrics(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    fact_orders,  # pylint: disable=unused-argument,redefined-outer-name
    tickets_per_order,  # pylint: disable=unused-argument,redefined-outer-name
) -> None:
    """Create metrics mart with AOV and ticket metrics."""
    sql = read_sql_file("metrics.sql")
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute(sql)

        # Fetch and log the metrics
        result = conn.execute("SELECT * FROM gold.metrics").fetchone()
        if result:
            aov, avg_tickets = result
            context.log.info("📊 KPIs:")
            context.log.info(f"   Average Order Value (AOV): ${aov:.2f}")
            context.log.info(f"   Avg Tickets per Order: {avg_tickets:.2f}")
    finally:
        conn.close()
