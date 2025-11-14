"""Bronze layer assets for CSV files."""

import os
from datetime import datetime

import pandas as pd
from dagster import asset, AssetExecutionContext

from src.resources.warehouse import DuckDBResource


@asset(group_name="bronze")
def raw_customers(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw customers CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_customers.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_customers")
    context.log.info(f"Loaded {len(df)} customers to bronze.raw_customers")


@asset(group_name="bronze")
def raw_orders(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw orders CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_orders.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_orders")
    context.log.info(f"Loaded {len(df)} orders to bronze.raw_orders")


@asset(group_name="bronze")
def raw_items(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw items CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_items.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_items")
    context.log.info(f"Loaded {len(df)} items to bronze.raw_items")


@asset(group_name="bronze")
def raw_products(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw products CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_products.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_products")
    context.log.info(f"Loaded {len(df)} products to bronze.raw_products")


@asset(group_name="bronze")
def raw_stores(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw stores CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_stores.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_stores")
    context.log.info(f"Loaded {len(df)} stores to bronze.raw_stores")


@asset(group_name="bronze")
def raw_supplies(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Load raw supplies CSV."""
    csv_dir = os.getenv("CSV_DATA_DIR", "data/csv")
    df = pd.read_csv(f"{csv_dir}/raw_supplies.csv")
    df["loaded_at"] = datetime.now()
    duckdb.write_dataframe(df, "bronze", "raw_supplies")
    context.log.info(f"Loaded {len(df)} supplies to bronze.raw_supplies")
