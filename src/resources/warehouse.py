"""DuckDB warehouse resource and IO manager for Dagster."""

import duckdb
import pandas as pd
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    """DuckDB connection resource."""

    database_path: str

    def get_connection(self):
        """Get a DuckDB connection."""
        return duckdb.connect(self.database_path)

    def execute_query(self, query: str):
        """Execute a SQL query."""
        conn = self.get_connection()
        try:
            result = conn.execute(query).fetchall()
            return result
        finally:
            conn.close()

    def execute_df(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return as DataFrame."""
        conn = self.get_connection()
        try:
            return conn.execute(query).df()
        finally:
            conn.close()

    def write_dataframe(self, dataframe: pd.DataFrame, schema: str, table: str):
        """Write DataFrame to DuckDB table.

        Note: DuckDB references the 'dataframe' parameter by name in the SQL query.
        """
        conn = self.get_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            full_table = f"{schema}.{table}"
            # DuckDB uses variable name 'dataframe' in SQL
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM dataframe"
            )
        finally:
            conn.close()
