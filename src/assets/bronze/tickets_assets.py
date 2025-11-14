"""Bronze layer asset for tickets JSONL from Azure Blob Storage."""

from datetime import datetime

from dagster import asset, AssetExecutionContext

from src.resources.azure import AzureBlobResource
from src.resources.warehouse import DuckDBResource


@asset(group_name="bronze")
def raw_tickets(
    context: AssetExecutionContext,
    azure_blob: AzureBlobResource,
    duckdb: DuckDBResource,
) -> None:
    """Load raw tickets from Azure Blob Storage JSONL files."""
    context.log.info("Fetching JSONL blobs from Azure...")
    blob_names = azure_blob.list_jsonl_blobs()
    context.log.info(f"Found {len(blob_names)} JSONL files: {blob_names}")

    df = azure_blob.read_all_jsonl_blobs()
    df["loaded_at"] = datetime.now()

    duckdb.write_dataframe(df, "bronze", "raw_tickets")
    context.log.info(
        f"Loaded {len(df)} tickets from {len(blob_names)} files to bronze.raw_tickets"
    )
