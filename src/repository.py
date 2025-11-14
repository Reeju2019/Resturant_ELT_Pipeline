"""Dagster repository definition for Restaurant ELT Pipeline."""

import os
from dotenv import load_dotenv
from dagster import Definitions, load_assets_from_modules

# Load environment variables
load_dotenv()

# Import resources
from src.resources.warehouse import DuckDBResource
from src.resources.azure import AzureBlobResource

# Import assets
from src.assets.bronze import csv_assets, tickets_assets
from src.assets.silver import transforms_sql
from src.assets.gold import marts_sql

# Import jobs and schedules
from src.jobs.elt_jobs import full_elt_job, bronze_job, silver_job, gold_job
from src.schedules.schedules import daily_elt_schedule

# Load all assets
bronze_assets = load_assets_from_modules([csv_assets, tickets_assets])
silver_assets = load_assets_from_modules([transforms_sql])
gold_assets = load_assets_from_modules([marts_sql])

all_assets = [*bronze_assets, *silver_assets, *gold_assets]

# Define resources
resources = {
    "duckdb": DuckDBResource(
        database_path=os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    ),
    "azure_blob": AzureBlobResource(
        container_sas_url=os.getenv("CONTAINER_SAS_URL", "")
    ),
}

# Create Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=[full_elt_job, bronze_job, silver_job, gold_job],
    schedules=[daily_elt_schedule],
    resources=resources,
)
