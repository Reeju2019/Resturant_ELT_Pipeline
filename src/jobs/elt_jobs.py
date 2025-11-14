"""Dagster jobs for ELT pipeline."""

from dagster import define_asset_job, AssetSelection


# Full ELT pipeline job
full_elt_job = define_asset_job(
    name="full_elt_pipeline",
    description="Run complete Bronze → Silver → Gold ELT pipeline",
    selection=AssetSelection.all(),
)

# Bronze only job
bronze_job = define_asset_job(
    name="bronze_ingestion",
    description="Load raw data into Bronze layer",
    selection=AssetSelection.groups("bronze"),
)

# Silver only job
silver_job = define_asset_job(
    name="silver_transformation",
    description="Transform Bronze to Silver layer",
    selection=AssetSelection.groups("silver"),
)

# Gold only job
gold_job = define_asset_job(
    name="gold_marts",
    description="Create Gold layer marts and metrics",
    selection=AssetSelection.groups("gold"),
)
