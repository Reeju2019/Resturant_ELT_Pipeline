"""Azure Blob Storage resource for Dagster."""

import io

import pandas as pd
from dagster import ConfigurableResource
from azure.storage.blob import ContainerClient


class AzureBlobResource(ConfigurableResource):
    """Azure Blob Storage resource for reading JSONL files."""

    container_sas_url: str

    def list_jsonl_blobs(self) -> list[str]:
        """List all .jsonl files in the container."""
        cc = ContainerClient.from_container_url(self.container_sas_url)
        return [b.name for b in cc.list_blobs() if b.name.endswith(".jsonl")]

    def read_jsonl_blob(self, blob_name: str) -> pd.DataFrame:
        """Read a JSONL blob and return as DataFrame."""
        cc = ContainerClient.from_container_url(self.container_sas_url)
        bc = cc.get_blob_client(blob_name)
        text = bc.download_blob().readall().decode("utf-8")
        df = pd.read_json(io.StringIO(text), lines=True)
        df["source_blob"] = blob_name
        return df

    def read_all_jsonl_blobs(self) -> pd.DataFrame:
        """Read all JSONL blobs and concatenate into single DataFrame."""
        blob_names = self.list_jsonl_blobs()
        if not blob_names:
            return pd.DataFrame()

        dfs = []
        for name in blob_names:
            df = self.read_jsonl_blob(name)
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)
