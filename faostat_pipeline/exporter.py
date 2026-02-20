"""
Export cleaned DataFrames to CSV and/or Parquet files.
"""

import os
from pathlib import Path

import pandas as pd


def export(
    df: pd.DataFrame,
    domain_code: str,
    output_dir: str | Path = "./output",
    to_csv: bool = True,
    to_parquet: bool = True,
    parquet_compression: str = "snappy",
) -> dict[str, Path]:
    """
    Save a cleaned DataFrame to disk as CSV and/or Parquet.

    Files are written to:
      {output_dir}/{domain_code}/data.csv
      {output_dir}/{domain_code}/data.parquet

    Returns a dict of {"csv": path, "parquet": path} for written files.
    """
    if df.empty:
        raise ValueError(f"DataFrame for domain '{domain_code}' is empty — nothing to export.")

    domain_dir = Path(output_dir) / domain_code
    domain_dir.mkdir(parents=True, exist_ok=True)

    written: dict[str, Path] = {}

    if to_csv:
        csv_path = domain_dir / "data.csv"
        df.to_csv(csv_path, index=False)
        written["csv"] = csv_path

    if to_parquet:
        parquet_path = domain_dir / "data.parquet"
        df.to_parquet(parquet_path, index=False, compression=parquet_compression)
        written["parquet"] = parquet_path

    return written


def export_partitioned(
    df: pd.DataFrame,
    domain_code: str,
    partition_col: str = "year",
    output_dir: str | Path = "./output",
    parquet_compression: str = "snappy",
) -> list[Path]:
    """
    Save a large DataFrame partitioned by a column (e.g. year) as Parquet.

    Files are written to:
      {output_dir}/{domain_code}/{partition_col}={value}/data.parquet

    Useful for very large domains. Returns list of written file paths.
    """
    if df.empty:
        raise ValueError(f"DataFrame for domain '{domain_code}' is empty — nothing to export.")

    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' not found in DataFrame.")

    base_dir = Path(output_dir) / domain_code
    written: list[Path] = []

    for value, group in df.groupby(partition_col):
        part_dir = base_dir / f"{partition_col}={value}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / "data.parquet"
        group.to_parquet(path, index=False, compression=parquet_compression)
        written.append(path)

    return written
