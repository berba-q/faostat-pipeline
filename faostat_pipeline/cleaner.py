"""
Data cleaning utilities for FAOSTAT API responses.
Normalises column names, casts types, removes nulls, and adds metadata.
"""

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def clean_data(raw: dict[str, Any] | list[dict]) -> pd.DataFrame:
    """
    Clean a raw FAOSTAT /data/ response into a tidy DataFrame.

    Steps:
      1. Extract records from response envelope
      2. Normalise column names to snake_case
      3. Cast numeric columns to correct types
      4. Drop fully-null rows
      5. Deduplicate
      6. Add fetched_at timestamp
    """
    records = _extract_records(raw)
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = _normalize_columns(df)
    df = _cast_types(df)
    df = _drop_empty_rows(df)
    df = df.drop_duplicates()
    df["fetched_at"] = datetime.now(tz=timezone.utc).isoformat()
    return df


def _extract_records(raw: dict[str, Any] | list[dict]) -> list[dict]:
    """Pull the list of records out of the FAOSTAT response envelope."""
    if isinstance(raw, list):
        return raw
    # Common FAOSTAT response keys: "data", "Data", "items", "results"
    for key in ("data", "Data", "items", "results"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]
    # If it's a flat dict with no nested list, wrap it
    return [raw] if raw else []


def _to_snake_case(name: str) -> str:
    """Convert a column name to snake_case."""
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[\s\-]+", "_", name)
    return name.lower().strip("_")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case."""
    df.columns = [_to_snake_case(col) for col in df.columns]
    return df


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast well-known columns to their expected types."""
    year_cols = [c for c in df.columns if "year" in c]
    for col in year_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    value_cols = [c for c in df.columns if c in ("value", "val", "quantity", "amount")]
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where all non-metadata columns are null."""
    meta_cols = {"fetched_at"}
    data_cols = [c for c in df.columns if c not in meta_cols]
    return df.dropna(subset=data_cols, how="all").reset_index(drop=True)
