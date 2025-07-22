# scripts/utils.py

import pandas as pd

def normalize_text(v):
    """Trim + lowercase string, or return empty string for NaN."""
    return str(v).strip().lower() if pd.notna(v) else ""

def normalize_dates(df, cols=("date_submitted", "date_cleared")):
    """Standardize specified date columns to datetime.date."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df

def proper_case_status(df):
    """Ensure the 'status' column values use title case (e.g., 'Approved')."""
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.title()
    return df


def build_row_id(df):
    """Create unique row_id by normalizing name and company."""
    if "name" not in df.columns:
        df["name"] = ""
    if "company" not in df.columns:
        df["company"] = ""
    return df["name"].apply(normalize_text) + "_" + df["company"].apply(normalize_text)
