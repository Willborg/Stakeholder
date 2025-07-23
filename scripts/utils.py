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
    """Create row_id using just Subject Name and Primary Position for clarity and stability."""

    def safe(v):
        return normalize_text(v) if pd.notna(v) else ""

    # Ensure required columns exist
    for col in ["subject name", "primary position"]:
        if col not in df.columns:
            df[col] = ""

    # Build simple row ID
    row_ids = (
        df["subject name"].apply(safe)
        + "_" +
        df["primary position"].apply(safe)
    )

    # Detect duplicates (optional safety check)
    if row_ids.duplicated().any():
        duplicates = row_ids[row_ids.duplicated()].unique()
        raise ValueError(f"❌ Duplicate row_id(s) found: {list(duplicates)}")

    return row_ids


