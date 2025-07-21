import pandas as pd
import os
from pathlib import Path

# Step 1: Define paths
raw_dir = Path("data/raw")
staging_dir = Path("data/staging")
staging_dir.mkdir(parents=True, exist_ok=True)

# Step 2: Find latest raw file
raw_files = list(raw_dir.glob("*.csv"))
if not raw_files:
    raise FileNotFoundError("❌ No raw files found in data/raw/")

latest_file = max(raw_files, key=os.path.getmtime)
print(f"📄 Found latest raw file: {latest_file.name}")

# Step 3: Read CSV and clean
df = pd.read_csv(latest_file)

# Step 4: Basic cleaning
df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

# Normalize status field
df["status_clean"] = df["status"].str.strip().str.title()

# Convert dates
for col in ["date_submitted", "date_cleared"]:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Drop completely empty rows
df = df.dropna(how='all')

# Step 5: Save cleaned file
output_file = staging_dir / f"Weekly_Cleaned_{pd.Timestamp.now().date()}.csv"
df.to_csv(output_file, index=False)
print(f"✅ Cleaned file saved to: {output_file}")
