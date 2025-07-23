import pandas as pd
from pathlib import Path
from datetime import datetime

# Setup paths
staging_dir = Path("data/staging")
live_path = Path("data/live/Stakeholder_Live_Clean.csv")
diffs_dir = Path("data/diffs")
diffs_dir.mkdir(parents=True, exist_ok=True)

# Find latest weekly cleaned file
weekly_files = list(staging_dir.glob("Weekly_Cleaned_*.csv"))
if not weekly_files:
    raise FileNotFoundError("❌ No cleaned weekly file found in data/staging/")
weekly_file = max(weekly_files, key=lambda f: f.stat().st_mtime)
print(f"📄 Using weekly file: {weekly_file.name}")

# Load datasets
weekly = pd.read_csv(weekly_file, parse_dates=["date_submitted", "date_cleared"])
live = pd.read_csv(live_path, parse_dates=["date_submitted", "date_cleared"])
print(f"📁 Live records: {len(live)}, Weekly records: {len(weekly)}")

# Normalize text fields
def normalize_text(val):
    return str(val).strip().lower() if pd.notna(val) else ""

text_cols = ["name", "company", "status", "nominator", "clearance_level"]
for col in text_cols:
    weekly[col] = weekly[col].apply(normalize_text)
    live[col] = live[col].apply(normalize_text)

# Add row_id for matching
weekly["row_id"] = weekly["name"] + "*" + weekly["company"] + "*" + weekly["date_submitted"].astype(str)
live["row_id"]   = live["name"]   + "*" + live["company"]   + "*" + live["date_submitted"].astype(str)

# Merge live and weekly
merged = pd.merge(
    live,
    weekly,
    on="row_id",
    how="inner",
    suffixes=("_live", "_weekly"),
    indicator=False
)

print("\n🔍 Merge outcome:")
print(f"Records compared: {len(merged)}")

# Compare only meaningful fields
comparison_fields = ["status", "nominator", "clearance_level", "date_cleared"]
changed_rows = []

for _, row in merged.iterrows():
    changed_fields = []
    for field in comparison_fields:
        val_live = row.get(f"{field}_live")
        val_weekly = row.get(f"{field}_weekly")
        if pd.isna(val_live) and pd.isna(val_weekly):
            continue
        if val_live != val_weekly:
            changed_fields.append(field)
    if changed_fields:
        row["changed_fields"] = ", ".join(changed_fields)
        changed_rows.append(row)

# Save output
if changed_rows:
    diffs_df = pd.DataFrame(changed_rows)
    print("\n🔍 Sample differences:")
    print(diffs_df[[f"{f}_live" for f in comparison_fields] + [f"{f}_weekly" for f in comparison_fields] + ["changed_fields"]].head())
    print(f"\n✅ Differences found: {len(diffs_df)}")

    date_str = datetime.now().date().isoformat()
    diff_path = diffs_dir / f"Changes_{date_str}.csv"
    diffs_df.to_csv(diff_path, index=False)
    print(f"📝 Saved to: {diff_path}")
else:
    print("✅ No differences found — nothing to save.")
