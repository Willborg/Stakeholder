# Enhanced CLI version of manual_approver.py with improvements 1-8
import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil
from scripts.utils import normalize_text, normalize_dates, proper_case_status, build_row_id
import argparse

DATE_FIELDS = [
    "date case created",
    "date suitability decision",
    "date clearance completed"
]


# ---------- Paths ----------
ROOT      = Path(__file__).resolve().parent.parent
DIFF_WIDE = ROOT / "data" / "diffs" / "wide"
LIVE_PATH = ROOT / "data" / "live" / "Stakeholder_Live_Clean.csv"
BACK_DIR  = LIVE_PATH.parent / "backups"
APPROVED_DIR = ROOT / "data" / "diffs" / "approved"
LOG_PATH = ROOT / "logs" / "manual_updates.log"

for p in (BACK_DIR, APPROVED_DIR, LOG_PATH.parent):
    p.mkdir(parents=True, exist_ok=True)

# ---------- CLI args ----------
parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="Run full process without saving any files")
args = parser.parse_args()

# ---------- Prompt helper ----------
def prompt(row) -> str:
    print("\n" + "-"*50)
    print(f"Row-ID : {row['row_id']}")
    print(f"Type   : {row['change_type']}")
    print(f"Fields : {row['changed_fields']}")
    for fld in row['changed_fields'].split(", "):
        print(f"{fld:14}: {row.get(f'{fld}_old')}  →  {row.get(f'{fld}_new')}")
    return (input("(y)es / (n)o / (s)kip rest / (o)verride  [y]: ").strip().lower() or "y")

# ---------- Main ----------
def main():
    diff_files = sorted(DIFF_WIDE.glob("Changes_*.csv"))
    if not diff_files:
        print("❌ No diff file found."); return
    diff = pd.read_csv(diff_files[-1]).dropna(how="all")
    live = pd.read_csv(LIVE_PATH).dropna(how="all")

    if "row_id" not in live.columns:
        live["row_id"] = build_row_id(live)

    approved = []
    counts = {"approved": 0, "manual": 0, "skipped": 0}

    for _, row in diff.iterrows():
        ans = prompt(row)
        if ans == "s":
            counts["skipped"] += 1
            break
        elif ans == "y":
            approved.append(row)
            counts["approved"] += 1
        elif ans == "o":
            print("Manual override. Leave blank to keep proposed value.")
            new_row = row.copy()
            for field in row["changed_fields"].split(", "):
                old_val = row.get(f"{field}_old", "")
                proposed_val = row.get(f"{field}_new", "")
                new_val = input(f"  {field} [{proposed_val}]: ").strip()
                if new_val:
                    new_row[f"{field}_new"] = new_val
            approved.append(new_row)
            counts["manual"] += 1

    if not approved:
        print("⚠️  No rows approved."); return

    upd = pd.DataFrame(approved)
    if "row_id" not in upd.columns:
        upd["row_id"] = build_row_id(upd)

    live.set_index("row_id", inplace=True)
    upd.set_index("row_id", inplace=True)

    compare_cols = [
    "date case created",
    "case status",
    "subject name",
    "employee type",
    "primary position",
    "sector",
    "region",
    "nominee personal email address",
    "requestor name",
    "cisa nominator / sponsor email address",
    "clearance type",
    "clearance status",
    "date suitability decision",
    "suitability decision",
    "date clearance completed"
]


    required_cols = [
    "date case created",
    "case status",
    "subject name",
    "employee type",
    "primary position",
    "sector",
    "region",
    "nominee personal email address",
    "requestor name",
    "cisa nominator / sponsor email address",
    "clearance type",
    "clearance status",
    "date suitability decision",
    "suitability decision",
    "date clearance completed"
]


    live = normalize_dates(live, DATE_FIELDS)
    upd = normalize_dates(upd, DATE_FIELDS)


    if missing := set(required_cols) - set(live.columns):
        raise ValueError(f"❌ Live data is missing columns: {missing}")

    common_idx = upd.index.intersection(live.index)
    for col in compare_cols:
        if f"{col}_new" in upd.columns:
            live.loc[common_idx, col] = upd.loc[common_idx, f"{col}_new"]

    if "change_type" in upd.columns and "new_record" in upd["change_type"].values:
        new_rows = upd[upd["change_type"] == "new_record"]
        cols_map = {f"{c}_new": c for c in compare_cols}
        new_df = new_rows.rename(columns=cols_map)[list(cols_map.values())]
        live = pd.concat([live, new_df.set_index(new_rows.index)])

    live = proper_case_status(live)

    if args.dry_run:
        print("ℹ️  Dry run complete. No files written.")
    else:
        approved_path = APPROVED_DIR / f"Approved_Changes_{datetime.now().date()}.csv"
        upd.reset_index().to_csv(approved_path, index=False)
        with open(LOG_PATH, "a") as log:
            for rid in upd.index:
                log.write(f"[{datetime.now()}] Row updated: {rid}\n")

        bkup = BACK_DIR / f"Stakeholder_Live_Clean_backup_manual_{datetime.now():%Y%m%d_%H%M%S}.csv"
        shutil.copy(LIVE_PATH, bkup)
        live.reset_index().to_csv(LIVE_PATH, index=False)
        print(f"✅ Live updated with approvals. Backup: {bkup}")
        print(f"📄 Approved entries saved to: {approved_path}")

    print("\nSummary:")
    print(f"  ✅ Approved: {counts['approved']}")
    print(f"  ✍️  Manual overrides: {counts['manual']}")
    print(f"  ⏭️  Skipped: {counts['skipped']}")

if __name__ == "__main__":
    main()
