import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil
from scripts.utils import normalize_text, normalize_dates, proper_case_status



ROOT      = Path(__file__).resolve().parent.parent
DIFF_WIDE = ROOT / "data" / "diffs" / "wide"
LIVE_PATH = ROOT / "data" / "live" / "Stakeholder_Live_Clean.csv"
BACK_DIR  = LIVE_PATH.parent / "backups"
BACK_DIR.mkdir(exist_ok=True)


def build_row_id(df):
    return df["name"].apply(normalize_text) + "_" + df["company"].apply(normalize_text)

def prompt(row) -> str:
    print("-"*50)
    print(f"Row-ID : {row['row_id']}")
    print(f"Type   : {row['change_type']}")
    print(f"Fields : {row['changed_fields']}")
    for fld in row['changed_fields'].split(", "):
        print(f"{fld:14}: {row.get(f'{fld}_old')}  →  {row.get(f'{fld}_new')}")
    return (input("(y)es / (n)o / (s)kip rest / (o)verride  [y]: ").strip().lower() or "y")


def main():
    # newest diff
    diff_files = sorted(DIFF_WIDE.glob("Changes_*.csv"))
    if not diff_files:
        print("No diff file found."); return
    diff = pd.read_csv(diff_files[-1]).dropna(how="all")
    live = pd.read_csv(LIVE_PATH).dropna(how="all")

    if "row_id" not in live.columns:
        live["row_id"] = build_row_id(live)

    # collect approvals
    approved = []
    for _, row in diff.iterrows():
        ans = prompt(row)
        if ans == "s":
            break
        elif ans == "y":
            approved.append(row)
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

    if not approved:
        print("No rows approved."); return

    upd = pd.DataFrame(approved)
    if "row_id" not in upd.columns:
        upd["row_id"] = build_row_id(upd)

    # ensure row_id is the index on both
    if live.index.name != "row_id":
        live.set_index("row_id", inplace=True)
    if upd.index.name != "row_id":
        upd.set_index("row_id", inplace=True)

    compare_cols = ["status", "nominator", "clearance_level", "date_cleared"]

    # 1️⃣ update only rows that already exist in live
    common_idx = upd.index.intersection(live.index)
    for col in compare_cols:
        if f"{col}_new" in upd.columns:
            live.loc[common_idx, col] = upd.loc[common_idx, f"{col}_new"]

    # 2️⃣ append approved brand-new rows
    if "change_type" in upd.columns and "new_record" in upd["change_type"].values:
        new_rows = upd[upd["change_type"] == "new_record"]
        cols_map = {f"{c}_new": c for c in compare_cols + ["name", "company", "date_submitted"]}
        new_df   = new_rows.rename(columns=cols_map)[list(cols_map.values())]
        live = pd.concat([live, new_df.set_index(new_rows.index)])


    # save + backup
    bkup = BACK_DIR / f"Stakeholder_Live_Clean_backup_manual_{datetime.now():%Y%m%d_%H%M%S}.csv"
    shutil.copy(LIVE_PATH, bkup)
    live = proper_case_status(live)
    live.reset_index().to_csv(LIVE_PATH, index=False)
    print(f"✅ Live updated with approvals. Backup: {bkup}")

if __name__ == "__main__":
    main()
