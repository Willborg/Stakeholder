"""
Weekly Stakeholder Pipeline  •  Manual-default mode
---------------------------------------------------
• Cleans newest raw CSV  -> data/staging/
• Archives a copy        -> data/history/
• Creates wide + long diffs in data/diffs/
• By default **does NOT** overwrite the live file.
  Use --auto-update to skip manual approval.
"""

from datetime import datetime
from pathlib import Path
import argparse, shutil, pandas as pd
from scripts.utils import normalize_text, normalize_dates, proper_case_status



# ---------- CLI ----------
parser = argparse.ArgumentParser()
parser.add_argument(
    "--auto-update",
    action="store_true",
    help="Directly overwrite Stakeholder_Live_Clean.csv without manual approval.",
)

# ---------- Paths ----------
ROOT        = Path(__file__).resolve().parent.parent
RAW_DIR     = ROOT / "data" / "raw"
STAGING_DIR = ROOT / "data" / "staging"
HIST_DIR    = ROOT / "data" / "history"
LIVE_PATH   = ROOT / "data" / "live" / "Stakeholder_Live_Clean.csv"
DIFF_WIDE   = ROOT / "data" / "diffs" / "wide"
DIFF_LONG   = ROOT / "data" / "diffs" / "long"
for p in (STAGING_DIR, HIST_DIR, DIFF_WIDE, DIFF_LONG):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------

def build_row_id(df):
    # ensure required cols exist even for empty DataFrames in unit-tests
    for col in ("name", "company"):
        if col not in df.columns:
            df[col] = ""
    return df["name"].apply(normalize_text) + "_" + df["company"].apply(normalize_text)

def _has_cols(df, *cols) -> bool:
    """True if *all* columns exist on the DataFrame."""
    return all(c in df.columns for c in cols)

def clean_raw() -> Path:
    latest = max(RAW_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    df.columns = df.columns.str.strip().str.lower()   # Standardize names

    # ------------- Add the schema check here -------------
    EXPECTED_COLS = {"name", "company", "status", "nominator", "clearance_level", "date_submitted", "date_cleared"}
    raw_cols = set(df.columns)
    missing = EXPECTED_COLS - raw_cols
    extra   = raw_cols - EXPECTED_COLS

    if missing:
        print(f"⚠️  WARNING: Raw data is missing expected columns: {missing}")
    if extra:
        print(f"⚠️  NOTE: Raw data has extra columns not in expected schema: {extra}")

    # Optionally abort (recommended for critical fields)
    if missing:
        raise ValueError(f"Aborting: Required columns missing: {missing}")
    # -----------------------------------------------------

    df = df.dropna(how="all")
    df = normalize_dates(df)
    df = proper_case_status(df)  # <---- ADD THIS LINE RIGHT BEFORE SAVING
    out = STAGING_DIR / f"Weekly_Cleaned_{datetime.now().date()}.csv"
    df.to_csv(out, index=False)
    print(f"✅ Cleaned file saved -> {out}")
    return out

# ---------- Diff core ----------
def diff_frames(old: pd.DataFrame, new: pd.DataFrame, tag: str):
    cmp_cols = ["status", "nominator", "clearance_level", "date_cleared"]

    for d in (old, new):
        normalize_dates(d)
        for c in cmp_cols:
            if c in d:
                d[c] = d[c].apply(normalize_text)

    old["row_id"] = build_row_id(old)
    new["row_id"] = build_row_id(new)

    m = pd.merge(old, new, on="row_id", how="outer",
                 suffixes=("_old", "_new"), indicator=True)

    added   = m[m["_merge"] == "right_only"]
    removed = m[m["_merge"] == "left_only"]

    # ---------- value-change mask ----------
    mask = False
    for c in cmp_cols:
        col_old, col_new = f"{c}_old", f"{c}_new"
        if _has_cols(m, col_old, col_new):
            mask |= m[col_old] != m[col_new]
    changed = m[(m["_merge"] == "both") & mask]

    # ---------- wide ----------
    wide = pd.concat([added, removed, changed], ignore_index=True)

    def tag_row(r):
        if r["_merge"] == "right_only": return "new_record"
        if r["_merge"] == "left_only":  return "removed_record"
        return "value_changed"
    wide["change_type"] = wide.apply(tag_row, axis=1)

    def changed_fields_row(r):
        out = []
        for c in cmp_cols:
            col_old, col_new = f"{c}_old", f"{c}_new"
            if col_old not in r or col_new not in r:
                continue
            left, right = r[col_old], r[col_new]
            if pd.isna(left) and pd.isna(right):
                continue
            if left != right:
                out.append(c)
        return ", ".join(out)
    wide["changed_fields"] = wide.apply(changed_fields_row, axis=1)

    # ---------- long ----------
    long_rows = []
    for _, r in m.iterrows():
        if r["_merge"] == "both":
            for c in cmp_cols:
                col_old, col_new = f"{c}_old", f"{c}_new"
                if col_old not in r or col_new not in r:
                    continue
                left, right = r[col_old], r[col_new]
                if pd.isna(left) and pd.isna(right) or left == right:
                    continue
                long_rows.append({"row_id": r["row_id"], "field": c,
                                  "old": left, "new": right, "tag": tag})
        else:
            long_rows.append({"row_id": r["row_id"], "field": "_row",
                              "old": None if r["_merge"]=="right_only" else "present",
                              "new": "present" if r["_merge"]=="right_only" else None,
                              "tag": tag})
    long = pd.DataFrame(long_rows)
    return wide, long

# ---------- Pipeline routine ----------
def main() -> None:
    args = parser.parse_args()

    week_clean = clean_raw()
    week_df = pd.read_csv(week_clean, parse_dates=["date_submitted","date_cleared"]).dropna(how="all")
    shutil.copy(week_clean, HIST_DIR / week_clean.name)

    if not LIVE_PATH.exists():
        week_df = proper_case_status(week_df)   # <--- Add here
        week_df.to_csv(LIVE_PATH, index=False)
        print("ℹ️  No live file found. Seeded live dataset and exiting.")
        return


    live_df = pd.read_csv(LIVE_PATH, parse_dates=["date_submitted","date_cleared"]).dropna(how="all")

    wide_wl, long_wl = diff_frames(live_df.copy(), week_df.copy(), "weekly_vs_live")

    hist_files = sorted(HIST_DIR.glob("Weekly_Cleaned_*.csv"))
    if len(hist_files) >= 2:
        last_week_df = pd.read_csv(hist_files[-2], parse_dates=["date_submitted","date_cleared"]).dropna(how="all")
        wide_ww, long_ww = diff_frames(last_week_df.copy(), week_df.copy(), "week_to_week")
        wide_out = pd.concat([wide_wl, wide_ww])
        long_out = pd.concat([long_wl, long_ww])
    else:
        wide_out, long_out = wide_wl, long_wl

    today = datetime.now().date().isoformat()
    wide_out.to_csv(DIFF_WIDE / f"Changes_{today}.csv",  index=False)
    long_out.to_csv(DIFF_LONG / f"ChangesLong_{today}.csv", index=False)
    print(f"✅ Wide diff  → {DIFF_WIDE / f'Changes_{today}.csv'}")
    print(f"✅ Long diff  → {DIFF_LONG / f'ChangesLong_{today}.csv'}")

    # ----- auto-update live (optional) -----
    if args.auto_update:
        backup_dir = LIVE_PATH.parent / "backups"; backup_dir.mkdir(exist_ok=True)
        bkup = backup_dir / f"Stakeholder_Live_Clean_backup_{datetime.now():%Y%m%d_%H%M%S}.csv"
        shutil.copy(LIVE_PATH, bkup)
        week_df = proper_case_status(week_df)
        week_df.to_csv(LIVE_PATH, index=False)
        print(f"🔄 Live dataset updated. Backup stored at {bkup}")
    else:
        print("ℹ️  Live file NOT updated (manual approval mode).")

# ---------- entry point ----------
if __name__ == "__main__":
    main()
