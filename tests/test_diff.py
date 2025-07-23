import pandas as pd
from scripts.run_weekly_pipeline import diff_frames, normalize_dates

def _df(rows):
    return pd.DataFrame(rows)

def test_no_change():
    old = _df([{"name":"A","company":"X","status":"approved"}])
    new = _df([{"name":"A","company":"X","status":"approved"}])
    wide, long = diff_frames(old, new, tag="unit")
    assert wide.empty
    assert long.empty

def test_value_change():
    old = _df([{"name":"A","company":"X","status":"approved"}])
    new = _df([{"name":"A","company":"X","status":"pending"}])
    wide, long = diff_frames(old, new, tag="unit")
    assert wide.loc[0, "change_type"] == "value_changed"
    assert "status" in wide.loc[0, "changed_fields"]
    assert long.iloc[0].to_dict()["field"] == "status"

def test_new_record():
    old = _df([])
    new = _df([{"name":"B","company":"Y","status":"pending"}])
    wide, long = diff_frames(old, new, tag="unit")
    assert (wide["change_type"] == "new_record").all()
    assert (long["field"] == "_row").all()

def test_nan_vs_nan_ignored():
    old = _df([{"name":"C","company":"Z","status":None}])
    new = _df([{"name":"C","company":"Z","status":None}])
    wide, long = diff_frames(old, new, tag="unit")
    assert wide.empty and long.empty

def test_date_time_vs_date():
    old = _df([{"name":"D","company":"Q","date_cleared":"2024-03-10 00:00:00"}])
    new = _df([{"name":"D","company":"Q","date_cleared":"2024-03-10"}])
    # normalise before diff
    old = normalize_dates(old, ["date_cleared"])
    new = normalize_dates(new, ["date_cleared"])
    wide, long = diff_frames(old, new, tag="unit")
    assert wide.empty
