import os
from pathlib import Path
from datetime import datetime

import pandas as pd
from flask import (
    Flask, request, render_template, jsonify,
    send_from_directory, abort
)
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "static_exports"
EXPORT_DIR.mkdir(exist_ok=True)

app = Flask(__name__)

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
CANDIDATE_TIME_COLS = [
    "Time Stamp","Timestamp","DateTime","Datetime","Date",
    "time","datetime","ts","Time"
]

def _find_time_col(df: pd.DataFrame) -> str:
    for c in CANDIDATE_TIME_COLS:
        if c in df.columns:
            return c
    # fallback to first col that can parse
    for c in df.columns:
        try:
            pd.to_datetime(df[c])
            return c
        except Exception:
            pass
    raise ValueError("No datetime-like column found.")

def _parse_and_index(df: pd.DataFrame, prefer_col="Time Stamp") -> pd.DataFrame:
    """Parse a df's time column and return indexed by datetime (ascending)."""
    time_col = prefer_col if prefer_col in df.columns else _find_time_col(df)
    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce", infer_datetime_format=True)
    out = out.dropna(subset=[time_col]).sort_values(time_col).set_index(time_col)
    return out

def _dedupe_cols(existing: set, cols: list, prefix: str) -> list:
    """Avoid name collisions by prefixing duplicates with file stem."""
    new_cols = []
    for c in cols:
        name = str(c).strip().replace("\n", " ")
        if (name in existing) or (name.lower() in [x.lower() for x in existing]):
            name = f"{prefix}.{name}"
        while name in existing:
            name += "_1"
        existing.add(name)
        new_cols.append(name)
    return new_cols

def normalize_to_grid(df_idxed: pd.DataFrame, grid="5S") -> pd.DataFrame:
    """
    Given a df indexed by datetime, force strict grid with forward-fill.
    (No aggregation: just ffill onto the grid.)
    """
    if df_idxed.empty:
        return df_idxed
    idx = pd.date_range(df_idxed.index.min(), df_idxed.index.max(), freq=grid)
    out = df_idxed.reindex(df_idxed.index.union(idx)).sort_index().ffill().reindex(idx)
    return out

def merge_many_to_5s(dfs: list[pd.DataFrame], stems: list[str], grid="5S") -> pd.DataFrame:
    """
    - dfs: list of raw DataFrames (with a time column)
    - stems: matching list of file stems for collision-safe naming
    Returns a single DataFrame with a strict 5S 'Time Stamp' column and all series merged.
    """
    if not dfs:
        raise ValueError("No CSVs provided.")
    # Parse + index
    parsed = [_parse_and_index(df) for df in dfs]

    # Build global 5-second grid over the combined extent
    global_min = min(p.index.min() for p in parsed if not p.empty)
    global_max = max(p.index.max() for p in parsed if not p.empty)
    if pd.isna(global_min) or pd.isna(global_max):
        raise ValueError("Could not determine a valid time range from inputs.")

    grid_index = pd.date_range(global_min, global_max, freq=grid)
    merged = pd.DataFrame(index=grid_index)

    # Merge each DF onto the grid with ffill
    existing = set()
    for p, stem in zip(parsed, stems):
        if p.empty:
            continue
        p5 = normalize_to_grid(p, grid=grid)  # strict 5s, ffill
        cols = _dedupe_cols(existing, list(p5.columns), prefix=stem)
        p5.columns = cols
        merged = merged.join(p5, how="left")

    # One more ffill across any remaining NaNs
    merged = merged.ffill()

    # Final shape with single canonical 'Time Stamp'
    out = merged.reset_index().rename(columns={"index": "Time Stamp"})
    # drop any accidental extra time columns
    for c in CANDIDATE_TIME_COLS:
        if c != "Time Stamp" and c in out.columns:
            out = out.drop(columns=[c])
    return out

def plot_dataframe(df: pd.DataFrame, title="CW Loop Viewer"):
    """Small preview: first two numeric columns on Y1/Y2."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    x = df["Time Stamp"]
    numeric = df.select_dtypes(include="number").columns.tolist()
    if numeric:
        fig.add_trace(
            go.Scatter(x=x, y=df[numeric[0]], name=numeric[0], mode="lines"),
            secondary_y=False
        )
    if len(numeric) > 1:
        fig.add_trace(
            go.Scatter(x=x, y=df[numeric[1]], name=numeric[1], mode="lines"),
            secondary_y=True
        )
    fig.update_layout(title=title, xaxis_title="Time Stamp")
    return fig

# --------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------
@app.route("/")
def index():
    # Your existing template
    return render_template("index.html")

@app.route("/exports/<path:filename>")
def exports(filename):
    """Serve files saved in static_exports/"""
    return send_from_directory(EXPORT_DIR, filename, as_attachment=False)

@app.route("/upload", methods=["POST"])
def upload():
    """
    Accepts one or many CSV files from the UI.
    Merges everything onto a single 5-second 'Time Stamp' with gap-fill.
    """
    # Support both 'file' and 'files' inputs (multi-upload)
    files = request.files.getlist("files")
    if not files:
        single = request.files.get("file")
        if single:
            files = [single]

    if not files:
        abort(400, "No CSV files uploaded.")

    dataframes = []
    stems = []
    for f in files:
        if not f or f.filename.strip() == "":
            continue
        try:
            df = pd.read_csv(f)
        except Exception as e:
            abort(400, f"Failed to read CSV '{f.filename}': {e}")
        dataframes.append(df)
        stems.append(Path(f.filename).stem or "data")

    if not dataframes:
        abort(400, "No valid CSV files uploaded.")

    merged = merge_many_to_5s(dataframes, stems, grid="5S")

    # Write outputs
    csv_name = "merged_5s.csv"
    html_name = "quick_check.html"
    out_csv = EXPORT_DIR / csv_name
    out_html = EXPORT_DIR / html_name

    merged.to_csv(out_csv, index=False)

    fig = plot_dataframe(merged, title="Cadence Check (5s)")
    fig.write_html(out_html, include_plotlyjs="cdn")

    return jsonify({
        "csv": f"/exports/{csv_name}",
        "html": f"/exports/{html_name}",
        "rows": len(merged)
    })

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
if __name__ == "__main__":
    # Debug True for dev; your platform can override via FLASK_DEBUG=0 in prod.
    app.run(host="0.0.0.0", port=5000, debug=True)
