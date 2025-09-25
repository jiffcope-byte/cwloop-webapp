import io
import os
import re
import zipfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from flask import (
    Flask, render_template, request, send_file, abort, url_for, redirect, flash, make_response
)
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# -----------------------------------------------------------------------------
# Flask setup
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

# Configure uploads (50 MB default; override with MAX_UPLOAD_MB)
max_mb = int(os.environ.get("MAX_UPLOAD_MB", "50"))
app.config["MAX_CONTENT_LENGTH"] = max_mb * 1024 * 1024

# A tiny in-memory cache for recent exports (shown on the homepage)
RECENT: List[dict] = []

# Paths
ROOT = Path(__file__).parent.resolve()
STATIC_EXPORTS = ROOT / "static" / "exports"
STATIC_EXPORTS.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
TIME_COL_PRIORITY = [
    " Time Stamp",  # you mentioned this exact header exists
    "Time Stamp",
    "Timestamp",
    "Date_Time",
    "Datetime",
    "DateTime",
    "Date",
    "Time"
]

SEQ_PAT = re.compile(r"^(?:seq|sequence)$", re.IGNORECASE)

def _slug(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    return re.sub(r"[^A-Za-z0-9_\-]+", "", s)

def pick_time_column(df: pd.DataFrame) -> Optional[str]:
    """Pick a sensible time column (prefers known names, otherwise first datetime-like)."""
    cols = list(df.columns)

    # Prefer explicit names
    for name in TIME_COL_PRIORITY:
        if name in cols:
            return name

    # Otherwise try to infer: pick the first column that parses to many datetimes
    for c in cols:
        try:
            converted = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)
            # If at least half are valid datetimes, use it
            if converted.notna().sum() >= len(df) * 0.5:
                return c
        except Exception:
            continue

    return None

def read_csv_filestorage(fs) -> pd.DataFrame:
    """Read a werkzeug FileStorage into a DataFrame robustly."""
    fs.stream.seek(0)
    data = fs.read()
    bio = io.BytesIO(data)

    # Try a few forgiving combos
    for kwargs in (
        dict(sep=None, engine="python", on_bad_lines="skip", encoding="utf-8-sig"),
        dict(sep=None, engine="python", on_bad_lines="skip", encoding_errors="ignore"),
        dict(sep=None, engine="python", on_bad_lines="skip"),
    ):
        bio.seek(0)
        try:
            df = pd.read_csv(bio, **kwargs)
            return df
        except Exception:
            continue

    # Final attempt (let pandas guess)
    bio.seek(0)
    return pd.read_csv(bio)

def coerce_time_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame index is a datetime index using the chosen time column."""
    time_col = pick_time_column(df)
    if not time_col:
        raise ValueError("Could not determine a time column in one of the CSVs.")

    ts = pd.to_datetime(df[time_col], errors="coerce", infer_datetime_format=True)
    if ts.isna().all():
        raise ValueError(f"Timestamp column '{time_col}' could not be parsed.")

    out = df.copy()
    out.index = ts
    out.drop(columns=[time_col], inplace=True)  # keep only series columns
    out = out[~out.index.isna()]
    out.sort_index(inplace=True)
    return out

def drop_sequence_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any columns that look like 'Sequence' (case-insensitive)."""
    bad = [c for c in df.columns if SEQ_PAT.match(str(c).strip())]
    return df.drop(columns=bad, errors="ignore")

def merge_on_global_time(
    base: pd.DataFrame,
    others: List[pd.DataFrame],
    tolerance_s: int
) -> pd.DataFrame:
    """Outer-join others onto base and forward-fill small gaps."""
    df = base.copy()
    for add in others:
        df = df.join(add, how="outer")

    # Forward fill within tolerance (seconds)
    if tolerance_s > 0:
        df = df.sort_index().ffill(limit=int(tolerance_s))

    return df

def to_two_axes(df: pd.DataFrame, setpoint_col: Optional[str]) -> Tuple[List[str], List[str]]:
    """Split columns between axis 1 and axis 2 if a setpoint is present."""
    cols = list(df.columns)
    if setpoint_col and setpoint_col in cols:
        y2 = [setpoint_col]
        y1 = [c for c in cols if c != setpoint_col]
    else:
        y1, y2 = cols, []
    return y1, y2

def write_export_bundle(
    title: str,
    merged: pd.DataFrame,
    y1_cols: List[str],
    y2_cols: List[str],
) -> Tuple[Path, Path, Path]:
    """Write CSV, HTML (plotly), and ZIP into dated folder. Return paths."""
    today = datetime.now().strftime("%Y-%m-%d")
    export_dir = STATIC_EXPORTS / today / _slug(title)
    export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / "merged.csv"
    html_path = export_dir / f"{_slug(title)}.html"
    zip_path = export_dir / f"{_slug(title)}.zip"

    # CSV
    merged_out = merged.copy()
    merged_out.index.name = "Time Stamp"
    merged_out.to_csv(csv_path)

    # Plotly
    fig = make_subplots(specs=[[{"secondary_y": bool(y2_cols)}]])
    for col in y1_cols:
        if col in merged.columns:
            fig.add_trace(go.Scatter(x=merged.index, y=merged[col], name=col, mode="lines"))
    for col in y2_cols:
        if col in merged.columns:
            fig.add_trace(
                go.Scatter(x=merged.index, y=merged[col], name=col, mode="lines"),
                secondary_y=True
            )

    fig.update_layout(
        title=title,
        template="plotly_white",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        margin=dict(l=30, r=20, t=60, b=40),
    )
    fig.update_xaxes(title="Time Stamp")
    fig.update_yaxes(title="Percent", secondary_y=False)
    if y2_cols:
        fig.update_yaxes(title="Setpoint / Secondary", secondary_y=True)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(fig.to_html(full_html=True, include_plotlyjs="cdn"))

    # ZIP (CSV + HTML)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(csv_path, arcname="merged.csv")
        z.write(html_path, arcname=html_path.name)

    return csv_path, html_path, zip_path

def add_recent(title: str, csv_url: str, html_url: str, zip_url: str):
    RECENT.insert(0, dict(
        title=title,
        when=datetime.now().strftime("%b %d, %Y %I:%M %p"),
        csv_url=csv_url,
        view_url=html_url,
        zip_url=zip_url,
    ))
    # Keep last 10
    del RECENT[10:]

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", exports_list=RECENT, pushed=False, site_base=None)

@app.errorhandler(Exception)
def on_error(e):
    # Log full traceback to Render logs
    traceback.print_exc()
    # Friendly page in browser with the root message (no stack)
    msg = str(e) or e.__class__.__name__
    return make_response(
        render_template(
            "index.html",
            exports_list=RECENT,
            pushed=False,
            site_base=None,
            error=f"Processing error: {msg}"
        ),
        500
    )

@app.route("/process", methods=["POST"])
def process():
    # --- Read inputs safely ---------------------------------------------------
    original = request.files.get("original")
    if not original or original.filename == "":
        abort(400, "Original CSV is required")

    additionals = request.files.getlist("additionals") or []
    tolerance_s = int(request.form.get("tolerance", "5") or 5)
    title = request.form.get("title", "CW Loop").strip() or "CW Loop"
    y_min = float(request.form.get("ymin", "0") or 0)
    y_max = float(request.form.get("ymax", "100") or 100)
    setpoint_col = (request.form.get("setpoint_col") or "").strip()
    cutoff_raw = (request.form.get("cutoff") or "").strip()

    # --- Load CSVs ------------------------------------------------------------
    base_raw = read_csv_filestorage(original)
    base_raw = drop_sequence_columns(base_raw)
    base = coerce_time_index(base_raw)

    others: List[pd.DataFrame] = []
    for fs in additionals:
        if not fs or fs.filename == "":
            continue
        df = read_csv_filestorage(fs)
        df = drop_sequence_columns(df)
        df = coerce_time_index(df)
        others.append(df)

    # --- Optional cutoff ------------------------------------------------------
    if cutoff_raw:
        try:
            cutoff = pd.to_datetime(cutoff_raw)
            base = base[base.index <= cutoff]
            others = [d[d.index <= cutoff] for d in others]
        except Exception:
            # Don't fail just because cutoff didn't parse
            pass

    # --- Merge & axis split ---------------------------------------------------
    merged = merge_on_global_time(base, others, tolerance_s=tolerance_s)

    # Clean column names: remove device prefix up to first "."
    renamed = {}
    for c in merged.columns:
        s = str(c)
        renamed[c] = s.split(".", 1)[1] if "." in s else s
    merged.rename(columns=renamed, inplace=True)

    y1_cols, y2_cols = to_two_axes(merged, setpoint_col=setpoint_col)

    # --- Write bundle ---------------------------------------------------------
    csv_path, html_path, zip_path = write_export_bundle(title, merged, y1_cols, y2_cols)

    # --- Add to Recent and render page with links -----------------------------
    # URLs for static
    # (Render serves /static/** already)
    csv_url  = url_for("static", filename=str(csv_path.relative_to(ROOT / "static")).replace("\\", "/"))
    html_url = url_for("static", filename=str(html_path.relative_to(ROOT / "static")).replace("\\", "/"))
    zip_url  = url_for("static", filename=str(zip_path.relative_to(ROOT / "static")).replace("\\", "/"))

    add_recent(title, csv_url, html_url, zip_url)

    # Immediately send the HTML for convenience and show links on home
    resp = make_response(send_file(html_path, mimetype="text/html"))
    return resp
