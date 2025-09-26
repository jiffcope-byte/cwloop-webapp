# app.py
import io
import os
import re
import json
import csv
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from flask import (
    Flask, request, render_template, send_file,
    abort, url_for, redirect, jsonify
)
from werkzeug.utils import secure_filename

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# -----------------------------------------------------------------------------
# Flask setup
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
STATIC_EXPORTS = BASE_DIR / "static" / "exports"
STATIC_EXPORTS.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, static_folder="static", template_folder="templates")

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def _today_str(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d")

def _ts_for_filename(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%H%M%S")

def _clean_colname(name: str) -> str:
    """Remove device prefix 'Device.Point' -> 'Point'."""
    if not isinstance(name, str):
        return name
    if "." in name:
        return name.split(".", 1)[1]
    return name

def _likely_time_col(cols):
    """Pick best timestamp column from a list of names."""
    candidates = [
        " Time Stamp", "Time Stamp", "Timestamp", "TimeStamp", "time", "Time"
    ]
    lowered = {c.lower(): c for c in cols}
    for c in candidates:
        for col in cols:
            if col.strip().lower() == c.strip().lower():
                return col
    # fallback: first column
    return cols[0]

def read_csv_to_df(file_storage) -> pd.DataFrame:
    """Read an uploaded CSV (Windows/Excel friendly), parse dates."""
    content = file_storage.read()
    file_storage.stream.seek(0)

    # Try multiple encodings
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc)
            break
        except Exception:
            df = None
    if df is None:
        raise ValueError("Unable to read CSV (encoding issue).")

    # Choose a time column then parse
    tcol = _likely_time_col(list(df.columns))
    # Some CSVs put timestamps in the second column; try that if needed
    try:
        ts = pd.to_datetime(df[tcol], errors="coerce")
        if ts.notna().sum() == 0 and len(df.columns) >= 2:
            tcol2 = df.columns[1]
            ts2 = pd.to_datetime(df[tcol2], errors="coerce")
            if ts2.notna().sum() > 0:
                tcol = tcol2
                ts = ts2
    except Exception:
        if len(df.columns) >= 2:
            tcol = df.columns[1]
            ts = pd.to_datetime(df[tcol], errors="coerce")
        else:
            raise

    df = df.copy()
    df.index = ts
    df.drop(columns=[tcol], inplace=True, errors="ignore")
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()

    # Drop entirely NA columns, clean names, and ignore "Sequence"
    df = df.dropna(axis=1, how="all")
    df.columns = [_clean_colname(c) for c in df.columns]
    df = df[[c for c in df.columns if str(c).strip().lower() != "sequence"]]
    return df

def align_to_reference(ref_index: pd.DatetimeIndex, df: pd.DataFrame, tolerance_s=5) -> pd.DataFrame:
    """Nearest align to reference timestamps within tolerance, then forward-fill."""
    if df.empty:
        return pd.DataFrame(index=ref_index)
    left = pd.DataFrame(index=ref_index).reset_index().rename(columns={"index": "ref_ts"})
    right = df.reset_index().rename(columns={"index": "ts"})
    # Merge asof with nearest match
    merged = pd.merge_asof(
        left.sort_values("ref_ts"),
        right.sort_values("ts"),
        left_on="ref_ts",
        right_on="ts",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=int(tolerance_s))
    )
    merged = merged.set_index("ref_ts")
    merged.drop(columns=["ts"], inplace=True, errors="ignore")
    # Forward fill where we didn't get a match
    merged = merged.ffill()
    merged.index.name = None
    return merged

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_browse_index(root_dir: Path, site_title="Exports"):
    """Write /static/exports/index.html and per-date indexes."""
    root_dir = Path(root_dir)
    dates = sorted([p for p in root_dir.iterdir() if p.is_dir()])
    # Top-level index
    top = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{site_title}</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial;margin:24px;background:#0f1116;color:#e6e6e6}a{color:#8bd5ff;text-decoration:none}a:hover{text-decoration:underline}",
        "h1,h2{font-weight:600}ul{line-height:1.8}</style></head><body>",
        f"<h1>{site_title}</h1>",
        "<ul>"
    ]
    for d in dates:
        rel = f"{d.name}/index.html"
        top.append(f"<li><a href='{rel}'>{d.name}</a></li>")
    top += ["</ul>", "</body></html>"]
    (root_dir / "index.html").write_text("\n".join(top), encoding="utf-8")

    # Per-date indexes
    for d in dates:
        items = sorted([p for p in d.iterdir() if p.is_file()])
        lines = [
            "<!doctype html><html><head><meta charset='utf-8'>",
            f"<title>{site_title} — {d.name}</title>",
            "<style>body{font-family:system-ui,Segoe UI,Arial;margin:24px;background:#0f1116;color:#e6e6e6}a{color:#8bd5ff;text-decoration:none}a:hover{text-decoration:underline}",
            "h1,h2{font-weight:600}ul{line-height:1.8}</style></head><body>",
            f"<h1>{d.name}</h1>",
            "<p><a href='../index.html'>⬅ Back</a></p>",
            "<ul>"
        ]
        for f in items:
            lines.append(f"<li><a href='{f.name}'>{f.name}</a></li>")
        lines += ["</ul>", "</body></html>"]
        (d / "index.html").write_text("\n".join(lines), encoding="utf-8")

def recent_exports(limit=12):
    """Return list of (title, date, html_rel_url, csv_rel_url)."""
    results = []
    if not STATIC_EXPORTS.exists():
        return results
    for d in sorted(STATIC_EXPORTS.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.html"), reverse=True):
            stem = f.stem
            csv_path = f.with_suffix(".csv")
            results.append({
                "date": d.name,
                "title": stem,
                "html_url": f"/static/exports/{d.name}/{f.name}",
                "csv_url": f"/static/exports/{d.name}/{csv_path.name}" if csv_path.exists() else ""
            })
            if len(results) >= limit:
                return results
    return results

# -----------------------------------------------------------------------------
# Plotly builder (unified hover)
# -----------------------------------------------------------------------------
def build_plot(merged: pd.DataFrame, title: str, y2_cols=None, y1_min=None, y1_max=None):
    y2_cols = y2_cols or []
    has_y2 = any(col in merged.columns for col in y2_cols)
    fig = make_subplots(specs=[[{"secondary_y": has_y2}]])

    # Left axis
    for col in merged.columns:
        if col in y2_cols:
            continue
        fig.add_trace(
            go.Scatter(
                x=merged.index, y=merged[col], name=col, mode="lines",
                hovertemplate="%{fullData.name}: %{y:.2f}<extra></extra>"
            ),
            secondary_y=False
        )

    # Right axis
    for col in y2_cols:
        if col in merged.columns:
            fig.add_trace(
                go.Scatter(
                    x=merged.index, y=merged[col], name=col, mode="lines",
                    hovertemplate="%{fullData.name}: %{y:.2f}<extra></extra>"
                ),
                secondary_y=True
            )

    fig.update_layout(
        title=title,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=30, r=20, t=60, b=40),
        hovermode="x unified",
        hoverdistance=0,
        hoverlabel=dict(namelength=-1)
    )
    fig.update_xaxes(
        title="Time Stamp",
        showspikes=True, spikemode="across", spikesnap="cursor", spikethickness=1
    )
    fig.update_yaxes(title="Percent", secondary_y=False, showspikes=True, spikemode="across")
    if has_y2:
        fig.update_yaxes(title="Setpoint / Secondary", secondary_y=True, showspikes=True, spikemode="across")

    if y1_min is not None or y1_max is not None:
        rng = [y1_min if y1_min is not None else None, y1_max if y1_max is not None else None]
        fig.update_yaxes(range=rng, secondary_y=False)

    return fig

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    # Recent export cards for the UI
    exports = recent_exports(10)
    return render_template("index.html", exports_list=exports)

@app.route("/process", methods=["POST"])
def process():
    # ---- use the original dark-mode form names ----
    if "original" not in request.files:
        abort(400, description="Original CSV is required")
    orig_file = request.files.get("original")
    if not orig_file or orig_file.filename.strip() == "":
        abort(400, description="Original CSV is required")

    # Form inputs (names from your template)
    tolerance_s = int(request.form.get("tolerance", "5") or 5)
    title = (request.form.get("title") or "CW Loop").strip()
    y1_min = request.form.get("ymin")
    y1_max = request.form.get("ymax")
    y1_min = float(y1_min) if y1_min not in (None, "",) else None
    y1_max = float(y1_max) if y1_max not in (None, "",) else None
    setpoint_col = (request.form.get("setpoint_col") or "").strip()
    cutoff_ts = (request.form.get("cutoff") or "").strip()
    if cutoff_ts:
        try:
            cutoff_ts = pd.to_datetime(cutoff_ts)
        except Exception:
            cutoff_ts = None
    else:
        cutoff_ts = None

    # Load original (reference) CSV
    ref_df = read_csv_to_df(orig_file)

    # Apply cutoff
    if cutoff_ts is not None:
        ref_df = ref_df.loc[ref_df.index <= cutoff_ts]

    # Prepare merged frame anchored to the original timestamps
    ref_index = ref_df.index
    merged = pd.DataFrame(index=ref_index)

    # Include *only* real process columns from original (Sequence already removed)
    for col in ref_df.columns:
        merged[col] = ref_df[col]

    # Load & align additional CSVs
    add_files = request.files.getlist("additionals")
    for f in add_files:
        if not f or not f.filename:
            continue
        df = read_csv_to_df(f)
        if cutoff_ts is not None:
            df = df.loc[df.index <= cutoff_ts]
        aligned = align_to_reference(ref_index, df, tolerance_s)
        # Overlay columns into merged (clean names already done in reader)
        for col in aligned.columns:
            out_col = col
            k = 2
            while out_col in merged.columns:
                out_col = f"{col} ({k})"
                k += 1
            merged[out_col] = aligned[col]

    # Secondary axis series (setpoint) if present in merged
    y2_cols = []
    if setpoint_col:
        ci = {c.lower(): c for c in merged.columns}
        match = ci.get(setpoint_col.lower(), None)
        if match:
            y2_cols = [match]

    # Build figure (unified hover)
    fig = build_plot(merged, title=title, y2_cols=y2_cols, y1_min=y1_min, y1_max=y1_max)

    # Save outputs
    day_dir = ensure_dir(STATIC_EXPORTS / _today_str())
    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", title).strip("_") or "CW_Loop"
    stem = f"{stem}_{_ts_for_filename()}"
    csv_path = day_dir / f"{stem}.csv"
    html_path = day_dir / f"{stem}.html"

    # Write CSV
    merged.to_csv(csv_path, index_label="Time Stamp")

    # Write HTML
    fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)

    # Update browse indexes
    write_browse_index(STATIC_EXPORTS, site_title="CW Loop Exports")

    # Build links for the UI
    html_rel = f"/static/exports/{day_dir.name}/{html_path.name}"
    csv_rel = f"/static/exports/{day_dir.name}/{csv_path.name}"

    # Render homepage with the new item at the top (no redirects needed)
    exports = recent_exports(10)
    return render_template(
        "index.html",
        exports_list=exports,
        pushed=True,
        latest_title=stem,
        latest_html_url=html_rel,
        latest_csv_url=csv_rel
    )

@app.route("/rebuild_indexes", methods=["POST", "GET"])
def rebuild_indexes():
    """Re-scan /static/exports and rebuild index pages."""
    write_browse_index(STATIC_EXPORTS, site_title="CW Loop Exports")
    if request.method == "GET":
        return redirect(url_for("index"))
    return jsonify({"ok": True})

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # For local dev
    app.run(host="0.0.0.0", port=5000, debug=True)
