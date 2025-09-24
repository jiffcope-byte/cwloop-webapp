import os
import io
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from flask import (
    Flask, render_template, request, send_file, redirect, url_for,
    abort, current_app
)
from werkzeug.utils import secure_filename

# -----------------------------------------------------------------------------
# Flask setup
# -----------------------------------------------------------------------------
app = Flask(__name__)

# Where we publish finished artifacts (served by Flask's static files)
EXPORTS_ROOT = Path(app.root_path) / "static" / "exports"
EXPORTS_ROOT.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _strip_prefix(col: str) -> str:
    """
    Remove device/file prefixes left of a dot. Keeps the last segment.
    e.g. "Plant Pumps.Active CW Flow Setpoint" -> "Active CW Flow Setpoint"
         "Cooling Tower.CT-2 LLC Panel Status" -> "CT-2 LLC Panel Status"
    """
    col = col.strip()
    parts = col.split(".")
    return parts[-1].strip() if len(parts) > 1 else col


def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [_strip_prefix(c) for c in cleaned.columns]
    return cleaned


def _try_parse_datetime(series: pd.Series) -> pd.Series | None:
    try:
        s = pd.to_datetime(series, errors="coerce", utc=False)
        if s.notna().sum() >= max(3, int(0.05 * len(s))):  # plausibly real datetimes
            return s
    except Exception:
        pass
    return None


CANDIDATE_TS_NAMES = [
    " Time Stamp",  # observed in your files (leading space)
    "Time Stamp",
    "Timestamp",
    "DateTime",
    "Datetime",
    "date",
    "time",
]


def _extract_timestamp(df: pd.DataFrame) -> pd.Series:
    """
    Pick the best timestamp column:
      1) try known names,
      2) else first datetime-like column,
      3) else first column coerced to datetime.
    Returns a timezone-naive pandas datetime64[ns] series.
    """
    # 1) Known names
    for name in CANDIDATE_TS_NAMES:
        if name in df.columns:
            s = _try_parse_datetime(df[name])
            if s is not None:
                return pd.to_datetime(s).dt.tz_localize(None)

    # 2) First datetime-like
    for c in df.columns:
        if c in CANDIDATE_TS_NAMES:
            continue
        s = _try_parse_datetime(df[c])
        if s is not None:
            return pd.to_datetime(s).dt.tz_localize(None)

    # 3) Fallback to first column
    first = df.columns[0]
    s = pd.to_datetime(df[first], errors="coerce")
    return pd.to_datetime(s).dt.tz_localize(None)


def _read_csv_from_file(fstorage) -> pd.DataFrame:
    # Use filename only for diagnostics—reading from stream
    content = fstorage.read()
    fstorage.stream.seek(0)
    df = pd.read_csv(io.BytesIO(content), encoding="utf-8", engine="python")
    return df


def _align_series_to(orig_index: pd.DatetimeIndex,
                     s: pd.Series,
                     tolerance_sec: int) -> pd.Series:
    """
    Align a series to the original timestamps by nearest within tolerance.
    Then forward-fill remaining gaps. Returns a series indexed by orig_index.
    """
    s = s.dropna()
    if s.empty:
        return pd.Series(index=orig_index, dtype="float64")

    s = s.copy()
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index, errors="coerce")
    s = s[~s.index.isna()].sort_index()

    aligned = s.reindex(
        orig_index, method="nearest",
        tolerance=pd.Timedelta(seconds=max(0, tolerance_sec))
    )
    return aligned.ffill()


def _build_plot(df: pd.DataFrame,
                title: str,
                y1_min: float | None,
                y1_max: float | None,
                setpoint_col: str | None) -> go.Figure:
    fig = go.Figure()

    cols = [c for c in df.columns if c.lower() != "time stamp"]
    # y2 if specified and present after cleaning
    y2_col = None
    if setpoint_col:
        # Clean setpoint name same way we cleaned headers
        sp_clean = _strip_prefix(setpoint_col)
        for c in cols:
            if _strip_prefix(c).lower() == sp_clean.lower():
                y2_col = c
                break

    for c in cols:
        if c == y2_col:
            continue
        fig.add_trace(
            go.Scatter(
                x=df["Time Stamp"], y=df[c], mode="lines",
                name=_strip_prefix(c), yaxis="y"
            )
        )

    if y2_col:
        fig.add_trace(
            go.Scatter(
                x=df["Time Stamp"], y=df[y2_col], mode="lines",
                name=_strip_prefix(y2_col), yaxis="y2"
            )
        )

    fig.update_layout(
        title=title or "CW Loop",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=50, r=30, t=60, b=40),
        xaxis=dict(title="Time Stamp"),
        yaxis=dict(title="Percent"),
        template="plotly"
    )
    if y1_min is not None or y1_max is not None:
        rng = [
            y1_min if y1_min is not None else None,
            y1_max if y1_max is not None else None,
        ]
        fig.update_yaxes(range=rng)

    if y2_col:
        fig.update_layout(
            yaxis2=dict(
                title="Setpoint",
                overlaying="y",
                side="right",
                showgrid=False
            )
        )

    return fig


def _write_export_bundle(df: pd.DataFrame, title: str) -> tuple[str, Path]:
    """
    Write merged.csv and viewer.html under:
      static/exports/YYYY-MM-DD/<slug>/
    Returns (public_url_to_viewer, folder_path).
    """
    day = datetime.now().strftime("%Y-%m-%d")
    slug = datetime.now().strftime("%H%M%S") + "-" + re.sub(r"[^a-zA-Z0-9\-]+", "-", title or "CW Loop").strip("-")
    out_dir = EXPORTS_ROOT / day / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "merged.csv"
    html_path = out_dir / "viewer.html"

    df.to_csv(csv_path, index=False)

    fig = _build_plot(
        df,
        title=title or "CW Loop",
        y1_min=None, y1_max=None,
        setpoint_col=None  # already plotted as y2 in _build_plot call above if needed
    )
    # Rebuild figure with y2 if present: handled outside when fig built in /process

    # Actually rebuild with the right axes (done in /process below) – we'll pass a fig object.
    # Here we just save from the fig passed back by /process.

    # Save done in /process; we keep this function for path creation only
    return (f"/static/exports/{day}/{slug}/viewer.html", out_dir)


def _write_index_html(folder: Path, title: str, rows: list[tuple[str, str]]) -> None:
    """
    Write a simple index listing.
    rows: list of (display_text, href)
    """
    html = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{title}</title>",
        "<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}"
        "a{color:#08f;text-decoration:none} a:hover{text-decoration:underline}"
        "ul{line-height:1.8}</style></head><body>",
        f"<h2>{title}</h2><ul>"
    ]
    for text, href in rows:
        html.append(f"<li><a href='{href}'>{text}</a></li>")
    html += ["</ul></body></html>"]
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "index.html").write_text("\n".join(html), encoding="utf-8")


def _rebuild_indexes() -> None:
    """
    Build:
      /static/exports/index.html  -> year-month-day folders
      /static/exports/<day>/index.html -> individual exports
    """
    days = sorted([p for p in EXPORTS_ROOT.iterdir() if p.is_dir()])
    day_rows: list[tuple[str, str]] = []
    for d in days:
        # Make per-day index
        items = sorted([p for p in d.iterdir() if p.is_dir()])
        rows = []
        for it in items:
            viewer = it / "viewer.html"
            csv = it / "merged.csv"
            if viewer.exists():
                label = f"{d.name} / {it.name}"
                rows.append((f"{label} – View", f"/static/exports/{d.name}/{it.name}/viewer.html"))
                if csv.exists():
                    rows.append((f"{label} – CSV", f"/static/exports/{d.name}/{it.name}/merged.csv"))
        if rows:
            _write_index_html(d, f"Exports for {d.name}", rows)
            day_rows.append((d.name, f"/static/exports/{d.name}/"))

    # Top level index
    if day_rows:
        # Newest first
        day_rows = sorted(day_rows, key=lambda x: x[0], reverse=True)
    _write_index_html(EXPORTS_ROOT, "CW Loop Exports", day_rows)


def _form_value(name: str, default: str = "") -> str:
    v = request.form.get(name, default)
    return v.strip() if isinstance(v, str) else default


def _get_original_file():
    """
    Robustly obtain the original CSV file regardless of field name.
    """
    files = request.files
    orig = (
        files.get("original_csv")
        or files.get("original")
        or (next(iter(files.values()), None) if files else None)
    )
    if orig and orig.filename and orig.filename.strip():
        return orig
    return None


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    # The template shows the form & (optionally) a list of recent exports
    # If you want the server to inject recent exports, you can glob here and pass into template.
    # Your existing templates/index.html already expects a list called "exports" (optional).
    # We'll assemble a few most-recent viewer links:
    exports = []
    if EXPORTS_ROOT.exists():
        days = sorted([d for d in EXPORTS_ROOT.iterdir() if d.is_dir()], reverse=True)
        for d in days[:7]:
            for bundle in sorted([b for b in d.iterdir() if b.is_dir()], reverse=True)[:10]:
                v = bundle / "viewer.html"
                if v.exists():
                    exports.append({
                        "title": f"{d.name}/{bundle.name}",
                        "view_url": f"/static/exports/{d.name}/{bundle.name}/viewer.html",
                        "csv_url": f"/static/exports/{d.name}/{bundle.name}/merged.csv",
                    })
    return render_template("index.html", exports=exports)


@app.route("/process", methods=["POST"])
def process():
    # ---- get files robustly
    orig_file = _get_original_file()
    if not orig_file:
        got_keys = list(request.files.keys())
        return f"Original CSV is required (got file fields: {got_keys})", 400

    add_files = request.files.getlist("additional_csvs") or []

    # ---- form parameters
    try:
        tol_seconds = int(_form_value("tolerance", "5"))
    except Exception:
        tol_seconds = 5

    title = _form_value("title", "CW Loop")
    y1_min = _form_value("y1_min", "")
    y1_max = _form_value("y1_max", "")
    y1_min = float(y1_min) if y1_min != "" else None
    y1_max = float(y1_max) if y1_max != "" else None

    setpoint_col = _form_value("setpoint", "") or None
    cutoff_str = _form_value("cutoff", "")

    # ---- read original df
    try:
        orig_df = _read_csv_from_file(orig_file)
    except Exception as e:
        return f"Failed reading original CSV: {e}", 400

    ts = _extract_timestamp(orig_df)
    if ts.isna().all():
        return "Could not find a valid timestamp column in the original CSV.", 400

    orig_df["Time Stamp"] = ts.dt.tz_localize(None)
    orig_df = orig_df.sort_values("Time Stamp").reset_index(drop=True)

    # Optional cutoff
    if cutoff_str:
        try:
            cutoff = pd.to_datetime(cutoff_str)
            orig_df = orig_df[orig_df["Time Stamp"] >= cutoff].reset_index(drop=True)
        except Exception:
            pass

    # Build the base with original's non-time columns
    base = orig_df.copy()
    # Keep all columns except those that look like sequence or duplicate time columns
    # We'll clean names afterwards
    # Align index
    base_index = pd.to_datetime(base["Time Stamp"])
    base_index = base_index.dt.tz_localize(None)
    base.set_index("Time Stamp", inplace=True)

    # Clean original's headers (except index)
    base = _clean_headers(base)
    # We'll keep these and then add aligned series from additional files

    # ---- ingest additional files
    for f in add_files:
        if not f or not f.filename.strip():
            continue
        try:
            df = _read_csv_from_file(f)
        except Exception:
            continue

        ts_add = _extract_timestamp(df)
        df["Time Stamp"] = ts_add.dt.tz_localize(None)
        df = df.sort_values("Time Stamp").dropna(subset=["Time Stamp"])

        df = _clean_headers(df)
        df = df.set_index("Time Stamp")

        # For each column, align onto original timestamps
        for c in df.columns:
            s = df[c]
            aligned = _align_series_to(base_index, s, tol_seconds)
            base[c] = aligned

    # Reset index to have "Time Stamp" column
    merged = base.reset_index().rename(columns={"index": "Time Stamp"})

    # ---- final clean: remove entirely empty columns
    keep_cols = ["Time Stamp"] + [c for c in merged.columns if c != "Time Stamp" and merged[c].notna().any()]
    merged = merged[keep_cols]

    # ---- plot with y2 if requested
    fig = _build_plot(
        merged,
        title=title,
        y1_min=y1_min,
        y1_max=y1_max,
        setpoint_col=setpoint_col
    )

    # ---- write output bundle
    day = datetime.now().strftime("%Y-%m-%d")
    slug = datetime.now().strftime("%H%M%S") + "-" + re.sub(r"[^a-zA-Z0-9\-]+", "-", title or "CW Loop").strip("-")
    out_dir = EXPORTS_ROOT / day / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "merged.csv"
    html_path = out_dir / "viewer.html"

    merged.to_csv(csv_path, index=False)
    fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)

    # Update indexes
    _rebuild_indexes()

    # Redirect to the viewer
    return redirect(f"/static/exports/{day}/{slug}/viewer.html")


@app.route("/rebuild-indexes", methods=["GET", "POST"])
def rebuild_indexes():
    _rebuild_indexes()
    # On GET, bounce home so you can see the button works; on POST return JSON-ish text
    if request.method == "GET":
        return redirect(url_for("home"))
    return {"status": "ok", "message": "indexes rebuilt"}


@app.route("/exports")
def exports_landing():
    """Convenience: jump straight to the static listing."""
    # ensure exists
    _rebuild_indexes()
    return redirect("/static/exports/")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # For local testing
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=True)
