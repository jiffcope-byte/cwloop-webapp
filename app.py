import os
import io
import re
import json
import zipfile
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

import pandas as pd
import plotly.graph_objects as go
from flask import Flask, render_template, request, send_file, abort, url_for

# ------------------------
# Configuration
# ------------------------
APP_TITLE = "Trend Merge & Viewer"
STATIC_EXPORTS = Path("static/exports")           # served by Flask or your static site
RECENT_JSON = STATIC_EXPORTS / "_recent.json"     # log of recent exports for the homepage
MAX_RECENTS = 50                                  # how many exports to remember in the list

# If you also host these same files from a Render **Static Site**, put that base URL here.
# If left blank, app will use this Flask app's /static/... URLs.
STATIC_SITE_BASE = os.getenv("STATIC_SITE_BASE", "").rstrip("/")

app = Flask(__name__)


# ------------------------
# Helpers
# ------------------------
def slugify(text: str) -> str:
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[\s_-]+", "-", text)

def now_strings():
    now = datetime.utcnow()
    return now.strftime("%Y-%m-%d"), now.strftime("%H%M%S")

def read_csv_best(file_storage) -> pd.DataFrame:
    """Read a CSV, try a few common encodings and separators."""
    raw = file_storage.read()
    for sep in [",", ";", "\t", "|"]:
        for enc in ["utf-8-sig", "utf-8", "cp1252"]:
            try:
                df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=sep)
                return df
            except Exception:
                continue
    # last resort
    return pd.read_csv(io.BytesIO(raw), encoding_errors="ignore")

def find_timestamp(df: pd.DataFrame) -> pd.Series:
    """
    Return a pd.Series of the best timestamp for the DF and its column name.
    Tries in order:
    - "Time Stamp" (with/without space), "Timestamp", "Date Time", "Date", "Time"
    - Special case: first column all 1970 -> use second column if it parses
    """
    candidates = [c for c in df.columns]

    # Specific favorites first
    preferred = [
        "Time Stamp", " Time Stamp", "Timestamp", "Date Time",
        "Datetime", "Date", "Time", "time", "datetime"
    ] + candidates

    seen = set()
    for col in preferred:
        if col in df.columns and col not in seen:
            seen.add(col)
            s = pd.to_datetime(df[col], errors="coerce")
            if s.notna().sum() > 1:
                return s, col

    # 1970 bug: if first col looks epoch-zero and second parses, use second
    if len(df.columns) >= 2:
        first = pd.to_datetime(df.iloc[:, 0], errors="coerce")
        second = pd.to_datetime(df.iloc[:, 1], errors="coerce")
        if (first.dt.year == 1970).sum() > 0 and second.notna().sum() > 1:
            return second, df.columns[1]

    # fallback: try to_datetime on index
    idx = pd.to_datetime(df.index, errors="coerce")
    if idx.notna().sum() > 1:
        return idx, None

    raise ValueError("No valid timestamp column found.")

SEQ_COL_RE = re.compile(
    r"^(seq(uence)?(\s*number)?)$|^index$|^row$|^sample$|^record$|^unnamed: \d+$",
    re.IGNORECASE
)

def strip_device_prefixes(name: str) -> str:
    # drop "Device." prefix
    if "." in name:
        return name.split(".", 1)[1].strip()
    return name

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    # drop sequence-like columns
    drop = [c for c in df.columns if SEQ_COL_RE.match(str(c).strip())]
    df = df.drop(columns=drop, errors="ignore")

    # remove columns that are entirely NaN
    df = df.loc[:, df.notna().any(axis=0)]

    # strip device prefixes
    rename = {c: strip_device_prefixes(str(c)) for c in df.columns}
    df = df.rename(columns=rename)
    return df

def coerce_numeric(df: pd.DataFrame, exclude=None):
    exclude = set(exclude or [])
    for c in df.columns:
        if c in exclude:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def merge_onto_original(original: pd.DataFrame, others: list[pd.DataFrame], tolerance_s: int) -> pd.DataFrame:
    """
    Merge 'others' onto 'original' using asof within tolerance.
    """
    merged = original.copy()

    for dfi in others:
        suffix_cols = [c for c in dfi.columns if c != "Time Stamp"]
        if not len(suffix_cols):
            continue
        # asof-merge each column separately for clarity
        for col in suffix_cols:
            tmp = pd.merge_asof(
                left=merged[["Time Stamp"]],
                right=dfi[["Time Stamp", col]].sort_values("Time Stamp"),
                on="Time Stamp",
                tolerance=pd.Timedelta(seconds=tolerance_s),
                direction="backward"
            )
            merged[col] = tmp[col]

    # forward fill to cover small gaps
    value_cols = [c for c in merged.columns if c != "Time Stamp"]
    merged[value_cols] = merged[value_cols].ffill()
    return merged

def build_plot(df: pd.DataFrame, title: str, setpoint_col: str | None, y1_min: float, y1_max: float):
    fig = go.Figure()
    cols = [c for c in df.columns if c != "Time Stamp"]
    cols_sorted = sorted(cols, key=str.lower)

    # Axis setup
    fig.update_layout(
        title=title,
        xaxis_title="Time Stamp",
        yaxis_title="Percent",
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
    )
    if y1_min is not None or y1_max is not None:
        fig.update_yaxes(range=[y1_min, y1_max] if y1_min is not None and y1_max is not None else None)

    # y2 for setpoint if present
    y2_used = False
    for col in cols_sorted:
        if setpoint_col and col.strip().lower() == setpoint_col.strip().lower():
            fig.add_trace(go.Scatter(
                x=df["Time Stamp"], y=df[col], mode="lines", name=col, yaxis="y2"
            ))
            y2_used = True
        else:
            fig.add_trace(go.Scatter(
                x=df["Time Stamp"], y=df[col], mode="lines", name=col
            ))

    if y2_used:
        fig.update_layout(
            yaxis2=dict(title="Setpoint", overlaying="y", side="right")
        )
    return fig


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_static_index_pages():
    """Rebuild top-level and per-day indexes."""
    ensure_dir(STATIC_EXPORTS)
    days = sorted([p for p in STATIC_EXPORTS.iterdir() if p.is_dir() and re.match(r"\d{4}-\d{2}-\d{2}", p.name)], reverse=True)

    # top-level index.html
    lines = [
        "<!doctype html><meta charset='utf-8'>",
        f"<title>Exports – {APP_TITLE}</title>",
        "<h1>Exports</h1><ul>"
    ]
    for d in days:
        lines.append(f"<li><a href='{quote(d.name)}/index.html'>{d.name}</a></li>")
    lines.append("</ul>")
    (STATIC_EXPORTS / "index.html").write_text("\n".join(lines), encoding="utf-8")

    # per-day index.html
    for d in days:
        items = []
        for sub in sorted(d.iterdir()):
            if sub.is_dir():
                title = sub.name.replace("-", " ")
                html_file = sub / "viewer.html"
                csv_file = sub / "merged.csv"
                if html_file.exists() or csv_file.exists():
                    items.append((sub.name, html_file.exists(), csv_file.exists()))

        html_lines = [
            "<!doctype html><meta charset='utf-8'>",
            f"<title>{d.name} – {APP_TITLE}</title>",
            f"<h1>Exports – {d.name}</h1><ul>"
        ]
        for name, has_html, has_csv in items:
            links = []
            if has_html:
                links.append(f"<a href='{quote(name)}/viewer.html'>View</a>")
            if has_csv:
                links.append(f"<a href='{quote(name)}/merged.csv' download>CSV</a>")
            html_lines.append(f"<li>{name.replace('-', ' ')} — {' | '.join(links)}</li>")
        html_lines.append("</ul>")
        (d / "index.html").write_text("\n".join(html_lines), encoding="utf-8")

def add_recent_entry(dt_day: str, slug: str, title: str, html_rel: str, csv_rel: str):
    ensure_dir(STATIC_EXPORTS)
    recs = []
    if RECENT_JSON.exists():
        try:
            recs = json.loads(RECENT_JSON.read_text(encoding="utf-8"))
        except Exception:
            recs = []
    recs.insert(0, {
        "day": dt_day,
        "slug": slug,
        "title": title,
        "html": html_rel,
        "csv": csv_rel,
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    })
    recs = recs[:MAX_RECENTS]
    RECENT_JSON.write_text(json.dumps(recs, indent=2), encoding="utf-8")


# ------------------------
# Routes
# ------------------------
@app.route("/")
def index():
    # read recent list if present
    recents = []
    if RECENT_JSON.exists():
        try:
            recents = json.loads(RECENT_JSON.read_text(encoding="utf-8"))
        except Exception:
            recents = []

    # convert relative static links to either Flask /static or STATIC_SITE_BASE
    def to_abs(p: str) -> str:
        rel = f"/static/exports/{p}"
        return (STATIC_SITE_BASE + rel) if STATIC_SITE_BASE else rel

    for r in recents:
        r["html_abs"] = to_abs(r["html"])
        r["csv_abs"] = to_abs(r["csv"])
        r["day_abs"] = to_abs(f"{r['day']}/index.html")

    return render_template("index.html", app_title=APP_TITLE, recents=recents)

@app.route("/process", methods=["POST"])
def process():
    # ------------------------
    # Validate input
    # ------------------------
    if "original" not in request.files or request.files["original"].filename == "":
        abort(400, "Original CSV is required")
    original_file = request.files["original"]

    others_files = request.files.getlist("others")
    tolerance = request.form.get("tolerance", "5")
    title = request.form.get("title", "CW Loop").strip() or "CW Loop"
    y1_min = request.form.get("y1_min", "")
    y1_max = request.form.get("y1_max", "")
    setpoint_col = request.form.get("setpoint", "").strip()
    cutoff_str = request.form.get("cutoff", "").strip()

    try:
        tol_s = int(tolerance)
    except Exception:
        tol_s = 5

    y1_min_val = None
    y1_max_val = None
    try:
        y1_min_val = float(y1_min) if y1_min != "" else None
    except Exception:
        pass
    try:
        y1_max_val = float(y1_max) if y1_max != "" else None
    except Exception:
        pass

    # ------------------------
    # Read & clean data
    # ------------------------
    orig_df = read_csv_best(original_file)
    ts, ts_colname = find_timestamp(orig_df)
    orig_df.insert(0, "Time Stamp", ts)
    if ts_colname is not None and ts_colname in orig_df.columns:
        orig_df = orig_df.drop(columns=[ts_colname])

    # basic cleanup
    orig_df = clean_columns(orig_df)
    orig_df = orig_df.drop_duplicates(subset=["Time Stamp"]).sort_values("Time Stamp")

    # optional cutoff
    if cutoff_str:
        try:
            cutoff_dt = pd.to_datetime(cutoff_str, errors="coerce")
            if pd.notna(cutoff_dt):
                orig_df = orig_df[orig_df["Time Stamp"] >= cutoff_dt]
        except Exception:
            pass

    # Other CSVs
    others = []
    for f in others_files:
        if not f or f.filename == "":
            continue
        df = read_csv_best(f)
        try:
            ts2, ts2_name = find_timestamp(df)
        except Exception:
            continue
        df.insert(0, "Time Stamp", ts2)
        if ts2_name and ts2_name in df.columns:
            df = df.drop(columns=[ts2_name])
        df = clean_columns(df)
        df = df.drop_duplicates(subset=["Time Stamp"]).sort_values("Time Stamp")
        if cutoff_str:
            try:
                cutoff_dt = pd.to_datetime(cutoff_str, errors="coerce")
                if pd.notna(cutoff_dt):
                    df = df[df["Time Stamp"] >= cutoff_dt]
            except Exception:
                pass
        others.append(df)

    # numeric coerce (exclude Time Stamp)
    orig_df = coerce_numeric(orig_df, exclude=["Time Stamp"])
    others = [coerce_numeric(df, exclude=["Time Stamp"]) for df in others]

    # ------------------------
    # Merge all
    # ------------------------
    merged = merge_onto_original(orig_df, others, tolerance_s=tol_s)

    # ------------------------
    # Plot
    # ------------------------
    fig = build_plot(merged, title, setpoint_col if setpoint_col else None, y1_min_val, y1_max_val)
    html_str = fig.to_html(full_html=True, include_plotlyjs="cdn")

    # ------------------------
    # Save under static/exports
    # ------------------------
    day, hhmmss = now_strings()
    day_dir = STATIC_EXPORTS / day
    ensure_dir(day_dir)
    slug = f"{hhmmss}_{slugify(title)}"
    run_dir = day_dir / slug
    ensure_dir(run_dir)

    csv_path = run_dir / "merged.csv"
    html_path = run_dir / "viewer.html"

    # write files
    merged.to_csv(csv_path, index=False)
    html_path.write_text(html_str, encoding="utf-8")

    # log for recents
    html_rel = f"{day}/{slug}/viewer.html"
    csv_rel = f"{day}/{slug}/merged.csv"
    add_recent_entry(day, slug, title, html_rel, csv_rel)

    # rebuild browseable indexes
    write_static_index_pages()

    # Build absolute links (for the confirmation page)
    def abs_link(p: str) -> str:
        rel = f"/static/exports/{p}"
        return (STATIC_SITE_BASE + rel) if STATIC_SITE_BASE else rel

    html_abs = abs_link(html_rel)
    csv_abs = abs_link(csv_rel)
    day_abs = abs_link(f"{day}/index.html")

    # ------------------------
    # Also return a ZIP (HTML + CSV)
    # ------------------------
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("viewer.html", html_str.encode("utf-8"))
        zf.writestr("merged.csv", merged.to_csv(index=False).encode("utf-8"))
    mem.seek(0)

    # Render a small confirmation page with links + also send the ZIP
    # We can’t stream two responses; so we prefer ZIP download (primary) and show links on the page afterwards.
    # To keep UX: we serve the HTML page that has the links, with a button to download the ZIP.
    return render_template(
        "done.html",
        app_title=APP_TITLE,
        title=title,
        html_abs=html_abs,
        csv_abs=csv_abs,
        day_abs=day_abs,
        zip_name=f"{slug}.zip",
    ), 200, {
        # Keep page display; the ZIP is available via a button on the page (done.html).
        # If you want auto-download, you can add JS in done.html to fetch a /download_zip route.
    }


# Optional: if you want a direct ZIP download route, uncomment and add a "Download ZIP" button in done.html
# @app.route("/download/<day>/<slug>.zip")
# def download_zip(day, slug):
#     run_dir = STATIC_EXPORTS / day / slug
#     if not run_dir.exists():
#         abort(404)
#     csv_path = run_dir / "merged.csv"
#     html_path = run_dir / "viewer.html"
#     if not (csv_path.exists() and html_path.exists()):
#         abort(404)
#     mem = io.BytesIO()
#     with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
#         zf.write(html_path, "viewer.html")
#         zf.write(csv_path, "merged.csv")
#     mem.seek(0)
#     return send_file(mem, as_attachment=True, download_name=f"{slug}.zip", mimetype="application/zip")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=False)
