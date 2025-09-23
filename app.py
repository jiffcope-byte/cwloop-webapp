import os
import io
import re
import json
import base64
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import plotly.graph_objects as go
from flask import Flask, render_template, request, send_file, jsonify

import requests

# -----------------------------------------------------------------------------
# Config & paths
# -----------------------------------------------------------------------------
app = Flask(__name__)

APP_ROOT = Path(__file__).resolve().parent
STATIC_EXPORT_ROOT = (APP_ROOT / "static" / "exports")
STATIC_EXPORT_ROOT.mkdir(parents=True, exist_ok=True)

# GitHub push settings (OPTIONAL, leave blank to disable pushing)
GH_TOKEN = os.getenv("GH_TOKEN", "").strip()
STATIC_REPO = os.getenv("STATIC_REPO", "").strip()              # e.g. 'jiffcope-byte/cwloop-webapp'
STATIC_BRANCH = os.getenv("STATIC_BRANCH", "main").strip()
STATIC_PATH = os.getenv("STATIC_PATH", "static/exports").strip() # path inside the repo
STATIC_SITE_BASE = os.getenv("STATIC_SITE_BASE", "").strip()     # e.g. https://cwloop-static.onrender.com

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _slugify(text: str) -> str:
    text = str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)

def _clean_header(name: str) -> str:
    """Remove device prefix to the left of first dot, keep the rest."""
    if "." in name:
        return name.split(".", 1)[1].strip()
    return name.strip()

def _detect_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    cand = [c for c in df.columns if c.strip().lower() in {
        "time stamp", "timestamp", "date time", "datetime", "time", "date", " time stamp"
    }]
    if cand:
        return cand[0]
    # fallback: first datetime-like column
    for c in df.columns:
        try:
            _ = pd.to_datetime(df[c], errors="raise", infer_datetime_format=True)
            return c
        except Exception:
            pass
    return None

def _parse_csv(file_storage) -> pd.DataFrame:
    df = pd.read_csv(file_storage)
    # Drop likely sequence columns
    seq_cols = [c for c in df.columns if c.strip().lower() in {"sequence", "seq", "index"}]
    if seq_cols:
        df = df.drop(columns=seq_cols, errors="ignore")
    return df

def _align_to_global(original: pd.DataFrame,
                     extras: List[pd.DataFrame],
                     tolerance_s: int) -> pd.DataFrame:
    """
    Align all additional CSVs to the original's time stamps using merge_asof
    (nearest within tolerance), then forward-fill gaps.
    """
    # Find original timestamp col
    ts_col = _detect_timestamp_column(original)
    if not ts_col:
        raise ValueError("Could not detect a timestamp column in the original CSV.")
    # Parse original timestamps
    original = original.copy()
    original[ts_col] = pd.to_datetime(original[ts_col], errors="coerce")
    original = original.dropna(subset=[ts_col]).sort_values(ts_col)
    # Drop duplicates on time, keep first
    original = original.drop_duplicates(subset=[ts_col], keep="first").reset_index(drop=True)

    # Clean original headers (remove prefixes)
    rename_map = {c: _clean_header(c) for c in original.columns}
    original = original.rename(columns=rename_map)
    ts_col_clean = rename_map[ts_col]

    global_df = original[[ts_col_clean]].copy()

    # Keep original (non-timestamp) series too
    for c in original.columns:
        if c != ts_col_clean:
            global_df[c] = original[c]

    tol = pd.Timedelta(seconds=tolerance_s if tolerance_s is not None else 0)

    # Merge each extra onto the global timestamps
    for extra in extras:
        df = extra.copy()
        ts2 = _detect_timestamp_column(df)
        if not ts2:
            # no timestamp => skip
            continue
        df[ts2] = pd.to_datetime(df[ts2], errors="coerce")
        df = df.dropna(subset=[ts2]).sort_values(ts2).drop_duplicates(subset=[ts2], keep="first")

        # Clean headers
        df = df.rename(columns={c: _clean_header(c) for c in df.columns})
        ts2_clean = _clean_header(ts2)

        # columns to bring (numeric and bool)
        cols = [c for c in df.columns if c != ts2_clean]

        # Merge_asof nearest within tolerance
        merged = pd.merge_asof(global_df[[ts_col_clean]], df[[ts2_clean] + cols],
                               left_on=ts_col_clean, right_on=ts2_clean,
                               direction="nearest", tolerance=tol)

        # Drop the right timestamp
        merged = merged.drop(columns=[ts2_clean], errors="ignore")

        # Forward fill (carry last known value)
        merged = merged.ffill()

        # Append new columns
        for c in cols:
            if c not in global_df.columns:
                global_df[c] = merged[c]
            else:
                # if name collision, create a unique name
                i = 2
                base = c
                while c in global_df.columns:
                    c = f"{base} ({i})"
                    i += 1
                global_df[c] = merged[base]

    # Final tidy
    global_df = global_df.sort_values(ts_col_clean).reset_index(drop=True)
    return global_df, ts_col_clean

def _build_plot(df: pd.DataFrame,
                time_col: str,
                title: str,
                y1_min: Optional[float],
                y1_max: Optional[float],
                setpoint_col: Optional[str]) -> go.Figure:
    fig = go.Figure()
    yaxis2_needed = False
    setpoint_norm = setpoint_col.strip().lower() if setpoint_col else ""

    for c in df.columns:
        if c == time_col:
            continue
        on_y2 = (setpoint_norm and c.strip().lower() == setpoint_norm)
        if on_y2:
            yaxis2_needed = True
        fig.add_trace(go.Scatter(
            x=df[time_col],
            y=df[c],
            mode="lines",
            name=c,
            yaxis="y2" if on_y2 else "y1",
            hovertemplate="%{y}<extra>%{fullData.name}</extra>"
        ))

    layout = dict(
        title=title or "CW Loop",
        hovermode="x unified",
        xaxis=dict(title=time_col, type="date"),
        yaxis=dict(title="Percent", rangemode="tozero"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=50, r=50, t=60, b=40)
    )
    if y1_min is not None or y1_max is not None:
        rng = []
        rng.append(y1_min if y1_min is not None else None)
        rng.append(y1_max if y1_max is not None else None)
        layout["yaxis"]["range"] = rng

    if yaxis2_needed:
        layout["yaxis2"] = dict(
            title="Setpoint",
            overlaying="y",
            side="right",
            showgrid=False
        )

    fig.update_layout(**layout)
    return fig

def _write_html(fig: go.Figure, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Standalone HTML
    html_str = fig.to_html(full_html=True, include_plotlyjs="cdn")
    out_path.write_text(html_str, encoding="utf-8")

def _write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")

# -----------------------------------------------------------------------------
# Index pages (top-level and per-day)
# -----------------------------------------------------------------------------
def _write_date_index(day_dir: Path):
    rel = day_dir.relative_to(STATIC_EXPORT_ROOT).as_posix()
    items = []
    for p in sorted(day_dir.iterdir()):
        if p.is_file() and p.suffix.lower() in {".html", ".csv"}:
            items.append((p.name, f"./{p.name}"))
    html = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>Exports – {rel}</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;padding:24px;max-width:1200px;margin:auto}a{color:#3b82f6;text-decoration:none}a:hover{text-decoration:underline}ul{line-height:1.8}</style>",
        "</head><body>",
        f"<h1>Exports – {rel}</h1>",
        "<p><a href='../index.html'>&larr; All dates</a></p>",
        "<ul>",
    ]
    for name, href in items:
        html.append(f"<li><a href='{href}'>{name}</a></li>")
    html += ["</ul></body></html>"]
    (day_dir / "index.html").write_text("\n".join(html), encoding="utf-8")

def _write_top_index(root_dir: Path):
    # list date folders
    dates = []
    for d in sorted(root_dir.iterdir()):
        if d.is_dir():
            dates.append(d.name)
    html = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Exports</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;padding:24px;max-width:1200px;margin:auto}a{color:#3b82f6;text-decoration:none}a:hover{text-decoration:underline}ul{line-height:1.8}</style>",
        "</head><body>",
        "<h1>Exports</h1>",
        "<ul>"
    ]
    for d in dates:
        html.append(f"<li><a href='./{d}/index.html'>{d}</a></li>")
    html += ["</ul></body></html>"]
    (root_dir / "index.html").write_text("\n".join(html), encoding="utf-8")

# -----------------------------------------------------------------------------
# GitHub push (create/update contents API)
# -----------------------------------------------------------------------------
def _github_put_content(repo_rel_path: str, content_bytes: bytes, commit_message: str) -> bool:
    """
    Create/update a file in GitHub repo using the Contents API.
    """
    if not (GH_TOKEN and STATIC_REPO and STATIC_BRANCH):
        return False

    url = f"https://api.github.com/repos/{STATIC_REPO}/contents/{repo_rel_path}"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    # check existing
    r = requests.get(url, headers=headers, params={"ref": STATIC_BRANCH}, timeout=30)
    sha = r.json().get("sha") if r.status_code == 200 else None

    payload = {
        "message": commit_message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": STATIC_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=headers, data=json.dumps(payload), timeout=30)
    return r.status_code in (200, 201)

def _push_static_file(repo_rel_path: str, content: bytes, message: str) -> bool:
    """Wrapper that respects GH env presence and pushes one file."""
    if not (GH_TOKEN and STATIC_REPO and STATIC_BRANCH and repo_rel_path):
        return False
    return _github_put_content(repo_rel_path, content, message)

def _push_indexes(day_folder: Optional[str]):
    """Always try to push the top index and the daily index (if any)."""
    if not (GH_TOKEN and STATIC_REPO and STATIC_BRANCH and STATIC_PATH):
        return

    # push top index
    top_index = (STATIC_EXPORT_ROOT / "index.html")
    if top_index.exists():
        _push_static_file(
            f"{STATIC_PATH}/index.html",
            top_index.read_bytes(),
            "Update exports top index"
        )

    # push date index
    if day_folder:
        date_index = (STATIC_EXPORT_ROOT / day_folder / "index.html")
        if date_index.exists():
            _push_static_file(
                f"{STATIC_PATH}/{day_folder}/index.html",
                date_index.read_bytes(),
                f"Update exports index for {day_folder}"
            )

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/")
def home():
    """
    Renders upload UI + shows recent exports (from disk) with direct 'View' links
    (if STATIC_SITE_BASE is configured).
    """
    # Build a small list of recent folders (dates)
    folders = []
    for d in sorted(STATIC_EXPORT_ROOT.iterdir()):
        if d.is_dir():
            folders.append(d.name)

    # Build a list of recent .html files for convenience
    recent = []
    for d in sorted(STATIC_EXPORT_ROOT.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.html")):
            rel_date = d.name
            if STATIC_SITE_BASE:
                web = f"{STATIC_SITE_BASE.rstrip('/')}/{STATIC_PATH.strip('/')}/{rel_date}/{f.name}"
            else:
                web = None
            recent.append({
                "date": rel_date,
                "name": f.name,
                "local": f.relative_to(APP_ROOT).as_posix(),
                "web": web
            })
        if len(recent) > 50:
            break

    return render_template("index.html",
                           recent_exports=recent,
                           static_site_base=STATIC_SITE_BASE,
                           static_path=STATIC_PATH)

@app.post("/process")
def process():
    """
    Receive original CSV + multiple additional CSVs, align to original timestamps,
    build Plotly, write CSV/HTML, ALWAYS push indexes to GitHub (if configured),
    then optionally push HTML/CSV.
    """
    try:
        # Inputs
        title = (request.form.get("title") or "CW Loop").strip()
        tol_s = int(request.form.get("tolerance", "5").strip() or "5")
        y1_min = request.form.get("y1_min", "").strip()
        y1_max = request.form.get("y1_max", "").strip()
        setpoint_col = (request.form.get("setpoint_col") or "").strip()

        y1_min_val = float(y1_min) if y1_min != "" else None
        y1_max_val = float(y1_max) if y1_max != "" else None

        if "original_csv" not in request.files:
            return jsonify({"error": "No original CSV provided."}), 400

        original_file = request.files["original_csv"]
        if not original_file or original_file.filename == "":
            return jsonify({"error": "Empty original CSV."}), 400

        add_files = request.files.getlist("additional_csvs")

        original_df = _parse_csv(original_file)
        extras = []
        for f in add_files:
            if f and f.filename:
                extras.append(_parse_csv(f))

        merged_df, time_col = _align_to_global(original_df, extras, tol_s)

        # Build figure
        fig = _build_plot(merged_df, time_col, title, y1_min_val, y1_max_val, setpoint_col)

        # Output file names
        today = datetime.now(timezone.utc).astimezone().date().isoformat()
        day_dir = STATIC_EXPORT_ROOT / today
        day_dir.mkdir(parents=True, exist_ok=True)

        title_slug = _slugify(title)
        html_name = f"{title_slug}-trend-viewer.html"
        csv_name = f"{title_slug}-merged.csv"

        html_path = day_dir / html_name
        csv_path = day_dir / csv_name

        _write_html(fig, html_path)
        _write_csv(merged_df, csv_path)

        # Update indexes locally
        _write_date_index(day_dir)
        _write_top_index(STATIC_EXPORT_ROOT)

        # --- Always push both index pages (if GH is configured) ---
        _push_indexes(today)

        # --- Optional GitHub push of the generated viewer + csv ---
        if GH_TOKEN and STATIC_REPO and STATIC_BRANCH and STATIC_PATH:
            repo_base = f"{STATIC_PATH}/{today}"
            msg = f"Publish {html_name} & {csv_name}"

            _push_static_file(f"{repo_base}/{html_name}", html_path.read_bytes(), msg)
            _push_static_file(f"{repo_base}/{csv_name}",  csv_path.read_bytes(),  msg)

            # Push indexes again (ensures they exist even if files were new)
            _push_indexes(today)

        # Prepare downloads
        html_bytes = html_path.read_bytes()
        csv_bytes = csv_path.read_bytes()

        # Web URLs (if configured)
        web_html = None
        if STATIC_SITE_BASE:
            web_html = f"{STATIC_SITE_BASE.rstrip('/')}/{STATIC_PATH.strip('/')}/{today}/{html_name}"

        return jsonify({
            "ok": True,
            "title": title,
            "time_col": time_col,
            "today": today,
            "files": {
                "html_name": html_name,
                "csv_name": csv_name
            },
            "download": {
                "html": f"/download?path={html_path.relative_to(APP_ROOT).as_posix()}",
                "csv":  f"/download?path={csv_path.relative_to(APP_ROOT).as_posix()}",
            },
            "web": {
                "html": web_html,
                "top_index": f"{STATIC_SITE_BASE.rstrip('/')}/{STATIC_PATH.strip('/')}/index.html" if STATIC_SITE_BASE else None,
                "date_index": f"{STATIC_SITE_BASE.rstrip('/')}/{STATIC_PATH.strip('/')}/{today}/index.html" if STATIC_SITE_BASE else None
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/download")
def download():
    rel = request.args.get("path", "")
    if not rel:
        return "Missing path", 400
    file_path = (APP_ROOT / rel).resolve()
    if not file_path.exists() or APP_ROOT not in file_path.parents:
        return "Not found", 404
    return send_file(file_path, as_attachment=True)

@app.post("/rebuild-indexes")
def rebuild_indexes():
    """
    Regenerate both levels of indexes and push them to GitHub.
    Useful if you want to sync the listing without making a new chart.
    """
    _write_top_index(STATIC_EXPORT_ROOT)
    for d in sorted(STATIC_EXPORT_ROOT.glob("*")):
        if d.is_dir():
            _write_date_index(d)

    # push all indexes if GH is set
    _push_indexes(None)
    for d in sorted(STATIC_EXPORT_ROOT.glob("*")):
        if d.is_dir():
            _push_indexes(d.name)

    return {"status": "ok", "pushed": bool(GH_TOKEN and STATIC_REPO and STATIC_BRANCH and STATIC_PATH)}

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=False)
