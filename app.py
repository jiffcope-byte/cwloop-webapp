# app.py
import io
import os
import re
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

BASE_DIR = Path(__file__).resolve().parent
STATIC_EXPORTS = BASE_DIR / "static" / "exports"
STATIC_EXPORTS.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, static_folder="static", template_folder="templates")

def _today_str(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d")

def _ts_for_filename(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%H%M%S")

def _clean_colname(name: str) -> str:
    if not isinstance(name, str):
        return name
    if "." in name:
        return name.split(".", 1)[1]
    return name

def _likely_time_col(cols):
    candidates = [" Time Stamp", "Time Stamp", "Timestamp", "TimeStamp", "time", "Time"]
    for c in candidates:
        for col in cols:
            if col.strip().lower() == c.strip().lower():
                return col
    return cols[0]

def read_csv_to_df(file_storage) -> pd.DataFrame:
    content = file_storage.read()
    file_storage.stream.seek(0)
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc)
            break
        except Exception:
            df = None
    if df is None:
        raise ValueError("Unable to read CSV")

    tcol = _likely_time_col(list(df.columns))
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
    df = df.dropna(axis=1, how="all")
    df.columns = [_clean_colname(c) for c in df.columns]
    df = df[[c for c in df.columns if str(c).strip().lower() != "sequence"]]
    return df

def align_to_reference(ref_index: pd.DatetimeIndex, df: pd.DataFrame, tolerance_s=5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(index=ref_index)
    left = pd.DataFrame(index=ref_index).reset_index().rename(columns={"index": "ref_ts"})
    right = df.reset_index().rename(columns={"index": "ts"})
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
    merged = merged.ffill()
    merged.index.name = None
    return merged

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_browse_index(root_dir: Path, site_title="Exports"):
    root_dir = Path(root_dir)
    dates = sorted([p for p in root_dir.iterdir() if p.is_dir()])
    top = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{site_title}</title>",
        "</head><body>",
        f"<h1>{site_title}</h1><ul>"
    ]
    for d in dates:
        rel = f"{d.name}/index.html"
        top.append(f"<li><a href='{rel}'>{d.name}</a></li>")
    top += ["</ul></body></html>"]
    (root_dir / "index.html").write_text("\n".join(top), encoding="utf-8")

def recent_exports(limit=12):
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

def build_plot(merged: pd.DataFrame, title: str, y2_cols=None, y1_min=None, y1_max=None):
    y2_cols = y2_cols or []
    has_y2 = any(col in merged.columns for col in y2_cols)
    fig = make_subplots(specs=[[{"secondary_y": has_y2}]])
    for col in merged.columns:
        if col in y2_cols:
            continue
        fig.add_trace(go.Scatter(x=merged.index, y=merged[col], name=col, mode="lines"), secondary_y=False)
    for col in y2_cols:
        if col in merged.columns:
            fig.add_trace(go.Scatter(x=merged.index, y=merged[col], name=col, mode="lines"), secondary_y=True)
    fig.update_layout(title=title, hovermode="x unified")
    return fig

@app.route("/", methods=["GET"])
def index():
    exports = recent_exports(10)
    return render_template("index.html", exports_list=exports)

@app.route("/process", methods=["POST"])
def process():
    if "original" not in request.files:
        abort(400, description="Original CSV is required")
    orig_file = request.files["original"]
    if not orig_file or orig_file.filename.strip() == "":
        abort(400, description="Original CSV is required")

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

    ref_df = read_csv_to_df(orig_file)
    if cutoff_ts:
        ref_df = ref_df.loc[ref_df.index <= cutoff_ts]

    ref_index = ref_df.index
    merged = pd.DataFrame(index=ref_index)
    for col in ref_df.columns:
        merged[col] = ref_df[col]

    add_files = request.files.getlist("additionals")
    for f in add_files:
        if not f or not f.filename:
            continue
        df = read_csv_to_df(f)
        if cutoff_ts:
            df = df.loc[df.index <= cutoff_ts]
        aligned = align_to_reference(ref_index, df, tolerance_s)
        for col in aligned.columns:
            out_col = col
            k = 2
            while out_col in merged.columns:
                out_col = f"{col} ({k})"
                k += 1
            merged[out_col] = aligned[col]

    y2_cols = []
    if setpoint_col:
        ci = {c.lower(): c for c in merged.columns}
        match = ci.get(setpoint_col.lower(), None)
        if match:
            y2_cols = [match]

    fig = build_plot(merged, title=title, y2_cols=y2_cols, y1_min=y1_min, y1_max=y1_max)
    day_dir = ensure_dir(STATIC_EXPORTS / _today_str())
    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", title).strip("_") or "CW_Loop"
    stem = f"{stem}_{_ts_for_filename()}"
    csv_path = day_dir / f"{stem}.csv"
    html_path = day_dir / f"{stem}.html"

    merged.to_csv(csv_path, index_label="Time Stamp")
    fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)
    write_browse_index(STATIC_EXPORTS, site_title="CW Loop Exports")

    html_rel = f"/static/exports/{day_dir.name}/{html_path.name}"
    csv_rel = f"/static/exports/{day_dir.name}/{csv_path.name}"
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
    write_browse_index(STATIC_EXPORTS, site_title="CW Loop Exports")
    if request.method == "GET":
        return redirect(url_for("index"))
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
