import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objs as go
import plotly.offline as pyo
from flask import Flask, render_template, request, send_file, url_for, abort

app = Flask(__name__)

# Make sure we're saving under the real /static directory that Flask serves
EXPORT_DIR = Path(app.static_folder) / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# helpers
# -----------------------------
def clean_column_name(name: str) -> str:
    """Remove device prefix: everything before the first '.'"""
    return name.split(".", 1)[-1] if "." in name else name


def find_original_file(files: "werkzeug.datastructures.MultiDict") -> "FileStorage | None":
    """
    Be permissive about the field name for the original file.
    Accepts: original_csv, original, file, csv, first item.
    """
    candidates = [
        "original_csv",
        "original",
        "file",
        "csv",
    ]
    for key in candidates:
        f = files.get(key)
        if f and f.filename.strip():
            return f

    # last resort: the first uploaded file
    if files:
        first = next(iter(files.values()), None)
        if first and first.filename.strip():
            return first
    return None


def scan_exports():
    """
    Return a list of exports found under /static/exports.
    Each item:
        {
            "date_dir": "YYYY-MM-DD",
            "title": "My Trend",
            "html_rel": "YYYY-MM-DD/My_Trend_153012.html",
            "csv_rel":  "YYYY-MM-DD/My_Trend_153012.csv" or None,
            "view_url": "/static/exports/YYYY-MM-DD/My_Trend_153012.html",
            "csv_url":  "/static/exports/YYYY-MM-DD/My_Trend_153012.csv",
            "html_url": "/static/exports/YYYY-MM-DD/My_Trend_153012.html",
        }
    """
    items = []
    for html_path in EXPORT_DIR.rglob("*.html"):
        rel_html = html_path.relative_to(EXPORT_DIR).as_posix()
        # Look for a sibling CSV with the same stem
        rel_csv = None
        csv_path = html_path.with_suffix(".csv")
        if csv_path.exists():
            rel_csv = csv_path.relative_to(EXPORT_DIR).as_posix()

        # Basic info from path parts
        parts = Path(rel_html).parts
        date_dir = parts[0] if parts else ""
        title = html_path.stem.replace("_", " ")

        items.append(
            {
                "date_dir": date_dir,
                "title": title,
                "html_rel": rel_html,
                "csv_rel": rel_csv,
                "view_url": f"/static/exports/{rel_html}",
                "csv_url": f"/static/exports/{rel_csv}" if rel_csv else None,
                "html_url": f"/static/exports/{rel_html}",
            }
        )

    # newest first by path (dated folder then filename)
    items.sort(key=lambda x: x["html_rel"], reverse=True)
    return items


# -----------------------------
# routes
# -----------------------------
@app.route("/")
def index():
    """Upload UI + browsable list of exports with direct links."""
    exports = scan_exports()
    return render_template("index.html", exports=exports)


@app.route("/process", methods=["POST"])
def process():
    files = request.files

    # --- original file (required) ---
    orig_file = find_original_file(files)
    if not orig_file:
        got_keys = list(files.keys())
        return f"Original CSV is required (received fields: {got_keys})", 400

    try:
        df = pd.read_csv(orig_file)
    except Exception as e:
        return f"Failed to read Original CSV: {e}", 400

    if "Time Stamp" not in df.columns:
        return "Original CSV must include a 'Time Stamp' column.", 400

    # normalize datetime as the global X index
    df["Time Stamp"] = pd.to_datetime(df["Time Stamp"])
    df.set_index("Time Stamp", inplace=True)

    # --- additional csvs (optional, can be multiple) ---
    add_files = files.getlist("additional_csvs")
    for f in add_files:
        if not f or not f.filename.strip():
            continue
        try:
            adf = pd.read_csv(f)
            if "Time Stamp" not in adf.columns:
                # ignore files without timestamp
                continue
            adf["Time Stamp"] = pd.to_datetime(adf["Time Stamp"])
            adf.set_index("Time Stamp", inplace=True)
            df = df.join(adf, how="outer")
        except Exception as e:
            # don't kill the job for one bad file; just log to console
            print(f"Skipping additional file '{f.filename}': {e}")

    # sort, forward-fill gaps
    df.sort_index(inplace=True)
    df.ffill(inplace=True)

    # tidy headers (remove device prefixes)
    df.columns = [clean_column_name(c) for c in df.columns]

    # chart title from form
    title = request.form.get("title", "Trend Viewer").strip() or "Trend Viewer"

    # build Plotly figure
    traces = []
    for col in df.columns:
        try:
            y = pd.to_numeric(df[col], errors="coerce")
        except Exception:
            y = df[col]
        traces.append(go.Scatter(x=df.index, y=y, mode="lines", name=col))

    fig = go.Figure(data=traces)
    fig.update_layout(title=title, xaxis_title="Time Stamp", yaxis_title="Percent")

    html_div = pyo.plot(fig, include_plotlyjs="cdn", output_type="div")

    # save outputs in dated folder
    dated_dir = EXPORT_DIR / datetime.now().strftime("%Y-%m-%d")
    dated_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%H%M%S")
    safe_base = title.replace(" ", "_")

    csv_path = dated_dir / f"{safe_base}_{ts}.csv"
    html_path = dated_dir / f"{safe_base}_{ts}.html"

    # write CSV + HTML
    df.to_csv(csv_path)
    html_path.write_text(f"<!doctype html><html><head><meta charset='utf-8'>"
                         f"<title>{title}</title></head><body>{html_div}</body></html>",
                         encoding="utf-8")

    # hand back the HTML file as a download (you can change to a success page if you prefer)
    return send_file(str(html_path), as_attachment=True)


# Optional: quick route to confirm static exists (debug)
@app.route("/static/exports/<path:subpath>")
def static_exports_passthrough(subpath):
    # Flask will normally serve this, but this gives a clearer 404 if the file is missing.
    full = EXPORT_DIR / subpath
    if not full.exists():
        abort(404)
    # Let Flask's static handler serve it (keeps correct mime types)
    return app.send_static_file(f"exports/{subpath}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
