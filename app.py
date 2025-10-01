import os, json, zipfile
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "static" / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# ---------- Helpers ----------
def safe_read_csv(file_storage):
    try:
        return pd.read_csv(file_storage, encoding="utf-8-sig", sep=None, engine="python")
    except Exception:
        try:
            file_storage.seek(0)
        except Exception:
            pass
        return pd.read_csv(file_storage)

def parse_datetime_index(df):
    if "Time Stamp" in df.columns:
        ts = pd.to_datetime(df["Time Stamp"], errors="coerce")
        df = df.loc[ts.notna()].copy()
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("UTC")
        df.index = ts[ts.notna()].values
        return df.drop(columns=["Time Stamp"], errors="ignore")
    ts = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    df = df.loc[ts.notna()].copy()
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("UTC")
    df.index = ts[ts.notna()].values
    return df.drop(df.columns[0], axis=1)

def merge_with_asof(base, others, tol_sec):
    base = base.sort_index()
    out = base.copy()
    for name, odf in others:
        odf = odf.sort_index().add_prefix(name + "::")
        left = out.reset_index().rename(columns={"index": "_ts"})
        right = odf.reset_index().rename(columns={"index": "_ts"})
        merged = pd.merge_asof(
            left.sort_values("_ts"),
            right.sort_values("_ts"),
            on="_ts",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=int(tol_sec)),
        ).set_index("_ts")
        out = merged
    return out

def ensure_utc_index(df_index):
    try:
        tz = getattr(df_index, "tz", None)
        if tz is None:
            idx = pd.to_datetime(df_index, errors="coerce")
            return idx.tz_localize("UTC")
        else:
            return df_index.tz_convert("UTC")
    except Exception:
        idx = pd.to_datetime(df_index, errors="coerce", utc=True)
        return idx

def clean_label(col_name: str) -> str:
    # drop "file::" prefix; then keep only after last '.'; trim
    base = col_name.split("::", 1)[-1]
    if "." in base:
        base = base.split(".")[-1]
    return base.strip()

# ---------- Routes ----------
@app.route("/", methods=["GET"])
def index():
    items = []
    for p in sorted(EXPORT_DIR.glob("*/result.json"), reverse=True):
        try:
            meta = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        folder = p.parent.name
        base = f"/static/exports/{folder}"
        items.append({
            "title": meta.get("title", "Export"),
            "when": meta.get("timestamp", ""),
            "view_url": f"{base}/view.html" if (p.parent / "view.html").exists() else None,
            "csv_url":  f"{base}/merged.csv" if (p.parent / "merged.csv").exists() else None,
            "zip_url":  f"{base}/bundle.zip" if (p.parent / "bundle.zip").exists() else None,
        })
    return render_template("index.html", exports_list=items)

@app.route("/process", methods=["GET", "POST"])
def process():
    if request.method == "GET":
        return redirect(url_for("index"))

    base_file = request.files.get("base_csv")
    if not base_file or base_file.filename == "":
        return render_template("error.html", message="Missing Original CSV."), 400

    tol_sec = int((request.form.get("tolerance") or "5").strip() or 5)
    title = (request.form.get("title") or "CW Loop").strip()
    y1min = (request.form.get("y1min") or "").strip()
    y1max = (request.form.get("y1max") or "").strip()
    setpoint_hint = (request.form.get("setpoint") or "").strip().lower()

    try:
        base_df = parse_datetime_index(safe_read_csv(base_file))

        others_list = []
        for f in request.files.getlist("extra_csvs") or []:
            if not f or not f.filename:
                continue
            odf = parse_datetime_index(safe_read_csv(f))
            others_list.append((Path(f.filename).stem, odf))

        merged = merge_with_asof(base_df, others_list, tol_sec)
        merged.index = ensure_utc_index(merged.index)

        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        folder = EXPORT_DIR / stamp
        folder.mkdir(parents=True, exist_ok=True)

        # Build traces with cleaned names (drop "Sequence")
        numeric_cols = [c for c in merged.columns if pd.api.types.is_numeric_dtype(merged[c])]
        clean_to_orig = {}
        for c in numeric_cols:
            label = clean_label(c)
            if label.lower() == "sequence":
                continue
            clean_to_orig[label] = c

        # Write merged CSV (UTC -> naive for file)
        merged_out = merged.copy()
        ts_series = merged_out.index.tz_convert("UTC").tz_localize(None).astype("datetime64[ns]")
        merged_out.insert(0, "Time Stamp", ts_series)
        (folder / "merged.csv").write_text(merged_out.to_csv(index=False), encoding="utf-8")

        ts_list = merged_out["Time Stamp"].astype(str).tolist()

        def is_y2(name: str) -> bool:
            if setpoint_hint:
                return name.lower() == setpoint_hint
            return "setpoint" in name.lower() or name.lower() == "sp"

        traces = []
        hover_tpl = "<b>%{fullData.name}</b>: %{y}<extra></extra>"
        has_y2 = False
        for label, orig in clean_to_orig.items():
            arr = merged[orig].astype("float64").where(merged[orig].notna(), None).tolist()
            trace = {
                "x": ts_list,
                "y": arr,
                "mode": "lines",
                "name": label,
                "type": "scatter",
                "hovertemplate": hover_tpl,
            }
            if is_y2(label):
                trace["yaxis"] = "y2"
                has_y2 = True
            traces.append(trace)

        # layout: top legend, unified hover, extra right margin; y2 axis visible when used
        layout = {
            "hovermode": "x unified",
            "showlegend": True,
            "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
            "margin": {"t": 80, "r": 80},
            "xaxis": {"type": "date"},
        }
        if y1min or y1max:
            layout["yaxis"] = {
                "range": [float(y1min) if y1min else None, float(y1max) if y1max else None]
            }
        if has_y2:
            layout["yaxis2"] = {
                "overlaying": "y",
                "side": "right",
                "title": "Setpoint",
                "showline": True,
                "ticks": "outside",
                "ticklen": 6,
                "tickcolor": "#9aa4ad"
            }

        # HTML
        html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} – Viewer</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body style="font-family:ui-sans-serif; background:#0b0f14; color:#e6edf3">
  <h2 style="margin:12px 16px;">{title}</h2>
  <div id="chart" style="width:100%;height:80vh"></div>
  <script>
    const data = {json.dumps(traces)};
    const layout = {json.dumps(layout)};
    Plotly.newPlot('chart', data, layout, {{responsive:true}});
  </script>
</body>
</html>"""
        (folder / "view.html").write_text(html, encoding="utf-8")

        with zipfile.ZipFile(folder / "bundle.zip", "w", zipfile.ZIP_DEFLATED) as z:
            for p in folder.glob("*"):
                if p.name != "bundle.zip":
                    z.write(p, p.name)

        (folder / "result.json").write_text(json.dumps({
            "title": title,
            "timestamp": stamp,
            "params": {"tolerance": tol_sec, "y1min": y1min, "y1max": y1max, "setpoint": setpoint_hint},
            "columns": list(clean_to_orig.keys())
        }, indent=2), encoding="utf-8")

        return redirect(url_for("index"))
    except Exception as e:
        import sys, traceback
        traceback.print_exc(file=sys.stderr)
        return render_template("error.html", message=str(e)), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
