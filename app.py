import os
import io
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, request, render_template, send_file, redirect, url_for

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "static" / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def safe_read_csv(file_storage):
    try:
        return pd.read_csv(file_storage, encoding="utf-8-sig", sep=None, engine="python")
    except Exception:
        file_storage.seek(0)
        return pd.read_csv(file_storage)

def parse_datetime_index(df):
    if "Time Stamp" in df.columns:
        ts = pd.to_datetime(df["Time Stamp"], errors="coerce")
        df = df.loc[ts.notna()].copy()
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        df.index = ts[ts.notna()].values
        return df.drop(columns=["Time Stamp"], errors="ignore")
    ts = pd.to_datetime(df.iloc[:,0], errors="coerce")
    if ts.notna().any():
        df = df.loc[ts.notna()].copy()
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        df.index = ts[ts.notna()].values
        return df.drop(df.columns[0], axis=1)
    raise ValueError("No usable timestamp column found")

# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    try:
        base_file = request.files.get("base_csv")
        other_files = request.files.getlist("other_csvs")
        title = request.form.get("title", "CW Loop")

        if not base_file:
            return render_template("error.html", message="No base CSV uploaded")

        base_df = parse_datetime_index(safe_read_csv(base_file))

        merged = base_df.copy()
        for f in other_files:
            odf = parse_datetime_index(safe_read_csv(f))
            merged = pd.merge_asof(
                merged.sort_index(), odf.sort_index(),
                left_index=True, right_index=True, tolerance=pd.Timedelta("5s")
            )

        # Export folder
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        folder = EXPORT_DIR / stamp
        folder.mkdir(parents=True, exist_ok=True)

        # Save merged CSV
        merged_out = merged.copy()
        merged_out.insert(
            0, "Time Stamp",
            merged_out.index.tz_convert("UTC").tz_localize(None).astype("datetime64[ns]")
        )
        merged_csv_path = folder / "merged.csv"
        merged_out.to_csv(merged_csv_path, index=False)

        # Plotly viewer HTML
        plotly_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body style="background-color:#111;color:#eee;">
    <div id="chart" style="width:100%;height:95vh;"></div>
    <script>
        fetch("merged.csv").then(r => r.text()).then(csv => {{
            let rows = csv.split("\n").map(r => r.split(","));
            let header = rows.shift();
            let time = rows.map(r => r[0]);
            let data = [];
            for (let c = 1; c < header.length; c++) {{
                let y = rows.map(r => parseFloat(r[c]) || null);
                data.push({{x: time, y: y, type:"scatter", mode:"lines", name: header[c]}});
            }}
            Plotly.newPlot("chart", data, {{
                title: "{title}",
                paper_bgcolor: "#111",
                plot_bgcolor: "#111",
                font: {{color:"#eee"}}
            }});
        }});
    </script>
</body>
</html>
"""
        (folder / "view.html").write_text(plotly_html)

        # Bundle zip
        zip_path = folder / "bundle.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(merged_csv_path, "merged.csv")
            zf.write(folder / "view.html", "view.html")

        return redirect(url_for("static", filename=f"exports/{stamp}/view.html"))
    except Exception as e:
        return render_template("error.html", message=str(e))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
