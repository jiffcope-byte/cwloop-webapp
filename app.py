from flask import Flask, render_template, request, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from io import BytesIO
from pathlib import Path
import tempfile
import os
import zipfile

app = Flask(__name__)
app.secret_key = "dev-secret"

ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def detect_time_col(df: pd.DataFrame):
    candidates = [c for c in df.columns if any(k in c.lower() for k in ["time", "date", "timestamp", "datetime"])]
    def try_parse(series):
        try:
            parsed = pd.to_datetime(series, errors="coerce", infer_datetime_format=True, utc=False)
            return parsed, parsed.notna().mean()
        except Exception:
            return None, 0.0
    for c in candidates + [c for c in df.columns if c not in candidates]:
        p, ratio = try_parse(df[c])
        if ratio >= 0.8:
            return c, p
    c = df.columns[0]
    p, _ = try_parse(df[c])
    return c, p

def drop_sequence_cols(df: pd.DataFrame):
    drop_cols = [c for c in df.columns if any(k in c.lower() for k in ["sequence", "seq #", "seq#", "seq ", "index"])]
    return df.drop(columns=drop_cols, errors="ignore")

def merge_align(original_csv_path: Path, other_csv_paths, tolerance_seconds=5, cutoff_str=None):
    orig_df_raw = pd.read_csv(original_csv_path)
    orig_df_raw = drop_sequence_cols(orig_df_raw.copy())
    orig_time_col, orig_parsed = detect_time_col(orig_df_raw)
    if orig_time_col is None:
        raise RuntimeError("Could not detect a datetime column in the original CSV.")
    orig_df = orig_df_raw.copy()
    orig_df[orig_time_col] = orig_parsed
    orig_df = orig_df[orig_df[orig_time_col].notna()].sort_values(orig_time_col)

    if cutoff_str:
        try:
            cutoff = pd.to_datetime(cutoff_str)
            orig_df = orig_df[orig_df[orig_time_col] >= cutoff]
        except Exception:
            pass

    orig_non_time_cols = [c for c in orig_df.columns if c != orig_time_col]
    merged = orig_df.set_index(orig_time_col)
    tol = pd.Timedelta(f"{int(tolerance_seconds)}s")

    for path in other_csv_paths:
        df_raw = pd.read_csv(path)
        df_raw = drop_sequence_cols(df_raw.copy())
        tcol, tparsed = detect_time_col(df_raw)
        if tcol is None:
            continue
        df = df_raw.copy()
        df[tcol] = tparsed
        df = df[df[tcol].notna()].sort_values(tcol)

        value_cols = []
        for c in df.columns:
            if c == tcol: continue
            col_num = pd.to_numeric(df[c], errors="coerce")
            if col_num.notna().sum() > 0:
                value_cols.append(c)
                df[c] = col_num
        if not value_cols:
            continue

        sub = df[[tcol] + value_cols].copy()
        left_tmp = merged.iloc[[], :].copy()
        left_tmp = left_tmp.reindex(merged.index).reset_index()
        left_time_name = left_tmp.columns[0]

        right_tmp = sub.sort_values(tcol).reset_index(drop=True)

        aligned = pd.merge_asof(
            left=left_tmp[[left_time_name]].sort_values(left_time_name),
            right=right_tmp,
            left_on=left_time_name,
            right_on=tcol,
            direction="nearest",
            tolerance=tol
        )

        keep_cols = [left_time_name] + value_cols
        aligned = aligned[keep_cols]
        aligned = aligned.set_index(left_time_name)
        merged = merged.join(aligned, how="left")

    drop_cols = [c for c in merged.columns if any(k in c.lower() for k in ["time", "date", "timestamp", "datetime"])]
    merged = merged.drop(columns=drop_cols, errors="ignore")

    new_cols = [c for c in merged.columns if c not in orig_non_time_cols]
    final_df = merged[orig_non_time_cols + new_cols].reset_index().rename(columns={merged.index.name: orig_time_col})
    return final_df, orig_time_col

def build_plot(df: pd.DataFrame, time_col: str, title="CW Loop", y1_min=0, y1_max=100, setpoint_name=None):
    numeric_cols = []
    for c in df.columns:
        if c == time_col: continue
        col_num = pd.to_numeric(df[c], errors="coerce")
        if col_num.notna().sum() > 0:
            numeric_cols.append(c)

    plot_df = df.copy()
    for c in numeric_cols:
        plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce")
    plot_df[numeric_cols] = plot_df[numeric_cols].ffill()

    if not setpoint_name:
        cands = ["Plant Pumps.Active CW Flow Setpoint", "Active CW Flow Setpoint", "CW Flow Setpoint"]
        lower_map = {c.lower(): c for c in numeric_cols}
        for cand in cands:
            if cand.lower() in lower_map:
                setpoint_name = lower_map[cand.lower()]
                break
        if not setpoint_name:
            for c in numeric_cols:
                if "flow" in c.lower() and "setpoint" in c.lower():
                    setpoint_name = c
                    break

    fig = go.Figure()
    for col in numeric_cols:
        use_y2 = (setpoint_name is not None and col == setpoint_name)
        fig.add_trace(go.Scatter(
            x=plot_df[time_col],
            y=plot_df[col],
            mode="lines",
            name=col,
            yaxis="y2" if use_y2 else "y",
            hovertemplate=f"{col}: %{{y}}<br>%{{x}}<extra></extra>"
        ))

    fig.update_layout(
        title=title,
        xaxis=dict(title=time_col, rangeslider=dict(visible=True), type="date"),
        yaxis=dict(title="Percent", range=[y1_min, y1_max]),
        yaxis2=dict(title=setpoint_name if setpoint_name else "Setpoint", overlaying="y", side="right", showgrid=False, autorange=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=80, t=60, b=40),
        hovermode="x unified",
        height=800
    )

    figure_html = pio.to_html(fig, include_plotlyjs=True, full_html=False)
    trace_names = [t.name for t in fig.data]
    checkbox_items = "\\n".join(
        f'<label style="margin-right:12px;"><input type="checkbox" class="series-toggle" data-trace="{{i}}" checked> {{name}}</label>'
        for i, name in enumerate(trace_names)
    )

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 16px; }}
    .controls {{ display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 8px 0 16px 0; border-bottom: 1px solid #eee; }}
    .legend-actions {{ margin-left: auto; }}
    .btn {{ cursor: pointer; background: #f3f3f3; border: 1px solid #ccc; border-radius: 6px; padding: 6px 10px; }}
    .plotly-graph-div {{ height: 75vh !important; }}
  </style>
</head>
<body>
  <div class="container">
    <h2>{title}</h2>
    <div class="controls">
      <div class="checkboxes">
        {checkbox_items}
      </div>
      <div class="legend-actions">
        <button class="btn" id="show-all">Show All</button>
        <button class="btn" id="hide-all">Hide All</button>
      </div>
    </div>
    {figure_html}
  </div>
  <script>
    document.addEventListener("DOMContentLoaded", function() {{
      var chartDiv = document.querySelector('.plotly-graph-div');
      var checkboxes = document.querySelectorAll('.series-toggle');
      function updateVisibility() {{
        var update = {{"visible": []}};
        checkboxes.forEach(function(cb, idx) {{
          update.visible[idx] = cb.checked ? true : "legendonly";
        }});
        Plotly.restyle(chartDiv, update);
      }}
      checkboxes.forEach(function(cb) {{
        cb.addEventListener('change', updateVisibility);
      }});
      document.getElementById('show-all').addEventListener('click', function() {{
        checkboxes.forEach(function(cb) {{ cb.checked = true; }});
        updateVisibility();
      }});
      document.getElementById('hide-all').addEventListener('click', function() {{
        checkboxes.forEach(function(cb) {{ cb.checked = false; }});
        updateVisibility();
      }});
    }});
  </script>
</body>
</html>"""
    return html

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    if "original_csv" not in request.files:
        flash("Please upload the Original CSV.")
        return redirect(url_for("index"))

    original_file = request.files["original_csv"]
    if original_file.filename == "" or not allowed_file(original_file.filename):
        flash("Invalid Original CSV.")
        return redirect(url_for("index"))

    other_files = request.files.getlist("other_csvs")
    right_paths = []
    import tempfile
    from werkzeug.utils import secure_filename
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        orig_path = tmpdir / secure_filename(original_file.filename)
        original_file.save(orig_path)

        for f in other_files:
            if f and f.filename and allowed_file(f.filename):
                p = tmpdir / secure_filename(f.filename)
                f.save(p)
                right_paths.append(p)

        tolerance = int(request.form.get("tolerance", "5") or 5)
        title = request.form.get("title", "CW Loop") or "CW Loop"
        setpoint_name = request.form.get("setpoint_name") or None
        y1_min = float(request.form.get("y1_min", "0") or 0)
        y1_max = float(request.form.get("y1_max", "100") or 100)
        cutoff_str = request.form.get("cutoff") or None

        merged_df, time_col = merge_align(orig_path, right_paths, tolerance_seconds=tolerance, cutoff_str=cutoff_str)

        csv_buf = BytesIO()
        merged_df.to_csv(csv_buf, index=False)
        csv_buf.seek(0)

        html = build_plot(merged_df, time_col=time_col, title=title, y1_min=y1_min, y1_max=y1_max, setpoint_name=setpoint_name)
        html_bytes = html.encode("utf-8")

    out_zip = BytesIO()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{title} - merged.csv", csv_buf.getvalue())
        zf.writestr(f"{title} - Trend Viewer.html", html_bytes)
    out_zip.seek(0)

    return send_file(out_zip, as_attachment=True, download_name=f"{title} - results.zip", mimetype="application/zip")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
