
from flask import Flask, render_template, request, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
import tempfile
import os
import zipfile
import re
import base64
import requests
import json

# Optional Google Drive (service account)
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaInMemoryUpload
    from google.oauth2 import service_account
    GDRIVE_AVAILABLE = True
except Exception:
    GDRIVE_AVAILABLE = False

app = Flask(__name__)
app.secret_key = "dev-secret"

# ---- Static exports directory (still saved locally & served by Flask) ----
EXPORTS_DIR = Path(app.static_folder) / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
LATEST_JSON = EXPORTS_DIR / "latest.json"

ALLOWED_EXTENSIONS = {"csv"}

def slugify(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9\-\_]+", "-", s.strip())
    s = re.sub(r"-+", "-", s).strip("-").lower()
    return s or "viewer"

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

        # pick numeric columns
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
            hovertemplate=f"{col}: %{{y:.2f}}<extra></extra>"
        ))

    fig.update_layout(
        title=title,
        xaxis=dict(title=time_col, rangeslider=dict(visible=True), type="date", showspikes=True, spikemode='across', spikesnap='cursor'),
        yaxis=dict(title="Percent", range=[y1_min, y1_max]),
        yaxis2=dict(title=setpoint_name if setpoint_name else "Setpoint", overlaying="y", side="right", showgrid=False, autorange=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=80, t=60, b=40),
        hovermode="x unified",
        hoverdistance=30,
        height=800
    )
    return pio.to_html(fig, include_plotlyjs=True, full_html=True)

# ---- GitHub upload (Option B with dated subfolders) ----
def push_to_github(repo: str, branch: str, path: str, content_bytes: bytes, message: str):
    token = os.getenv("GH_TOKEN")
    if not token or not repo or not branch or not path:
        return None, "Missing GH_TOKEN/STATIC_REPO/STATIC_BRANCH/PATH"
    api = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    sha = None
    r = requests.get(api, params={"ref": branch}, headers=headers, timeout=30)
    if r.status_code == 200 and isinstance(r.json(), dict) and "sha" in r.json():
        sha = r.json()["sha"]
    payload = {"message": message, "content": base64.b64encode(content_bytes).decode("utf-8"), "branch": branch}
    if sha: payload["sha"] = sha
    resp = requests.put(api, headers=headers, json=payload, timeout=60)
    if resp.status_code not in (200, 201):
        return None, f"GitHub upload failed: {resp.status_code} {resp.text}"
    return resp.json(), None

# ---- Google Drive helpers ----
def get_drive():
    if not GDRIVE_AVAILABLE:
        return None, "Drive libs not installed"
    b64 = os.getenv("GDRIVE_SA_JSON_B64", "")
    if not b64:
        return None, "GDRIVE_SA_JSON_B64 not set"
    try:
        data = json.loads(base64.b64decode(b64).decode("utf-8"))
        creds = service_account.Credentials.from_service_account_info(
            data, scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False), None
    except Exception as e:
        return None, f"Drive auth error: {e}"

def upload_to_drive(service, folder_id, name, mimetype, content_bytes, make_public=True):
    file_metadata = {"name": name, "parents": [folder_id]}
    media = MediaInMemoryUpload(content_bytes, mimetype=mimetype, resumable=False)
    file = service.files().create(body=file_metadata, media_body=media, fields="id,webViewLink,webContentLink").execute()
    file_id = file["id"]
    if make_public:
        try:
            service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
        except Exception:
            pass
    return file.get("webViewLink"), file_id

@app.route("/", methods=["GET"])
def index():
    # Recent hosted viewers (local Render static)
    exports = []
    try:
        for p in sorted(EXPORTS_DIR.glob("*.html"), key=lambda x: x.stat().st_mtime, reverse=True)[:30]:
            slug = p.stem  # without .html
            html_url = f"/static/exports/{p.name}"
            csv_name = f"{slug}.csv"
            csv_url = f"/static/exports/{csv_name}" if (EXPORTS_DIR / csv_name).exists() else None
            exports.append({"slug": slug, "html": html_url, "csv": csv_url})
    except Exception:
        pass

    # Last pushed links (GitHub/Drive) from latest.json
    pushed = {}
    if LATEST_JSON.exists():
        try:
            pushed = json.loads(LATEST_JSON.read_text())
        except Exception:
            pushed = {}

    site_base = os.getenv("STATIC_SITE_BASE", "").rstrip("/")
    return render_template("index.html", exports_list=exports, pushed=pushed, site_base=site_base)

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

    # Local (Render) save
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    slug = slugify(title)
    hosted_html = EXPORTS_DIR / f"{slug}-{ts}.html"
    hosted_csv  = EXPORTS_DIR / f"{slug}-{ts}.csv"
    try:
        hosted_html.write_bytes(html_bytes)
        hosted_csv.write_bytes(csv_buf.getvalue())
    except Exception:
        pass

    # ---- GitHub push with optional dated subfolder ----
    repo   = os.getenv("STATIC_REPO")      # e.g. "yourname/cwloop-viewers"
    branch = os.getenv("STATIC_BRANCH", "main")
    base   = os.getenv("STATIC_PATH", "").strip("/")
    use_dates = os.getenv("STATIC_DATED_SUBFOLDERS", "true").lower() in ("1","true","yes","on")
    dt = datetime.now(timezone.utc)
    date_path = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}" if use_dates else ""

    def join_path(base, date_path, name):
        parts = [p for p in [base, date_path, name] if p]
        return "/".join(parts)

    gh_html_rel = join_path(base, date_path, f"{slug}-{ts}.html")
    gh_csv_rel  = join_path(base, date_path, f"{slug}-{ts}.csv")

    pushed_links = {"github": {}, "gdrive": {}}

    if repo and os.getenv("GH_TOKEN"):
        res1, err1 = push_to_github(repo, branch, gh_html_rel, html_bytes, f"Add viewer {slug}-{ts}.html")
        res2, err2 = push_to_github(repo, branch, gh_csv_rel,  csv_buf.getvalue(), f"Add CSV {slug}-{ts}.csv")
        site_base = os.getenv("STATIC_SITE_BASE", "").rstrip("/")
        if not err1 and site_base:
            pushed_links["github"]["html"] = f"{site_base}/{gh_html_rel}"
        if not err2 and site_base:
            pushed_links["github"]["csv"]  = f"{site_base}/{gh_csv_rel}"
        if err1 or err2:
            flash(f"GitHub upload warning: {err1 or err2}")

    # ---- Google Drive upload (optional) ----
    gdrive_folder = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if gdrive_folder and GDRIVE_AVAILABLE:
        service, err = get_drive()
        if service and not err:
            try:
                html_link, _ = upload_to_drive(service, gdrive_folder, f"{slug}-{ts}.html", "text/html", html_bytes, make_public=True)
                csv_link, _  = upload_to_drive(service, gdrive_folder, f"{slug}-{ts}.csv",  "text/csv",  csv_buf.getvalue(), make_public=True)
                pushed_links["gdrive"]["html"] = html_link
                pushed_links["gdrive"]["csv"]  = csv_link
            except Exception as e:
                flash(f"Drive upload warning: {e}")

    # Save latest links so index can render copy buttons
    try:
        LATEST_JSON.write_text(json.dumps(pushed_links, indent=2))
    except Exception:
        pass

    # Return ZIP immediately
    out_zip = BytesIO()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{title} - merged.csv", csv_buf.getvalue())
        zf.writestr(f"{title} - Trend Viewer.html", html_bytes)
    out_zip.seek(0)

    if pushed_links.get("github") or pushed_links.get("gdrive"):
        flash("Upload complete — links on the homepage (copy buttons available).")
    else:
        flash("Saved to /static/exports and bundled ZIP returned. (Configure Option B env vars to auto-push to GitHub/Drive.)")

    return send_file(out_zip, as_attachment=True, download_name=f"{title} - results.zip", mimetype="application/zip")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
