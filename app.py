import os
import io
import re
import json
import base64
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import requests

from flask import Flask, render_template, request, send_file

# =============================================================================
# Configuration
# =============================================================================

APP_TITLE = "Trend Merge & Viewer"

# Where we save generated files (and what Render serves)
STATIC_EXPORT_ROOT = Path("static") / "exports"

# Option B: GitHub static push (fine-grained PAT)
GH_TOKEN = os.getenv("GH_TOKEN", "").strip()
STATIC_REPO = os.getenv("STATIC_REPO", "").strip()            # e.g. jiffcope-byte/cwloop-webapp
STATIC_BRANCH = os.getenv("STATIC_BRANCH", "main").strip()
STATIC_PATH = os.getenv("STATIC_PATH", "static/exports").strip()  # path inside the repo
STATIC_SITE_BASE = os.getenv("STATIC_SITE_BASE", "").rstrip("/")  # e.g. https://cwloop-static.onrender.com
STATIC_DATED_SUBFOLDERS = os.getenv("STATIC_DATED_SUBFOLDERS", "true").lower() in ("1", "true", "yes")

# Friendly timestamp column candidates
TIME_CANDIDATES = [
    "Time Stamp", "Timestamp", "DateTime", "Datetime", "TimeStamp",
    "Date", "Time", "Date/Time", "Date Time"
]

app = Flask(__name__)

# =============================================================================
# Helpers
# =============================================================================

def log(msg: str) -> None:
    print(msg, flush=True)

def _read_csv(file_storage) -> pd.DataFrame:
    """Read CSV with tolerant encodings; strip column whitespace."""
    file_storage.stream.seek(0)
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            df = pd.read_csv(file_storage.stream, encoding=enc)
            break
        except Exception:
            file_storage.stream.seek(0)
            continue
    df.columns = [c.strip() for c in df.columns]
    return df

def _find_time_col(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if c.strip() in TIME_CANDIDATES:
            return c
    # fallback: first parsable column
    for c in df.columns:
        try:
            pd.to_datetime(df[c])
            return c
        except Exception:
            pass
    return None

def _as_time_index(df: pd.DataFrame, tcol: str) -> pd.DataFrame:
    df = df.copy()
    df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
    df = df.dropna(subset=[tcol]).drop_duplicates(subset=[tcol])
    df = df.sort_values(tcol).set_index(tcol)
    return df

def _drop_seq_cols(df: pd.DataFrame) -> pd.DataFrame:
    patt = re.compile(r"^(seq(uence)?|index)$", re.I)
    return df[[c for c in df.columns if not patt.match(c)]]

def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]+", "", name.strip())
    name = re.sub(r"\s+", " ", name).strip()
    return name or "CW-loop"

def _today_folder_name() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _strip_prefix(name: str) -> str:
    """Label helper: return text after the LAST '.'; unchanged if none."""
    return name.split(".")[-1].strip()

def _github_put_content(path_in_repo: str, content_bytes: bytes, message: str) -> bool:
    """Create/Update file in GitHub repo via API (PUT /contents)."""
    if not (GH_TOKEN and STATIC_REPO and STATIC_BRANCH):
        return False

    api = f"https://api.github.com/repos/{STATIC_REPO}/contents/{path_in_repo}"

    # Get existing sha (update vs create)
    sha = None
    try:
        r = requests.get(api, headers={"Authorization": f"Bearer {GH_TOKEN}"}, timeout=20)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except Exception as e:
        log(f"[GitHub] HEAD failed: {e}")

    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": STATIC_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    try:
        r2 = requests.put(
            api,
            headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"},
            data=json.dumps(payload),
            timeout=30,
        )
        ok = r2.status_code in (200, 201)
        if not ok:
            log(f"[GitHub] PUT failed {r2.status_code}: {r2.text[:200]}")
        return ok
    except Exception as e:
        log(f"[GitHub] PUT exception: {e}")
        return False

def _collect_recent_exports() -> list[dict]:
    """
    Scan static/exports for latest HTMLs and build a list with direct links:
    - local (served by this app): /static/exports/...
    - static site (if configured): STATIC_SITE_BASE/static/exports/...
    - github (blob link)
    - CSV partners (local/static)
    """
    exports: list[dict] = []
    if not STATIC_EXPORT_ROOT.exists():
        return exports

    for day_dir in sorted(STATIC_EXPORT_ROOT.glob("*"), reverse=True):
        if not day_dir.is_dir():
            continue
        for html in sorted(day_dir.glob("*.html"), reverse=True):
            base = html.stem
            csv = html.with_suffix(".csv")

            rel_path_html = html.as_posix()     # static/exports/...
            rel_path_csv  = csv.as_posix()

            rec = {
                "title": base.replace("-", " ").title(),
                "ts": datetime.fromtimestamp(html.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
                "local": f"/{rel_path_html}",
                "csv_local": f"/{rel_path_csv}" if csv.exists() else None,
            }
            if STATIC_SITE_BASE:
                rec["static_url"] = f"{STATIC_SITE_BASE}/{rel_path_html}"
                rec["csv_static"] = f"{STATIC_SITE_BASE}/{rel_path_csv}" if csv.exists() else None
            if STATIC_REPO:
                rec["github"] = f"https://github.com/{STATIC_REPO}/blob/{STATIC_BRANCH}/{rel_path_html}"

            exports.append(rec)

    return exports[:50]

# =============================================================================
# Routes
# =============================================================================

@app.get("/")
def index():
    exports = _collect_recent_exports()
    return render_template(
        "index.html",
        title=APP_TITLE,
        exports_list=exports,
        site_base=STATIC_SITE_BASE,
    )

@app.post("/process")
def process():
    """
    1) Read original + extra CSVs
    2) Align to original timeline, forward-fill
    3) Optional cutoff, y2 axis for setpoint column
    4) Save HTML + merged CSV to static/exports/YYYY-MM-DD/
    5) Optional GitHub push (html+csv) into STATIC_PATH
    6) Return ZIP containing both files for download
    """
    original = request.files.get("original_csv")
    extras   = request.files.getlist("extra_csvs")

    if not original or not original.filename:
        return "Original CSV is required", 400

    title = request.form.get("title", "CW Loop").strip() or "CW Loop"
    setpoint_col_req = request.form.get("setpoint_col", "").strip()
    cutoff = request.form.get("cutoff", "").strip()

    try:
        y1_min = float(request.form.get("y1_min", 0))
        y1_max = float(request.form.get("y1_max", 100))
    except Exception:
        y1_min, y1_max = 0, 100

    # --- Original CSV ---
    orig_df = _read_csv(original)
    tcol = _find_time_col(orig_df)
    if not tcol:
        return "Could not find a timestamp column in the Original CSV", 400

    base = _as_time_index(orig_df, tcol)
    base = _drop_seq_cols(base)

    # --- Join Extras ---
    accepted = 0
    for f in (extras or []):
        if not f or not f.filename:
            continue
        df = _read_csv(f)
        t2 = _find_time_col(df)
        if not t2:
            continue
        df = _as_time_index(df, t2)
        df = _drop_seq_cols(df)
        # avoid column collisions: add (filename) suffix if duplicates
        dup = [c for c in df.columns if c in base.columns]
        if dup:
            suffix = f" ({os.path.basename(f.filename)})"
            df = df.rename(columns={c: f"{c}{suffix}" for c in dup})
        base = base.join(df, how="left")
        accepted += 1

    # forward fill extras
    base = base.ffill()

    # optional cutoff
    if cutoff:
        try:
            cutoff_dt = pd.to_datetime(cutoff)
            base = base[base.index >= cutoff_dt]
        except Exception:
            pass

    # map stripped names -> real columns (for setpoint lookup)
    stripped_to_real: dict[str, str] = {}
    for c in base.columns:
        s = _strip_prefix(c)
        stripped_to_real.setdefault(s, c)

    setpoint_col = None
    if setpoint_col_req:
        if setpoint_col_req in base.columns:
            setpoint_col = setpoint_col_req
        elif setpoint_col_req in stripped_to_real:
            setpoint_col = stripped_to_real[setpoint_col_req]

    use_y2 = bool(setpoint_col)

    # --- Build Plotly Figure with stripped labels ---
    fig = go.Figure()
    for col in base.columns:
        label = _strip_prefix(col)
        if use_y2 and col == setpoint_col:
            fig.add_trace(go.Scatter(
                x=base.index, y=base[col], name=label, mode="lines", yaxis="y2"
            ))
        else:
            fig.add_trace(go.Scatter(
                x=base.index, y=base[col], name=label, mode="lines"
            ))

    fig.update_layout(
        title=title,
        xaxis_title="Time Stamp",
        yaxis=dict(title="Percent", range=[y1_min, y1_max]),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        template="plotly_white"
    )
    if use_y2:
        fig.update_layout(yaxis2=dict(overlaying="y", side="right", title="Setpoint"))

    html_bytes = fig.to_html(full_html=True, include_plotlyjs="cdn").encode("utf-8")

    merged = base.reset_index().rename(columns={"index": "Time Stamp"})
    csv_bytes = merged.to_csv(index=False).encode("utf-8-sig")

    # --- Save to static/exports/YYYY-MM-DD ---
    day_folder = _today_folder_name() if STATIC_DATED_SUBFOLDERS else ""
    out_dir = STATIC_EXPORT_ROOT / day_folder if day_folder else STATIC_EXPORT_ROOT
    out_dir.mkdir(parents=True, exist_ok=True)

    base_name = _sanitize_filename(title).replace(" ", "-")
    html_name = f"{base_name}.html"
    csv_name  = f"{base_name}.csv"

    (out_dir / html_name).write_bytes(html_bytes)
    (out_dir / csv_name).write_bytes(csv_bytes)

    rel_html = (out_dir / html_name).as_posix()  # static/exports/...
    rel_csv  = (out_dir / csv_name).as_posix()

    log(f"[process] extras uploaded: {len(extras)}; accepted with time: {accepted}")
    log(f"[process] saved: {rel_html}, {rel_csv}")

    # --- Optional GitHub push (for static site / GitHub browsing) ---
    pushed_ok = False
    if GH_TOKEN and STATIC_REPO and STATIC_BRANCH and STATIC_PATH:
        repo_html = f"{STATIC_PATH}/{day_folder}/{html_name}" if day_folder else f"{STATIC_PATH}/{html_name}"
        repo_csv  = f"{STATIC_PATH}/{day_folder}/{csv_name}"  if day_folder else f"{STATIC_PATH}/{csv_name}"
        msg = f"Publish {html_name} & {csv_name}"
        ok1 = _github_put_content(repo_html, html_bytes, msg)
        ok2 = _github_put_content(repo_csv,  csv_bytes,  msg)
        pushed_ok = ok1 and ok2
        log(f"[GitHub] push html={ok1} csv={ok2} base={STATIC_PATH}")

    # --- Return ZIP (html+csv) for immediate download ---
    import zipfile
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(html_name, html_bytes)
        z.writestr(csv_name,  csv_bytes)
    mem.seek(0)

    return send_file(
        mem,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{base_name}.zip"
    )

# =============================================================================
# Dev entry
# =============================================================================

if __name__ == "__main__":
    # local dev
    app.run(host="0.0.0.0", port=5000, debug=True)
