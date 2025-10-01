import os, io, zipfile, datetime as dt
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, jsonify
from werkzeug.utils import secure_filename
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / 'static' / 'exports'
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED = {'csv'}

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

# ---------- helpers ----------
def allowed(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED

def parse_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    # prefer common timestamp headers
    for c in df.columns:
        lc = c.lower().strip()
        if lc in ('time stamp', 'timestamp', 'time', 'datetime', 'date time'):
            ts = pd.to_datetime(df[c], errors='coerce', utc=True)
            df = df.loc[ts.notna()].copy()
            df.index = ts[ts.notna()].values
            return df.drop(columns=[c], errors='ignore')
    # fallback: first column
    c = df.columns[0]
    ts = pd.to_datetime(df[c], errors='coerce', utc=True)
    if ts.notna().any():
        df = df.loc[ts.notna()].copy()
        df.index = ts[ts.notna()].values
        return df.drop(columns=[c], errors='ignore')
    raise ValueError('No datetime-like column found')

def merge_with_asof(base: pd.DataFrame, others: list[tuple[str, pd.DataFrame]], tol_sec: int) -> pd.DataFrame:
    base = base.sort_index()
    out = base.copy()
    for name, odf in others:
        odf = odf.sort_index().add_prefix(f'{name}::')
        left = out.reset_index().rename(columns={'index': '_ts'})
        right = odf.reset_index().rename(columns={'index': '_ts'})
        merged = pd.merge_asof(
            left.sort_values('_ts'),
            right.sort_values('_ts'),
            on='_ts',
            direction='nearest',
            tolerance=pd.Timedelta(seconds=tol_sec)
        ).set_index('_ts')
        out = merged
    return out

# ---------- health + errors ----------
@app.route('/healthz')
def healthz():
    return 'ok', 200

@app.errorhandler(413)
def too_large(e):
    return jsonify(error='File too large (over MAX_CONTENT_LENGTH).'), 413

@app.errorhandler(Exception)
def handle_any_error(e):
    # log to stderr for Render logs
    import sys, traceback
    traceback.print_exc(file=sys.stderr)
    return render_template('error.html', message='Server error while processing. Check logs for details.'), 500

# ---------- UI ----------
@app.route('/', methods=['GET'])
def index():
    items = []
    for p in sorted(EXPORT_DIR.glob('*/result.json'), reverse=True):
        try:
            meta_txt = p.read_text(encoding='utf-8')
            import json
            meta = json.loads(meta_txt)
        except Exception:
            continue
        folder = p.parent.name
        base = f'/static/exports/{folder}'
        items.append({
            'title': meta.get('title', 'Export'),
            'when': meta.get('timestamp', ''),
            'view_url': f'{base}/view.html' if (p.parent / 'view.html').exists() else None,
            'csv_url': f'{base}/merged.csv' if (p.parent / 'merged.csv').exists() else None,
            'zip_url': f'{base}/bundle.zip' if (p.parent / 'bundle.zip').exists() else None,
        })
    return render_template('index.html', exports_list=items)

@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'GET':
        return redirect(url_for('index'))

    if 'base_csv' not in request.files:
        return render_template('error.html', message="Missing 'Original CSV'."), 400

    base_file = request.files['base_csv']
    if base_file.filename == '':
        return render_template('error.html', message='No file selected for Original CSV.'), 400
    if not allowed(base_file.filename):
        return render_template('error.html', message='Original CSV must be a .csv file.'), 400

    others_files = request.files.getlist('extra_csvs') or []
    for f in others_files:
        if f and f.filename and not allowed(f.filename):
            return render_template('error.html', message=f'Unsupported file: {f.filename}'), 400

    title = (request.form.get('title') or 'CW Loop').strip()
    tol_sec = int((request.form.get('tolerance') or '5').strip() or 5)
    y1min = (request.form.get('y1min') or '').strip()
    y1max = (request.form.get('y1max') or '').strip()
    setpoint = (request.form.get('setpoint') or '').strip()
    cutoff = (request.form.get('cutoff') or '').strip()

    try:
        stamp = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
        folder = EXPORT_DIR / stamp
        folder.mkdir(parents=True, exist_ok=True)

        base_df = pd.read_csv(base_file)
        base_df = parse_datetime_index(base_df)

        others: list[tuple[str, pd.DataFrame]] = []
        for f in others_files:
            if not f or not f.filename:
                continue
            odf = pd.read_csv(f)
            odf = parse_datetime_index(odf)
            from pathlib import Path as _P
            others.append((_P(f.filename).stem, odf))

        merged = merge_with_asof(base_df, others, tol_sec)

        if cutoff:
            try:
                cutoff_ts = pd.to_datetime(cutoff, utc=True)
                merged = merged.loc[merged.index <= cutoff_ts]
            except Exception:
                pass

        merged_out = merged.copy()
        merged_out.insert(0, 'Time Stamp', merged_out.index.tz_convert(None).astype('datetime64[ns]'))
        (folder / 'merged.csv').write_text(merged_out.to_csv(index=False), encoding='utf-8')

        # Plotly viewer (CDN)
        cols = [c for c in merged.columns if merged[c].dtype != 'O']
        ts_list = merged_out['Time Stamp'].astype(str).tolist()

        html = []
        html += [
            "<!doctype html><html><head><meta charset='utf-8'>",
            f"<title>{title} – Viewer</title>",
            "<meta name='viewport' content='width=device-width, initial-scale=1'>",
            "<script src='https://cdn.plot.ly/plotly-2.35.2.min.js'></script>",
            "</head><body style='font-family:ui-sans-serif; background:#0b0f14; color:#e6edf3'>",
            f"<h2>{title}</h2>",
            "<div id='chart' style='width:100%;height:80vh'></div>",
            f"<script>const X = {ts_list!r}; const data = [];</script>"
        ]
        for c in cols:
            arr = merged[c].astype('float64').where(merged[c].notna(), None).tolist()
            safe = c.replace(\"'\", \"\\'\")
            html.append(f\"<script>data.push({{x:X,y:{arr!r},mode:'lines',name:'{safe}'}});</script>\")
        yaxis = {}
        if y1min:
            yaxis.setdefault('range', [float(y1min), None])
        if y1max:
            if 'range' in yaxis:
                yaxis['range'][1] = float(y1max)
            else:
                yaxis['range'] = [None, float(y1max)]
        html.append(f\"<script>Plotly.newPlot('chart', data, {{hovermode:'x unified', xaxis:{{type:'date'}}, yaxis:{yaxis}}}, {{responsive:true}});</script>\")
        html.append(\"</body></html>\")
        (folder / 'view.html').write_text('\\n'.join(html), encoding='utf-8')

        with zipfile.ZipFile(folder / 'bundle.zip', 'w', zipfile.ZIP_DEFLATED) as z:
            for p in folder.glob('*'):
                if p.name != 'bundle.zip':
                    z.write(p, p.name)

        import json
        (folder / 'result.json').write_text(json.dumps({
            'title': title,
            'timestamp': stamp,
            'params': {'tolerance': tol_sec, 'y1min': y1min, 'y1max': y1max, 'setpoint': setpoint, 'cutoff': cutoff},
            'columns': cols
        }, indent=2), encoding='utf-8')

        return redirect(url_for('index'))

    except Exception:
        import sys, traceback
        traceback.print_exc(file=sys.stderr)
        return render_template('error.html', message='Processing failed. See server logs for details.'), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
