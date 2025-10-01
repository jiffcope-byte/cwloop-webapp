
import os, io, zipfile, datetime as dt, json
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
def allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED

def safe_read_csv(file_storage):
    # Robust CSV reader: handles BOM, autodetects delimiter.
    try:
        return pd.read_csv(file_storage, encoding='utf-8-sig', sep=None, engine='python')
    except Exception:
        file_storage.seek(0)
        return pd.read_csv(file_storage)

def parse_datetime_index(df):
    # Prefer 'Time Stamp' column; localize tz-naive to UTC.
    if "Time Stamp" in df.columns:
        ts = pd.to_datetime(df["Time Stamp"], errors="coerce")
        df = df.loc[ts.notna()].copy()
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        df.index = ts[ts.notna()].values
        return df.drop(columns=["Time Stamp"], errors="ignore")
    # fallback: first column
    ts = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    if ts.notna().any():
        df = df.loc[ts.notna()].copy()
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        df.index = ts[ts.notna()].values
        return df.drop(df.columns[0], axis=1)
    raise ValueError("No usable timestamp column found")

def merge_with_asof(base, others, tol_sec):
    base = base.sort_index()
    out = base.copy()
    for name, odf in others:
        odf = odf.sort_index().add_prefix(name + '::')
        left = out.reset_index().rename(columns={'index': '_ts'})
        right = odf.reset_index().rename(columns={'index': '_ts'})
        merged = pd.merge_asof(
            left.sort_values('_ts'),
            right.sort_values('_ts'),
            on='_ts',
            direction='nearest',
            tolerance=pd.Timedelta(seconds=int(tol_sec))
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
    import sys, traceback
    traceback.print_exc(file=sys.stderr)
    try:
        return render_template('error.html', message='Server error while processing. Check logs for details.'), 500
    except Exception:
        return jsonify(error='Server error while processing. Check logs for details.'), 500

# ---------- UI ----------
def list_exports():
    items = []
    for p in sorted(EXPORT_DIR.glob('*/result.json'), reverse=True):
        try:
            meta = json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            continue
        folder = p.parent.name
        base = '/static/exports/' + folder
        items.append({
            'title': meta.get('title', 'Export'),
            'when': meta.get('timestamp', ''),
            'view_url': base + '/view.html' if (p.parent / 'view.html').exists() else None,
            'csv_url':  base + '/merged.csv' if (p.parent / 'merged.csv').exists() else None,
            'zip_url':  base + '/bundle.zip' if (p.parent / 'bundle.zip').exists() else None,
        })
    return items

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', exports_list=list_exports())

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
            return render_template('error.html', message='Unsupported file: ' + f.filename), 400

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

        base_df = safe_read_csv(base_file)
        base_df = parse_datetime_index(base_df)

        others_list = []
        for f in others_files:
            if not f or not f.filename:
                continue
            odf = safe_read_csv(f)
            odf = parse_datetime_index(odf)
            stem = Path(f.filename).stem
            others_list.append((stem, odf))

        merged = merge_with_asof(base_df, others_list, tol_sec)

        if cutoff:
            try:
                cutoff_ts = pd.to_datetime(cutoff, utc=True)
                merged = merged.loc[merged.index <= cutoff_ts]
            except Exception:
                pass

        merged_out = merged.copy()
        merged_out.insert(
            0, 'Time Stamp',
            merged_out.index.tz_convert('UTC').tz_localize(None).astype('datetime64[ns]')
        )
        (folder / 'merged.csv').write_text(merged_out.to_csv(index=False), encoding='utf-8')

        cols = [c for c in merged.columns if merged[c].dtype != 'O']
        ts_list = merged_out['Time Stamp'].astype(str).tolist()

        traces = []
        for c in cols:
            arr = merged[c].astype('float64').where(merged[c].notna(), None).tolist()
            traces.append({'x': ts_list, 'y': arr, 'mode': 'lines', 'name': str(c), 'type': 'scatter'})

        yaxis = {}
        if y1min:
            yaxis['range'] = [float(y1min), None]
        if y1max:
            if 'range' in yaxis:
                yaxis['range'][1] = float(y1max)
            else:
                yaxis['range'] = [None, float(y1max)]

        layout = {'hovermode': 'x unified', 'xaxis': {'type': 'date'}, 'yaxis': yaxis}

        html = []
        html.append("<!doctype html><html><head><meta charset='utf-8'>")
        html.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
        html.append("<title>" + title + " – Viewer</title>")
        html.append("<script src='https://cdn.plot.ly/plotly-2.35.2.min.js'></script>")
        html.append("</head><body style='font-family:ui-sans-serif; background:#0b0f14; color:#e6edf3'>")
        html.append("<h2>" + title + "</h2>")
        html.append("<div id='chart' style='width:100%;height:80vh'></div>")
        html.append("<script>const data = " + json.dumps(traces) + ";")
        html.append("const layout = " + json.dumps(layout) + ";")
        html.append("Plotly.newPlot('chart', data, layout, {responsive:true});</script>")
        html.append("</body></html>")

        (folder / 'view.html').write_text("
".join(html), encoding='utf-8')

        with zipfile.ZipFile(folder / 'bundle.zip', 'w', zipfile.ZIP_DEFLATED) as z:
            for p in folder.glob('*'):
                if p.name != 'bundle.zip':
                    z.write(p, p.name)

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
        try:
            return render_template('error.html', message='Processing failed. See server logs for details.'), 400
        except Exception:
            return jsonify(error='Processing failed. See server logs for details.'), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
