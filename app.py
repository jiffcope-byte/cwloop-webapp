import os, io, zipfile, datetime as dt
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for
from werkzeug.utils import secure_filename
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / 'static' / 'exports'
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED = {'csv'}

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

def allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED

@app.route('/healthz')
def healthz(): return ('ok', 200)

def list_exports():
    items = []
    for p in sorted(EXPORT_DIR.glob('*/result.json'), reverse=True):
        try:
            meta = p.read_text(encoding='utf-8')
            import json
            meta = json.loads(meta)
        except Exception:
            continue
        title = meta.get('title', 'Export')
        stamp = meta.get('timestamp', '')
        folder = p.parent.name
        base = f'/static/exports/{folder}'
        items.append({
            'title': title,
            'when': stamp,
            'view_url': f'{base}/view.html' if (p.parent / 'view.html').exists() else None,
            'csv_url': f'{base}/merged.csv' if (p.parent / 'merged.csv').exists() else None,
            'zip_url': f'{base}/bundle.zip' if (p.parent / 'bundle.zip').exists() else None,
        })
    return items

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', exports_list=list_exports())

def parse_datetime_index(df):
    for c in df.columns:
        lc = c.lower().strip()
        if lc in ('time stamp','timestamp','time','datetime','date time'):
            ts = pd.to_datetime(df[c], errors='coerce', utc=True).dropna()
            if len(ts):
                df = df.loc[ts.index].copy()
                df.index = ts.values
                df = df.drop(columns=[c])
                return df
    c = df.columns[0]
    ts = pd.to_datetime(df[c], errors='coerce', utc=True)
    if ts.notna().any():
        df = df.loc[ts.notna()].copy()
        df.index = ts[ts.notna()].values
        df = df.drop(columns=[c])
        return df
    raise ValueError('No datetime column found')

def merge_with_asof(base, others, tolerance_seconds):
    base = base.sort_index()
    out = base.copy()
    for name, odf in others:
        odf = odf.sort_index()
        odf = odf.add_prefix(f'{name}::')
        left = out.reset_index().rename(columns={'index':'_ts'})
        right = odf.reset_index().rename(columns={'index':'_ts'})
        merged = pd.merge_asof(
            left.sort_values('_ts'),
            right.sort_values('_ts'),
            on='_ts',
            direction='nearest',
            tolerance=pd.Timedelta(seconds=tolerance_seconds)
        )
        merged = merged.set_index('_ts')
        out = merged
    return out

@app.route('/process', methods=['POST'])
def process():
    if 'base_csv' not in request.files or request.files['base_csv'].filename == '':
        return ('Missing Original CSV (base_csv). Use the form on /.', 400)

    base_file = request.files['base_csv']
    base_name = secure_filename(base_file.filename)
    if not allowed(base_name): return ('Original CSV must be .csv', 400)

    others_files = request.files.getlist('extra_csvs')
    others_clean = []
    for f in others_files:
        if not f or f.filename == '': continue
        fname = secure_filename(f.filename)
        if not allowed(fname): return (f'Unsupported file: {fname}', 400)
        others_clean.append((fname, f))

    title = request.form.get('title') or 'CW Loop'
    tol_s = int(request.form.get('tolerance') or '5')
    y1min = request.form.get('y1min') or ''
    y1max = request.form.get('y1max') or ''
    setpoint = request.form.get('setpoint') or ''
    cutoff = request.form.get('cutoff') or ''

    stamp = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
    folder = EXPORT_DIR / stamp
    folder.mkdir(parents=True, exist_ok=True)

    base_df = pd.read_csv(base_file)
    base_df = parse_datetime_index(base_df)

    others = []
    for fname, fs in others_clean:
        odf = pd.read_csv(fs)
        odf = parse_datetime_index(odf)
        others.append((Path(fname).stem, odf))

    merged = merge_with_asof(base_df, others, tol_s)

    if cutoff.strip():
        try:
            cutoff_ts = pd.to_datetime(cutoff, utc=True)
            merged = merged.loc[merged.index <= cutoff_ts]
        except Exception:
            pass

    merged_out = merged.copy()
    merged_out.insert(0, 'Time Stamp', merged_out.index.tz_convert(None).astype('datetime64[ns]'))
    merged_out.to_csv(folder / 'merged.csv', index=False)

    # Make Plotly HTML (CDN)
    cols = [c for c in merged.columns if merged[c].dtype != 'O']
    ts_list = merged_out['Time Stamp'].astype(str).tolist()

    html_parts = []
    html_parts.append("<!doctype html><html><head><meta charset='utf-8'><title>{}</title>".format(title))
    html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    html_parts.append("<script src='https://cdn.plot.ly/plotly-2.35.2.min.js'></script></head><body>")
    html_parts.append("<div id='chart' style='width:100%;height:80vh'></div>")
    html_parts.append("<script>const X = {};</script>".format(ts_list))

    series_js = ["const data = [];"]
    for c in cols:
        arr = merged[c].astype('float64').where(merged[c].notna(), None).tolist()
        safe = c.replace("'", "\'")
        series_js.append("data.push({x:X,y:%s,mode:'lines',name:'%s'});" % (arr, safe))

    layout_js = "const layout={margin:{l:60,r:20,t:20,b:40},hovermode:'x unified',xaxis:{type:'date'}};"
    html_parts.append("<script>" + "\n".join(series_js) + "\nPlotly.newPlot('chart', data, layout,{responsive:true});</script>")
    html_parts.append("</body></html>")
    (folder / 'view.html').write_text("\n".join(html_parts), encoding='utf-8')

    # Bundle
    zpath = folder / 'bundle.zip'
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as z:
        for p in folder.glob('*'):
            if p.name != 'bundle.zip':
                z.write(p, p.name)

    import json
    (folder / 'result.json').write_text(json.dumps({
        'title': title, 'timestamp': stamp,
        'params': {'tolerance': tol_s, 'y1min': y1min, 'y1max': y1max, 'setpoint': setpoint, 'cutoff': cutoff},
        'columns': cols
    }, indent=2), encoding='utf-8')

    return redirect(url_for('index'))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
