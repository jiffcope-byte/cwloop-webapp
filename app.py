import os, io, zipfile, datetime as dt
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / 'static' / 'exports'
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED = {'csv'}

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25 MB

def allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED

@app.errorhandler(413)
def too_large(e): return ('File too large', 413)

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

@app.route('/process', methods=['POST'])
def process():
    if 'base_csv' not in request.files or request.files['base_csv'].filename == '':
        return ('Missing Original CSV (base_csv). Use the form on /.', 400)

    base_file = request.files['base_csv']
    base_name = secure_filename(base_file.filename)
    if not allowed(base_name): return ('Original CSV must be .csv', 400)

    others = request.files.getlist('extra_csvs')
    other_paths = []
    for f in others:
        if not f or f.filename == '': continue
        fname = secure_filename(f.filename)
        if not allowed(fname): return (f'Unsupported file: {fname}', 400)
        other_paths.append((fname, f.read()))

    title = request.form.get('title') or 'CW Loop'
    tol = request.form.get('tolerance') or '5'
    y1min = request.form.get('y1min') or '0'
    y1max = request.form.get('y1max') or '100'
    setpoint = request.form.get('setpoint') or ''
    cutoff = request.form.get('cutoff') or ''

    stamp = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
    folder = EXPORT_DIR / stamp
    folder.mkdir(parents=True, exist_ok=True)

    base_path = folder / 'base.csv'
    base_file.stream.seek(0)
    base_path.write_bytes(base_file.read())

    for fname, data in other_paths:
        (folder / f'extra_{fname}').write_bytes(data)

    (folder / 'merged.csv').write_bytes(base_path.read_bytes())

    (folder / 'view.html').write_text(
        "<!doctype html><meta charset='utf-8'><title>View</title>"
        "<h1>Placeholder viewer</h1><p>Replace with Plotly viewer.</p>", encoding='utf-8'
    )

    zpath = folder / 'bundle.zip'
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as z:
        for p in folder.glob('*'):
            if p.name != 'bundle.zip':
                z.write(p, p.name)

    import json
    (folder / 'result.json').write_text(json.dumps({
        'title': title, 'timestamp': stamp,
        'params': {'tolerance': tol, 'y1min': y1min, 'y1max': y1max, 'setpoint': setpoint, 'cutoff': cutoff}
    }, indent=2), encoding='utf-8')

    return redirect(url_for('index'))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
