import os
from flask import Flask, render_template, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename

app = Flask(__name__, static_folder='static', template_folder='templates')

# Limits (adjust if needed)
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25 MB
ALLOWED_EXTENSIONS = {'csv', 'txt', 'json'}

def allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.errorhandler(413)
def too_large(e):
    return jsonify(error='File too large'), 413

@app.errorhandler(400)
def bad_request(e):
    return jsonify(error='Bad request'), 400

@app.errorhandler(500)
def internal(e):
    return jsonify(error='Internal server error'), 500

@app.route('/healthz')
def healthz():
    return 'ok', 200

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    # Expecting multipart/form-data with a 'file' field
    if 'file' not in request.files:
        return jsonify(error="Missing 'file' field"), 400

    f = request.files['file']
    if f.filename == '':
        return jsonify(error='No selected file'), 400

    if not allowed(f.filename):
        return jsonify(error='Unsupported file type'), 400

    filename = secure_filename(f.filename)
    tmp_path = os.path.join('/tmp', filename)
    f.save(tmp_path)

    try:
        # Example CSV handling (replace with your real logic)
        import pandas as pd
        df = pd.read_csv(tmp_path)
        head = df.head(10)
        result = {
            'rows': int(len(df)),
            'columns': list(map(str, df.columns)),
            'preview': head.to_dict(orient='records')
        }
        return jsonify(result=result), 200
    except Exception as ex:
        # Log to Render logs
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        return jsonify(error='Processing failed'), 500
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
