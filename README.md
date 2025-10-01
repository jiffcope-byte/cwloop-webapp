CWLoop Complete Example (Fixes Dark Mode + /process 500)
========================================================

Run locally:
------------
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py

Open http://127.0.0.1:5000/

Deploy to Render (Python web service):
--------------------------------------
- Connect this folder/repo
- Ensure Build uses requirements.txt
- Start command comes from Procfile (or set to: gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120)
- Health check path: /healthz

Notes:
------
- Dark mode is guaranteed by static/darkmode-early.js, included first in <head>.
- /process validates uploads, logs exceptions to stderr, and returns JSON 400s for user errors.
- Replace the pandas preview with your actual processing logic.
