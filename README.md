# Trend Merge & Viewer (Flask)

- Upload an **Original CSV** and any number of **Additional CSVs**
- Aligns additional values to the original timestamps (nearest within tolerance)
- Outputs a merged CSV + a standalone Plotly HTML viewer (y1=0–100, y2 for setpoint)

## Deploy to Render (Free)

1. Push this folder to a new GitHub repo.
2. Go to https://render.com → **New** → **Web Service** → connect your repo.
3. Render reads `render.yaml` which sets:
   - Build: `pip install -r requirements.txt`
   - Start: `gunicorn app:app`
   - Plan: **Free**
4. Click **Create Web Service**. After build, your app is live at a public URL.
