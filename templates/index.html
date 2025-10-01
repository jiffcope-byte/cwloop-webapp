<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Trend Merge & Viewer</title>
    <script src="/static/darkmode-early.js"></script>
    <link rel="stylesheet" href="/static/style.css" />
  </head>
  <body>
    <div class="container">
      <header class="topbar">
        <h1>Trend Merge & Viewer</h1>
        <button id="themeToggle" class="btn">Toggle theme</button>
      </header>

      <section class="panel">
        <form id="mergeForm" action="/process" method="post" enctype="multipart/form-data">
          <div class="grid-2">
            <div class="form-group">
              <label>Original CSV</label>
              <input id="baseCsv" type="file" name="base_csv" accept=".csv" required />
              <div class="hint">Use this file’s timestamps as the global X-axis.</div>
            </div>

            <div class="form-group">
              <label>Title</label>
              <input type="text" name="title" placeholder="CW Loop" />
            </div>
          </div>

          <div class="form-group">
            <label>Additional CSVs (you can select multiple)</label>
            <input type="file" name="extra_csvs" accept=".csv" multiple />
            <div class="hint">Aligned by nearest timestamp within tolerance; forward-fills small gaps.</div>
          </div>

          <div class="grid-2">
            <div class="form-group">
              <label>Tolerance (seconds)</label>
              <input type="number" name="tolerance" value="5" />
            </div>
            <div class="form-group">
              <label>Y1 Min</label>
              <input type="number" name="y1min" placeholder="auto" />
            </div>
          </div>

          <div class="grid-2">
            <div class="form-group">
              <label>Y1 Max</label>
              <input type="number" name="y1max" placeholder="auto" />
            </div>
            <div class="form-group">
              <label>Setpoint Column (optional)</label>
              <input type="text" name="setpoint" placeholder="e.g., Active CW Flow Setpoint" />
            </div>
          </div>

          <div class="form-group">
            <label>Cutoff Datetime (optional)</label>
            <input type="text" name="cutoff" placeholder="YYYY-MM-DD HH:MM:SS" />
          </div>

          <div class="actions">
            <button id="processBtn" class="btn" type="submit" disabled>Process</button>
            <button class="btn-outline" type="reset">Reset</button>
            <span class="tip">Tip: Use this form; browsing directly to <code>/process</code> won’t work.</span>
          </div>
        </form>
      </section>

      <section class="panel">
        <h2>Recent Exports</h2>
        {% if exports_list and exports_list|length %}
          <div class="exports">
            {% for x in exports_list %}
              <div class="export-item">
                <div class="export-title">{{ x.title }}</div>
                <div class="export-meta">{{ x.when }}</div>
                <div class="export-actions">
                  {% if x.view_url %}<a class="btn-sm" href="{{ x.view_url }}" target="_blank">View</a>{% endif %}
                  {% if x.csv_url %}<a class="btn-sm" href="{{ x.csv_url }}" target="_blank">CSV</a>{% endif %}
                  {% if x.zip_url %}<a class="btn-sm" href="{{ x.zip_url }}" target="_blank">ZIP</a>{% endif %}
                </div>
              </div>
            {% endfor %}
          </div>
        {% else %}
          <div class="hint">No exports yet.</div>
        {% endif %}
      </section>
    </div>

    <script>
      // Theme toggle
      document.getElementById('themeToggle').addEventListener('click', () => {
        const root = document.documentElement;
        const isDark = root.classList.toggle('dark');
        try { localStorage.setItem('theme', isDark ? 'dark' : 'light'); } catch(e) {}
      });

      // Prevent accidental 500s: disable Process until a file is chosen
      const baseCsv = document.getElementById('baseCsv');
      const processBtn = document.getElementById('processBtn');
      baseCsv.addEventListener('change', () => {
        processBtn.disabled = !baseCsv.files.length;
      });
    </script>
  </body>
</html>
