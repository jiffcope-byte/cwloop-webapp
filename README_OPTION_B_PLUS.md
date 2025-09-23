
# Option B+ — GitHub Static push + Copy links + Dated subfolders + Google Drive

This patch enhances Option B with:
- **Copy-link buttons** on the homepage for the most recent upload
- **Dated subfolders** in your static repo: `YYYY/MM/DD/slug-YYYYmmdd-HHMMSS.html`
- **Google Drive** upload (service account), public links

## New/updated files
- `app.py` — enhanced backend
- `templates/index.html` — copy buttons and nicer listing
- `requirements.txt` — adds Google Drive client libs

## Environment variables
- GitHub static site:
  - `GH_TOKEN` — GitHub fine-grained PAT with contents:write
  - `STATIC_REPO` — e.g. `yourname/cwloop-viewers`
  - `STATIC_BRANCH` — `main`
  - `STATIC_PATH` — optional subfolder inside the repo
  - `STATIC_SITE_BASE` — `https://<your-static-site>.onrender.com` (for friendly links)
  - `STATIC_DATED_SUBFOLDERS` — `true`/`false` (default `true`)

- Google Drive (optional):
  - `GDRIVE_SA_JSON_B64` — base64 of your service-account JSON
  - `GDRIVE_FOLDER_ID` — folder ID shared with the service account (Editor)

## Install
Replace your files with these, commit and push.
```bash
git add app.py templates/index.html requirements.txt
git commit -m "feat: Option B+ (GitHub push + copy links + dated subfolders + Drive)"
git push
```
