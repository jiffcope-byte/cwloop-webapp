@echo off
cd /d C:\path\to\cwloop-webapp

REM Stage all changes (you can change to `git add app.py` if you only want that file)
git add .

REM Commit with a generic message (can be overridden below)
set /p msg=Enter commit message: 
if "%msg%"=="" set msg=update app.py

git commit -m "%msg%"

REM Push to GitHub main branch
git push origin main

pause

cmd /k 