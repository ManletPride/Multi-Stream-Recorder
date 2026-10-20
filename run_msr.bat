@echo off
rem Multi-Stream Recorder launcher (no console window).
rem
rem Data folder (config.ini, channels.json, cookies.txt, window state):
rem   - %MSR_DATA% if set (e.g. setx MSR_DATA "D:\MSR"), otherwise
rem   - this repo folder.
rem Optional untracked run_msr.local.bat next to this file can set MSR_DATA
rem or MSR_PYTHON (e.g. a venv's pythonw.exe). It is git-ignored.
setlocal
cd /d "%~dp0"
if exist "%~dp0run_msr.local.bat" call "%~dp0run_msr.local.bat"
if not defined MSR_PYTHON (
  if exist "%~dp0.venv\Scripts\pythonw.exe" (
    set "MSR_PYTHON=%~dp0.venv\Scripts\pythonw.exe"
  ) else (
    set "MSR_PYTHON=pythonw"
  )
)
if defined MSR_DATA (
  if not exist "%MSR_DATA%\" (
    echo MSR_DATA folder not found: %MSR_DATA%
    pause
    exit /b 1
  )
  start "" "%MSR_PYTHON%" "%~dp0Multi-Stream-Recorder.py" --data-dir "%MSR_DATA%" %*
) else (
  start "" "%MSR_PYTHON%" "%~dp0Multi-Stream-Recorder.py" %*
)
