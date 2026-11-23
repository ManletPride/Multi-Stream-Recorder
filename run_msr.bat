@echo off
rem Multi-Stream Recorder launcher (no console window).
rem
rem Data folder (config.ini, channels.json, cookies.txt, window state):
rem   - %MSR_DATA% if set (e.g. setx MSR_DATA "D:\MSR"), otherwise
rem   - this repo folder.
rem Optional untracked run_msr.local.bat next to this file can set MSR_DATA
rem or MSR_PYTHON (your own pythonw.exe: skips MSR's .venv). It is git-ignored.
setlocal
cd /d "%~dp0"
if exist "%~dp0run_msr.local.bat" call "%~dp0run_msr.local.bat"
if defined MSR_DATA (
  if not exist "%MSR_DATA%\" (
    echo MSR_DATA folder not found: %MSR_DATA%
    pause
    exit /b 1
  )
)
rem Default: run_msr.pyw sets up / uses MSR's own .venv (first launch installs it, see README).
if not defined MSR_PYTHON (
  where pyw >nul 2>&1 && (
    py -3 -c "import sys" >nul 2>&1 && (
      start "" pyw -3 "%~dp0run_msr.pyw" %*
      exit /b 0
    )
  )
  pythonw -c "import sys" >nul 2>&1 && (
    start "" pythonw "%~dp0run_msr.pyw" %*
    exit /b 0
  )
  py -3 -c "import sys" >nul 2>&1 && (
    start "" py -3 "%~dp0run_msr.pyw" %*
    exit /b 0
  )
  python -c "import sys" >nul 2>&1 && (
    start "" python "%~dp0run_msr.pyw" %*
    exit /b 0
  )
  echo Python 3 was not found on PATH ^(checked pyw, pythonw, py -3, python^).
  echo Install Python 3 from https://www.python.org/downloads/ and check "Add python.exe to PATH".
  pause
  exit /b 1
)
if defined MSR_DATA (
  start "" "%MSR_PYTHON%" "%~dp0Multi-Stream-Recorder.py" --data-dir "%MSR_DATA%" %*
) else (
  start "" "%MSR_PYTHON%" "%~dp0Multi-Stream-Recorder.py" %*
)
