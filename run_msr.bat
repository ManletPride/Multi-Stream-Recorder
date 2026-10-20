@echo off
rem Multi-Stream Recorder launcher: code from this repo, data from MSR_DATA.
rem MSR_DATA holds config.ini, channels.json, cookies.txt, window_state.json,
rem kick_channel_ids.json. Logs/recordings go to streams_dir in that config.ini.
rem Edit these two lines if your folders or Python differ. No secrets here.
set "MSR_DATA=E:\streams"
set "MSR_PYTHON=pythonw"

cd /d "%~dp0"
if not exist "%MSR_DATA%\config.ini" (
  echo config.ini not found in %MSR_DATA% - check MSR_DATA in run_msr.bat
  pause
  exit /b 1
)
start "" "%MSR_PYTHON%" "%~dp0Multi-Stream-Recorder.py" --data-dir "%MSR_DATA%" %*
