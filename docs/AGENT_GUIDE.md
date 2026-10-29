# Multi-Stream Recorder: quick start for agents

This guide is for a script, bot or AI agent that drives Multi-Stream Recorder (MSR) with `msr_ctl.py`. The full command list is generated from the CLI itself: run `python msr_ctl.py help --json`. The design and protocol are in `AGENTS.md` (Agent control).

## 1. Find MSR

- **Data folder.** This folder holds `config.ini`, `channels.json`, `msr_control.json` and `msr_status.json`. `msr_ctl.py` looks for it in this order: `--data-dir`, then `MSR_HOME`, then `MSR_DATA`, then the current folder. Set `MSR_HOME` once so you don't have to pass `--data-dir` on every call.
- **Is it running?** `python msr_ctl.py ping`. Exit 0 means a copy answers. Exit 3 means none is running: roster, settings, library and history commands then edit the files directly, and session commands (start, stop, wait...) fail.
- **`msr_control.json`** exists only while a copy runs. It holds `port`, `token`, `pid`, `mode` (`gui` or `headless`) and `api_version`. `msr_ctl.py` reads it for you. Never copy the token anywhere.
- **`msr_status.json`** is rewritten every few seconds. Read it for a cheap snapshot (channels, finished recordings, health) without sending a command.
- **Compatibility.** Every reply surface carries `api_version`. If `msr_ctl.py` and the running copy disagree, the command exits 7: use the `msr_ctl.py` that sits next to the running copy.

## 2. Start it headless

```powershell
python Multi-Stream-Recorder.py --headless --data-dir "D:\msr-data"
```

Recording of the enabled channels starts at once. It runs until `python msr_ctl.py quit` (or Ctrl+C in its console). Only one copy can run per recordings folder.

## 3. Core flows

**Add a channel and record it**
```
python msr_ctl.py add kick:knut twitch:someone      # platform:name, a bare Kick name or a channel URL
python msr_ctl.py wait --event live --channel knut --timeout 3600
# returns at once ("already": true) if knut is already recording
python msr_ctl.py --json status                      # recording, file, size, duration per channel
```
Adding or enabling a channel starts watching it at once. You don't need `start` unless someone ran `stop`.

**Stop one recording and get the file**
```
python msr_ctl.py --json record-stop knut            # channels[].result: stopped_recording | was_idle
python msr_ctl.py wait --event finished --channel knut --timeout 900
```
After a stop, the raw `.ts` is remuxed to `.mp4` in the background, usually within seconds of the stop plus the remux time. Meanwhile the channel shows `finishing` (`waiting_for_cleanup`, then `remuxing`). The `finished` event carries `path` and `remux_ok`. A `finished` wait for a stopped channel starts at its stop, so a finish that happened before you called `wait` is returned at once. You can also pass `--after <finish_after_seq>` from the `record-stop` reply. `was_idle` means nothing was recording: no `finished` will come. `remux_ok: null` with a `note` means no file was left to remux (for example, it was too short).

**Wait for anything**
```
python msr_ctl.py wait --timeout 600                 # live | stopped | remux_started | finished | error | offline | ended
python msr_ctl.py wait --event offline --channel knut --timeout 3600
# offline: the stream ended on its own (was_recording, duration_seconds, in_grace_period);
# ended: the reconnect grace period ran out without the stream coming back.
# stopped fires only when a recording was actually stopped (record-stop / remove of an idle channel: no event).
python msr_ctl.py events --after 41                  # what happened since seq 41
```
Each event has a `seq`. Pass the last `seq` you saw as `--after` and nothing is missed between calls. Exit 6 means nothing happened before the timeout.

**List recordings**
```
python msr_ctl.py --json recordings --since 24h      # path, channel, size, duration, finished_at
python msr_ctl.py --json recordings pending          # raw files waiting for a remux, PendingDeletion
```

**Free disk space** (always ask the user before step 3)
```
python msr_ctl.py --json library suggest --free 200          # 1. read only: what could go
python msr_ctl.py --json library plan pending --path "<file>" ...   # 2. dry run of exactly those files
python msr_ctl.py library run pending --path "<file>" ... --yes     # 3. only with the user's OK
python msr_ctl.py library restore --last --yes                       # undo the last move
```
`pending` moves files to `PendingDeletion` (reversible, purged after `auto_purge_days`). Prefer it to `permanent`.

**Troubleshoot**
```
python msr_ctl.py health          # tools, disk space vs min_disk_space_gb, cookie expiry, Fishtank login, problems
python msr_ctl.py logs -n 100 --filter error
python msr_ctl.py --json status   # per channel: error, last_error, errors, restarts, last_live
```

## 4. Output and exit codes

Add `--json` (before the command) to get the full reply: `{"ok": true, "result": ...}` or `{"ok": false, "code": "...", "error": "..."}`. Errors go to stderr.

| Exit | Meaning |
| --- | --- |
| 0 | ok |
| 1 | refused or failed (see `code`) |
| 2 | usage error |
| 3 | not running (or not answering) |
| 4 | wrong token |
| 5 | needs confirmation (`--yes` / `--confirm WORD`) |
| 6 | timed out (`wait`, or no reply in time) |
| 7 | control API version mismatch |
| 130 | interrupted (Ctrl+C) |

## 5. Safety rules

- Every command has a safety level in `help --json`: `read`, `write` (safe, reversible), `session` (starts or stops recording, or the app) and `destructive`.
- Destructive commands need `--yes`. A permanent delete needs `--confirm <word>`, where the word comes from `library plan permanent ...`. Never add `--yes` or `--confirm` on your own initiative: show the user the plan (files, size, channels) and get their OK first.
- Never delete recordings, empty `PendingDeletion`, restore history or install an update without the user's explicit approval.
- `stop` and `quit` end every recording. Prefer `record-stop <channel>` for a single channel.
- Secrets: `config get` masks passwords. Set them with `config set Section.key --stdin` so they stay out of argv and shell history. Never print `msr_control.json` or cookie files.

## 6. Don't

- Don't edit `channels.json` or `config.ini` by hand while MSR runs. Use the commands, which save atomically and keep a history.
- Don't start a second copy for the same recordings folder, and don't kill MSR's processes. Use `stop` / `quit` so recordings are remuxed.
- Don't touch files under `Recorded/` while a channel records. Library commands refuse busy channels for you.
- Don't poll `status` in a tight loop. Use `wait`, or read `msr_status.json`.
- Don't parse the human output. Use `--json`.
