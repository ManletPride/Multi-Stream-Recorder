"""Updates dialog: installed/latest/status per dependency, Check now, Update selected, Update all.

Network and pip run in a worker thread; the worker only puts results on a queue that after()
drains, so Tk is touched from the main thread only.
"""

from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import ttk

from msr import dialogs, depsupdate

STATUS_TEXT = {
    "up_to_date": "Up to date", "update_available": "Update available", "not_installed": "Not installed",
    "unknown": "Could not check", "unchecked": "Not checked yet", "external": "Managed outside MSR",
}
COLUMNS = (("label", "Item", 170), ("installed", "Installed", 170), ("latest", "Latest", 110),
           ("status", "Status", 140), ("source", "Source / note", 330))


def open_deps_dialog(parent, *, config, state_file, ffmpeg_path="ffmpeg", on_msr_update=None,
                     on_badge=None, check_now=True):
    """Open (or raise) the dialog. on_msr_update() starts the app's own updater; on_badge(names)
    tells the main window which items have updates."""
    existing = getattr(parent, "_msr_deps_dialog", None)
    if existing is not None and existing.winfo_exists():
        existing.deiconify(); existing.lift(); existing.focus_force()
        return existing
    win, frame = dialogs.new_dialog(parent, "Updates")
    parent._msr_deps_dialog = win
    q: queue.Queue = queue.Queue()
    state = {"rows": depsupdate.rows(False, ffmpeg_path=ffmpeg_path), "busy": False, "job": None}

    ttk.Label(frame, text="Tools and packages Multi-Stream Recorder uses. Updates install in the background; "
                          "recordings already running keep their version, new recordings use the new one.",
              wraplength=900, justify="left").pack(anchor="w", pady=(0, 8))
    tree = ttk.Treeview(frame, columns=[c[0] for c in COLUMNS], show="headings", height=11, selectmode="extended")
    for key, title, width in COLUMNS:
        tree.heading(key, text=title)
        tree.column(key, width=width, anchor="w", stretch=key == "source")
    tree.pack(fill="both", expand=True)
    msg = ttk.Label(frame, text="", wraplength=900, justify="left")
    msg.pack(anchor="w", fill="x", pady=(8, 4))

    daily = tk.BooleanVar(value=config.getboolean("Updates", "daily_check", fallback=True))

    def _daily_changed():
        if not config.config.has_section("Updates"):
            config.config.add_section("Updates")
        config.config.set("Updates", "daily_check", "true" if daily.get() else "false")
        config.save()
    ttk.Checkbutton(frame, text="Check for updates once a day in the background", variable=daily,
                    command=_daily_changed).pack(anchor="w")

    bar = ttk.Frame(frame)
    bar.pack(fill="x", pady=(10, 0))
    btn_check = ttk.Button(bar, text="Check now", command=lambda: _check())
    btn_sel = ttk.Button(bar, text="Update selected", command=lambda: _update(_selected()))
    btn_all = ttk.Button(bar, text="Update all", command=lambda: _update(_all_updatable()))
    btn_close = ttk.Button(bar, text="Close", command=lambda: _close())
    for b in (btn_check, btn_sel, btn_all):
        b.pack(side="left", padx=(0, 6))
    btn_close.pack(side="right")

    def _fill():
        tree.delete(*tree.get_children())
        for r in state["rows"]:
            note = r.get("note") or ""
            src = r.get("source") or ""
            tree.insert("", "end", iid=r["name"], values=(
                r["label"], r.get("installed") or "\u2014", r.get("latest") or "\u2014",
                STATUS_TEXT.get(r["status"], r["status"]), f"{src}  {note}".strip()))
        if on_badge:
            on_badge(depsupdate.updates_available(state["rows"]))
        _buttons()

    def _buttons():
        st = "disabled" if state["busy"] else "normal"
        for b in (btn_check, btn_sel, btn_all):
            b.configure(state=st)

    def _selected():
        return list(tree.selection())

    def _all_updatable():
        return [r["name"] for r in state["rows"] if r.get("updatable") and r["name"] != "msr"]

    def _work(fn, kind):
        state["busy"] = True
        _buttons()
        def run():
            try:
                q.put((kind, fn()))
            except Exception as e:     # never let the worker die silently
                q.put(("error", f"{type(e).__name__}: {e}"))
        threading.Thread(target=run, daemon=True, name=f"deps-{kind}").start()

    def _check():
        if state["busy"]:
            return
        msg.configure(text="Checking\u2026")
        def fn():
            rs = depsupdate.rows(True, ffmpeg_path=ffmpeg_path)
            try:
                depsupdate.remember(state_file, rs)
            except Exception:
                pass
            return rs
        _work(fn, "rows")

    def _update(names):
        if state["busy"]:
            return
        by = {r["name"]: r for r in state["rows"]}
        if "msr" in names:
            if by["msr"].get("updatable") and on_msr_update:
                if dialogs.ask_yes_no(win, "Update Multi-Stream Recorder",
                                      "Updating the app stops running recordings and restarts it. Continue?"):
                    _close()
                    on_msr_update()
                    return
            names = [n for n in names if n != "msr"]
        skipped = [by[n]["label"] for n in names if n in by and not by[n].get("updatable")]
        names = [n for n in names if n in by and by[n].get("updatable")]
        if not names:
            msg.configure(text="Nothing to update" + (f" ({', '.join(skipped)} cannot be updated here)." if skipped
                                                     else ". Press Check now first."))
            return
        msg.configure(text=f"Updating {', '.join(names)}\u2026 (recordings keep running)")
        def fn():
            res = depsupdate.update_packages(names, log=lambda line: q.put(("log", line)))
            return res, depsupdate.rows(True, ffmpeg_path=ffmpeg_path)
        _work(fn, "update")

    def _drain():
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "log":
                    msg.configure(text=payload)
                    continue
                state["busy"] = False
                if kind == "rows":
                    state["rows"] = payload
                    n = len(depsupdate.updates_available(payload))
                    msg.configure(text=f"{n} update(s) available." if n else "Everything is up to date.")
                elif kind == "update":
                    res, rs = payload
                    state["rows"] = rs
                    msg.configure(text=summary(res))
                else:
                    msg.configure(text=f"Failed: {payload}")
                _fill()
        except queue.Empty:
            pass
        if win.winfo_exists():
            state["job"] = win.after(150, _drain)

    def _close():
        if state["job"]:
            try:
                win.after_cancel(state["job"])
            except tk.TclError:
                pass
        parent._msr_deps_dialog = None
        win.destroy()

    win.protocol("WM_DELETE_WINDOW", _close)
    win.bind("<Escape>", lambda e: _close())
    _fill()
    dialogs.present(win, parent)
    state["job"] = win.after(150, _drain)
    if check_now:
        _check()
    win._deps_state = state      # tests
    return win


def summary(results) -> str:
    ok = [f"{r['name']} {r.get('old') or '-'} -> {r.get('new')}" for r in results if r.get("ok")]
    bad = [f"{r['name']}: {r.get('error')}" + (" (previous version restored)" if r.get("rolled_back") else "")
           for r in results if not r.get("ok")]
    parts = []
    if ok:
        parts.append("Updated: " + "; ".join(ok) + ". New recordings use the new version.")
    if bad:
        parts.append("Not updated: " + " ".join(bad))
    return "  ".join(parts) or "Nothing done."
