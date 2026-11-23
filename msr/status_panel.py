"""Status tab panel for Multi-Stream Recorder.

Owns the Status header (action buttons, Recording-first toggle), storage and
missing-ffmpeg notices, I/O meter, Telegram upload queue line, embedded
``PreviewStrip``, Status ``Treeview``, context menu, and Clip / Screenshot /
DVR / Clip Editor launchers.
"""
from __future__ import annotations

import datetime
import logging
import os
import threading
import tkinter as tk
from tkinter import ttk
import webbrowser

from msr import deps as _deps
from msr import dialogs as _dialogs
from msr import themes as _themes
from msr import themeui as _themeui
from msr.config import default_streams_dir
from msr.context_menu import bind_context_menu
from msr.deps import HAS_PSUTIL
from msr.iometer import IoSampler, format_header, format_tooltip, meter_severity, sum_stream_mbps
from msr.preview_strip import PreviewStrip
from msr.tooltip import Tooltip
from msr.util import (
    channel_file_stem,
    channel_key_to_dirs,
    channel_watch_url,
    open_existing_folder,
    open_local_path,
    parse_custom_url,
)
from msr.worker import (
    CATCHUP_UNAVAILABLE,
    channel_status_allows_clip,
    clip_unavailable_reason,
    create_clip,
    create_screenshot,
    find_active_recording_file,
    screenshot_extension,
)


def platform_label(ch_name: str) -> str:
    """Extract a display platform from a channel name string."""
    if ch_name.startswith("twitch:"):
        return "Twitch"
    if ch_name.startswith("youtube:"):
        return "YouTube"
    if ch_name.startswith("fishtank:"):
        return "Fishtank"
    if ch_name.startswith("rumble:"):
        return "Rumble"
    if ch_name.startswith("tiktok:"):
        return "TikTok"
    if ch_name.startswith("custom:"):
        platform, _ = parse_custom_url(ch_name.split(":", 1)[1])
        return platform.capitalize()
    return "Kick"


def display_name(ch_name: str) -> str:
    """Extract a clean display name for the status table."""
    if ch_name.startswith("custom:"):
        _, channel = parse_custom_url(ch_name.split(":", 1)[1])
        return channel
    if ":" in ch_name:
        return ch_name.split(":", 1)[1]
    return ch_name


class StatusPanel:
    """Status tab UI, media clip/screenshot/DVR actions, meters, and preview strip."""

    VISIBLE_STATUS_COLUMNS = ("Channel", "Status", "Size", "Elapsed", "Platform")

    def __init__(
        self,
        notebook,
        root,
        *,
        config,
        win_state: dict,
        state_file: str,
        load_state,
        save_state,
        get_recorder,
        get_channels,
        get_telegram_uploader,
        sounds,
        notifications_enabled: bool,
        send_notification,
        header_action_button,
        grab_preview_frame,
        on_status_refresh,
        on_open_options,
        on_select_channel,
        on_preview_select,
        set_nav_lock,
    ):
        self.root = root
        self.config = config
        self.state_file = state_file
        self._load_state = load_state
        self._save_state = save_state
        self._get_recorder = get_recorder
        self._get_channels = get_channels
        self._get_telegram_uploader = get_telegram_uploader
        self.sounds = sounds
        self.notifications_enabled = notifications_enabled
        self._send_notification = send_notification
        self._header_action_button = header_action_button
        self._on_status_refresh = on_status_refresh
        self._on_open_options = on_open_options
        self._on_select_channel = on_select_channel
        self._set_nav_lock = set_nav_lock

        self._media_jobs_in_flight: set[tuple[str, str]] = set()
        self._storage_notice_shown = ""
        self._last_tg_queue = None

        self.recording_first_var = tk.BooleanVar(
            value=bool(win_state.get("status_recording_first", True))
        )

        self.frame = ttk.Frame(notebook)
        self._build_header()
        self._build_notices()
        self.preview_strip = PreviewStrip(
            self.frame, root,
            config=config,
            get_jobs=self._recording_preview_jobs,
            get_display_name=display_name,
            on_select=on_preview_select,
            grab_frame=grab_preview_frame,
            is_media_busy=self.is_media_busy,
        )
        self._build_tree(win_state)
        self._build_header_buttons()
        self._build_io_meter()
        self._build_tg_queue_bar()

        self.refresh_ffmpeg_notice()

    # ── Widget construction ─────────────────────────────────────────────────

    def _build_header(self) -> None:
        self.header = tk.Frame(self.frame)
        self.header.pack(fill=tk.X, padx=10, pady=(10, 4))
        self.meter_row = tk.Frame(self.frame)

        self.status_label = tk.Label(
            self.header, text="Live Recording Status", font=("Segoe UI", 13, "bold"), anchor="w",
        )
        self.status_label.pack(side=tk.LEFT)

        self.recording_first_cb = ttk.Checkbutton(
            self.header, text="Recording first",
            variable=self.recording_first_var,
            command=self._on_recording_first_toggle,
        )
        self.recording_first_cb.pack(side=tk.LEFT, padx=(12, 0))
        Tooltip(
            self.recording_first_cb,
            "Keep Recording / Remuxing rows at the\n"
            "top of the status table, in roster order.",
        )

    def _build_notices(self) -> None:
        self.storage_notice_label = tk.Label(
            self.frame, text="", font=("Segoe UI", 9, "bold"),
            anchor="w", justify="left", wraplength=820,
        )
        self.ffmpeg_notice_frame = tk.Frame(self.frame)
        _install_hint = (
            "winget install Gyan.FFmpeg"
            if os.name == "nt" else "sudo apt install ffmpeg"
        )
        self.ffmpeg_notice_label = tk.Label(
            self.ffmpeg_notice_frame,
            text=(
                f"\u26a0 ffmpeg was not found. Recording, remuxing, and clips require ffmpeg. "
                f"Install it ({_install_hint}) or pick its path in Options \u2192 Other \u2192 ffmpeg."
            ),
            font=("Segoe UI", 9, "bold"), anchor="w", justify="left", wraplength=700,
        )
        self.ffmpeg_notice_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.ffmpeg_notice_btn = ttk.Button(
            self.ffmpeg_notice_frame, text="Open Options", command=self._on_open_options,
        )
        self.ffmpeg_notice_btn.pack(side=tk.RIGHT, padx=(8, 0))

    def _build_tree(self, win_state: dict) -> None:
        columns = self.VISIBLE_STATUS_COLUMNS + ("_key",)
        self.tree = ttk.Treeview(
            self.frame, columns=columns,
            displaycolumns=self.VISIBLE_STATUS_COLUMNS,
            show="headings", height=20,
        )
        for col, width, anchor in (
            ("Channel", 200, "w"),
            ("Status", 300, "w"),
            ("Size", 90, "center"),
            ("Elapsed", 80, "center"),
            ("Platform", 75, "center"),
        ):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor=anchor)
        self.tree.column("_key", width=0, stretch=False)

        saved_widths = win_state.get("column_widths", {})
        for col in self.VISIBLE_STATUS_COLUMNS:
            if col in saved_widths:
                try:
                    self.tree.column(col, width=int(saved_widths[col]))
                except (ValueError, tk.TclError):
                    pass
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.ctx_menu = tk.Menu(self.root, tearoff=0)
        bind_context_menu(self.tree, self._show_context_menu)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

    def _build_header_buttons(self) -> None:
        btn = self._header_action_button
        self.streams_folder_btn = btn(
            self.header, text="Streams", command=self.open_recordings_folder, width=8,
        )
        self.streams_folder_btn.pack(side=tk.RIGHT)
        Tooltip(
            self.streams_folder_btn,
            "Open the recordings folder\n(streams_dir: Recorded, Processed, Clips...).",
        )

        self.screenshot_now_btn = btn(
            self.header, text="Screenshot", command=self.screenshot_selected_channel, width=11,
        )
        self.screenshot_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
        Tooltip(
            self.screenshot_now_btn,
            "Grab a still frame from the channel\nselected in the status list.",
        )

        self.clips_folder_btn = btn(
            self.header, text="Clips", command=self.open_selected_clips_dir, width=6,
        )
        self.clips_folder_btn.pack(side=tk.RIGHT, padx=(0, 6))
        Tooltip(
            self.clips_folder_btn,
            "Open the clips & screenshots folder\nof the channel selected in the status list.",
        )

        self.dvr_btn = btn(
            self.header, text="DVR", command=self.clip_editor_selected_channel, width=6,
        )
        self.dvr_btn.pack(side=tk.RIGHT, padx=(0, 6))
        Tooltip(
            self.dvr_btn,
            "Scrub back through a live recording and cut a clip\n"
            "(Clip Editor: In/Out marks, sound, live edge).\n"
            "Not recording? Opens the channel's newest finished recording.",
        )

        self.clip_now_btn = btn(
            self.header, text="Clip Now", style="Accent.TButton",
            command=self.clip_selected_channel, width=9,
        )
        self.clip_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
        self.clip_now_btn.bind("<Shift-Button-1>", self.clip_editor_selected_channel)
        Tooltip(self.clip_now_btn, self._clip_now_tip)

    def _build_io_meter(self) -> None:
        self._io_sampler = IoSampler(self.config.get("Paths", "streams_dir"))
        self.io_meter_label = tk.Label(
            self.meter_row, text=format_header(self._io_sampler.last),
            font=("Segoe UI", 9), anchor="e", width=32,
        )
        if HAS_PSUTIL:
            self.meter_row.pack(fill=tk.X, padx=10, pady=(0, 4), after=self.header)
            self.io_meter_label.pack(side=tk.RIGHT)
            Tooltip(self.io_meter_label, lambda: format_tooltip(self._io_sampler.last))

    def _build_tg_queue_bar(self) -> None:
        self.tg_queue_label = tk.Label(
            self.frame, text="", anchor="w", justify="left", font=("Segoe UI", 9),
        )
        self.root.after(1000, self._refresh_tg_queue)

    # ── Timers & notices ────────────────────────────────────────────────────

    def start_timers(self) -> None:
        self.preview_strip.start(2000)
        if HAS_PSUTIL:
            self.root.after(1000, self._refresh_io_meter)

    def refresh_ffmpeg_notice(self) -> None:
        if _deps.HAS_FFMPEG:
            if self.ffmpeg_notice_frame.winfo_manager():
                self.ffmpeg_notice_frame.pack_forget()
        elif not self.ffmpeg_notice_frame.winfo_manager():
            self.ffmpeg_notice_frame.pack(fill=tk.X, padx=10, pady=(0, 4), after=self.header)

    def show_storage_notice(self, text: str) -> None:
        t = _themes.current()
        self.storage_notice_label.configure(bg=t["bg"], fg=t["error_fg"], text=text)
        if text and not self.storage_notice_label.winfo_manager():
            self.storage_notice_label.pack(fill=tk.X, padx=10, pady=(0, 4), after=self.header)
        elif not text and self.storage_notice_label.winfo_manager():
            self.storage_notice_label.pack_forget()
        if text and text != self._storage_notice_shown:
            self.sounds.play("problem", "storage")
            if self.notifications_enabled:
                self._send_notification(
                    "Recordings folder", text, category="error", channel="storage", detail=text,
                )
        self._storage_notice_shown = text

    def _paint_io_meter(self) -> None:
        if not HAS_PSUTIL:
            return
        t = _themes.current()
        snap = self._io_sampler.last
        sev = meter_severity(snap.disk_busy_frac)
        fg = t["error_fg"] if sev == "hot" else (t["remux_fg"] if sev == "warn" else t["fg"])
        self.io_meter_label.configure(bg=t["bg"], fg=fg, text=format_header(snap))

    def _refresh_io_meter(self) -> None:
        try:
            if not self.root.winfo_viewable():
                self.root.after(1000, self._refresh_io_meter)
                return
        except tk.TclError:
            return
        sizes = {}
        details = []
        rec = self._get_recorder()
        try:
            if rec and rec.is_running:
                for ch_name, st in rec.status_dict.items():
                    if not str(st.get("status", "")).lower().startswith("recording"):
                        continue
                    details.append(st.get("detail", ""))
                    raw = st.get("size_bytes")
                    if isinstance(raw, (int, float)) and raw >= 0:
                        sizes[ch_name] = int(raw)
        except Exception:
            pass
        self._io_sampler.sample(msr_sizes=sizes, msr_stream_mbps=sum_stream_mbps(details))
        self._paint_io_meter()
        self.root.after(1000, self._refresh_io_meter)

    def _paint_tg_queue(self) -> bool:
        up = self._get_telegram_uploader()
        text = ""
        enabled = False
        if up is not None:
            try:
                st = up.status()
            except Exception:
                st = None
            if st and st.get("enabled"):
                enabled = True
                from msr.telegram_queue import format_status
                text = format_status(st)
        t = _themes.current()
        sig = (text, t["bg"], t["fg"])
        if sig != self._last_tg_queue:
            self._last_tg_queue = sig
            self.tg_queue_label.configure(bg=t["bg"], fg=t["fg"])
            if text:
                self.tg_queue_label.configure(text=text)
                if not self.tg_queue_label.winfo_ismapped():
                    anchor = self.meter_row if self.meter_row.winfo_ismapped() else self.header
                    self.tg_queue_label.pack(fill=tk.X, padx=10, pady=(0, 4), after=anchor)
            elif self.tg_queue_label.winfo_ismapped():
                self.tg_queue_label.pack_forget()
        return enabled

    def _refresh_tg_queue(self) -> None:
        enabled = False
        try:
            if self.root.winfo_viewable():
                enabled = self._paint_tg_queue()
        except tk.TclError:
            return
        except Exception:
            logging.debug("Telegram queue line", exc_info=True)
        self.root.after(1000 if enabled else 5000, self._refresh_tg_queue)

    # ── Selection, ordering, and preview jobs ───────────────────────────────

    def _on_recording_first_toggle(self) -> None:
        try:
            st = self._load_state(self.state_file)
            st["status_recording_first"] = bool(self.recording_first_var.get())
            self._save_state(self.state_file, st)
        except Exception:
            pass
        self.reorder_rows()

    def _on_tree_select(self, _event=None) -> None:
        ch_name = self.get_selected_channel()
        if ch_name:
            self._on_select_channel(ch_name)

    def get_selected_channel(self) -> str | None:
        selected = self.tree.selection()
        if not selected:
            return None
        try:
            key = self.tree.set(selected[0], "_key")
        except tk.TclError:
            return None
        return key or None

    def select_row(self, ch_name: str, see: bool = False) -> None:
        for iid in self.tree.get_children():
            try:
                if self.tree.set(iid, "_key") == ch_name:
                    self.tree.selection_set(iid)
                    if see:
                        self.tree.see(iid)
                    return
            except tk.TclError:
                continue

    def scroll_row_into_view(self, ch_name: str) -> None:
        for iid in self.tree.get_children():
            try:
                if self.tree.set(iid, "_key") == ch_name:
                    self.tree.see(iid)
                    return
            except tk.TclError:
                continue

    def reorder_rows(self) -> None:
        rec = self._get_recorder()
        if not rec:
            return
        roster = [ch["name"] for ch in self._get_channels()]
        keys = []
        iid_by_key = {}
        for iid in self.tree.get_children():
            try:
                key = self.tree.set(iid, "_key")
            except tk.TclError:
                continue
            if not key:
                continue
            keys.append(key)
            iid_by_key[key] = iid
        from msr.gui import ordered_status_keys
        ordered = ordered_status_keys(
            keys, rec.status_dict, roster,
            recording_first=bool(self.recording_first_var.get()),
        )
        if ordered == keys:
            return
        self._set_nav_lock(True)
        try:
            for i, ch_name in enumerate(ordered):
                iid = iid_by_key.get(ch_name)
                if iid:
                    self.tree.move(iid, "", i)
        finally:
            self._set_nav_lock(False)

    def column_widths(self) -> dict[str, int]:
        out = {}
        for col in self.VISIBLE_STATUS_COLUMNS:
            try:
                out[col] = self.tree.column(col, "width")
            except tk.TclError:
                pass
        return out

    def _recording_preview_jobs(self) -> list[tuple[str, str]]:
        rec = self._get_recorder()
        if not rec or not rec.is_running:
            return []
        jobs = []
        seen = set()
        roster = [ch["name"] for ch in self._get_channels()]
        extra = [k for k in rec.status_dict if k not in roster]
        for ch_name in roster + extra:
            if ch_name in seen:
                continue
            seen.add(ch_name)
            st = rec.status_dict.get(ch_name) or {}
            if not channel_status_allows_clip(st.get("status", "")):
                continue
            path = st.get("raw_file") or ""
            if not path or not os.path.isfile(path):
                path = find_active_recording_file(rec, ch_name) or ""
            if not path:
                continue
            size = st.get("size_bytes") or 0
            if size and size < 64 * 1024:
                continue
            jobs.append((ch_name, path))
        return jobs

    def is_media_busy(self, ch_name: str) -> bool:
        return (ch_name, "clip") in self._media_jobs_in_flight or (
            ch_name, "screenshot"
        ) in self._media_jobs_in_flight

    # ── Channel & media actions ─────────────────────────────────────────────

    def _stop_selected_channel(self) -> None:
        ch_name = self.get_selected_channel()
        rec = self._get_recorder()
        if not ch_name or not rec or not rec.is_running:
            return
        if rec.status_dict.get(ch_name, {}).get("status", "").lower() in ("stopped",):
            return

        def _bg():
            rec.stop_channel(ch_name)
            self.root.after(0, self._on_status_refresh)

        threading.Thread(target=_bg, daemon=True, name=f"stop-{ch_name}").start()

    def _start_selected_channel(self) -> None:
        ch_name = self.get_selected_channel()
        rec = self._get_recorder()
        if not ch_name or not rec or not rec.is_running:
            return
        if rec.status_dict.get(ch_name, {}).get("status", "").lower() not in ("stopped",):
            return
        rec.start_channel(ch_name)
        self._on_status_refresh()

    def _check_now_selected_channel(self) -> None:
        ch_name = self.get_selected_channel()
        rec = self._get_recorder()
        if not ch_name or not rec or not rec.is_running:
            return
        rec.check_now(ch_name)

    @staticmethod
    def open_channel_in_browser(ch_name: str) -> None:
        url = channel_watch_url(ch_name)
        if not url:
            logging.info(f"Open in Browser: no URL for {ch_name}")
            return
        webbrowser.open(url)

    def _open_status_channel_url(self) -> None:
        ch_name = self.get_selected_channel()
        if ch_name:
            self.open_channel_in_browser(ch_name)

    def _clips_output_dir(self, platform: str, username_dir: str) -> str:
        from msr.util import reuse_dir, reuse_path
        base = self.config.get("Clipping", "clips_dir", fallback="").strip()
        if not base:
            base = reuse_dir(self.config.get("Paths", "streams_dir"), "Clips")
        return reuse_path(base, platform, username_dir)

    def _clips_base(self) -> str:
        from msr.util import reuse_dir
        return (
            self.config.get("Clipping", "clips_dir", fallback="").strip()
            or reuse_dir(self.config.get("Paths", "streams_dir"), "Clips")
        )

    def _run_clip_job(self, ch_name: str, kind: str) -> None:
        display = display_name(ch_name)
        label = "Clip" if kind == "clip" else "Screenshot"
        rec = self._get_recorder()
        raw_file = find_active_recording_file(rec, ch_name)
        if not raw_file:
            logging.warning(f"{label}: no active recording file found for {display}")
            self._send_notification(
                f"{label} failed", f"{display}: no active recording found",
                category="error", channel=ch_name,
            )
            return
        platform, username_dir = channel_key_to_dirs(ch_name)
        out_dir = self._clips_output_dir(platform, username_dir)
        file_stem = channel_file_stem(username_dir)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        ffmpeg_path = self.config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"

        if kind == "clip":
            clip_seconds = self.config.getint("Clipping", "clip_length_seconds", fallback=30)
            out_file = os.path.join(out_dir, f"{file_stem}_{ts}_{clip_seconds}s.mp4")
            ok, msg = create_clip(raw_file, out_file, clip_seconds, ffmpeg_path, logging)
        else:
            shot_fmt = self.config.get("Clipping", "screenshot_format", fallback="jpg")
            shot_quality = self.config.getint("Clipping", "screenshot_quality", fallback=2)
            ext = screenshot_extension(shot_fmt)
            out_file = os.path.join(out_dir, f"{file_stem}_{ts}{ext}")
            ok, msg = create_screenshot(
                raw_file, out_file, ffmpeg_path, logging, fmt=shot_fmt, quality=shot_quality,
            )

        if ok:
            logging.info(f"{label} saved for {display}: {out_file}")
            self._send_notification(
                f"{label} saved", f"{display} \u2014 {os.path.basename(out_file)}",
                category="complete", channel=ch_name,
            )
            self.sounds.play("clip")
        else:
            logging.error(f"{label} failed for {display}: {msg}")
            self._send_notification(
                f"{label} failed", f"{display}: {msg}", category="error", channel=ch_name,
            )
            self.sounds.play("problem", f"clip:{ch_name}")

    def _start_media_job(self, ch_name: str, kind: str, start_msg: str) -> None:
        job_key = (ch_name, kind)
        label = "Clip Now" if kind == "clip" else "Screenshot Now"
        if job_key in self._media_jobs_in_flight:
            logging.info(f"{label}: already in progress for {display_name(ch_name)} \u2014 ignored")
            return
        self._media_jobs_in_flight.add(job_key)
        logging.info(start_msg)

        def _run():
            try:
                self._run_clip_job(ch_name, kind)
            finally:
                self._media_jobs_in_flight.discard(job_key)

        threading.Thread(
            target=_run, daemon=True,
            name=f"{'clip' if kind == 'clip' else 'shot'}-{ch_name}",
        ).start()

    def clip_selected_channel(self) -> None:
        ch_name = self.get_selected_channel()
        if not ch_name:
            logging.info("Clip Now: select a channel in the status list first")
            return
        rec = self._get_recorder()
        if not rec or not rec.is_running:
            logging.info("Clip Now: recording is not running")
            return
        st = rec.status_dict.get(ch_name, {})
        if not channel_status_allows_clip(st.get("status", "")):
            logging.info(f"Clip Now: {display_name(ch_name)} is not currently recording")
            return
        if clip_unavailable_reason(st):
            logging.info(
                f"Clip Now: {display_name(ch_name)} - {CATCHUP_UNAVAILABLE} (catching up from the start)"
            )
            return
        clip_seconds = self.config.getint("Clipping", "clip_length_seconds", fallback=30)
        self._start_media_job(
            ch_name, "clip",
            f"Clip Now: cutting last {clip_seconds}s for {display_name(ch_name)}",
        )

    def screenshot_selected_channel(self) -> None:
        ch_name = self.get_selected_channel()
        if not ch_name:
            logging.info("Screenshot Now: select a channel in the status list first")
            return
        rec = self._get_recorder()
        if not rec or not rec.is_running:
            logging.info("Screenshot Now: recording is not running")
            return
        st = rec.status_dict.get(ch_name, {})
        if not channel_status_allows_clip(st.get("status", "")):
            logging.info(f"Screenshot Now: {display_name(ch_name)} is not currently recording")
            return
        if clip_unavailable_reason(st):
            logging.info(
                f"Screenshot Now: {display_name(ch_name)} - {CATCHUP_UNAVAILABLE} (catching up from the start)"
            )
            return
        self._start_media_job(
            ch_name, "screenshot",
            f"Screenshot Now: grabbing a frame for {display_name(ch_name)}",
        )

    def open_clip_editor(self, path=None, ch_name=None, live=False, start_at=None, end_at=None) -> None:
        from msr import clip_editor as _clip_editor
        rec = self._get_recorder()

        def follow():
            return find_active_recording_file(rec, ch_name) if (rec is not None and ch_name) else None

        try:
            _clip_editor.open_editor(
                self.root, path,
                ffmpeg=self.config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg",
                clips_base=self._clips_base(),
                title_name=display_name(ch_name) if ch_name else "",
                live=live, follow=follow if live else None,
                state_file=self.state_file, logger=logging,
                opener=open_local_path, start_at=start_at, end_at=end_at,
            )
        except Exception as e:
            logging.error(f"Clip Editor: could not open: {e}")
            _dialogs.show_error(self.root, "Clip Editor", f"Could not open the Clip Editor: {e}")

    def _newest_processed(self, ch_name: str) -> str | None:
        try:
            from msr.util import read_layout_paths
            platform, username_dir = channel_key_to_dirs(ch_name)
            mp4s = []
            for folder in read_layout_paths(
                self.config.get("Paths", "streams_dir"), "Processed", platform, username_dir,
            ):
                if not os.path.isdir(folder):
                    continue
                mp4s.extend(
                    os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".mp4")
                )
            return max(mp4s, key=os.path.getmtime) if mp4s else None
        except (OSError, ValueError):
            return None

    def clip_editor_selected_channel(self, _event=None) -> str:
        ch_name = self.get_selected_channel()
        if not ch_name:
            _dialogs.show_info(
                self.root, "DVR / Clip Editor",
                "Select a channel in the status list first.\n\n"
                "While it records you can scrub back and cut a clip; otherwise its "
                "newest finished recording opens.",
            )
            return "break"
        raw = None
        rec = self._get_recorder()
        if rec and rec.is_running:
            st = rec.status_dict.get(ch_name, {})
            if clip_unavailable_reason(st):
                _dialogs.show_info(
                    self.root, "Clip Editor",
                    f"{display_name(ch_name)}: {CATCHUP_UNAVAILABLE}.\n\n"
                    "It is still catching up from the start of the broadcast. "
                    "The live Clip Editor works once it reaches live.",
                )
                return "break"
            if channel_status_allows_clip(st.get("status", "")):
                raw = find_active_recording_file(rec, ch_name)
        if raw:
            self.open_clip_editor(raw, ch_name, live=True)
        else:
            done = self._newest_processed(ch_name)
            if done:
                self.open_clip_editor(done, ch_name, live=False)
            else:
                _dialogs.show_info(
                    self.root, "Clip Editor",
                    f"{display_name(ch_name)} is not recording and has no finished recording yet.",
                )
        return "break"

    def open_clips_dir_for_channel(self, ch_name: str) -> None:
        if not ch_name:
            logging.info("Open Clips Folder: select a channel first")
            return
        try:
            platform, username_dir = channel_key_to_dirs(ch_name)
            path = self._clips_output_dir(platform, username_dir)
            os.makedirs(path, exist_ok=True)
            open_local_path(path)
        except Exception as e:
            logging.warning(f"Open Clips Folder: {e}")

    def open_selected_clips_dir(self) -> None:
        ch_name = self.get_selected_channel()
        if not ch_name:
            logging.info("Open Clips Folder: select a channel in the status list first")
            return
        self.open_clips_dir_for_channel(ch_name)

    def open_recordings_folder(self) -> None:
        ok, why = open_existing_folder(
            self.config.get("Paths", "streams_dir", fallback=""),
            default_streams_dir(),
        )
        if not ok:
            _dialogs.show_info(self.root, "Streams folder", why)

    def _clip_now_tip(self) -> str:
        base = (
            "Cut the last N seconds (Clip Length setting)\n"
            "of the channel selected in the status list.\n"
            "Shift+click or DVR: open the Clip Editor."
        )
        ch = self.get_selected_channel()
        rec = self._get_recorder()
        st = rec.status_dict.get(ch, {}) if (ch and rec) else {}
        if clip_unavailable_reason(st):
            return f"{CATCHUP_UNAVAILABLE}.\nStill catching up from the start of the broadcast.\n\n" + base
        return base

    def _show_context_menu(self, event) -> None:
        item = self.tree.identify_row(event.y)
        if not item:
            return
        self.tree.selection_set(item)
        self.ctx_menu.delete(0, tk.END)

        ch_name = self.get_selected_channel()
        rec = self._get_recorder()
        if ch_name and rec and rec.is_running:
            st = rec.status_dict.get(ch_name, {})
            status_lower = st.get("status", "").lower()
            if status_lower == "stopped":
                self.ctx_menu.add_command(label="Restart Channel", command=self._start_selected_channel)
            else:
                self.ctx_menu.add_command(label="Stop Channel", command=self._stop_selected_channel)
                if status_lower.startswith(("offline", "error")):
                    self.ctx_menu.add_command(label="Check Now", command=self._check_now_selected_channel)
                if channel_status_allows_clip(status_lower) and clip_unavailable_reason(st):
                    self.ctx_menu.add_command(
                        label=f"Clip Now: {CATCHUP_UNAVAILABLE}", state=tk.DISABLED,
                    )
                elif channel_status_allows_clip(status_lower):
                    clip_seconds = self.config.getint("Clipping", "clip_length_seconds", fallback=30)
                    self.ctx_menu.add_command(
                        label=f"Clip Now ({clip_seconds}s)", command=self.clip_selected_channel,
                    )
                    self.ctx_menu.add_command(
                        label="Screenshot Now", command=self.screenshot_selected_channel,
                    )
                self.ctx_menu.add_command(label="Clip Editor\u2026", command=self.clip_editor_selected_channel)
            self.ctx_menu.add_separator()

        self.ctx_menu.add_command(label="Open in Browser", command=self._open_status_channel_url)
        self.ctx_menu.add_command(label="Open Clips Folder", command=self.open_selected_clips_dir)
        try:
            self.ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.ctx_menu.grab_release()

    # ── Theming ─────────────────────────────────────────────────────────────

    def apply_theme(self, t) -> None:
        self.tree.tag_configure("recording", foreground=t["rec_fg"], font=("Segoe UI", 10, "bold"))
        self.tree.tag_configure("completed", foreground=t["completed_fg"])
        self.tree.tag_configure("offline", foreground=t["offline_fg"])
        self.tree.tag_configure("checking", foreground=t["check_fg"])
        self.tree.tag_configure("remuxing", foreground=t["remux_fg"])
        self.tree.tag_configure("error", foreground=t["error_fg"])
        self.tree.tag_configure("stopped", foreground=t["offline_fg"])
        self.tree.tag_configure("unknown", foreground=t["offline_fg"])

        for w in (self.header, self.meter_row, self.ffmpeg_notice_frame):
            w.configure(bg=t["bg"])
        self.status_label.configure(bg=t["bg"], fg=t["fg"])
        self.storage_notice_label.configure(bg=t["bg"], fg=t["error_fg"])
        self.ffmpeg_notice_label.configure(bg=t["bg"], fg=t["error_fg"])
        self.io_meter_label.configure(bg=t["bg"])
        self._paint_io_meter()
        self.tg_queue_label.configure(bg=t["bg"], fg=t["fg"])
        self.preview_strip.apply_theme(t)
        self.ctx_menu.configure(**_themeui.menu_colors(t))
