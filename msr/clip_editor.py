"""Clip Editor: a small DVR-style window to cut a clip out of a recording, also while it is
still being recorded.

Timeline over the whole recording so far (the live edge grows), scrub with a preview frame,
play with sound and a volume slider, mark In / Out (I / O), nudge, and Save Clip (a stream
copy that starts at a keyframe, or an exact re-encode). The source is only read.

Threads: workers never touch Tk; they post to self.q, drained by an after() poll.
"""

from __future__ import annotations

import os
import queue
import subprocess
import threading
import time
import tkinter as tk
from tkinter import ttk

from msr import clip_core as core
from msr import dialogs, themes, timeline_view, waveform
from msr.nowindow import no_window_kwargs
from msr.wheel import bind_wheel, wheel_delta

POLL_MS = 40
PLAY_POLL_MS = 4          # while playing: a frame lands within a few ms of its time
QUEUE_SECONDS = 0.2       # decoded frames kept ahead (6 at 30 fps, 12 at 60)
MAX_FPS = 60              # source rate up to this
LIMITED_FPS = 30          # "Limit preview to 30 fps"
LIMIT_KEY = "clip_editor_limit_30fps"
WAVE_KEY = "clip_editor_waveform"
WAVE_REFRESH_S = 30.0   # re-read the new end of a live file this often
TIMELINE_H, WAVE_H = 62, 32
CACHE_FRAMES = 16         # exact preview frames kept (about 11 MB at 640x360)
LENGTH_EVERY_S = 2.0
FPS = 30
PREVIEW_W = 640
NUDGE = 0.5
REPLAY = 2.0
MIN_SIZE = (760, 560)
HINT = ("I / O mark, Space plays, \u2190/\u2192 step (Shift: 5 s), L jumps to live, "
        "Ctrl+S or Enter saves the clip.  Timeline: Alt+wheel or +/- zooms, 0 or double-click resets, "
        "Ctrl+wheel or Ctrl+\u2190/\u2192 steps (scales with zoom), wheel / Shift+wheel pans.")


def _audio_player(src, ffmpeg):
    from msr.clip_audio import AudioPlayer
    return AudioPlayer(src, ffmpeg=ffmpeg)


class VideoFeed:
    """Playback picture: ONE ffmpeg decoding a continuous rgb24 stream at preview size, read on a
    background thread into a small queue. The Tk side takes the newest frame due by the audio
    clock and drops late ones (counted), so the picture never lags the sound."""

    def __init__(self, ffmpeg: str, src: str, start: float, size: tuple[int, int] | None = None,
                 fps: float = FPS, width: int = PREVIEW_W, spawn=None):
        self.start, self.fps = start, float(fps)
        self.frames: queue.Queue = queue.Queue(maxsize=max(6, int(round(self.fps * QUEUE_SECONDS))))
        self.decoded = self.dropped = 0
        self._stop = threading.Event()
        self.raw = size is not None
        cmd = (core.raw_video_command(ffmpeg, src, start, size, self.fps) if self.raw
               else core.video_command(ffmpeg, src, start, int(round(self.fps)), width))
        if self.raw:
            w, h = size
            self._header = f"P6\n{w} {h}\n255\n".encode("ascii")
            self._frame_bytes = w * h * 3
        try:
            self.proc = (spawn or self._spawn)(cmd)
        except OSError:
            self.proc = None
            return
        threading.Thread(target=self._run, daemon=True).start()

    @staticmethod
    def _spawn(cmd):
        return subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                bufsize=1 << 20, **no_window_kwargs())

    def _read_raw(self):
        buf = bytearray(self._frame_bytes)
        view, got = memoryview(buf), 0
        while got < self._frame_bytes:
            n = self.proc.stdout.readinto(view[got:])
            if not n:
                return None
            got += n
        return self._header + bytes(buf)

    def _run(self):
        n = 0
        try:
            while not self._stop.is_set():
                frame = self._read_raw() if self.raw else core.read_ppm(self.proc.stdout)
                if frame is None:
                    break
                item = (self.start + n / self.fps, frame)
                n += 1
                self.decoded = n
                while not self._stop.is_set():
                    try:
                        self.frames.put(item, timeout=0.2)
                        break
                    except queue.Full:
                        continue
        except (OSError, ValueError):
            pass   # pipe closed under us
        finally:
            core.reap(self.proc)

    def take_until(self, t: float):
        """The newest frame due at t, or None. Older due frames are dropped (late)."""
        last = None
        while True:
            try:
                item = self.frames.queue[0] if self.frames.qsize() else None
            except IndexError:
                item = None
            if item is None or item[0] > t:
                return last
            if last is not None:
                self.dropped += 1
            last = self.frames.get_nowait()[1]

    def close(self):
        """Kill the decoder; the reader thread then reaps it."""
        self._stop.set()
        if self.proc is not None:
            try:
                self.proc.kill()
            except OSError:
                pass


class ClipEditor:
    def __init__(self, parent: tk.Misc, src: str, *, ffmpeg: str = "ffmpeg", clips_base: str,
                 title_name: str = "", live: bool = False, follow=None, state_file: str | None = None,
                 logger=None, audio_factory=_audio_player, frame_grabber=core.grab_frame,
                 length_reader=None, opener=None, show=True, start_at: float | None = None,
                 end_at: float | None = None, wave_runner=None, wave_cache_dir=None):
        self.src, self.ffmpeg, self.clips_base = src, ffmpeg, clips_base
        self._start_at, self._end_at = start_at, end_at   # 'Open source' from a clip: go there, marked
        self.live, self.follow, self.logger, self.opener = live, follow, logger, opener
        self.ffprobe = core.ffprobe_for(ffmpeg)
        self._audio_factory, self._grab = audio_factory, frame_grabber
        self._length = length_reader or (lambda p: core.media_length(p, self.ffprobe))
        self.q: queue.Queue = queue.Queue()
        self.marks = core.Marks(0.0)
        self.pos = 0.0
        self.playing = False
        self.audio = None
        self.video: VideoFeed | None = None
        self._play_from = (0.0, 0.0)       # (media time, wall clock) fallback clock
        self._stop_at: float | None = None
        self._frame_gen = 0
        self._last_length_check = 0.0
        self._length_busy = False
        self._saving = False
        self._gone = False
        self._img = None
        self._img_size = b""
        self._closed = False
        self._job = None
        self.state_file = state_file
        self.view = timeline_view.View()
        self.wave: waveform.Wave | None = None
        self._wave_src = None
        self._wave_job = None
        self._wave_seen = -1
        self._wave_at = 0.0
        self._wave_runner = wave_runner
        self._wave_dir = wave_cache_dir or waveform.default_cache_dir()
        self._cache: dict = {}        # (src, t to 0.01 s) -> exact PPM, most recent last
        self._exact_gen = 0           # newest scrub position whose exact frame is on screen
        self._vinfo = None          # (w, h, fps) of the source, read once in the background
        self.shown = 0              # frames shown while playing (measurements, tests)
        self._vinfo_reader = lambda p: core.video_info(p, self.ffprobe)

        self.win, body = dialogs.new_dialog(parent, f"Clip Editor \u2014 {title_name or os.path.basename(src)}",
                                            padding=10)
        self.win.resizable(True, True)
        self.win.minsize(*MIN_SIZE)
        self.win.protocol("WM_DELETE_WINDOW", self.close)
        self._build(body)
        self._bind_keys()
        self.win.update_idletasks()   # larger fonts / DPI: never smaller than the controls need
        self.win.minsize(max(MIN_SIZE[0], self.win.winfo_reqwidth()), max(MIN_SIZE[1], self.win.winfo_reqheight()))
        if state_file:
            try:
                from msr import winstate
                winstate.attach(self.win, state_file, "clip_editor", MIN_SIZE)
            except Exception:
                pass
        if show:
            dialogs.present(self.win, parent)
        self._refresh_length(force=True)
        self._job = self.win.after(POLL_MS, self._poll)

    # ------------------------------------------------------------- layout
    def _build(self, body: ttk.Frame) -> None:
        body.columnconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)
        top = ttk.Frame(body)
        top.grid(row=0, column=0, sticky="ew")
        self.file_label = ttk.Label(top, text=os.path.basename(self.src))
        self.file_label.pack(side="left")
        self.live_label = ttk.Label(top, text="")
        self.live_label.pack(side="right")

        self.preview = ttk.Label(body, anchor="center", text="Loading preview\u2026")
        self.preview.grid(row=1, column=0, sticky="nsew", pady=(6, 6))

        self.timeline = tk.Canvas(body, height=TIMELINE_H, highlightthickness=0, bd=0)
        self.timeline.grid(row=2, column=0, sticky="ew")
        self.timeline.bind("<Configure>", lambda _e: self._draw())
        self.timeline.bind("<Button-1>", self._scrub)
        self.timeline.bind("<B1-Motion>", self._scrub)
        self.timeline.bind("<Double-Button-1>", lambda _e: self.zoom_reset())
        bind_wheel(self.timeline, self._wheel)

        times = ttk.Frame(body)
        times.grid(row=3, column=0, sticky="ew", pady=(4, 0))
        self.pos_label = ttk.Label(times, text="0:00.0 / --:--")
        self.pos_label.pack(side="left")
        self.marks_label = ttk.Label(times, text="In --  Out --  Clip --")
        self.marks_label.pack(side="right")

        row = ttk.Frame(body)
        row.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        b = lambda text, cmd, w=None: ttk.Button(row, text=text, command=cmd, width=w)  # noqa: E731
        b("\u25c0 5s", lambda: self.seek(self.pos - 5), 5).pack(side="left")
        b("\u25c0 1s", lambda: self.seek(self.pos - 1), 5).pack(side="left", padx=(2, 0))
        self.play_btn = b("Play", self.toggle_play, 7)
        self.play_btn.pack(side="left", padx=4)
        b("1s \u25b6", lambda: self.seek(self.pos + 1), 5).pack(side="left")
        b("5s \u25b6", lambda: self.seek(self.pos + 5), 5).pack(side="left", padx=(2, 10))
        b("Mark In (I)", self.mark_in).pack(side="left")
        b("Mark Out (O)", self.mark_out).pack(side="left", padx=(4, 10))
        self.live_btn = b("Jump to live", self.jump_live)
        self.live_btn.pack(side="left")
        vol = ttk.Frame(row)
        vol.pack(side="right")
        ttk.Label(vol, text="Volume").pack(side="left")
        self.volume = tk.DoubleVar(value=80.0)
        ttk.Scale(vol, from_=0, to=100, variable=self.volume, length=110,
                  command=lambda _v: self._apply_volume()).pack(side="left", padx=(4, 0))

        row2 = ttk.Frame(body)
        row2.grid(row=5, column=0, sticky="ew", pady=(6, 0))
        b2 = lambda text, cmd, w=None: ttk.Button(row2, text=text, command=cmd, width=w)  # noqa: E731
        ttk.Label(row2, text="In").pack(side="left")
        b2("-", lambda: self.nudge("in", -NUDGE), 2).pack(side="left", padx=(2, 0))
        b2("+", lambda: self.nudge("in", NUDGE), 2).pack(side="left")
        b2("Replay In", lambda: self.replay("in")).pack(side="left", padx=(4, 12))
        ttk.Label(row2, text="Out").pack(side="left")
        b2("-", lambda: self.nudge("out", -NUDGE), 2).pack(side="left", padx=(2, 0))
        b2("+", lambda: self.nudge("out", NUDGE), 2).pack(side="left")
        b2("Replay Out", lambda: self.replay("out")).pack(side="left", padx=(4, 12))
        self.limit_fps = tk.BooleanVar(value=self._load_limit())
        ttk.Checkbutton(row2, text="Limit preview to 30 fps", variable=self.limit_fps,
                        command=self._limit_changed).pack(side="left")
        self.show_wave = tk.BooleanVar(value=self._load_wave_pref())
        ttk.Checkbutton(row2, text="Waveform", variable=self.show_wave,
                        command=self._wave_changed).pack(side="left", padx=(10, 0))
        if self.show_wave.get():
            self.timeline.configure(height=TIMELINE_H + WAVE_H)

        # Own row, Save first: pack gives space in packing order, so it can never be squeezed out
        # (it was last on the crowded row above and got zero width at the default size).
        actions = ttk.Frame(body)
        actions.grid(row=6, column=0, sticky="ew", pady=(10, 0))
        self.save_btn = ttk.Button(actions, text="Save clip", style="Accent.TButton", command=self.save)
        self.save_btn.pack(side="left")
        self.exact = tk.BooleanVar(value=False)
        ttk.Checkbutton(actions, text="Exact (re-encode)", variable=self.exact).pack(side="left", padx=(10, 0))
        ttk.Button(actions, text="Close", command=self.close).pack(side="right")
        self.clips_btn = ttk.Button(actions, text="Clips folder", command=self.open_clips)
        self.clips_btn.pack(side="right", padx=(0, 6))

        self.status = ttk.Label(body, text=HINT, wraplength=660)
        self.status.grid(row=7, column=0, sticky="ew", pady=(8, 0))
        body.bind("<Configure>", lambda e: self.status.configure(wraplength=max(200, e.width - 20)), add="+")

    def _bind_keys(self) -> None:
        w = self.win
        w.bind("<space>", lambda e: (self.toggle_play(), "break")[1])
        for key, fn in (("i", self.mark_in), ("o", self.mark_out), ("l", self.jump_live)):
            w.bind(f"<KeyPress-{key}>", lambda e, f=fn: f())
            w.bind(f"<KeyPress-{key.upper()}>", lambda e, f=fn: f())
        w.bind("<Left>", lambda e: self.seek(self.pos - 1))
        w.bind("<Right>", lambda e: self.seek(self.pos + 1))
        w.bind("<Shift-Left>", lambda e: self.seek(self.pos - 5))
        w.bind("<Shift-Right>", lambda e: self.seek(self.pos + 5))
        w.bind("<Home>", lambda e: self.seek(0))
        w.bind("<End>", lambda e: self.jump_live())
        w.bind("<Escape>", lambda e: self.close())
        w.bind("<Control-s>", lambda e: self.save())
        w.bind("<Control-S>", lambda e: self.save())
        w.bind("<Return>", lambda e: self.save())
        keys = ((("<Alt-equal>", "<Alt-plus>", "<plus>", "<equal>", "<KP_Add>"), lambda: self.zoom_by(timeline_view.ZOOM_STEP)),
                (("<Alt-minus>", "<minus>", "<KP_Subtract>"), lambda: self.zoom_by(1 / timeline_view.ZOOM_STEP)),
                (("<Alt-Key-0>", "<Key-0>"), self.zoom_reset),
                (("<Control-Left>",), lambda: self.step(-1)),
                (("<Control-Right>",), lambda: self.step(1)),
                (("<Control-Shift-Left>",), lambda: self.step(-5)),
                (("<Control-Shift-Right>",), lambda: self.step(5)))
        for seqs, fn in keys:
            for seq in seqs:
                w.bind(seq, lambda e, f=fn: None if self._typing() else (f(), "break")[1])
        # Space is play/pause only: buttons and checks must not take it
        tag = "ClipEditorKeys"
        self.win.bind_class(tag, "<space>", lambda e: (self.toggle_play(), "break")[1])
        def tagall(widget):
            for child in widget.winfo_children():
                if child.winfo_class() in ("TButton", "TCheckbutton", "TScale"):
                    child.bindtags((tag,) + child.bindtags())
                tagall(child)
        tagall(self.win)

    # ------------------------------------------------------------- drawing
    def _draw(self) -> None:
        c = self.timeline
        t = themes.current()
        c.configure(bg=t["bg"])
        c.delete("all")
        w = max(20, c.winfo_width())
        L = self.marks.length or 0.0
        ww = w - 12
        view = self.view
        x = (lambda s: 6 + view.x(s, ww, L) if L > 0 else 6)  # noqa: E731
        clip = (lambda v: min(max(v, 6), w - 6))  # noqa: E731
        c.create_rectangle(6, 14, w - 6, 32, fill=t["chart_track"], outline="")
        if self.marks.cut_in is not None or self.marks.cut_out is not None:
            a = self.marks.cut_in if self.marks.cut_in is not None else 0.0
            b = self.marks.cut_out if self.marks.cut_out is not None else L
            if clip(x(a)) < clip(x(b)):
                c.create_rectangle(clip(x(a)), 14, clip(x(b)), 32, fill=t["chart_accent"], outline="")
        for val, label in ((self.marks.cut_in, "I"), (self.marks.cut_out, "O")):
            if val is not None and 6 <= x(val) <= w - 6:
                c.create_line(x(val), 8, x(val), 38, fill=t["fg"], width=2, tags=("mark_" + label,))
                c.create_text(x(val), 3, text=label, fill=t["fg"], anchor="n", font=("Segoe UI", 7))
        if 6 <= x(self.pos) <= w - 6:
            c.create_line(x(self.pos), 6, x(self.pos), 40, fill=t["select_bg"], width=3, tags=("playhead",))
        a, b = view.span(L)
        if view.zoomed() and L > 0:   # where the window sits in the whole file
            c.create_rectangle(6, 43, w - 6, 46, fill=t["chart_track"], outline="")
            c.create_rectangle(6 + ww * a / L, 42, 6 + max(ww * b / L, ww * a / L + 3), 47,
                               fill=t["select_bg"], outline="", tags=("window",))
            info = f"\u00d7{view.factor(L):.0f}  {core.fmt_time(a)} \u2013 {core.fmt_time(b)}   (0 or double-click: whole file)"
        else:
            info = "Alt+wheel zooms"
        c.create_text(6, 60, text=info, fill=t["muted"], anchor="sw", font=("Segoe UI", 7), tags=("zoominfo",))
        c.create_text(w - 6, 60, text=("LIVE " if self.live and not self._gone else "") + core.fmt_time(L),
                      fill=t["muted"], anchor="se", font=("Segoe UI", 7))
        if self.show_wave.get():
            self._draw_wave(c, t, a, b, ww)
        self._labels()

    def _draw_wave(self, c, t, a, b, ww) -> None:
        top, bottom = TIMELINE_H + 2, TIMELINE_H + WAVE_H - 2
        c.create_rectangle(6, top, 6 + ww, bottom, fill=t["chart_track"], outline="")
        wave = self.wave
        self._wave_seen = wave.version if wave is not None else -1
        cols = wave.window_columns(a, b, ww) if wave is not None else []
        if not any(cols):
            c.create_text(8 + 0, (top + bottom) / 2, anchor="w", fill=t["muted"], font=("Segoe UI", 7),
                          text="reading audio\u2026" if self._wave_job is not None and self._wave_job.running()
                          else "no audio", tags=("wave_note",))
            return
        mid, half = (top + bottom) / 2, (bottom - top) / 2
        pts = [(6 + i, mid - h * half) for i, h in enumerate(cols)]
        pts += [(6 + i, mid + h * half) for i, h in reversed(list(enumerate(cols)))]
        c.create_polygon(*[v for p in pts for v in p], fill=t["chart_accent"], outline="", tags=("wave",))

    # ------------------------------------------------------------- zoom / scroll
    def _typing(self) -> bool:
        try:
            f = self.win.focus_get()
        except (tk.TclError, KeyError):
            return False
        return f is not None and f.winfo_class() in ("Entry", "TEntry", "Spinbox", "TSpinbox", "Text", "TCombobox")

    def zoom_by(self, factor: float, at: float | None = None) -> None:
        L = self.marks.length or 0.0
        if at is None:   # keys zoom around the playhead
            at = self.pos
        self.view.zoom(factor, at, L)
        self._draw()

    def zoom_reset(self) -> None:
        self.view.reset()
        self._draw()

    def step(self, mult: float = 1.0) -> None:
        a, b = self.view.span(self.marks.length or 0.0)
        self.seek(self.pos + mult * timeline_view.seek_step(b - a))

    def _wheel(self, event):
        # Button-4/5 become ±120; a real MouseWheel delta is passed through,
        # so a high-resolution wheel keeps the same fractional notches.
        kind, n = timeline_view.wheel_action(event.state, wheel_delta(event))
        L = self.marks.length or 0.0
        if kind == "zoom":
            at = self.view.t(event.x - 6, max(1, self.timeline.winfo_width() - 12), L)
            self.view.zoom(timeline_view.WHEEL_ZOOM ** n, at, L)
            self._draw()
        elif kind == "seek":   # wheel up = back
            mult = 5 if timeline_view.is_shift(event.state) else 1
            self.step(-n * mult)
        elif self.view.zoomed():
            a, b = self.view.span(L)
            self.view.pan(-n * (b - a) * timeline_view.PAN_FRACTION, L)
            self._draw()
        return "break"

    # ------------------------------------------------------------- waveform
    def _load_wave_pref(self) -> bool:
        if not self.state_file:
            return False
        from msr import winstate
        return bool(winstate.load(self.state_file).get(WAVE_KEY, False))

    def _wave_changed(self) -> None:
        on = bool(self.show_wave.get())
        if self.state_file:
            from msr import winstate
            winstate.merge(self.state_file, {WAVE_KEY: on})
        if not on:
            self._wave_stop()
        self.timeline.configure(height=TIMELINE_H + (WAVE_H if on else 0))
        self._wave_tick()
        self._draw()

    def _wave_stop(self) -> None:
        if self._wave_job is not None:
            self._wave_job.cancel()
        self._wave_job = None

    def _wave_tick(self) -> None:
        """Start / resume reading the waveform in the background; never blocks Tk."""
        if not self.show_wave.get() or self._gone:
            return
        if self._wave_src != self.src:
            self._wave_stop()
            self._wave_src = self.src
            self.wave = waveform.load_cache(self.src, self._wave_dir) or waveform.Wave()
            self._wave_at = 0.0
        busy = self._wave_job is not None and self._wave_job.running()
        if busy:
            return
        L = self.marks.length or 0.0
        now = time.monotonic()
        due = self._wave_at == 0.0 or (self.live and now - self._wave_at >= WAVE_REFRESH_S)
        if due and (L <= 0 or self.wave.seconds() < L - 1.0):
            self._wave_at = now
            self._wave_job = waveform.WaveJob(self.src, self.ffmpeg, self.wave, self._wave_dir,
                                              run_lines=self._wave_runner).start()

    def _labels(self) -> None:
        m = self.marks
        self.pos_label.configure(text=f"{core.fmt_time(self.pos)} / {core.fmt_time(m.length)}")
        self.marks_label.configure(text=f"In {core.fmt_time(m.cut_in) if m.cut_in is not None else '--'}   "
                                        f"Out {core.fmt_time(m.cut_out) if m.cut_out is not None else '--'}   "
                                        f"Clip {core.fmt_time(m.clip_length) if m.clip_length else '--'}")
        self.live_label.configure(text="\u25cf recording (live edge grows)" if self.live and not self._gone else "")
        self.play_btn.configure(text="Pause" if self.playing else "Play")
        state = "normal" if (m.ready and not self._saving and not self._gone) else "disabled"
        self.save_btn.configure(state=state)
        self.live_btn.configure(text="Jump to live" if self.live else "Jump to end")

    def say(self, text: str) -> None:
        self.status.configure(text=text)

    # ------------------------------------------------------------- actions
    def seek(self, t: float, preview: bool = True) -> None:
        self.pos = min(max(0.0, t), self.marks.length or 0.0)
        if self.playing:
            self._start_playback(self.pos, self._stop_at)
        elif preview:
            self._request_frame()
        self.view.follow(self.pos, self.marks.length or 0.0)
        self._draw()

    def _scrub(self, event) -> None:
        w = max(10, self.timeline.winfo_width())
        L = self.marks.length or 0.0
        self.seek(self.view.t(event.x - 6, w - 12, L))

    def mark_in(self) -> None:
        self.marks.set_in(self.pos)
        self._draw()

    def mark_out(self) -> None:
        self.marks.set_out(self.pos)
        self._draw()

    def nudge(self, which: str, delta: float) -> None:
        self.marks.nudge(which, delta)
        point = self.marks.cut_in if which == "in" else self.marks.cut_out
        if point is not None and not self.playing:
            self.pos = point
            self._request_frame()
        self._draw()

    def replay(self, which: str) -> None:
        point = self.marks.cut_in if which == "in" else self.marks.cut_out
        if point is None:
            self.say(f"Mark the {which} point first.")
            return
        self._start_playback(max(0.0, point - REPLAY), point + REPLAY)

    def jump_live(self) -> None:
        self.seek(max(0.0, (self.marks.length or 0.0) - (3.0 if self.live else 0.5)))

    def toggle_play(self) -> None:
        if self.playing:
            self._stop_playback()
        else:
            self._start_playback(self.pos, None)
        self._draw()

    def _apply_volume(self) -> None:
        if self.audio is not None:
            self.audio.set_gain(self.volume.get() / 100.0)

    # ------------------------------------------------------------- playback
    def _start_playback(self, t: float, stop_at: float | None) -> None:
        self._stop_playback()
        if self._gone:
            return
        self.pos, self._stop_at = t, stop_at
        try:
            self.audio = self._audio_factory(self.src, self.ffmpeg)
            self.audio.set_gain(self.volume.get() / 100.0)
            self.audio.start(t)
        except Exception as e:  # no sound: the picture still plays on the wall clock
            self.audio = None
            self.say(f"No sound ({e}); playing the picture only.")
        size, fps = None, FPS
        if self._vinfo:
            w, h, src_fps = self._vinfo
            size, fps = core.preview_size(w, h, PREVIEW_W), min(self.fps_cap(), src_fps)
        self.video = VideoFeed(self.ffmpeg, self.src, t, size=size, fps=fps)
        self._play_from = (t, time.monotonic())
        self.playing = True
        self._draw()

    def _stop_playback(self) -> None:
        self.playing = False
        if self.audio is not None:
            try:
                self.audio.close()
            except Exception:
                pass
            self.audio = None
        if self.video is not None:
            self.video.close()
            self.video = None

    def _clock(self) -> float:
        sink = getattr(self.audio, "sink", None)
        real_sound = self.audio is not None and getattr(sink, "available", True)
        p = self.audio.position() if real_sound else None   # a silent sink runs ahead: use the wall clock
        if p is None or p < self._play_from[0]:
            p = self._play_from[0] + (time.monotonic() - self._play_from[1])
        return p

    def _tick_playback(self) -> None:
        p = self._clock()
        if self.video is not None:
            frame = self.video.take_until(p)
            if frame:
                self.shown += 1
                self._show(frame)
        self.pos = min(p, self.marks.length or p)
        ended = self.audio is not None and self.audio.ended() and p >= self.marks.length - 0.5
        if (self._stop_at is not None and p >= self._stop_at) or ended:
            self._stop_playback()
            self._request_frame()
        self.view.follow(self.pos, self.marks.length or 0.0)
        self._draw()

    # ------------------------------------------------------------- settings
    def _load_limit(self) -> bool:
        if not self.state_file:
            return False
        from msr import winstate
        return bool(winstate.load(self.state_file).get(LIMIT_KEY, False))

    def _limit_changed(self) -> None:
        if self.state_file:
            from msr import winstate
            winstate.merge(self.state_file, {LIMIT_KEY: bool(self.limit_fps.get())})
        if self.playing:   # takes effect at once
            self._start_playback(self.pos, self._stop_at)

    def fps_cap(self) -> float:
        return LIMITED_FPS if self.limit_fps.get() else MAX_FPS

    # ------------------------------------------------------------- preview frames (paused)
    def _cache_key(self, src, t):
        return (src, round(t, 2))

    def _request_frame(self) -> None:
        """Two stages: the keyframe at or before the spot first (no forward decode: fast after a
        far jump on a slow disk), then the exact frame. A newer position cancels both."""
        self._frame_gen += 1
        gen, t, src = self._frame_gen, self.pos, self.src
        hit = self._cache.get(self._cache_key(src, t))
        if hit is not None:
            self._cache[self._cache_key(src, t)] = self._cache.pop(self._cache_key(src, t))
            self.q.put(("frame", gen, hit, True))
            return
        stale = lambda: gen != self._frame_gen or self._closed  # noqa: E731

        def work(keyframe):
            time.sleep(0.02)              # coalesce a burst of drag events
            if stale():
                return
            ppm = self._grab(self.ffmpeg, src, t, keyframe=keyframe, cancel=stale)
            if not stale() and (ppm or not keyframe):
                self.q.put(("frame", gen, ppm, not keyframe))
        # both at once: the keyframe-only decode lands first, the exact one replaces it
        for keyframe in (True, False):
            threading.Thread(target=work, args=(keyframe,), daemon=True).start()

    def _on_frame(self, gen, ppm, exact) -> None:
        if gen != self._frame_gen or self.playing:
            return
        if not ppm:
            if exact and not self._img:
                self.preview.configure(text="No preview at this point yet.")
            return
        if exact:
            key = self._cache_key(self.src, self.pos)
            self._cache[key] = ppm
            while len(self._cache) > CACHE_FRAMES:
                self._cache.pop(next(iter(self._cache)))
            self._exact_gen = gen
        elif self._exact_gen == gen:
            return                        # the exact one already landed
        self._show(ppm)

    def _show(self, ppm: bytes) -> None:
        try:
            if self._img is not None and self._img_size == ppm[:24].split(b"\n255\n")[0]:
                self._img.configure(data=ppm, format="ppm")   # same size: reuse the image, no relayout
                return
            img = tk.PhotoImage(data=ppm, format="ppm")
        except tk.TclError:
            return
        self._img = img
        self._img_size = ppm[:24].split(b"\n255\n")[0]
        self.preview.configure(image=img, text="")

    # ------------------------------------------------------------- length / source
    def _refresh_length(self, force: bool = False) -> None:
        now = time.monotonic()
        if self._length_busy or (not force and now - self._last_length_check < LENGTH_EVERY_S):
            return
        if not force and not self.live and self.marks.length:
            return
        self._last_length_check, self._length_busy = now, True
        src = self.src

        need_info = self._vinfo is None

        def work():
            exists = os.path.isfile(src)
            n = self._length(src) if exists else None
            if exists and need_info:
                try:
                    self.q.put(("vinfo", src, self._vinfo_reader(src)))
                except Exception:
                    pass
            self.q.put(("length", src, exists, n))
        threading.Thread(target=work, daemon=True).start()

    def _on_length(self, src: str, exists: bool, n: float | None) -> None:
        self._length_busy = False
        if src != self.src:
            return
        first = not self.marks.length
        if not exists:
            nxt = core.finished_successor(self.src)
            if nxt:
                self._switch(nxt, "The recording ended; now editing the finished file (same timeline).")
                return
            if not self._gone:
                self._gone = True
                self._stop_playback()
                self.say("The recording file is not reachable any more (moved, or the drive is gone). "
                         "Your marks are kept; Save is off until it is back.")
            self._draw()
            return
        if self._gone:
            self._gone = False
            self.say("The file is back.")
        if n is not None:
            self.marks.grow(n)
        if self.live and self.follow is not None:
            try:
                current = self.follow()
            except Exception:
                current = None
            if current is None:
                self.live = False
            elif os.path.normcase(current) != os.path.normcase(self.src):
                self.live = False
                self.say("A new part of the recording started; this editor keeps the part you opened.")
            if not self.live:
                self._wave_at = 0.0   # live edge stopped growing: one last waveform read of the tail
        if first and n:
            self.pos = max(0.0, n - 10.0) if self.live else 0.0
            if self._start_at is not None and not self.live:
                self.pos = min(max(0.0, self._start_at), n)
                if self._end_at is not None and self._end_at > self._start_at:
                    self.marks.set_in(self.pos)
                    self.marks.set_out(self._end_at)
            self._request_frame()
        self._draw()

    def _switch(self, path: str, note: str) -> None:
        self._stop_playback()
        self.src, self.live, self._vinfo = path, False, None
        self.file_label.configure(text=os.path.basename(path))
        self.say(note)
        self._refresh_length(force=True)

    # ------------------------------------------------------------- saving
    def save(self) -> None:
        if not self.marks.ready or self._saving:
            return
        self._saving = True
        a, b, exact = self.marks.cut_in, self.marks.cut_out, self.exact.get()
        src, out_dir = self.src, core.clips_folder_for(self.src, self.clips_base)
        self.say("Saving the clip\u2026" + (" (re-encoding, this takes a moment)" if exact else ""))
        self._draw()

        def work():
            try:
                res = core.make_clip(self.ffmpeg, src, a, b, out_dir, exact=exact, ffprobe=self.ffprobe)
                core.write_clip_meta(res, src)   # links the clip to its source (Library > Recordings)
                self.q.put(("saved", res, None))
            except core.ClipError as e:
                self.q.put(("saved", None, str(e)))
            except Exception as e:  # never leave the window stuck in "saving"
                self.q.put(("saved", None, f"{type(e).__name__}: {e}"))
        threading.Thread(target=work, daemon=True).start()

    def _on_saved(self, res, err) -> None:
        self._saving = False
        if err:
            self.say(f"Not saved: {err}")
        else:
            self.last_saved = res
            how = ("exact cut" if res.exact else
                   f"starts at the keyframe {core.fmt_time(res.start)} (asked {core.fmt_time(res.requested_start)})"
                   if res.snapped else "keyframe-aligned")
            self.say(f"Saved {os.path.basename(res.path)}: {core.fmt_time(res.start)} \u2013 "
                     f"{core.fmt_time(res.end)}, {res.duration:.1f} s, {how}.")
            if self.logger is not None:
                self.logger.info(f"Clip Editor: saved {res.path} ({res.duration:.1f} s)")
        self._draw()

    def open_clips(self) -> None:
        folder = core.clips_folder_for(self.src, self.clips_base)
        if not os.path.isdir(folder):
            folder = self.clips_base
        try:
            if self.opener is None:
                raise OSError("no opener")
            self.opener(folder)   # the app's own (hidden) folder opener
        except Exception:
            self.say(f"Clips folder: {folder}")

    # ------------------------------------------------------------- loop
    def _drain(self) -> None:
        while True:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                return
            kind = item[0]
            if kind == "frame":
                self._on_frame(*item[1:])
            elif kind == "vinfo":
                if item[1] == self.src and item[2]:
                    self._vinfo = item[2]
            elif kind == "length":
                self._on_length(*item[1:])
            elif kind == "saved":
                self._on_saved(*item[1:])

    def _poll(self) -> None:
        if self._closed:
            return
        try:
            self._drain()
            if self.playing:
                self._tick_playback()
            self._refresh_length()
            self._wave_tick()
            if self.wave is not None and self.wave.version != self._wave_seen and self.show_wave.get() and not self.playing:
                self._draw()
        except Exception:   # one bad item must not stop the loop (and freeze the window)
            if self.logger is not None:
                self.logger.exception("Clip Editor: update failed")
        finally:
            if not self._closed:
                self._job = self.win.after(PLAY_POLL_MS if self.playing else POLL_MS, self._poll)

    def close(self) -> None:
        self._closed = True
        self._wave_stop()
        self._stop_playback()
        if self._job is not None:
            try:
                self.win.after_cancel(self._job)
            except tk.TclError:
                pass
        try:
            self.win.destroy()
        except tk.TclError:
            pass


def open_editor(parent, src, **kw) -> ClipEditor | None:
    if not src or not os.path.isfile(src):
        dialogs.show_info(parent, "Clip Editor", "There is no recording file to edit for this yet.")
        return None
    return ClipEditor(parent, src, **kw)
