"""Live thumbnail preview strip for the Status tab.

Extracted from ``msr/gui.py`` so preview state, tile lifecycle, pinned big
preview, and background ``ffmpeg`` frame grabbing live in a self-contained,
theme-aware widget controller with zero ``NameError`` guards.
"""
from __future__ import annotations

import logging
import os
import tempfile
import threading
import tkinter as tk
from tkinter import ttk

try:
    from PIL import Image
except ImportError:
    Image = None

from msr import preview_state as _preview_state
from msr import themes as _themes


class PreviewStrip:
    """Horizontal strip of live recording thumbnails with double-click pin view."""

    PREVIEW_W = 160
    PREVIEW_H = 90
    PREVIEW_BIG_W = 960
    PREVIEW_BIG_H = 540  # 6x strip; sharper pin without a full 1080p still
    PREVIEW_MS = 4000

    def __init__(
        self,
        parent,
        root,
        *,
        config,
        get_jobs,
        get_display_name,
        on_select,
        grab_frame,
        is_media_busy=None,
    ):
        self.root = root
        self.config = config
        self._get_jobs = get_jobs
        self._get_display_name = get_display_name
        self._on_select = on_select
        self._grab_frame = grab_frame
        self._is_media_busy = is_media_busy or (lambda _ch: False)

        self._photos: dict[str, tk.PhotoImage] = {}
        self._tiles: dict[str, dict] = {}
        self._busy = False
        self._again = False
        self._big_ch: str | None = None
        self._big_photo: tk.PhotoImage | None = None
        self._selected: str | None = None
        self._pstate = _preview_state.PreviewState()
        self._after_id = None
        self._kick_queued = False

        self.wrap = tk.Frame(parent)
        self.wrap.pack(fill=tk.X, padx=10, pady=(0, 6))

        self.big_frame = tk.Frame(self.wrap, padx=6, pady=6, highlightthickness=1)
        self.big_header = tk.Frame(self.big_frame)
        self.big_header.pack(fill=tk.X, pady=(0, 4))
        self.big_name = tk.Label(
            self.big_header, text="", font=("Segoe UI", 10, "bold"), anchor="w",
        )
        self.big_name.pack(side=tk.LEFT)
        self.big_close = ttk.Button(
            self.big_header, text="Close", width=7, command=self.hide_big,
        )
        self.big_close.pack(side=tk.RIGHT)
        self.big_hint = tk.Label(
            self.big_header,
            text="Double-click a tile to pin \u00b7 Close to dismiss",
            font=("Segoe UI", 8), anchor="e",
        )
        self.big_hint.pack(side=tk.RIGHT, padx=(8, 6))
        self.big_img = tk.Label(self.big_frame, bd=0)
        self.big_img.pack()
        self.big_img.bind("<Button-1>", lambda _e: (
            self._on_select(self._big_ch) if self._big_ch else None
        ))
        self.big_img.bind("<Double-Button-1>", lambda _e: self.hide_big())

        self.canvas = tk.Canvas(
            self.wrap, height=28, highlightthickness=1, bd=0,
        )
        self.hsb = ttk.Scrollbar(
            self.wrap, orient="horizontal", command=self.canvas.xview,
        )
        self.canvas.configure(xscrollcommand=self.hsb.set)
        self.canvas.pack(fill=tk.X, side=tk.TOP)
        self.canvas.msr_drag_passive = True
        self.hsb.pack(fill=tk.X, side=tk.TOP)

        self.inner = tk.Frame(self.canvas)
        self._canvas_win = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.empty_label = tk.Label(
            self.inner,
            text="Live previews appear here while channels are recording",
            font=("Segoe UI", 9), anchor="w", padx=8, pady=4,
        )
        self.empty_label.pack(side=tk.LEFT)
        self.inner.bind(
            "<Configure>",
            lambda _e=None: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )
        self.wrap.bind("<Map>", self.kick, add="+")

    # ── Theming & selection ─────────────────────────────────────────────────

    def apply_theme(self, t) -> None:
        self.wrap.configure(bg=t["bg"])
        self.canvas.configure(
            bg=t["tree_field"], highlightbackground=t["border"],
            highlightcolor=t["border"],
        )
        self.inner.configure(bg=t["tree_field"])
        self.empty_label.configure(bg=t["tree_field"], fg=t["offline_fg"])
        self.big_frame.configure(
            bg=t["tree_field"], highlightbackground=t["accent"],
            highlightcolor=t["accent"],
        )
        self.big_header.configure(bg=t["tree_field"])
        self.big_name.configure(bg=t["tree_field"], fg=t["fg"])
        self.big_hint.configure(bg=t["tree_field"], fg=t["offline_fg"])
        self.big_img.configure(bg=t["tree_field"], fg=t["fg"])
        for tile in self._tiles.values():
            tile["frame"].configure(bg=t["tree_field"], highlightbackground=t["border"])
            tile["img"].configure(bg=t["tree_field"])
            tile["name"].configure(bg=t["tree_field"], fg=t["fg"])
        self.paint_selection(self._selected)

    def _style_tile(self, ch_name: str, t, selected: bool) -> None:
        tile = self._tiles.get(ch_name)
        if not tile:
            return
        look = _preview_state.tile_look(self._pstate.status(ch_name), t, selected)
        tile["frame"].configure(highlightbackground=look.border, highlightthickness=look.thickness)
        tile["name"].configure(text=self._get_display_name(ch_name) + look.suffix, fg=look.name_fg)

    def _style_big(self, t) -> None:
        ch_name = self._big_ch
        if not ch_name:
            return
        st = self._pstate.status(ch_name)
        look = _preview_state.tile_look(st, t)
        border = _preview_state.pane_border(st, t)
        self.big_frame.configure(highlightbackground=border, highlightcolor=border)
        self.big_name.configure(text=self._get_display_name(ch_name) + look.suffix, fg=look.name_fg)

    def paint_selection(self, ch_name: str | None) -> None:
        """Accent the active tile and show each tile's age."""
        t = _themes.current()
        self._selected = ch_name
        for name in self._tiles:
            self._style_tile(name, t, name == ch_name)
        self._style_big(t)

    def scroll_into_view(self, ch_name: str | None) -> None:
        tile = self._tiles.get(ch_name) if ch_name else None
        if not tile:
            return
        try:
            inner_w = self.inner.winfo_reqwidth()
            canvas_w = self.canvas.winfo_width()
            if inner_w > canvas_w > 1:
                x = tile["frame"].winfo_x()
                self.canvas.xview_moveto(max(0.0, min(1.0, x / inner_w)))
        except tk.TclError:
            pass

    # ── Pinned big preview ──────────────────────────────────────────────────

    @staticmethod
    def _cache_dir() -> str:
        if os.name == "nt":
            folder = os.path.join(tempfile.gettempdir(), "msr-previews")
        else:
            xdg = (os.environ.get("XDG_CACHE_HOME") or "").strip()
            base = os.path.abspath(os.path.expanduser(xdg)) if xdg else os.path.join(
                os.path.expanduser("~"), ".cache",
            )
            parent = os.path.join(base, "multi-stream-recorder")
            folder = os.path.join(parent, "previews")
            try:
                os.makedirs(parent, mode=0o700, exist_ok=True)
                os.chmod(parent, 0o700)
                os.makedirs(folder, mode=0o700, exist_ok=True)
                os.chmod(folder, 0o700)
            except OSError:
                pass
        return folder

    @classmethod
    def cleanup_cache(cls) -> int:
        """Delete cached preview PNGs on app exit (never follows symlinks)."""
        removed = 0
        folder = cls._cache_dir()
        if not os.path.isdir(folder) or os.path.islink(folder):
            return 0
        try:
            entries = os.listdir(folder)
        except OSError:
            return 0
        for name in entries:
            if not name.lower().endswith(".png"):
                continue
            path = os.path.join(folder, name)
            try:
                if os.path.islink(path):
                    continue
                os.remove(path)
                removed += 1
            except OSError:
                pass
        return removed

    @classmethod
    def _cache_file(cls, ch_name: str, suffix: str = "") -> str:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in ch_name)[:80]
        return os.path.join(cls._cache_dir(), f"{safe}{suffix}.png")

    def _set_big_image(self, png_path=None, from_small=None) -> None:
        photo = None
        if png_path and os.path.isfile(png_path):
            try:
                photo = tk.PhotoImage(file=png_path)
            except tk.TclError:
                photo = None
        if photo is None and from_small is not None:
            try:
                zx = max(1, self.PREVIEW_BIG_W // self.PREVIEW_W)
                zy = max(1, self.PREVIEW_BIG_H // self.PREVIEW_H)
                photo = from_small.zoom(zx, zy)
            except tk.TclError:
                photo = None
        if photo is None:
            return
        self.big_img.configure(image=photo)
        self._big_photo = photo

    def hide_big(self) -> None:
        self._big_ch = None
        self._big_photo = None
        try:
            self.big_frame.pack_forget()
            self.big_img.configure(image="")
        except tk.TclError:
            pass

    def show_big(self, ch_name: str | None) -> None:
        if not ch_name:
            return
        if self._big_ch == ch_name:
            self.hide_big()
            return
        if self._pstate.status(ch_name).kind == "ended":
            return
        self._big_ch = ch_name
        self.big_name.configure(text=self._get_display_name(ch_name))
        self._style_big(_themes.current())
        try:
            packed = bool(self.big_frame.pack_info())
        except tk.TclError:
            packed = False
        if not packed:
            self.big_frame.pack(
                fill=tk.X, side=tk.TOP, before=self.canvas, pady=(0, 6),
            )
        self._set_big_image(from_small=self._photos.get(ch_name))
        self._on_select(ch_name)

    # ── Tiles & periodic grab ───────────────────────────────────────────────

    def _sync_tiles(self, keys: list[str]) -> None:
        t = _themes.current()
        for ch_name in keys:
            self._pstate.mark_active(ch_name)
        shown = list(keys)
        for ch_name in list(self._tiles):
            if ch_name in keys:
                continue
            self._pstate.mark_ended(ch_name)
            if self._pstate.keep_ended(ch_name):
                shown.append(ch_name)
                continue
            self._tiles[ch_name]["frame"].destroy()
            self._tiles.pop(ch_name, None)
            self._photos.pop(ch_name, None)
            self._pstate.forget(ch_name)
        if self._big_ch and self._big_ch not in keys:
            self.hide_big()
        if not shown:
            self.empty_label.pack(side=tk.LEFT)
            self.canvas.configure(height=28)
            return
        self.empty_label.pack_forget()
        self.canvas.configure(height=self.PREVIEW_H + 42)
        for ch_name in shown:
            if ch_name in self._tiles:
                continue
            tile = tk.Frame(
                self.inner, padx=4, pady=4,
                highlightthickness=1, highlightbackground=t["border"],
                bg=t["tree_field"],
            )
            blank = tk.PhotoImage(width=self.PREVIEW_W, height=self.PREVIEW_H)
            img_lbl = tk.Label(tile, image=blank, bg=t["tree_field"], bd=0)
            self._photos[ch_name] = blank
            img_lbl.pack()
            name_lbl = tk.Label(
                tile, text=self._get_display_name(ch_name),
                font=("Segoe UI", 8), bg=t["tree_field"], fg=t["fg"],
                wraplength=self.PREVIEW_W, justify=tk.CENTER,
            )
            name_lbl.pack()
            for widget in (tile, img_lbl, name_lbl):
                widget.bind("<Button-1>", lambda _e, k=ch_name: self._on_select(k))
                widget.bind("<Double-Button-1>", lambda _e, k=ch_name: self.show_big(k))
            self._tiles[ch_name] = {"frame": tile, "img": img_lbl, "name": name_lbl}

        packed = []
        for child in self.inner.pack_slaves():
            for name, tile in self._tiles.items():
                if tile["frame"] is child:
                    packed.append(name)
                    break
        if packed != shown:
            for ch_name in shown:
                self._tiles[ch_name]["frame"].pack_forget()
            for ch_name in shown:
                self._tiles[ch_name]["frame"].pack(side=tk.LEFT, padx=(0, 6))
        self.paint_selection(self._selected)

    def _set_tile_image(self, ch_name: str, png_path: str) -> None:
        tile = self._tiles.get(ch_name)
        if not tile or not png_path or not os.path.isfile(png_path):
            return
        try:
            photo = tk.PhotoImage(file=png_path)
        except tk.TclError:
            return
        tile["img"].configure(image=photo, width=self.PREVIEW_W, height=self.PREVIEW_H)
        self._photos[ch_name] = photo

    def _finish_batch(self, results, keys) -> None:
        self._busy = False
        try:
            if not self.root.winfo_exists():
                return
        except tk.TclError:
            return
        self._sync_tiles(keys)
        for row in results:
            ch_name = row[0]
            png_path = row[1] if len(row) > 1 else None
            big_path = row[2] if len(row) > 2 else None
            if png_path:
                self._set_tile_image(ch_name, png_path)
            if self._big_ch == ch_name:
                self._set_big_image(
                    png_path=big_path,
                    from_small=self._photos.get(ch_name),
                )
        self.paint_selection(self._selected)
        if self._again:
            self._again = False
            self.kick()

    def tick(self) -> None:
        self._kick_queued = False
        try:
            if not self.root.winfo_exists():
                return
        except tk.TclError:
            return
        if self._after_id is not None:
            try:
                self.root.after_cancel(self._after_id)
            except tk.TclError:
                pass
        self._after_id = self.root.after(self.PREVIEW_MS, self.tick)
        try:
            if not self.wrap.winfo_viewable():
                return
        except tk.TclError:
            return
        if self._busy:
            return
        jobs = self._get_jobs()
        keys = [ch for ch, _path in jobs]
        if not jobs:
            self._sync_tiles([])
            self.paint_selection(self._selected)
            return
        self._busy = True
        ffmpeg_path = self.config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"

        def _work():
            grabbed = []
            try:
                for ch_name, raw_file in jobs:
                    if self._is_media_busy(ch_name):
                        continue
                    self._pstate.note_source(ch_name, raw_file)
                    if not self._pstate.due(ch_name):
                        continue
                    out = self._cache_file(ch_name)
                    started = self._pstate.clock()
                    report = {}
                    is_pinned = (self._big_ch == ch_name)
                    big_out = None
                    if is_pinned and Image is not None:
                        big_path = self._cache_file(ch_name, "_big")
                        ok, msg = self._grab_frame(
                            raw_file, big_path, ffmpeg_path, logging,
                            width=self.PREVIEW_BIG_W, height=self.PREVIEW_BIG_H,
                            attempts=self._pstate.plan(ch_name), report=report,
                        )
                        if ok:
                            big_out = big_path
                            self._pstate.record_success(ch_name, report.get("attempt"))
                            try:
                                with Image.open(big_path) as im:
                                    im.resize((self.PREVIEW_W, self.PREVIEW_H)).save(out, format="PNG")
                            except Exception:
                                ok, msg = self._grab_frame(
                                    raw_file, out, ffmpeg_path, logging,
                                    width=self.PREVIEW_W, height=self.PREVIEW_H,
                                    attempts=self._pstate.plan(ch_name, allow_retry=False),
                                )
                        else:
                            wait = self._pstate.record_failure(ch_name, started)
                            logging.debug("Preview grab failed for %s (%s); next try in %.0f s", ch_name, msg, wait)
                    else:
                        ok, msg = self._grab_frame(
                            raw_file, out, ffmpeg_path, logging,
                            width=self.PREVIEW_W, height=self.PREVIEW_H,
                            attempts=self._pstate.plan(ch_name), report=report,
                        )
                        if ok:
                            self._pstate.record_success(ch_name, report.get("attempt"))
                        else:
                            wait = self._pstate.record_failure(ch_name, started)
                            logging.debug("Preview grab failed for %s (%s); next try in %.0f s", ch_name, msg, wait)
                        if ok and is_pinned:
                            big_path = self._cache_file(ch_name, "_big")
                            ok_big, _msg = self._grab_frame(
                                raw_file, big_path, ffmpeg_path, logging,
                                width=self.PREVIEW_BIG_W, height=self.PREVIEW_BIG_H,
                                attempts=self._pstate.plan(ch_name, allow_retry=False),
                            )
                            if ok_big:
                                big_out = big_path
                    grabbed.append((ch_name, out if ok else None, big_out))
            except Exception:
                logging.debug("Preview grab failed", exc_info=True)
            try:
                self.root.after(0, lambda: self._finish_batch(grabbed, keys))
            except Exception:
                self._busy = False

        threading.Thread(target=_work, daemon=True, name="preview-grab").start()

    def kick(self, _event=None) -> None:
        """Grab now (strip just became visible) instead of waiting for the next timer."""
        self._pstate.clear_backoff()
        try:
            self.paint_selection(self._selected)
        except Exception:
            pass
        if self._busy:
            self._again = True
            return
        if self._kick_queued:
            return
        self._kick_queued = True
        try:
            self.root.after_idle(self.tick)
        except tk.TclError:
            self._kick_queued = False

    def start(self, delay_ms: int = 2000) -> None:
        self._after_id = self.root.after(delay_ms, self.tick)
