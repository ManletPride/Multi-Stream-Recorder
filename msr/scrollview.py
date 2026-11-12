"""A vertical scrolling frame, the same arrangement the Options tab uses.

One canvas, a ttk scrollbar on the right, and the mouse wheel from ``msr.wheel``
(Windows/macOS ``<MouseWheel>`` and X11 ``<Button-4>`` / ``<Button-5>``). The wheel
binding is ``bind_all`` only while the pointer is inside this frame, so it works
over the entries and charts, and it is dropped on Leave and when the tab is
hidden. A wheel whose widget is outside this frame does nothing here, which
leaves a Treeview, Text or list beside the frame free to scroll itself.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from msr import themes
from msr.wheel import bind_wheel, unbind_wheel, wheel_steps, widget_is_inside


class ScrollArea:
    """Vertical canvas plus scrollbar.

    ``stretch=False`` (Options): the inner frame is as tall as its content, and
    only the width follows the canvas.
    ``stretch=True`` (Library overview): the inner frame is at least as tall as
    its content, and at least as tall as the canvas. A short window scrolls
    instead of squashing the content; a tall window lets the content fill it.
    """

    def __init__(self, container, *, padding=0, stretch: bool = False, passive: bool = False):
        bg = themes.current()["bg"]
        self.stretch = stretch
        self.frame = ttk.Frame(container)
        self.canvas = tk.Canvas(self.frame, highlightthickness=0, borderwidth=0, bg=bg)
        if passive:
            # Empty canvas, no click of its own: a press there may drag the window.
            self.canvas.msr_drag_passive = True
        self.scroll = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scroll.set)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.body = ttk.Frame(self.canvas, padding=padding)
        self.window = self.canvas.create_window(0, 0, window=self.body, anchor="nw")
        self.body.bind("<Configure>", self._on_body)
        self.canvas.bind("<Configure>", self._on_canvas)
        # Leave also fires when the pointer moves onto a child (NotifyInferior);
        # that must not drop the binding. Hiding the tab (Unmap) does drop it.
        self.frame.bind("<Enter>", self.wheel_on, add="+")
        self.frame.bind("<Leave>", self.wheel_off, add="+")
        self.frame.bind("<Unmap>", self.wheel_unmap, add="+")
        self.frame.bind("<Destroy>", self._on_destroy, add="+")

    def apply_theme(self) -> None:
        self.canvas.configure(bg=themes.current()["bg"])

    def _on_body(self, _event=None):
        if self.stretch:
            self._place()
        else:
            box = self.canvas.bbox("all")
            if box:
                self.canvas.configure(scrollregion=box)

    def _on_canvas(self, event):
        if self.stretch:
            self._place(width=event.width, height=event.height)
        else:
            self.canvas.itemconfigure(self.window, width=event.width)

    def _place(self, width=None, height=None):
        if width is None:
            width = self.canvas.winfo_width()
        if height is None:
            height = self.canvas.winfo_height()
        if width <= 1:
            return
        self.canvas.itemconfigure(self.window, width=width)
        if height > 1:
            # Requested height, not the height we already forced. A short canvas
            # keeps the content at that size and scrolls; a tall one grows it.
            target = max(self.body.winfo_reqheight(), height)
            current = int(float(self.canvas.itemcget(self.window, "height") or 0))
            if current != target:
                self.canvas.itemconfigure(self.window, height=target)
        box = self.canvas.bbox("all")
        if box:
            self.canvas.configure(scrollregion=box)

    def wheel_on(self, _event=None):
        bind_wheel(self.canvas, self.wheel, all=True)

    def wheel_off(self, _event=None):
        try:
            x, y = self.frame.winfo_pointerxy()
            under = self.frame.winfo_containing(x, y)
        except tk.TclError:
            under = None
        if widget_is_inside(under, self.frame):
            return
        unbind_wheel(self.canvas, all=True)

    def wheel_unmap(self, event):
        if getattr(event, "widget", None) is self.frame:
            unbind_wheel(self.canvas, all=True)

    def _on_destroy(self, event):
        if getattr(event, "widget", None) is self.frame:
            unbind_wheel(self.canvas, all=True)

    def wheel(self, event):
        widget = getattr(event, "widget", None)
        # bind_all is global. A wheel over a list that is not inside this frame
        # belongs to that list (its own binding already ran).
        if widget is not None and not widget_is_inside(widget, self.frame):
            return
        steps = wheel_steps(event)
        if steps and self.canvas.yview() != (0.0, 1.0):
            self.canvas.yview_scroll(-steps, "units")
