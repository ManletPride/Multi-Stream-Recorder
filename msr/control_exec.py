"""Ordered execution for control commands (no tkinter here).

Control requests arrive on server threads. They never touch the window or the recorder
themselves: they put ``(callable, reply slot)`` on a ``SerialQueue`` and wait. The owner
thread runs the jobs one at a time, in order:

* GUI: the Tk thread, through ``TkPump`` (its own ``after(50)`` loop, like the Library and
  Updates queues). ``GuiBridge`` is the GUI's control adapter built on it.
* headless and the recorder lane: a ``SerialExecutor`` thread.

A job that has not started when its caller gives up is cancelled (it never runs later, so a
retry cannot do it twice) and the caller gets a ``busy`` error naming the command.
"""

from __future__ import annotations

import logging
import queue
import threading
import time

from msr.control import AppAdapter, ControlError

DEFAULT_TIMEOUT = 30.0
_local = threading.local()


class command_name:
    """``with command_name("clip"):`` names the command in timeout/busy errors on this thread."""

    def __init__(self, name):
        self.name = str(name or "")

    def __enter__(self):
        self._old = getattr(_local, "name", None)
        _local.name = self.name
        return self

    def __exit__(self, *exc):
        _local.name = self._old
        return False


def current_command() -> str:
    return getattr(_local, "name", None) or "command"


class _Job:
    __slots__ = ("fn", "done", "lock", "state", "value", "error")

    def __init__(self, fn):
        self.fn = fn
        self.done = threading.Event()
        self.lock = threading.Lock()
        self.state = "queued"      # queued -> running -> done, or queued -> cancelled
        self.value = None
        self.error = None

    def run(self):
        with self.lock:
            if self.state != "queued":
                return
            self.state = "running"
        try:
            self.value = self.fn()
        except BaseException as e:  # handed to the caller's thread
            self.error = e
        finally:
            self.state = "done"
            self.done.set()

    def cancel(self) -> bool:
        with self.lock:
            if self.state == "queued":
                self.state = "cancelled"
                return True
            return False


class SerialQueue:
    """Jobs from any thread, run in order on one owner thread (``drain`` runs there)."""

    def __init__(self, label, timeout=DEFAULT_TIMEOUT):
        self.label = label
        self.timeout = float(timeout)
        self.owner = None
        self._q = queue.Queue()
        self._closed = False

    def is_owner(self) -> bool:
        return self.owner is not None and threading.current_thread() is self.owner

    @property
    def closed(self) -> bool:
        return self._closed

    def post(self, fn) -> None:
        """Run fn on the owner later; nobody waits for it."""
        if self.is_owner():
            fn()
            return
        if not self._closed:
            self._q.put(_Job(fn))

    def call(self, fn, what=None, timeout=None):
        """Run fn on the owner thread and return its result (or raise its error)."""
        if self.is_owner():
            return fn()
        what = what or current_command()
        if self._closed:
            raise ControlError(f"'{what}' was refused: the {self.label} is shutting down.", "busy")
        wait = self.timeout if timeout is None else float(timeout)
        job = _Job(fn)
        self._q.put(job)
        if not job.done.wait(wait):
            if job.cancel():
                raise ControlError(f"'{what}' timed out: the {self.label} was busy for {wait:g} s and did not "
                                   "start it. Nothing was done; try again.", "busy")
            if not job.done.wait(wait):
                raise ControlError(f"'{what}' is still running in the {self.label} after {2 * wait:g} s; "
                                   "check status before trying again.", "timeout")
        if job.state == "cancelled":
            raise ControlError(f"'{what}' was dropped: the {self.label} is shutting down.", "busy")
        if job.error is not None:
            raise job.error
        return job.value

    def drain(self, budget=0.05) -> int:
        """Owner thread: run queued jobs for up to ``budget`` seconds (the window stays responsive)."""
        end = time.monotonic() + budget
        ran = 0
        while True:
            try:
                job = self._q.get_nowait()
            except queue.Empty:
                break
            job.run()
            ran += 1
            if time.monotonic() >= end:
                break
        return ran

    def close(self) -> None:
        """Refuse new jobs and drop queued ones (their callers get ``busy`` at once)."""
        self._closed = True
        while True:
            try:
                job = self._q.get_nowait()
            except queue.Empty:
                break
            if job.cancel():
                job.done.set()


class TkPump:
    """Drains a SerialQueue on the Tk thread every ``interval`` ms. Create it on that thread."""

    def __init__(self, root, jobs, interval=50):
        self.root = root
        self.jobs = jobs
        self.interval = int(interval)
        self._stopped = False
        self._after = None
        jobs.owner = threading.current_thread()

    def start(self):
        self._after = self.root.after(self.interval, self._tick)
        return self

    def _tick(self):
        if self._stopped:
            return
        try:
            self.jobs.drain()
        except Exception:
            logging.exception("control: queued job failed")
        try:
            self._after = self.root.after(self.interval, self._tick)
        except Exception:  # window gone
            self._stopped = True

    def stop(self):
        """Tk thread: stop draining and fail anything still queued."""
        self._stopped = True
        self.jobs.close()
        try:
            if self._after is not None:
                self.root.after_cancel(self._after)
        except Exception:
            pass


class SerialExecutor(SerialQueue):
    """A SerialQueue with its own daemon thread as the owner (headless, recorder lane)."""

    def __init__(self, label, timeout=DEFAULT_TIMEOUT):
        super().__init__(label, timeout)
        self.owner = threading.Thread(target=self._loop, daemon=True, name=f"control-{label.replace(' ', '-')}")
        self.owner.start()

    def _loop(self):
        while not self._closed:
            try:
                job = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                job.run()
            except Exception:
                logging.exception("control: queued job failed")


class GuiBridge(AppAdapter):
    """The GUI's control adapter. Everything that reads or changes window state (the roster
    list, the tree, start/stop, quit, setting selectors) runs on the Tk thread via ``jobs``.

    ``hooks``: get_recorder, get_presence, channels (the live list), after_edit (redraw + save),
    start, stop, schedule_quit(after_children), reload (count), reflect(section, key),
    apply_setting(section, key, value) (checks + save + reflect), twitch_changed, update_tag,
    health_report, health_check_now, sound_test(event)."""

    mode = "gui"

    def __init__(self, config, jobs, hooks):
        super().__init__(config)
        self.jobs = jobs
        self.hooks = hooks

    def _h(self, name):
        hook = self.hooks.get(name)
        if hook is None:
            raise ControlError(f"The window has no '{name}' hook.", "unsupported")
        return hook

    @property
    def recorder(self):
        return self._h("get_recorder")()

    @property
    def presence(self):
        return self._h("get_presence")()

    def records(self):
        return self.jobs.call(lambda: [dict(c) for c in list(self._h("channels")())])

    def mutate(self, fn):
        def work():
            result = fn(self._h("channels")())
            self._h("after_edit")()
            return result
        return self.jobs.call(work)

    def start_session(self):
        return self.jobs.call(self._h("start"))

    def stop_session(self):
        def work():
            rec = self._h("get_recorder")()
            if not (rec and rec.is_running):
                raise ControlError("Recording is not running.", "conflict")
            self._h("stop")()
            return "stopping"
        return self.jobs.call(work)

    def request_quit(self, after_children=None):
        self.jobs.post(lambda: self._h("schedule_quit")(after_children))

    def reload_channels(self):
        return self.jobs.call(self._h("reload"))

    def settings_changed(self, section, key):
        self.jobs.call(lambda: self._h("reflect")(section, key))

    def apply_setting(self, section, key, value):
        return self.jobs.call(lambda: self._h("apply_setting")(section, key, value))

    def health_report(self):
        return self.jobs.call(self._h("health_report")) if "health_report" in self.hooks else None

    def health_check_now(self):
        if "health_check_now" not in self.hooks:
            return False
        self.jobs.call(self._h("health_check_now"))
        return True

    def sound_test(self, event):
        return self.jobs.call(lambda: self._h("sound_test")(event))

    def twitch_changed(self):
        self.jobs.post(self._h("twitch_changed"))

    def update_info(self):
        tag = self._h("update_tag")()
        return {"available": bool(tag), "latest": tag}

    def close(self):
        self.jobs.close()
        super().close()
