"""Forward worker-process log records to the GUI without writing the file twice.

Each recording worker keeps the file and console handlers from
``setup_child_logging`` (one line in ``stream_recorder.log``, same format as
before). When the GUI is running it also attaches ``WorkerLogHandler``, which
puts a copy of the record on a ``multiprocessing.Manager`` queue. A daemon
thread in the main process (``LogBridge``) hands those records to the existing
GUI handler only. The Logs tab then batches, trims and filters them the same
way as main-process lines.

The queue lives in the manager process, like the status queue, so a worker
killed mid-put does not corrupt a pipe the listener is blocked on. Puts never
block: a full queue drops the oldest record. The file already has the line.
"""

from __future__ import annotations

import logging
import queue
import threading
import traceback

# How many queued records one wake of the drain thread will hand to the GUI
# handler. The handler only enqueues text; the Tk thread still inserts at most
# 50 lines per tick. This bound is so stop() is noticed during a burst.
_BURST = 200
_FINAL_DRAIN = 500


def _queue_is_full(exc: BaseException) -> bool:
    """True for queue.Full, including the copy a manager proxy re-raises."""
    if isinstance(exc, queue.Full):
        return True
    return type(exc).__name__ in ("Full", "RemoteError") and "Full" in str(exc)


def prepare_for_queue(record: logging.LogRecord) -> logging.LogRecord:
    """A picklable copy. Does not change ``record`` (the file handler still owns it)."""
    data = dict(record.__dict__)
    try:
        msg = record.getMessage()
    except Exception:
        msg = str(data.get("msg", ""))
    data["msg"] = msg
    data["args"] = None
    data["message"] = msg
    if data.get("exc_info"):
        if not data.get("exc_text"):
            try:
                data["exc_text"] = "".join(traceback.format_exception(*data["exc_info"]))
            except Exception:
                pass
        data["exc_info"] = None
    if not isinstance(data.get("stack_info"), (str, type(None))):
        data["stack_info"] = None
    return logging.makeLogRecord(data)


class WorkerLogHandler(logging.Handler):
    """Non-blocking send of one record to the main process. Never raises.

    A full queue drops the oldest pending record so a busy Logs tab cannot
    stall the recording worker. The worker's file handler has already written
    the line.
    """

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        from msr.util import RedactLogFilter
        self.addFilter(RedactLogFilter())

    def emit(self, record: logging.LogRecord) -> None:
        try:
            prepared = prepare_for_queue(record)
        except Exception:
            return
        self._put(prepared)

    def _put(self, record: logging.LogRecord) -> None:
        q = self.log_queue
        try:
            q.put_nowait(record)
            return
        except Exception as exc:
            if not _queue_is_full(exc):
                return  # manager gone: the file already has the line
        try:
            q.get_nowait()  # drop the oldest
        except Exception:
            pass
        try:
            q.put_nowait(record)
        except Exception:
            pass


class _Empty:
    """Sentinel: the pull got nothing. A log record is never this object."""


_EMPTY = _Empty()


class LogBridge:
    """Drain a worker log queue into one handler (the GUI's). Not a file writer.

    ``stop`` returns within ``timeout`` even if a worker was killed while
    putting, or the queue is empty. The thread is a daemon either way.
    """

    def __init__(self, log_queue, handler: logging.Handler):
        self.queue = log_queue
        self.handler = handler
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._errors = 0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._errors = 0
        self._thread = threading.Thread(
            target=self._run, name="worker-log-bridge", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> bool:
        """Ask the drain to finish and wait up to ``timeout`` seconds. Never blocks past that."""
        self._stop.set()
        thread = self._thread
        if (thread is not None and thread.is_alive()
                and thread is not threading.current_thread()):
            thread.join(timeout)
        return thread is None or not thread.is_alive()

    def _pull(self, timeout: float):
        try:
            if timeout:
                return self.queue.get(timeout=timeout)
            return self.queue.get_nowait()
        except queue.Empty:
            return _EMPTY
        except Exception:
            # A dead manager, or one bad record. Keep listening until stop();
            # a tight loop of failures backs off so shutdown can still join us.
            self._errors += 1
            if self._errors == 1:
                logging.warning(
                    "Worker log forwarding hit a bad record; the log file is unaffected")
            if self._errors >= 3:
                self._stop.set()
            return _EMPTY

    def _emit(self, record) -> None:
        if not isinstance(record, logging.LogRecord):
            return
        handler = self.handler
        try:
            if record.levelno < handler.level:
                return
            handler.handle(record)
        except Exception:
            pass

    def _drain(self, timeout: float, limit: int) -> None:
        for _ in range(limit):
            record = self._pull(timeout)
            if record is _EMPTY:
                return
            self._emit(record)
            timeout = 0  # only the first call waits

    def _run(self) -> None:
        while not self._stop.is_set():
            self._drain(0.2, _BURST)
        self._drain(0, _FINAL_DRAIN)
