"""Queue, split plan and safety rules for uploading a finished recording.

No network and no Tk. ``msr/telegram_upload.py`` performs the upload; tests
drive these functions with a fake sender.

Size limits (checked against Telegram's own docs, not a guess):

* Hosted Bot API ``sendDocument`` multipart upload: **50 MB**
  (https://core.telegram.org/bots/api, "Sending files"). Useless for a
  recording, so MSR does not use it for the file itself.
* MTProto, non-Premium (a bot is not Premium): ``upload_max_fileparts_default``
  is 4000 and the largest part is 512 KiB, so **2,097,152,000 bytes**
  (https://core.telegram.org/api/config, https://core.telegram.org/api/files).
  Premium's 8000-part / 4 GB cap does not apply to the bot.

Parts stay under that cap. The local file is removed only when every part
of that file has a confirmed message and the delete option is on.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid

from msr import dpapi, history

# 50 * 1024 * 1024. Documented so a later change does not "optimise" us onto it.
BOT_API_MAX_BYTES = 50 * 1024 * 1024
# 4000 parts * 512 KiB. The largest file a non-Premium account, including a bot, can upload.
MTPROTO_MAX_BYTES = 4000 * 512 * 1024
# One mebibyte under the cap. A part Telegram accounts slightly larger still fits.
UPLOAD_LIMIT_BYTES = MTPROTO_MAX_BYTES - 1024 * 1024
PART_TARGET_RATIO = 0.90
MAX_PARTS = 80
CAPTION_MAX = 900
VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".m4v"}

INBOX_DIR = "telegram_inbox"
QUEUE_FILE = "telegram_queue.json"
SESSION_FILE = "telegram_session.json"
PARTS_DIR = "telegram_parts"
DONE_KEEP = 300

_BACKOFF = (15, 30, 60, 120, 300, 600, 900)
_MEMBER_OK = ("creator", "administrator", "member", "restricted")


class TelegramError(Exception):
    """A send failed in a way the queue can classify. ``kind`` is flood, auth, config, network or missing."""

    kind = "network"


class Flood(TelegramError):
    kind = "flood"

    def __init__(self, seconds, message=""):
        self.seconds = max(0, int(seconds))
        super().__init__(message or f"FloodWait {self.seconds}s")


class AuthRejected(TelegramError):
    kind = "auth"


class DestinationRejected(TelegramError):
    kind = "config"


class NotInstalled(TelegramError):
    kind = "missing"


# Matched on the exception class name only. The message text can quote a secret, so it is not used.
_API_ID_NAMES = ("ApiIdInvalid", "ConnectionApiIdInvalid")
_TOKEN_NAMES = (
    "AccessTokenInvalid", "AccessTokenExpired", "AuthKey", "SessionRevoked",
    "SessionExpired", "UserDeactivated", "TokenInvalid", "Unauthorized", "BotToken",
)
_TOKEN_MESSAGE = ("The bot token was rejected. Check it in Options. "
                  "Uploads are paused; recordings stay on disk.")
_API_ID_MESSAGE = "The Telegram api id or api hash was rejected. Check them in Options."
_CONFIG_NAMES = (
    "ChatWriteForbidden", "ChannelPrivate", "PeerIdInvalid", "ChatAdminRequired",
    "TopicDeleted", "MessageIdInvalid", "ChannelInvalid", "UserBannedInChannel",
    "ChatRestricted", "InputUserDeactivated",
)


def classify_failure(exc: BaseException) -> TelegramError:
    """Map a Telethon (or fake) exception onto one queue decision.

    The user-facing text for a sign-in failure is chosen from the exception
    class name. The original message is not copied: Telethon sometimes quotes
    the token or the api hash.
    """
    if isinstance(exc, TelegramError):
        return exc
    name = type(exc).__name__
    if "Flood" in name or "SlowMode" in name:
        seconds = getattr(exc, "seconds", None)
        return Flood(30 if seconds is None else seconds, str(exc))
    if any(bit in name for bit in _API_ID_NAMES):
        return AuthRejected(_API_ID_MESSAGE)
    if any(bit in name for bit in _TOKEN_NAMES):
        return AuthRejected(_TOKEN_MESSAGE)
    if any(bit in name for bit in _CONFIG_NAMES):
        return DestinationRejected(scrub(f"{name}: {exc}"))
    return TelegramError(scrub(f"{name}: {exc}"))


def backoff_seconds(attempts: int) -> int:
    """15s, then 30, 60, 120, 300, 600, and 900 after that. ``attempts`` is how many failures so far."""
    idx = min(max(0, int(attempts) - 1), len(_BACKOFF) - 1)
    return _BACKOFF[idx]


def scrub(text, *secrets) -> str:
    """Drop bot tokens, Bot API URLs and any secret we were handed, then cap the length."""
    from msr.util import redact_for_log
    out = redact_for_log(str(text or ""))
    for secret in secrets:
        if secret and len(str(secret)) >= 4:
            out = out.replace(str(secret), "***")
    return out[:500]


def _fp(*parts) -> str:
    raw = "\n".join(str(p) for p in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def upload_enabled(config) -> bool:
    try:
        return bool(config.getboolean("Telegram", "enabled", fallback=False))
    except Exception:
        return False


def delete_after_upload(config) -> bool:
    try:
        return bool(config.getboolean("Telegram", "delete_after_upload", fallback=False))
    except Exception:
        return False


class Settings:
    """What the uploader needs. ``__repr__`` never includes the hash or the token."""

    def __init__(self, enabled, api_id, api_hash, bot_token, chat_id, topic_id, owner_id, delete_local):
        self.enabled = bool(enabled)
        self.api_id = int(api_id or 0)
        self.api_hash = str(api_hash or "")
        self.bot_token = str(bot_token or "")
        self.chat_id = int(chat_id or 0)
        self.topic_id = int(topic_id or 0)
        self.owner_id = int(owner_id or 0)
        self.delete_local = bool(delete_local)

    def __repr__(self):
        return (f"Settings(enabled={self.enabled}, api_id={self.api_id}, chat_id={self.chat_id}, "
                f"topic_id={self.topic_id}, owner_id={self.owner_id}, delete_local={self.delete_local}, "
                f"hash={'set' if self.api_hash else 'empty'}, token={'set' if self.bot_token else 'empty'})")

    def credential_fp(self) -> str:
        return _fp(self.api_id, self.api_hash, self.bot_token)

    def dest_fp(self) -> str:
        return _fp(self.chat_id, self.topic_id, self.owner_id)

    def missing(self) -> str:
        """'' when a send is allowed, else the one thing the user still has to set."""
        if self.api_id <= 0:
            return "Set the api id from my.telegram.org."
        if not self.api_hash.strip():
            return "Set the api hash from my.telegram.org."
        if ":" not in self.bot_token:
            return "Set the bot token from BotFather."
        if not str(self.chat_id).startswith("-100"):
            return "Set the group id (a supergroup id starting with -100)."
        if self.topic_id <= 0:
            return "Set the topic id (the last number in the topic's t.me/c/… link)."
        if self.owner_id <= 0:
            return "Set your user id so MSR can confirm this group is yours."
        return ""

    def secrets(self):
        return (self.api_hash, self.bot_token)


def _int_value(config, key) -> int:
    raw = str(config.get("Telegram", key, fallback="") or "").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def read_settings(config) -> Settings:
    from msr.config import read_secret
    return Settings(
        upload_enabled(config),
        _int_value(config, "api_id"),
        read_secret(config, "Telegram", "api_hash"),
        read_secret(config, "Telegram", "bot_token"),
        _int_value(config, "chat_id"),
        _int_value(config, "topic_id"),
        _int_value(config, "owner_id"),
        delete_after_upload(config),
    )


class SettingError(Exception):
    """A Telegram setting value MSR will not store."""


def normalize_setting(key: str, value: str) -> str:
    """Checked text for one Telegram setting. Blank is allowed (the feature stays off)."""
    value = "" if value is None else str(value).strip()
    if "\n" in value or "\r" in value:
        raise SettingError("A setting cannot contain a line break.")
    if key in ("api_hash", "bot_token"):
        if key == "bot_token" and value and ":" not in value:
            raise SettingError("bot_token should look like 123456:ABC… from BotFather.")
        return value
    if key == "api_id":
        if not value:
            return ""
        try:
            number = int(value)
        except ValueError:
            raise SettingError("Telegram.api_id must be the number from my.telegram.org.") from None
        if number <= 0:
            raise SettingError("Telegram.api_id must be a positive number.")
        return str(number)
    if key == "chat_id":
        if not value:
            return ""
        try:
            number = int(value)
        except ValueError:
            raise SettingError("Telegram.chat_id must be the group's numeric id.") from None
        text = str(number)
        # "-100" plus the channel id. A bare "-100" or a user id is not a group.
        if not (text.startswith("-100") and text[4:].isdigit() and len(text) >= 10):
            raise SettingError(
                "Telegram.chat_id must be the supergroup id and start with -100. "
                "A user id would send recordings into a private chat, so it is refused.")
        return str(number)
    if key in ("topic_id", "owner_id"):
        if not value:
            return ""
        try:
            number = int(value)
        except ValueError:
            raise SettingError(f"Telegram.{key} must be a number.") from None
        if number <= 0:
            raise SettingError(f"Telegram.{key} must be a positive number.")
        return str(number)
    return value


def config_for_worker(config) -> dict:
    """The config dict handed to a recording process, without the bot token or api hash.

    Workers only need ``Telegram.enabled`` (and the live runtime flag). The token
    never crosses into the child process.
    """
    parser = config.config
    out = {section: dict(parser.items(section)) for section in parser.sections()}
    tg = out.get("Telegram")
    if tg:
        for key in list(tg):
            low = key.lower()
            if "hash" in low or "token" in low:
                tg.pop(key, None)
    return out


def plan_spans(size: int, duration: float | None, limit: int = UPLOAD_LIMIT_BYTES) -> list[dict]:
    """Time spans that cover ``duration``, each estimated under ``limit``.

    ``[{"whole": True}]`` when the file already fits (no ffmpeg). ``[]`` when
    the file is over the limit and cannot be split under it (unknown length,
    or one second of it is already too big, or it would take more than
    ``MAX_PARTS``).
    """
    if size <= 0 or limit <= 0:
        return []
    if size <= limit:
        return [{"whole": True}]
    if not duration or duration <= 0:
        return []
    rate = size / float(duration)
    if rate * 1.0 > limit:
        return []
    target = max(1, int(limit * PART_TARGET_RATIO))
    spans = []
    t = 0.0
    # A tiny tail (under a quarter second) is absorbed by the previous span.
    while t < duration - 0.25:
        if len(spans) >= MAX_PARTS:
            return []
        remain = duration - t
        if rate * remain <= limit:
            spans.append({"start": round(t, 3), "duration": round(remain, 3)})
            break
        span = max(1.0, target / rate)
        if t + span >= duration:
            span = remain
        spans.append({"start": round(t, 3), "duration": round(span, 3)})
        t = spans[-1]["start"] + spans[-1]["duration"]
    if spans:
        end = spans[-1]["start"] + spans[-1]["duration"]
        if end < duration:
            spans[-1]["duration"] = round(spans[-1]["duration"] + (duration - end), 3)
    return spans


def shrink_span(span: dict, actual: int, limit: int) -> dict | None:
    """A shorter copy of ``span`` after an extracted part came out over the limit. None if it cannot shrink."""
    if span.get("whole") or not span.get("duration"):
        return None
    ratio = (limit * PART_TARGET_RATIO) / max(int(actual), 1)
    dur = span["duration"] * ratio
    if dur >= span["duration"] - 0.5:
        dur = span["duration"] * 0.5
    if dur < 1.0:
        return None
    return {"start": span["start"], "duration": round(dur, 3)}


def part_filename(filename: str, n: int, m: int) -> str:
    """``name.mp4`` when there is one part, else ``name.part01of03.mp4``."""
    if m <= 1:
        return filename
    stem, ext = os.path.splitext(filename)
    width = max(2, len(str(m)))
    return f"{stem}.part{n:0{width}d}of{m:0{width}d}{ext}"


def build_caption(channel: str, platform: str, title: str, recorded_at: str, n: int, m: int) -> str:
    """Plain-text caption: channel, title, date, and ``part n/m`` when split. Capped at 900 characters."""
    when = str(recorded_at or "").replace("T", " ")[:16]
    lines = [f"{channel or 'channel'} ({platform or 'stream'})"]
    if title:
        lines.append(str(title).replace("\n", " ").strip())
    if when:
        lines.append(when)
    if m > 1:
        lines.append(f"part {n}/{m}")
    text = "\n".join(line for line in lines if line)
    if len(text) > CAPTION_MAX:
        text = text[: CAPTION_MAX - 1] + "…"
    return text


def landed_in_topic(peer_id, reply_msg, reply_top, chat_id, topic_id) -> bool:
    """True only when the message Telegram returned is in this group and this topic."""
    try:
        if int(peer_id) != int(chat_id):
            return False
        topic = int(topic_id)
    except (TypeError, ValueError):
        return False
    if topic <= 0:
        return False
    return topic in (reply_msg, reply_top)


def deletion_allowed(job: dict, size: int, mtime_ns: int, setting_on: bool) -> tuple[bool, str]:
    """The original file may be removed only after every part is confirmed and it has not changed."""
    if not setting_on:
        return False, "delete after upload is off"
    parts = job.get("parts") or []
    if not parts or any(not p.get("confirmed") for p in parts):
        return False, "not every part is confirmed"
    if job.get("state") == "error":
        return False, "the upload failed"
    if int(size) != int(job.get("size") or -1) or int(mtime_ns) != int(job.get("mtime_ns") or -2):
        return False, "the file changed"
    if not job.get("path"):
        return False, "no file"
    return True, ""


def guard_recording(path: str, roots) -> tuple[bool, str]:
    """A regular video under ``<root>/Processed/<platform>/<channel>/``, not a link, not another tree."""
    if not path or not os.path.isfile(path) or os.path.islink(path):
        return False, "not a regular file"
    ext = os.path.splitext(path)[1].lower()
    if ext not in VIDEO_EXTS:
        return False, "not a finished recording"
    try:
        size = os.path.getsize(path)
    except OSError:
        return False, "cannot read the file"
    if size < 1024:
        return False, "file is empty"
    real = os.path.realpath(path)
    for root in roots or []:
        if not root:
            continue
        base = os.path.realpath(root)
        try:
            rel = os.path.relpath(real, base)
        except ValueError:
            continue
        if rel.startswith(".."):
            continue
        parts = rel.split(os.sep)
        # Processed / platform / channel / file  (custom URLs add another folder)
        if len(parts) < 4 or parts[0].casefold() != "processed":
            continue
        blocked = {"pendingdeletion", "clips", "recorded"}
        if any(p.casefold() in blocked for p in parts):
            continue
        return True, ""
    return False, "outside the Processed folder"


def guard_part(path: str, job_id: str, parts_root: str, original: str) -> bool:
    """A part is either the original recording or a file we wrote under this job's temp dir."""
    if not path or os.path.islink(path):
        return False
    real = os.path.realpath(path)
    if original and real == os.path.realpath(original):
        return True
    if not job_id or not parts_root:
        return False
    home = os.path.realpath(os.path.join(parts_root, job_id))
    try:
        return os.path.commonpath([real, home]) == home and os.path.isfile(real)
    except ValueError:
        return False


def _stat(path):
    st = os.stat(path)
    return st.st_size, getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))


_UNSET = object()


def _flag_on(value) -> bool:
    """True only for a real opt-in. The string ``"false"`` is not true."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def topic_or_none(value):
    """A stored topic id, or None when it is blank or not a positive number.

    A bad value is dropped rather than refusing to load the channel list.
    """
    if value in (None, "", 0, "0", False):
        return None
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def normalize_topic_id(value):
    """What the user typed. Blank clears the override. Anything else must be a positive number."""
    if value is None or str(value).strip() in ("", "0"):
        return None
    number = topic_or_none(value)
    if number is None:
        raise SettingError(
            "The topic id must be a positive number, or blank to use the topic in Options.")
    return number


def telegram_of(item) -> tuple[bool, int | None]:
    """``(upload this channel, topic override or None)`` from one channels.json entry.

    Missing keys are off and "use the global topic". That is how an existing list loads.
    """
    if not isinstance(item, dict):
        return False, None
    return _flag_on(item.get("telegram_upload")), topic_or_none(item.get("telegram_topic"))


def apply_channel_telegram(record: dict, upload=_UNSET, topic=_UNSET) -> None:
    """Set the per-channel upload flag and topic override. An unset argument is left alone.

    Off and blank are stored by removing the key, so a list that never opted in stays
    in the shape it had.
    """
    if upload is not _UNSET:
        if _flag_on(upload):
            record["telegram_upload"] = True
        else:
            record.pop("telegram_upload", None)
    if topic is not _UNSET:
        number = normalize_topic_id(topic)
        if number is None:
            record.pop("telegram_topic", None)
        else:
            record["telegram_topic"] = number


def channels_path(data_dir: str, configured: str | None) -> str:
    path = configured or "channels.json"
    if not os.path.isabs(path):
        path = os.path.join(data_dir or ".", path)
    return path


def read_channel_telegram(path: str, key: str) -> tuple[bool, int | None]:
    """The choice saved for ``key``. A missing file or a missing channel is off."""
    try:
        with open(path, encoding="utf-8-sig") as fh:
            loaded = json.load(fh)
    except (OSError, ValueError):
        return False, None
    if not isinstance(loaded, list):
        return False, None
    for item in loaded:
        if isinstance(item, dict) and item.get("name") == key:
            return telegram_of(item)
    return False, None


def queue_if_channel(data_dir: str, channels_file: str | None, channel_key: str,
                     global_on: bool, info: dict) -> str | None:
    """Queue one finished recording when the global switch and this channel are both on.

    The note remembers a per-channel topic. A blank topic means the global one, decided
    when the upload starts. Credentials are not written.
    """
    if not global_on or not channel_key:
        return None
    upload, topic = read_channel_telegram(channels_path(data_dir, channels_file), channel_key)
    if not upload:
        return None
    payload = dict(info or {})
    if topic:
        payload["topic_id"] = topic
    return note_ready(data_dir, True, payload)


def note_ready(data_dir: str, enabled: bool, info: dict) -> str | None:
    """Drop one inbox note for a recording that just became final. No-op when uploads are off.

    The note carries the path and the caption fields. It does not carry credentials.
    """
    if not enabled:
        return None
    path = os.path.abspath(str((info or {}).get("path") or ""))
    if not path or not os.path.isfile(path) or os.path.islink(path):
        return None
    inbox = os.path.join(data_dir or ".", INBOX_DIR)
    os.makedirs(inbox, exist_ok=True)
    if os.name != "nt":
        try:
            os.chmod(inbox, 0o700)
        except OSError:
            pass
    created = info.get("created")
    if created is None:
        created = time.time_ns()
    note = {
        "path": path,
        "channel": str(info.get("channel") or "")[:200],
        "platform": str(info.get("platform") or "")[:40],
        "title": str(info.get("title") or "")[:300],
        "recorded_at": str(info.get("recorded_at") or "")[:40],
        "created": created,
    }
    topic = topic_or_none(info.get("topic_id"))
    if topic:
        note["topic_id"] = topic
    dest = os.path.join(inbox, uuid.uuid4().hex + ".json")
    _write_private(dest, json.dumps(note).encode("utf-8"))
    return dest


def iter_inbox(data_dir: str):
    inbox = os.path.join(data_dir or ".", INBOX_DIR)
    try:
        names = sorted(os.listdir(inbox))
    except OSError:
        return
    for name in names:
        if name.endswith(".json"):
            yield os.path.join(inbox, name)


def ordered_inbox(data_dir: str):
    """Inbox notes oldest first. A burst of finishes stays in the order they were queued."""
    rows = []
    for path in iter_inbox(data_dir):
        note = read_note(path)
        created = 0.0
        if isinstance(note, dict):
            try:
                created = float(note.get("created") or 0)
            except (TypeError, ValueError):
                created = 0.0
        rows.append((created, path, note))
    rows.sort(key=lambda row: (row[0], row[1]))
    return rows


def read_note(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def discard_note(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


class QueueStore:
    """``telegram_queue.json`` in the data folder. Paths live here; secrets do not."""

    def __init__(self, path: str):
        self.path = path
        self.jobs: list[dict] = []
        self.load()

    def load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as fh:
                doc = json.load(fh)
            jobs = doc.get("jobs") if isinstance(doc, dict) else None
            self.jobs = [j for j in jobs if isinstance(j, dict) and j.get("id")] if isinstance(jobs, list) else []
        except FileNotFoundError:
            self.jobs = []
        except (OSError, ValueError):
            logging.warning("Telegram: the upload queue could not be read; pending uploads start fresh. "
                            "The old file was left beside it.")
            try:
                os.replace(self.path, self.path + ".bad")
            except OSError:
                pass
            self.jobs = []
        for job in self.jobs:
            if job.get("state") == "uploading":
                job["state"] = "pending"

    def save(self) -> None:
        done = [j for j in self.jobs if j.get("state") == "done"]
        if len(done) > DONE_KEEP:
            drop = set(id(j) for j in done[:-DONE_KEEP])
            self.jobs = [j for j in self.jobs if id(j) not in drop]
        body = json.dumps({"version": 1, "jobs": self.jobs}, ensure_ascii=False).encode("utf-8")
        _write_private(self.path, body)

    def find(self, path: str, size: int, mtime_ns: int) -> dict | None:
        real = os.path.realpath(path)
        for job in self.jobs:
            if os.path.realpath(job.get("path") or "") == real and int(job.get("size") or -1) == int(size) \
                    and int(job.get("mtime_ns") or -2) == int(mtime_ns):
                return job
        return None

    def enqueue(self, path: str, channel: str, platform: str, title: str, recorded_at: str,
                topic_id=None) -> dict | None:
        try:
            size, mtime_ns = _stat(path)
        except OSError:
            return None
        existing = self.find(path, size, mtime_ns)
        if existing is not None:
            return None
        job = {
            "id": uuid.uuid4().hex,
            "path": os.path.abspath(path),
            "name": os.path.basename(path),
            "channel": channel,
            "platform": platform,
            "title": title,
            "recorded_at": recorded_at,
            "size": size,
            "mtime_ns": mtime_ns,
            "state": "pending",
            "attempts": 0,
            "next_try": 0,
            "error": "",
            "dest": None,
            "parts": [],
            "created": time.time(),
        }
        topic = topic_or_none(topic_id)
        if topic:
            job["topic_id"] = topic
        self.jobs.append(job)
        return job

    def counts(self) -> dict:
        pending = failed = done = 0
        for job in self.jobs:
            state = job.get("state")
            if state == "done":
                done += 1
            elif state == "error":
                failed += 1
            elif state in ("pending", "uploading"):
                pending += 1
        return {"pending": pending, "failed": failed, "done": done}

    def next_time(self, now: float):
        times = [float(j.get("next_try") or 0) for j in self.jobs
                 if j.get("state") in ("pending", "uploading")]
        if not times:
            return None
        return min(times) if min(times) > now else now


def _size_text(n: int) -> str:
    n = max(0, int(n))
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.0f} KiB"
    return f"{n} B"


def _eta_text(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds >= 3600:
        return f"{seconds // 3600}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"
    if seconds >= 60:
        return f"{seconds // 60}:{seconds % 60:02d}"
    return f"{seconds}s"


def upload_start_line(name: str, n: int, m: int, size: int, topic) -> str:
    """Logged once when a part actually starts uploading."""
    return f"uploading {name} part {int(n)}/{int(m)} ({_size_text(size)}) to topic {topic}"


def upload_progress_line(name: str, sent: int, total: int, elapsed: float) -> str:
    """Percent, megabytes, average speed and ETA for one progress log line."""
    sent = max(0, int(sent))
    total = max(0, int(total))
    percent = int(100 * sent / total) if total else 0
    speed = sent / elapsed if elapsed and elapsed > 0 else 0.0
    if speed > 0 and total > sent:
        eta = _eta_text((total - sent) / speed)
    elif total and sent >= total:
        eta = "0s"
    else:
        eta = "…"
    return (f"Uploading {name} {percent}% ({_size_text(sent)}/{_size_text(total)}, "
            f"{speed / (1024 * 1024):.1f} MB/s, ETA {eta})")


def progress_log_due(percent: int, now: float, last_percent: int, last_at: float, *,
                     step: int = 10, gap: float = 10.0) -> bool:
    """True on a 10-point step, or after 10 s if the percent moved.

    Telethon calls the progress callback many times a second. Logging on every
    call floods the log; waiting only for 10% hides a slow upload. A step that
    arrives inside the 10 s gap is still logged, and a crawl that has not
    gained 10 points is logged once the gap has passed.
    """
    percent = int(percent)
    if percent <= int(last_percent):
        return False
    if percent - int(last_percent) >= step:
        return True
    return (float(now) - float(last_at)) >= gap


def format_status(st: dict) -> str:
    """One status line: what is happening, and how many recordings are still in the queue.

    ``pending`` counts the upload in progress as well as the ones waiting. No paths, no secrets.
    """
    line = str((st or {}).get("line") or (st or {}).get("state") or "")
    try:
        pending = int((st or {}).get("pending") or 0)
    except (TypeError, ValueError):
        pending = 0
    current = str(((st or {}).get("current") or {}).get("name") or "")
    parts = [line]
    if current and current not in line:
        parts.append(current)
    parts.append(f"queue {pending}")
    return "Telegram: " + " · ".join(part for part in parts if part)


def _write_private(path: str, data: bytes) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    history.atomic_write(path, data)
    if os.name != "nt":
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


def session_path(data_dir: str) -> str:
    return os.path.join(data_dir or ".", SESSION_FILE)


def save_session(data_dir: str, session: str, fp: str) -> None:
    """The Telethon session string, encrypted, mode 0600.

    Windows uses DPAPI. Elsewhere the same user-key envelope as the bot token
    (``msr/filecrypt.py``). The file never holds the session in the clear, and
    it does not hold the bot token.
    """
    payload = json.dumps({"v": 1, "fp": fp, "session": session})
    if dpapi.available():
        body = json.dumps({"v": 1, "dpapi": dpapi.encrypt_text(payload)}).encode("utf-8")
    else:
        from msr import filecrypt
        body = json.dumps({"v": 1, "box": filecrypt.encrypt_text(payload)}).encode("utf-8")
    path = session_path(data_dir)
    _write_private(path, body)
    if os.name == "nt":
        try:
            from msr.winacl import restrict_to_current_user
            restrict_to_current_user(path)
        except Exception:
            logging.debug("Telegram: session file keeps the folder's permissions")


def load_session(data_dir: str, fp: str) -> str | None:
    """The saved session, or None when it is missing, unreadable, or was saved for a different bot."""
    path = session_path(data_dir)
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict):
        return None
    plain_file = False
    if "dpapi" in doc:
        if not dpapi.available():
            logging.info("Telegram: the saved bot session is encrypted for Windows and cannot be read here")
            return None
        try:
            doc = json.loads(dpapi.decrypt_text(doc["dpapi"]))
        except Exception:
            logging.warning("Telegram: the saved bot session cannot be decrypted; the bot will sign in again")
            return None
    elif "box" in doc:
        from msr import filecrypt
        try:
            doc = json.loads(filecrypt.decrypt_text(doc["box"]))
        except Exception:
            logging.warning("Telegram: the saved bot session cannot be decrypted; the bot will sign in again")
            return None
    else:
        plain_file = True
    if not isinstance(doc, dict) or doc.get("fp") != fp:
        return None
    text = doc.get("session")
    if not isinstance(text, str) or not text:
        return None
    if plain_file:
        try:
            save_session(data_dir, text, fp)
        except Exception:
            # Keep the file. Deleting it would drop a session that still works.
            logging.warning("Telegram: could not encrypt the session file; it was left unchanged")
    return text


def clear_session(data_dir: str) -> None:
    try:
        os.remove(session_path(data_dir))
    except OSError:
        pass
