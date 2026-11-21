Draft notes for the next release. Not tagged. `__version__` is still 3.1.5 until David cuts the release.

Chosen as **3.2.0** because the latest tag is v3.1.5 and this adds features (Telegram uploads, per-channel opt-in, Linux encryption of the Fishtank password and Twitch sign-in). 3.1.1 through 3.1.5 were small fix releases. It is not 4.0.0: recording without Telegram is unchanged, Windows still uses DPAPI, and an existing install migrates on the next start.

## Added

- **Finished recordings can go to one Telegram topic.** Off until you turn on **Options > Telegram > Upload finished recordings** and, for each channel, **Upload finished recordings from this channel**. An existing channel list stays off. Right-click the channel and choose **Telegram…**, or `msr_ctl.py edit <channel> --telegram-upload on`.

- **Setup.** Api id and api hash from [my.telegram.org](https://my.telegram.org). Bot token from [@BotFather](https://t.me/BotFather). Group id from a topic link `https://t.me/c/1234567890/55` (`-100` plus that first number). Topic id is the last number. Your numeric user id must be a member of the group. Add the bot as an admin. **Send test message** (or `msr_ctl.py telegram test`) checks the bot, the group and that user id. MSR logs in as the bot only.

- **A channel can use its own topic.** Set a topic id on the channel to override the one in Options. Blank uses the global topic. Same group and same bot.

- **Files use MTProto, not the 50 MB Bot API cap.** Each part stays under 2,097,152,000 bytes (4000 × 512 KiB, about 2000 MB). A larger file is split with ffmpeg stream-copy, no re-encode, as `name.part01of03.mp4`, with a caption (channel, title, date, part n/m).

- **One upload at a time.** Oldest finished recording first. A FloodWait holds the ones behind it. The queue is saved and continues after a restart. The send starts within a few seconds of the remux. There is no extra wait for the file to sit still.

- **Progress.** The log says `Telegram: uploading name part n/m (size) to topic T` when a part starts, then percent, megabytes sent and total, speed and ETA about every 10% or 10 seconds. The Status tab and `msr_ctl.py telegram status` show `Uploading name 45%` and how many are still queued.

- **Commands.** `msr_ctl.py telegram status`, `msr_ctl.py telegram test`, and `msr_ctl.py edit <channel> --telegram-upload on|off --telegram-topic 55` (blank topic clears the override).

## Changed

- **Linux encrypts the Fishtank password and the Twitch sign-in.** They use the same user key as the Telegram secrets: `~/.config/multi-stream-recorder/secret.key` (mode 0600, directory 0700, or `$XDG_CONFIG_HOME`). A plain value is encrypted on the next start. The ciphertext is `password_box` in `config.ini` and a `box` field in `twitch_token.json`. If the key is missing or damaged, MSR does not delete the saved value and does not crash: a ciphertext it cannot open asks you to enter it again, and a plain value it cannot encrypt is left as it is. Windows still uses DPAPI only.

- **Left unencrypted on purpose.** `cookies.txt` (yt-dlp and streamlink read it), the Fishtank email, the Twitch public client id, `MSR_FISHTANK_PASSWORD` (environment only), and the short-lived `msr_control.json` token.

## Fixed

- **A valid bot token was reported as rejected.** Telethon's sign-in calls are coroutines. MSR now runs them on the upload thread's own loop. A bad api id or api hash, and a login that is not a bot, are named as those problems. The log line is the exception class name.

- **The Windows key file.** If a user key is written on Windows, it is written in binary mode, so a newline or Ctrl+Z in the key is not rewritten. It is not given mode 0600, because Python 3.14 turns that mode into an access list a later open cannot read. Windows secrets themselves stay DPAPI.

- **The per-channel Telegram window** opens at the size of its contents. On Hyprland a dialog that only set its position was a large empty window.

## Upgrade notes

- Install Telethon if this copy does not have it (`pip install "Telethon>=1.36,<2"`, or let `run_msr.pyw` install `requirements.txt`) and restart. Without it the Telegram section is greyed out and recording is unchanged.
- Existing channels do not start uploading until you opt each one in.
- Keep `secret.key`. A copied `config.ini` or `twitch_token.json` does not decrypt without it. An older MSR sees a blank Fishtank password line.
- **Delete the local file after every part is confirmed** stays off unless you turn it on.
- Your recordings are kept.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/main/CHANGELOG.md) (the Unreleased section, until this is tagged).
