# Multi-Stream Recorder v3.1.4

Fixes for long recordings, the Logs tab, the Library, and Linux (tooltips, the tray, and Clip Editor sound).

## Fixed

- **Clip Now cuts from the live edge of a long recording.** On every operating system, Clip Now could start about an hour behind live once a recording had been going for a long time. A still-growing `.ts` whose clock starts a few thousand seconds in, and which then runs longer than that, already reports its elapsed length. MSR treated that length as a raw clock value and subtracted the start a second time, so a two-minute clip (clock start about 2658 seconds, elapsed about 10186 seconds) landed near 4800 seconds. Elapsed time now comes from the first and last timestamps in the file. If the clock jumps backwards mid-recording, as it does after some ads, the cut still comes from the end of the file, including on disks that have no creation time. Screenshot and the live-preview fallback use the same elapsed time.

- **Space no longer opens another Clip Editor.** With the DVR button focused, each press of Space opened another editor, and the editor never took the keyboard. The status-row buttons (Clip Now, DVR, Clips, Screenshot and Streams) ignore Space. Opening an editor takes the keyboard, so Space plays and pauses that editor. Another click on DVR, or Shift+click on Clip Now, still opens another editor. Space in the channel list still ticks and unticks the selected channels.

- **The Logs tab shows worker lines, including remux progress.** Each channel's worker writes live checks, recording, splits and remux progress to `stream_recorder.log`. Those lines now also appear in the Logs tab as they are written. The level filter and the text filter apply to them the same way as to lines from the window. Each line is still written to the log file once, in the same format. Headless mode and `msr_ctl logs` are unchanged; they read that file. A worker that is stopped mid-line cannot stall shutdown.

- **The Library overview scrolls.** On a short window the "Recorded per month" chart used to be squashed to an empty sliver. It keeps its height, and the overview scrolls to it, the same way the Options tab scrolls. A tall window still stretches the overview to fill the tab. The mouse wheel over Channels, Recordings, PendingDeletion or Clean up still scrolls that list. The Library toolbar wraps onto a second line when the pane is too narrow for Remove folder.

- **Old lowercase recordings show up on Linux, and new files reuse those folders.** A drive that still has the lowercase `processed`, `recorded`, `pendingdeletion` or `clips` folders Windows wrote used to look almost empty: Linux created a new `Processed` folder beside them, and the Library only counted that. Every spelling is scanned and merged, including the platform and channel folders underneath. New recordings and remuxes go into the folder that is already there. Windows paths are unchanged.

- **Clip Editor audio plays on Linux.** Starting playback on a long live recording decoded and volume-scaled the file as fast as the CPU allowed, because Linux had no player and the sink accepted every sample at once. The picture stalled, then skipped ahead. Linux now plays through PipeWire (`pw-play` or `pw-cat`) or PulseAudio (`paplay`), and the picture follows that clock. With no player, or with no sound device on any system, playback stays in real time (silent when there is no player). The default volume is applied by ffmpeg; moving the slider still takes effect on the next chunk. Windows playback is still the Windows audio device.

- **Linux tooltips no longer flash, and the tray can dock on Hyprland.** A hover tip used to flash a large unthemed box. It is now one themed window, sized and then shown. The tray uses the AppIndicator backend when PyGObject and Ayatana AppIndicator (or AppIndicator3) can be imported. Hyprland has no XEmbed tray, so if that import fails the tray stays off: one info line in the log, no traceback, and closing or minimizing the window does not hide it into a tray that is not there. `PyGObject>=3.50` is a Linux-only line in `requirements.txt`. Windows and macOS skip it.

- **The mouse wheel scrolls on Linux.** The Options tab, the channel-list badge strip, the Clip Editor timeline and the Library overview follow the wheel. Linux reports a wheel notch as a mouse button rather than a wheel event; those buttons now scroll the same way the wheel does on Windows. The channel, recording and log lists already scrolled.

- **A Linux right-click leaves the menu open.** The menu used to open on the button press, so the release activated whatever was under the pointer (Enable on a channel, for example). Channel list, status list and Library menus now open on the release and stay up until an item is chosen, the pointer clicks elsewhere, or Esc. Windows still opens the menu on the press.

- **Hyprland leaves menus and tooltips at the size Tk posted.** Omarchy's floating workspaces resize a new menu, combobox list or tooltip to about 1100×760 and center it. A float rule for window classes `Menu`, `ComboboxPopdown` and `TkTooltip` keeps them out of that resize. MSR's tooltip uses the class `TkTooltip` so the rule matches it. Menus and combobox lists already use those class names. The rule is in [docs/linux.md](../linux.md), under **Hyprland / Omarchy (menus, dropdowns, tooltips)**.

- **Status previews refresh after the window comes back, and the pinned still is sharper.** Restoring from the taskbar or the tray refreshes the preview strip at once. The age badge updates immediately, and a grab that was already running is followed by one more. The pinned preview (double-click a tile) is a 960×540 still from the live edge. The strip tiles stay 160×90. There is still no full-window 1080p preview.

## Changed

- **The Library tab can add and remove extra folders.** **Add folder…** and **Remove folder** remember them in `[Library] extra_dirs`, the same setting as Options. Videos in those folders are matched to a site and channel from the `.meta.json` sidecar, then from a parent folder named like a known platform, otherwise Other / (unsorted). The scan only reads those folders.

## Upgrade notes

- Replace the program files as usual, or use the in-app update. Your settings and recordings are kept: `config.ini`, `channels.json`, cookies, Twitch sign-in and the recordings folder stay as they are.
- No new packages on Windows or macOS. Since v3.1.3, `requirements.txt` adds one line, and only on Linux: `PyGObject>=3.50` (`sys_platform == "linux"`), so the tray can dock on Hyprland and GNOME. pip builds it from source, which needs the gobject-introspection, cairo and Python headers first. The package names are in [docs/linux.md](../linux.md) under **Linux tray**. If those headers are missing, recording still runs and the tray stays off.
- Linux Clip Editor sound uses `pw-play`, `pw-cat` or `paplay`, which come with PipeWire or PulseAudio. They are not pip packages, and they are usually already installed. Without one, the picture still plays in real time, with no sound.
- YouTube recording needs [Deno](https://deno.com/) installed and on PATH. yt-dlp uses it to solve YouTube's JavaScript challenge. Without Deno, YouTube recordings drop out about every 15 seconds. On Windows, `winget install DenoLand.Deno`, then restart MSR.
- On Hyprland or Omarchy, add the float rule in [docs/linux.md](../linux.md) (**Hyprland / Omarchy (menus, dropdowns, tooltips)**) so right-click menus, dropdowns and tooltips stay the size Tk posted.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.1.4/CHANGELOG.md).
