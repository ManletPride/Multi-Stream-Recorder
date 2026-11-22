# Linux

Tray libraries and the Hyprland float rule. The install steps are in the [README](../README.md#install-windows).

## Linux tray (Hyprland, GNOME, and other StatusNotifierItem bars)

`requirements.txt` installs `PyGObject>=3.50` only on Linux (`sys_platform == "linux"`). Windows and macOS skip that line. PyGObject has no wheels, so pip compiles it (and pycairo) and needs the system headers. The Ayatana typelib is what the tray icon actually loads. Install these, then launch MSR (or run `pip install -r requirements.txt` yourself):

```
# Arch / Omarchy
sudo pacman -S gobject-introspection cairo libayatana-appindicator

# Debian / Ubuntu
sudo apt install gcc pkg-config python3-dev libgirepository-2.0-dev libcairo2-dev gir1.2-ayatanaappindicator3-0.1
```

PyGObject 3.50 and newer needs the girepository **2.0** headers (glib 2.80 or newer), which is `libgirepository-2.0-dev` on Debian/Ubuntu, plus Python headers (`python3-dev`) so pycairo can compile. An older release only has `libgirepository1.0-dev`; that is not enough for this version. On Arch the `python` package already includes headers; `base-devel` supplies the compiler. If the headers are missing and `gi` cannot be imported, recording still runs and the log has one info line: the system tray is off. No traceback. Creating the venv with `python3 -m venv --system-site-packages .venv` is the other way to see a distro `gi` that already imports AyatanaAppIndicator3. If you set `PYSTRAY_BACKEND` yourself, MSR leaves that choice alone.

## Hyprland / Omarchy (menus, dropdowns, tooltips)

Omarchy's floating workspaces resize every new non-modal window to about 1100×760 and center it. That catches a right-click menu, a combobox list and a tooltip, so the menu opens as a huge dark box instead of at the pointer. A window whose class already has a user float rule is left out of that opener. Tk reports menus as class `Menu` and combobox lists as `ComboboxPopdown`. MSR's tooltip is class `TkTooltip` (a plain Toplevel is class `Toplevel`, which would also match dialogs). Add this at the end of `~/.config/hypr/hyprland.lua`, then run `hyprctl reload`:

```lua
o.window({ class = "^(Menu|ComboboxPopdown|TkTooltip)$" }, { float = true })
```

Plain Hyprland, in `hyprland.conf`, is the same match without Omarchy's helper:

```
windowrule = float on, match:class ^(Menu|ComboboxPopdown|TkTooltip)$
```
