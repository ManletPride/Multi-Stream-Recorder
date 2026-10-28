"""Published palettes for the Omarchy-inspired themes: data only, no logic.

Each entry is the palette as published by the Omarchy theme port named in ``port`` (values copied
from its colors.toml, or from its terminal files when it has none), and ``themes.from_palette()`` turns
it into MSR's ~60 colour tokens (nudging any colour that is too faint to read; see that function).
Hex values may live here and in themes.py only (tests/test_theme_guard.py).

Keys per palette:
  name, dark, group        the Theme window groups the themes by ``group`` ("dark" / "light")
  bg fg selection accent   surface, text, the port's selection colour (buttons, title bar), accent
  red green yellow blue magenta cyan [orange] [lighter] [darker]   (orange and the two tints are optional)
  fix                      colours MSR replaced on purpose: {"green": "#rrggbb"} and why, in ``fix_note``
  credit                   who made the palette and under which licence; shown in the Theme window and in
                           THIRD_PARTY_NOTICES.md (a test keeps the three in step)

Credits: every ``credit`` names the original author and licence (read from each project's LICENSE file); the
notices file spells the full licence texts out.

Skipped on purpose (see docs): Artzen (no real status colours, no licence), Evergarden and Modus
Operandi (GPL-3.0), Milky Matcha and Ristretto Light (palettes too faint to keep their look).
"""
from __future__ import annotations

PALETTES: tuple[dict, ...] = (
    dict(
        name="Arc Blueberry", dark=True, group="dark",
        bg="#111422", fg="#bcc1dc", selection="#313b62", accent="#8eb0e6",
        red="#e35535", green="#3cec85", yellow="#eacd61", blue="#69c3ff", magenta="#f38cec", cyan="#22ecdb",
        orange="#e9954f", lighter="#1a1e33", darker="#0d101b",
        credit=dict(
            port="https://github.com/vale-c/omarchy-arc-blueberry", port_license="MIT, Copyright (c) 2025 Vale",
            upstream="Arc Blueberry from Bearded Theme by BeardedBear (src/variations/arc.ts)", upstream_url="https://github.com/BeardedBear/bearded-theme",
            upstream_license="MIT as of 2021 (Copyright (c) 2021 BeardedBear); GPL-3.0 since 2023-04-14. Only color values are used; no code or images",
            file="colors.toml"),
    ),
    dict(
        name="Catppuccin Mocha OLED", dark=True, group="dark",
        bg="#010101", fg="#cdd6f4", selection="#f5e0dc", accent="#89b4fa",
        red="#f38ba8", green="#a6e3a1", yellow="#f9e2af", blue="#89b4fa", magenta="#f5c2e7", cyan="#94e2d5",
        credit=dict(
            port="https://github.com/Luquatic/omarchy-catppuccin-dark", port_license="none (the README says MIT, but the repository has no LICENSE file)",
            upstream="Catppuccin Mocha by the Catppuccin organisation", upstream_url="https://github.com/catppuccin/catppuccin",
            upstream_license="MIT, Copyright (c) 2021 Catppuccin",
            file="colors.toml"),
    ),
    dict(
        name="Rose Pine", dark=True, group="dark",
        bg="#191724", fg="#e0def4", selection="#524f67", accent="#ebbcba",
        red="#eb6f92", green="#31748f", yellow="#f6c177", blue="#9ccfd8", magenta="#c4a7e7", cyan="#ebbcba",
        orange="#ebbcba", lighter="#26233a", darker="#0d0c12",
        fix=dict(green="#318f50"),
        fix_note="Rose Pine has no green: its ANSI green is the teal 'pine' (#31748f), which made the Start button and the OK "
                 "colour look blue. MSR uses pine turned to a green hue (#318f50, same saturation and lightness) instead, "
                 "lightened until it reads on the background.",
        credit=dict(
            port="https://github.com/guilhermetk/omarchy-rose-pine-dark", port_license="none (no LICENSE file)",
            upstream="Rose Pine by the Rose Pine project", upstream_url="https://github.com/rose-pine/rose-pine-theme",
            upstream_license="MIT, Copyright (c) 2023 Rosé Pine",
            file="colors.toml"),
    ),
    dict(
        name="Gruvbox Material", dark=True, group="dark",
        bg="#282828", fg="#d4be98", selection="#45403d", accent="#e78a4e",
        red="#ea6962", green="#a9b665", yellow="#d8a657", blue="#7daea3", magenta="#d3869b", cyan="#89b482",
        orange="#e78a4e", lighter="#32302f", darker="#141414",
        credit=dict(
            port="https://github.com/curbol/omarchy-gruvbox-material", port_license="none (no LICENSE file)",
            upstream="Gruvbox Material by sainnhe", upstream_url="https://github.com/sainnhe/gruvbox-material",
            upstream_license="MIT, Copyright (c) 2020 sainnhe",
            file="colors.toml"),
    ),
    dict(
        name="Dracula", dark=True, group="dark",
        bg="#282a36", fg="#f8f8f2", selection="#44475a", accent="#8be9fd",
        red="#ff5555", green="#50fa7b", yellow="#f1fa8c", blue="#bd93f9", magenta="#ff79c6", cyan="#8be9fd",
        orange="#ffb86c", lighter="#343746", darker="#191a24",
        credit=dict(
            port="https://github.com/catlee/omarchy-dracula-theme", port_license="none (no LICENSE file)",
            upstream="Dracula Theme", upstream_url="https://github.com/dracula/dracula-theme",
            upstream_license="MIT, Copyright (c) 2023 Dracula Theme",
            file="colors.toml"),
    ),
    dict(
        name="Flexoki Dark", dark=True, group="dark",
        bg="#100f0f", fg="#cecdc3", selection="#403e3c", accent="#4385be",
        red="#d14d41", green="#879a39", yellow="#d0a215", blue="#4385be", magenta="#ce5d97", cyan="#3aa99f",
        credit=dict(
            port="https://github.com/euandeas/omarchy-flexoki-dark-theme", port_license="MIT, Copyright (c) 2025 Euan",
            upstream="Flexoki by Steph Ango", upstream_url="https://github.com/kepano/flexoki",
            upstream_license="MIT, Copyright (c) 2023 Steph Ango",
            file="colors.toml (ANSI bright colours)"),
    ),
    dict(
        name="Vesper", dark=True, group="dark",
        bg="#101010", fg="#ffffff", selection="#988049", accent="#ffc799",
        red="#f5a191", green="#90b99f", yellow="#e6b99d", blue="#aca1cf", magenta="#e29eca", cyan="#ea83a5",
        orange="#ffc799",
        credit=dict(
            port="https://github.com/thmoee/omarchy-vesper-theme", port_license="none (no LICENSE file)",
            upstream="Vesper by Rauno Freiberg", upstream_url="https://github.com/raunofreiberg/vesper",
            upstream_license="MIT, Copyright (c) 2023 Rauno Freiberg",
            file="alacritty.toml, hyprland.conf"),
    ),
    dict(
        name="Tokyo Night OLED", dark=True, group="dark",
        bg="#000000", fg="#a9b1d6", selection="#7aa2f7", accent="#7aa2f7",
        red="#f7768e", green="#9ece6a", yellow="#e0af68", blue="#7aa2f7", magenta="#ad8ee6", cyan="#449dab",
        orange="#ff9e64",
        credit=dict(
            port="https://github.com/Justin-De-Sio/omarchy-tokyoled-theme", port_license="none (no LICENSE file)",
            upstream="Tokyo Night by Enkia", upstream_url="https://github.com/tokyo-night/tokyo-night-vscode-theme",
            upstream_license="MIT, Copyright (c) 2018-present Enkia",
            file="colors.toml"),
    ),
    dict(
        name="Tokyo Day", dark=False, group="light",
        bg="#e9ecf5", fg="#080808", selection="#c4d2f0", accent="#3760bf",
        red="#ad3459", green="#456a27", yellow="#80601c", blue="#3760bf", magenta="#7848ab", cyan="#006b7d",
        orange="#a34d28", lighter="#f7f8fc", darker="#d2d9e9",
        credit=dict(
            port="https://github.com/lexeko/omarchy-tokyo-day-theme", port_license="MIT, Copyright (c) 2026 Alexey Konoplev, Copyright (c) David Heinemeier Hansson",
            upstream="Tokyo Night by Enkia (light companion made by Alexey Konoplev)", upstream_url="https://github.com/tokyo-night/tokyo-night-vscode-theme",
            upstream_license="MIT, Copyright (c) 2018-present Enkia",
            file="colors.toml"),
    ),
    dict(
        name="Solarized Light", dark=False, group="light",
        bg="#fdf6e3", fg="#586e75", selection="#eee8d5", accent="#268bd2",
        red="#dc322f", green="#859900", yellow="#b58900", blue="#268bd2", magenta="#d33682", cyan="#2aa198",
        orange="#cb4b16", lighter="#eee8d5",
        credit=dict(
            port="https://github.com/dfrico/omarchy-solarized-light-theme", port_license="none (no LICENSE file)",
            upstream="Solarized by Ethan Schoonover", upstream_url="https://github.com/altercation/solarized",
            upstream_license="MIT, Copyright (c) 2011 Ethan Schoonover",
            file="alacritty.toml, btop.theme"),
    ),
)
