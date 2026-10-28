# Third-party notices

Multi-Stream Recorder is released under the MIT licence (see `LICENSE`). This file lists the sources of the colour palettes behind the themes that are inspired by [Omarchy](https://omarchy.org/themes/) themes (Arc Blueberry, Catppuccin Mocha OLED, Rose Pine, Gruvbox Material, Dracula, Flexoki Dark, Vesper, Tokyo Night OLED, Tokyo Day, Solarized Light).

What was taken: **only colour values** (a dozen `#rrggbb` numbers per theme) as published in the Omarchy theme repository named below. No wallpapers, icons, configuration files or code were copied. The palettes live in `msr/theme_palettes.py`; `msr/themes.py` turns each into MSR's colour names and, where a published colour is too faint to read on its own background, lightens (dark themes) or darkens (light themes) it just enough to reach MSR's contrast rules (4.5:1 for text, 3:1 for charts). Rose Pine's green is the one colour MSR replaced on purpose. Both are recorded in the source.

Colour values on their own are generally not protected by copyright, but the authors are credited here regardless. If an author prefers a theme to be changed or removed, please open an issue.

## Themes

| Theme | Palette by (upstream) | Upstream licence | Omarchy theme repository (read) | Port licence | Files read |
|---|---|---|---|---|---|
| Arc Blueberry | [Bearded Theme Arc Blueberry by BeardedBear](https://github.com/BeardedBear/bearded-theme) | GitHub lists GPL-3.0 for the Bearded Theme repository; the licence of this one variant could not be confirmed | <https://github.com/vale-c/omarchy-arc-blueberry> | MIT (c) 2025 Vale | colors.toml |
| Catppuccin Mocha OLED | [Catppuccin Mocha by the Catppuccin organisation](https://github.com/catppuccin/catppuccin) | MIT (c) 2021 Catppuccin | <https://github.com/Luquatic/omarchy-catppuccin-dark> | none (README says MIT, no LICENSE file) | colors.toml |
| Rose Pine | [Rose Pine by the Rose Pine project](https://github.com/rose-pine/rose-pine-theme) | MIT (c) 2023 Rose Pine | <https://github.com/guilhermetk/omarchy-rose-pine-dark> | none (no LICENSE file) | colors.toml |
| Gruvbox Material | [Gruvbox Material by sainnhe](https://github.com/sainnhe/gruvbox-material) | MIT (c) 2020 sainnhe | <https://github.com/curbol/omarchy-gruvbox-material> | none (no LICENSE file) | colors.toml |
| Dracula | [Dracula Theme](https://github.com/dracula/dracula-theme) | MIT (c) 2023 Dracula Theme | <https://github.com/catlee/omarchy-dracula-theme> | none (no LICENSE file) | colors.toml |
| Flexoki Dark | [Flexoki by Steph Ango](https://github.com/kepano/flexoki) | MIT (c) 2023 Steph Ango | <https://github.com/euandeas/omarchy-flexoki-dark-theme> | MIT (c) 2025 Euan | colors.toml (ANSI bright colours) |
| Vesper | [Vesper by Rauno Freiberg](https://github.com/raunofreiberg/vesper) | MIT (c) 2023 Rauno Freiberg | <https://github.com/thmoee/omarchy-vesper-theme> | none (no LICENSE file) | alacritty.toml, hyprland.conf |
| Tokyo Night OLED | [Tokyo Night (colour scheme by enkia; see also folke/tokyonight.nvim)](https://github.com/folke/tokyonight.nvim) | folke/tokyonight.nvim is Apache-2.0; the original Tokyo Night licence was not checked | <https://github.com/Justin-De-Sio/omarchy-tokyoled-theme> | none (no LICENSE file) | colors.toml |
| Tokyo Day | [Tokyo Night (Omarchy's Tokyo Night theme, light companion)](https://github.com/folke/tokyonight.nvim) | folke/tokyonight.nvim is Apache-2.0; the original Tokyo Night licence was not checked | <https://github.com/lexeko/omarchy-tokyo-day-theme> | MIT (c) 2026 Alexey Konoplev, (c) David Heinemeier Hansson | colors.toml |
| Solarized Light | [Solarized by Ethan Schoonover](https://github.com/altercation/solarized) | MIT (c) 2011 Ethan Schoonover | <https://github.com/dfrico/omarchy-solarized-light-theme> | none (no LICENSE file) | alacritty.toml, btop.theme |

**Rose Pine:** Rose Pine has no green: its ANSI green is the teal 'pine' (#31748f), which made the Start button and the OK colour look blue. MSR uses pine turned to a green hue (#318f50, same saturation and lightness) instead, lightened until it reads on the background.

## Notes and caveats

- **Arc Blueberry**: the Omarchy theme is MIT (c) 2025 Vale and says its palette is based on Bearded Theme Arc Blueberry by BeardedBear. GitHub lists the Bearded Theme repository (<https://github.com/BeardedBear/bearded-theme>) as GPL-3.0; the licence of this one variant could not be confirmed. Only the palette's colour values are used, with credit to both.
- **Catppuccin Mocha OLED**: the Omarchy port uses an almost black background (`#010101`) instead of Mocha's usual base colour, hence the name. Its README says MIT but the repository has no LICENSE file; the upstream Catppuccin palette is MIT.
- **Tokyo Night OLED / Tokyo Day**: colours come from Omarchy's Tokyo Night family; [folke/tokyonight.nvim](https://github.com/folke/tokyonight.nvim) is Apache-2.0. The licence of the original Tokyo Night scheme (by enkia) was not checked.
- **Ports without a licence file** (Catppuccin Mocha OLED, Rose Pine, Gruvbox Material, Dracula, Vesper, Tokyo Night OLED, Solarized Light): the Omarchy theme repositories carry no licence, so only the colour values (which come from the MIT-licensed upstream palettes above) are used; nothing else from those repositories is included.
- Palette values are the Omarchy ports' values read on 2026-10-28; they were not re-compared with each upstream project.
- Not used: Artzen (no real status colours, no licence), Evergarden and Modus Operandi (GPL-3.0), Milky Matcha and Ristretto Light (too faint to keep their look).

## Licence notices

Copyright holders named by the licences above:

- Omarchy: Copyright (c) David Heinemeier Hansson (MIT), <https://github.com/omacom/omarchy>
- omarchy-arc-blueberry: Copyright (c) 2025 Vale (MIT)
- omarchy-flexoki-dark-theme: Copyright (c) 2025 Euan (MIT)
- omarchy-tokyo-day-theme: Copyright (c) 2026 Alexey Konoplev; Copyright (c) David Heinemeier Hansson (MIT)
- Catppuccin: Copyright (c) 2021 Catppuccin (MIT)
- Rose Pine: Copyright (c) 2023 Rosé Pine (MIT)
- Gruvbox Material: Copyright (c) 2020 sainnhe (MIT)
- Dracula Theme: Copyright (c) 2023 Dracula Theme (MIT)
- Flexoki: Copyright (c) 2023 Steph Ango (MIT)
- Solarized: Copyright (c) 2011 Ethan Schoonover (MIT)
- Vesper: Copyright (c) 2023 Rauno Freiberg (MIT)

The MIT licence for each of the above reads:

> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

The Apache-2.0 licence for folke/tokyonight.nvim is at <https://github.com/folke/tokyonight.nvim/blob/main/LICENSE>.

