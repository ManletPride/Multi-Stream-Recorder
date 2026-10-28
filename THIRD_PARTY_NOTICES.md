# Third-party notices

Multi-Stream Recorder is released under the MIT licence (see `LICENSE`). This file lists the sources of the colour palettes behind the themes that are inspired by [Omarchy](https://omarchy.org/themes/) themes (Arc Blueberry, Catppuccin Mocha OLED, Rose Pine, Gruvbox Material, Dracula, Flexoki Dark, Vesper, Tokyo Night OLED, Tokyo Day, Solarized Light), with the full licence text of every source that has one.

What was taken: **only colour values** (a dozen `#rrggbb` numbers per theme) as published in the Omarchy theme repository named below. No wallpapers, icons, configuration files or code were copied. The palettes live in `msr/theme_palettes.py`; `msr/themes.py` turns each into MSR's colour names and, where a published colour is too faint to read on its own background, lightens (dark themes) or darkens (light themes) it just enough to reach MSR's contrast rules (4.5:1 for text, 3:1 for charts). Rose Pine's green is the one colour MSR replaced on purpose. Both are recorded in the source.

Colour values on their own are generally not protected by copyright, but the authors are credited here regardless. If an author prefers a theme to be changed or removed, please open an issue.

## Omarchy

Omarchy theme system and palettes: MIT, Copyright (c) David Heinemeier Hansson (<https://github.com/omacom/omarchy>; the older address <https://github.com/basecamp/omarchy> redirects there). Every theme below reached MSR through an Omarchy theme, so this credit applies to all of them; the licence text is under "Licence texts".

## Themes

| Theme | Palette by (upstream) | Upstream licence | Omarchy theme repository (read) | Port licence | Files read |
|---|---|---|---|---|---|
| Arc Blueberry | [Arc Blueberry from Bearded Theme by BeardedBear (src/variations/arc.ts)](https://github.com/BeardedBear/bearded-theme) | MIT as of 2021 (Copyright (c) 2021 BeardedBear); GPL-3.0 since 2023-04-14. Only color values are used; no code or images | <https://github.com/vale-c/omarchy-arc-blueberry> | MIT, Copyright (c) 2025 Vale | colors.toml |
| Catppuccin Mocha OLED | [Catppuccin Mocha by the Catppuccin organisation](https://github.com/catppuccin/catppuccin) | MIT, Copyright (c) 2021 Catppuccin | <https://github.com/Luquatic/omarchy-catppuccin-dark> | none (the README says MIT, but the repository has no LICENSE file) | colors.toml |
| Rose Pine | [Rose Pine by the Rose Pine project](https://github.com/rose-pine/rose-pine-theme) | MIT, Copyright (c) 2023 Rosé Pine | <https://github.com/guilhermetk/omarchy-rose-pine-dark> | none (no LICENSE file) | colors.toml |
| Gruvbox Material | [Gruvbox Material by sainnhe](https://github.com/sainnhe/gruvbox-material) | MIT, Copyright (c) 2020 sainnhe | <https://github.com/curbol/omarchy-gruvbox-material> | none (no LICENSE file) | colors.toml |
| Dracula | [Dracula Theme](https://github.com/dracula/dracula-theme) | MIT, Copyright (c) 2023 Dracula Theme | <https://github.com/catlee/omarchy-dracula-theme> | none (no LICENSE file) | colors.toml |
| Flexoki Dark | [Flexoki by Steph Ango](https://github.com/kepano/flexoki) | MIT, Copyright (c) 2023 Steph Ango | <https://github.com/euandeas/omarchy-flexoki-dark-theme> | MIT, Copyright (c) 2025 Euan | colors.toml (ANSI bright colours) |
| Vesper | [Vesper by Rauno Freiberg](https://github.com/raunofreiberg/vesper) | MIT, Copyright (c) 2023 Rauno Freiberg | <https://github.com/thmoee/omarchy-vesper-theme> | none (no LICENSE file) | alacritty.toml, hyprland.conf |
| Tokyo Night OLED | [Tokyo Night by Enkia](https://github.com/tokyo-night/tokyo-night-vscode-theme) | MIT, Copyright (c) 2018-present Enkia | <https://github.com/Justin-De-Sio/omarchy-tokyoled-theme> | none (no LICENSE file) | colors.toml |
| Tokyo Day | [Tokyo Night by Enkia (light companion made by Alexey Konoplev)](https://github.com/tokyo-night/tokyo-night-vscode-theme) | MIT, Copyright (c) 2018-present Enkia | <https://github.com/lexeko/omarchy-tokyo-day-theme> | MIT, Copyright (c) 2026 Alexey Konoplev, Copyright (c) David Heinemeier Hansson | colors.toml |
| Solarized Light | [Solarized by Ethan Schoonover](https://github.com/altercation/solarized) | MIT, Copyright (c) 2011 Ethan Schoonover | <https://github.com/dfrico/omarchy-solarized-light-theme> | none (no LICENSE file) | alacritty.toml, btop.theme |

**Rose Pine:** Rose Pine has no green: its ANSI green is the teal 'pine' (#31748f), which made the Start button and the OK colour look blue. MSR uses pine turned to a green hue (#318f50, same saturation and lightness) instead, lightened until it reads on the background.

## Notes and caveats

- **Arc Blueberry** is Bearded Theme's Arc Blueberry variation by BeardedBear (<https://github.com/BeardedBear/bearded-theme>, `src/variations/arc.ts`), read through Vale's Omarchy theme (<https://github.com/vale-c/omarchy-arc-blueberry>, MIT, Copyright (c) 2025 Vale). The Bearded Theme repository was MIT as of 2021 (Copyright (c) 2021 BeardedBear) and has been GPL-3.0 since 2023-04-14 (checked in its LICENSE history on GitHub). Only colour values are used; no code or images. The background `#111422`, the accent `#8eb0e6` and the blue, green, yellow, red, turquoise and pink colours match `arcBlueBerry` / `arcColors` in `arc.ts`.
- **Tokyo Night: color palette adapted from <https://github.com/tokyo-night/tokyo-night-vscode-theme>** (MIT, Copyright (c) 2018-present Enkia). Tokyo Night OLED uses the Omarchy port by Justin-De-Sio, which has no licence file; eight of its nine main colours also appear in Enkia's `tokyo-night-color-theme.json`.
- **Tokyo Day** is a light companion made by Alexey Konoplev (<https://github.com/lexeko/omarchy-tokyo-day-theme>, MIT) that his README describes as based on Omarchy's Tokyo Night theme. Its palette is not copied from Enkia's "Tokyo Night Light" or folke's `tokyonight.nvim` "day": the only value they share is the accent `#3760bf`. It is credited to Enkia as the original Tokyo Night, and to Alexey Konoplev as its author.
- **Catppuccin Mocha OLED**: the Omarchy port uses an almost black background (`#010101`) instead of Mocha's usual base colour, hence the name. The port's README says MIT, but its repository has no LICENSE file, so that claim is not backed by a licence document; the upstream Catppuccin palette is MIT (Copyright (c) 2021 Catppuccin).
- **Ports without a licence file** (Catppuccin Mocha OLED, Rose Pine, Gruvbox Material, Dracula, Vesper, Tokyo Night OLED, Solarized Light): GitHub shows no licence for these Omarchy theme repositories, so only the colour values (which come from the MIT-licensed upstream palettes) are used; nothing else from those repositories is included.
- **How this was checked**: on 2026-10-28 each licence below was read from the project's LICENSE file on GitHub (Arc Blueberry's from the Bearded Theme repository's LICENSE history). Palette values are the Omarchy ports' values; they were compared with the upstream source only for Arc Blueberry (`arc.ts`) and Tokyo Night OLED (Enkia's theme file).
- Not used: Artzen (no real status colours, no licence), Evergarden and Modus Operandi (GPL-3.0), Milky Matcha and Ristretto Light (too faint to keep their look).

## Licence texts

Each text below is copied from the named project's LICENSE file.

### Tokyo Night: color palette adapted from https://github.com/tokyo-night/tokyo-night-vscode-theme

```text
The MIT License (MIT)

Copyright (c) 2018-present Enkia

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### Omarchy (https://github.com/omacom/omarchy)

```text
Copyright (c) David Heinemeier Hansson

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### omarchy-arc-blueberry by Vale (https://github.com/vale-c/omarchy-arc-blueberry)

```text
MIT License

Copyright (c) 2025 Vale

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Bearded Theme as of 2021 (https://github.com/BeardedBear/bearded-theme, LICENSE at commit c51fcbc; GPL-3.0 since 2023-04-14)

```text
MIT License

Copyright (c) 2021 BeardedBear

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### omarchy-tokyo-day-theme by Alexey Konoplev (https://github.com/lexeko/omarchy-tokyo-day-theme)

```text
MIT License

Copyright (c) 2026 Alexey Konoplev
Copyright (c) David Heinemeier Hansson

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### omarchy-flexoki-dark-theme by Euan (https://github.com/euandeas/omarchy-flexoki-dark-theme)

```text
MIT License

Copyright (c) 2025 Euan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Catppuccin (https://github.com/catppuccin/catppuccin)

```text
MIT License

Copyright (c) 2021 Catppuccin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Rose Pine (https://github.com/rose-pine/rose-pine-theme)

```text
MIT License

Copyright (c) 2023 Rosé Pine

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Gruvbox Material by sainnhe (https://github.com/sainnhe/gruvbox-material)

```text
MIT License

Copyright (c) 2020 sainnhe

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Dracula Theme (https://github.com/dracula/dracula-theme)

```text
The MIT License (MIT)

Copyright (c) 2023 Dracula Theme

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Flexoki by Steph Ango (https://github.com/kepano/flexoki)

```text
MIT License

Copyright (c) 2023 Steph Ango

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Vesper by Rauno Freiberg (https://github.com/raunofreiberg/vesper)

```text
MIT License

Copyright (c) 2023 Rauno Freiberg

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Solarized by Ethan Schoonover (https://github.com/altercation/solarized)

```text
Copyright (c) 2011 Ethan Schoonover

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

