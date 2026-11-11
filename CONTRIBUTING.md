# Contributing

## Tests

```
python -m unittest discover -s tests -t .
```

GUI tests need a display and a Python built with Tk. Windows and macOS installers include Tk. On Linux install `python3-tk` (Debian/Ubuntu) and run under a virtual display:

```
xvfb-run -a python -m unittest discover -s tests -t .
```

Windows-only tests (DPAPI, drive letters, ACLs, title-bar theming) skip unless `sys.platform == 'win32'` or `os.name == 'nt'`. They still run on the Windows CI job. Do not delete those skips to make Linux green, and do not skip a cross-platform test to hide a failure.

## Continuous integration

`.github/workflows/tests.yml` runs the suite above on every push, every pull request, and when someone starts it by hand (Actions → tests → Run workflow).

| Runner | Python | Display |
| --- | --- | --- |
| `ubuntu-latest` | 3.12 and 3.14 | Xvfb, system Tk libraries so `actions/setup-python`'s Tkinter can open a window |
| `windows-latest` | 3.12 and 3.14 | The runner's own Tk |

`fail-fast` is off: a Linux failure does not cancel Windows, and the other way around. Pip downloads are cached from `requirements.txt`.

The four check names are:

- `ubuntu-latest / Python 3.12`
- `ubuntu-latest / Python 3.14`
- `windows-latest / Python 3.12`
- `windows-latest / Python 3.14`

## Branch protection

MSR is Windows-first. Linux support is growing, and a green Linux job must not hide a Windows regression. On `main`, require all four checks above before merging. That is a GitHub branch protection rule; nothing in this repository turns it on.
