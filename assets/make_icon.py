"""MSR app icon: hand-written SVG geometry (256 grid), a 'small' variant for <=24 px."""
import io, sys, cairosvg
from PIL import Image

BG, BLUE, RED = "#0B1630", "#2E86FF", "#FF1E2D"

def svg(small=False, medium=False):
    if medium and not small:   # 32-48 px: a little bigger glyph and dot
        s, dot_r, lens_round, bg_rx = 1.12, 15, 9, 52
    elif small:   # 16-24 px: glyph fills more of the tile, bigger dot, rounder corners stay
        s, dot_r, lens_round, bg_rx = 1.28, 21, 10, 48
    else:
        s, dot_r, lens_round, bg_rx = 1.0, 11.5, 8, 54
    cx, cy = 130.5, 126          # glyph centre in the drawing below
    tf = f"translate(128 128) scale({s}) translate({-cx} {-cy})"
    dx, dy = (84, 107) if small else ((82, 105) if medium else (81, 104))
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="256" height="256">
  <title>Multi-Stream Recorder</title>
  <rect x="0" y="0" width="256" height="256" rx="{bg_rx}" fill="{BG}"/>
  <g transform="{tf}">
    <rect x="52" y="75" width="115" height="102" rx="15" fill="{BLUE}"/>
    <path d="M160 106 L205 84 L205 168 L160 146 Z" fill="{BLUE}" stroke="{BLUE}"
          stroke-width="{lens_round}" stroke-linejoin="round"/>
    <circle cx="{dx}" cy="{dy}" r="{dot_r}" fill="{RED}"/>
  </g>
</svg>
'''

def png(size):
    data = cairosvg.svg2png(bytestring=svg(size <= 24, size <= 48).encode(), output_width=size * 4, output_height=size * 4)
    im = Image.open(io.BytesIO(data)).convert("RGBA")
    return im.resize((size, size), Image.LANCZOS)

if __name__ == "__main__":
    out = sys.argv[1]
    open(f"{out}/icon.svg", "w").write(svg())
    open(f"{out}/icon_small.svg", "w").write(svg(True))
    open(f"{out}/icon_medium.svg", "w").write(svg(medium=True))
    for n in (16, 20, 24, 32, 40, 48, 64, 128, 256, 512):
        png(n).save(f"{out}/icon_{n}.png")
    ico = [16, 20, 24, 32, 40, 48, 64, 128, 256]
    imgs = [png(n) for n in ico]
    imgs[-1].save(f"{out}/icon.ico", sizes=[(n, n) for n in ico], append_images=imgs[:-1])
    # previews
    png(256).save(f"{out}/preview_256.png")
    strip = Image.new("RGBA", (16 + 24 + 32 + 48 + 50, 60), (255, 255, 255, 255))
    x = 10
    for n in (16, 24, 32, 48):
        strip.alpha_composite(png(n), (x, (60 - n) // 2)); x += n + 10
    strip.save(f"{out}/preview_strip_16_24_32_48.png")
    big = strip.resize((strip.width * 4, strip.height * 4), Image.NEAREST)
    big.save(f"{out}/preview_strip_x4.png")
