"""Canvas charts (msr/charts.py): pure geometry plus drawing/tooltips on a hidden window."""
import tkinter as tk
import unittest
from unittest import mock

from msr import charts, dialogs


class GeometryTests(unittest.TestCase):
    def test_slice_angles_run_clockwise_from_the_top_and_fill_the_circle(self):
        angles = charts.slice_angles([50, 25, 25])
        self.assertEqual(angles[0], (90.0, -180.0))
        self.assertEqual(angles[1], (-90.0, -90.0))
        self.assertAlmostEqual(sum(e for _s, e in angles), -360.0)

    def test_a_single_slice_is_nearly_but_not_exactly_360(self):
        self.assertEqual(charts.slice_angles([7]), [(90.0, -359.99)])
        self.assertEqual(charts.slice_angles([]), [])
        self.assertEqual(charts.slice_angles([0, 0]), [])
        self.assertEqual(charts.slice_angles([0, 10])[0][1], -0.0)

    def test_bar_widths(self):
        self.assertEqual(charts.bar_widths([100, 50, 0, 1], 200), [200, 100, 0, 2])  # tiny values stay visible
        self.assertEqual(charts.bar_widths([0, 0], 200), [0, 0])
        self.assertEqual(charts.bar_widths([5], 0), [0])
        self.assertEqual(charts.bar_widths([], 100), [])

    def test_ellipsize(self):
        self.assertEqual(charts.ellipsize("short", 10), "short")
        self.assertEqual(charts.ellipsize("a" * 30, 10), "a" * 9 + "\u2026")
        self.assertEqual(charts.ellipsize("abc", 0), "")

    def test_month_axis_fills_gaps_and_crosses_years(self):
        months = {"2025-11": 1, "2026-02": 2}
        self.assertEqual(charts.month_axis(months), ["2025-11", "2025-12", "2026-01", "2026-02"])
        many = {f"2024-{m:02d}": 1 for m in range(1, 13)} | {f"2025-{m:02d}": 1 for m in range(1, 13)} | {"2026-01": 1}
        axis = charts.month_axis(many, limit=24)
        self.assertEqual((len(axis), axis[0], axis[-1]), (24, "2024-02", "2026-01"))
        self.assertEqual(charts.month_axis({}), [])
        self.assertEqual(charts.month_axis({"unknown": 1, "2026-01": 3}), ["2026-01"])

    def test_label_step(self):
        self.assertEqual(charts.label_step(12, 600), 1)
        self.assertEqual(charts.label_step(24, 400), 3)
        self.assertEqual(charts.label_step(0, 400), 1)

    def test_colours(self):
        self.assertEqual(charts.color_for("Kick"), charts.PLATFORM_COLORS["kick"])
        self.assertIn(charts.color_for("newsite", 1), charts.FALLBACK_COLORS)


class TkCase(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)


class DrawTests(TkCase):
    def test_bar_chart_tooltips_on_the_row_and_the_bar(self):
        chart = charts.BarChart(self.root, width=600)
        chart.pack()
        chart.set_data([("alice", 100, "9.0 GB", "alice: 9.0 GB (50%)", "#53b518"),
                        ("bob", 50, "4.5 GB", "bob: 4.5 GB (25%)", "#9146ff")])
        w, h = chart.size()
        self.assertEqual(w, 600)
        self.assertEqual(chart.tooltip_at(5, 6 + 12), "alice: 9.0 GB (50%)")
        self.assertEqual(chart.tooltip_at(5, 6 + 24 + 12), "bob: 4.5 GB (25%)")
        self.assertIsNone(chart.tooltip_at(5, 400))
        self.assertGreaterEqual(int(chart.cget("height")), 2 * 24)

    def test_donut_slices_legend_and_centre(self):
        chart = charts.DonutChart(self.root)
        chart.center_text = "12 GB"
        rows = [("Kick", 3, "Kick  3.0 GB (75%)", "Kick: 3.0 GB", "#53b518"),
                ("Twitch", 1, "Twitch  1.0 GB (25%)", "Twitch: 1.0 GB", "#9146ff")]
        chart.set_data(rows)
        texts = [chart.itemcget(i, "text") for i in chart.find_all() if chart.type(i) == "text"]
        self.assertIn("12 GB", texts)
        self.assertIn("Kick  3.0 GB (75%)", texts)
        self.assertEqual(len([i for i in chart.find_all() if chart.type(i) == "arc"]), 2)
        self.assertIn("Kick: 3.0 GB", chart._hits.values())

    def test_a_single_platform_still_draws_a_ring(self):
        chart = charts.DonutChart(self.root)
        chart.set_data([("Kick", 5, "Kick", "Kick: all", "#53b518")])
        self.assertEqual(len([i for i in chart.find_all() if chart.type(i) == "arc"]), 1)

    def test_column_chart_has_one_hover_area_per_month(self):
        chart = charts.ColumnChart(self.root, height=140, width=500)
        chart.set_data([("26-01", 0, "none"), ("26-02", 10, "Feb: 10"), ("26-03", 5, "Mar: 5")])
        self.assertIn("Feb: 10", chart._hits.values())
        x = 8 + (500 - 16) / 3 * 1 + 20
        self.assertEqual(chart.tooltip_at(int(x), 60), "Feb: 10")
        self.assertEqual(chart.tooltip_at(int(8 + 10), 60), "none")  # empty month is hoverable too

    def test_empty_data_shows_a_message(self):
        for cls in (charts.BarChart, charts.DonutChart, charts.ColumnChart):
            chart = cls(self.root)
            chart.set_data([])
            texts = [chart.itemcget(i, "text") for i in chart.find_all()]
            self.assertEqual(texts, [cls.empty_text])

    def test_tooltip_is_drawn_inside_the_canvas_and_removed(self):
        chart = charts.BarChart(self.root, width=300)
        chart.set_data([("a", 1, "1", "a tip that is quite long indeed " * 2, "#fff")])
        chart.show_tip(295, 30, "a tip that is quite long indeed " * 2)
        x0, y0, x1, y1 = chart.bbox("tip")
        self.assertGreaterEqual(x0, 0)
        self.assertLessEqual(x1, 300 + 2)
        chart.hide_tip()
        self.assertEqual(chart.find_withtag("tip"), ())

    def test_follows_the_dark_and_light_theme(self):
        chart = charts.BarChart(self.root, width=300)
        with mock.patch.object(dialogs, "is_dark", return_value=True):
            dark = charts.palette(chart)
        with mock.patch.object(dialogs, "is_dark", return_value=False):
            light = charts.palette(chart)
        self.assertNotEqual(dark["track"], light["track"])
        self.assertNotEqual(dark["tip_bg"], light["tip_bg"])

    def test_resize_redraws_once_and_destroy_cancels_it(self):
        chart = charts.ColumnChart(self.root, height=120, width=300)
        chart.set_data([("a", 1, "a")])
        ev = type("E", (), {"width": 777})()
        chart._on_configure(ev)
        chart._on_configure(ev)  # second call while one is pending: no second job
        self.assertIsNotNone(chart._job)
        chart.destroy()  # must not leave an after() script behind
        self.root.update()


if __name__ == "__main__":
    unittest.main()