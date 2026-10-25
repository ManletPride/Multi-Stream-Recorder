"""open_existing_folder (the Streams button's helper): opens like Clips, creates nothing."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from msr import util


class OpenExistingFolderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)

    def test_opens_an_existing_folder_with_the_given_opener(self):
        opener = mock.Mock()
        ok, shown = util.open_existing_folder(str(self.base), opener=opener)
        self.assertTrue(ok)
        opener.assert_called_once_with(str(self.base))

    def test_default_open_helper_is_the_one_the_clips_button_uses(self):
        with mock.patch.object(util, "open_local_path") as opener:
            ok, _ = util.open_existing_folder(str(self.base))
        self.assertTrue(ok)
        opener.assert_called_once_with(str(self.base))

    def test_blank_config_value_falls_back_to_the_default(self):
        opener = mock.Mock()
        for blank in ("", "   ", None):
            ok, _ = util.open_existing_folder(blank, default=str(self.base), opener=opener)
            self.assertTrue(ok)
        self.assertEqual(opener.call_count, 3)

    def test_nothing_set_anywhere_is_a_message_not_an_error(self):
        opener = mock.Mock()
        ok, why = util.open_existing_folder("", "", opener=opener)
        self.assertFalse(ok)
        self.assertIn("streams_dir", why)
        opener.assert_not_called()

    def test_missing_folder_is_refused_and_not_created(self):
        gone = self.base / "not-there"
        opener = mock.Mock()
        ok, why = util.open_existing_folder(str(gone), opener=opener)
        self.assertFalse(ok)
        self.assertIn("does not exist", why)
        opener.assert_not_called()
        self.assertFalse(gone.exists())
        self.assertEqual(os.listdir(self.base), [])

    def test_a_file_is_not_a_folder(self):
        f = self.base / "file.txt"
        f.write_text("x", encoding="utf-8")
        opener = mock.Mock()
        self.assertFalse(util.open_existing_folder(str(f), opener=opener)[0])
        opener.assert_not_called()

    def test_opener_failure_is_reported(self):
        ok, why = util.open_existing_folder(str(self.base), opener=mock.Mock(side_effect=OSError("no handler")))
        self.assertFalse(ok)
        self.assertIn("no handler", why)

    def test_unicode_and_spaces(self):
        folder = self.base / "str\u00e9ams dossier"
        folder.mkdir()
        opener = mock.Mock()
        self.assertTrue(util.open_existing_folder(str(folder), opener=opener)[0])
        opener.assert_called_once_with(str(folder))


if __name__ == "__main__":
    unittest.main()
