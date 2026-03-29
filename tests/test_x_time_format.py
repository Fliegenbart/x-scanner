import unittest
from datetime import datetime, timezone

from social_scanner import format_x_datetime


class FormatXDatetimeTests(unittest.TestCase):
    def test_formats_utc_without_microseconds(self) -> None:
        dt = datetime(2026, 3, 29, 15, 27, 19, 896283, tzinfo=timezone.utc)

        self.assertEqual(format_x_datetime(dt), "2026-03-29T15:27:19Z")


if __name__ == "__main__":
    unittest.main()
