import unittest
from datetime import date, timedelta
from unittest.mock import Mock, patch

import requests

from pipeline import METRICS, fetch, normalize, windows


class PipelineTest(unittest.TestCase):
    day = date(2026, 10, 1)

    def test_windows_are_inclusive_and_nonoverlapping(self):
        end = self.day + timedelta(days=60)
        spans = list(windows(self.day, end))
        self.assertEqual([(last-first).days + 1 for first, last in spans], [30, 30, 1])
        self.assertEqual(spans[-1], (end, end))

    def test_incomplete_day_does_not_replace_previous_data(self):
        hours = {f"Hour{h:02}": "123.4" for h in range(1, 25)}
        next_day = self.day + timedelta(days=1)
        payload = {"Items": [
            {"Date": str(self.day), "HourlyEntities": [{"Values": hours}]},
            {"Date": str(next_day), "HourlyEntities": [{"Values": {"Hour01": "5"}}]},
        ]}
        accepted, rejected = normalize("price", METRICS["price"], payload, self.day, next_day)
        self.assertEqual(len(accepted[self.day]), 24)
        self.assertEqual(set(rejected), {next_day})

    @patch("pipeline.sleep")
    @patch("pipeline.requests.post")
    def test_transient_timeout_retries(self, post, sleep):
        good = Mock(status_code=200)
        post.side_effect = [requests.Timeout(), good]
        self.assertIs(fetch(METRICS["price"], self.day, self.day), good)
        sleep.assert_called_once_with(2)


if __name__ == "__main__":
    unittest.main()
