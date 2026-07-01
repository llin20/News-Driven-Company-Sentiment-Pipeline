import unittest

from streaming.producer.gdelt_backfill import _parse_articles_payload, _parse_retry_after as backfill_parse_retry_after
from streaming.producer.news_producer import GdeltApiClient


class BackfillHelperTests(unittest.TestCase):
    def test_parse_articles_payload_accepts_missing_articles(self):
        payload = b'{"status":"ok"}'
        articles = _parse_articles_payload(payload)
        self.assertEqual(articles, [])

    def test_parse_articles_payload_rejects_non_list_articles(self):
        payload = b'{"articles": {"bad": true}}'
        with self.assertRaises(ValueError):
            _parse_articles_payload(payload)

    def test_parse_retry_after_numeric(self):
        self.assertEqual(backfill_parse_retry_after("10"), 10.0)
        self.assertIsNone(backfill_parse_retry_after("bad"))


class LiveClientHelperTests(unittest.TestCase):
    def test_parse_retry_after_numeric_and_http_date(self):
        client = GdeltApiClient(query="Apple")
        self.assertEqual(client._parse_retry_after("15"), 15.0)

        http_date = "Wed, 21 Oct 2037 07:28:00 GMT"
        parsed = client._parse_retry_after(http_date)
        self.assertIsNotNone(parsed)
        self.assertGreater(parsed, 0)

    def test_failure_cooldown_grows_with_consecutive_failures(self):
        client = GdeltApiClient(
            query="Apple",
            failure_cooldown_base_sec=10,
            failure_cooldown_cap_sec=60,
        )
        client.consecutive_fetch_failures = 1
        first = client._compute_failure_cooldown()
        client.consecutive_fetch_failures = 3
        third = client._compute_failure_cooldown()

        self.assertGreaterEqual(third, first)
        self.assertLessEqual(third, 60)


if __name__ == "__main__":
    unittest.main()
