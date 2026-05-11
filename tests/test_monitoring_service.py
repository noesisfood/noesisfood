import logging
import unittest

from app.services.monitoring_service import get_event_counters, log_event, reset_event_counters


class MonitoringServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_event_counters()

    def test_log_event_returns_structured_payload_and_increments_counter(self) -> None:
        logger = logging.getLogger("noesisfood.test.monitoring")

        payload = log_event(
            logger,
            "scan_started",
            source="barcode",
            lang="en",
            fields={"nested": True},
        )

        self.assertEqual(payload["event"], "scan_started")
        self.assertEqual(payload["source"], "barcode")
        self.assertEqual(payload["fields"]["nested"], True)
        self.assertEqual(get_event_counters()["scan_started"], 1)


if __name__ == "__main__":
    unittest.main()
