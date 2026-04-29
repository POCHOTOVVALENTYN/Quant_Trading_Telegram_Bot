import unittest

from services.signal_engine.engine import TradingOrchestrator


class TestSignalExpiryLogic(unittest.TestCase):
    def test_open_time_expiry_uses_lag_after_close(self):
        # 1h candle opened at 00:00, now is 01:55 => lag after close is 3300s.
        # This is correctly stale vs default 1h expiry window (~540s).
        candle_open_ts = 1_700_000_000.0
        now_ts = candle_open_ts + 6900.0
        lag_after_close, expiry_window, tf_secs, _ = TradingOrchestrator._compute_expiry_lag_after_close(
            candle_open_ts, "1h", now_ts=now_ts
        )
        self.assertEqual(tf_secs, 3600)
        self.assertAlmostEqual(lag_after_close, 3300.0, places=3)
        self.assertGreater(lag_after_close, expiry_window)

    def test_not_expired_shortly_after_close(self):
        candle_open_ts = 1_700_000_000.0
        now_ts = candle_open_ts + 3600.0 + 200.0  # 200s after close
        lag_after_close, expiry_window, tf_secs, _ = TradingOrchestrator._compute_expiry_lag_after_close(
            candle_open_ts, "1h", now_ts=now_ts
        )
        self.assertEqual(tf_secs, 3600)
        self.assertAlmostEqual(lag_after_close, 200.0, places=3)
        self.assertLess(lag_after_close, expiry_window)

    def test_millisecond_timestamp_is_handled(self):
        candle_open_ts_sec = 1_700_000_000.0
        candle_open_ts_ms = candle_open_ts_sec * 1000.0
        now_ts = candle_open_ts_sec + 3600.0 + 200.0
        lag_after_close, expiry_window, tf_secs, candle_ts = TradingOrchestrator._compute_expiry_lag_after_close(
            candle_open_ts_ms, "1h", now_ts=now_ts
        )
        self.assertEqual(tf_secs, 3600)
        self.assertAlmostEqual(candle_ts, candle_open_ts_sec, places=3)
        self.assertAlmostEqual(lag_after_close, 200.0, places=3)
        self.assertLess(lag_after_close, expiry_window)

    def test_recovering_stream_expands_expiry_window(self):
        candle_open_ts = 1_700_000_000.0
        now_ts = candle_open_ts + 3600.0 + 2000.0  # 2000s after close
        lag_after_close, expiry_window, tf_secs, _ = TradingOrchestrator._compute_expiry_lag_after_close(
            candle_open_ts, "1h", now_ts=now_ts, stream_recovering=True
        )
        self.assertEqual(tf_secs, 3600)
        self.assertAlmostEqual(lag_after_close, 2000.0, places=3)
        self.assertEqual(expiry_window, 2700.0)
        self.assertLess(lag_after_close, expiry_window)


if __name__ == "__main__":
    unittest.main()
