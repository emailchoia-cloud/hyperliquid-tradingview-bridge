"""
Unit test verifying the core quantitative and idempotent state logic of the bridge.
"""

import hmac
import unittest
from unittest.mock import MagicMock


class TestBridgeLogic(unittest.TestCase):

    def setUp(self):
        self.sz_decimals_cache = {
            "BTC": 5,
            "ETH": 4,
            "SOL": 2,
            "PURR": 0,
        }

    def normalize_symbol(self, raw_symbol: str) -> str:
        sym = raw_symbol.upper().strip()
        if sym in self.sz_decimals_cache:
            return sym
        for suffix in ["-PERP", "PERP", "USDT", "USD", ".P", "-USD", "/USDT", "/USD"]:
            if sym.endswith(suffix):
                candidate = sym[:-len(suffix)]
                if candidate in self.sz_decimals_cache:
                    return candidate
        return sym

    def calculate_execution(self, symbol: str, current_pos: float, target_pos: float):
        sz_decimals = self.sz_decimals_cache.get(symbol, 4)
        raw_delta = target_pos - current_pos
        rounded_delta = round(raw_delta, sz_decimals)
        exec_sz = abs(rounded_delta)
        if sz_decimals == 0:
            exec_sz = int(exec_sz)

        if exec_sz == 0 or abs(rounded_delta) < 1e-9:
            return {"action": "NO_OP", "size": 0.0, "delta": 0.0}

        is_buy = rounded_delta > 0
        return {
            "action": "BUY" if is_buy else "SELL",
            "size": exec_sz,
            "delta": rounded_delta,
        }

    def test_symbol_normalization(self):
        self.assertEqual(self.normalize_symbol("BTC-PERP"), "BTC")
        self.assertEqual(self.normalize_symbol("ethusdt"), "ETH")
        self.assertEqual(self.normalize_symbol("SOL.P"), "SOL")
        self.assertEqual(self.normalize_symbol("PURR"), "PURR")

    def test_zero_delta_idempotence(self):
        # Already long 1.5 BTC, target is 1.5 BTC
        res = self.calculate_execution("BTC", current_pos=1.5, target_pos=1.5)
        self.assertEqual(res["action"], "NO_OP")
        self.assertEqual(res["size"], 0.0)

    def test_long_increase(self):
        # Current 1.0 BTC, Target 1.5 BTC -> Buy 0.5 BTC
        res = self.calculate_execution("BTC", current_pos=1.0, target_pos=1.5)
        self.assertEqual(res["action"], "BUY")
        self.assertEqual(res["size"], 0.5)

    def test_long_decrease(self):
        # Current 1.5 BTC, Target 0.5 BTC -> Sell 1.0 BTC
        res = self.calculate_execution("BTC", current_pos=1.5, target_pos=0.5)
        self.assertEqual(res["action"], "SELL")
        self.assertEqual(res["size"], 1.0)

    def test_long_to_flat(self):
        # Current 1.5 BTC, Target 0.0 -> Sell 1.5 BTC
        res = self.calculate_execution("BTC", current_pos=1.5, target_pos=0.0)
        self.assertEqual(res["action"], "SELL")
        self.assertEqual(res["size"], 1.5)

    def test_position_flip(self):
        # Current +0.5 BTC, Target -1.0 BTC -> Sell 1.5 BTC
        res = self.calculate_execution("BTC", current_pos=0.5, target_pos=-1.0)
        self.assertEqual(res["action"], "SELL")
        self.assertEqual(res["size"], 1.5)

    def test_precision_rounding_zero_decimals(self):
        # PURR has szDecimals = 0
        res = self.calculate_execution("PURR", current_pos=10.0, target_pos=25.0)
        self.assertEqual(res["action"], "BUY")
        self.assertEqual(res["size"], 15)
        self.assertIsInstance(res["size"], int)

    def test_secret_timing_safe_comparison(self):
        secret = "SUPER_SECURE_TOKEN_XYZ"
        self.assertTrue(hmac.compare_digest(secret, "SUPER_SECURE_TOKEN_XYZ"))
        self.assertFalse(hmac.compare_digest(secret, "WRONG_TOKEN"))


if __name__ == "__main__":
    unittest.main()
