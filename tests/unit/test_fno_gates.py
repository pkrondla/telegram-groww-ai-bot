"""Index/stock F&O entry gates: classification, StateEngine, commands, persist."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "-100123")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1447109810")
os.environ.setdefault("OPENAI_API_KEY", "x")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.core.config import Config
from src.core.env_file import persist_env_bool
from src.core.models import TradeLeg, TradeSignal
from src.integrations.telegram.listener import BotListener
from src.trading.execution.trader import Trader
from src.trading.reference.option_symbols import is_index_fno
from src.trading.state.state_engine import StateEngine


class _FakeTrader:
    def __init__(self):
        self.account_id = "test"
        self.active: dict = {}
        self.pending: list = []
        self.executed: list = []

    def _load_active_trades(self):
        return self.active

    def _load_pending(self):
        return self.pending

    def _save_pending(self, items):
        self.pending = list(items)

    def execute_trade(self, data):
        self.executed.append(data)
        return f"OK {data.get('action')} {data.get('trading_symbol') or data.get('stock')}"

    def resolve_active_symbol(self, symbol):
        return symbol if symbol in self.active else None

    def reconcile_trade_symbol(self, symbol):
        return symbol in self.active


def _buy(symbol: str, official: str = "") -> TradeSignal:
    return TradeSignal(
        action="BUY",
        official_symbol=official or symbol,
        trading_symbol=symbol,
        strike="200",
        option_type="CE",
        expiry_day="29",
        expiry_month="OCT",
        expiry_year="2026",
        entry_range=[10.0, 12.0],
        stop_loss=8.0,
        target=20.0,
    )


class TestIsIndexFno(unittest.TestCase):
    def test_index_roots(self):
        for root in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"):
            self.assertTrue(is_index_fno(root), root)

    def test_index_trading_symbols(self):
        self.assertTrue(is_index_fno("NIFTY26O0622800CE"))
        self.assertTrue(is_index_fno("BANKNIFTY26APR48000PE"))
        self.assertTrue(is_index_fno("SENSEX2640976500CE"))

    def test_stock_symbols_are_not_index(self):
        self.assertFalse(is_index_fno("RELIANCE"))
        self.assertFalse(is_index_fno("RELIANCE26APR200CE"))
        self.assertFalse(is_index_fno("WIPRO26APR200PE"))
        self.assertFalse(is_index_fno("360ONE26AUG1180CE"))

    def test_niftynxt50_is_not_nifty(self):
        self.assertFalse(is_index_fno("NIFTYNXT50"))
        self.assertFalse(is_index_fno("NIFTYNXT5026APR70000CE"))

    def test_empty_is_false(self):
        self.assertFalse(is_index_fno(""))
        self.assertFalse(is_index_fno(None))  # type: ignore[arg-type]


class TestFnoEntryBlockReason(unittest.TestCase):
    def tearDown(self):
        Config.INDEX_FNO_ENABLED = True
        Config.STOCK_FNO_ENABLED = True

    def test_defaults_allow_both(self):
        Config.INDEX_FNO_ENABLED = True
        Config.STOCK_FNO_ENABLED = True
        self.assertIsNone(Config.fno_entry_block_reason("NIFTY26O0622800CE"))
        self.assertIsNone(Config.fno_entry_block_reason("RELIANCE26APR200CE"))

    def test_index_off_blocks_only_index(self):
        Config.INDEX_FNO_ENABLED = False
        Config.STOCK_FNO_ENABLED = True
        self.assertEqual(
            Config.fno_entry_block_reason("NIFTY26O0622800CE"),
            "Index F&O disabled",
        )
        self.assertIsNone(Config.fno_entry_block_reason("RELIANCE26APR200CE"))

    def test_stock_off_blocks_only_stock(self):
        Config.INDEX_FNO_ENABLED = True
        Config.STOCK_FNO_ENABLED = False
        self.assertEqual(
            Config.fno_entry_block_reason("RELIANCE26APR200CE"),
            "Stock F&O disabled",
        )
        self.assertIsNone(Config.fno_entry_block_reason("BANKNIFTY26APR48000CE"))

    def test_both_off_mixed_spread(self):
        Config.INDEX_FNO_ENABLED = False
        Config.STOCK_FNO_ENABLED = False
        self.assertEqual(
            Config.fno_entry_block_reason(
                trade_data={
                    "legs": [
                        {"trading_symbol": "NIFTY26O0622800CE"},
                        {"trading_symbol": "RELIANCE26APR200CE"},
                    ]
                }
            ),
            "Index and stock F&O disabled",
        )


class TestStateEngineFnoGates(unittest.TestCase):
    def setUp(self):
        self.trader = _FakeTrader()
        self.engine = StateEngine(self.trader)

    def tearDown(self):
        Config.INDEX_FNO_ENABLED = True
        Config.STOCK_FNO_ENABLED = True

    def test_blocks_new_index_buy(self):
        Config.INDEX_FNO_ENABLED = False
        msg = self.engine.process(_buy("NIFTY26O0622800CE", "NIFTY"))
        self.assertIn("Index F&O disabled", msg)
        self.assertEqual(self.trader.executed, [])

    def test_allows_stock_buy_when_index_off(self):
        Config.INDEX_FNO_ENABLED = False
        msg = self.engine.process(_buy("RELIANCE26APR200CE", "RELIANCE"))
        self.assertTrue(str(msg).startswith("OK BUY"))
        self.assertEqual(len(self.trader.executed), 1)

    def test_management_still_runs_when_index_off(self):
        Config.INDEX_FNO_ENABLED = False
        self.trader.active["NIFTY26O0622800CE"] = {
            "fsm_state": "ACTIVE",
            "smart_order_id": "abc",
        }
        msg = self.engine.process(
            TradeSignal(
                action="EXIT_ALL",
                official_symbol="NIFTY",
                trading_symbol="NIFTY26O0622800CE",
                strike="22800",
                option_type="CE",
                expiry_day="06",
                expiry_month="OCT",
                expiry_year="2026",
            )
        )
        self.assertTrue(str(msg).startswith("OK EXIT_ALL"))
        self.assertEqual(self.trader.executed[0]["action"], "EXIT_ALL")

    def test_extend_range_new_entry_blocked(self):
        Config.STOCK_FNO_ENABLED = False
        msg = self.engine.process(
            TradeSignal(
                action="EXTEND_RANGE",
                official_symbol="RELIANCE",
                trading_symbol="RELIANCE26APR200CE",
                strike="200",
                option_type="CE",
                expiry_day="29",
                expiry_month="OCT",
                expiry_year="2026",
                entry_range=[10.0, 15.0],
            )
        )
        self.assertIn("Stock F&O disabled", msg)
        self.assertEqual(self.trader.executed, [])

    def test_spread_entry_blocked_if_any_leg_disabled(self):
        Config.INDEX_FNO_ENABLED = False
        msg = self.engine.process(
            TradeSignal(
                action="SPREAD",
                is_spread=True,
                legs=[
                    TradeLeg(
                        action="BUY",
                        official_symbol="NIFTY",
                        trading_symbol="NIFTY26O0622800CE",
                        strike="22800",
                        option_type="CE",
                        expiry_day="06",
                        expiry_month="OCT",
                        expiry_year="2026",
                    ),
                    TradeLeg(
                        action="SELL",
                        official_symbol="NIFTY",
                        trading_symbol="NIFTY26O0622900CE",
                        strike="22900",
                        option_type="CE",
                        expiry_day="06",
                        expiry_month="OCT",
                        expiry_year="2026",
                    ),
                ],
            )
        )
        self.assertIn("Index F&O disabled", msg)
        self.assertEqual(self.trader.executed, [])


class TestPendingFnoBlock(unittest.TestCase):
    def tearDown(self):
        Config.INDEX_FNO_ENABLED = True
        Config.STOCK_FNO_ENABLED = True

    def test_limit_pending_index_blocked(self):
        Config.INDEX_FNO_ENABLED = False
        reason = Trader._pending_fno_block_reason(
            {
                "kind": "limit_single",
                "trading_symbol": "NIFTY26O0622800CE",
                "trade_data": {"official_symbol": "NIFTY", "trading_symbol": "NIFTY26O0622800CE"},
            }
        )
        self.assertEqual(reason, "Index F&O disabled")

    def test_stock_pending_allowed_when_only_index_off(self):
        Config.INDEX_FNO_ENABLED = False
        reason = Trader._pending_fno_block_reason(
            {
                "kind": "single",
                "trading_symbol": "RELIANCE26APR200CE",
                "trade_data": {"official_symbol": "RELIANCE"},
            }
        )
        self.assertIsNone(reason)


class TestFnoCommands(unittest.TestCase):
    def test_parse_fno_commands(self):
        parse = BotListener._parse_override
        dummy = object()
        self.assertEqual(parse(dummy, "FNO"), "fno_status")
        self.assertEqual(parse(dummy, "FNO INDEX OFF"), "fno_index_off")
        self.assertEqual(parse(dummy, "TRADES STOCK ON"), "fno_stock_on")
        self.assertEqual(parse(dummy, "FNO ALL ON"), "fno_all_on")
        self.assertEqual(parse(dummy, "FNO NONE"), "fno_none")
        self.assertEqual(parse(dummy, "DISABLE INDEX"), "fno_index_off")
        self.assertEqual(parse(dummy, "ENABLE ALL"), "fno_all_on")
        self.assertEqual(parse(dummy, "TRADES"), "trades")

    def test_persist_env_bool_writes_existing_file(self):
        target = Path(__file__).with_name("_tmp_fno.env")
        try:
            target.write_text("INDEX_FNO_ENABLED=true\n", encoding="utf-8")
            written = persist_env_bool("INDEX_FNO_ENABLED", False, target)
            self.assertTrue(written)
            self.assertIn("INDEX_FNO_ENABLED=false", target.read_text(encoding="utf-8"))
            self.assertEqual(os.environ.get("INDEX_FNO_ENABLED"), "false")
        finally:
            if target.exists():
                target.unlink()
            os.environ.pop("INDEX_FNO_ENABLED", None)


if __name__ == "__main__":
    unittest.main()
