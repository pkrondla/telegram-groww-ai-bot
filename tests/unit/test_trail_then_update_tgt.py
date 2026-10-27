"""TRAIL_SL then companion UPDATE_TGT must not revert the broker SL."""
from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock, patch

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1")
os.environ.setdefault("OPENAI_API_KEY", "x")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

_groww = types.ModuleType("growwapi")


class _FakeGrowwAPI:
    SMART_ORDER_TYPE_OCO = "OCO"
    SEGMENT_FNO = "FNO"

    @classmethod
    def get_access_token(cls, **_kwargs):
        return "ok-token"

    def __init__(self, token):
        self.token = token


_groww.GrowwAPI = _FakeGrowwAPI
sys.modules.setdefault("growwapi", _groww)

from src.trading.execution.trader import Trader  # noqa: E402

STALE_PAYLOAD = {
    "status": "ACTIVE",
    "target": {"trigger_price": 236.5, "order_type": "MARKET"},
    "stop_loss": {"trigger_price": 150.0, "price": 150.0, "order_type": "SL"},
}


def _trader() -> Trader:
    with patch.object(Trader, "_initialize_groww_client"):
        t = Trader(dry_run=True, account_id="test")
    t.client = MagicMock()
    t.client.SMART_ORDER_TYPE_OCO = "OCO"
    t.client.SEGMENT_FNO = "FNO"
    t.SMART_SEGMENT = "FNO"
    return t


class TestBrokerSlForTargetModify:
    def test_preferred_sl_beats_stale_payload(self):
        t = _trader()
        sl = t._broker_sl_for_target_modify(
            "BAJAJ-AUTO26NOV10500CE",
            STALE_PAYLOAD,
            preferred_sl=198.0,
        )
        assert sl == 198.0

    def test_local_book_beats_stale_payload(self):
        t = _trader()
        t._get_active_trade_record = MagicMock(
            return_value={"stop_loss": 198.0, "sl_true": 198.0}
        )
        sl = t._broker_sl_for_target_modify(
            "BAJAJ-AUTO26NOV10500CE",
            STALE_PAYLOAD,
        )
        assert sl == 198.0

    def test_payload_used_when_local_sl_is_zero(self):
        t = _trader()
        t._get_active_trade_record = MagicMock(
            return_value={"stop_loss": 0.0, "sl_true": 0.0}
        )
        sl = t._broker_sl_for_target_modify(
            "BAJAJ-AUTO26NOV10500CE",
            STALE_PAYLOAD,
        )
        assert sl == 150.0


class TestModifyOcoTargetPreservesTrail:
    def test_target_modify_sends_trailed_sl_not_stale_payload(self):
        t = _trader()
        t._get_oco_order_payload_cached = MagicMock(return_value=STALE_PAYLOAD)
        t._get_active_trade_record = MagicMock(
            return_value={"stop_loss": 198.0, "sl_true": 198.0}
        )
        captured = {}

        def _capture(_fn, **kwargs):
            captured.update(kwargs)

        t._groww_call = _capture
        t._modify_oco_target(
            "OCO-1",
            298.5,
            symbol="BAJAJ-AUTO26NOV10500CE",
            preserve_sl=True,
            preserve_sl_price=198.0,
        )
        sl = captured["stop_loss"]
        assert float(sl["trigger_price"]) == 198.0
        assert float(captured["target"]["trigger_price"]) == 298.5
        assert float(sl["trigger_price"]) != 150.0
