"""
Unit tests for Assistant._extract_signal_targets,
Assistant._split_context_and_tail, and related target / hit logic.
"""
import sys
import os
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

os.environ.setdefault("TELEGRAM_API_ID",       "1")
os.environ.setdefault("TELEGRAM_API_HASH",     "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID","1")
os.environ.setdefault("TELEGRAM_ADMIN_ID",     "1")
os.environ.setdefault("OPENAI_API_KEY",        "x")

from src.integrations.ai.assistant import Assistant  # noqa: E402

LAURUS_FORCED_LLM = {
    "official_symbol": "LAURUSLABS",
    "stock": "LAURUSLABS",
    "strike": "2100",
    "option_type": "CE",
    "expiry_day": "01",
    "expiry_month": "NOV",
    "expiry_year": "2026",
    "entry_range": [0, 0],
    "stop_loss": 0,
    "target": 0,
    "is_trade": True,
    "action": "TRAIL_SL",
}

LAURUS_TARGET_HIT_TSL_AT_COST = (
    "TARGET 1 : 40.25 HITS ⭐️⭐️⭐️\n\n"
    "GAINS : Rs. 4035/ LOT💰💰\n\n"
    "TSL AT COST 35\n"
    "HOLD FOR 46.50, 56\n\n"
    "#LAURUSLABS  2100 CE\n"
    "35.50 TO 40.25"
)


class TestExtractSignalTargets:
    def test_tgt_slash_format(self):
        result = Assistant._extract_signal_targets("TGT: 68/88/105")
        assert result == [68.0, 88.0, 105.0]

    def test_targets_comma_format(self):
        result = Assistant._extract_signal_targets("TARGETS: 68, 88")
        assert result == [68.0, 88.0]

    def test_targets_with_plus_suffix(self):
        result = Assistant._extract_signal_targets("TGT : 18/24/36+")
        assert result == [18.0, 24.0, 36.0]

    def test_ignores_rr_multipliers(self):
        # "2X" / "3X" should not be captured as target prices
        result = Assistant._extract_signal_targets("TARGETS : 26, 48 [2X,3X]")
        assert result == [26.0, 48.0]

    def test_empty_text_returns_empty(self):
        assert Assistant._extract_signal_targets("") == []

    def test_no_target_keyword_returns_empty(self):
        assert Assistant._extract_signal_targets("BUY NIFTY 24500 CE SL 68") == []

    def test_deduplication(self):
        result = Assistant._extract_signal_targets("TGT: 68/68/88")
        assert result == [68.0, 88.0]

    def test_multiline_picks_first_match(self):
        text = "some preamble\nTARGETS: 50, 75\nother line"
        result = Assistant._extract_signal_targets(text)
        assert result == [50.0, 75.0]


class TestSplitContextAndTail:
    def test_plain_message_returns_empty_context(self):
        ctx, tail = Assistant._split_context_and_tail("BUY NIFTY CE")
        assert ctx == ""
        assert tail == "BUY NIFTY CE"

    def test_reply_splits_correctly(self):
        text = "[CONTEXT: parent message] child message"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == "parent message"
        assert tail == "child message"

    def test_no_closing_bracket_falls_back_to_full(self):
        text = "[CONTEXT: unclosed"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == ""
        assert tail == text

    def test_empty_context_inner(self):
        text = "[CONTEXT: ] child only"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == ""
        assert tail == "child only"


class TestExtractSignalTargetsHitLines:
    """Regression tests: hit-announcement lines must not corrupt target ladders."""

    def test_hit_line_with_price_ignored(self):
        # "TARGET 2 : 374 HITS" — the 2 and 374 must not become ladder prices.
        result = Assistant._extract_signal_targets("TARGET 2 : 374 HITS")
        assert result == []

    def test_hit_line_target1_ignored(self):
        result = Assistant._extract_signal_targets("TARGET 1 👍 : 255 HITS")
        assert result == []

    def test_multiline_hit_lines_produce_no_ladder(self):
        # Mirrors the exact channel messages that caused the bug.
        child = "TARGET 2 : 374 HITS ⭐️⭐️⭐️⭐️\n\n#SENSEX 77900 PE\n200 TO 374 🔥🔥🔥🔥 | +174 PTS"
        parent = (
            "TARGET 1 👍 : 255 HITS ✅ ✨\n\n"
            "TSL TO 202.5 COST\n"
            "HOLD FOR 402\n\n"
            "#SENSEX 77900 PE 13TH AUG\n"
            "202.5 TO 255 | +52.5 PTS [+25.9%] 💰"
        )
        combined = "\n".join([child, parent])
        result = Assistant._extract_signal_targets(combined)
        # Neither "1", "2", "255", nor "374" should appear — all are on HIT lines.
        assert result == []

    def test_real_ladder_line_still_parsed(self):
        # A genuine ladder directive must still work after the hit-line fix.
        result = Assistant._extract_signal_targets("TGT : 255/374/402")
        assert result == [255.0, 374.0, 402.0]

    def test_hold_for_line_not_captured(self):
        # "HOLD FOR 402" is not a TGT/TARGET line — should return nothing.
        result = Assistant._extract_signal_targets("HOLD FOR 402")
        assert result == []

    def test_hits_keyword_anywhere_on_line_suppresses(self):
        # HIT/HITS anywhere after TGT/TARGET on the same line → skip.
        result = Assistant._extract_signal_targets("TARGETS 6/11/16 HITS")
        assert result == []


class TestUpdateTargetDetection:
    def test_detect_update_tgt_accepts_plain_target_price(self):
        assert Assistant._detect_update_tgt("TARGET 52") == 52.0

    def test_detect_update_tgt_ignores_target_hit_progression(self):
        msg = "TARGET 1 HITS | TSL TO 21.5 | HOLD FOR 52"
        assert Assistant._detect_update_tgt(msg) is None


class TestHasTargetHitPhrase:
    """RECONCILE gate: hit phrases must be detected even with prices embedded."""

    def test_target_2_hits_with_price(self):
        assert Assistant._has_target_hit_phrase("TARGET 2 : 374 HITS ⭐️") is True

    def test_target_1_hits_with_price(self):
        assert Assistant._has_target_hit_phrase("TARGET 1 👍 : 255 HITS ✅") is True

    def test_t1_hit_shorthand(self):
        assert Assistant._has_target_hit_phrase("T1 HIT") is True

    def test_plain_target_price_not_hit(self):
        # "TARGET 52" without HIT/HITS is not a hit announcement.
        assert Assistant._has_target_hit_phrase("TARGET 52") is False

    def test_hold_for_not_hit(self):
        assert Assistant._has_target_hit_phrase("HOLD FOR 402") is False

    def test_laurus_channel_update_is_hit(self):
        assert Assistant._has_target_hit_phrase(LAURUS_TARGET_HIT_TSL_AT_COST) is True


class TestTrailSlAtCost:
    """Channel phrase 'TSL AT COST 35' must trail, not reconcile."""

    def test_extracts_price_after_cost(self):
        assert Assistant._extract_trail_sl_price("TSL AT COST 35") == 35.0
        assert Assistant._extract_trail_sl_price("TSL TO COST 21.5") == 21.5
        assert Assistant._extract_trail_sl_price("TSL COST 40") == 40.0
        assert Assistant._extract_trail_sl_price("SL AT COST 35") == 35.0

    def test_existing_tsl_to_cost_suffix_still_works(self):
        # COST after the price is a label, not a blocker.
        assert Assistant._extract_trail_sl_price("TSL TO 202.5 COST") == 202.5
        assert Assistant._extract_trail_sl_price("TSL TO 55") == 55.0
        assert Assistant._extract_trail_sl_price("TSL 40") == 40.0

    def test_forced_intent_is_trail_with_price(self):
        a = Assistant()
        assert a._detect_forced_intent("TSL AT COST 35", "TSL AT COST 35") == (
            "TRAIL_SL",
            35.0,
        )
        assert a._detect_forced_intent(
            LAURUS_TARGET_HIT_TSL_AT_COST, LAURUS_TARGET_HIT_TSL_AT_COST
        ) == ("TRAIL_SL", 35.0)

    def test_tail_has_explicit_trail(self):
        assert Assistant._tail_has_explicit_exit_or_trail("TSL AT COST 35") is True
        assert Assistant._tail_has_explicit_exit_or_trail("TARGET 1 : 40.25 HITS") is False

    def test_tsl_hit_still_exit_not_trail(self):
        a = Assistant()
        assert a._detect_forced_intent("TSL HIT", "TSL HIT") == ("EXIT_ALL", None)

    def test_hold_for_companion_target(self):
        assert Assistant._extract_hold_target_price(LAURUS_TARGET_HIT_TSL_AT_COST) == 46.50

    def test_parse_target_hit_with_tsl_at_cost_is_trail(self):
        a = Assistant()
        a._llm_parse_forced = MagicMock(return_value=dict(LAURUS_FORCED_LLM))
        raw = a.parse_signal(
            {
                "is_reply": True,
                "parent_text": "#LAURUSLABS BUY 2100 CE NOV\n35.50 TO 38",
                "child_text": LAURUS_TARGET_HIT_TSL_AT_COST,
                "full_text": (
                    "[CONTEXT: #LAURUSLABS BUY 2100 CE NOV\n35.50 TO 38] "
                    + LAURUS_TARGET_HIT_TSL_AT_COST
                ),
            }
        )
        assert raw is not None
        assert raw["action"] == "TRAIL_SL"
        assert float(raw["stop_loss"]) == 35.0
        assert float(raw["target"]) == 46.50

    def test_pure_target_hit_reply_still_reconciles(self):
        a = Assistant()
        a._llm_parse_forced = MagicMock(return_value=dict(LAURUS_FORCED_LLM))
        child = "TARGET 1 : 40.25 HITS ⭐️⭐️⭐️"
        raw = a.parse_signal(
            {
                "is_reply": True,
                "parent_text": "TSL TO 202.5 COST\n#LAURUSLABS 2100 CE",
                "child_text": child,
                "full_text": f"[CONTEXT: TSL TO 202.5 COST\n#LAURUSLABS 2100 CE] {child}",
            }
        )
        assert raw is not None
        assert raw["action"] == "RECONCILE"
        assert float(raw.get("stop_loss") or 0) == 0.0
